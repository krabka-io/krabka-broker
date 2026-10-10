//! The first sequence at a new producer epoch, before and after a restart.
//!
//! Kafka's `ProducerAppendInfo.checkSequence` answers
//! `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) when a batch changes the producer epoch
//! and its first sequence is not 0. A transaction-version-2 end marker bumps
//! the epoch, so the next transaction must start at sequence 0. Each case
//! sends the rejected batch to the live broker, restarts the broker on the
//! same data directory, sends the rejected batch again, and then sends the
//! batch that Kafka accepts. The answer must not depend on the restart.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{init_producer_id_request::InitProducerIdRequest, produce_request::ProduceRequest},
    records::{Attributes, RecordBatch},
};
use tempfile::TempDir;

use crate::{
    support::{
        records::{batch_from_records, value_record},
        transactions::end_transaction_request,
    },
    txnver_harness::{
        admin_client, config, create_topic, downgrade_transaction_version, find_coordinator,
        topic_id,
    },
};

const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// How the producer reaches the epoch that the probes use.
#[derive(Debug, Clone, Copy)]
enum Setup {
    /// A transactional producer commits one transaction of three records.
    /// `downgrade_to` selects the `transaction.version` level.
    Transaction { downgrade_to: Option<i16> },
    /// An idempotent producer writes three records at epoch 0. The probes use
    /// epoch 1, as the Java client does after a KIP-360 local epoch bump.
    Idempotent,
}

struct Case {
    name: &'static str,
    topic: &'static str,
    setup: Setup,
    /// `(epoch offset from the setup epoch, base sequence)` of the batch Kafka
    /// rejects.
    rejected: (i16, i32),
    /// `(epoch offset from the setup epoch, base sequence)` of the batch Kafka
    /// accepts.
    accepted: (i16, i32),
}

struct Identity {
    transactional_id: Option<&'static str>,
    producer_id: i64,
    epoch: i16,
}

fn batch(producer: &Identity, epoch: i16, base_sequence: i32, records: i32) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(producer.transactional_id.is_some()),
        producer_id: producer.producer_id,
        producer_epoch: epoch,
        base_sequence,
        last_offset_delta: records - 1,
        max_timestamp: 1,
        ..batch_from_records(
            (0..records)
                .map(|offset_delta| {
                    value_record(offset_delta, Some(bytes::Bytes::from_static(b"v")))
                })
                .collect(),
        )
    }
}

/// Send one batch. Retry only while the partition or its leader is not ready
/// after a start, and return the final error code.
async fn produce(
    client: &Client,
    topic: &str,
    (batch_transactional_id, batch): (Option<&str>, RecordBatch),
) -> i16 {
    let id = topic_id(client, topic).await;
    let request = ProduceRequest {
        // A Kafka producer names its transactional id on every request.
        transactional_id: batch_transactional_id.map(str::to_owned),
        ..crate::support::produce::single_partition_produce(
            topic,
            id,
            0,
            Some(batch.into()),
            (-1, 5_000),
        )
    };
    let response =
        crate::support::transaction_wire::settled_produce(client, request, Duration::from_secs(10))
            .await;
    response.responses[0].partition_responses[0].error_code
}

/// Send a coordinator request until the coordinator has loaded its state.
async fn until_coordinator_ready<F, Fut>(send: F) -> i16
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = i16>,
{
    crate::support::transaction_wire::retry_coordinator(
        send,
        |code| *code,
        |code| {
            crate::support::transaction_wire::coordinator_loading(code)
                || code == CONCURRENT_TRANSACTIONS
        },
        Duration::from_secs(10),
    )
    .await
}

async fn add_partition(client: &Client, producer: &Identity, topic: &str, epoch: i16) -> i16 {
    let transactional_id = producer.transactional_id.expect("transactional producer");
    find_coordinator(client, transactional_id).await;
    until_coordinator_ready(|| async {
        let response = client
            .send(crate::support::transaction_wire::add_partition_request(
                transactional_id,
                topic,
                (producer.producer_id, epoch),
            ))
            .await
            .expect("AddPartitionsToTxn");
        crate::support::transaction_wire::partition_error(&response, false)
    })
    .await
}

/// `FindCoordinator` for a transactional id. It also starts the creation of
/// the transaction-state topic on a new cluster.
async fn init_transactional_producer(
    client: &Client,
    transactional_id: &str,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse {
    find_coordinator(client, transactional_id).await;
    crate::support::transaction_wire::initialize_transactional(
        client,
        transactional_id,
        |code| {
            crate::support::transaction_wire::coordinator_loading(code)
                || code == CONCURRENT_TRANSACTIONS
        },
        Duration::from_secs(10),
    )
    .await
}

async fn produce_setup(client: &Client, case: &Case, producer: &Identity) {
    let code = produce(
        client,
        case.topic,
        (
            producer.transactional_id,
            batch(producer, producer.epoch, 0, 3),
        ),
    )
    .await;
    assert!(code == 0, "{}: setup produce", case.name);
}

/// Run the setup and return the producer at the epoch the probes start from.
async fn set_up(client: &Client, case: &Case) -> Identity {
    match case.setup {
        Setup::Idempotent => {
            let init = client
                .send(InitProducerIdRequest {
                    transactional_id: None,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                })
                .await
                .expect("InitProducerId");
            let producer = Identity {
                transactional_id: None,
                producer_id: init.producer_id,
                epoch: init.producer_epoch,
            };
            produce_setup(client, case, &producer).await;
            Identity {
                epoch: producer.epoch + 1,
                ..producer
            }
        }
        Setup::Transaction { downgrade_to } => {
            if let Some(level) = downgrade_to {
                downgrade_transaction_version(client, level).await;
            }
            let transactional_id = case.name;
            let init = init_transactional_producer(client, transactional_id).await;
            let producer = Identity {
                transactional_id: Some(transactional_id),
                producer_id: init.producer_id,
                epoch: init.producer_epoch,
            };
            let added = add_partition(client, &producer, case.topic, producer.epoch).await;
            assert!(added == 0, "{}: AddPartitionsToTxn", case.name);
            produce_setup(client, case, &producer).await;
            let end = client
                .send(end_transaction_request(
                    transactional_id,
                    (producer.producer_id, producer.epoch),
                    true,
                ))
                .await
                .expect("EndTxn");
            assert!(end.error_code == 0, "{}: EndTxn {end:?}", case.name);
            // EndTxn v5 carries the bumped epoch at every cluster level, since
            // the request version decides the client transaction version.
            let epoch = if end.producer_id >= 0 {
                end.producer_epoch
            } else {
                producer.epoch
            };
            Identity { epoch, ..producer }
        }
    }
}

async fn probe(
    client: &Client,
    case: &Case,
    producer: &Identity,
    (offset, sequence): (i16, i32),
) -> i16 {
    let epoch = producer.epoch + offset;
    if producer.transactional_id.is_some() {
        let added = add_partition(client, producer, case.topic, epoch).await;
        assert!(
            added == 0,
            "{}: AddPartitionsToTxn before a probe",
            case.name
        );
    }
    produce(
        client,
        case.topic,
        (
            producer.transactional_id,
            batch(producer, epoch, sequence, 1),
        ),
    )
    .await
}

/// Error codes: the rejected batch live, the rejected batch after a restart,
/// and the accepted batch after the restart.
async fn codes_across_restart(case: &Case) -> [i16; 3] {
    let directory = TempDir::new().expect("tempdir");
    let log_dir = directory.path().to_path_buf();

    let broker = Broker::start(config(log_dir.clone(), None))
        .await
        .expect("start broker");
    let client = admin_client(&broker.listen_addr().to_string()).await;
    create_topic(
        &client,
        case.topic,
        crate::support::topics::TopicPartitionCount(1),
    )
    .await;
    let producer = set_up(&client, case).await;
    let live_rejected = probe(&client, case, &producer, case.rejected).await;
    broker.shutdown().await;

    let broker = Broker::start(config(log_dir, Some(BootstrapMode::Rejoin)))
        .await
        .expect("restart broker");
    let client = admin_client(&broker.listen_addr().to_string()).await;
    let recovered_rejected = probe(&client, case, &producer, case.rejected).await;
    let recovered_accepted = probe(&client, case, &producer, case.accepted).await;
    broker.shutdown().await;

    [live_rejected, recovered_rejected, recovered_accepted]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_sequence_at_a_new_epoch_gets_the_same_answer_before_and_after_restart() {
    let cases = [
        Case {
            name: "tv2-commit-bumps-the-epoch",
            topic: "seq-tv2",
            setup: Setup::Transaction { downgrade_to: None },
            rejected: (0, 3),
            accepted: (0, 0),
        },
        Case {
            name: "tv1-cluster-v5-commit-bumps-the-epoch",
            topic: "seq-tv1",
            setup: Setup::Transaction {
                downgrade_to: Some(1),
            },
            // The EndTxn v5 request of this client is a TV_2 client whatever
            // `transaction.version` the cluster finalized (Kafka's
            // `transactionVersionForEndTxn`), so the commit bumps the epoch as
            // it does at TV_2.
            rejected: (0, 3),
            accepted: (0, 0),
        },
        Case {
            name: "idempotent-epoch-bump",
            topic: "seq-idempotent",
            setup: Setup::Idempotent,
            rejected: (0, 3),
            accepted: (0, 0),
        },
    ];
    for case in &cases {
        let codes = codes_across_restart(case).await;
        assert!(
            codes
                == [
                    OUT_OF_ORDER_SEQUENCE_NUMBER,
                    OUT_OF_ORDER_SEQUENCE_NUMBER,
                    0
                ],
            "{}: [live rejected, recovered rejected, recovered accepted]",
            case.name
        );
    }
}
