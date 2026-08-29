//! `ListOffsets(LATEST)` and the request's `isolation_level` (KIP-98), proved
//! over the wire against a live broker with a real transaction open.
//!
//! # Why this suite exists
//!
//! `read_committed` is the isolation level a consumer picks when it must not
//! see the records of a transaction that has not resolved. `ListOffsets` with
//! the `LATEST` sentinel is how such a consumer finds the end of a partition --
//! it is what `seekToEnd`, `endOffsets` and every lag calculation are built on.
//! A broker that answers `LATEST` from the log end offset whatever the request
//! asked for hands a `read_committed` consumer a position past records it is
//! not allowed to read, which is precisely the distinction the isolation level
//! exists to draw. Kafka answers such a request with the partition's last
//! stable offset instead, and this suite is the tier that says so on a real
//! socket through the real Kafka codecs.
//!
//! # Both isolation levels are asked on both sides of the resolution
//!
//! Every case reads `LATEST` at `read_uncommitted` *and* at `read_committed`,
//! while the transaction is open and again after it ends. The pair has to
//! disagree in the first reading and agree in the second. A case that compared
//! them only after the commit would pass against a broker that ignored
//! `isolation_level` entirely, because a resolved transaction pins nothing --
//! the disagreement while the transaction is open is the whole assertion.
//!
//! # Records precede the transaction
//!
//! Each case writes two ordinary records before it opens the transaction. The
//! `read_committed` answer is then the offset the transaction started at rather
//! than zero, so a broker that answered the log start offset, or a constant, is
//! caught here too.

mod support;

use assert2::check;
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsPartitionResponse,
};

/// Request timestamp sentinel (-1) asking for the end of the partition.
/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;
/// Request `replica_id` (-1) that marks an ordinary client. Kafka's
/// `ListOffsetsRequest.CONSUMER_REPLICA_ID`.
const CONSUMER_REPLICA_ID: i32 = -1;
/// Request `isolation_level` (0). Kafka's `IsolationLevel.READ_UNCOMMITTED`.
const READ_UNCOMMITTED: i8 = 0;
/// Request `isolation_level` (1). Kafka's `IsolationLevel.READ_COMMITTED`.
const READ_COMMITTED: i8 = 1;

/// How the transaction ends.
///
/// Both resolutions write a control marker to the partition and both release
/// the last stable offset past it, so `ListOffsets` must answer the same way
/// for either. Which of the records a later `Fetch` then hands back is the
/// difference between them, and that is `Fetch`'s business rather than this
/// suite's.
#[derive(Clone, Copy, Debug)]
enum Resolution {
    Commit,
    Abort,
}

/// The end of one partition as the two isolation levels see it at one instant.
///
/// The pair is one value so a case asserts against a whole expected struct
/// rather than against two offsets in sequence, which is what makes "they
/// disagree here and agree there" a single readable claim.
#[derive(Debug, PartialEq, Eq)]
struct EndOfPartition {
    read_uncommitted: ListOffsetsPartitionResponse,
    read_committed: ListOffsetsPartitionResponse,
}

/// The whole `LATEST` row a healthy partition 0 answers with.
///
/// `LATEST` matches no record, so the response echoes Kafka's
/// `UNKNOWN_TIMESTAMP` (-1), and the handler leaves the leader epoch at the
/// same sentinel. Spelling the full row out is what makes an isolation level
/// that quietly changed the error code or the timestamp fail here too.
fn latest_row(offset: i64) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: codes::NONE,
        timestamp: -1,
        offset,
        leader_epoch: -1,
        ..Default::default()
    }
}

async fn create_topic(client: &Client, name: &str) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    check!(
        response.topics[0].error_code == codes::NONE,
        "create_topic({name}): {response:?}"
    );
}

/// One `ListOffsets(LATEST)` for partition 0 of `topic` at `isolation_level`.
async fn latest(client: &Client, topic: &str, isolation_level: i8) -> ListOffsetsPartitionResponse {
    let mut response = client
        .send(ListOffsetsRequest {
            replica_id: CONSUMER_REPLICA_ID,
            isolation_level,
            topics: vec![ListOffsetsTopic {
                name: topic.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: LATEST_TIMESTAMP,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    response.topics.remove(0).partitions.remove(0)
}

/// Ask for the end of `topic` at both isolation levels.
async fn end_of_partition(client: &Client, topic: &str) -> EndOfPartition {
    EndOfPartition {
        read_uncommitted: latest(client, topic, READ_UNCOMMITTED).await,
        read_committed: latest(client, topic, READ_COMMITTED).await,
    }
}

/// Wait until partition 0 of `topic` holds `offset` records and has committed
/// all of them, so a `ListOffsets` taken next is reading settled state.
///
/// Both bounds are needed. The log end offset says the append landed, and the
/// high watermark says it is acknowledged -- and the last stable offset a
/// `read_committed` client is answered with is capped at the high watermark, so
/// a reading taken before the watermark caught up would measure the wrong
/// thing.
async fn wait_for_settled_log(broker: &BrokerHandle, topic: &str, offset: i64) {
    broker
        .wait_until_local_log_end_offset(topic, 0, offset)
        .await;
    broker.wait_until_high_watermark(topic, 0, offset).await;
}

fn record(topic: &str, value: &'static str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        value: Some(Bytes::from_static(value.as_bytes())),
        ..Default::default()
    }
}

async fn send_ok(producer: &Producer, topic: &str, value: &'static str) {
    producer
        .send(record(topic, value))
        .await
        .await
        .expect("producer delivery channel open")
        .expect("produce acknowledged");
}

/// `LATEST` under `read_committed` stops at the first record of an open
/// transaction, and catches up with `read_uncommitted` once that transaction
/// resolves either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_latest_stops_at_an_open_transaction() {
    let p = support::start().await;
    let bootstrap = p.broker.listen_addr().to_string();

    for (label, resolution) in [
        // A commit publishes the records. The last stable offset moves past
        // the commit marker, so the end of the partition is the whole log.
        ("commit", Resolution::Commit),
        // An abort discards them for a `read_committed` reader, but it still
        // writes a marker and still releases the last stable offset. A broker
        // that released it only on commit would leave every `read_committed`
        // consumer of the partition pinned behind a transaction that is over.
        ("abort", Resolution::Abort),
    ] {
        let topic = format!("list-offsets-isolation-{label}");
        create_topic(&p.client, &topic).await;
        p.broker.wait_until_partition_present(&topic, 0).await;

        // Two ordinary records first, so the transaction does not start at
        // offset 0 and the `read_committed` answer below cannot be confused
        // with the log start offset.
        let plain = Producer::builder()
            .bootstrap(bootstrap.clone())
            .build()
            .await
            .expect("plain producer build");
        for value in ["settled-a", "settled-b"] {
            send_ok(&plain, &topic, value).await;
        }
        wait_for_settled_log(&p.broker, &topic, 2).await;

        // With no transaction anywhere on the partition the two isolation
        // levels must already agree. This is the control reading: it separates
        // "the last stable offset holds a `read_committed` client back" from
        // "the `read_committed` answer is broken".
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(2),
                    read_committed: latest_row(2),
                },
            "{label}: a partition with no transaction on it has one end"
        );

        let producer = Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id(format!("list-offsets-isolation-{label}-tid"))
            .build()
            .await
            .expect("transactional producer build");
        producer
            .init_transactions()
            .await
            .expect("init_transactions");
        let txn = producer
            .begin_transaction()
            .await
            .expect("begin_transaction");
        for value in ["open-c", "open-d"] {
            send_ok(&producer, &topic, value).await;
        }
        wait_for_settled_log(&p.broker, &topic, 4).await;

        // The transaction is open. `read_uncommitted` sees the whole log, and
        // `read_committed` stops at offset 2, the first record the transaction
        // wrote. This is the reading the bug got wrong: it answered 4 to both.
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(4),
                    read_committed: latest_row(2),
                },
            "{label}: an open transaction holds read_committed at its first record"
        );

        match resolution {
            Resolution::Commit => txn.commit().await.expect("commit"),
            Resolution::Abort => txn.abort().await.expect("abort"),
        }

        // The marker is the fifth record on the partition, and it releases the
        // last stable offset. Both isolation levels are back to one answer.
        wait_for_settled_log(&p.broker, &topic, 5).await;
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(5),
                    read_committed: latest_row(5),
                },
            "{label}: a resolved transaction releases read_committed"
        );

        producer
            .close()
            .await
            .expect("transactional producer close");
        plain.close().await.expect("plain producer close");
    }

    p.broker.shutdown().await;
}
