//! Table-driven coverage for #694: the `TransactionalId` `Write` ACL gate on
//! `Produce`.
//!
//! Kafka's `KafkaApis.handleProduceRequest` authorizes `Write` on
//! `TransactionalId(transactional_id)` when, and only when, the request
//! carries a transactional batch (`RequestUtils.hasTransactionalRecords`),
//! and it answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) on every
//! partition row of the whole request, before any topic is resolved, when
//! that check does not pass. This suite drives `handle()` (the same
//! `Produce` entry point the dispatcher calls) directly, against a broker
//! whose authorizer grants ACLs by the principal's name, so each case names
//! exactly the resource-type/operation pairs it holds.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_protocol::{
    owned::{
        create_topics_request::{self, CreatableTopic, CreateTopicsRequest},
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Attributes, Record, RecordBatch, RecordsPayload},
};

use super::{FIRST_TOPIC_ID_VERSION, handle};
use crate::{
    broker::Broker,
    codes,
    test_support::{
        GrantsInPrincipalName, decode_response, dispatch_context, encode_request, peer, principal,
        request_context, start_broker_with,
    },
    txn::coordinator::leadership::LoadStatus,
};

const TOPIC: &str = "orders";
const TXN_ID: &str = "t1";

/// A grant string a `GrantsInPrincipalName` principal needs to create topics.
const ADMIN_GRANTS: &str = "Cluster:Create";

async fn boot() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(GrantsInPrincipalName);
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    handle.wait_until_controller_leader().await;
    handle.wait_until_brokers_registered(1).await;
    let broker = handle.broker_arc_for_test();
    // The `__transaction_state` topic is normally bootstrapped lazily by a
    // `FindCoordinator(TRANSACTION)` call; `open_transaction` below drives
    // `InitProducerId` directly, so it is bootstrapped here instead, exactly
    // as `add_partitions_to_txn::authorization_tests` does.
    crate::txn::bootstrap::ensure_topic(
        &broker.controller,
        1,
        1,
        &crate::txn::bootstrap::topic_configs(
            broker.config.transaction_state_segment_bytes,
            broker.config.transaction_state_min_isr,
        ),
    )
    .await
    .expect("bootstrap __transaction_state");
    // Wait for the coordinator's own leadership/load bookkeeping, not just
    // the partition object -- see the same wait in
    // `add_partitions_to_txn::authorization_tests`.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while broker.txn_coordinator.load_status(PartitionIndex(0)).await
            != Some(LoadStatus::Loaded)
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("__transaction_state-0 becomes local");
    create_topic(&broker, TOPIC).await;
    (handle, dir)
}

async fn create_topic(broker: &Broker, name: &str) {
    let admin = principal(ADMIN_GRANTS);
    let address = peer();
    let ctx = request_context(&admin, &address, "produce-txn-authz-admin");
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    dispatch_context(
        broker,
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&request, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
}

/// Drives `InitProducerId` for `TXN_ID`, as a principal granted
/// `TransactionalId:Write`, and returns the producer id and epoch a
/// transactional batch must now carry. It does not also drive
/// `AddPartitionsToTxn`: every case below uses Produce v12+, which is
/// transaction version 2 (KIP-890) and enlists the partition itself as part
/// of the append (`FIRST_ADD_PARTITION_PRODUCE_VERSION` in
/// `producer_checks.rs`), the same way a real v12+ client would.
async fn open_transaction(broker: &Broker) -> (i64, i16) {
    let grantee = principal("TransactionalId:Write");
    let address = peer();
    let ctx = request_context(&grantee, &address, "produce-txn-authz-open");

    let init_request = InitProducerIdRequest {
        transactional_id: Some(TXN_ID.to_string()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let init_bytes = crate::handlers::init_producer_id::handle(
        broker,
        version,
        1,
        &encode_request(&init_request, version),
        &ctx,
    )
    .await
    .expect("InitProducerId");
    let init: InitProducerIdResponse = decode_response(&init_bytes, version);
    assert!(init.error_code == codes::NONE, "InitProducerId: {init:?}");
    (init.producer_id, init.producer_epoch)
}

/// One v2 batch with one record, non-transactional.
fn plain_batch() -> RecordsPayload {
    RecordsPayload::V2(vec![RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }])
}

/// One v2 batch with one record, marked transactional (KIP-98) under
/// `producer_id`/`producer_epoch`.
fn transactional_batch(producer_id: i64, producer_epoch: i16) -> RecordsPayload {
    RecordsPayload::V2(vec![RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        producer_id,
        producer_epoch,
        base_sequence: 0,
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }])
}

/// One request naming `TOPIC` by name (`version < FIRST_TOPIC_ID_VERSION`) or
/// by id (`version >= FIRST_TOPIC_ID_VERSION`), carrying `records` and
/// `transactional_id`.
fn produce_request(
    topic_id: WireUuid,
    version: i16,
    transactional_id: Option<&str>,
    records: RecordsPayload,
) -> ProduceRequest {
    let id_only = version >= FIRST_TOPIC_ID_VERSION;
    ProduceRequest {
        transactional_id: transactional_id.map(str::to_owned),
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: if id_only {
                String::new()
            } else {
                TOPIC.to_string()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(records),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The response row Kafka's per-partition `PartitionResponse(error)`
/// constructor builds for a row that failed before the append: `base_offset`,
/// `log_append_time_ms` and `log_start_offset` are all the -1 sentinel.
fn refused_row(index: i32, error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index,
        error_code,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        ..Default::default()
    }
}

async fn drive(
    broker: &Broker,
    grants: &str,
    version: i16,
    topic_id: WireUuid,
    transactional_id: Option<&str>,
    records: RecordsPayload,
) -> ProduceResponse {
    let request = produce_request(topic_id, version, transactional_id, records);
    let user = principal(grants);
    let address = peer();
    let ctx = request_context(&user, &address, "produce-txn-authz");
    let request_bytes = encode_request(&request, version);
    let response_bytes = handle(
        broker,
        version,
        7,
        &request_bytes,
        request_bytes.clone(),
        &ctx,
    )
    .await
    .expect("handle produce");
    decode_response(&response_bytes, version)
}

/// One case of [`transactional_id_write_gates_exactly_on_the_batch_not_the_request_field`].
struct Case<'a> {
    name: &'a str,
    version: i16,
    transactional: bool,
    transactional_id: Option<&'a str>,
    grants: &'a str,
    expected: i16,
}

/// Every case names its batch, its request-level `transactional_id`, the
/// grants its principal holds, and the row Kafka answers.
///
/// - "a transactional batch with a null `transactional_id`" is the
///   vulnerable case from #694: krabka used to key the check on
///   `transactional_id.is_some()` alone, so this request skipped the
///   `TransactionalId` `Write` check entirely and the batch reached
///   `verify_transactional_produce`, which resolves *some* transactional id
///   from the batch's producer id -- a principal with only topic `Write`
///   could write into a transaction it was never authorized against.
/// - "a non-transactional batch with a denied `transactional_id`" is the
///   mirror bug: krabka used to run the check whenever `transactional_id` was
///   present, regardless of the batch, and refused a plain produce that
///   Kafka accepts.
#[tokio::test]
async fn transactional_id_write_gates_exactly_on_the_batch_not_the_request_field() {
    let (handle, _dir) = boot().await;
    let broker = handle.broker_arc_for_test();
    let topic_id = {
        let image = handle.controller_image_for_test();
        WireUuid(
            image
                .topic(TOPIC)
                .expect("topic exists")
                .topic_id
                .into_bytes(),
        )
    };
    let (producer_id, producer_epoch) = open_transaction(&broker).await;
    let refused = codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED;

    let cases = [
        Case {
            name: "transactional batch, no transactional_id, no grant -> refused (the #694 hijack path)",
            version: 12,
            transactional: true,
            transactional_id: None,
            grants: "Topic:Write",
            expected: refused,
        },
        Case {
            name: "transactional batch, transactional_id, denied grant -> refused",
            version: 12,
            transactional: true,
            transactional_id: Some(TXN_ID),
            grants: "Topic:Write",
            expected: refused,
        },
        Case {
            name: "transactional batch, transactional_id, allowed grant -> appended",
            version: 12,
            transactional: true,
            transactional_id: Some(TXN_ID),
            grants: "Topic:Write+TransactionalId:Write",
            expected: codes::NONE,
        },
        Case {
            name: "non-transactional batch, transactional_id, denied grant -> appended (not checked)",
            version: 12,
            transactional: false,
            transactional_id: Some(TXN_ID),
            grants: "Topic:Write",
            expected: codes::NONE,
        },
        Case {
            name: "non-transactional batch, no transactional_id, no grant -> appended",
            version: 12,
            transactional: false,
            transactional_id: None,
            grants: "Topic:Write",
            expected: codes::NONE,
        },
    ];

    // Every appending case writes one record to the same partition, so the
    // base offset it gets back advances by one each time.
    let mut next_offset: i64 = 0;
    for case in cases {
        let records = if case.transactional {
            transactional_batch(producer_id, producer_epoch)
        } else {
            plain_batch()
        };
        let actual = drive(
            &broker,
            case.grants,
            case.version,
            topic_id,
            case.transactional_id,
            records,
        )
        .await;
        let expected_row = if case.expected == codes::NONE {
            let row = PartitionProduceResponse {
                index: 0,
                error_code: codes::NONE,
                base_offset: next_offset,
                log_append_time_ms: -1,
                log_start_offset: 0,
                ..Default::default()
            };
            next_offset += 1;
            row
        } else {
            refused_row(0, case.expected)
        };
        let expected = ProduceResponse {
            responses: vec![TopicProduceResponse {
                name: TOPIC.to_string(),
                topic_id: WireUuid::ZERO,
                partition_responses: vec![expected_row],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(actual == expected, "{}", case.name);
    }
    handle.shutdown().await;
}

/// Kafka answers the 53 row before it resolves any topic: an unknown topic id
/// at v13+, which would otherwise answer `UNKNOWN_TOPIC_ID` (100), still
/// answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) when the request
/// carries a transactional batch and the `transactional_id` check fails.
#[tokio::test]
async fn transactional_denial_precedes_topic_resolution_at_v13() {
    let (handle, _dir) = boot().await;
    let broker = handle.broker_arc_for_test();

    let unknown_id = WireUuid([0x0b; 16]);
    let version = 13;
    let actual = drive(
        &broker,
        "Topic:Write",
        version,
        unknown_id,
        Some(TXN_ID),
        transactional_batch(999, 0),
    )
    .await;
    let expected = ProduceResponse {
        responses: vec![TopicProduceResponse {
            name: String::new(),
            topic_id: unknown_id,
            partition_responses: vec![refused_row(0, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(actual == expected);
    handle.shutdown().await;
}
