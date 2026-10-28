//! Handler tests for the order of the per-partition acknowledge checks of
//! `ShareFetch` and `ShareAcknowledge`, and for the partition that the
//! metadata does not hold.
//!
//! Kafka's `KafkaApis.handleAcknowledgements` runs
//! `validateAcknowledgementBatches` first (`INVALID_REQUEST`), then the topic
//! `Read` check (`TOPIC_AUTHORIZATION_FAILED`), then the metadata check
//! (`UNKNOWN_TOPIC_OR_PARTITION`). Only a partition that passes all three
//! reaches the share partition. The fetch half of a `ShareFetch` row runs the
//! `Read` check and the metadata check on its own, so a row can carry a fetch
//! error and a different acknowledge error.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::AuthorizationResult,
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            AcknowledgementMode, AcknowledgementPartitionSetup, OffsetRun, ShareSessionEpoch,
            StartedBroker, acknowledge_batches_request, acquired_share_records,
            create_topic as create_topic_from_test_support, initialize_earliest_share,
            produce_records, share_acknowledge_wire, share_fetch_wire, share_record_states,
        },
        share_partition::state::RecordState::{self, Acquired},
        test_support::{start_broker_no_audit_with, tolerate_starved_heartbeats},
    },
    assert2::assert,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned::{
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_request::{
                AcknowledgementBatch as FetchAcknowledgeBatch, FetchPartition, FetchTopic,
                ShareFetchRequest,
            },
            share_fetch_response::ShareFetchResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

const TOPIC: &str = "ack-order";
const VERSION: i16 = 2;

/// The principal that the topic `Read` check refuses.
const DENIED: &str = "denied";
/// The principal that may read everything.
const READER: &str = "reader";

/// The row's `(error_code, acknowledge_error_code)` of a `ShareFetch`.
#[derive(Clone, Copy, PartialEq, Eq)]
struct RowCodes {
    error_code: i16,
    acknowledge_error_code: i16,
}

/// Denies topic `Read` to [`DENIED`] and allows everything else.
#[derive(Debug)]
struct DenyOnePrincipal;

krabka_macros::test_authorizer!(DenyOnePrincipal, request; {
    if request.principal.name == DENIED
        && request.resource_type == ResourceType::Topic
        && request.operation == AclOperation::Read
    {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

async fn start() -> StartedBroker {
    let HandlerStartedBroker { broker, dir } = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(DenyOnePrincipal);
        tolerate_starved_heartbeats(cfg);
    })
    .await;
    StartedBroker { broker, dir }
}

async fn create_topic(broker: &BrokerHandle) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("ack-order-test")
        .name(TOPIC)
        .call()
        .await
}

/// Appends one batch of three records to partition 0.
async fn produce(broker: &BrokerHandle) {
    produce_records().broker(broker).topic(TOPIC).call().await;
}

/// Starts `group` at the earliest offset and lets [`READER`] acquire offsets
/// 0 to 2 at epoch 0.
async fn acquire_all(broker: &BrokerHandle, group: &str, topic_id: WireUuid) {
    initialize_earliest_share(broker, group, topic_id, 0..1).await;
    let response = share_fetch()
        .broker(broker)
        .group(group)
        .epoch(0)
        .topic_id(topic_id)
        .call()
        .await;
    let acquired: Vec<_> = acquired_share_records(&response.responses[0].partitions[0]);
    assert!(acquired == vec![OffsetRun::new(0, 2)], "{response:?}");
}

/// A `ShareFetch` that names each `(partition, batches)` row of `rows`.
#[derive(krabka_macros::FieldDefaults)]
struct ValidationFetchSetup<'a> {
    #[default(READER)]
    user: &'a str,
    #[default("ack-order")]
    group: &'a str,
    epoch: ShareSessionEpoch,
    topic_id: WireUuid,
    #[default(vec![AcknowledgementPartitionSetup::default()])]
    rows: Vec<AcknowledgementPartitionSetup>,
    mode: AcknowledgementMode,
}

#[bon::builder]
async fn share_fetch(
    broker: &BrokerHandle,
    #[builder(default = READER)] user: &'_ str,
    #[builder(default = "ack-order")] group: &'_ str,
    #[builder(into, default)] epoch: ShareSessionEpoch,
    #[builder(default)] topic_id: WireUuid,
    #[builder(default = vec ! [AcknowledgementPartitionSetup :: default ()])] rows: Vec<
        AcknowledgementPartitionSetup,
    >,
    #[builder(default)] mode: AcknowledgementMode,
) -> ShareFetchResponse {
    let setup = ValidationFetchSetup {
        user,
        group,
        epoch,
        topic_id,
        rows,
        mode,
    };
    let request = ShareFetchRequest {
        group_id: Some(setup.group.into()),
        member_id: Some("member".into()),
        share_session_epoch: setup.epoch.0,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        is_renew_ack: matches!(setup.mode, AcknowledgementMode::Renew),
        topics: vec![FetchTopic {
            topic_id: setup.topic_id,
            partitions: setup
                .rows
                .into_iter()
                .map(|row| FetchPartition {
                    partition_index: row.index.0,
                    acknowledgement_batches: krabka_macros::acknowledgement_batches!(FetchAcknowledgeBatch; [row.batches]),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    share_fetch_wire()
        .broker(broker)
        .request(&request)
        .version(VERSION)
        .user(setup.user)
        .call()
        .await
}

#[derive(krabka_macros::FieldDefaults)]
struct ValidationAcknowledgeSetup<'a> {
    #[default(READER)]
    user: &'a str,
    #[default("ack-order")]
    group: &'a str,
    topic_id: WireUuid,
    partition: AcknowledgementPartitionSetup,
}

#[bon::builder]
async fn share_acknowledge(
    broker: &BrokerHandle,
    #[builder(default = READER)] user: &'_ str,
    #[builder(default = "ack-order")] group: &'_ str,
    #[builder(default)] topic_id: WireUuid,
    #[builder(default)] partition: AcknowledgementPartitionSetup,
) -> ShareAcknowledgeResponse {
    let setup = ValidationAcknowledgeSetup {
        user,
        group,
        topic_id,
        partition,
    };
    let request = acknowledge_batches_request()
        .group(setup.group)
        .topic_id(setup.topic_id)
        .partition(setup.partition)
        .call();
    share_acknowledge_wire()
        .broker(broker)
        .request(&request)
        .version(VERSION)
        .user(setup.user)
        .call()
        .await
}

async fn record_states(broker: &BrokerHandle, group: &str, topic_id: WireUuid) -> Vec<RecordState> {
    share_record_states(broker, group, topic_id).await
}

#[derive(Default)]
struct AcknowledgementOrderScenario {
    started: Option<StartedBroker>,
    topic_id: WireUuid,
    rows: Vec<(i32, i16, i16, Vec<RecordState>)>,
}
#[rstest::fixture]
fn acknowledgement_order() -> AcknowledgementOrderScenario {
    let started = None;
    AcknowledgementOrderScenario {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "reader acquired offsets 0 through 2 of ack-order partition 0 in separate fetch and acknowledge groups"
)]
async fn acquired_ack_order_records(acknowledgement_order: &mut AcknowledgementOrderScenario) {
    let started = start().await;
    let topic_id = create_topic(&started.broker).await;
    produce(&started.broker).await;
    acquire_all(&started.broker, "fetch-order", topic_id).await;
    acquire_all(&started.broker, "acknowledge-order", topic_id).await;
    acknowledgement_order.topic_id = topic_id;
    acknowledgement_order.started = Some(started);
}
#[rstest_bdd_macros::when(
    "principal {user} sends these acknowledgement batches for partition {partition} through both share APIs"
)]
async fn ordered_ack_requests(
    acknowledgement_order: &mut AcknowledgementOrderScenario,
    user: String,
    partition: i32,
    datatable: Vec<Vec<String>>,
) {
    use crate::handlers::test_support::{AcknowledgementBatchSetup, AcknowledgementCode};
    let batches = datatable
        .into_iter()
        .skip(1)
        .map(|cells| AcknowledgementBatchSetup {
            first_offset: krabka_log::Offset(cells[0].parse().expect("first offset")),
            last_offset: krabka_log::Offset(cells[1].parse().expect("last offset")),
            types: cells[2]
                .split(',')
                .filter(|kind| *kind != "none")
                .map(|kind| AcknowledgementCode(kind.parse().expect("ack type")))
                .collect(),
        })
        .collect();
    let input = AcknowledgementPartitionSetup {
        index: krabka_ids::PartitionIndex(partition),
        batches,
    };
    let broker = &acknowledgement_order
        .started
        .as_ref()
        .expect("acquired groups")
        .broker;
    let fetch = share_fetch()
        .broker(broker)
        .user(&user)
        .group("fetch-order")
        .topic_id(acknowledgement_order.topic_id)
        .rows(vec![input.clone()])
        .call()
        .await;
    let row = &fetch.responses[0].partitions[0];
    acknowledgement_order.rows.push((
        row.partition_index,
        row.error_code,
        row.acknowledge_error_code,
        record_states(broker, "fetch-order", acknowledgement_order.topic_id).await,
    ));
    let acknowledge = share_acknowledge()
        .broker(broker)
        .user(&user)
        .group("acknowledge-order")
        .topic_id(acknowledgement_order.topic_id)
        .partition(input)
        .call()
        .await;
    let row = &acknowledge.responses[0].partitions[0];
    acknowledgement_order.rows.push((
        row.partition_index,
        row.error_code,
        codes::NONE,
        record_states(broker, "acknowledge-order", acknowledgement_order.topic_id).await,
    ));
}
#[rstest_bdd_macros::then("the share API rows and remaining acquired records are exactly")]
async fn acknowledgement_order_results(
    acknowledgement_order: &mut AcknowledgementOrderScenario,
    datatable: Vec<Vec<String>>,
) {
    let expected = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            (
                cells[1].parse().expect("partition"),
                cells[2].parse().expect("fetch or acknowledge error"),
                cells[3].parse().expect("piggyback acknowledge error"),
                cells[4]
                    .split(',')
                    .filter(|state| *state != "none")
                    .map(|state| match state {
                        "acquired" => Acquired,
                        other => panic!("unknown record state {other}"),
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert!(acknowledgement_order.rows == expected);
    acknowledgement_order
        .started
        .take()
        .expect("started broker")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Authorized valid acknowledgements settle acquired records"
)]
#[tokio::test]
async fn acknowledgement_order_case_0(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Topic denial precedes state mutation"
)]
#[tokio::test]
async fn acknowledgement_order_case_1(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Batch validation precedes acknowledge ACL checks"
)]
#[tokio::test]
async fn acknowledgement_order_case_2(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Reversed offsets are invalid"
)]
#[tokio::test]
async fn acknowledgement_order_case_3(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Overlapping batches are invalid"
)]
#[tokio::test]
async fn acknowledgement_order_case_4(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "A missing acknowledge type is invalid"
)]
#[tokio::test]
async fn acknowledgement_order_case_5(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Per-offset type counts must match the range"
)]
#[tokio::test]
async fn acknowledgement_order_case_6(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Unknown acknowledge types are invalid"
)]
#[tokio::test]
async fn acknowledgement_order_case_7(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Renew requires the renewal request flag"
)]
#[tokio::test]
async fn acknowledgement_order_case_8(acknowledgement_order: AcknowledgementOrderScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_ack_validation_order.feature",
    name = "Metadata validation rejects a missing partition"
)]
#[tokio::test]
async fn acknowledgement_order_case_9(acknowledgement_order: AcknowledgementOrderScenario) {}

/// A denied topic row without acknowledgements in a request without any
/// acknowledgement answers only the fetch error.
#[tokio::test]
async fn a_denied_row_without_acknowledgements_has_no_acknowledge_error() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let topic_id = create_topic(&broker).await;
    produce(&broker).await;
    acquire_all(&broker, "no-acks", topic_id).await;

    let response = share_fetch()
        .broker(&broker)
        .user(DENIED)
        .group("no-acks")
        .topic_id(topic_id)
        .call()
        .await;
    let row = &response.responses[0].partitions[0];

    assert!(
        RowCodes {
            error_code: row.error_code,
            acknowledge_error_code: row.acknowledge_error_code
        } == RowCodes {
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            acknowledge_error_code: codes::NONE
        }
    );
    broker.shutdown().await;
}
