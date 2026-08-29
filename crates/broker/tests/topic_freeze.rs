//! KFC-9 topic write freeze, proved over the wire against a live broker.
//!
//! The broker half of the freeze is unit-tested inside `crabka-broker`, and
//! nothing in that tier reaches the wire. A resolver that answers correctly and
//! a produce path that ignores its answer both pass those tests. This suite is
//! the tier that says a refusal refuses, on a real socket, through the real
//! Kafka codecs.
//!
//! # Every case runs a control topic
//!
//! Each case creates an unfrozen topic beside the frozen one and produces to it
//! in the same shape. That is the form KFC-1's suite established, and it is
//! what separates "this topic is frozen" from "the produce path is broken".
//! Delete the control half and a suite that refused *every* write would still
//! be green.
//!
//! # Every refusal asserts the log end offset
//!
//! A rejection is checked as a whole [`ProduceOutcome`]: the error code, the
//! `error_message` the producer's on-call reads, and the partition's log end
//! offset. The third field is the load-bearing one. The freeze gate sits ahead
//! of the idempotent-sequence gate precisely so a refused batch leaves producer
//! state and the log untouched, and an error code alone does not rule out a
//! broker that answered `POLICY_VIOLATION` *and* appended. That is the worst
//! failure this feature can have, so it is asserted rather than assumed.
//!
//! # The signing bytes are reproduced here
//!
//! `crabka_broker::freeze::signing::freeze_signing_bytes` is `pub(crate)`
//! inside a `pub(crate)` module, so no test crate can call it. [`signing_bytes`]
//! rebuilds the layout that `crates/broker/src/freeze/signing.rs` documents,
//! field by field. `crabka-guard` carries a third copy for the same reason, and
//! a drift between any two of them fails here: a signature this file makes has
//! to verify inside the broker, which is only true while both layouts agree.

mod support;

use std::{
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use bytes::Bytes;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, codes};
use crabka_client_core::Client;
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_protocol::{
    krabka::{
        break_glass::{ApproveBreakGlassRequest, ProposeBreakGlassRequest},
        freeze::{
            DescribeTopicFreezesRequest, DescribedTopicFreeze, PATTERN_TYPE_LITERAL,
            PATTERN_TYPE_PREFIXED, SetTopicFreezeRequest, SetTopicFreezeResponse,
        },
    },
    owned::{
        alter_configs_request::{
            AlterConfigsRequest, AlterConfigsResource as AlterResource,
            AlterableConfig as AlterConfig,
        },
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        describe_cluster_request::DescribeClusterRequest,
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResourceResult,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        incremental_alter_configs_request::{
            AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
            IncrementalAlterConfigsRequest,
        },
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::PartitionProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};
use support::OperatorKey;

/// The unfrozen topic that every case produces to beside the frozen one.
const CONTROL: &str = "control";

/// Kafka's `RESOURCE_TYPE` for a topic, which both config paths take.
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// `IncrementalAlterConfigs` `config_operation` SET.
const CONFIG_OP_SET: i8 = 0;

/// `config_operation` DELETE, which is how an operator would try to *clear* a
/// freeze through the config path.
const CONFIG_OP_DELETE: i8 = 1;

/// The synthesised read-only topic config that reports the freeze.
const WRITE_FREEZE: &str = "write.freeze";

/// Domain separator for a freeze-record signature.
///
/// It is the value `crate::signing_domains::FREEZE_DOMAIN` holds inside the
/// broker. That constant is `pub(crate)`, so this suite carries the literal.
const FREEZE_DOMAIN: &[u8] = b"crabka-topic-freeze-v1\0";

/// The `ThawTopicFreeze` break-glass action, on the wire.
const ACTION_THAW: i8 = 1;

/// How long a wire-visible state change gets before a case gives up on it.
const SETTLE: Duration = Duration::from_secs(10);

// ── the wire, in the shapes every case reuses ────────────────────────────────

/// Milliseconds since the Unix epoch, which is what `set_at_ms` carries.
fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_millis(),
    )
    .expect("a timestamp inside i64")
}

/// Create a one-partition topic and wait for its partition to exist locally.
async fn create_topic(broker: &BrokerHandle, client: &Client, name: &str) -> WireUuid {
    let resp = client
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
    let created = &resp.topics[0];
    assert!(
        created.error_code == 0,
        "create {name}: {:?}",
        created.error_message
    );
    broker.wait_until_partition_present(name, 0).await;
    support::topic_id_for(client, name).await
}

/// A single-record batch, in the shape a plain (non-idempotent) producer sends.
fn one_record(value: &str) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    batch.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(value.to_owned())),
        ..Default::default()
    });
    batch
}

/// Produce one record and hand back the partition row.
async fn produce(client: &Client, topic: &str, topic_id: WireUuid) -> PartitionProduceResponse {
    let resp = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(vec![one_record("v")])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    resp.responses[0].partition_responses[0].clone()
}

/// What one produce did, in the three terms a freeze case cares about.
///
/// The log end offset is part of the value rather than a second assertion,
/// because the two have to be read together: a `POLICY_VIOLATION` that still
/// moved the log is a pass on the error code and a catastrophe on the feature.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProduceOutcome {
    error_code: i16,
    error_message: Option<String>,
    log_end_offset: Option<i64>,
}

/// Produce one record and read the partition's log end offset afterwards.
async fn produce_outcome(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
) -> ProduceOutcome {
    let response = produce(client, topic, topic_id).await;
    ProduceOutcome {
        error_code: response.error_code,
        error_message: response.error_message,
        log_end_offset: broker.local_log_end_offset(topic, 0),
    }
}

/// The outcome of an accepted produce that leaves the log at `log_end_offset`.
fn accepted(log_end_offset: i64) -> ProduceOutcome {
    ProduceOutcome {
        error_code: codes::NONE,
        error_message: None,
        log_end_offset: Some(log_end_offset),
    }
}

/// The outcome of a produce that a freeze on `scope` refused, with the log
/// still at `log_end_offset`.
///
/// The message is spelled out rather than matched loosely. It is the only thing
/// KIP-108's `POLICY_VIOLATION` gives the producer's on-call engineer, and the
/// whole argument for reusing code 44 instead of minting a private one is that
/// the message carries the detail. A message that stopped naming the scope
/// would leave an operator with a non-retriable failure and no next step.
fn refused(kind: &str, scope: &str, reason: &str, log_end_offset: i64) -> ProduceOutcome {
    ProduceOutcome {
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(format!(
            "a write freeze on the {kind} scope {scope:?} refuses this write: {reason}"
        )),
        log_end_offset: Some(log_end_offset),
    }
}

// ── the freeze control plane ────────────────────────────────────────────────

/// Send one `SetTopicFreeze` (api key 1015) and hand back the whole response.
///
/// A refusal rides the response's own `error_code`, never a transport failure,
/// so every case reads one shape whatever the outcome.
async fn set_freeze(client: &Client, request: SetTopicFreezeRequest) -> SetTopicFreezeResponse {
    client.send(request).await.expect("SetTopicFreeze")
}

/// The unsigned freeze an operator reaches for in one command.
fn freeze_request(pattern_type: i8, scope: &str, reason: &str) -> SetTopicFreezeRequest {
    SetTopicFreezeRequest {
        scope: scope.to_owned(),
        pattern_type,
        frozen: true,
        reason: reason.to_owned(),
        ..SetTopicFreezeRequest::default()
    }
}

/// Freeze `scope`, assert the broker took it, and wait until the registry the
/// wire serves shows it.
///
/// The wait is on `DescribeTopicFreezes` rather than on a sleep. Both that
/// handler and the produce gate read the controller's current image, so a
/// registry that answers over the wire is a produce path that answers too.
async fn freeze_scope(client: &Client, pattern_type: i8, scope: &str, reason: &str) {
    let before = describe_freezes(client).await.len();
    let response = set_freeze(client, freeze_request(pattern_type, scope, reason)).await;
    assert!(
        response.error_code == codes::NONE,
        "freeze {scope}: {response:?}"
    );
    wait_for_registry_len(client, before + 1).await;
}

/// Read the whole registry through `DescribeTopicFreezes` (api key 1016).
async fn describe_freezes(client: &Client) -> Vec<DescribedTopicFreeze> {
    let response = client
        .send(DescribeTopicFreezesRequest::default())
        .await
        .expect("DescribeTopicFreezes");
    assert!(
        response.error_code == codes::NONE,
        "DescribeTopicFreezes: {response:?}"
    );
    response.freezes
}

/// Wait until the registry holds exactly `want` entries, and return them.
async fn wait_for_registry_len(client: &Client, want: usize) -> Vec<DescribedTopicFreeze> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let entries = describe_freezes(client).await;
        if entries.len() == want {
            return entries;
        }
        assert!(
            Instant::now() < deadline,
            "the freeze registry never reached {want} entries; it holds {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The cluster id, read the way `crabka-guard` reads it before it signs.
///
/// It is inside the signed bytes, which is what stops a signature made for one
/// cluster from being replayed into another.
async fn cluster_id(client: &Client) -> String {
    let response = client
        .send(DescribeClusterRequest::default())
        .await
        .expect("DescribeCluster");
    assert!(
        response.error_code == codes::NONE,
        "DescribeCluster: {response:?}"
    );
    response.cluster_id
}

/// The freeze-record fields a signature covers.
#[derive(Debug, Clone, Copy)]
struct SigningInput<'a> {
    cluster_id: &'a str,
    pattern_type: i8,
    scope: &'a str,
    frozen: bool,
    reason: &'a str,
    set_by: &'a str,
    set_at_ms: i64,
    proposal_id: [u8; 16],
}

/// Rebuild the canonical bytes that the broker verifies against.
///
/// The layout is the one `crates/broker/src/freeze/signing.rs` documents:
/// the domain separator, then `cluster_id`, `pattern_type`, `scope`, `frozen`,
/// `reason`, `set_by`, `set_at_ms` and `proposal_id`, with every variable field
/// behind a `u32` big-endian length. The length prefixes are what stop a scope
/// of `"a"` with a reason of `"bc"` from signing the same bytes as a scope of
/// `"ab"` with a reason of `"c"`.
fn signing_bytes(input: &SigningInput<'_>) -> Vec<u8> {
    fn put(bytes: &mut Vec<u8>, field: &[u8]) {
        let len = u32::try_from(field.len()).expect("a field inside u32");
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(field);
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(FREEZE_DOMAIN);
    put(&mut bytes, input.cluster_id.as_bytes());
    bytes.push(input.pattern_type.to_be_bytes()[0]);
    put(&mut bytes, input.scope.as_bytes());
    bytes.push(u8::from(input.frozen));
    put(&mut bytes, input.reason.as_bytes());
    put(&mut bytes, input.set_by.as_bytes());
    bytes.extend_from_slice(&input.set_at_ms.to_be_bytes());
    bytes.extend_from_slice(&input.proposal_id);
    bytes
}

/// Everything a signed `SetTopicFreeze` needs beyond the record itself.
struct SignedFreeze<'a> {
    key: &'a OperatorKey,
    cluster_id: &'a str,
    pattern_type: i8,
    scope: &'a str,
    frozen: bool,
    reason: &'a str,
    set_at_ms: i64,
    proposal_id: uuid::Uuid,
}

/// Sign a freeze or a thaw on the caller's own machine, exactly as
/// `crabka-guard --sign-with` does: the private key never reaches the broker,
/// and only the `key_id` and the detached signature go on the wire.
fn signed_request(signed: &SignedFreeze<'_>) -> SetTopicFreezeRequest {
    let proposal_id = *signed.proposal_id.as_bytes();
    let bytes = signing_bytes(&SigningInput {
        cluster_id: signed.cluster_id,
        pattern_type: signed.pattern_type,
        scope: signed.scope,
        frozen: signed.frozen,
        reason: signed.reason,
        set_by: &signed.key.principal,
        set_at_ms: signed.set_at_ms,
        proposal_id,
    });
    SetTopicFreezeRequest {
        scope: signed.scope.to_owned(),
        pattern_type: signed.pattern_type,
        frozen: signed.frozen,
        reason: signed.reason.to_owned(),
        proposal_id: WireUuid(proposal_id),
        set_at_ms: signed.set_at_ms,
        key_id: signed.key.key_id.clone(),
        signature: signed.key.pair().sign(&bytes).as_ref().to_vec(),
        ..SetTopicFreezeRequest::default()
    }
}

// ── the produce gate ────────────────────────────────────────────────────────

/// A literal freeze stops writes to the topic it names, and to nothing else.
///
/// This is the feature in one case. Both topics take a write before the freeze
/// lands, so the refusal afterwards is the registry entry rather than a produce
/// path that stopped working; and the control topic keeps taking writes after
/// it, so the refusal is scoped rather than global. Delete this case and
/// nothing proves that `POLICY_VIOLATION` ever reaches a producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_literal_freeze_refuses_produce_and_the_control_topic_still_accepts() {
    let p = support::start().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    check!(produce_outcome(&p.broker, &p.client, "orders", frozen).await == accepted(1));
    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "DR cutover").await;

    check!(
        produce_outcome(&p.broker, &p.client, "orders", frozen).await
            == refused("literal", "orders", "DR cutover", 1)
    );
    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(2));

    p.broker.shutdown().await;
}

/// A prefixed freeze stops writes to every topic in the namespace, and stops at
/// the namespace boundary.
///
/// The namespace scope is the half an ACL cannot express atomically, and it is
/// the reason this feature exists rather than a deny binding per topic. The
/// case runs two controls on purpose: `CONTROL` shows the produce path works at
/// all, and `tenant-b.orders` shows the prefix walk stops where the scope stops
/// rather than matching every topic once the registry is non-empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prefix_freeze_refuses_produce_to_every_topic_it_covers() {
    let p = support::start().await;
    let covered = create_topic(&p.broker, &p.client, "tenant-a.orders").await;
    let neighbour = create_topic(&p.broker, &p.client, "tenant-b.orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    check!(produce_outcome(&p.broker, &p.client, "tenant-a.orders", covered).await == accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_PREFIXED, "tenant-a.", "offboarding").await;

    check!(
        produce_outcome(&p.broker, &p.client, "tenant-a.orders", covered).await
            == refused("prefixed", "tenant-a.", "offboarding", 1)
    );
    check!(
        produce_outcome(&p.broker, &p.client, "tenant-b.orders", neighbour).await == accepted(1)
    );
    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(1));

    p.broker.shutdown().await;
}

/// A topic created after a covering prefix freeze is frozen the moment it
/// exists.
///
/// This is the disaster-recovery case the design is written for: an operator
/// freezes a namespace *before* a restore writes into it, so the resolve has to
/// run against the topic name at produce time and not against a set of names
/// materialised when the freeze landed. A cached set would let the newest topic
/// through, which is exactly the topic the restore is about to fill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_created_after_a_covering_prefix_freeze_is_frozen_on_arrival() {
    let p = support::start().await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    freeze_scope(&p.client, PATTERN_TYPE_PREFIXED, "tenant-a.", "pre-restore").await;

    let late = create_topic(&p.broker, &p.client, "tenant-a.late").await;
    check!(
        produce_outcome(&p.broker, &p.client, "tenant-a.late", late).await
            == refused("prefixed", "tenant-a.", "pre-restore", 0)
    );
    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(1));

    p.broker.shutdown().await;
}

// ── the Kafka config surface ────────────────────────────────────────────────

/// The one row an alter path answers with, in the two terms an operator reads.
type AlterOutcome = (i16, Option<String>);

/// Try to write `write.freeze` through `AlterConfigs`.
async fn alter_configs(client: &Client, topic: &str, value: Option<&str>) -> AlterOutcome {
    let response = client
        .send(AlterConfigsRequest {
            resources: vec![AlterResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configs: vec![AlterConfig {
                    name: WRITE_FREEZE.to_owned(),
                    value: value.map(ToOwned::to_owned),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        })
        .await
        .expect("AlterConfigs");
    let row = &response.responses[0];
    (row.error_code, row.error_message.clone())
}

/// Try to write `write.freeze` through `IncrementalAlterConfigs`.
async fn incremental_alter_configs(
    client: &Client,
    topic: &str,
    operation: i8,
    value: Option<&str>,
) -> AlterOutcome {
    let response = client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![IncrementalResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configs: vec![IncrementalConfig {
                    name: WRITE_FREEZE.to_owned(),
                    config_operation: operation,
                    value: value.map(ToOwned::to_owned),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs");
    let row = &response.responses[0];
    (row.error_code, row.error_message.clone())
}

/// Neither config path can set a freeze, and neither can clear one.
///
/// The freeze deliberately looks like a topic config through `DescribeConfigs`,
/// which is the whole reason the JVM tools can see it. That resemblance is also
/// the risk: whoever holds `Alter` on a topic is the producing team, and a
/// freeze has to hold against exactly that team. So this case comes at the key
/// from all four directions an operator could try — set and delete, on both
/// alter APIs — and asserts the freeze is still in force afterwards. The last
/// two assertions are the point: an ordinary topic config still alters, so the
/// four refusals are the key being refused by name and not the alter path being
/// broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neither_alter_path_can_set_or_clear_a_freeze() {
    let p = support::start().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;
    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;

    let refusal = (
        codes::INVALID_CONFIG,
        Some(
            "topic config write.freeze is controller-managed and read-only; use `crabka-guard \
             freeze set` to set it and `crabka-guard freeze clear` to clear it"
                .to_owned(),
        ),
    );

    // Setting a freeze on the topic that has none.
    check!(
        alter_configs(&p.client, CONTROL, Some("true")).await == refusal,
        "AlterConfigs set"
    );
    check!(
        incremental_alter_configs(&p.client, CONTROL, CONFIG_OP_SET, Some("true")).await == refusal,
        "IncrementalAlterConfigs set"
    );
    // Clearing the freeze on the topic that has one.
    check!(
        alter_configs(&p.client, "orders", Some("false")).await == refusal,
        "AlterConfigs clear"
    );
    check!(
        incremental_alter_configs(&p.client, "orders", CONFIG_OP_DELETE, None).await == refusal,
        "IncrementalAlterConfigs delete"
    );

    // The registry is untouched by all four, and the control topic gained none.
    check!(
        produce_outcome(&p.broker, &p.client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 0)
    );
    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(1));

    // The alter path itself still works, so the four refusals above are about
    // the key and not about the API.
    let ordinary = client_alter_retention(&p.client, CONTROL).await;
    check!(
        ordinary == (codes::NONE, None),
        "an ordinary topic config still alters"
    );

    p.broker.shutdown().await;
}

/// Alter an ordinary topic config, as the control for the four refusals.
async fn client_alter_retention(client: &Client, topic: &str) -> AlterOutcome {
    let response = client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![IncrementalResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configs: vec![IncrementalConfig {
                    name: "retention.ms".to_owned(),
                    config_operation: CONFIG_OP_SET,
                    value: Some("60000".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs");
    let row = &response.responses[0];
    (row.error_code, row.error_message.clone())
}

/// Read one topic's `write.freeze` entry through `DescribeConfigs`.
async fn write_freeze_config(client: &Client, topic: &str) -> DescribeConfigsResourceResult {
    let response = client
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configuration_keys: Some(vec![WRITE_FREEZE.to_owned()]),
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let result = &response.results[0];
    assert!(
        result.error_code == codes::NONE,
        "DescribeConfigs({topic}): {result:?}"
    );
    result
        .configs
        .iter()
        .find(|entry| entry.name == WRITE_FREEZE)
        .cloned()
        .unwrap_or_else(|| panic!("no {WRITE_FREEZE} entry for {topic}"))
}

/// `kafka-configs --describe` shows the freeze, read-only, naming the scope.
///
/// An operator who holds only the JVM tools cannot call `DescribeTopicFreezes`,
/// so this key is the whole of what they can see. The value has to name the
/// scope rather than say `true`, because the thaw is a different command
/// depending on whether one topic or a thousand-topic namespace is frozen. The
/// unfrozen control reports `false` rather than nothing, because an absent key
/// cannot be told apart from a broker that does not have the feature.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_the_freeze_read_only_and_names_the_scope() {
    let p = support::start().await;
    create_topic(&p.broker, &p.client, "tenant-a.orders").await;
    create_topic(&p.broker, &p.client, "orders").await;
    create_topic(&p.broker, &p.client, CONTROL).await;

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    freeze_scope(&p.client, PATTERN_TYPE_PREFIXED, "tenant-a.", "offboarding").await;

    for (label, topic, value, read_only) in [
        (
            "a topic frozen by its own name",
            "orders",
            "frozen:literal:orders",
            true,
        ),
        (
            "a topic frozen by a namespace",
            "tenant-a.orders",
            "frozen:prefixed:tenant-a.",
            true,
        ),
        ("a topic no freeze covers", CONTROL, "false", true),
    ] {
        let entry = write_freeze_config(&p.client, topic).await;
        check!(entry.value.as_deref() == Some(value), "{label}");
        check!(entry.read_only == read_only, "{label}");
    }

    p.broker.shutdown().await;
}

// ── everything a freeze must leave alone ────────────────────────────────────

/// [`support::start`] with the Prometheus listener bound.
///
/// The harness leaves `metrics_listen_addr` unset, and the one case that
/// scrapes `/metrics` over HTTP needs a socket to scrape.
async fn start_with_metrics() -> (BrokerHandle, Client, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("a loopback address"));
    let broker = Broker::start(config).await.expect("broker start");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("crabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client, tempdir)
}

/// Scrape the `OpenMetrics` body from the broker's `/metrics` endpoint.
async fn scrape(addr: std::net::SocketAddr) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the scrape request");
    stream.flush().await.expect("flush");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read the body");
    let text = String::from_utf8(buf).expect("a UTF-8 body");
    let body = text.find("\r\n\r\n").map_or(0, |i| i + 4);
    text[body..].to_owned()
}

/// The number of records a fetch from offset zero returns.
async fn fetch_record_count(client: &Client, topic: &str, topic_id: WireUuid) -> usize {
    let response = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Fetch");
    assert!(response.error_code == codes::NONE, "Fetch: {response:?}");
    let partition = &response.responses[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "Fetch partition: {partition:?}"
    );
    partition
        .records
        .as_ref()
        .and_then(crabka_protocol::records::RecordsPayload::as_v2)
        .map_or(0, |batches| {
            batches.iter().map(|batch| batch.records.len()).sum()
        })
}

/// A frozen topic stays readable, stays visible, and stays observable.
///
/// "The cluster is up, every read works, and the broker must not accept a new
/// write" is the state this feature exists to give. A freeze that also broke
/// reads would be a deny ACL with extra steps, so the read paths are asserted
/// rather than assumed. The metrics half closes the gap KFC-7's suite found
/// late: both counters were declared, registered and documented, and a live
/// broker scraped zero for them, because nothing on a real request moved them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_metadata_and_the_metrics_endpoint_still_answer_for_a_frozen_topic() {
    let (broker, client, _dir) = start_with_metrics().await;
    let metrics_addr = broker
        .metrics_addr()
        .expect("the metrics listener is bound");
    let frozen = create_topic(&broker, &client, "orders").await;
    let control = create_topic(&broker, &client, CONTROL).await;
    check!(produce_outcome(&broker, &client, "orders", frozen).await == accepted(1));

    freeze_scope(&client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );

    // The record written before the freeze is still readable, and the topic is
    // still in the metadata a client routes on.
    check!(fetch_record_count(&client, "orders", frozen).await == 1);
    let metadata = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("orders".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    let topic = &metadata.topics[0];
    check!(topic.error_code == codes::NONE, "Metadata: {topic:?}");
    check!(topic.partitions.len() == 1);

    broker
        .wait_for_metrics("topic_freezes_active reaches 1", |m| {
            m.topic_freezes_active.get() == 1
        })
        .await;
    let body = scrape(metrics_addr).await;
    for needle in [
        "crabka_broker_topic_freezes_active 1",
        "crabka_broker_topic_freeze_rejections_total{topic=\"orders\"} 1",
    ] {
        check!(body.contains(needle), "missing {needle} in:\n{body}");
    }

    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

/// A consumer of a frozen topic can still record where it got to.
///
/// `OffsetCommit` appends to `__consumer_offsets` and not to the frozen topic,
/// and a cutover is exactly when the reader positions matter most: the whole
/// point of freezing rather than deleting is that consumers drain the frozen
/// prefix and commit as they go. A freeze that stopped the commits would strand
/// every group at its last pre-freeze position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_commit_still_works_against_a_frozen_topic() {
    let p = support::start().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;
    check!(produce_outcome(&p.broker, &p.client, "orders", frozen).await == accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&p.broker, &p.client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );

    for (label, topic, topic_id) in [
        ("the frozen topic", "orders", frozen),
        ("the control topic", CONTROL, control),
    ] {
        let response = p
            .client
            .send(OffsetCommitRequest {
                group_id: "drainers".into(),
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                topics: vec![OffsetCommitRequestTopic {
                    name: topic.into(),
                    topic_id,
                    partitions: vec![OffsetCommitRequestPartition {
                        partition_index: 0,
                        committed_offset: 1,
                        committed_leader_epoch: -1,
                        committed_metadata: Some(String::new()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("OffsetCommit");
        check!(
            response.topics[0].partitions[0].error_code == codes::NONE,
            "{label}: {response:?}"
        );
    }

    p.broker.shutdown().await;
}

// ── the paths that remove data ──────────────────────────────────────────────

/// Trim `topic` up to `offset`, and hand back the partition row's error code.
async fn delete_records(client: &Client, topic: &str, offset: i64) -> i16 {
    let response = client
        .send(DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: topic.into(),
                partitions: vec![DeleteRecordsPartition {
                    partition_index: 0,
                    offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteRecords");
    response.topics[0].partitions[0].error_code
}

/// Delete `topic`, and hand back the topic row's error code.
async fn delete_topic(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic.to_owned()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    response.responses[0].error_code
}

/// A freeze refuses both paths that take data out of the topic.
///
/// A freeze that stopped new writes and let a trim through would be worse than
/// no freeze at all: an operator who froze a topic for a disaster-recovery
/// promotion needs the frozen prefix byte-identical between sites, and both of
/// these paths destroy exactly that. The break-glass workflow is deliberately
/// off on this broker, so nothing but the freeze can produce the refusal, and
/// the control topic proves both paths still work when nothing is frozen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_and_delete_topics_refuse_on_a_frozen_topic() {
    let p = support::start().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;
    for _ in 0..2 {
        check!(produce(&p.client, "orders", frozen).await.error_code == codes::NONE);
        check!(produce(&p.client, CONTROL, control).await.error_code == codes::NONE);
    }

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "promotion").await;

    check!(delete_records(&p.client, "orders", 1).await == codes::POLICY_VIOLATION);
    check!(p.broker.partition_log_start_for_test("orders", 0) == Some(0));
    check!(p.broker.local_log_end_offset("orders", 0) == Some(2));

    check!(delete_topic(&p.client, "orders").await == codes::POLICY_VIOLATION);
    check!(p.broker.partition_exists_for_test("orders", 0));

    // The control topic takes both, so the two refusals above are the freeze
    // and not a broker that refuses every deletion.
    check!(delete_records(&p.client, CONTROL, 1).await == codes::NONE);
    check!(delete_topic(&p.client, CONTROL).await == codes::NONE);

    p.broker.shutdown().await;
}

// ── durability ──────────────────────────────────────────────────────────────

/// A freeze survives a controller restart.
///
/// The registry lives in the metadata log rather than in memory precisely so
/// that a restart cannot thaw a cluster silently. This case restarts the only
/// controller in the cluster and asserts the refusal is still there. A registry
/// that lived anywhere else would pass every other case in this file and fail
/// this one, in the worst possible way: the topic would start accepting writes
/// during the incident that the freeze was declared for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_freeze_survives_a_controller_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        let frozen = create_topic(&broker, &client, "orders").await;
        create_topic(&broker, &client, CONTROL).await;
        check!(produce_outcome(&broker, &client, "orders", frozen).await == accepted(1));

        freeze_scope(&client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
        check!(
            produce_outcome(&broker, &client, "orders", frozen).await
                == refused("literal", "orders", "cutover", 1)
        );
        broker.shutdown().await;
    }

    let (broker, client) = support::start_with_dir(dir.path()).await;
    for topic in ["orders", CONTROL] {
        broker.wait_until_partition_present(topic, 0).await;
        broker
            .wait_until_local_partition_leader(topic, 0, crabka_broker::NodeId(broker.node_id()))
            .await;
    }

    let entries = wait_for_registry_len(&client, 1).await;
    check!(entries[0].scope == "orders");
    check!(entries[0].pattern_type == PATTERN_TYPE_LITERAL);

    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );
    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));

    broker.shutdown().await;
}

// ── the operator signature, end to end ──────────────────────────────────────

/// [`support::start_with_operator_key`] with `freeze.require_signature` on.
///
/// The harness helper takes the default, and a running broker's configuration
/// is not mutable, so the cases that need the strict setting build it here.
/// Everything else is what the harness builds: the same trust set, the same
/// single-approver break-glass set, the same plaintext listener.
async fn start_requiring_signatures(dir: &Path, key: &OperatorKey) -> (BrokerHandle, Client) {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = crabka_broker::operator_keys::OperatorKeys::load(&[key.entry()])
        .expect("load the operator trust set");
    config.break_glass.approvers = vec![support::ANONYMOUS.to_owned()];
    config.freeze.require_signature = true;
    let broker = Broker::start(config).await.expect("broker start");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("crabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client)
}

/// Re-verify a registry entry's signature the way `freeze list
/// --verify-signatures` does: on the reader's own machine, against the operator
/// public key, with no trust in the broker that served it.
fn verifies_locally(key: &OperatorKey, cluster_id: &str, entry: &DescribedTopicFreeze) -> bool {
    let public = std::fs::read(&key.public_path).expect("read the operator public key");
    let bytes = signing_bytes(&SigningInput {
        cluster_id,
        pattern_type: entry.pattern_type,
        scope: &entry.scope,
        frozen: true,
        reason: &entry.reason,
        set_by: &entry.set_by,
        set_at_ms: entry.set_at_ms,
        proposal_id: entry.proposal_id.0,
    });
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public)
        .verify(&bytes, &entry.signature)
        .is_ok()
}

/// `freeze.require_signature` is what decides whether the broker takes an
/// operator's word for a freeze.
///
/// The asymmetry it controls is deliberate and easy to lose: a freeze is the
/// safe direction, and an operator has to reach it in one command on a cluster
/// where nobody installed key material yet. That leaves a registry that can
/// hold proved entries beside attested ones, and this setting is the only thing
/// that removes the mixture. The refused arm carries its own control: the
/// topic never froze, so it keeps taking writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn require_signature_decides_whether_an_unsigned_freeze_is_accepted() {
    for (label, require_signature, code, after) in [
        (
            "the default takes the operator's attestation",
            false,
            codes::NONE,
            refused("literal", "orders", "incident", 1),
        ),
        (
            "require_signature demands the proof",
            true,
            codes::OPERATOR_SIGNATURE_REQUIRED,
            accepted(2),
        ),
    ] {
        let keys = tempfile::tempdir().expect("tempdir");
        let logs = tempfile::tempdir().expect("tempdir");
        let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
        let (broker, client) = if require_signature {
            start_requiring_signatures(logs.path(), &key).await
        } else {
            let (broker, client, _config) =
                support::start_with_operator_key(logs.path(), &key).await;
            (broker, client)
        };
        let frozen = create_topic(&broker, &client, "orders").await;
        let control = create_topic(&broker, &client, CONTROL).await;
        check!(
            produce_outcome(&broker, &client, "orders", frozen).await == accepted(1),
            "{label}"
        );

        let response = set_freeze(
            &client,
            freeze_request(PATTERN_TYPE_LITERAL, "orders", "incident"),
        )
        .await;
        check!(response.error_code == code, "{label}: {response:?}");
        wait_for_registry_len(&client, usize::from(code == codes::NONE)).await;

        check!(
            produce_outcome(&broker, &client, "orders", frozen).await == after,
            "{label}"
        );
        check!(
            produce_outcome(&broker, &client, CONTROL, control).await == accepted(1),
            "{label}"
        );
        broker.shutdown().await;
    }
}

/// A signed freeze reaches the registry with its `key_id` and its signature
/// intact, and the signature verifies away from the broker.
///
/// The signature is the only part of a freeze record that the broker cannot
/// forge, so it is the only part that proves who set it. That proof is worth
/// nothing unless the exact bytes the operator signed come back out of
/// `DescribeTopicFreezes`, which is why the whole entry is compared rather than
/// a field at a time: a broker that dropped the signature, re-stamped
/// `set_at_ms`, or rewrote `set_by` would still answer every other case here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_freeze_round_trips_with_its_key_id_and_signature_intact() {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
    let (broker, client, _config) = support::start_with_operator_key(logs.path(), &key).await;
    let frozen = create_topic(&broker, &client, "orders").await;
    let control = create_topic(&broker, &client, CONTROL).await;

    let cluster = cluster_id(&client).await;
    let set_at_ms = now_ms();
    let request = signed_request(&SignedFreeze {
        key: &key,
        cluster_id: &cluster,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: "orders",
        frozen: true,
        reason: "incident",
        set_at_ms,
        proposal_id: uuid::Uuid::nil(),
    });
    let signature = request.signature.clone();
    let response = set_freeze(&client, request).await;
    check!(
        response.error_code == codes::NONE,
        "signed freeze: {response:?}"
    );

    let entries = wait_for_registry_len(&client, 1).await;
    check!(
        entries[0]
            == DescribedTopicFreeze {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                reason: "incident".to_owned(),
                set_by: support::ANONYMOUS.to_owned(),
                set_at_ms,
                proposal_id: WireUuid::ZERO,
                key_id: key.key_id.clone(),
                signature,
                ..DescribedTopicFreeze::default()
            }
    );
    check!(verifies_locally(&key, &cluster, &entries[0]));

    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "incident", 0)
    );
    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

/// An unsigned thaw is refused whatever `freeze.require_signature` says.
///
/// This is the half of the asymmetry that carries the security of the whole
/// feature. A freeze that one unsigned command can lift is exactly as strong as
/// the one credential that sends it, and when the incident is a compromise the
/// attacker already holds that credential. `require_signature` is about the
/// freeze direction alone, so it is asserted with the setting both on and off:
/// a thaw that started depending on it would look correct in the strict
/// configuration and be wide open in the default one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsigned_thaw_is_refused_whatever_require_signature_says() {
    for (label, require_signature) in [
        ("with require_signature off", false),
        ("with require_signature on", true),
    ] {
        let keys = tempfile::tempdir().expect("tempdir");
        let logs = tempfile::tempdir().expect("tempdir");
        let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
        let (broker, client) = if require_signature {
            start_requiring_signatures(logs.path(), &key).await
        } else {
            let (broker, client, _config) =
                support::start_with_operator_key(logs.path(), &key).await;
            (broker, client)
        };
        let frozen = create_topic(&broker, &client, "orders").await;
        let control = create_topic(&broker, &client, CONTROL).await;

        let cluster = cluster_id(&client).await;
        let response = set_freeze(
            &client,
            signed_request(&SignedFreeze {
                key: &key,
                cluster_id: &cluster,
                pattern_type: PATTERN_TYPE_LITERAL,
                scope: "orders",
                frozen: true,
                reason: "incident",
                set_at_ms: now_ms(),
                proposal_id: uuid::Uuid::nil(),
            }),
        )
        .await;
        check!(response.error_code == codes::NONE, "{label}: {response:?}");
        wait_for_registry_len(&client, 1).await;

        let thaw = set_freeze(
            &client,
            SetTopicFreezeRequest {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                frozen: false,
                reason: "let me back in".to_owned(),
                proposal_id: WireUuid(*uuid::Uuid::new_v4().as_bytes()),
                set_at_ms: now_ms(),
                ..SetTopicFreezeRequest::default()
            },
        )
        .await;
        check!(
            thaw.error_code == codes::OPERATOR_SIGNATURE_REQUIRED,
            "{label}: {thaw:?}"
        );

        check!(
            wait_for_registry_len(&client, 1).await[0].scope == "orders",
            "{label}"
        );
        check!(
            produce_outcome(&broker, &client, "orders", frozen).await
                == refused("literal", "orders", "incident", 0),
            "{label}"
        );
        check!(
            produce_outcome(&broker, &client, CONTROL, control).await == accepted(1),
            "{label}"
        );
        broker.shutdown().await;
    }
}

/// A signature captured from a freeze cannot be replayed as the thaw.
///
/// `frozen` and `set_at_ms` are both inside the signed bytes for this attack
/// and no other. Drop `frozen` from the payload and the freeze record and the
/// thaw record differ by one byte that nothing covers, so one captured
/// signature would authorize both directions -- and the direction an attacker
/// wants is the one that lifts the freeze. Both replays are asserted only on
/// the error code, because all six signature checks answer
/// `OPERATOR_SIGNATURE_INVALID` on purpose: a code that separated them would
/// tell an attacker which check they got past.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_captured_from_a_freeze_is_refused_as_a_thaw() {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
    let (broker, client, _config) = support::start_with_operator_key(logs.path(), &key).await;
    let frozen = create_topic(&broker, &client, "orders").await;
    let control = create_topic(&broker, &client, CONTROL).await;

    let cluster = cluster_id(&client).await;
    let set_at_ms = now_ms();
    let freeze = signed_request(&SignedFreeze {
        key: &key,
        cluster_id: &cluster,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: "orders",
        frozen: true,
        reason: "incident",
        set_at_ms,
        proposal_id: uuid::Uuid::nil(),
    });
    let captured = freeze.signature.clone();
    check!(set_freeze(&client, freeze).await.error_code == codes::NONE);
    wait_for_registry_len(&client, 1).await;

    // The two shapes the capture can take: the record the attacker holds, with
    // only `frozen` flipped, and the same signature carried forward onto a
    // fresh timestamp so the "newer than the entry it replaces" rule cannot be
    // what refuses it.
    for (label, replay_at_ms) in [
        ("the otherwise identical thaw", set_at_ms),
        ("the same signature on a fresh timestamp", now_ms() + 1_000),
    ] {
        let thaw = set_freeze(
            &client,
            SetTopicFreezeRequest {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                frozen: false,
                reason: "incident".to_owned(),
                proposal_id: WireUuid::ZERO,
                set_at_ms: replay_at_ms,
                key_id: key.key_id.clone(),
                signature: captured.clone(),
                ..SetTopicFreezeRequest::default()
            },
        )
        .await;
        check!(
            thaw.error_code == codes::OPERATOR_SIGNATURE_INVALID,
            "{label}: {thaw:?}"
        );
    }

    check!(wait_for_registry_len(&client, 1).await[0].scope == "orders");
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "incident", 0)
    );
    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

/// A signature survives a controller restart and still verifies from the
/// reloaded image.
///
/// This is the durability claim the design makes about the proof rather than
/// about the state: an auditor holding the operator public keys can say who
/// froze a topic, from a broker that was not running when they signed. A broker
/// that kept the registry across a restart but dropped the signature would pass
/// every other durability case and quietly turn every proved entry into an
/// attested one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_survives_a_controller_restart_and_still_verifies() {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
    let (broker, client, mut config) = support::start_with_operator_key(logs.path(), &key).await;
    create_topic(&broker, &client, "orders").await;
    create_topic(&broker, &client, CONTROL).await;

    let cluster = cluster_id(&client).await;
    let response = set_freeze(
        &client,
        signed_request(&SignedFreeze {
            key: &key,
            cluster_id: &cluster,
            pattern_type: PATTERN_TYPE_LITERAL,
            scope: "orders",
            frozen: true,
            reason: "incident",
            set_at_ms: now_ms(),
            proposal_id: uuid::Uuid::nil(),
        }),
    )
    .await;
    check!(
        response.error_code == codes::NONE,
        "signed freeze: {response:?}"
    );
    let before = wait_for_registry_len(&client, 1).await;
    drop(client);
    broker.shutdown().await;

    // The harness cannot infer the mode on a second boot, and a node that
    // re-bootstraps comes back with an empty registry -- which would make this
    // case fail for a reason that has nothing to do with the signature.
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = support::start_reusing_addrs(&config, "the signed-freeze restart").await;
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("crabka-broker-test")
        .build()
        .await
        .expect("client build");
    for topic in ["orders", CONTROL] {
        broker.wait_until_partition_present(topic, 0).await;
        broker
            .wait_until_local_partition_leader(topic, 0, crabka_broker::NodeId(broker.node_id()))
            .await;
    }

    let after = wait_for_registry_len(&client, 1).await;
    check!(after == before);
    check!(cluster_id(&client).await == cluster);
    check!(verifies_locally(&key, &cluster, &after[0]));

    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "incident", 0)
    );
    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

// ── the thaw, which takes two people ────────────────────────────────────────

/// Open a break-glass proposal to thaw `target`, and return its id.
async fn propose_thaw(client: &Client, target: &str) -> uuid::Uuid {
    let response = client
        .send(ProposeBreakGlassRequest {
            action: ACTION_THAW,
            target: target.to_owned(),
            reason: "the cutover finished".to_owned(),
            // Zero asks for `break_glass.proposal_ttl`.
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "ProposeBreakGlass: {response:?}"
    );
    uuid::Uuid::from_bytes(response.proposal_id.0)
}

/// Add one approval, and return how many the proposal now holds.
async fn approve(client: &Client, proposal_id: uuid::Uuid) -> i32 {
    let response = client
        .send(ApproveBreakGlassRequest {
            proposal_id: WireUuid(*proposal_id.as_bytes()),
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "ApproveBreakGlass: {response:?}"
    );
    response.approvals_held
}

/// A thaw lifts the freeze and the topic takes writes again.
///
/// The reverse direction has to be proved as its own case, because a freeze
/// that could not be lifted would be a broken cluster rather than a safe one,
/// and none of the refusal cases above can tell a working thaw from a registry
/// entry that nothing removes. It runs over SASL because the two-person rule
/// needs three distinct principals -- a proposer who may not approve, and two
/// approvers -- and every connection on a plaintext listener is the same
/// anonymous principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thaw_restores_writes() {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    let key = support::mint_operator_key(keys.path(), "alice-yubi", "User:alice");
    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl(
        logs.path(),
        &[&key],
        // The proposer has to be in the set as well: a proposer outside it
        // would turn a rule about three people into a rule about two people
        // and a stranger. Alice still may not approve her own proposal.
        &["User:alice", "User:bob", "User:carol"],
        &[("alice", "pw"), ("bob", "pw"), ("carol", "pw")],
    )
    .await;
    let alice = support::sasl_client(&bootstrap, "alice", "pw").await;
    let frozen = create_topic(&broker, &alice, "orders").await;
    let control = create_topic(&broker, &alice, CONTROL).await;
    check!(produce_outcome(&broker, &alice, "orders", frozen).await == accepted(1));

    freeze_scope(&alice, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&broker, &alice, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );

    // Alice proposes and may not approve her own proposal, so the two
    // approvals come from two other people.
    let proposal_id = propose_thaw(&alice, "literal:orders").await;
    let bob = support::sasl_client(&bootstrap, "bob", "pw").await;
    let carol = support::sasl_client(&bootstrap, "carol", "pw").await;
    check!(approve(&bob, proposal_id).await == 1);
    check!(approve(&carol, proposal_id).await == 2);

    let thaw = set_freeze(
        &alice,
        signed_request(&SignedFreeze {
            key: &key,
            cluster_id: &cluster_id(&alice).await,
            pattern_type: PATTERN_TYPE_LITERAL,
            scope: "orders",
            frozen: false,
            reason: "the cutover finished",
            set_at_ms: now_ms(),
            proposal_id,
        }),
    )
    .await;
    check!(thaw.error_code == codes::NONE, "thaw: {thaw:?}");
    wait_for_registry_len(&alice, 0).await;

    check!(produce_outcome(&broker, &alice, "orders", frozen).await == accepted(2));
    check!(produce_outcome(&broker, &alice, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

// ── the transaction that was already in flight ──────────────────────────────

/// The `read_committed` last stable offset of one partition.
///
/// It is read from a `read_committed` Fetch rather than from `ListOffsets`,
/// because this broker's `ListOffsets` answers the high watermark whatever the
/// request's `isolation_level` says. The Fetch response carries the last stable
/// offset as its own field, so it reports the value an open transaction pins.
async fn stable_offset(client: &Client, topic: &str, topic_id: WireUuid) -> i64 {
    let response = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            // 1 is `read_committed`, the isolation level an open transaction
            // holds back.
            isolation_level: 1,
            topics: vec![FetchTopic {
                topic: topic.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Fetch");
    assert!(response.error_code == codes::NONE, "Fetch: {response:?}");
    let partition = &response.responses[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "Fetch({topic}): {partition:?}"
    );
    partition.last_stable_offset
}

/// A transaction that enlisted the partition before the freeze still commits,
/// and the `read_committed` last stable offset advances past its marker.
///
/// This is the sharp edge of the rule, and the one place where a freeze
/// deliberately lets an append through. The commit decision is already durable
/// in `__transaction_state` by the time the marker is written, so refusing the
/// marker would not undo the transaction -- it would leave it permanently open,
/// which pins the last stable offset and stops every `read_committed` consumer
/// of the partition. A freeze exists to keep a topic readable while it is not
/// writable, so a freeze that pinned the LSO forever would break the half of
/// the feature it was meant to keep.
///
/// The case asserts the LSO on both sides of the commit rather than only after
/// it. A broker that had never pinned the LSO at all would pass a
/// one-sided assertion while proving nothing about the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_that_enlisted_before_the_freeze_still_commits() {
    let p = support::start().await;
    let bootstrap = p.broker.listen_addr().to_string();
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .transactional_id("cutover-tid")
        .build()
        .await
        .expect("producer build");
    producer
        .init_transactions()
        .await
        .expect("init_transactions");
    let txn = producer
        .begin_transaction()
        .await
        .expect("begin_transaction");
    producer
        .send(ProducerRecord {
            topic: "orders".into(),
            value: Some(Bytes::from_static(b"in-flight")),
            ..Default::default()
        })
        .await
        .await
        .expect("producer delivery channel open")
        .expect("the in-flight record is acknowledged");
    p.broker
        .wait_until_local_log_end_offset("orders", 0, 1)
        .await;
    // The transaction is open, so `read_committed` cannot see past its first
    // record yet.
    check!(stable_offset(&p.client, "orders", frozen).await == 0);

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;

    // The freeze is live while the transaction is open: a new plain write is
    // refused, and it does not move the log.
    check!(
        produce_outcome(&p.broker, &p.client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );

    txn.commit()
        .await
        .expect("a transaction that enlisted before the freeze still commits");

    // The marker was appended, so the log grew by one and `read_committed`
    // reached the end of it. A freeze that refused the marker would leave both
    // of these at their pre-commit values forever.
    p.broker
        .wait_until_local_log_end_offset("orders", 0, 2)
        .await;
    check!(stable_offset(&p.client, "orders", frozen).await == 2);

    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(1));
    producer.close().await.expect("producer close");
    p.broker.shutdown().await;
}
