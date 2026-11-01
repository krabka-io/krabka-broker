//! The share-group dead-letter queue (KIP-1191), end to end.
//!
//! At `share.version` 2, a record that a member rejects is written to the
//! group's dead-letter topic before the share partition archives it. The
//! dead-letter record carries six headers that say where the record came from
//! and why it is there, and the key and value of the source record when the
//! group copies them. Kafka archives the record whatever becomes of the write,
//! so a group whose topic cannot be written still moves on.
//!
//! The queue is Kafka trunk's, so each broker here runs with
//! `unstable.api.versions.enable` and `unstable.feature.versions.enable` on.

use {
    crate::{
        NONE, REJECT,
        harness::{broker_config, broker_test_permit, produce_n, produce_values, topic_id, wire},
        share_rpc::{acquired_count, fetch_until_acquired, share_fetch},
        support::{
            configs::{
                ConfigOperation, feature_update, incremental_config, incremental_request,
                incremental_resource,
            },
            fetch::{FetchLimits, RequestWaitMillis, single_partition_fetch},
            offsets::{list_offset_partition, single_partition_list_offsets},
            share::{
                MemberSession, ShareAck, ShareFetchSetup, ShareRecordCount, ShareSessionEpoch,
                ShareSessionSetup, StartedShareTopic, acknowledge_partition, acknowledge_request,
                acknowledge_success, acknowledge_topic, join_consume_member, start_group_topic,
            },
            topics::{self, topic_configs},
        },
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{
        BrokerConfig, BrokerHandle, RemoteStorageBackend,
        api_catalog::UnstableApiVersions,
        metrics::{BrokerMetrics, ShareGroupIdLabel},
    },
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            create_topics_request::CreateTopicsRequest, list_offsets_request::ListOffsetsRequest,
            share_acknowledge_request::AcknowledgementBatch, share_fetch_response::PartitionData,
            update_features_request::UpdateFeaturesRequest,
        },
        records::{Record, RecordBatch, RecordsPayload},
    },
    std::{iter, sync::Arc, time::Duration},
    tokio::time,
};

const GROUP: &str = "g1";
const DLQ_TOPIC: &str = "dlq.g1";

/// A broker that serves what krabka implements from Kafka trunk, over a fresh
/// data directory.
fn trunk_config(dir: &tempfile::TempDir) -> BrokerConfig {
    let mut config = broker_config(dir.path().to_path_buf());
    config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    config
}

/// Finalizes `share.version` at 2, the level of the dead-letter queue.
async fn finalize_share_version_two(client: &Client) {
    let response = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update("share.version", 2, 1)],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(response.error_code == 0, "{response:?}");
}

/// One config name and the value it is set to.
#[derive(Clone, Copy, Debug)]
struct ConfigEntry<'a> {
    name: &'a str,
    value: &'a str,
}

/// Creates the dead-letter topic with the topic config that opts it in.
async fn create_dead_letter_topic(broker: &BrokerHandle, client: &Client) {
    create_dead_letter_topic_with(broker, client, &[]).await;
}

/// [`create_dead_letter_topic`], with `extra` topic configs beside the one that
/// opts the topic in.
async fn create_dead_letter_topic_with(
    broker: &BrokerHandle,
    client: &Client,
    extra: &[ConfigEntry<'_>],
) {
    let configs = topic_configs(
        iter::once(("errors.deadletterqueue.group.enable", "true")).chain(
            extra
                .iter()
                .map(|ConfigEntry { name, value }| (*name, *value)),
        ),
    );
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![
                topics::creatable_topic_with_configs()
                    .name(DLQ_TOPIC.into())
                    .configs(configs)
                    .call(),
            ],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == 0, "{response:?}");
    broker.wait_until_partition_present(DLQ_TOPIC, 0).await;
}

#[derive(Clone, Copy, Default)]
enum DeadLetterConfigTarget {
    #[default]
    Group,
    Broker,
    Topic,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DeadLetterConfigSetup<'a> {
    target: DeadLetterConfigTarget,
    #[default(GROUP)]
    name: &'a str,
    #[default(&[])]
    values: &'a [ConfigEntry<'a>],
}

/// Sets `configs` on the resource of `resource_type` and `resource_name`.
#[bon::builder]
async fn set_configs(
    client: &Client,
    #[builder(default)] target: DeadLetterConfigTarget,
    #[builder(default = GROUP)] name: &'_ str,
    #[builder(default = & [])] values: &'_ [ConfigEntry<'_>],
) {
    let setup = DeadLetterConfigSetup {
        target,
        name,
        values,
    };
    let response = client
        .send(
            incremental_request()
                .resources(vec![incremental_resource(
                    match setup.target {
                        DeadLetterConfigTarget::Group => 32,
                        DeadLetterConfigTarget::Broker => 4,
                        DeadLetterConfigTarget::Topic => 2,
                    },
                    setup.name,
                    setup
                        .values
                        .iter()
                        .map(|ConfigEntry { name, value }| {
                            incremental_config(*name, Some((*value).into()), ConfigOperation::Set)
                        })
                        .collect(),
                )])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs");
    assert!(
        response.responses[0].error_code == 0,
        "{:?} rejected: {:?}",
        setup.values,
        response.responses[0].error_message
    );
}

/// Gives the group its dead-letter topic, and asks it to copy the records.
async fn point_group_at_dead_letter_topic(client: &Client) {
    set_configs()
        .client(client)
        .values(&[
            ConfigEntry {
                name: "errors.deadletterqueue.topic.name",
                value: DLQ_TOPIC,
            },
            ConfigEntry {
                name: "errors.deadletterqueue.copy.record.enable",
                value: "true",
            },
        ])
        .call()
        .await;
}

/// The records of the first batch of the dead-letter topic, once there is one.
async fn dead_letter_records(broker: &BrokerHandle, client: &Client) -> Vec<Record> {
    let dlq_id = topic_id(broker, DLQ_TOPIC);
    let response = client
        .send(
            single_partition_fetch()
                .topic(DLQ_TOPIC.into())
                .topic_id(wire(dlq_id))
                .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                .call(),
        )
        .await
        .expect("Fetch");
    records_of(response.responses[0].partitions[0].records.as_ref())
}

/// The records of a response row, decoded or not.
fn records_of(payload: Option<&RecordsPayload>) -> Vec<Record> {
    match payload {
        Some(RecordsPayload::V2(batches)) => batches
            .iter()
            .flat_map(|batch| batch.records.clone())
            .collect(),
        Some(RecordsPayload::Raw(bytes)) => {
            let mut cursor = bytes.clone();
            let mut records = Vec::new();
            while !cursor.is_empty() {
                let batch = RecordBatch::decode_batch(&mut cursor).expect("decode a record batch");
                records.extend(batch.records);
            }
            records
        }
        _ => Vec::new(),
    }
}

/// Polls the dead-letter topic until it holds `count` records.
async fn wait_for_dead_letters(
    broker: &BrokerHandle,
    client: &Client,
    count: ShareRecordCount,
) -> Vec<Record> {
    let expected = usize::try_from(count.0).expect("dead-letter count fits usize");
    time::timeout(Duration::from_secs(60), async {
        loop {
            let records = dead_letter_records(broker, client).await;
            if records.len() >= expected {
                return records;
            }
            // intentional: bounded poll of a topic that a background task writes;
            // nothing signals the append to the test.
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the dead-letter records reached the topic")
}

// A header of `record` as text.
krabka_macros::record_header_text!(header, lossy);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RejectRangesSetup<'a> {
    topic_id: uuid::Uuid,
    #[default(ShareSessionEpoch(1))]
    epoch: ShareSessionEpoch,
    #[default(&[])]
    ranges: &'a [RejectedRange],
}

/// An inclusive range of offsets to reject.
#[derive(Clone, Copy)]
struct RejectedRange {
    first: krabka_ids::Offset,
    last: krabka_ids::Offset,
}

/// One `ShareAcknowledge` that rejects each of `ranges`, as the first and last
/// offsets of partition 0, in a batch of its own.
#[bon::builder]
async fn reject_ranges(
    client: &Client,
    member: &str,
    #[builder(default)] topic_id: uuid::Uuid,
    #[builder(into, default = ShareSessionEpoch(1))] epoch: ShareSessionEpoch,
    #[builder(default = & [])] ranges: &'_ [RejectedRange],
) {
    let setup = RejectRangesSetup {
        topic_id,
        epoch,
        ranges,
    };
    let response = client
        .send(
            acknowledge_request()
                .maybe_group_id(Some(Some(GROUP.into())))
                .maybe_member_id(Some(Some(member.into())))
                .epoch(setup.epoch)
                .topics(vec![acknowledge_topic(wire(setup.topic_id), vec![
                    acknowledge_partition(
                        0,
                        setup
                            .ranges
                            .iter()
                            .map(|RejectedRange { first, last }| AcknowledgementBatch {
                                first_offset: first.0,
                                last_offset: last.0,
                                acknowledge_types: vec![
                                    REJECT;
                                    usize::try_from(last.0 - first.0 + 1)
                                        .unwrap()
                                ],
                                ..Default::default()
                            })
                            .collect(),
                    ),
                ])])
                .call(),
        )
        .await
        .expect("ShareAcknowledge");
    assert!(response.error_code == NONE, "{response:?}");
    assert!(
        response.responses[0].partitions[0].error_code == NONE,
        "{response:?}"
    );
}

krabka_macros::share_dlq_meters!(
    /// What the `DeadLetterQueue*` meters of the group count on the broker: the
    /// records written, the attempts to produce, and the writes that failed.
    meters,
    BrokerMetrics,
    ShareGroupIdLabel
);

/// Starts a trunk broker with a source topic `t` of `records` records and a
/// member that holds all of them acquired, then returns what a test drives.
struct Cluster {
    broker: BrokerHandle,
    client: Arc<Client>,
    tid: uuid::Uuid,
    member: String,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn reject_acquired(&self, first: krabka_ids::Offset, last: krabka_ids::Offset) {
        acknowledge_success(
            &self.client,
            ShareAck::prefix_for(&self.member, self.tid, last)
                .starting_at(first)
                .reject(),
        )
        .await;
    }
}

#[derive(Clone, Copy, Default)]
enum DeadLetterShareVersion {
    One,
    #[default]
    Two,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DeadLetterQueueSetup<'a> {
    #[default(ShareRecordCount(3))]
    records: ShareRecordCount,
    version: DeadLetterShareVersion,
    #[default(&[])]
    topic_configs: &'a [ConfigEntry<'a>],
}

/// Keep the feature gate and topic/group configuration in their original order.
async fn configure_dead_letters(
    client: &Client,
    broker: &BrokerHandle,
    version: DeadLetterShareVersion,
    topic_configs: &[ConfigEntry<'_>],
) {
    if matches!(version, DeadLetterShareVersion::Two) {
        finalize_share_version_two(client).await;
    }
    create_dead_letter_topic_with(broker, client, topic_configs).await;
    point_group_at_dead_letter_topic(client).await;
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct AcquiredRecordsSetup {
    #[default(ShareRecordCount(3))]
    records: ShareRecordCount,
}

/// Required script steps for a custom acquisition, separate from its defaulted record setup.
struct AcquisitionHooks<T, C, P> {
    tweak: T,
    configure: C,
    produce: P,
}

async fn acquired_records(
    records: ShareRecordCount,
    tweak: impl FnOnce(&mut BrokerConfig),
    configure: impl AsyncFnOnce(&Client, &BrokerHandle),
) -> Cluster {
    acquired_records_from()
        .records(records)
        .hooks(AcquisitionHooks {
            tweak,
            configure,
            produce: async |client: &Client, tid| {
                produce_n()
                    .client(client)
                    .topic_id(tid)
                    .count(records.0)
                    .call()
                    .await;
            },
        })
        .call()
        .await
}

/// A joined member and the row of its first acquiring fetch.
struct AcquiredMember {
    member: String,
    row: PartitionData,
}

/// [`acquired_records`], with the records that `produce` writes to `t` in place
/// of the small ones that `produce_n` writes.
async fn join_and_acquire(
    broker: &BrokerHandle,
    client: &Client,
    tid: uuid::Uuid,
) -> AcquiredMember {
    let MemberSession { member, .. } = join_consume_member(broker, client, tid).await;
    let row = fetch_until_acquired(client, ShareSessionSetup {
        group: GROUP,
        ..ShareSessionSetup::opening(&member, tid)
    })
    .await;
    AcquiredMember { member, row }
}

#[bon::builder]
async fn acquired_records_from<T, C, P>(
    #[builder(into, default = ShareRecordCount(3))] records: ShareRecordCount,
    hooks: AcquisitionHooks<T, C, P>,
) -> Cluster
where
    T: FnOnce(&mut BrokerConfig),
    C: AsyncFnOnce(&Client, &BrokerHandle),
    P: AsyncFnOnce(&Client, uuid::Uuid),
{
    let setup = AcquiredRecordsSetup { records };
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = trunk_config(&dir);
    (hooks.tweak)(&mut config);
    let StartedShareTopic {
        broker,
        client,
        topic_id: tid,
    } = start_group_topic().config(config).call().await;
    (hooks.configure)(&client, &broker).await;
    (hooks.produce)(&client, tid).await;
    let AcquiredMember { member, row } = join_and_acquire(&broker, &client, tid).await;
    assert!(acquired_count(&row) == setup.records.0, "{row:?}");
    Cluster {
        broker,
        client,
        tid,
        member,
        _dir: dir,
    }
}

/// Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP` (KIP-405).
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

// Turns `t` into a tiered topic whose records leave local disk as soon as the
// remote tier holds them: `produce_n` stamps no record timestamp, so every
// segment is past a one-millisecond `local.retention.ms` at once, and
// `retention.ms=-1` keeps the tier from deleting them for the same reason.
async fn tier_the_source_topic(broker: &BrokerHandle, client: &Client) {
    set_configs()
        .client(client)
        .target(DeadLetterConfigTarget::Topic)
        .name("t")
        .values(&[
            ConfigEntry {
                name: "remote.storage.enable",
                value: "true",
            },
            ConfigEntry {
                name: "local.retention.ms",
                value: "1",
            },
            ConfigEntry {
                name: "retention.ms",
                value: "-1",
            },
        ])
        .call()
        .await;
    time::timeout(Duration::from_secs(30), async {
        loop {
            if broker
                .partition_log_config_for_test("t", 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && config.local_retention == Some(krabka_units::millis(1))
                })
            {
                return;
            }
            // intentional: the reconcile loop applies the topic config to the
            // partition's `LogConfig` with no awaiter or metric.
            time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the tiered-storage config reached the partition");
}

// Waits until `ListOffsets(EARLIEST_LOCAL)` on `t` answers `offset`: every
// record below it is in the remote tier and gone from local disk.
async fn wait_until_local_log_start(client: &Client, offset: krabka_ids::Offset) {
    time::timeout(Duration::from_secs(30), async {
        loop {
            let mut response = client
                .send(ListOffsetsRequest {
                    replica_id: -1,
                    timeout_ms: 5_000,
                    ..single_partition_list_offsets(
                        "t",
                        list_offset_partition(0, EARLIEST_LOCAL_TIMESTAMP),
                    )
                })
                .await
                .expect("ListOffsets");
            let row = response.topics.remove(0).partitions.remove(0);
            if row.error_code == NONE && row.offset == offset.0 {
                return;
            }
            // intentional: the remote-log manager moves the local log start
            // on its own timer, and nothing signals it to the test.
            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the records left local disk for the remote tier");
}

// Kafka's `ShareConsumerDLQTieredStorageTest`: every record of the source topic
// is in the remote tier and gone from local disk before the group fetches it.
// The member acquires the records out of the tier, with their values, and
// rejects them, and the dead-letter records carry the values that the copy
// read back from the tier.
#[bon::builder]
async fn acquired_dead_letters(
    customize: impl FnOnce(&mut BrokerConfig),
    #[builder(into, default = ShareRecordCount(3))] records: ShareRecordCount,
    #[builder(default)] version: DeadLetterShareVersion,
    #[builder(default = & [])] topic_configs: &'_ [ConfigEntry<'_>],
) -> Cluster {
    let setup = DeadLetterQueueSetup {
        records,
        version,
        topic_configs,
    };
    acquired_records(setup.records, customize, async |client, broker| {
        configure_dead_letters(client, broker, setup.version, setup.topic_configs).await;
    })
    .await
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
    tokio::sync::OwnedSemaphorePermit,
};

#[derive(Default)]
struct DeadLetterScenario {
    permit: Option<OwnedSemaphorePermit>,
    cluster: Option<Cluster>,
    remote_directory: Option<TempDir>,
    records: Option<Vec<Record>>,
    acquisition: Option<PartitionData>,
}

#[fixture]
fn dead_letter_case() -> DeadLetterScenario {
    // Keep original source records, DLQ output, and actual broker meters independent.
    DeadLetterScenario::default()
}

#[given(
    "an acquired DLQ batch has {records} records at share version {version} with message limit {limit}"
)]
async fn acquired_dlq_batch(
    dead_letter_case: &mut DeadLetterScenario,
    records: i64,
    version: i16,
    limit: String,
) {
    dead_letter_case.permit = Some(broker_test_permit().await);
    let configs = if limit == "unlimited" {
        vec![]
    } else {
        vec![ConfigEntry {
            name: "max.message.bytes",
            value: &limit,
        }]
    };
    dead_letter_case.cluster = Some(
        acquired_dead_letters()
            .customize(|_| {})
            .records(records)
            .version(if version == 1 {
                DeadLetterShareVersion::One
            } else {
                DeadLetterShareVersion::Two
            })
            .topic_configs(&configs)
            .call()
            .await,
    );
}

#[given("one acquired DLQ record has a 150-millisecond lock and a two-delivery limit")]
async fn delivery_limited_dlq_record(dead_letter_case: &mut DeadLetterScenario) {
    dead_letter_case.permit = Some(broker_test_permit().await);
    dead_letter_case.cluster = Some(
        acquired_dead_letters()
            .customize(|config| {
                config.share_group.record_lock_duration = Duration::from_millis(150);
                config.share_group.max_delivery_attempts = 2;
            })
            .records(1)
            .call()
            .await,
    );
}

#[given(
    "the DLQ topic is absent, automatic creation is {automatic}, and {records} source records are acquired"
)]
async fn missing_dlq_topic(
    dead_letter_case: &mut DeadLetterScenario,
    automatic: bool,
    records: i64,
) {
    dead_letter_case.permit = Some(broker_test_permit().await);
    dead_letter_case.cluster = Some(
        acquired_records(
            ShareRecordCount(records),
            |_| {},
            async |client, _| {
                finalize_share_version_two(client).await;
                if automatic {
                    set_configs()
                        .client(client)
                        .target(DeadLetterConfigTarget::Broker)
                        .name("")
                        .values(&[ConfigEntry {
                            name: "errors.deadletterqueue.auto.create.topics.enable",
                            value: "true",
                        }])
                        .call()
                        .await;
                }
                point_group_at_dead_letter_topic(client).await;
            },
        )
        .await,
    );
}

#[given("one acquired 1000-byte source record targets a DLQ topic with a 1100-byte message limit")]
async fn oversized_dlq_copy(dead_letter_case: &mut DeadLetterScenario) {
    dead_letter_case.permit = Some(broker_test_permit().await);
    dead_letter_case.cluster = Some(
        acquired_records_from()
            .records(1)
            .hooks(AcquisitionHooks {
                tweak: |_: &mut BrokerConfig| {},
                configure: async |client: &Client, broker: &BrokerHandle| {
                    configure_dead_letters(client, broker, DeadLetterShareVersion::default(), &[
                        ConfigEntry {
                            name: "max.message.bytes",
                            value: "1100",
                        },
                    ])
                    .await;
                },
                produce: async |client: &Client, tid: uuid::Uuid| {
                    produce_values()
                        .client(client)
                        .values(vec![Bytes::from(vec![b'x'; 1_000])])
                        .topic_id(tid)
                        .call()
                        .await;
                },
            })
            .call()
            .await,
    );
}

#[given("three source records have left local disk and are acquired from the remote tier")]
async fn remote_dlq_source(dead_letter_case: &mut DeadLetterScenario) {
    dead_letter_case.permit = Some(broker_test_permit().await);
    let directory = TempDir::new().unwrap();
    let remote = TempDir::new().unwrap();
    let mut config = trunk_config(&directory);
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote.path().to_path_buf(),
    });
    config.remote_log_manager_interval = krabka_units::millis(100);
    let StartedShareTopic {
        broker,
        client,
        topic_id: tid,
    } = start_group_topic().config(config).call().await;
    finalize_share_version_two(&client).await;
    create_dead_letter_topic(&broker, &client).await;
    point_group_at_dead_letter_topic(&client).await;
    tier_the_source_topic(&broker, &client).await;
    produce_n()
        .client(&client)
        .topic_id(tid)
        .count(3)
        .call()
        .await;
    wait_until_local_log_start(&client, krabka_ids::Offset(3)).await;
    let AcquiredMember { member, row } = join_and_acquire(&broker, &client, tid).await;
    dead_letter_case.acquisition = Some(row);
    dead_letter_case.cluster = Some(Cluster {
        broker,
        client,
        tid,
        member,
        _dir: directory,
    });
    dead_letter_case.remote_directory = Some(remote);
}

#[when("the member rejects source offsets {first} through {last}")]
async fn reject_dlq_records(dead_letter_case: &DeadLetterScenario, first: i64, last: i64) {
    dead_letter_case
        .cluster()
        .reject_acquired(krabka_ids::Offset(first), krabka_ids::Offset(last))
        .await;
}

#[when("the member rejects the disjoint source offsets zero, two, and four")]
async fn reject_disjoint_dlq_records(dead_letter_case: &DeadLetterScenario) {
    reject_ranges()
        .client(&dead_letter_case.cluster().client)
        .member(&dead_letter_case.cluster().member)
        .topic_id(dead_letter_case.cluster().tid)
        .ranges(&[
            RejectedRange {
                first: krabka_ids::Offset(0),
                last: krabka_ids::Offset(0),
            },
            RejectedRange {
                first: krabka_ids::Offset(2),
                last: krabka_ids::Offset(2),
            },
            RejectedRange {
                first: krabka_ids::Offset(4),
                last: krabka_ids::Offset(4),
            },
        ])
        .call()
        .await;
}

#[when("the first DLQ acquisition expires and the member fetches its second delivery")]
async fn exhaust_dlq_delivery(dead_letter_case: &mut DeadLetterScenario) {
    dead_letter_case
        .cluster()
        .broker
        .wait_until_share_acquired_count(GROUP, dead_letter_case.cluster().tid, 0, 0)
        .await;
    dead_letter_case.acquisition = Some(
        share_fetch(
            &dead_letter_case.cluster().client,
            ShareFetchSetup::from(ShareSessionSetup {
                group: GROUP,
                ..ShareSessionSetup::joined(
                    &dead_letter_case.cluster().member,
                    dead_letter_case.cluster().tid,
                )
            }),
        )
        .await,
    );
}

#[then("the DLQ source is acquired once at delivery count two")]
fn exhausted_dlq_delivery(dead_letter_case: &DeadLetterScenario) {
    let response = dead_letter_case
        .acquisition
        .as_ref()
        .expect("second delivery fetched");
    assert!(
        acquired_count(response) == 1 && response.acquired_records[0].delivery_count == 2,
        "{response:?}"
    );
}

#[when("the consumer waits for {records} dead-letter records")]
async fn read_dlq_records(dead_letter_case: &mut DeadLetterScenario, records: i64) {
    dead_letter_case.records = Some(
        wait_for_dead_letters(
            &dead_letter_case.cluster().broker,
            &dead_letter_case.cluster().client,
            ShareRecordCount(records),
        )
        .await,
    );
}

#[then("the source share start offset reaches {offset}")]
async fn dlq_source_archived(dead_letter_case: &DeadLetterScenario, offset: i64) {
    dead_letter_case
        .cluster()
        .broker
        .wait_until_share_spso(GROUP, dead_letter_case.cluster().tid, 0, offset)
        .await;
    check!(
        dead_letter_case
            .cluster()
            .broker
            .share_state_summary_for_test(GROUP, dead_letter_case.cluster().tid, 0)
            .await
            .map(|(_, _, start, _)| start)
            == Some(offset)
    );
}

#[then(
    "the DLQ meters report {records} written records, {produces} produce attempts, and {failed} failures"
)]
async fn dlq_meters(
    dead_letter_case: &DeadLetterScenario,
    records: u64,
    produces: u64,
    failed: u64,
) {
    dead_letter_case
        .cluster()
        .broker
        .wait_for_metrics("dead-letter meters", |metrics| {
            meters(metrics, GROUP) == (records, produces, failed)
        })
        .await;
}

#[then("all three rejected records retain their source values and all six DLQ headers")]
fn rejected_batch_dlq_headers(dead_letter_case: &DeadLetterScenario) {
    let records = dead_letter_case.records.as_ref().expect("DLQ records read");
    let seen: Vec<_> = records
        .iter()
        .map(|record| {
            (
                record.value.clone(),
                header(record, "__dlq.errors.topic"),
                header(record, "__dlq.errors.partition"),
                header(record, "__dlq.errors.offset"),
                header(record, "__dlq.errors.group"),
                header(record, "__dlq.errors.delivery.count"),
                header(record, "__dlq.errors.message"),
            )
        })
        .collect();
    let expected: Vec<_> = (0..3)
        .map(|offset| {
            (
                Some(Bytes::from(format!("v{offset}"))),
                Some("t".to_owned()),
                Some("0".to_owned()),
                Some(offset.to_string()),
                Some(GROUP.to_owned()),
                Some("1".to_owned()),
                Some("Offset rejected by client.".to_owned()),
            )
        })
        .collect();
    check!(seen == expected);
}

#[then(
    "the disjoint DLQ records retain exactly offsets zero, two, and four with rejection metadata"
)]
fn disjoint_dlq_headers(dead_letter_case: &DeadLetterScenario) {
    let mut records = dead_letter_case
        .records
        .as_ref()
        .expect("DLQ records read")
        .clone();
    records.sort_by_key(|record| header(record, "__dlq.errors.offset"));
    check!(
        records
            .iter()
            .map(|record| (
                header(record, "__dlq.errors.offset"),
                record.value.clone(),
                header(record, "__dlq.errors.delivery.count"),
                header(record, "__dlq.errors.message"),
            ))
            .collect::<Vec<_>>()
            == [0, 2, 4]
                .map(|offset| (
                    Some(offset.to_string()),
                    Some(Bytes::from(format!("v{offset}"))),
                    Some("1".to_owned()),
                    Some("Offset rejected by client.".to_owned()),
                ))
                .to_vec()
    );
}

#[then("the DLQ is empty")]
async fn empty_dlq(dead_letter_case: &DeadLetterScenario) {
    check!(
        dead_letter_records(
            &dead_letter_case.cluster().broker,
            &dead_letter_case.cluster().client
        )
        .await
        .is_empty()
    );
}

#[when("500 milliseconds elapse for an intentionally disabled DLQ write")]
async fn disabled_dlq_window() { time::sleep(Duration::from_millis(500)).await; }

#[then("the delivery-limited DLQ record preserves v0 and threshold metadata at delivery count two")]
fn delivery_limited_dlq_headers(dead_letter_case: &DeadLetterScenario) {
    let record = &dead_letter_case.records.as_ref().expect("DLQ records read")[0];
    check!(
        (
            header(record, "__dlq.errors.offset"),
            header(record, "__dlq.errors.delivery.count"),
            header(record, "__dlq.errors.message"),
            record.value.clone()
        ) == (
            Some("0".to_owned()),
            Some("2".to_owned()),
            Some("Offset delivery count exceeded the threshold.".to_owned()),
            Some(Bytes::from_static(b"v0")),
        )
    );
}

#[then("the missing DLQ topic has not been created")]
fn missing_dlq_not_created(dead_letter_case: &DeadLetterScenario) {
    check!(
        dead_letter_case
            .cluster()
            .broker
            .controller_image_for_test()
            .topic(DLQ_TOPIC)
            .is_none()
    );
}

#[when("the automatically created DLQ topic appears in metadata")]
async fn created_dlq_topic(dead_letter_case: &DeadLetterScenario) {
    dead_letter_case
        .cluster()
        .broker
        .wait_until_partition_present(DLQ_TOPIC, 0)
        .await;
}

#[then("the created DLQ contains v0 and opts into group dead-letter writes")]
fn created_dlq_record(dead_letter_case: &DeadLetterScenario) {
    check!(
        dead_letter_case.records.as_ref().expect("DLQ records read")[0].value
            == Some(Bytes::from_static(b"v0"))
    );
    check!(
        dead_letter_case
            .cluster()
            .broker
            .controller_image_for_test()
            .topic_config(DLQ_TOPIC)
            .and_then(|configs| configs.get("errors.deadletterqueue.group.enable").cloned())
            == Some("true".to_owned())
    );
}

#[then("the remote-tier acquisition contains all three original v0,v1,v2 values")]
fn remote_dlq_acquisition(dead_letter_case: &DeadLetterScenario) {
    let response = dead_letter_case
        .acquisition
        .as_ref()
        .expect("remote source acquired");
    check!(acquired_count(response) == 3);
    check!(
        records_of(response.records.as_ref())
            .iter()
            .map(|record| record.value.clone())
            .collect::<Vec<_>>()
            == source_values()
    );
}

#[then("the remote-source DLQ contains all three original v0,v1,v2 values")]
fn remote_dlq_values(dead_letter_case: &DeadLetterScenario) {
    check!(
        dead_letter_case
            .records
            .as_ref()
            .expect("DLQ records read")
            .iter()
            .map(|record| record.value.clone())
            .collect::<Vec<_>>()
            == source_values()
    );
}

impl DeadLetterScenario {
    async fn shutdown(&mut self) {
        self.cluster
            .take()
            .expect("DLQ cluster")
            .broker
            .shutdown()
            .await;
        self.remote_directory.take();
        self.permit.take();
    }
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a rejected batch is dead lettered then archived"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_batch_is_dead_lettered_then_archived(dead_letter_case: DeadLetterScenario) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "rejects that are not neighbours are all dead lettered and counted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_that_are_not_neighbours_are_all_dead_lettered_and_counted(
    dead_letter_case: DeadLetterScenario,
) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a write the topic refuses counts as failed and still archives"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_the_topic_refuses_counts_as_failed_and_still_archives(
    dead_letter_case: DeadLetterScenario,
) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a record that uses up its deliveries is dead lettered"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_that_uses_up_its_deliveries_is_dead_lettered(
    dead_letter_case: DeadLetterScenario,
) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a reject below share version two writes nothing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reject_below_share_version_two_writes_nothing(dead_letter_case: DeadLetterScenario) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a failed write still archives the record"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_still_archives_the_record(dead_letter_case: DeadLetterScenario) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a missing topic is created when the cluster allows it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_topic_is_created_when_the_cluster_allows_it(
    dead_letter_case: DeadLetterScenario,
) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "a copied record too big for the topic fails and still archives"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copied_record_too_big_for_the_topic_fails_and_still_archives(
    dead_letter_case: DeadLetterScenario,
) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/dead_letter_queue.feature",
    name = "records that only the remote tier holds are acquired and dead lettered"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_that_only_the_remote_tier_holds_are_acquired_and_dead_lettered(
    dead_letter_case: DeadLetterScenario,
) {
    dead_letter_case.shutdown().await;
    drop(dead_letter_case);
}

impl DeadLetterScenario {
    fn cluster(&self) -> &Cluster { self.cluster.as_ref().expect("live scenario cluster") }
}

fn source_values() -> Vec<Option<Bytes>> {
    (0..3)
        .map(|offset| Some(Bytes::from(format!("v{offset}"))))
        .collect()
}
