//! Scenarios for reading a published cut with an ordinary Kafka consumer.
//!
//! The steps live inside the crate because the cut format they decode with,
//! [`decode_cut`], is crate-private: the scenario checks the bytes a consumer
//! reads against the one decoder that the broker itself uses.

use {
    crate::{
        barrier::{
            STATE_TOPIC,
            persistence::{
                CutStatus, CutValue, MissingPartition, PartitionOffset, RecordKind, TopicOffsets,
                decode_cut, decode_key,
            },
        },
        broker::BrokerHandle,
        codes,
        test_support::{StartedBroker, start_broker_with},
        time_util::now_ms,
    },
    assert2::{assert, check},
    krabka_bdd::Given,
    krabka_client_admin::{
        AdminClient, AdminClientLike as _, CreateTopicSpec, TopicMutationOptions,
    },
    krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord},
    krabka_client_core::Client,
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_protocol::krabka::barrier::{
        AlterBarrierGroupsRequest, AlterableBarrierGroup, TriggerBarrierRequest,
        TriggerBarrierResponse,
    },
    rstest_bdd_macros::{given, then, when},
    std::{
        collections::BTreeMap,
        ops::RangeInclusive,
        slice,
        time::{Duration, Instant},
    },
};

/// The group name a scenario uses when the name does not matter.
const GROUP: &str = "orders-cut";

/// The `interval_ms` that asks for no periodic injection, so the only cut is
/// the one the scenario triggers.
const ON_DEMAND_ONLY: i64 = -1;

/// A single-node broker, and a client of the barrier requests.
struct Cluster {
    handle: BrokerHandle,
    _dir: tempfile::TempDir,
    bootstrap: String,
    client: Client,
}

impl Cluster {
    /// Boot a broker whose one `__barrier_state` partition this node leads.
    ///
    /// The test default of 50 partitions at replication factor three cannot be
    /// hosted by one node, so the partition a group hashes to might have no
    /// leader.
    async fn start() -> Self {
        let StartedBroker { broker, dir } = start_broker_with(|config| {
            config.barrier_state_num_partitions = 1;
            config.barrier_state_replication_factor = 1;
        })
        .await;
        let bootstrap = broker.listen_addr().to_string();
        let client = Client::builder()
            .bootstrap(bootstrap.clone())
            .client_id("barrier-cut-consumption")
            .build()
            .await
            .expect("client build");
        Self {
            handle: broker,
            _dir: dir,
            bootstrap,
            client,
        }
    }

    async fn create_topic(&self, topic: &str, partitions: i32) {
        let outcomes = AdminClient::connect(slice::from_ref(&self.bootstrap))
            .await
            .expect("admin connect")
            .create_topics(
                &[CreateTopicSpec {
                    name: topic.to_owned(),
                    partitions,
                    replicas: 1,
                    configs: BTreeMap::default(),
                    replica_assignments: BTreeMap::new(),
                }],
                TopicMutationOptions::with_timeout(krabka_units::secs(10)),
            )
            .await
            .expect("create topic");
        assert!(
            outcomes.iter().all(|outcome| outcome.error.is_none()),
            "{outcomes:?}"
        );
        for partition in 0..partitions {
            self.handle
                .wait_until_partition_present(topic, partition)
                .await;
        }
    }

    async fn define(&self, topic: &str) {
        let response = self
            .client
            .send(AlterBarrierGroupsRequest {
                groups: vec![AlterableBarrierGroup {
                    group: GROUP.to_owned(),
                    topics: vec![topic.to_owned()],
                    interval_ms: ON_DEMAND_ONLY,
                    retained_cuts: 4,
                    delete: false,
                    ..AlterableBarrierGroup::default()
                }],
                ..AlterBarrierGroupsRequest::default()
            })
            .await
            .expect("alter barrier groups");
        let codes: Vec<i16> = response.results.iter().map(|r| r.error_code).collect();
        assert!(codes == vec![codes::NONE], "{response:?}");
    }

    async fn trigger(&self) -> TriggerBarrierResponse {
        let response = self
            .client
            .send(TriggerBarrierRequest {
                group: GROUP.to_owned(),
                timeout_ms: 30_000,
                ..TriggerBarrierRequest::default()
            })
            .await
            .expect("trigger barrier");
        assert!(response.error_code == codes::NONE, "{response:?}");
        response
    }

    /// Read `__barrier_state` from the beginning with a plain consumer, until
    /// a cut record of the scenario's group arrives.
    async fn read_state_topic(&self) -> Vec<ConsumerRecord> {
        let mut consumer = Consumer::builder()
            .bootstrap(&self.bootstrap)
            .group_id("cut-reader")
            .subscribe(vec![STATE_TOPIC.to_owned()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .expect("consumer build");
        let mut records = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !records.iter().any(is_cut_record) && Instant::now() < deadline {
            records.extend(
                consumer
                    .poll(krabka_units::millis(500))
                    .await
                    .expect("poll"),
            );
        }
        consumer.close().await.expect("consumer close");
        records
    }
}

/// Whether `record` is a live cut record of the scenario's group.
fn is_cut_record(record: &ConsumerRecord) -> bool {
    record.value.is_some()
        && record
            .key
            .as_deref()
            .and_then(|key| decode_key(key).ok())
            .is_some_and(|key| key.kind == RecordKind::Cut && key.group == GROUP)
}

/// A cut as the trigger response states it: its epoch and its value.
///
/// The response carries no timestamps, so the caller supplies them.
fn cut_of_response(
    response: &TriggerBarrierResponse,
    triggered_at: i64,
    completed_at: i64,
) -> (i64, CutValue) {
    let cut = CutValue {
        triggered_at,
        completed_at,
        status: CutStatus::try_from(response.status).expect("a known cut status"),
        topics: response
            .topics
            .iter()
            .map(|topic| TopicOffsets {
                topic: topic.topic.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|p| PartitionOffset {
                        partition: PartitionIndex(p.partition),
                        offset: Offset(p.offset),
                    })
                    .collect(),
            })
            .collect(),
        missing: response
            .missing
            .iter()
            .map(|m| MissingPartition {
                topic: m.topic.clone(),
                partition: PartitionIndex(m.partition),
            })
            .collect(),
    };
    (response.epoch, cut)
}

/// What the trigger answered, and the wall-clock window it answered in.
struct Triggered {
    response: TriggerBarrierResponse,
    window: RangeInclusive<i64>,
}

struct CutConsumptionScenario {
    cluster: Option<Cluster>,
    triggered: Given<Triggered>,
    records: Given<Vec<ConsumerRecord>>,
}

impl CutConsumptionScenario {
    async fn cluster(&mut self) -> &Cluster {
        if self.cluster.is_none() {
            self.cluster = Some(Cluster::start().await);
        }
        self.cluster.as_ref().expect("the cluster was just started")
    }
}

#[rstest::fixture]
fn cut_consumption() -> CutConsumptionScenario {
    CutConsumptionScenario {
        cluster: None,
        triggered: Given::new(
            "triggered",
            "Given the operator has triggered a cut of the group",
        ),
        records: Given::new(
            "records",
            "When a plain Kafka consumer reads __barrier_state from the beginning",
        ),
    }
}

#[given("the {topic} topic has {partitions} partitions")]
async fn topic_has_partitions(
    cut_consumption: &mut CutConsumptionScenario,
    topic: String,
    partitions: i32,
) {
    cut_consumption
        .cluster()
        .await
        .create_topic(&topic, partitions)
        .await;
}

#[given("an operator has defined a barrier group over the {topic} topic")]
async fn defined_over_one_topic(cut_consumption: &mut CutConsumptionScenario, topic: String) {
    cut_consumption.cluster().await.define(&topic).await;
}

#[given("the operator has triggered a cut of the group")]
async fn triggered_cut(cut_consumption: &mut CutConsumptionScenario) {
    let before = now_ms();
    let response = cut_consumption.cluster().await.trigger().await;
    let after = now_ms();
    cut_consumption.triggered.set(Triggered {
        response,
        window: before..=after,
    });
}

#[when("a plain Kafka consumer reads __barrier_state from the beginning")]
async fn consume_state_topic(cut_consumption: &mut CutConsumptionScenario) {
    let records = cut_consumption.cluster().await.read_state_topic().await;
    cut_consumption.records.set(records);
}

#[then("the cut record it reads holds the epoch and the offsets of the trigger response")]
fn cut_record_matches_trigger(cut_consumption: &CutConsumptionScenario) {
    let consumed: Vec<(i64, CutValue)> = cut_consumption
        .records
        .get()
        .iter()
        .filter(|record| is_cut_record(record))
        .map(|record| {
            let key = decode_key(record.key.as_deref().expect("a cut record has a key"))
                .expect("the key decodes");
            let value = decode_cut(record.value.as_deref().expect("a cut record has a value"))
                .expect("the value decodes with the persisted cut format");
            (key.epoch, value)
        })
        .collect();
    let Triggered { response, window } = cut_consumption.triggered.get();
    let [(_, read)] = consumed.as_slice() else {
        panic!("expected exactly one cut record, read {consumed:?}");
    };
    // The trigger response names no time, so the two timestamps are held to
    // the window the trigger ran in, and the rest of the value must be equal.
    check!(
        window.contains(&read.triggered_at),
        "{read:?} in {window:?}"
    );
    check!(
        window.contains(&read.completed_at),
        "{read:?} in {window:?}"
    );
    check!(read.triggered_at <= read.completed_at, "{read:?}");
    assert!(
        consumed
            == vec![cut_of_response(
                response,
                read.triggered_at,
                read.completed_at
            )]
    );
}

krabka_bdd::scenarios!(
    "tests/features/barrier_cut_consumption",
    fixtures = [cut_consumption: CutConsumptionScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
