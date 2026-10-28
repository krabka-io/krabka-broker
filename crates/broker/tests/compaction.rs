#![cfg(test)]

//! Log compaction end-to-end broker integration test.
//!
//! The test produces 30 records across 3 keys, k1, k2, and k3, into a compacted
//! topic. It waits for a compaction pass, force-rolls the active segment, and
//! waits for another pass. It then fetches the records and asserts that exactly
//! 3 distinct keys survive with only their latest values, v10-kN. Old values
//! v0..v9 must be gone from the sealed segments.
//!
//! Gated to non-Windows to match the multi-broker test convention established by the existing integration suites.

mod support;

use {
    crate::{
        compaction_cluster::start_broker_with_fast_cleaner,
        compaction_records::{FlatRecord, fetch_all},
        compaction_rpc::{create_topic_with_configs, get_topic_id, produce_record},
        support::topics::CreateTopicSetup,
    },
    krabka_broker::metrics::PartitionLabel,
};

mod kafka_wire;
// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `compaction/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "compaction/compaction_cluster.rs"]
mod compaction_cluster;
#[path = "compaction/compaction_records.rs"]
mod compaction_records;
#[path = "compaction/compaction_rpc.rs"]
mod compaction_rpc;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-compaction-test";

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_protocol::primitives::uuid::Uuid,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{collections::BTreeSet, net::SocketAddr},
    tempfile::TempDir,
};

#[derive(Default)]
struct CompactedLog {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    address: Option<SocketAddr>,
    topic: String,
    topic_id: Uuid,
    records: Vec<FlatRecord>,
}

#[fixture]
fn compacted() -> CompactedLog {
    // Start this scenario with fresh inputs and observations.

    CompactedLog {
        broker: None,
        directory: None,
        address: None,
        topic: String::new(),
        topic_id: Uuid::default(),
        records: Vec::new(),
    }
}

#[given("a compacted topic {topic} with segment size {segment_bytes} and a one-second cleaner")]
async fn compacted_topic(compacted: &mut CompactedLog, topic: String, segment_bytes: u32) {
    let (handle, directory, addr) = start_broker_with_fast_cleaner().await;
    let size = segment_bytes.to_string();
    create_topic_with_configs(addr, CreateTopicSetup {
        topic: &topic,
        configs: &[
            ("cleanup.policy", "compact"),
            ("internal.segment.bytes", &size),
        ],
        ..Default::default()
    })
    .await;
    handle.wait_until_partition_present(&topic, 0).await;
    handle
        .wait_for_metrics("compaction LogConfig propagated", |_m| {
            handle
                .partition_log_config_for_test(&topic, 0)
                .is_some_and(|cfg| {
                    cfg.cleanup_policy == krabka_log::CleanupPolicy::Compact
                        && cfg.segment_size == krabka_units::bytes(segment_bytes)
                })
        })
        .await;
    compacted.topic_id = get_topic_id(addr, &topic).await;
    compacted.topic = topic;
    compacted.address = Some(addr);
    compacted.directory = Some(directory);
    compacted.broker = Some(handle);
}

impl CompactedLog {
    async fn append(&self, key: &str, value: &str) {
        produce_record()
            .addr(self.address.unwrap())
            .topic(&self.topic)
            .topic_id(self.topic_id)
            .key(key.as_bytes())
            .value(value.as_bytes())
            .call()
            .await;
    }

    async fn await_next_compaction(&self) {
        let handle = self.broker.as_ref().unwrap();
        let label = PartitionLabel {
            topic: self.topic.clone().into(),
            partition: 0,
        };
        let before = handle
            .metrics()
            .log_compactions_total
            .get_or_create(&label)
            .get();
        handle
            .wait_for_metrics("another compaction pass on sealed segments", |m| {
                m.log_compactions_total.get_or_create(&label).get() > before
            })
            .await;
    }
}

#[when(
    "rounds {start} through {end} write values vROUND-KEY for keys {keys} and a compaction pass completes"
)]
async fn historical_values(compacted: &CompactedLog, start: u32, end: u32, keys: String) {
    for round in start..=end {
        for key in keys.split(',') {
            compacted.append(key, &format!("v{round}-{key}")).await;
        }
    }
    compacted.await_next_compaction().await;
}

#[when(
    "round {round} writes final values for keys {keys}, followed by {padding} padding records and another compaction pass"
)]
async fn final_values(compacted: &CompactedLog, round: u32, keys: String, padding: u32) {
    for key in keys.split(',') {
        compacted.append(key, &format!("v{round}-{key}")).await;
    }
    for index in 0..padding {
        compacted
            .append("__pad__", &format!("padding-{index}"))
            .await;
    }
    compacted.await_next_compaction().await;
}

#[when("the compacted log is fetched from offset zero")]
async fn read_log(compacted: &mut CompactedLog) {
    compacted.records = fetch_all(
        compacted.address.unwrap(),
        &compacted.topic,
        compacted.topic_id,
    )
    .await;
}

#[then(
    "the surviving non-padding keys are {keys}, each retains round {latest}, and rounds {start} through {end} are absent"
)]
fn retained_values(compacted: &CompactedLog, keys: String, latest: u32, start: u32, end: u32) {
    let expected: BTreeSet<String> = keys.split(',').map(str::to_owned).collect();
    let actual: BTreeSet<String> = compacted
        .records
        .iter()
        .map(|record| String::from_utf8(record.key.clone()).unwrap())
        .filter(|key| key != "__pad__")
        .collect();
    assert!(actual == expected);
    for key in &expected {
        let values: Vec<String> = compacted
            .records
            .iter()
            .filter(|record| record.key == key.as_bytes())
            .map(|record| String::from_utf8(record.value.clone()).unwrap())
            .collect();
        assert!(
            values.contains(&format!("v{latest}-{key}")),
            "key {key} lost its latest value: {values:?}"
        );
        for round in start..=end {
            assert!(
                !values.contains(&format!("v{round}-{key}")),
                "key {key} retains stale round {round}: {values:?}"
            );
        }
    }
}

#[scenario(
    path = "tests/features/migrated/tests/compaction.feature",
    name = "compaction dedupes via native client"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_dedupes_via_native_client(compacted: CompactedLog) {
    compacted.broker.unwrap().shutdown().await;
}
