//! Scenario 3: a Krabka follower truncates a divergent suffix from a JVM leader.
//!
//! This is the reverse direction of scenario 2. The scenario parks replication
//! behind a phantom leader, appends a Krabka-only suffix, then promotes the JVM
//! replica, and asserts that the Krabka follower truncates to the shared prefix
//! and resumes at the JVM leader's exact log end offset.

use {
    crate::{
        mixed_cluster::MixedCluster,
        test_support::truncation::{prelude::*, *},
    },
    krabka_metadata::PartitionRecord,
    tokio::time,
};
const TOPIC: &str = "krabka-kip320-krabka-follower";
#[derive(Default)]
struct KrabkaFollowerRecovery {
    cluster: Option<MixedCluster>,
    container: String,
    bootstrap_all: String,
    prefix_leo: i64,
    partition: Option<PartitionRecord>,
    parked_epoch: Option<LeaderEpoch>,
    diverged_leo: Option<i64>,
    truncated_leo: Option<i64>,
    prefix_dump: Option<String>,
    final_leo: Option<i64>,
}
#[rstest::fixture]
fn krabka_follower_recovery() -> KrabkaFollowerRecovery {
    // Each recovery owns a real mixed cluster and its independently authored logs.
    KrabkaFollowerRecovery::default()
}
#[rstest_bdd_macros::given(
    "a mixed topic krabka-kip320-krabka-follower with a committed shared prefix"
)]
async fn reverse_divergence_prefix(krabka_follower_recovery: &mut KrabkaFollowerRecovery) {
    let (container, cluster, bootstrap_all, prefix_leo) =
        prepare_divergence(TOPIC, DivergenceDirection::KrabkaFollower).await;
    krabka_follower_recovery.cluster = Some(cluster);
    krabka_follower_recovery.container = container;
    krabka_follower_recovery.bootstrap_all = bootstrap_all;
    krabka_follower_recovery.prefix_leo = prefix_leo;
}
#[rstest_bdd_macros::when(
    "replication is parked behind phantom leader 99 and Krabka appends {count} divergent records"
)]
async fn park_krabka_suffix(krabka_follower_recovery: &mut KrabkaFollowerRecovery, count: usize) {
    let cluster = krabka_follower_recovery
        .cluster
        .as_ref()
        .expect("mixed cluster");
    let c1 = &cluster.krabka[0].0;
    let container = &krabka_follower_recovery.container;
    c1.wait_until_partition_present(TOPIC, 0).await;
    let partition = c1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record present after wait");
    let parked_epoch = LeaderEpoch(partition.leader_epoch.0 + 1);

    // Freeze the JVM replica before parking replication. Otherwise it can
    // copy part of the deliberately forged suffix while the phantom-leader
    // metadata is still propagating, making its authoritative prefix longer.
    set_container_paused(container, true);

    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(
        single_leader_record()
            .previous(&partition)
            .topic(TOPIC)
            .leader(99)
            .epoch(parked_epoch)
            .call(),
    ))
    .await
    .expect("park reverse-direction replicas behind phantom leader");
    c1.wait_until_local_partition_target(TOPIC, 0, krabka_broker::NodeId(99), parked_epoch)
        .await;

    c1.produce_records_for_test(TOPIC, 0, count)
        .await
        .expect("append divergent suffix on parked Krabka replica");

    krabka_follower_recovery.diverged_leo = Some(c1.local_log_end_offset(TOPIC, 0).unwrap_or(0));
    krabka_follower_recovery.partition = Some(partition);
    krabka_follower_recovery.parked_epoch = Some(parked_epoch);
}
#[rstest_bdd_macros::then("the parked Krabka log ends {count} offsets beyond the shared prefix")]
fn parked_krabka_bounds(krabka_follower_recovery: &KrabkaFollowerRecovery, count: i64) {
    assert2::assert!(
        krabka_follower_recovery
            .diverged_leo
            .expect("divergent append executed")
            == krabka_follower_recovery.prefix_leo + count
    );
}
#[rstest_bdd_macros::when("the JVM replica is promoted in the next epoch and Krabka recovers")]
async fn promote_reverse_jvm(krabka_follower_recovery: &mut KrabkaFollowerRecovery) {
    let cluster = krabka_follower_recovery
        .cluster
        .as_ref()
        .expect("mixed cluster");
    let c1 = &cluster.krabka[0].0;
    let container = &krabka_follower_recovery.container;
    let bootstrap_all = &krabka_follower_recovery.bootstrap_all;
    let prefix_leo = krabka_follower_recovery.prefix_leo;
    let partition = krabka_follower_recovery
        .partition
        .as_ref()
        .expect("partition metadata");
    let parked_epoch = krabka_follower_recovery.parked_epoch.expect("parked epoch");
    let krabka_leo_diverged = krabka_follower_recovery.diverged_leo.expect("diverged log");
    let jvm_epoch = LeaderEpoch(parked_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(
        single_leader_record()
            .previous(partition)
            .topic(TOPIC)
            .leader(3)
            .epoch(jvm_epoch)
            .partition_epoch_delta(2)
            .call(),
    ))
    .await
    .expect("promote JVM broker for reverse-direction recovery");

    set_container_paused(container, false);

    wait_for_described_leader(bootstrap_all, TOPIC, 3, LEADER_WAIT).await;

    // 5. Observe the truncation itself, before adding any new leader records.
    //    Equal final LEOs alone would not distinguish truncate-and-refetch from
    //    leaving the bogus suffix in place.
    let dl = Instant::now() + Duration::from_secs(45);
    let mut final_leo = krabka_leo_diverged;
    loop {
        final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(final_leo);
        if final_leo == prefix_leo {
            break;
        }
        assert2::assert!(
            Instant::now() <= dl,
            "Krabka follower did not truncate to JVM prefix LEO {prefix_leo}; current LEO={final_leo}"
        );
        time::sleep(Duration::from_millis(500)).await;
    }
    let jvm_prefix_dump =
        dump_log_in_container(container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));

    krabka_follower_recovery.truncated_leo = Some(final_leo);
    krabka_follower_recovery.prefix_dump = Some(jvm_prefix_dump);
}
#[rstest_bdd_macros::then(
    "Krabka truncates exactly to the shared prefix and the JVM dump excludes {forbidden}"
)]
fn reverse_truncation_boundary(
    krabka_follower_recovery: &KrabkaFollowerRecovery,
    forbidden: String,
) {
    let prefix = krabka_follower_recovery.prefix_leo;
    let dump = krabka_follower_recovery
        .prefix_dump
        .as_ref()
        .expect("JVM prefix dump");
    assert2::assert!(
        krabka_follower_recovery
            .truncated_leo
            .expect("truncation observed")
            == prefix
    );
    assert2::assert!(
        max_offset_in_dump(dump) == Some(prefix - 1),
        "JVM prefix dump: {dump}"
    );
    assert2::assert!(
        !dump.contains(&forbidden),
        "Krabka suffix leaked to JVM: {dump}"
    );
}
#[rstest_bdd_macros::when("the JVM leader appends authoritative payloads {records}")]
async fn authoritative_jvm_suffix(
    krabka_follower_recovery: &mut KrabkaFollowerRecovery,
    records: String,
) {
    let records: Vec<String> = serde_json::from_str(&records).expect("authoritative payloads");
    produce_lines_via_jvm(&krabka_follower_recovery.bootstrap_all, TOPIC, &records);
    let c1 = &krabka_follower_recovery
        .cluster
        .as_ref()
        .expect("mixed cluster")
        .krabka[0]
        .0;
    let count = i64::try_from(records.len()).expect("record count");
    c1.wait_until_local_log_end_offset(TOPIC, 0, krabka_follower_recovery.prefix_leo + count)
        .await;
    krabka_follower_recovery.final_leo = Some(c1.local_log_end_offset(TOPIC, 0).unwrap_or(0));
}
#[rstest_bdd_macros::then("the recovered Krabka log ends {count} offsets beyond the shared prefix")]
fn recovered_krabka_bounds(krabka_follower_recovery: &KrabkaFollowerRecovery, count: i64) {
    assert2::assert!(
        krabka_follower_recovery
            .final_leo
            .expect("replication observed")
            == krabka_follower_recovery.prefix_leo + count
    );
}
#[rstest_bdd_macros::then("the reverse divergence cluster is shut down")]
async fn finish_reverse_recovery(krabka_follower_recovery: &mut KrabkaFollowerRecovery) {
    krabka_follower_recovery
        .cluster
        .take()
        .expect("mixed cluster")
        .shutdown()
        .await;
}
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_kip320_divergence/krabka_follower_truncation.feature",
    name = "Krabka removes its isolated suffix before following a promoted JVM leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kip320_krabka_follower_truncates_from_jvm_leader(
    krabka_follower_recovery: KrabkaFollowerRecovery,
) {
    let _ = krabka_follower_recovery;
}
