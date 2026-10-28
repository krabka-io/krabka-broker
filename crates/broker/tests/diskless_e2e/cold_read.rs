//! Flush, trim, then read a trimmed offset back out of the object store.
//!
//! The WAL quorum keeps a committed prefix on local disks only until the
//! flusher has moved it into the object store and the index record naming it
//! has been committed and projected. After that the local logs are trimmed and
//! the only copy of those offsets is the flushed object.
//!
//! What makes this a real cold read, and not an accidental local hit, is the
//! assertion that the partition's log start has actually passed the offset
//! being fetched. Below the log start there is nothing left to serve locally,
//! so `OFFSET_OUT_OF_RANGE` is the only answer the log path can give and the
//! object store is the only thing that can rescue it.

use {
    crate::{
        TOPIC,
        topic::{flushing_workload, seeded_leader},
        wire::{
            assert_matches_produced, delete_records_below, earliest_offset, fetch_error_code,
            fetch_log,
        },
    },
    assert2::assert,
    std::time::{Duration, Instant},
    tokio::time,
};

/// The leader's log start, high watermark and projected flush frontier.
struct TrimmedFlush {
    log_start: i64,
    high_watermark: i64,
    frontier: Option<i64>,
}

/// Poll the leader's own flush inputs until the committed prefix is both
/// flushed and trimmed, and return the log start, high watermark and projected
/// flush frontier.
///
/// `diskless_flush_state_for_test` reports the same values the flusher itself
/// reads, so this waits on the real flush pipeline rather than on a proxy for
/// it. The `diskless` flag is asserted here because every other assertion in
/// this case would also hold for an ordinary topic that simply never trimmed.
async fn await_trimmed_flush(leader: &krabka_broker::BrokerHandle, committed: i64) -> TrimmedFlush {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let state = leader.diskless_flush_state_for_test(TOPIC, 0).await;
        if let Some((diskless, _, log_start, _, high_watermark, frontier)) = state {
            assert!(
                diskless,
                "the topic must be diskless for this case to mean anything"
            );
            if log_start > 0 && frontier == Some(committed) {
                return TrimmedFlush {
                    log_start,
                    high_watermark,
                    frontier,
                };
            }
        }
        assert!(
            Instant::now() <= deadline,
            "the diskless flusher did not flush and trim the committed prefix; last state \
             was {state:?}"
        );
        time::sleep(Duration::from_millis(50)).await;
    }
}

use {
    crate::{cluster::DisklessCluster, wire::FetchedLog},
    krabka_broker::NodeId,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
};
#[derive(Default)]
struct DisklessColdRead {
    cluster: Option<DisklessCluster>,
    admin: Option<Client>,
    producer: Option<Client>,
    topic: Option<WireUuid>,
    leader: Option<NodeId>,
    trimmed: Option<TrimmedFlush>,
    fetched: Option<FetchedLog>,
    earliest: Option<i64>,
    deleted: Option<i64>,
    deleted_errors: Option<Vec<i16>>,
}
#[rstest::fixture]
fn diskless_cold_read() -> DisklessColdRead {
    // Each cold-read contract owns its cluster, WAL and shared object tier.
    DisklessColdRead::default()
}
#[rstest_bdd_macros::given(
    "a diskless quorum with 24 acknowledged diskless-e2e-record payloads and flush interval {millis} milliseconds"
)]
async fn cold_diskless_prefix(diskless_cold_read: &mut DisklessColdRead, millis: u32) {
    let (cluster, admin, topic, leader, _values, producer) =
        flushing_workload(krabka_units::millis(millis)).await;
    diskless_cold_read.cluster = Some(cluster);
    diskless_cold_read.admin = Some(admin);
    diskless_cold_read.producer = Some(producer);
    diskless_cold_read.topic = Some(topic);
    diskless_cold_read.leader = Some(leader);
}
#[rstest_bdd_macros::when("the published flush frontier trims the leader's local committed prefix")]
async fn flush_and_trim_diskless(diskless_cold_read: &mut DisklessColdRead) {
    let (leader, committed) = seeded_leader(
        diskless_cold_read.cluster.as_ref().expect("cluster"),
        diskless_cold_read.leader.expect("leader"),
    );
    diskless_cold_read.trimmed = Some(await_trimmed_flush(leader, committed).await);
}
#[rstest_bdd_macros::then(
    "the flush frontier and high watermark are {committed} and the local log start exceeds {floor}"
)]
fn cold_trim_boundaries(diskless_cold_read: &DisklessColdRead, committed: i64, floor: i64) {
    let trimmed = diskless_cold_read.trimmed.as_ref().expect("trim observed");
    assert!(trimmed.frontier == Some(committed));
    assert!(trimmed.high_watermark == committed);
    assert!(trimmed.log_start > floor);
}
#[rstest_bdd_macros::when(
    "the diskless leader fetches {count} records from offset {offset} with a 90 second deadline"
)]
async fn fetch_cold_records(diskless_cold_read: &mut DisklessColdRead, count: usize, offset: i64) {
    let cluster = diskless_cold_read.cluster.as_ref().expect("cluster");
    let bootstrap = cluster.bootstrap_for_node(diskless_cold_read.leader.expect("leader"));
    let topic = diskless_cold_read.topic.expect("topic");
    diskless_cold_read.fetched =
        Some(fetch_log(&bootstrap, topic, offset, count, Duration::from_secs(90)).await);
    diskless_cold_read.earliest = Some(earliest_offset(&bootstrap).await);
}
#[rstest_bdd_macros::then(
    "the cold read contains exactly the produced {count} payloads at contiguous offsets from {offset}"
)]
fn cold_payloads(diskless_cold_read: &DisklessColdRead, count: usize, offset: i64) {
    assert_matches_produced(
        diskless_cold_read
            .fetched
            .as_ref()
            .expect("cold fetch executed"),
        offset,
        count,
    );
}
#[rstest_bdd_macros::then("the earliest diskless offset is {offset}")]
fn cold_earliest_offset(diskless_cold_read: &DisklessColdRead, offset: i64) {
    assert!(
        diskless_cold_read
            .earliest
            .expect("earliest offset queried")
            == offset
    );
}
#[rstest_bdd_macros::when("DeleteRecords removes diskless offsets below {floor}")]
async fn delete_cold_prefix(diskless_cold_read: &mut DisklessColdRead, floor: i64) {
    let cluster = diskless_cold_read.cluster.as_ref().expect("cluster");
    let bootstrap = cluster.bootstrap_for_node(diskless_cold_read.leader.expect("leader"));
    let topic = diskless_cold_read.topic.expect("topic");
    diskless_cold_read.deleted = Some(delete_records_below(&bootstrap, floor).await);
    diskless_cold_read.earliest = Some(earliest_offset(&bootstrap).await);
    diskless_cold_read.deleted_errors = Some(vec![
        fetch_error_code(&bootstrap, topic, 0).await,
        fetch_error_code(&bootstrap, topic, floor - 1).await,
    ]);
}
#[rstest_bdd_macros::then(
    "the delete low watermark is {floor} and deleted fetch errors are {errors}"
)]
fn cold_delete_result(diskless_cold_read: &DisklessColdRead, floor: i64, errors: String) {
    let errors: Vec<i16> = serde_json::from_str(&errors).expect("expected deleted-offset errors");
    assert!(diskless_cold_read.deleted.expect("delete executed") == floor);
    assert!(
        diskless_cold_read
            .deleted_errors
            .as_ref()
            .expect("deleted offsets queried")
            == &errors
    );
}
#[rstest_bdd_macros::then("the retained cold read starts exactly at offset {offset}")]
fn retained_cold_offset(diskless_cold_read: &DisklessColdRead, offset: i64) {
    assert!(
        diskless_cold_read
            .fetched
            .as_ref()
            .expect("retained fetch executed")
            .records[0]
            .offset
            == offset
    );
}
#[rstest_bdd_macros::then("the cold-read clients and cluster are shut down")]
async fn finish_cold_reads(diskless_cold_read: &mut DisklessColdRead) {
    drop(diskless_cold_read.producer.take());
    drop(diskless_cold_read.admin.take());
    diskless_cold_read
        .cluster
        .take()
        .expect("cluster")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/diskless_e2e/cold_read.feature",
    name = "trimmed diskless offsets are served from the object store"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trimmed_diskless_offsets_are_served_from_the_object_store(
    diskless_cold_read: DisklessColdRead,
) {
    let _ = diskless_cold_read;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/diskless_e2e/cold_read.feature",
    name = "deleted diskless records leave the object store unreadable"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_diskless_records_leave_the_object_store_unreadable(
    diskless_cold_read: DisklessColdRead,
) {
    let _ = diskless_cold_read;
}
