//! `read_committed` reads of a transactional range, before and after local
//! eviction moves it to the remote tier (KIP-405 under KIP-98 and KIP-932).
//!
//! Kafka 4.3.1 serves a `read_committed` `Fetch` of an offset below the local
//! log start from `RemoteLogManager.read`. The tier only holds segments that
//! end below the last stable offset (`RLMCopyTask.candidateLogSegments`), so
//! no remote batch is past it, and `collectAbortedTransactions` fills the
//! response's `AbortedTransactions` from the remote `.txnindex` of the
//! segment and every later one, then from the local segments past the tier.
//! A share group under `share.isolation.level=read_committed` acquires up to
//! the last stable offset, and `SharePartition` archives the data of aborted
//! transactions and the control batches instead of handing them out.
//!
//! The test writes interleaved committed and aborted transactions across
//! many 1 KiB segments, one abort whose marker lands past the last stable
//! offset, and one transaction it leaves open with plain records after it.
//! It reads the range with `read_committed` while both tiers hold it, evicts
//! it from local disk, reads it again from the tier alone, and requires both
//! reads to see exactly the committed records. It then commits the open
//! transaction and reads on past the old last stable offset.
#![cfg(test)]

use {
    assert2::{assert, check},
    tokio::time,
};
mod support;

#[path = "tiered_read_committed/reads.rs"]
mod reads;
#[path = "tiered_read_committed/tiered_partition.rs"]
mod tiered_partition;
#[path = "tiered_read_committed/workload.rs"]
mod workload;

use {
    reads::{CommittedRead, ShareReader, acquisitions_of, read_committed_fetch},
    std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    },
    support::transactions::TransactionOutcome,
    tiered_partition::{TOPIC, TieredPartition, start_tiered_partition},
    workload::{Workload, Written, write_workload},
};

/// How long the transaction markers may take to reach the partition.
const MARKER_DEADLINE: Duration = Duration::from_secs(30);

/// Reads from `from` until the `read_committed` view is `want`, so the
/// coordinator's asynchronous markers have all landed.
async fn await_committed_read(
    tiered: &TieredPartition,
    from: i64,
    want: &CommittedRead,
) -> CommittedRead {
    let deadline = Instant::now() + MARKER_DEADLINE;
    // intentional: `EndTxn` answers before its markers are written, and the
    // last stable offset is the only signal that they were.
    loop {
        let read = read_committed_fetch(tiered, from).await;
        if read == *want {
            return read;
        }
        assert!(
            Instant::now() <= deadline,
            "the read from {from} stayed {read:?}, not {want:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// The offsets of `records`.
fn offsets(records: &[Written]) -> BTreeSet<i64> {
    records.iter().map(|record| record.offset).collect()
}

/// What a `read_committed` reader of `[0, LSO)` must see while the
/// transaction `workload` left open is still open.
fn committed_below_open(workload: &Workload) -> CommittedRead {
    CommittedRead {
        last_stable_offset: workload.open_first_offset,
        visible: workload.visible_below_open(),
        aborted: workload.aborted.clone(),
        batches_past_last_stable_offset: Vec::new(),
        records_without_aborted_list: Vec::new(),
    }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct TieredVisibility {
    partition: Option<TieredPartition>,
    workload: Option<Workload>,
    expected: Option<CommittedRead>,
    before: Option<CommittedRead>,
    after: Option<CommittedRead>,
    before_reader: Option<ShareReader>,
    after_reader: Option<ShareReader>,
    tiered_end: Option<i64>,
    local_start: Option<i64>,
    open_lso: Option<i64>,
    committed_suffix: Option<Vec<Written>>,
    suffix_expected: Option<CommittedRead>,
    suffix_read: Option<CommittedRead>,
}

#[fixture]
fn tiered_visibility() -> TieredVisibility {
    // Keep both readers and their real Fetch observations across the physical tier eviction.
    TieredVisibility::default()
}

impl TieredVisibility {
    fn partition(&self) -> &TieredPartition { self.partition.as_ref().expect("tiered partition") }

    fn expected(&self) -> &CommittedRead { self.expected.as_ref().expect("pre-commit visibility") }
}

#[given("a tiered partition has 1-KiB segments and retains all local segments")]
async fn tiered_visibility_partition(tiered_visibility: &mut TieredVisibility) {
    tiered_visibility.partition = Some(start_tiered_partition().await);
}

#[given(
    "five interleaved commit-abort rounds each write three two-record batches and one plain record, followed by eight abort batches, two open batches and two plain tail records"
)]
async fn tiered_visibility_workload(tiered_visibility: &mut TieredVisibility) {
    tiered_visibility.workload = Some(
        write_workload(
            &tiered_visibility.partition().client,
            TOPIC,
            tiered_visibility.partition().topic_id,
        )
        .await,
    );
    tiered_visibility.open_lso = Some(
        tiered_visibility
            .workload
            .as_ref()
            .expect("transaction workload")
            .open_first_offset,
    );
    tiered_visibility.expected = Some(committed_below_open(
        tiered_visibility
            .workload
            .as_ref()
            .expect("transaction workload"),
    ));
    await_committed_read(
        tiered_visibility.partition(),
        0,
        tiered_visibility.expected(),
    )
    .await;
}

#[when("the remote tier copies every segment eligible below the open transaction's LSO")]
async fn copy_tiered_visibility_prefix(tiered_visibility: &mut TieredVisibility) {
    let end = tiered_visibility
        .partition()
        .copy_bound(tiered_visibility.open_lso.expect("open transaction LSO"));
    tiered_visibility
        .partition()
        .await_tiered_through(end)
        .await;
    tiered_visibility.tiered_end = Some(end);
}

#[then(
    "the tier holds a-last data while its abort marker stays local and local start remains zero"
)]
async fn copied_tiered_visibility_prefix(tiered_visibility: &TieredVisibility) {
    assert!(
        tiered_visibility
            .workload
            .as_ref()
            .expect("transaction workload")
            .last_abort
            .1
            < tiered_visibility.tiered_end.expect("copied prefix end")
    );
    check!(tiered_visibility.partition().local_log_start().await == 0);
}

#[when("Fetch and a fresh committed share group read the range before local eviction")]
async fn read_before_tiered_eviction(tiered_visibility: &mut TieredVisibility) {
    tiered_visibility.before = Some(read_committed_fetch(tiered_visibility.partition(), 0).await);
    tiered_visibility.before_reader =
        Some(ShareReader::join(tiered_visibility.partition(), "rc-before-eviction").await);
    let visible = offsets(&tiered_visibility.expected().visible);
    tiered_visibility
        .before_reader
        .as_mut()
        .expect("before-eviction share reader")
        .acquire_through(
            tiered_visibility
                .partition
                .as_ref()
                .expect("tiered partition"),
            &visible,
        )
        .await;
}

#[then(
    "both pre-eviction readers see exactly committed and plain records below the open LSO with complete abort metadata"
)]
fn before_tiered_visibility(tiered_visibility: &TieredVisibility) {
    check!(
        tiered_visibility
            .before
            .as_ref()
            .expect("pre-eviction Fetch")
            == tiered_visibility.expected()
    );
    check!(
        tiered_visibility
            .before_reader
            .as_ref()
            .expect("pre-eviction share read")
            .acquired
            == acquisitions_of(&tiered_visibility.expected().visible)
    );
}

#[when("all copied prefix segments are evicted from local disk")]
async fn evict_tiered_visibility_prefix(tiered_visibility: &TieredVisibility) {
    tiered_visibility
        .partition()
        .evict_local_segments(tiered_visibility.tiered_end.expect("copied prefix end"))
        .await;
}

#[when("Fetch and a different committed share group read the same range after eviction")]
async fn read_after_tiered_eviction(tiered_visibility: &mut TieredVisibility) {
    tiered_visibility.after = Some(read_committed_fetch(tiered_visibility.partition(), 0).await);
    tiered_visibility.after_reader =
        Some(ShareReader::join(tiered_visibility.partition(), "rc-after-eviction").await);
    let visible = offsets(&tiered_visibility.expected().visible);
    tiered_visibility
        .after_reader
        .as_mut()
        .expect("after-eviction share reader")
        .acquire_through(
            tiered_visibility
                .partition
                .as_ref()
                .expect("tiered partition"),
            &visible,
        )
        .await;
    tiered_visibility.local_start = Some(tiered_visibility.partition().local_log_start().await);
}

#[then(
    "post-eviction visibility matches both original readers and most returned records are remote-only"
)]
async fn after_tiered_visibility(tiered_visibility: &TieredVisibility) {
    check!(
        tiered_visibility
            .after
            .as_ref()
            .expect("post-eviction Fetch")
            == tiered_visibility.expected()
    );
    check!(
        tiered_visibility
            .after_reader
            .as_ref()
            .expect("post-eviction share read")
            .acquired
            == acquisitions_of(&tiered_visibility.expected().visible)
    );
    let end = tiered_visibility.tiered_end.expect("copied prefix end");
    check!(tiered_visibility.local_start == Some(end));
    let remote_only = offsets(&tiered_visibility.expected().visible)
        .range(..end)
        .count();
    check!(remote_only * 2 > tiered_visibility.expected().visible.len());
    check!(
        tiered_visibility
            .partition()
            .broker
            .high_watermark_for_test(TOPIC, 0)
            .await
            == Some(
                tiered_visibility
                    .workload
                    .as_ref()
                    .expect("transaction workload")
                    .log_end_while_open()
            )
    );
}

#[when("the open transaction commits and both readers continue beyond the old LSO")]
async fn commit_tiered_visibility_tail(tiered_visibility: &mut TieredVisibility) {
    let workload = tiered_visibility
        .workload
        .take()
        .expect("open transaction workload");
    let committed = workload.visible_once_open_commits();
    let high_watermark = workload.log_end_while_open() + 1;
    tiered_visibility.suffix_expected = Some(CommittedRead {
        last_stable_offset: high_watermark,
        visible: committed.clone(),
        aborted: BTreeSet::from([workload.last_abort]),
        batches_past_last_stable_offset: vec![],
        records_without_aborted_list: vec![],
    });
    workload
        .open
        .end(
            &tiered_visibility.partition().client,
            TransactionOutcome::Commit,
        )
        .await;
    tiered_visibility.suffix_read = Some(
        await_committed_read(
            tiered_visibility.partition(),
            tiered_visibility.open_lso.expect("old LSO"),
            tiered_visibility
                .suffix_expected
                .as_ref()
                .expect("committed-tail visibility"),
        )
        .await,
    );
    tiered_visibility
        .after_reader
        .as_mut()
        .expect("after-eviction share reader")
        .acquire_through(
            tiered_visibility
                .partition
                .as_ref()
                .expect("tiered partition"),
            &offsets(&committed),
        )
        .await;
    tiered_visibility.committed_suffix = Some(committed);
}

#[then(
    "the new LSO includes the committed open transaction and plain tail while reporting the late abort"
)]
fn committed_tiered_visibility_tail(tiered_visibility: &TieredVisibility) {
    check!(
        tiered_visibility
            .suffix_read
            .as_ref()
            .expect("committed-tail Fetch")
            == tiered_visibility
                .suffix_expected
                .as_ref()
                .expect("committed-tail visibility")
    );
    let mut expected = acquisitions_of(&tiered_visibility.expected().visible);
    expected.extend(acquisitions_of(
        tiered_visibility
            .committed_suffix
            .as_ref()
            .expect("committed tail"),
    ));
    check!(
        tiered_visibility
            .after_reader
            .as_ref()
            .expect("continued share reader")
            .acquired
            == expected
    );
}

#[scenario(
    path = "tests/features/migrated/tests/tiered_read_committed.feature",
    name = "read committed reads of a transactional range agree across local eviction"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_reads_of_a_transactional_range_agree_across_local_eviction(
    tiered_visibility: TieredVisibility,
) {
    tiered_visibility
        .partition
        .take()
        .expect("tiered partition")
        .broker
        .shutdown()
        .await;
    drop(tiered_visibility);
}
