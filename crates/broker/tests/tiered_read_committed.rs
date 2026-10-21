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

use assert2::{assert, check};
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
async fn await_committed_read(tiered: &TieredPartition, from: i64, want: &CommittedRead) {
    let deadline = Instant::now() + MARKER_DEADLINE;
    // intentional: `EndTxn` answers before its markers are written, and the
    // last stable offset is the only signal that they were.
    loop {
        let read = read_committed_fetch(tiered, from).await;
        if read == *want {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the read from {from} stayed {read:?}, not {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_reads_of_a_transactional_range_agree_across_local_eviction() {
    let tiered = start_tiered_partition().await;
    let workload = write_workload(&tiered.client, TOPIC, tiered.topic_id).await;
    let lso = workload.open_first_offset;
    let expected = committed_below_open(&workload);
    let expected_acquisitions = acquisitions_of(&expected.visible);
    await_committed_read(&tiered, 0, &expected).await;

    // Both tiers hold `[0, tiered_end)`: the copy pass ran, retention did not.
    let tiered_end = tiered.copy_bound(lso);
    tiered.await_tiered_through(tiered_end).await;
    // The tier holds data of `a-last`, whose abort marker is local only.
    assert!(
        workload.last_abort.1 < tiered_end,
        "a-last starts at {}, the tier ends at {tiered_end}",
        workload.last_abort.1
    );
    check!(tiered.local_log_start().await == 0);
    let before = read_committed_fetch(&tiered, 0).await;
    let mut before_eviction = ShareReader::join(&tiered, "rc-before-eviction").await;
    before_eviction
        .acquire_through(&tiered, &offsets(&expected.visible))
        .await;

    // Only the tier holds `[0, tiered_end)`.
    tiered.evict_local_segments(tiered_end).await;
    let after = read_committed_fetch(&tiered, 0).await;
    let mut after_eviction = ShareReader::join(&tiered, "rc-after-eviction").await;
    after_eviction
        .acquire_through(&tiered, &offsets(&expected.visible))
        .await;
    let local_start_after_reads = tiered.local_log_start().await;

    check!(before == expected);
    check!(after == expected);
    check!(before_eviction.acquired == expected_acquisitions);
    check!(after_eviction.acquired == expected_acquisitions);
    // The reads after eviction came from the tier: the local log started at
    // `tiered_end` throughout, and most of what they saw lies below it.
    check!(local_start_after_reads == tiered_end);
    let remote_only = offsets(&expected.visible).range(..tiered_end).count();
    check!(
        remote_only * 2 > expected.visible.len(),
        "{remote_only} of {} below {tiered_end}",
        expected.visible.len()
    );
    check!(
        tiered.broker.high_watermark_for_test(TOPIC, 0).await
            == Some(workload.log_end_while_open())
    );

    // Committing the open transaction moves the LSO past it: its records and
    // the plain records after it become visible, and the `a-last` abort,
    // whose marker lies past the old LSO, is reported for that range.
    let last_abort = workload.last_abort;
    let committed = workload.visible_once_open_commits();
    // The commit marker is the last offset.
    let high_watermark = workload.log_end_while_open() + 1;
    workload
        .open
        .end(&tiered.client, TransactionOutcome::Commit)
        .await;
    let after_commit = CommittedRead {
        last_stable_offset: high_watermark,
        visible: committed.clone(),
        aborted: BTreeSet::from([last_abort]),
        batches_past_last_stable_offset: Vec::new(),
        records_without_aborted_list: Vec::new(),
    };
    await_committed_read(&tiered, lso, &after_commit).await;
    after_eviction
        .acquire_through(&tiered, &offsets(&committed))
        .await;
    let mut all = expected_acquisitions;
    all.extend(acquisitions_of(&committed));
    check!(after_eviction.acquired == all);

    tiered.broker.shutdown().await;
}
