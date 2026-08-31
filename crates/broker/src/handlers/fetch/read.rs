//! The local log read: the plan a fetch derives under the partition's log
//! mutex, the blocking seek-and-read that serves it, and the response
//! fields the served bytes fill in.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use krabka_log::{Log, Offset};
use krabka_protocol::{
    owned::fetch_response::{AbortedTransaction, PartitionData},
    records::RecordsPayload,
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::runtime::{Handle, RuntimeFlavor};

use super::{FetchWatermarks, VisibilityWindow, compute_visibility_window};
use crate::{codes, error::BrokerError, partition::Partition};

/// Hold the partition's log mutex for a short time to read the offsets, and
/// optionally the verbatim on-disk batch bytes through `Log::read_raw`.
///
/// The read fills `out` in place with a `RecordsPayload::Raw`. It returns the
/// byte-size estimate of the records it put in `out`, or 0 when it put none.
///
/// When `read_committed` is `true`, on a consumer fetch with
/// `isolation_level=1`:
/// - the raw bytes are clamped at `min(lso, hw)`, so
///   `base_offset < min(lso, hw)`
/// - there is NO server-side batch filtering. Aborted batches and control
///   batches stay in the byte stream, and the consumer drops them on the
///   client side with the list below
/// - `out.last_stable_offset` is set to `min(lso, hw)`
/// - `out.aborted_transactions` comes from the partition's `.txnindex` files
///
/// When `is_follower_fetch` is `true`:
/// - the raw bytes go up to LEO, with no HW clamp
/// - `out.high_watermark` and `out.last_stable_offset` are set to `log_end`
///
/// When `read_committed` is `false` and `is_follower_fetch` is `false`, on a
/// consumer fetch in `read_uncommitted`:
/// - the raw bytes are clamped at HW, so `base_offset < hw`
/// - `out.high_watermark` and `out.last_stable_offset` are set to `hw`
/// - `out.aborted_transactions` is `None`
enum ReadPlan {
    OffsetOutOfRange,
    Empty,
    Read {
        limit_offset: Offset,
        effective_lso: Offset,
        read_committed_aborts: bool,
    },
}

pub(super) struct ReadRequest {
    pub(super) topic_id: Option<uuid::Uuid>,
    pub(super) hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
    pub(super) fetch_offset: Offset,
    pub(super) max_bytes: i32,
    pub(super) read_committed: bool,
    pub(super) is_follower_fetch: bool,
    pub(super) sendfile_capable: bool,
    pub(super) sendfile_min_bytes: usize,
}

pub(super) async fn do_read(
    part: &Partition,
    request: ReadRequest,
    out: &mut PartitionData,
) -> Result<usize, BrokerError> {
    let ReadRequest {
        topic_id,
        hot_tail,
        fetch_offset,
        max_bytes,
        read_committed,
        is_follower_fetch,
        sendfile_capable,
        sendfile_min_bytes,
    } = request;
    let hw = part.high_watermark().await;
    let (log_start, w, plan) = plan_read(
        part,
        fetch_offset,
        hw,
        read_committed,
        is_follower_fetch,
        out,
    );
    // Log mutex released here.

    // The cache answers with whole batches, so it takes the window's limit and
    // serves only a batch that ends below it. The high watermark is
    // batch-aligned by construction and never cut one short, but the delivery
    // watermark of KFC-1 is a second cap on the same window, and a cache that
    // ignored it would hand out a batch the log read path would have held back.
    if part.diskless
        && !read_committed
        && let ReadPlan::Read { limit_offset, .. } = &plan
        && let (Some(topic_id), Some(hot_tail)) = (topic_id, hot_tail.as_ref())
        && let Some(bytes) = hot_tail.get(
            topic_id,
            part.index,
            fetch_offset.0,
            limit_offset.0,
            usize::try_from(max_bytes.max(0)).unwrap_or(0),
        )
    {
        return Ok(finish_read(
            out,
            &w,
            log_start,
            read_committed,
            is_follower_fetch,
            Vec::new(),
            Some(RecordsPayload::Raw(bytes)),
        ));
    }

    let (records, aborted_txns): (Option<RecordsPayload>, Vec<AbortedTransaction>) = match plan {
        ReadPlan::OffsetOutOfRange => return Ok(0),
        ReadPlan::Empty => (None, Vec::new()),
        ReadPlan::Read {
            limit_offset,
            effective_lso,
            read_committed_aborts,
        } => {
            let read_max = ByteSize::from_bytes_i64(i64::from(max_bytes.max(0)));
            run_blocking_read(
                &part.log,
                &BlockingRead {
                    fetch_offset,
                    limit_offset,
                    effective_lso,
                    read_max,
                    read_committed_aborts,
                    sendfile_capable,
                    sendfile_min_bytes,
                },
            )
            .await?
        }
    };

    Ok(finish_read(
        out,
        &w,
        log_start,
        read_committed,
        is_follower_fetch,
        aborted_txns,
        records,
    ))
}

/// Everything the blocking half of one partition's read needs, decided by
/// `plan_read` while it held the log mutex and carried across the point where
/// that mutex was released.
struct BlockingRead {
    fetch_offset: Offset,
    limit_offset: Offset,
    effective_lso: Offset,
    read_max: ByteSize,
    read_committed_aborts: bool,
    sendfile_capable: bool,
    sendfile_min_bytes: usize,
}

/// Run the blocking seek-and-read away from normal async polling.
///
/// A fetch reads its partitions one after another, so whatever this hand-off
/// costs, a consumer subscribed to 200 partitions pays 200 times before a
/// single byte goes out. `bench_fetch_handoff` priced the three ways of making
/// it (microseconds per fetch, one 1 KiB record per partition, fastest of five
/// runs):
///
/// | partitions | per-partition `spawn_blocking` | one batched `spawn_blocking` | `block_in_place` |
/// |-----------:|-------------------------------:|-----------------------------:|-----------------:|
/// |          1 |                            6.7 |                          6.3 |              0.6 |
/// |         16 |                          109.7 |                         16.1 |              9.9 |
/// |        200 |                         1324.5 |                        120.2 |            115.9 |
///
/// A task allocation, a queue push, a pool wakeup and a `JoinHandle` await per
/// partition dominate a warm read: dropping them is worth 12x at one partition
/// and 11x at two hundred. Batching the whole pending set into one hand-off
/// buys the same order of magnitude and no more, and never beats keeping the
/// read in place: it is 10x behind at one partition, 1.6x behind at sixteen
/// and level at two hundred -- while it would additionally cost the
/// per-partition remote-tier and diskless fallbacks, which are async and
/// cannot run inside one blocking closure. So the read side lands where the
/// append side did in [`crate::partition_writer`]: `block_in_place`, called
/// once per partition, which is what the arm above measures. Only a call that
/// still holds a worker's core pays for a hand-off, and after the first read
/// in a fetch the calling thread usually does not hold one -- the replacement
/// took it while that read ran. The 200-partition column prices the whole
/// sequence either way.
///
/// What it costs is a thread. `block_in_place` parks the one it runs on and
/// hands that worker's core, with the tasks queued on it, to a replacement
/// taken from the same blocking pool `spawn_blocking` draws on. So a fetch
/// reading 200 partitions holds a thread for the whole read rather than
/// releasing it between partitions, and the broker's `#[tokio::main]` is
/// multi-threaded, so there is a replacement to take the core.
///
/// That pool is the bound, 512 threads by default. While it is saturated the
/// replacement is queued instead of started, and the handed-off core waits for
/// a thread rather than resuming: a read that would merely have queued under
/// `spawn_blocking` costs a unit of runtime parallelism instead. Three things
/// keep that bounded. There is one core per worker thread and a call can only
/// give away the core it holds, so no more than `worker_threads` are ever in
/// flight. The tasks on a handed-off core stay stealable through the
/// scheduler's remote handles, so they are delayed rather than stranded. And
/// reads do not queue on this path, so the wait ends when any in-flight read
/// finishes rather than behind a backlog of them. Bounding it further is one
/// broker-wide admission decision over blocking work rather than a
/// per-call-site one: appends in [`crate::partition_writer`], WAL fsync and
/// trim, and quorum replica IO all draw on the same pool the same way.
///
/// `block_in_place` is also flatly illegal on a current-thread runtime, where
/// it panics rather than degrading, so the current-thread runtimes that
/// `#[tokio::test]` builds keep the `spawn_blocking` path.
async fn run_blocking_read(
    log: &Arc<Mutex<Log>>,
    read: &BlockingRead,
) -> Result<(Option<RecordsPayload>, Vec<AbortedTransaction>), BrokerError> {
    let served = if Handle::current().runtime_flavor() == RuntimeFlavor::MultiThread {
        catch_unwind(AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| read_records(log, read))
        }))
        .map_err(|_| read_task_panicked(&"block_in_place panic"))?
    } else {
        let log = Arc::clone(log);
        let read = BlockingRead { ..*read };
        tokio::task::spawn_blocking(move || read_records(&log, &read))
            .await
            .map_err(|error| read_task_panicked(&error))?
    };
    let (records, aborted) = served?;
    let records = (records.payload_len() > 0).then_some(records);
    Ok((records, aborted))
}

fn read_task_panicked(cause: &impl std::fmt::Display) -> BrokerError {
    BrokerError::Io(std::io::Error::other(format!(
        "fetch read task panicked: {cause}"
    )))
}

/// The blocking body of one partition's read: take the log mutex, seek, and
/// either describe or copy the records run out of it.
fn read_records(
    log: &Mutex<Log>,
    read: &BlockingRead,
) -> Result<(RecordsPayload, Vec<AbortedTransaction>), BrokerError> {
    let &BlockingRead {
        fetch_offset,
        limit_offset,
        effective_lso,
        read_max,
        read_committed_aborts,
        sendfile_capable,
        sendfile_min_bytes,
    } = read;
    let log = log.lock().expect("log mutex poisoned");

    // Zero-copy (Increments D + E): on a plaintext connection
    // (SENDFILE alias: Linux + Apple + FreeBSD/DragonFly), describe
    // the records run with a cheap header-only walk (`read_raw_desc`)
    // instead of `pread`ing the payload. If the run is large enough
    // to amortize the sendfile syscall, return file-backed regions
    // for the `sendfile` drain; otherwise fall back to the byte-copy
    // `read_raw` path (small/fragmented fetches stay on the vectored
    // path). The descriptor is captured here under the log lock so
    // retention can't truncate the region out from under the later
    // async send (the `Arc<File>` pins the inode).
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    let records: RecordsPayload = {
        let mut chosen: Option<RecordsPayload> = None;
        // READ_COMMITTED responses also carry aborted-transaction
        // metadata and are consumed by ordinary Kafka clients as one
        // framed response. Keep those on the raw-byte encoder; the
        // file-region writer can otherwise detach the records payload
        // from the metadata frame and leave a fresh stable-topic reader
        // with HW/LSO but no decoded batches.
        if sendfile_capable && !read_committed_aborts {
            let desc = log.read_raw_desc(fetch_offset, limit_offset, read_max)?;
            if should_use_sendfile(desc.total, !desc.regions.is_empty(), sendfile_min_bytes) {
                chosen = Some(RecordsPayload::FileRegions(desc.regions));
            }
        }
        match chosen {
            Some(p) => p,
            None => RecordsPayload::Raw(log.read_raw(fetch_offset, limit_offset, read_max)?.bytes),
        }
    };
    // Windows fallback: no safe `sendfile`/`TransmitFile`, so always
    // `read_raw` + copy (the Increment C vectored path drains it).
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    )))]
    let records: RecordsPayload = {
        // Both sendfile inputs are consumed only by the platform
        // branch above, so discard them here or `-D warnings` fails
        // the build on this target.
        let _ = (sendfile_capable, sendfile_min_bytes);
        RecordsPayload::Raw(log.read_raw(fetch_offset, limit_offset, read_max)?.bytes)
    };

    // read_committed does NO server-side batch filtering: verbatim
    // bytes (including aborted/control batches) are returned and the
    // consumer drops them client-side via `aborted_transactions`,
    // matching Apache Kafka's behavior. Skip the Vec allocation
    // entirely when there are no aborted txns in range.
    let aborted = if read_committed_aborts {
        let mut it = log
            .aborted_in_range(fetch_offset, effective_lso)
            .into_iter();
        if let Some(first) = it.next() {
            let mut v = vec![AbortedTransaction {
                // Unwrap the log-layer `ProducerId` into the wire `i64` field.
                producer_id: first.producer_id.get(),
                // Unwrap the log-layer `Offset` into the wire `i64` field.
                first_offset: first.start_offset.0,
                ..Default::default()
            }];
            v.extend(it.map(|e| AbortedTransaction {
                producer_id: e.producer_id.get(),
                first_offset: e.start_offset.0,
                ..Default::default()
            }));
            v
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Ok::<_, BrokerError>((records, aborted))
}

fn finish_read(
    response: &mut PartitionData,
    window: &VisibilityWindow,
    log_start: Offset,
    read_committed: bool,
    follower_fetch: bool,
    aborted: Vec<AbortedTransaction>,
    records: Option<RecordsPayload>,
) -> usize {
    response.error_code = codes::NONE;
    response.high_watermark = window.response_hw.0;
    response.log_start_offset = log_start.0;
    response.last_stable_offset = window.response_lso.0;
    if read_committed && !follower_fetch {
        response.aborted_transactions = Some(aborted);
    }
    let bytes = records.as_ref().map_or(0, RecordsPayload::payload_len);
    response.records = records;
    bytes
}

/// KFC-1's delivery watermark for this fetch, capped at the high watermark.
///
/// The value is recomputed here, under the log mutex the fetch already holds,
/// rather than read from the partition's lock-free mirror. The mirror is only
/// as fresh as the last append or the last scheduler tick, and a fetch that
/// caps itself on a stale value either holds back a batch that has come due or,
/// after a truncation, serves one that has not. The delivery scheduler exists
/// for liveness; this call is what makes a fetch correct.
///
/// The clamp is the precondition the verified kernel asks of its caller, and it
/// is a real bound as well: the window may never expose beyond the high
/// watermark. The lower end needs no clamp, because
/// [`Log::advance_delivery_watermark`] already answers inside
/// `[log_start_offset(), log_end_offset()]`.
///
/// A follower fetch is never gated, so it does no work here. A scheduled record
/// replicates, and counts toward the ISR and the high watermark, long before
/// any consumer may see it. The high watermark is what the kernel wants in that
/// case: the kernel ignores the value, and this keeps its precondition true.
///
/// A topic that delivers immediately answers the log end offset before it reads
/// a single batch header, so the clamp gives the high watermark straight back
/// and the read path reaches `sendfile` exactly where it does today.
///
/// `now_ms` comes from the partition's own delivery clock rather than from the
/// system clock directly, so an append, the scheduler and a fetch all read one
/// timeline. That is what lets a test drive a mock clock across an activation
/// boundary and assert what a fetch returns on each side of it.
fn deliverable_offset(
    log: &mut Log,
    high_watermark: Offset,
    follower_fetch: bool,
    now_ms: i64,
) -> Offset {
    if follower_fetch {
        return high_watermark;
    }
    log.advance_delivery_watermark(now_ms)
        .watermark
        .min(high_watermark)
}

fn plan_read(
    partition: &Partition,
    fetch_offset: Offset,
    high_watermark: Offset,
    read_committed: bool,
    follower_fetch: bool,
    response: &mut PartitionData,
) -> (Offset, VisibilityWindow, ReadPlan) {
    let mut log = partition.log.lock().expect("log mutex poisoned");
    let log_start = log.log_start_offset();
    let log_end = log.log_end_offset();
    let deliverable = deliverable_offset(
        &mut log,
        high_watermark,
        follower_fetch,
        partition.delivery.now_ms(),
    );
    let window = compute_visibility_window(
        follower_fetch,
        read_committed,
        FetchWatermarks {
            log_start,
            hw: high_watermark,
            lso: log.lso(),
            log_end,
            deliverable,
        },
        fetch_offset,
    );
    let plan = if window.out_of_range {
        response.error_code = codes::OFFSET_OUT_OF_RANGE;
        response.log_start_offset = log_start.0;
        response.high_watermark = window.response_hw.0;
        response.last_stable_offset = window.response_lso.0;
        ReadPlan::OffsetOutOfRange
    } else if window.empty {
        ReadPlan::Empty
    } else {
        ReadPlan::Read {
            limit_offset: window.limit_offset,
            effective_lso: window.effective_lso,
            read_committed_aborts: window.read_committed_aborts,
        }
    };
    (log_start, window, plan)
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn should_use_sendfile(total_bytes: usize, has_regions: bool, minimum_bytes: usize) -> bool {
    total_bytes >= minimum_bytes && has_regions
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use bytes::Bytes;
    use krabka_log::{DeliveryPolicy, Log, LogConfig, Offset};
    use krabka_protocol::{
        owned::fetch_response::{AbortedTransaction, PartitionData},
        records::{Attributes, Record, RecordBatch, RecordsPayload},
    };
    use krabka_units::prelude::mebibytes;
    use qubit_clock::{Clock as _, SystemClock};

    /// The read budget for the hand-off test: larger than the log it reads, so
    /// the served bytes are the whole batch under either runtime flavor.
    const UNBOUNDED: krabka_units::ByteSize = mebibytes(1);

    /// The producer that opens and aborts the transaction the
    /// `read_committed` case reads back.
    const PID: i64 = 1000;

    /// One batch of two records in a fresh log directory. The directory is
    /// returned because dropping it deletes the segments underneath the log.
    fn two_record_log() -> (tempfile::TempDir, Log) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        log.append(&mut RecordBatch {
            last_offset_delta: 1,
            records: vec![
                Record {
                    offset_delta: 0,
                    value: Some(Bytes::from_static(b"first")),
                    ..Record::default()
                },
                Record {
                    offset_delta: 1,
                    value: Some(Bytes::from_static(b"second")),
                    ..Record::default()
                },
            ],
            ..RecordBatch::default()
        })
        .expect("append the batch under test");
        (dir, log)
    }

    /// The plain read the cases below vary: everything from offset 0 to
    /// `limit`, no byte budget in the way, no sendfile and no `read_committed`
    /// bookkeeping.
    fn whole_log_read(limit: Offset) -> super::BlockingRead {
        super::BlockingRead {
            fetch_offset: Offset(0),
            limit_offset: limit,
            effective_lso: limit,
            read_max: UNBOUNDED,
            read_committed_aborts: false,
            sendfile_capable: false,
            sendfile_min_bytes: 0,
        }
    }

    /// [`super::run_blocking_read`] picks its hand-off from the runtime
    /// flavor: `block_in_place` on the multi-threaded runtime the broker runs
    /// on, and `spawn_blocking` on a current-thread runtime, where
    /// `block_in_place` panics outright rather than degrading. Both arms have
    /// to serve exactly the bytes a direct read serves.
    #[test]
    fn either_runtime_flavor_serves_the_same_bytes() {
        let (_dir, log) = two_record_log();
        let limit = log.log_end_offset();
        let log = Arc::new(Mutex::new(log));
        let read = whole_log_read(limit);
        let expected = (
            Some(RecordsPayload::Raw(
                log.lock()
                    .expect("log mutex poisoned")
                    .read_raw(Offset(0), limit, UNBOUNDED)
                    .expect("the test log holds the range it is asked for")
                    .bytes,
            )),
            Vec::new(),
        );

        let served = |runtime: tokio::runtime::Runtime| {
            runtime
                .block_on(super::run_blocking_read(&log, &read))
                .expect("the blocking read succeeds")
        };
        let current_thread = served(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("build a current-thread runtime"),
        );
        let multi_thread = served(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .build()
                .expect("build a multi-threaded runtime"),
        );

        assert!(current_thread == expected);
        assert!(multi_thread == expected);
    }

    #[test]
    fn sendfile_eligibility_honors_nondefault_threshold() {
        assert!(super::should_use_sendfile(64, true, 64));
        assert!(!super::should_use_sendfile(63, true, 64));
        assert!(!super::should_use_sendfile(64, false, 64));
    }

    /// On a plaintext connection [`super::read_records`] describes the records
    /// run for the `sendfile` drain instead of `pread`ing it, but only once
    /// the run is long enough to pay for the syscall. Below the threshold it
    /// falls back to the byte copy, and both answers carry the same bytes.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    #[test]
    fn a_sendfile_capable_read_describes_the_run_only_above_the_threshold() {
        let (_dir, log) = two_record_log();
        let limit = log.log_end_offset();
        let raw = log
            .read_raw(Offset(0), limit, UNBOUNDED)
            .expect("the test log holds the range it is asked for")
            .bytes;
        let log = Mutex::new(log);

        let described = super::read_records(
            &log,
            &super::BlockingRead {
                sendfile_capable: true,
                ..whole_log_read(limit)
            },
        )
        .expect("the blocking read succeeds");
        let copied = super::read_records(
            &log,
            &super::BlockingRead {
                sendfile_capable: true,
                sendfile_min_bytes: raw.len() + 1,
                ..whole_log_read(limit)
            },
        )
        .expect("the blocking read succeeds");

        assert!(let RecordsPayload::FileRegions(_) = &described.0);
        assert!(described.0.payload_len() == raw.len());
        assert!(described.1.is_empty());
        assert!(copied == (RecordsPayload::Raw(raw), Vec::new()));
    }

    /// A `read_committed` fetch does no server-side filtering: it serves the
    /// verbatim bytes, aborted batches included, and reports the aborted
    /// ranges beside them so the consumer can drop those client-side. The same
    /// read without the flag reports none, and neither answer moves a byte.
    #[test]
    fn read_committed_reports_the_aborted_ranges_beside_the_same_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        log.append(&mut transactional_batch(PID))
            .expect("append the transaction's one data batch");
        log.append(&mut abort_marker(PID))
            .expect("append the abort marker that closes it");
        let limit = log.log_end_offset();
        let raw = log
            .read_raw(Offset(0), limit, UNBOUNDED)
            .expect("the test log holds the range it is asked for")
            .bytes;
        let log = Mutex::new(log);

        let read_committed = super::read_records(
            &log,
            &super::BlockingRead {
                read_committed_aborts: true,
                ..whole_log_read(limit)
            },
        )
        .expect("the blocking read succeeds");
        let read_uncommitted =
            super::read_records(&log, &whole_log_read(limit)).expect("the blocking read succeeds");

        assert!(
            read_committed
                == (
                    RecordsPayload::Raw(raw.clone()),
                    vec![AbortedTransaction {
                        producer_id: PID,
                        first_offset: 0,
                        ..AbortedTransaction::default()
                    }],
                )
        );
        assert!(read_uncommitted == (RecordsPayload::Raw(raw), Vec::new()));
    }

    /// One transactional data record from `pid`, the batch that opens the
    /// transaction on this partition. `Log::append` rewrites the offsets.
    fn transactional_batch(pid: i64) -> RecordBatch {
        RecordBatch {
            producer_id: pid,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                offset_delta: 0,
                value: Some(Bytes::from_static(b"in a transaction")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// `pid`'s abort control batch: a control record whose 4-byte key is
    /// (version=0: i16, `marker_type`=0: i16) big-endian, with the
    /// coordinator epoch in its value. Appending it writes the transaction's
    /// offset range into the partition's `.txnindex`.
    fn abort_marker(pid: i64) -> RecordBatch {
        let mut key = [0u8; 4];
        key[2..4].copy_from_slice(&0i16.to_be_bytes());
        let mut value = [0u8; 6];
        value[2..6].copy_from_slice(&17i32.to_be_bytes());
        RecordBatch {
            producer_id: pid,
            attributes: Attributes::default()
                .with_transactional(true)
                .with_control(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(&key)),
                value: Some(Bytes::copy_from_slice(&value)),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// A consumer's `read_uncommitted` request for everything from
    /// `fetch_offset`, with a byte budget larger than the log.
    fn consumer_request(fetch_offset: i64) -> super::ReadRequest {
        super::ReadRequest {
            topic_id: None,
            hot_tail: None,
            fetch_offset: Offset(fetch_offset),
            max_bytes: 1 << 20,
            read_committed: false,
            is_follower_fetch: false,
            sendfile_capable: false,
            sendfile_min_bytes: 0,
        }
    }

    /// [`super::do_read`] hands the bounds its plan decided to the blocking
    /// read and fills the response from what came back. The three cases are
    /// the three shapes the plan has: a range to serve, nothing to serve
    /// because the fetch sits at the high watermark, and an offset outside the
    /// log, which reports `OFFSET_OUT_OF_RANGE` and serves nothing.
    #[tokio::test]
    async fn do_read_serves_the_window_its_plan_decided() {
        let (partition, _dir) =
            crate::partition::test_support::test_partition(Arc::new(tokio::sync::Notify::new()));
        let (limit, records) = {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            log.append(&mut RecordBatch {
                last_offset_delta: 1,
                records: vec![
                    Record {
                        offset_delta: 0,
                        value: Some(Bytes::from_static(b"first")),
                        ..Record::default()
                    },
                    Record {
                        offset_delta: 1,
                        value: Some(Bytes::from_static(b"second")),
                        ..Record::default()
                    },
                ],
                ..RecordBatch::default()
            })
            .expect("append the batch under test");
            let limit = log.log_end_offset();
            let records = log
                .read_raw(Offset(0), limit, UNBOUNDED)
                .expect("the test log holds the range it is asked for")
                .bytes;
            (limit, records)
        };
        partition.replica_state.lock().await.hw = limit;

        let served = PartitionData {
            error_code: crate::codes::NONE,
            high_watermark: limit.0,
            last_stable_offset: limit.0,
            log_start_offset: 0,
            records: Some(RecordsPayload::Raw(records.clone())),
            ..PartitionData::default()
        };
        let cases = [
            ("the whole log", 0, records.len(), served.clone()),
            (
                "nothing left to serve",
                limit.0,
                0,
                PartitionData {
                    records: None,
                    ..served.clone()
                },
            ),
            (
                "past the high watermark",
                limit.0 + 1,
                0,
                PartitionData {
                    records: None,
                    ..served
                },
            ),
        ];

        for (name, fetch_offset, expected_bytes, expected) in cases {
            let mut out = PartitionData::default();
            let bytes = super::do_read(&partition, consumer_request(fetch_offset), &mut out)
                .await
                .expect("the read succeeds");
            assert!(bytes == expected_bytes, "{name}");
            assert!(out == expected, "{name}");
        }
    }

    /// The one plan shape that serves nothing and still reports an error: a
    /// fetch below the log start, which is where a consumer resuming from an
    /// offset retention has already deleted lands. The response carries the
    /// bounds the consumer needs to reset itself, and no records.
    #[tokio::test]
    async fn do_read_reports_a_fetch_below_the_log_start_out_of_range() {
        let (partition, _dir) =
            crate::partition::test_support::test_partition(Arc::new(tokio::sync::Notify::new()));
        let limit = {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            log.append(&mut RecordBatch {
                records: vec![Record {
                    offset_delta: 0,
                    value: Some(Bytes::from_static(b"deleted by retention")),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            })
            .expect("append the batch retention then deletes");
            let limit = log.log_end_offset();
            log.trim_to_offset(limit)
                .expect("trim the whole log away from under the fetch");
            limit
        };
        partition.replica_state.lock().await.hw = limit;

        let mut out = PartitionData::default();
        let bytes = super::do_read(&partition, consumer_request(0), &mut out)
            .await
            .expect("the read succeeds");

        assert!(bytes == 0);
        assert!(
            out == PartitionData {
                error_code: crate::codes::OFFSET_OUT_OF_RANGE,
                high_watermark: limit.0,
                last_stable_offset: limit.0,
                log_start_offset: limit.0,
                records: None,
                ..PartitionData::default()
            }
        );
    }

    /// A log holding one batch per entry of `activations`, two records each.
    fn scheduled_log(policy: DeliveryPolicy, activations: &[i64]) -> (tempfile::TempDir, Log) {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            delivery_policy: policy,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).expect("open log");
        for activation_ms in activations {
            log.append(&mut crate::delivery::test_support::batch_at(*activation_ms))
                .expect("append a batch");
        }
        (dir, log)
    }

    #[test]
    fn a_consumer_is_capped_at_the_delivery_watermark_and_a_follower_is_not() {
        let now_ms = SystemClock::new().millis();
        // Two batches of two records: [0, 1] is due, [2, 3] is an hour out.
        let (_dir, mut log) = scheduled_log(
            DeliveryPolicy::Scheduled,
            &[now_ms - 3_600_000, now_ms + 3_600_000],
        );

        assert!(super::deliverable_offset(&mut log, Offset(4), false, now_ms) == Offset(2));
        // Replication is not gated: the follower reads the whole log.
        assert!(super::deliverable_offset(&mut log, Offset(4), true, now_ms) == Offset(4));
    }

    #[test]
    fn an_immediate_topic_is_capped_at_the_high_watermark_alone() {
        let now_ms = SystemClock::new().millis();
        let (_dir, mut log) = scheduled_log(
            DeliveryPolicy::Immediate,
            &[now_ms + 3_600_000, now_ms + 7_200_000],
        );

        // Nothing is held back by time, and the high watermark still caps the
        // window below the log end.
        assert!(super::deliverable_offset(&mut log, Offset(3), false, now_ms) == Offset(3));
        assert!(super::deliverable_offset(&mut log, Offset(4), false, now_ms) == Offset(4));
    }
}
