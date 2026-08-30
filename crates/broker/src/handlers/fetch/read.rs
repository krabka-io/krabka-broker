//! The local log read: the plan a fetch derives under the partition's log
//! mutex, the blocking seek-and-read that serves it, and the response
//! fields the served bytes fill in.

use std::sync::Arc;

use krabka_log::{Log, Offset};
use krabka_protocol::{
    owned::fetch_response::{AbortedTransaction, PartitionData},
    records::RecordsPayload,
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

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
            // Run the blocking seek+read (and, for read_committed, the
            // aborted-txn index scan) off the reactor thread. The lock is
            // re-acquired inside the closure for the brief duration of the
            // syscalls.
            let log = part.log.clone();
            let join = tokio::task::spawn_blocking(move || {
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
                        if should_use_sendfile(
                            desc.total,
                            !desc.regions.is_empty(),
                            sendfile_min_bytes,
                        ) {
                            chosen = Some(RecordsPayload::FileRegions(desc.regions));
                        }
                    }
                    match chosen {
                        Some(p) => p,
                        None => RecordsPayload::Raw(
                            log.read_raw(fetch_offset, limit_offset, read_max)?.bytes,
                        ),
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
            });
            await_blocking_read(join).await?
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

async fn await_blocking_read(
    join: tokio::task::JoinHandle<Result<(RecordsPayload, Vec<AbortedTransaction>), BrokerError>>,
) -> Result<(Option<RecordsPayload>, Vec<AbortedTransaction>), BrokerError> {
    let (records, aborted) = join.await.map_err(|error| {
        BrokerError::Io(std::io::Error::other(format!(
            "fetch read task panicked: {error}"
        )))
    })??;
    let records = (records.payload_len() > 0).then_some(records);
    Ok((records, aborted))
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
    use assert2::assert;
    use krabka_log::{DeliveryPolicy, Log, LogConfig, Offset};
    use qubit_clock::{Clock as _, SystemClock};

    #[test]
    fn sendfile_eligibility_honors_nondefault_threshold() {
        assert!(super::should_use_sendfile(64, true, 64));
        assert!(!super::should_use_sendfile(63, true, 64));
        assert!(!super::should_use_sendfile(64, false, 64));
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
