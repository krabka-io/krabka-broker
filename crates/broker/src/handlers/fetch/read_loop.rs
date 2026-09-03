//! The read loop: every planned partition is read once, and a fetch that did
//! not reach `min_bytes` parks on the partitions' notifiers, reading a
//! partition again only when its own notifier fires, until the accumulated
//! bytes clear the floor, an epoch check fails, or `max_wait_ms` runs out.

use std::{sync::Arc, time::Duration};

use krabka_log::Offset;
use krabka_protocol::owned::fetch_response::{FetchableTopicResponse, PartitionData};
use krabka_units::convert::ByteSizeExt as _;
use tokio::sync::Notify;

use super::{
    plan::{PendingRead, apply_epoch_checks},
    read::{ReadRequest, do_read},
    remote::try_remote_read,
    request::EffectivePartition,
    response::group_into_topic_responses,
};
use crate::{broker::Broker, codes, error::BrokerError, partition::Partition};

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = Woken> + Send>>;

/// The waiter that fired: which pending read it belongs to, and the notifier
/// it parked on, so the poll loop can arm a replacement on the same notifier
/// before it reads.
struct Woken {
    pending: usize,
    notify: Arc<Notify>,
}

/// What a fetch that has already read once needs in order to decide whether to
/// keep parking.
struct LongPollState {
    /// The `min_bytes` floor of the request, clamped at zero. Kafka treats
    /// `fetch.min.bytes` as a contract and not as a hint: the fetch is held
    /// until this many bytes are readable across its partitions or the wait
    /// expires.
    min_bytes: usize,
    max_wait_ms: i32,
    sendfile_capable: bool,
    /// Bytes the latest read of each pending entry produced, indexed like
    /// `pending`. A re-read replaces its entry rather than adding to it,
    /// because the re-read replaces the records too.
    bytes: Vec<usize>,
    /// `true` where the cold tier, and not the local log, answered the entry.
    cold_served: Vec<bool>,
}

impl LongPollState {
    fn total(&self) -> usize {
        self.bytes.iter().sum()
    }
}

/// Arms one waiter on `notify` and tags it with the pending entry it belongs
/// to.
///
/// The waiter registers here rather than on its first poll. Every producer
/// path signals with `notify_waiters`, which wakes only the waiters already
/// registered and leaves no permit behind, so a waiter armed after the read
/// pass would miss an append that landed during it and would then sleep out
/// the whole `max_wait_ms`.
fn arm_wait(pending: usize, notify: Arc<Notify>) -> WaitFut {
    let mut notified = Box::pin(Arc::clone(&notify).notified_owned());
    notified.as_mut().enable();
    Box::pin(async move {
        notified.await;
        Woken { pending, notify }
    })
}

/// Arms every planned partition's waiters, before any of them is read.
///
/// A fetch that then finds enough bytes on the first pass drops the whole set
/// unused, so a wake set costs one waiter-list insertion and one removal per
/// notifier whether or not the fetch parks. That is the price of the guarantee
/// -- registering after the read is what loses an append -- and it is small
/// beside the per-partition log read the same pass makes. Only a fetch that
/// asked to wait pays it at all: `max_wait_ms == 0` arms nothing.
fn arm_waits(pending: &[PendingRead]) -> Vec<WaitFut> {
    let mut waits = Vec::new();
    for (index, read) in pending.iter().enumerate() {
        let Some(part) = read.partition.as_ref() else {
            continue;
        };
        waits.push(arm_wait(index, part.append_notify.clone()));
        // KIP-392: a consumer reading from a follower becomes unblocked
        // when the follower's HW advances (via set_follower_hw), not only
        // on raw append. Follower (inter-broker) fetches don't need this.
        //
        // KFC-1: a consumer parked exactly at the delivery watermark is
        // waiting for time to pass and not for bytes to arrive. Nothing
        // appends and the HW does not move when the batch it wants comes
        // due, so the delivery advance is its own wake. Without it the
        // consumer sleeps out its whole long poll in the one case the
        // feature exists for.
        if !read.is_follower_fetch {
            waits.push(arm_wait(index, part.hw_advance_notify.clone()));
            waits.push(arm_wait(index, part.delivery.advance_notify.clone()));
        }
    }
    waits
}

/// Read every planned partition once, then long-poll until the fetch reaches
/// `min_bytes` or runs out of wait.
///
/// The loop is sequential, so the cost of getting one partition's blocking
/// read off the reactor is paid once per partition before any bytes go out: a
/// consumer subscribed to 200 partitions pays it 200 times. `do_read` makes
/// that hand-off with `block_in_place` rather than a `spawn_blocking` per
/// partition, which `bench_fetch_handoff` measured at a tenth of the cost;
/// [`super::read::run_blocking_read`] carries the numbers and the trade.
///
/// The per-partition step stays a step, because the cold-tier fallback a
/// partition falls through to -- [`serve_from_cold_tier`], covering the
/// remote tier and diskless -- is async and cannot run inside one blocking
/// closure covering the whole pending set.
pub(super) async fn execute_pending_reads(
    broker: &Broker,
    mut pending: Vec<PendingRead>,
    min_bytes: i32,
    max_wait_ms: i32,
    sendfile_capable: bool,
    phases: &crate::metrics::RequestPhases,
) -> Result<(Vec<FetchableTopicResponse>, Vec<Vec<u64>>), BrokerError> {
    let mut state = LongPollState {
        min_bytes: usize::try_from(min_bytes.max(0)).unwrap_or(0),
        max_wait_ms,
        sendfile_capable,
        bytes: vec![0; pending.len()],
        cold_served: vec![false; pending.len()],
    };
    // Arm the long poll's waiters before the first read pass, so that an
    // append landing between a partition's read and the park cannot be lost.
    // Kafka closes the same window by re-running `tryComplete` right after it
    // registers the watch (`tryCompleteElseWatch`).
    let waits = if max_wait_ms > 0 {
        arm_waits(&pending)
    } else {
        Vec::new()
    };
    for (index, read) in pending.iter_mut().enumerate() {
        let Some(partition) = read.partition.clone() else {
            continue;
        };
        let started = std::time::Instant::now();
        state.bytes[index] = do_read(
            &partition,
            ReadRequest {
                topic_id: Some(uuid::Uuid::from_bytes(read.topic_id.0)),
                hot_tail: Some(broker.hot_tail.clone()),
                fetch_offset: Offset(read.fetch_offset),
                max_bytes: read.max_bytes,
                read_committed: read.read_committed,
                is_follower_fetch: read.is_follower_fetch,
                sendfile_capable,
                sendfile_min_bytes: broker.config.sendfile_min.bytes_usize(),
            },
            &mut read.out,
        )
        .await?;
        read.cpu_micros = read
            .cpu_micros
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        // The same interval, on the request's local phase. The two accounts
        // differ in what they roll up to: `cpu_micros` is per partition and
        // feeds the rebalancer, while the phase is per request and is the
        // Fetch half of `request_local_duration_seconds`.
        phases.add_local(started.elapsed());
        let cold = serve_from_cold_tier(broker, read, &partition, phases).await;
        state.cold_served[index] = cold > 0;
        state.bytes[index] += cold;
    }
    if state.total() < state.min_bytes && max_wait_ms > 0 {
        long_poll_then_reread(broker, &mut pending, waits, &mut state, phases).await?;
    }
    Ok(group_into_topic_responses(pending))
}

/// Park on the armed waiters until the fetch has `min_bytes` to answer with,
/// an epoch check fences one of its partitions, or `max_wait_ms` expires.
///
/// Each wake reads only the partition whose notifier fired: the rest keep the
/// records their last read gave them, which is what lets the wait accumulate
/// across several appends instead of restarting on each one. The single
/// deadline is the whole request's, so a fetch that wakes ten times still
/// answers within its `max_wait_ms`.
// cargo-mutants: long-poll serve-loop glue -- parks on partition append/HW
// notifiers, then replays `do_read` for the partition that woke it. The live
// fetch integration suite covers notifier-driven re-reads; the focused unit
// tests below cover the `min_bytes` floor, the append that lands before the
// park, and epoch revalidation after the wait.
#[cfg_attr(test, mutants::skip)]
async fn long_poll_then_reread(
    broker: &Broker,
    pending: &mut [PendingRead],
    mut waits: Vec<WaitFut>,
    state: &mut LongPollState,
    phases: &crate::metrics::RequestPhases,
) -> Result<(), BrokerError> {
    let max_wait = Duration::from_millis(u64::from(u32::try_from(state.max_wait_ms).unwrap_or(0)));
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        if revalidate_epochs(broker, pending) || state.total() >= state.min_bytes {
            return Ok(());
        }
        if waits.is_empty() {
            return Ok(());
        }
        // The park is the Fetch remote phase: this broker has read everything
        // it holds and is waiting for someone else to append, so the time
        // belongs beside the Produce `acks=all` gate and not beside the local
        // read.
        let parked = std::time::Instant::now();
        let outcome =
            tokio::time::timeout_at(deadline, futures_util::future::select_all(waits)).await;
        phases.add_remote(parked.elapsed());
        let Ok((woken, _fired, rest)) = outcome else {
            // The deadline passed. Nothing more will be read, but the loop
            // head still revalidates the epochs before the response goes out.
            waits = Vec::new();
            continue;
        };
        waits = rest;
        // Arm the replacement before the read and not after it, for the reason
        // `arm_wait` gives: an append landing while this read runs would
        // otherwise leave nothing registered to catch it.
        waits.push(arm_wait(woken.pending, woken.notify));
        reread_woken(broker, pending, woken.pending, state, phases).await?;
    }
}

/// Re-run the leader-epoch and divergence checks the plan applied before the
/// first read, and replace the response of every partition they now fence.
///
/// This is the half of Kafka's `DelayedFetch.tryComplete` that costs no I/O:
/// an epoch that moved while the fetch was parked completes the fetch whatever
/// the accumulated byte count is. A partition that still passes keeps the
/// records its last read gave it.
fn revalidate_epochs(broker: &Broker, pending: &mut [PendingRead]) -> bool {
    let image = broker.controller.current_image();
    let mut fenced = false;
    for read in pending.iter_mut() {
        let Some(part) = read.partition.clone() else {
            continue;
        };
        let request = EffectivePartition {
            partition: read.partition_index,
            current_leader_epoch: read.current_leader_epoch,
            last_fetched_epoch: read.last_fetched_epoch,
            fetch_offset: read.fetch_offset,
            partition_max_bytes: read.max_bytes,
        };
        let mut fresh = PartitionData {
            partition_index: read.partition_index,
            ..Default::default()
        };
        if apply_epoch_checks(
            &image,
            &read.topic_name,
            read.partition_index,
            &request,
            &part,
            &mut fresh,
        ) {
            read.out = fresh;
            fenced = true;
        }
    }
    fenced
}

/// Read the one partition whose notifier fired, replacing both its response
/// and its contribution to the accumulated byte count.
async fn reread_woken(
    broker: &Broker,
    pending: &mut [PendingRead],
    index: usize,
    state: &mut LongPollState,
    phases: &crate::metrics::RequestPhases,
) -> Result<(), BrokerError> {
    let Some(read) = pending.get_mut(index) else {
        return Ok(());
    };
    let Some(part) = read.partition.clone() else {
        return Ok(());
    };
    // A partition the cold tier already answered keeps that answer. The local
    // log does not hold the offset -- that is what sent it to the tier in the
    // first place -- so a re-read would trade a served batch for another
    // object-store round trip.
    if state.cold_served[index] {
        return Ok(());
    }
    read.out = PartitionData {
        partition_index: read.partition_index,
        ..Default::default()
    };
    // Time the re-read so its duration accumulates into the same
    // per-partition CPU counter as the first pass (wall-clock delta;
    // see the first-pass comment for why this replaces TaskMonitor).
    let read_start = std::time::Instant::now();
    let bytes = do_read(
        &part,
        ReadRequest {
            topic_id: Some(uuid::Uuid::from_bytes(read.topic_id.0)),
            hot_tail: Some(broker.hot_tail.clone()),
            // Wrap the decoded-request wire offset into `Offset` for the read.
            fetch_offset: Offset(read.fetch_offset),
            max_bytes: read.max_bytes,
            read_committed: read.read_committed,
            is_follower_fetch: read.is_follower_fetch,
            sendfile_capable: state.sendfile_capable,
            sendfile_min_bytes: broker.config.sendfile_min.bytes_usize(),
        },
        &mut read.out,
    )
    .await?;
    let micros = u64::try_from(read_start.elapsed().as_micros()).unwrap_or(u64::MAX);
    read.cpu_micros = read.cpu_micros.saturating_add(micros);
    // The re-read is local work like the first pass, so it accumulates on
    // the same phase.
    phases.add_local(read_start.elapsed());

    // The partition may have aged past this offset while the fetch was parked,
    // in which case the cold tier is where the records now are.
    let cold = serve_from_cold_tier(broker, read, &part, phases).await;
    state.cold_served[index] = cold > 0;
    state.bytes[index] = bytes + cold;
    Ok(())
}

/// Serve `read`'s offset out of the cold tier when the local log no longer
/// holds it, charging the object-store round trip to the request's remote
/// phase.
///
/// A KIP-405 tiered read and a diskless WAL cold read are both a network round
/// trip to an object store rather than work on this broker's own log, so they
/// belong beside the long poll and the Produce `acks=all` gate and not beside
/// `do_read`. Kafka accounts them the same way: a fetch that misses the local
/// log becomes a `DelayedRemoteFetch` in the purgatory, and the purgatory wait
/// is what `RequestMetrics.RemoteTimeMs` measures.
///
/// Returns the bytes the cold tier served, and zero when the local log already
/// answered or when no tier holds the offset. A read the local log answered
/// charges nothing at all: the clock is only read once the fallback is
/// entered, so a cluster with no tiered or diskless topic sees an unchanged
/// remote phase.
async fn serve_from_cold_tier(
    broker: &Broker,
    read: &mut PendingRead,
    part: &Partition,
    phases: &crate::metrics::RequestPhases,
) -> usize {
    if read.out.error_code != codes::OFFSET_OUT_OF_RANGE {
        return 0;
    }
    let started = std::time::Instant::now();
    let served = match try_remote_read(broker, read, part).await {
        Some(remote_bytes) => remote_bytes,
        None => crate::diskless::read::try_diskless_read(broker, read, part)
            .await
            .unwrap_or(0),
    };
    phases.add_remote(started.elapsed());
    served
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig};
    use krabka_protocol::{
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch},
    };
    use object_store::{ObjectStoreExt as _, PutPayload, path::Path};

    use crate::{broker::Broker, metrics::RequestPhases};

    /// A cold read is a round trip to an object store, so it belongs to the
    /// remote phase. Before this was charged, a tiered or diskless fetch could
    /// spend its whole latency in the object store while both phase histograms
    /// stayed near zero and the time fell into the unnamed remainder the
    /// rustdoc describes as decode, authorization and encode.
    #[tokio::test]
    async fn cold_tier_fallback_charges_the_object_store_read_to_the_remote_phase() {
        let dir = tempfile::tempdir().expect("tempdir");
        let object_dir = tempfile::tempdir().expect("object tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: object_dir.path().to_path_buf(),
        });
        config.remote_log_metadata = crate::config::RlmmKind::InMemory;
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();

        let topic_id = uuid::Uuid::from_u128(0xC01D);
        let mut flushed = BytesMut::new();
        RecordBatch {
            base_offset: 0,
            records: vec![Record {
                value: Some(Bytes::from_static(b"cold")),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut flushed)
        .expect("encode flushed batch");
        let flushed = flushed.freeze();
        let read_handle = broker.diskless_read.as_ref().expect("diskless read handle");
        read_handle
            .object_store()
            .put(
                &Path::from("diskless-wal/cold"),
                PutPayload::from(flushed.clone()),
            )
            .await
            .expect("put flushed run");
        read_handle
            .index
            .lock()
            .await
            .apply(&crate::diskless::wal_index::WalFlushRecord {
                object_key: "diskless-wal/cold".into(),
                format_version: 1,
                entries: vec![crate::diskless::wal_index::WalIndexEntry {
                    topic_id,
                    partition: 0,
                    first_offset: 0,
                    last_offset: 0,
                    byte_start: 0,
                    byte_len: u32::try_from(flushed.len()).expect("small run"),
                    max_timestamp_ms: 0,
                }],
            });

        let part_dir = dir.path().join("cold-0");
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let part = crate::broker::spawn_partition(
            "cold".into(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            true,
        );
        let mut pending = super::PendingRead {
            topic_name: "cold".into(),
            topic_id: WireUuid(topic_id.into_bytes()),
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            max_bytes: i32::try_from(flushed.len()).expect("small run"),
            read_committed: false,
            is_follower_fetch: false,
            partition: Some(std::sync::Arc::clone(&part)),
            out: super::PartitionData {
                error_code: crate::codes::OFFSET_OUT_OF_RANGE,
                high_watermark: 1,
                log_start_offset: 1,
                ..Default::default()
            },
            cpu_micros: 0,
        };

        let phases = RequestPhases::default();
        let served = super::serve_from_cold_tier(&broker, &mut pending, &part, &phases).await;

        assert!(served == flushed.len());
        assert!(pending.out.error_code == crate::codes::NONE);
        assert!(phases.remote_seconds() > 0.0);
        // The object-store trip is charged to exactly one phase: the local
        // phase belongs to `do_read`, which this call does not make.
        assert!(phases.local_seconds() < 1e-9);

        // A partition the local log answered never enters the fallback, so a
        // cluster with no cold tier sees an unchanged remote phase.
        pending.out.error_code = crate::codes::NONE;
        let local_only = RequestPhases::default();
        let served = super::serve_from_cold_tier(&broker, &mut pending, &part, &local_only).await;

        assert!(served == 0);
        assert!(local_only.remote_seconds() < 1e-9);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn long_poll_reread_rechecks_follower_epoch() {
        const TOPIC: &str = "long-poll-epoch";

        let dir = tempfile::tempdir().expect("tempdir");
        let broker_handle = Broker::start(crate::config::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join(format!("{TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let part = crate::broker::spawn_partition(
            TOPIC.to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            partition_max_bytes: 1024,
        };
        let mut pending = [super::PendingRead::planned(
            TOPIC,
            WireUuid::ZERO,
            &request,
            (false, true),
            Some(std::sync::Arc::clone(&part)),
            super::PartitionData {
                partition_index: 0,
                ..Default::default()
            },
        )];

        part.install_leader_change(1, 1).await;
        part.produce_batch(RecordBatch {
            partition_leader_epoch: 1,
            records: vec![Record {
                value: Some(Bytes::from_static(b"new-epoch")),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("append new-epoch record");

        let phases = crate::metrics::RequestPhases::default();
        let mut state = state_for(&pending, 0, 0);
        super::long_poll_then_reread(&broker, &mut pending, Vec::new(), &mut state, &phases)
            .await
            .expect("re-read");

        assert!(pending[0].out.error_code == crate::codes::FENCED_LEADER_EPOCH);
        assert!(pending[0].out.records.is_none());
        broker_handle.shutdown().await;
    }

    /// The accumulator a fetch carries into the long poll, for a pending set
    /// that has just been read and produced `bytes` bytes in total.
    fn state_for(
        pending: &[super::PendingRead],
        min_bytes: usize,
        max_wait_ms: i32,
    ) -> super::LongPollState {
        super::LongPollState {
            min_bytes,
            max_wait_ms,
            sendfile_capable: false,
            bytes: vec![0; pending.len()],
            cold_served: vec![false; pending.len()],
        }
    }

    fn sized_batch(payload: &'static [u8]) -> RecordBatch {
        RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(payload)),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The base offsets of the batches a fetch answered with, in order.
    fn served_base_offsets(out: &super::PartitionData) -> Vec<i64> {
        let Some(krabka_protocol::records::RecordsPayload::Raw(raw)) = out.records.as_ref() else {
            return Vec::new();
        };
        let krabka_protocol::records::RecordsPayload::V2(batches) =
            krabka_protocol::records::RecordsPayload::from_bytes(raw.clone())
                .expect("decode served")
        else {
            return Vec::new();
        };
        batches.iter().map(|batch| batch.base_offset).collect()
    }

    /// `fetch.min.bytes` is a floor and not a hint. A wake that does not carry
    /// enough bytes parks again, and the response the fetch finally sends
    /// holds every append that arrived up to the one that cleared the floor.
    #[tokio::test]
    async fn min_bytes_holds_the_long_poll_until_three_appends_add_up() {
        const TOPIC: &str = "min-bytes-floor";
        const PAYLOAD: &[u8; 64] = &[b'x'; 64];

        let dir = tempfile::tempdir().expect("tempdir");
        let broker_handle = Broker::start(crate::config::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join(format!("{TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let part = crate::broker::spawn_partition(
            TOPIC.to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );

        // One batch's worth of bytes, so the floor can be set between two
        // appends and three.
        let mut sized = BytesMut::new();
        sized_batch(PAYLOAD)
            .encode(&mut sized)
            .expect("encode one batch");
        let one_batch = sized.len();
        let min_bytes = i32::try_from(one_batch * 2 + 1).expect("small floor");

        let producer = std::sync::Arc::clone(&part);
        let appends = tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                producer
                    .produce_batch(sized_batch(PAYLOAD))
                    .await
                    .expect("append");
            }
        });

        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            partition_max_bytes: i32::try_from(one_batch * 8).expect("small budget"),
        };
        let pending = vec![super::PendingRead::planned(
            TOPIC,
            WireUuid::ZERO,
            &request,
            (false, true),
            Some(std::sync::Arc::clone(&part)),
            super::PartitionData {
                partition_index: 0,
                ..Default::default()
            },
        )];
        let phases = RequestPhases::default();
        let (topics, _cpu) =
            super::execute_pending_reads(&broker, pending, min_bytes, 30_000, false, &phases)
                .await
                .expect("fetch");

        appends.await.expect("producer task");
        let served = &topics[0].partitions[0];
        assert!(served_base_offsets(served) == vec![0, 1, 2]);
        broker_handle.shutdown().await;
    }

    /// An append that lands after the read pass and before the park is not
    /// lost: the waiters are armed before anything is read, so the producer's
    /// `notify_waiters` -- which leaves no permit behind for a waiter that
    /// registers later -- still has someone to wake.
    ///
    /// Without the pre-armed waiters this fetch sleeps out its whole
    /// `max_wait_ms`, which the test's own timeout stands in for.
    #[tokio::test]
    async fn an_append_that_lands_before_the_park_is_not_missed() {
        const TOPIC: &str = "append-before-park";

        let dir = tempfile::tempdir().expect("tempdir");
        let broker_handle = Broker::start(crate::config::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join(format!("{TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let part = crate::broker::spawn_partition(
            TOPIC.to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );

        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            partition_max_bytes: 1024,
        };
        let mut pending = [super::PendingRead::planned(
            TOPIC,
            WireUuid::ZERO,
            &request,
            (false, true),
            Some(std::sync::Arc::clone(&part)),
            super::PartitionData {
                partition_index: 0,
                ..Default::default()
            },
        )];

        // What `execute_pending_reads` does in this order: arm the waiters,
        // read (the log is empty, so the read finds nothing), then park.
        let waits = super::arm_waits(&pending);
        let mut state = state_for(&pending, 1, 60_000);

        // The race the arming closes: the append lands between the read and
        // the park.
        part.produce_batch(sized_batch(b"raced"))
            .await
            .expect("append");

        let phases = RequestPhases::default();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::long_poll_then_reread(&broker, &mut pending, waits, &mut state, &phases),
        )
        .await
        .expect("the fetch answers on the append it raced")
        .expect("re-read");

        assert!(served_base_offsets(&pending[0].out) == vec![0]);
        broker_handle.shutdown().await;
    }
}
