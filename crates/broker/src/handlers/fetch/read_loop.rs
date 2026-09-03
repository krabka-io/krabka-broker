//! The read loop: every planned partition is read once, and a fetch that
//! did not reach `min_bytes` parks on the partitions' notifiers and reads
//! them all again before it answers.

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

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Read every planned partition once, then long-poll and read them all again
/// if the fetch did not reach `min_bytes`.
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
    let mut total_bytes = 0;
    for read in &mut pending {
        let Some(partition) = read.partition.clone() else {
            continue;
        };
        let started = std::time::Instant::now();
        total_bytes += do_read(
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
        total_bytes += serve_from_cold_tier(broker, read, &partition, phases).await;
    }
    let wants_more = total_bytes < usize::try_from(min_bytes.max(0)).unwrap_or(0);
    if wants_more && max_wait_ms > 0 {
        long_poll_then_reread(broker, &mut pending, max_wait_ms, sendfile_capable, phases).await?;
    }
    Ok(group_into_topic_responses(pending))
}

/// Wait with a timeout for the `append_notify` of any readable partition to
/// fire, then revalidate the request epoch and read every partition once more.
///
/// The function resets each partition's accumulated records before it reads
/// again, so the new read replaces the old one.
// cargo-mutants: long-poll serve-loop glue — parks on partition append/HW
// notifiers, then replays `do_read` per partition. The live fetch integration
// suite covers notifier-driven re-reads; the focused unit test below covers
// epoch revalidation after the wait.
#[cfg_attr(test, mutants::skip)]
async fn long_poll_then_reread(
    broker: &Broker,
    pending: &mut [PendingRead],
    max_wait_ms: i32,
    sendfile_capable: bool,
    phases: &crate::metrics::RequestPhases,
) -> Result<(), BrokerError> {
    let mut notifies: Vec<Arc<Notify>> = Vec::new();
    for p in pending.iter() {
        if let Some(part) = p.partition.as_ref() {
            notifies.push(part.append_notify.clone());
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
            if !p.is_follower_fetch {
                notifies.push(part.hw_advance_notify.clone());
                notifies.push(part.delivery.advance_notify.clone());
            }
        }
    }
    if notifies.is_empty() {
        return Ok(());
    }
    // `Notify::notified()` returns a non-Send `Notified<'_>` that borrows
    // from its `Arc<Notify>`. Move the Arc into an `async move` block so
    // the future owns its Arc and is `'static + Send` (see `WaitFut` type
    // alias above).
    let waits: Vec<WaitFut> = notifies
        .into_iter()
        .map(|n| Box::pin(async move { n.notified().await }) as WaitFut)
        .collect();
    let max_wait = Duration::from_millis(u64::from(u32::try_from(max_wait_ms).unwrap_or(0)));
    // The park is the Fetch remote phase: this broker has read everything it
    // holds and is waiting for someone else to append, so the time belongs
    // beside the Produce `acks=all` gate and not beside the local read.
    let parked = std::time::Instant::now();
    let _ = tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await;
    phases.add_remote(parked.elapsed());

    let image = broker.controller.current_image();
    for p in pending.iter_mut() {
        let Some(part) = p.partition.clone() else {
            continue;
        };
        p.out = PartitionData {
            partition_index: p.partition_index,
            ..Default::default()
        };
        let request = EffectivePartition {
            partition: p.partition_index,
            current_leader_epoch: p.current_leader_epoch,
            last_fetched_epoch: p.last_fetched_epoch,
            fetch_offset: p.fetch_offset,
            partition_max_bytes: p.max_bytes,
        };
        if apply_epoch_checks(
            &image,
            &p.topic_name,
            p.partition_index,
            &request,
            &part,
            &mut p.out,
        ) {
            continue;
        }
        // Time the re-read so its duration accumulates into the same
        // per-partition CPU counter as the first pass (wall-clock delta;
        // see the first-pass comment for why this replaces TaskMonitor).
        let read_start = std::time::Instant::now();
        do_read(
            &part,
            ReadRequest {
                topic_id: Some(uuid::Uuid::from_bytes(p.topic_id.0)),
                hot_tail: Some(broker.hot_tail.clone()),
                // Wrap the decoded-request wire offset into `Offset` for the read.
                fetch_offset: Offset(p.fetch_offset),
                max_bytes: p.max_bytes,
                read_committed: p.read_committed,
                is_follower_fetch: p.is_follower_fetch,
                sendfile_capable,
                sendfile_min_bytes: broker.config.sendfile_min.bytes_usize(),
            },
            &mut p.out,
        )
        .await?;
        let micros = u64::try_from(read_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        p.cpu_micros = p.cpu_micros.saturating_add(micros);
        // The re-read is local work like the first pass, so it accumulates on
        // the same phase.
        phases.add_local(read_start.elapsed());

        // Re-attempt the cold-tier read on the re-read pass so a long-poll
        // that fires on a non-tiered partition doesn't clobber the remote
        // batch we'd already served on this one.
        serve_from_cold_tier(broker, p, &part, phases).await;
    }
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
        super::long_poll_then_reread(&broker, &mut pending, 0, false, &phases)
            .await
            .expect("re-read");

        assert!(pending[0].out.error_code == crate::codes::FENCED_LEADER_EPOCH);
        assert!(pending[0].out.records.is_none());
        broker_handle.shutdown().await;
    }
}
