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
use crate::{broker::Broker, codes, error::BrokerError};

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

pub(super) async fn execute_pending_reads(
    broker: &Broker,
    mut pending: Vec<PendingRead>,
    min_bytes: i32,
    max_wait_ms: i32,
    sendfile_capable: bool,
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
        if read.out.error_code == codes::OFFSET_OUT_OF_RANGE {
            if let Some(remote_bytes) = try_remote_read(broker, read, &partition).await {
                total_bytes += remote_bytes;
            } else if let Some(diskless_bytes) =
                crate::diskless::read::try_diskless_read(broker, read, &partition).await
            {
                total_bytes += diskless_bytes;
            }
        }
    }
    let wants_more = total_bytes < usize::try_from(min_bytes.max(0)).unwrap_or(0);
    if wants_more && max_wait_ms > 0 {
        long_poll_then_reread(broker, &mut pending, max_wait_ms, sendfile_capable).await?;
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
    let _ = tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await;

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

        // Re-attempt the remote-tier read on the re-read pass
        // so a long-poll that fires on a non-tiered partition doesn't
        // clobber the remote batch we'd already served on this one.
        if p.out.error_code == codes::OFFSET_OUT_OF_RANGE
            && try_remote_read(broker, p, &part).await.is_none()
        {
            let _ = crate::diskless::read::try_diskless_read(broker, p, &part).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig};
    use krabka_protocol::{
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch},
    };

    use crate::broker::Broker;

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

        super::long_poll_then_reread(&broker, &mut pending, 0, false)
            .await
            .expect("re-read");

        assert!(pending[0].out.error_code == crate::codes::FENCED_LEADER_EPOCH);
        assert!(pending[0].out.records.is_none());
        broker_handle.shutdown().await;
    }
}
