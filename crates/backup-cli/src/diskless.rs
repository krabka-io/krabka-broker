//! Committed diskless-WAL index capture.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures_util::StreamExt as _;
use krabka_remote_storage::diskless::{DisklessWalCapture, REPLAY_FENCE_KEY, WalCaptureProjection};
use krabka_remote_storage_topic::{MetadataEventLog, PartitionStart};
use uuid::Uuid;

/// Replay committed keyed state through per-partition fences and freeze it.
///
/// # Errors
/// Returns an error when the index cannot be fenced, replayed, decoded, or projected.
pub async fn capture_projection<S: std::hash::BuildHasher>(
    log: Arc<dyn MetadataEventLog>,
    topic_names: &HashMap<Uuid, String, S>,
    captured_at_ms: u64,
) -> Result<DisklessWalCapture, String> {
    let starts = (0..log.partition_count())
        .map(|partition| PartitionStart {
            partition,
            start_offset: 0,
        })
        .collect();
    let (mut stream, _handle) = log.subscribe(starts);
    let mut cutoffs = Vec::with_capacity(usize::try_from(log.partition_count()).unwrap_or(0));
    for partition in 0..log.partition_count() {
        cutoffs.push(
            log.publish_keyed(
                partition,
                Bytes::from_static(REPLAY_FENCE_KEY),
                Some(Bytes::new()),
            )
            .await
            .map_err(|error| error.to_string())?,
        );
    }
    let mut reached = HashSet::new();
    let mut projection = WalCaptureProjection::default();
    tokio::time::timeout(Duration::from_secs(30), async {
        while reached.len() < cutoffs.len() {
            let event = stream.next().await.ok_or_else(|| {
                "diskless WAL index replay stopped before its capture fences".to_owned()
            })?;
            let index = usize::try_from(event.partition)
                .map_err(|_| "negative diskless WAL index partition".to_owned())?;
            let cutoff = *cutoffs
                .get(index)
                .ok_or_else(|| "diskless WAL index partition outside capture".to_owned())?;
            if event.offset > cutoff {
                continue;
            }
            if event.key.as_deref() == Some(REPLAY_FENCE_KEY) {
                if event.offset == cutoff {
                    reached.insert(event.partition);
                }
                continue;
            }
            projection.apply(
                event.key.as_deref(),
                (!event.tombstone).then_some(event.payload.as_ref()),
            )?;
        }
        Ok::<_, String>(())
    })
    .await
    .map_err(|_| "diskless WAL index capture timed out".to_owned())??;
    projection.capture(topic_names, cutoffs, captured_at_ms)
}
