//! Committed diskless-WAL index capture.

use std::{collections::HashMap, sync::Arc, time::Duration};

use krabka_remote_storage::diskless::{DisklessWalCapture, REPLAY_FENCE_KEY, WalCaptureProjection};
use krabka_remote_storage_topic::MetadataEventLog;
use uuid::Uuid;

/// How long the whole index read may take before the capture gives up.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);

/// Replay the committed keyed index state up to each partition's high
/// watermark and freeze it.
///
/// The capture only reads. Each partition's cutoff is its high watermark
/// (`ListOffsets` `LATEST`) when the capture starts, and the capture reads
/// from the partition's low watermark up to that cutoff. An empty partition
/// is complete at once. Replay fences that brokers publish into the index are
/// not index state, so the capture skips them. Kafka refuses a `Produce` to an
/// internal topic from any client but `__admin_client`, so a capture that
/// wrote its own fence would fail against a Kafka-compatible broker.
///
/// # Errors
/// Returns an error when the index watermarks cannot be read, when the read
/// fails or takes longer than 30 seconds, or when a record cannot be decoded
/// or projected.
pub async fn capture_projection<S: std::hash::BuildHasher>(
    log: Arc<dyn MetadataEventLog>,
    topics: &HashMap<Uuid, (String, i32), S>,
    captured_at_ms: u64,
) -> Result<DisklessWalCapture, String> {
    let cutoffs = log
        .high_water_marks()
        .await
        .map_err(|error| error.to_string())?;
    let starts = log
        .low_water_marks()
        .await
        .map_err(|error| error.to_string())?;
    let partition_count = usize::try_from(log.partition_count())
        .map_err(|_| "negative diskless WAL index partition count".to_owned())?;
    if cutoffs.len() != partition_count || starts.len() != partition_count {
        return Err(format!(
            "diskless WAL index has {partition_count} partitions but reported {} high and {} low \
             watermarks",
            cutoffs.len(),
            starts.len()
        ));
    }
    let mut projection = WalCaptureProjection::default();
    tokio::time::timeout(CAPTURE_TIMEOUT, async {
        for (partition, (&start, &cutoff)) in starts.iter().zip(&cutoffs).enumerate() {
            if start >= cutoff {
                continue;
            }
            let partition = i32::try_from(partition)
                .map_err(|_| "diskless WAL index partition overflow".to_owned())?;
            let records = log
                .read_range(partition, start, cutoff)
                .await
                .map_err(|error| error.to_string())?;
            for record in records {
                if record.key.as_deref() == Some(REPLAY_FENCE_KEY) {
                    continue;
                }
                projection.apply(
                    record.key.as_deref(),
                    (!record.tombstone).then_some(record.payload.as_ref()),
                )?;
            }
        }
        Ok::<_, String>(())
    })
    .await
    .map_err(|_| "diskless WAL index capture timed out".to_owned())??;
    projection.finish_legacy_replay();
    projection.capture(topics, cutoffs, captured_at_ms)
}
