//! The durable append path for `__share_group_state` and the best-effort prune
//! of that partition's log prefix.
//!
//! `persist_record` is the single write path that `initialize`, `write`, and
//! `delete` share. `maybe_prune` is the KIP-932 log trim that follows a folded
//! `ShareSnapshot`. Both talk to the partition log rather than to the delivery
//! state machine, so they live apart from `state_machine`.

use std::sync::Arc;

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::records::{Record, RecordBatch};
use tokio::sync::Mutex;
use tracing::warn;

use super::ShareCoordinator;
use crate::{
    error::BrokerError,
    share_coordinator::{
        bootstrap,
        persistence::{ShareStateKey, encode_state_key},
        pruning::redundant_offset,
        state::SharePartitionState,
    },
};

impl ShareCoordinator {
    /// Appends one `(key, value)` record to `__share_group_state`-`p`.
    ///
    /// This method returns the base offset of the record. A `None` value writes
    /// a tombstone.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Share`] if the partition log is not open
    /// locally. Returns the append error if `produce_batch` fails.
    pub(super) async fn persist_record(
        &self,
        state_partition: PartitionIndex,
        key: ShareStateKey,
        value: Option<Bytes>,
    ) -> Result<Offset, BrokerError> {
        let part = self
            .partitions
            .get(bootstrap::TOPIC, state_partition)
            .ok_or_else(|| {
                BrokerError::Share(format!("__share_group_state-{state_partition} not local"))
            })?;

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(encode_state_key(&key)),
            value,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await
    }

    /// Prunes the log prefix of `state_partition` on a best-effort basis.
    ///
    /// This method computes `redundant_offset`, the smallest
    /// `last_snapshot_offset` across every live key mapped to that state
    /// partition. If `redundant_offset` is more than the current
    /// `log_start_offset` of the partition, the method trims the log up to it.
    /// Every retained key keeps its latest snapshot, so the trim is safe. The
    /// method logs each error and then discards it. A prune never fails a
    /// write.
    pub(super) async fn maybe_prune(&self, state_partition: PartitionIndex) {
        let Some(part) = self.partitions.get(bootstrap::TOPIC, state_partition) else {
            return;
        };

        // Collect this partition's keys' last-snapshot offsets.
        let handles: Vec<Arc<Mutex<SharePartitionState>>> = self
            .state
            .iter()
            .filter(|e| {
                let (g, t, p) = e.key();
                self.state_partition_for(g, t, *p) == state_partition
            })
            .map(|e| e.value().clone())
            .collect();

        let mut offsets = Vec::with_capacity(handles.len());
        for h in handles {
            offsets.push(h.lock().await.last_snapshot_offset);
        }

        let Some(redundant) = redundant_offset(&offsets) else {
            return;
        };
        if redundant > part.log_start_offset()
            && let Err(e) = part.trim_to_offset(redundant).await
        {
            warn!(
                partition = state_partition.get(),
                error = %e,
                "share-state log prune failed; continuing"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        partition_registry::PartitionRegistry,
        share_coordinator::{
            config::ShareCoordinatorConfig,
            coordinator::test_support::{batch, lead_all, open_state_partition},
        },
    };

    #[tokio::test]
    async fn snapshot_fold_after_threshold_resets_counter() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
            open_state_partition(&reg, dir.path(), p);
        }
        // Small threshold so a few writes trigger a fold.
        let cfg = ShareCoordinatorConfig {
            snapshot_update_records_per_snapshot: 3,
            ..ShareCoordinatorConfig::default()
        };
        let coord = ShareCoordinator::new(krabka_audit::NodeId(1), reg.clone(), cfg);
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([9; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        for i in 0..3 {
            let base = i64::from(i) * 10;
            coord
                .write(
                    "g",
                    tid,
                    0,
                    (1, 1),
                    (Offset(0), 0),
                    vec![batch(base, base + 9)],
                )
                .await
                .unwrap();
        }

        let st = coord.read("g", tid, 0).await.expect("present");
        // After the 3rd update crossed the threshold, a snapshot was folded
        // and the counter reset.
        assert!(st.updates_since_snapshot == 0);
        assert!(st.snapshot_epoch == 1);
    }

    /// After a snapshot fold, `maybe_prune` must trim the state-partition log.
    ///
    /// The trim goes up to the redundant offset, which is the offset of the
    /// folded snapshot. The `log_start_offset` of the partition then advances
    /// past 0. If the prune does not run, `log_start_offset` stays at 0.
    #[tokio::test]
    async fn snapshot_fold_prunes_log_prefix() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
            open_state_partition(&reg, dir.path(), p);
        }
        // Fold after 2 updates so a snapshot lands a few records in.
        let cfg = ShareCoordinatorConfig {
            snapshot_update_records_per_snapshot: 2,
            ..ShareCoordinatorConfig::default()
        };
        let coord = ShareCoordinator::new(krabka_audit::NodeId(1), reg.clone(), cfg);
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([13; 16]);
        let state_partition = coord.state_partition_for("g", &tid, 0);
        let part = reg
            .get(bootstrap::TOPIC, state_partition)
            .expect("state partition open");

        // record 0: initialize snapshot.
        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        // records 1,2: updates; the 2nd crosses the threshold and folds a
        // snapshot at record 3, then prunes up to it.
        coord
            .write("g", tid, 0, (1, 1), (Offset(0), 0), vec![batch(0, 9)])
            .await
            .unwrap();
        coord
            .write("g", tid, 0, (1, 1), (Offset(0), 0), vec![batch(10, 19)])
            .await
            .unwrap();

        // The folded snapshot's offset is the sole key's last-snapshot offset,
        // which is > 0 and exceeds the log's initial start (0), so the prune
        // advanced the prefix. Without pruning the start stays at 0.
        let start = part.log_start_offset();
        check!(start > 0);
    }
}
