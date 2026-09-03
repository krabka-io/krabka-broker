//! The lazily loaded acquisition-state cells: the load-on-miss path, the
//! test-only peek, and the invalidation the admin offset RPCs use.
//!
//! This is the only module that inserts into or removes from the `leaders`
//! map, so the rule that no `DashMap` guard is held across an `.await` is
//! checkable by reading one file.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use tokio::sync::Mutex;
use tracing::warn;

use super::SharePartitionLeaderManager;
use crate::{
    coordinator::unified::streams::config::ShareAutoOffsetReset,
    share_partition::state::AcquisitionState,
};

impl SharePartitionLeaderManager {
    /// Gets the acquisition-state cell for `(group, topic_id, partition)`, and
    /// loads it lazily on a miss.
    ///
    /// On a cache miss the method reads the durable state from the persister
    /// and folds it into a fresh [`AcquisitionState`]. If no durable state
    /// exists, the group's `share.auto.offset.reset` decides where the empty
    /// window starts, and the method persists that decision so a later leader
    /// does not resolve it again against a moved log or a moved clock. The
    /// method drops the `DashMap` guard before the load `.await`. A concurrent
    /// loader that loses the insert race adopts the cell of the winner.
    ///
    /// The `ShareFetch` and `ShareAcknowledge` handlers call this method.
    pub(crate) async fn get_or_load(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Arc<Mutex<AcquisitionState>> {
        let key = (group.to_string(), topic_id, partition);
        if let Some(cell) = self.leaders.get(&key) {
            return cell.value().clone();
        }

        // Miss: load from the persister WITHOUT holding any DashMap guard.
        let leader_epoch = self.leader_epoch_for(topic_id, partition);
        let mut loaded = match self.persister.read_state(group, topic_id, partition).await {
            // Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET` is -1: the
            // record the group coordinator writes when it registers a share
            // partition, before any fetch has resolved where the group starts.
            // It is not a start offset, so it takes the strategy path below.
            Ok(Some(persisted)) if persisted.start_offset.0 >= 0 => {
                let mut st = AcquisitionState::new(persisted.start_offset);
                st.load_from(
                    persisted.start_offset,
                    persisted.state_epoch,
                    leader_epoch,
                    persisted.delivery_complete_count,
                    &persisted.state_batches,
                );
                st
            }
            Ok(uninitialized) => {
                let start = self.initial_start_offset(group, topic_id, partition).await;
                let mut st = AcquisitionState::new(start);
                // The strategy decides only where the window starts. The state
                // epoch stays the coordinator's: it is the fencing token the
                // group coordinator stamped when it registered the partition,
                // and a write-back carrying a lower one is refused with
                // FENCED_STATE_EPOCH, which would strand every later SPSO
                // advance in memory.
                st.state_epoch = uninitialized.map_or(0, |persisted| persisted.state_epoch);
                st.leader_epoch = leader_epoch;
                // The resolved start is durable state: persist it now so the
                // next leader inherits it instead of re-resolving a `latest`
                // or `by_duration` strategy against a log that has moved on.
                st.dirty = true;
                st
            }
            Err(e) => {
                warn!(
                    group,
                    %topic_id, partition, error = %e,
                    "share-partition state load failed; starting from empty window"
                );
                let mut st = AcquisitionState::new(Offset(0));
                st.leader_epoch = leader_epoch;
                st
            }
        };

        self.persist_if_dirty(group, topic_id, partition, &mut loaded)
            .await;
        let cell = Arc::new(Mutex::new(loaded));
        // Adopt the winner if another task loaded the same key concurrently.
        self.leaders.entry(key).or_insert(cell).value().clone()
    }

    /// Where a share partition with no persisted state starts.
    ///
    /// The group's `share.auto.offset.reset` picks the offset: `earliest` the
    /// log start, `latest` the high watermark, and `by_duration:<d>` the first
    /// record at or after `now - d`, which falls back to the high watermark
    /// when the log holds no such record. Every answer is clamped to the log
    /// start, so a retention-truncated partition never starts below it.
    ///
    /// A partition this broker does not hold, or a topic id the image does not
    /// know, yields offset 0: there is no log to resolve against, and the next
    /// load re-resolves once the partition is materialized.
    async fn initial_start_offset(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Offset {
        let image = self.controller.current_image();
        let strategy = image
            .group_config(group)
            .map_or_else(ShareAutoOffsetReset::default, |overrides| {
                ShareAutoOffsetReset::from_group_overrides(overrides)
            });
        let local = image
            .topics()
            .find(|t| t.topic_id == topic_id)
            .and_then(|topic| self.partitions.get(&topic.name, PartitionIndex(partition)));
        let Some(local) = local else {
            return Offset(0);
        };
        let log_start = local.log_start_offset();
        match strategy {
            ShareAutoOffsetReset::Earliest => log_start,
            ShareAutoOffsetReset::Latest => local.high_watermark().await.max(log_start),
            ShareAutoOffsetReset::ByDuration(duration) => {
                let target = now_ms()
                    .saturating_sub(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
                let found = {
                    let log = local.log.lock().expect("log mutex poisoned");
                    log.offset_for_timestamp(target).map(|(offset, _)| offset)
                };
                match found {
                    Some(offset) => offset.max(log_start),
                    // Every record predates the window: the group starts at
                    // the end of the log, as Kafka's `offsetForTimestamp`
                    // fallback does.
                    None => local.high_watermark().await.max(log_start),
                }
            }
        }
    }

    /// Test-only: borrows the live acquisition cell without a persister load.
    ///
    /// Returns `None` if this node does not currently lead the partition or has
    /// not loaded the cell.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn peek_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<AcquisitionState>>> {
        self.leaders
            .get(&(group.to_string(), topic_id, partition))
            .map(|c| c.value().clone())
    }

    /// Drops the cached acquisition-state cell for
    /// `(group, topic_id, partition)`.
    ///
    /// The next `get_or_load` then re-reads the durable SPSO. The admin offset
    /// RPCs call this method after `AlterShareGroupOffsets` or
    /// `DeleteShareGroupOffsets` rewrites the persister state. A later
    /// `ShareFetch` on this broker thus sees an in-flight reset. A cell on
    /// another broker refreshes on its own next load, which matches the classic
    /// offset-reset behavior.
    pub(crate) fn invalidate(&self, group: &str, topic_id: uuid::Uuid, partition: i32) {
        self.leaders
            .remove(&(group.to_string(), topic_id, partition));
    }
}

/// Wall-clock milliseconds since the Unix epoch, the unit a record timestamp
/// carries. A clock before the epoch reads as 0.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_ids::LeaderEpoch;
    use krabka_log::Offset;
    use krabka_metadata::{
        GroupConfigRecord, MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord,
    };

    use crate::{
        coordinator::unified::streams::config::KEY_SHARE_AUTO_OFFSET_RESET,
        share_partition::manager::test_support::{
            manager, manager_with_image_and_partitions, open_data_partition,
        },
    };

    /// Every `share.auto.offset.reset` strategy, resolved against one real
    /// log: two records stamped three hours ago at offsets 0-1, two stamped
    /// ninety minutes ago at offsets 2-3, and a high watermark of 4.
    ///
    /// The resolution is exercised where `get_or_load` calls it. The load
    /// itself cannot reach it over this fixture, because a metadata image with
    /// no brokers cannot bootstrap `__share_group_state`, so `read_state`
    /// fails before it can report that the partition has no state.
    #[tokio::test]
    async fn fresh_cell_starts_where_the_group_strategy_says() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tid = uuid::Uuid::from_bytes([41; 16]);
        let now = super::now_ms();
        let hour = 60 * 60 * 1_000;
        let reg = std::sync::Arc::new(crate::partition_registry::PartitionRegistry::new());
        open_data_partition(
            &reg,
            dir.path(),
            "t",
            0,
            &[
                (now - 3 * hour, &[b"stale-0", b"stale-1"]),
                (now - hour - hour / 2, &[b"recent-0", b"recent-1"]),
            ],
            Offset(4),
        )
        .await;

        // `default` carries no override at all, so the broker default decides.
        let strategies = [
            ("default", None, Offset(4)),
            ("latest", Some("latest"), Offset(4)),
            ("earliest", Some("earliest"), Offset(0)),
            // The window reaches back past the second batch only.
            ("in-window", Some("by_duration:PT2H"), Offset(2)),
            // No record is inside the window: start at the high watermark.
            ("past-the-end", Some("by_duration:PT1H"), Offset(4)),
        ];
        let mut records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: tid,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: 0,
                leader: NodeId(1),
                replicas: vec![NodeId(1)],
                isr: vec![NodeId(1)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ];
        for (group, value, _) in strategies {
            if let Some(value) = value {
                records.push(MetadataRecord::V1GroupConfig(GroupConfigRecord {
                    group_id: group.to_string(),
                    configs: maplit::btreemap! {
                        KEY_SHARE_AUTO_OFFSET_RESET.to_owned() => value.to_owned()
                    },
                }));
            }
        }
        let mgr = manager_with_image_and_partitions(
            Arc::new(MetadataImage::from_records(uuid::Uuid::nil(), &records)),
            reg,
        );

        for (group, value, want) in strategies {
            let got = mgr.initial_start_offset(group, tid, 0).await;
            assert!(
                got == want,
                "{KEY_SHARE_AUTO_OFFSET_RESET}={value:?}: got {got:?}, want {want:?}"
            );
        }
    }

    #[tokio::test]
    async fn get_or_load_fresh_returns_empty_window_and_caches() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([21; 16]);

        // The image knows no such topic, so there is no log to resolve the
        // group's strategy against and the window starts at 0. The write-back
        // of that decision cannot reach a share-state topic over a broker-less
        // image, so `dirty` stays set for the retry, which
        // `persist_if_dirty_keeps_dirty_on_write_failure` covers.
        let cell = mgr.get_or_load("g1", tid, 0).await;
        let st = cell.lock().await;
        assert!(st.start_offset == 0);
        drop(st);
        // A second call returns the same cached cell.
        let cell2 = mgr.get_or_load("g1", tid, 0).await;
        assert!(Arc::ptr_eq(&cell, &cell2));
    }

    #[tokio::test]
    async fn invalidate_removes_cached_cell() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([24; 16]);

        // Populate the cache, then invalidate; a subsequent load yields a
        // fresh, distinct cell.
        let cell = mgr.get_or_load("g1", tid, 0).await;
        mgr.invalidate("g1", tid, 0);
        let cell2 = mgr.get_or_load("g1", tid, 0).await;
        assert!(!Arc::ptr_eq(&cell, &cell2));
    }
}
