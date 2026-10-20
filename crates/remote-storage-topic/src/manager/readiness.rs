//! Per-metadata-partition readiness of the topic-backed manager, for metrics.
//!
//! The broker exports one readiness sample per assigned `__remote_log_metadata`
//! partition, so a partition that stays `NotReady` is visible and can alert.
//! The sample reuses the read gate, so it cannot disagree with what a gated
//! read answers.

use super::{
    TopicBasedRemoteLogMetadataManager,
    assignment::{HWM_UNKNOWN, ReadGate},
};

/// Readiness of one assigned `__remote_log_metadata` partition, as
/// [`TopicBasedRemoteLogMetadataManager::metadata_partition_readiness`]
/// reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataPartitionReadiness {
    /// The metadata partition.
    pub partition: i32,
    /// The high-water mark observed when the partition was assigned: the
    /// offset after the last event the consumer must apply before reads are
    /// answered. `None` while that high-water mark is unknown, because the
    /// assignment-time fetch failed and a later reconcile has not yet
    /// refreshed it.
    pub target_offset: Option<i64>,
    /// The offset of the last event applied to the cache, or `-1` when none
    /// has been applied.
    pub applied_offset: i64,
    /// `true` when gated reads for this partition are answered, and `false`
    /// while they return the retriable `NotReady`.
    pub ready: bool,
}

impl TopicBasedRemoteLogMetadataManager {
    /// One [`MetadataPartitionReadiness`] for each metadata partition this
    /// manager is assigned, sorted by partition. An unassigned partition has
    /// no entry.
    ///
    /// `ready` comes from the same gate that every gated read consults.
    ///
    /// # Panics
    ///
    /// Panics if the assignment-readiness or applied-offset mutex is poisoned.
    #[must_use]
    pub fn metadata_partition_readiness(&self) -> Vec<MetadataPartitionReadiness> {
        let mut targets: Vec<(i32, i64)> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .iter()
            .map(|(&partition, &target)| (partition, target))
            .collect();
        targets.sort_unstable();
        targets
            .into_iter()
            .map(|(partition, target)| {
                let applied_offset = usize::try_from(partition).ok().map_or(-1, |index| {
                    self.applied
                        .lock()
                        .expect("applied mutex poisoned")
                        .get(index)
                        .copied()
                        .unwrap_or(-1)
                });
                MetadataPartitionReadiness {
                    partition,
                    target_offset: (target != HWM_UNKNOWN).then_some(target),
                    applied_offset,
                    ready: matches!(self.metadata_partition_gate(partition), ReadGate::Ready),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{HWM_UNKNOWN, MetadataPartitionReadiness},
        crate::{
            log::{InProcessMetadataEventLog, MetadataEventLog as _},
            manager::test_support::{
                HwmFlakyLog, SeededManager, seeded_manager, start_manager, tp, wait_ready,
            },
        },
        assert2::{assert, check},
        std::collections::HashMap,
    };

    /// One readiness case: the assignment targets and applied offsets the
    /// reconciler and the pump would have recorded, and the report expected.
    struct Case {
        name: &'static str,
        targets: &'static [(i32, i64)],
        applied: [i64; 4],
        expected: Vec<MetadataPartitionReadiness>,
    }

    const fn readiness(
        partition: i32,
        target_offset: Option<i64>,
        applied_offset: i64,
        ready: bool,
    ) -> MetadataPartitionReadiness {
        MetadataPartitionReadiness {
            partition,
            target_offset,
            applied_offset,
            ready,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn readiness_reports_each_assigned_partition_against_its_gate() {
        let cases = [
            Case {
                name: "unassigned",
                targets: &[],
                applied: [5, 5, 5, 5],
                expected: vec![],
            },
            Case {
                name: "assigned empty partition, target 0",
                targets: &[(2, 0)],
                applied: [-1, -1, -1, -1],
                expected: vec![readiness(2, Some(0), -1, true)],
            },
            Case {
                name: "assigned behind",
                targets: &[(1, 10)],
                applied: [-1, 7, -1, -1],
                expected: vec![readiness(1, Some(10), 7, false)],
            },
            Case {
                name: "assigned caught up",
                targets: &[(1, 10)],
                applied: [-1, 9, -1, -1],
                expected: vec![readiness(1, Some(10), 9, true)],
            },
            Case {
                name: "HWM unknown",
                targets: &[(3, HWM_UNKNOWN)],
                applied: [-1, -1, -1, 4],
                expected: vec![readiness(3, None, 4, false)],
            },
            Case {
                name: "several partitions, sorted",
                targets: &[(3, 2), (0, HWM_UNKNOWN), (2, 0)],
                applied: [-1, -1, -1, 0],
                expected: vec![
                    readiness(0, None, -1, false),
                    readiness(2, Some(0), -1, true),
                    readiness(3, Some(2), 0, false),
                ],
            },
        ];
        for Case {
            name,
            targets,
            applied,
            expected,
        } in cases
        {
            // The manager consumes nothing, so the pump never touches the
            // state the case installs.
            let manager = start_manager(InProcessMetadataEventLog::new(4));
            *manager.ready_targets.lock().unwrap() =
                targets.iter().copied().collect::<HashMap<_, _>>();
            *manager.applied.lock().unwrap() = applied.to_vec();
            check!(manager.metadata_partition_readiness() == expected, "{name}");
            manager.shutdown();
        }
    }

    /// The reconciler's own sentinel reads as an unknown target, and the
    /// refreshed real target as a known one once the pump catches up.
    #[tokio::test(flavor = "multi_thread")]
    async fn readiness_follows_a_failed_then_refreshed_high_water_mark() {
        let log = HwmFlakyLog::new(4);
        let SeededManager { manager, partition } = seeded_manager(log.clone()).await;
        let hwm = log.high_water_marks().await.unwrap()[usize::try_from(partition).unwrap()];
        assert!(hwm > 0);

        log.set_fail_hwm(true);
        manager.reconcile_assignment(&[partition]).await;
        // The pump is already consuming, so the applied offset is whatever it
        // has reached; the target and the gate are what the sentinel decides.
        let unknown = manager.metadata_partition_readiness();
        let applied = unknown.first().map_or(-1, |entry| entry.applied_offset);
        check!(unknown == [readiness(partition, None, applied, false)]);

        log.set_fail_hwm(false);
        manager.reconcile_assignment(&[partition]).await;
        wait_ready(&manager, &tp()).await;
        check!(
            manager.metadata_partition_readiness()
                == [readiness(partition, Some(hwm), hwm - 1, true)]
        );

        manager.reconcile_assignment(&[]).await;
        check!(manager.metadata_partition_readiness().is_empty());
        manager.shutdown();
    }
}
