//! The diskless voter-set half of the shard engine: the configured quorum, the
//! durable offsets its voters report, and the watermark those offsets imply.
//!
//! A diskless shard acknowledges an append once a majority of the
//! metadata-selected voters has fsynced it, which is a different rule from the
//! local replica harness in the module root. The state that only this rule
//! reads, and every method that writes it, live here.

use std::{collections::HashMap, sync::atomic::Ordering};

use krabka_ids::Offset;
use krabka_kraft_core::NodeId;

use super::{DistributedQuorum, WalShardEngine, strict_majority};

impl WalShardEngine {
    /// Install the metadata-selected broker voter set. Production registries
    /// call this before the shard can acknowledge another append.
    pub(crate) fn configure_distributed(&self, me: NodeId, voters: &[NodeId]) {
        self.distributed_required.store(true, Ordering::Release);
        let mut distributed = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_voters = distributed
            .as_ref()
            .map_or_else(Vec::new, |quorum| quorum.voters.clone());
        if voters.first() != Some(&me) || voters.len() != self.expected_voters {
            *distributed = None;
            drop(distributed);
            self.durable_advanced.notify_waiters();
            if let Some((shard, metrics)) = self.observability.get() {
                metrics.remove_diskless_wal_shard(
                    shard.topic_id,
                    shard.partition,
                    &previous_voters,
                );
            }
            return;
        }
        if distributed
            .as_ref()
            .is_some_and(|current| current.voters == voters)
        {
            return;
        }
        let previous = distributed
            .take()
            .map_or_else(HashMap::new, |current| current.durable_offsets);
        self.remove_voter_observability(&previous_voters);
        let durable_offsets = voters
            .iter()
            .filter_map(|voter| previous.get(voter).copied().map(|offset| (*voter, offset)))
            .collect();
        *distributed = Some(DistributedQuorum {
            me,
            voters: voters.to_vec(),
            durable_offsets,
        });
        if let Some(source) = self.replicas.first() {
            let (log_start, log_end) = {
                let log = source.log.lock();
                (log.log_start_offset(), log.log_end_offset())
            };
            drop(distributed);
            let local_durable = Offset(
                self.local_durable
                    .load(Ordering::Acquire)
                    .clamp(log_start.0, log_end.0),
            );
            self.record_durable_offset(me, local_durable, log_start, log_end);
        }
        self.record_observability();
    }

    /// Record the offset a remote voter requested after its preceding fsync.
    /// Returns `true` when the quorum-durable watermark advanced.
    pub(crate) fn record_follower_ack(&self, from: NodeId, offset: Offset) -> bool {
        let is_remote_voter = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|quorum| quorum.voters.get(1..))
            .is_some_and(|followers| followers.contains(&from));
        if !is_remote_voter {
            return false;
        }
        let Some(source) = self.replicas.first() else {
            return false;
        };
        let (log_start, log_end) = {
            let log = source.log.lock();
            (log.log_start_offset(), log.log_end_offset())
        };
        if !(log_start..=log_end).contains(&offset) {
            return false;
        }
        self.record_durable_offset(from, offset, log_start, log_end)
    }

    /// Mark an adopted checkpointed follower prefix as locally durable.
    /// Promotion has already fsynced and exact-validated this range against
    /// the canonical log. A quorum watermark still advances only after enough
    /// configured voters report the same frontier.
    pub(crate) fn adopt_local_durable_prefix(
        &self,
        durable: Offset,
        log_start: Offset,
        log_end: Offset,
    ) {
        if !(log_start..=log_end).contains(&durable) {
            return;
        }
        self.local_durable.fetch_max(durable.0, Ordering::AcqRel);
        let me = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|quorum| quorum.me);
        if let Some(me) = me {
            self.record_durable_offset(me, durable, log_start, log_end);
        }
    }

    pub(super) fn record_durable_offset(
        &self,
        from: NodeId,
        offset: Offset,
        log_start: Offset,
        leader_end: Offset,
    ) -> bool {
        let mut distributed = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(quorum) = distributed.as_mut() else {
            return false;
        };
        if !quorum.voters.contains(&from) {
            return false;
        }
        let previous = quorum
            .durable_offsets
            .get(&from)
            .copied()
            .unwrap_or(log_start);
        if offset.cmp(&previous).is_gt() {
            quorum.durable_offsets.insert(from, offset);
        }
        let follower_ends = quorum
            .voters
            .iter()
            .filter(|voter| **voter != quorum.me)
            .map(|voter| {
                // A remembered follower offset can outlive the leadership
                // that produced it. Clamp it into the verified kernel's
                // precondition domain for the current leader.
                quorum
                    .durable_offsets
                    .get(voter)
                    .copied()
                    .unwrap_or(log_start)
                    .0
                    .min(leader_end.0)
            })
            .collect::<Vec<_>>();
        let current = self.durable_watermark();
        let verified_current = current.0.min(leader_end.0);
        // A log normally has start <= end. Keep that kernel precondition local
        // even if this internal boundary receives an inconsistent range.
        let durable = Offset(krabka_verified::recompute_high_watermark(
            leader_end.0,
            &follower_ends,
            strict_majority(quorum.voters.len()),
            verified_current,
            log_start.0.min(leader_end.0),
            true,
        ));
        let offset_changed = offset > previous;
        if durable <= current {
            drop(distributed);
            if offset_changed {
                self.record_observability_at(log_start, leader_end);
            }
            return false;
        }
        self.durable_watermark.store(durable.0, Ordering::Release);
        drop(distributed);
        self.durable_advanced.notify_waiters();
        self.record_observability_at(log_start, leader_end);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use krabka_log::{Log, LogConfig};

    use super::*;

    #[test]
    fn bounds_verified_watermark_inputs_to_the_leader_end() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path().join("source"), LogConfig::default()).unwrap();
        let engine = WalShardEngine::new_distributed(Arc::new(Mutex::new(log)), 3).unwrap();
        engine.configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);

        assert!(engine.record_durable_offset(NodeId(2), Offset(2), Offset(0), Offset(1),));
        assert!(!engine.record_durable_offset(NodeId(3), Offset(2), Offset(0), Offset(1),));
        assert!(engine.durable_watermark() == Offset(1));
        assert!(!engine.record_durable_offset(NodeId(2), Offset(2), Offset(2), Offset(1),));
        assert!(engine.durable_watermark() == Offset(1));

        engine
            .durable_watermark
            .store(Offset(2).0, Ordering::Release);
        assert!(!engine.record_durable_offset(NodeId(3), Offset(1), Offset(0), Offset(1),));
        assert!(engine.durable_watermark() == Offset(2));
    }
}
