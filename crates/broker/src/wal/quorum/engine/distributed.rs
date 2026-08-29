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
    /// Switch this shard from the compatibility-only local replica harness to
    /// the metadata-selected broker voter set. Production registries call this
    /// before the shard can acknowledge another append.
    pub(crate) fn configure_distributed(&self, me: NodeId, voters: &[NodeId]) {
        self.distributed_required.store(true, Ordering::Release);
        let mut distributed = self
            .distributed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if voters.first() != Some(&me) || voters.len() != self.expected_voters {
            *distributed = None;
            drop(distributed);
            self.durable_advanced.notify_waiters();
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
                quorum
                    .durable_offsets
                    .get(voter)
                    .copied()
                    .unwrap_or(log_start)
                    .0
            })
            .collect::<Vec<_>>();
        let current = self.durable_watermark();
        let durable = Offset(krabka_verified::recompute_high_watermark(
            leader_end.0,
            &follower_ends,
            strict_majority(quorum.voters.len()),
            current.0,
            log_start.0,
        ));
        if durable <= current {
            return false;
        }
        self.durable_watermark.store(durable.0, Ordering::Release);
        drop(distributed);
        self.durable_advanced.notify_waiters();
        true
    }
}
