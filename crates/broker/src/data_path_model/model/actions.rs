use super::*;

impl DpModel {
    pub(super) fn model_init_states(&self) -> Vec<<Self as Model>::State> {
        vec![DpState {
            log: [vec![], vec![], vec![]],
            hwm: 0,
            leader: 0,
            leader_epoch: 1,
            // The diskless runtime foundation is a single-node local-fsync WAL. Keep the RF=3
            // model for classic clean/unclean checks, but constrain diskless to
            // the leader broker so WAL durability is not incorrectly invalidated
            // by electing a different replica that never fsynced the record.
            isr: if self.diskless { 0b001 } else { 0b111 },
            live: if self.diskless { 0b001 } else { 0b111 },
            // A partition whose ISR meets min ISR has no eligible-leader set,
            // and every configuration starts with a full ISR.
            elr: 0,
            committed: vec![],
            wal_acked: vec![],
            seq_next: 0,
            assigned: vec![],
            lost: false,
            elr_trace: 0,
        }]
    }
}

impl DpModel {
    pub(super) fn model_actions(
        &self,
        s: &<Self as Model>::State,
        acts: &mut Vec<<Self as Model>::Action>,
    ) {
        let leader_live = has(s.live, s.leader);
        // Data-path actions require a live leader.
        if leader_live {
            if s.log[usize::from(s.leader)].len() < self.max_len && s.leader_epoch <= MAX_EPOCH {
                acts.push(Act::Produce);
                if self.diskless && s.assigned.len() < 3 {
                    acts.push(Act::Assign(1));
                    acts.push(Act::Assign(2));
                }
            }
            if self.diskless && s.wal_acked.len() < s.log[s.leader as usize].len() {
                acts.push(Act::WalSync);
            }
            if !self.diskless {
                for b in 0..NB_U8 {
                    if b != s.leader
                        && has(s.live, b)
                        && model_offset(s.log[usize::from(b)].len()) < s.leader_leo()
                    {
                        acts.push(Act::Replicate(b));
                    }
                }
                acts.push(Act::AdvanceHwm);
            }
            for fo in 0..=s.leader_leo() {
                acts.push(Act::ConsumerFetch {
                    read_committed: false,
                    fetch_offset: fo,
                });
                acts.push(Act::ConsumerFetch {
                    read_committed: true,
                    fetch_offset: fo,
                });
            }
        }
        // Liveness + failover.
        let live_count = u32::from(s.live).count_ones();
        for b in 0..NB_U8 {
            if self.diskless && b != s.leader {
                continue;
            }
            if has(s.live, b) && (live_count > 1 || self.diskless) {
                acts.push(Act::Die(b));
            }
            if !has(s.live, b) {
                acts.push(Act::Revive(b));
                // Controller failover: elect (dead leader, epoch headroom) or
                // shrink the ISR (dead non-leader ISR member).
                if !self.diskless
                    && ((b == s.leader && s.leader_epoch < MAX_EPOCH)
                        || (b != s.leader && has(s.isr, b)))
                {
                    acts.push(Act::Failover(b));
                }
            }
            // Re-admit a follower to the ISR only once the leader's real
            // expansion rule admits it: an epoch-consistent prefix of the
            // leader's log (it has truncated + replicated any divergence via
            // the real protocol) whose log end reaches the HWM and the start of
            // the leader's epoch. Checking LEO alone would admit a stale,
            // divergent follower that hasn't reconciled — which is unreachable
            // in real Kafka, where the follower fetch/OffsetForLeaderEpoch loop
            // truncates before its reported progress can make it eligible.
            if !self.diskless
                && has(s.live, b)
                && b != s.leader
                && !has(s.isr, b)
                && isr_eligible(s, b)
            {
                acts.push(Act::ExpandIsr(b));
            }
        }
    }
}
