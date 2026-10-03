use super::*;

impl DpModel {
    // Keep the Model transition signature and body unchanged in this split.
    #[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
    pub(super) fn model_next_state(
        &self,
        last: &<Self as Model>::State,
        a: <Self as Model>::Action,
    ) -> Option<<Self as Model>::State> {
        let mut s = last.clone();
        match a {
            Act::Produce => {
                s.log[usize::from(s.leader)].push(s.leader_epoch);
            }
            Act::Assign(count) => {
                let (base, next) = self.reserve(s.seq_next, i64::from(count));
                s.assigned.push((base, base + i64::from(count)));
                s.seq_next = next;
            }
            Act::Replicate(b) => {
                let leader_log = s.log[usize::from(s.leader)].clone();
                let trunc =
                    model_index(real_truncation_offset(&s.log[usize::from(b)], &leader_log));
                s.log[usize::from(b)].truncate(trunc);
                if s.log[usize::from(b)].len() < leader_log.len() {
                    let off = s.log[usize::from(b)].len();
                    s.log[usize::from(b)].push(leader_log[off]);
                }
            }
            Act::AdvanceHwm => {
                // The real core: frozen while the ISR is under min ISR (Kafka's
                // `Partition.maybeIncrementLeaderHW`), otherwise the minimum ISR
                // LEO if that is higher. It never falls within one leadership;
                // only an election that drops records lowers it, in
                // `apply_elect`. Every record it passes therefore reached the
                // HWM with at least min ISR replicas holding it, which is the
                // obligation KIP-966's eligible-leader set is a claim about.
                s.hwm = real_hwm(&s, self.base, self.min_isr);
                let leader_log = &s.log[usize::from(s.leader)];
                while model_offset(s.committed.len()) < s.hwm {
                    let off = s.committed.len();
                    s.committed.push(leader_log[off]);
                }
            }
            Act::WalSync => {
                // fsync makes the leader's appended prefix durable and releases
                // it through the same HW seam the broker's diskless path uses.
                let leader_log = &s.log[s.leader as usize];
                while s.wal_acked.len() < leader_log.len() {
                    let off = s.wal_acked.len();
                    s.wal_acked.push(leader_log[off]);
                }
                s.hwm = real_wal_hwm(s.leader, model_offset(s.wal_acked.len()), self.base);
                while model_offset(s.committed.len()) < s.hwm {
                    let off = s.committed.len();
                    s.committed.push(leader_log[off]);
                }
            }
            Act::ConsumerFetch {
                read_committed,
                fetch_offset,
            } => {
                let leader_log_len = s.leader_leo();
                let vw = compute_visibility_window(
                    false, // consumer, not follower
                    read_committed,
                    FetchWatermarks {
                        log_start: Offset(0),
                        hw: Offset(s.hwm),
                        lso: Offset(s.hwm), // lso = hwm (no txns in v1)
                        log_end: Offset(leader_log_len),
                        // This model's topic delivers immediately.
                        deliverable: Offset(s.hwm),
                    },
                    Offset(fetch_offset),
                );
                assert2::assert!(
                    vw.limit_offset <= s.hwm,
                    "consumer limit {} exceeds HWM {}",
                    vw.limit_offset,
                    s.hwm
                );
                assert2::assert!(vw.response_hw == s.hwm, "response_hw drift");
            }
            Act::Die(b) => {
                s.live &= !(1 << b);
            }
            Act::Revive(b) => {
                s.live |= 1 << b;
            }
            Act::ExpandIsr(b) => {
                let previous = elr::partition_record(s.leader, s.isr, s.leader_epoch);
                s.isr |= 1 << b;
                // An ISR change is a partition change, and every controller
                // path that submits one runs the ELR publisher over it. An
                // expansion back to min ISR is how the set empties again.
                if self.tracks_elr() {
                    elr::maintain(&self.image, &mut s, &previous);
                }
            }
            Act::Failover(dead) => do_failover(
                self.tracks_elr().then_some(&self.image),
                &mut s,
                dead,
                self.unclean,
            ),
        }
        Some(s)
    }
}
