//! The model configuration and its stateright implementation: the initial
//! state, the enabled actions, the transition, the properties and the search
//! boundary.
//!
//! A `Model` implementation is one indivisible unit — the action generator,
//! the transition and the properties only make sense against each other — so
//! it stays whole in this file, and every transition it takes calls out to the
//! seam modules that wrap the real broker cores.

use std::time::Instant;

use krabka_log::Offset;
use stateright::{Model, Property};

use super::{
    bounds::{MAX_EPOCH, MAX_LEN, NB, NB_U8, has, model_index, model_offset},
    election::do_failover,
    hwm::{real_hwm, real_wal_hwm},
    state::{Act, DpState, isr_eligible},
    truncation::real_truncation_offset,
};
use crate::handlers::fetch::{FetchWatermarks, compute_visibility_window};

pub(super) struct DpModel {
    pub(super) base: Instant,
    pub(super) unclean: bool,  // false in DPC-1/2 (clean), true in DPC-3
    pub(super) diskless: bool, // true drives the WAL durability path instead of ISR-HWM
}

impl Model for DpModel {
    type State = DpState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![DpState {
            log: [vec![], vec![], vec![]],
            hwm: 0,
            leader: 0,
            leader_epoch: 1,
            // Slice 1 diskless is a single-node local-fsync WAL. Keep the RF=3
            // model for classic clean/unclean checks, but constrain diskless to
            // the leader broker so WAL durability is not incorrectly invalidated
            // by electing a different replica that never fsynced the record.
            isr: if self.diskless { 0b001 } else { 0b111 },
            live: if self.diskless { 0b001 } else { 0b111 },
            committed: vec![],
            wal_acked: vec![],
            seq_next: 0,
            assigned: vec![],
            lost: false,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        let leader_live = has(s.live, s.leader);
        // Data-path actions require a live leader.
        if leader_live {
            if s.log[usize::from(s.leader)].len() < MAX_LEN && s.leader_epoch <= MAX_EPOCH {
                acts.push(Act::Produce);
                if self.diskless && s.assigned.len() < 3 {
                    acts.push(Act::Assign(1));
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
            // Re-admit a follower to the ISR only once it is genuinely in-sync:
            // an epoch-consistent prefix of the leader's log (it has truncated +
            // replicated any divergence via the real protocol) AND caught up to
            // the HWM. Checking LEO alone would admit a stale, divergent follower
            // that hasn't reconciled — which is unreachable in real Kafka, where
            // the follower fetch/OffsetForLeaderEpoch loop truncates before its
            // reported progress can make it eligible.
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

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match a {
            Act::Produce => {
                s.log[usize::from(s.leader)].push(s.leader_epoch);
            }
            Act::Assign(count) => {
                let start = s.seq_next;
                let end = start + i64::from(count);
                s.assigned.push((start, end));
                s.seq_next = end;
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
                // HWM = min ISR LEO (real core). Monotonic within a leader epoch
                // by construction (ISR expansion is gated on `leo >= hwm`, shrink
                // only raises the min), but it may legitimately REGRESS on a leader
                // change (KIP-207 — the new leader recomputes from its own ISR's
                // LEOs). So no monotonicity assert: durability is the
                // `committed_durable` property, not HWM monotonicity.
                s.hwm = real_hwm(&s, self.base);
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
                s.isr |= 1 << b;
            }
            Act::Failover(dead) => do_failover(&mut s, dead, self.unclean),
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("committed_durable", |_, s: &DpState| {
                let lg = &s.log[usize::from(s.leader)];
                s.committed
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("wal_acked_durable", |_, s: &DpState| {
                let lg = &s.log[s.leader as usize];
                s.wal_acked
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("hwm_within_leader_log", |_, s: &DpState| {
                s.hwm <= s.leader_leo()
            }),
        ];
        if self.diskless {
            props.extend([
                Property::always("diskless_hw_released_by_wal_sync", |_, s: &DpState| {
                    s.hwm == model_offset(s.wal_acked.len())
                }),
                Property::sometimes("wal_acked_progress", |_, s: &DpState| {
                    !s.wal_acked.is_empty()
                }),
                Property::sometimes("wal_acked_survives_broker_down", |_, s: &DpState| {
                    !has(s.live, s.leader) && !s.wal_acked.is_empty()
                }),
                Property::always("offsets_contiguous_and_unique", |_, s: &DpState| {
                    let mut next = 0;
                    for &(start, end) in &s.assigned {
                        if start != next || end <= start {
                            return false;
                        }
                        next = end;
                    }
                    next == s.seq_next
                }),
            ]);
        } else {
            props.extend([
                Property::sometimes("committed_progress", |_, s: &DpState| {
                    !s.committed.is_empty()
                }),
                Property::sometimes("full_replication", |_, s: &DpState| {
                    s.hwm == s.leader_leo() && s.hwm > 0
                }),
                // A leader change occurred.
                Property::sometimes("leader_changed", |_, s: &DpState| s.leader_epoch >= 2),
                // The ISR shrank below the full replica set.
                Property::sometimes("isr_shrunk", |_, s: &DpState| {
                    u32::from(s.isr).count_ones() < u32::from(NB_U8)
                }),
                // Two brokers hold different epochs at one offset — truncation
                // territory (a follower must truncate to reconcile).
                Property::sometimes("divergence_present", |_, s: &DpState| {
                    (0..MAX_LEN).any(|off| {
                        let mut seen: Option<u8> = None;
                        for b in 0..NB {
                            if let Some(&e) = s.log[b].get(off) {
                                match seen {
                                    None => seen = Some(e),
                                    Some(x) if x != e => return true,
                                    _ => {}
                                }
                            }
                        }
                        false
                    })
                }),
            ]);
        }
        if self.unclean {
            // Loss characterization: an unclean-election data loss is reachable
            // (and `committed_durable` above still holds — `committed` is the LIVE
            // durability obligation, truncated when an unclean election drops it).
            props.push(Property::sometimes("unclean_loss", |_, s: &DpState| s.lost));
        } else {
            // Clean config: NO committed-data loss ever occurs.
            props.push(Property::always("no_loss_when_clean", |_, s: &DpState| {
                !s.lost
            }));
        }
        props
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.iter().all(|l| l.len() <= MAX_LEN) && s.leader_epoch <= MAX_EPOCH + 1
    }
}
