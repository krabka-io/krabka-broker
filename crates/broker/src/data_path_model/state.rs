//! The state the search enumerates, the actions that move between two states,
//! and the in-sync predicate that both the action generator and the ISR
//! bookkeeping read.
//!
//! `DpState` carries the per-broker logs and the leadership bookkeeping
//! alongside the ghost fields — `committed`, `wal_acked`, `assigned` and
//! `lost` — that no broker holds but that the properties are stated over. Its
//! equality and hash go through one projection so that a ghost field added to
//! the struct cannot silently drop out of state identity.

use std::hash::{Hash, Hasher};

use super::bounds::{NB, model_offset};

#[derive(Clone, Debug)]
pub(super) struct DpState {
    pub(super) log: [Vec<u8>; NB], // log[b] = Vec<epoch>; offset = index
    pub(super) hwm: i64,           // leader-authoritative high watermark
    pub(super) leader: u8,
    pub(super) leader_epoch: u8,
    pub(super) isr: u8,                   // bitmask over brokers
    pub(super) live: u8,                  // bitmask over brokers
    pub(super) committed: Vec<u8>,        // ghost: committed[off] = epoch, for offsets ever <= hwm
    pub(super) wal_acked: Vec<u8>, // ghost: wal_acked[off] = epoch, for offsets made WAL-durable (diskless mode)
    pub(super) seq_next: i64,      // ghost: controller's next assignable diskless offset
    pub(super) assigned: Vec<(i64, i64)>, // ghost: half-open assigned ranges
    pub(super) lost: bool,         // ghost: an unclean loss has occurred
}

type StateProjection = (
    Vec<Vec<u8>>,
    i64,
    u8,
    u8,
    u8,
    u8,
    Vec<u8>,
    Vec<u8>,
    i64,
    Vec<(i64, i64)>,
    bool,
);

impl DpState {
    pub(super) fn leader_leo(&self) -> i64 {
        model_offset(self.log[usize::from(self.leader)].len())
    }
    fn proj(&self) -> StateProjection {
        (
            self.log.to_vec(),
            self.hwm,
            self.leader,
            self.leader_epoch,
            self.isr,
            self.live,
            self.committed.clone(),
            self.wal_acked.clone(),
            self.seq_next,
            self.assigned.clone(),
            self.lost,
        )
    }
}
impl PartialEq for DpState {
    fn eq(&self, o: &Self) -> bool {
        self.proj() == o.proj()
    }
}
impl Eq for DpState {}
impl Hash for DpState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum Act {
    Produce,
    Assign(u8),
    Replicate(u8), // follower b fetches one step from the leader
    AdvanceHwm,
    WalSync, // diskless: make the leader's appended prefix fsync-durable
    ConsumerFetch {
        read_committed: bool,
        fetch_offset: i64,
    },
    Die(u8),
    Revive(u8),
    Failover(u8),  // controller reacts to broker `b` being down
    ExpandIsr(u8), // re-admit a caught-up follower to the ISR
}

/// Whether follower `b` is genuinely in-sync and may be admitted or re-admitted
/// to the ISR. Its log must be an epoch-consistent prefix of the leader's, with
/// no unreconciled divergence, AND it must be caught up to at least the HWM.
/// This mirrors the real invariant. A follower's reported progress is only ever
/// post-truncation consistent, so a divergent follower can never appear
/// caught-up.
pub(super) fn isr_eligible(s: &DpState, b: u8) -> bool {
    let f = &s.log[usize::from(b)];
    let l = &s.log[usize::from(s.leader)];
    model_offset(f.len()) >= s.hwm && f.iter().enumerate().all(|(off, &e)| l.get(off) == Some(&e))
}
