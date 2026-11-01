//! The state that the search enumerates, the actions that move between two
//! states, and the pure queries over one state that every transition module
//! shares.
//!
//! The types live apart from the transitions so a reader can see the whole
//! search space, including the client's cached routing and the accepted
//! batch, on one screen.

use {
    super::{
        bounds::{BASE_SEQUENCE, MAX_LOG_LEN, NB, PRODUCER_EPOCH, PRODUCER_ID},
        witness::{WITNESS_ACKED_BEFORE_FAILOVER, WITNESS_FAILOVER, Witnesses},
    },
    crate::producer_state::{ProducerEntry, committed_entry},
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct LogBatch {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct AcceptedBatch {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum BatchState {
    Empty,
    Prepared,
    Appended,
    Acked,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ProduceResult {
    NotLeader,
    TimedOutUnknown,
    AppendedUnacked,
    Acked,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RequestOutcome {
    NotLeader,
    AppendedUnacked,
    TimedOutUnknown,
    Duplicate,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FailoverState {
    pub logs: [[Option<LogBatch>; MAX_LOG_LEN]; NB],
    pub leader: usize,
    pub live: u8,
    pub hwm: u8,
    pub cached_leader: usize,
    pub refresh_needed: bool,
    pub batch: BatchState,
    pub next_sequence: i32,
    pub accepted: Option<AcceptedBatch>,
    /// The leader's idempotent-producer tracker: what the dedup check reads.
    pub tracker: Option<ProducerEntry>,
    /// The leader's appends, by offset, whose `acks=all` gate has not resolved,
    /// so the tracker has not committed them yet.
    pub pending_commit: [bool; MAX_LOG_LEN],
    pub acked_offset: Option<i64>,
    pub last_result: Option<ProduceResult>,
    pub send_attempts: u8,
    pub metadata_refreshes: u8,
    pub witnesses: Witnesses,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Action {
    ClientSend(RequestOutcome),
    ClientRetry(RequestOutcome),
    Replicate(usize),
    AdvanceHwm,
    AckCommitted,
    KillLeader,
    ElectClean(usize),
    RefreshMetadata,
    /// The gate of the leader's append at this offset resolves, replicated or
    /// timed out, and the leader commits the append into its tracker.
    Commit(usize),
}

impl LogBatch {
    pub fn at(offset: i64) -> Self {
        Self {
            producer_id: PRODUCER_ID,
            producer_epoch: PRODUCER_EPOCH,
            base_sequence: BASE_SEQUENCE,
            offset,
        }
    }
}

/// The tracker after the commit of `batch`: `ProducerState::commit`'s rule.
pub fn commit_into(tracker: Option<ProducerEntry>, batch: LogBatch) -> Option<ProducerEntry> {
    committed_entry(
        tracker,
        (batch.producer_id, batch.producer_epoch),
        (batch.base_sequence, 0),
        (batch.offset, 0, false),
    )
    .or(tracker)
}

impl FailoverState {
    pub fn live(&self, broker: usize) -> bool { self.live & (1 << broker) != 0 }

    pub fn live_count(&self) -> u32 { self.live.count_ones() }

    pub fn log_len(&self, broker: usize) -> usize { self.logs[broker].iter().flatten().count() }

    pub fn log_contains_base(&self, broker: usize) -> bool {
        self.logs[broker]
            .iter()
            .flatten()
            .any(|batch| batch.producer_id == PRODUCER_ID && batch.base_sequence == BASE_SEQUENCE)
    }

    pub fn contains_hwm_prefix(&self, broker: usize) -> bool {
        self.hwm == 0 || self.log_len(broker) >= usize::from(self.hwm)
    }

    pub fn hwm_prefix_replicated(&self) -> bool {
        self.live(self.leader)
            && self.log_len(self.leader) > 0
            && (0..NB)
                .filter(|broker| self.live(*broker) && self.logs[*broker] == self.logs[self.leader])
                .count()
                >= 2
    }

    /// The producer state of `broker`'s log. The log tracks every batch as it
    /// appends it, so this is each batch committed in offset order.
    pub fn log_producer_entry(&self, broker: usize) -> Option<ProducerEntry> {
        self.logs[broker]
            .iter()
            .flatten()
            .filter(|batch| batch.producer_id == PRODUCER_ID)
            .fold(None, |tracker, batch| commit_into(tracker, *batch))
    }

    /// The new leader's tracker, which `Partition::install_local_leadership`
    /// replaces with the state of its log on promotion. The gates of the
    /// previous leader's appends resolve on that leader, not on this one.
    pub fn install_leader_tracker(&mut self) {
        self.tracker = self.log_producer_entry(self.leader);
        self.pending_commit = [false; MAX_LOG_LEN];
    }

    /// Whether the tracker has yet to commit the leader's append at `offset`.
    pub fn commit_pending(&self, offset: i64) -> bool {
        usize::try_from(offset).is_ok_and(|index| self.pending_commit.get(index) == Some(&true))
    }

    pub fn leader_contains_accepted(&self) -> bool {
        let Some(accepted) = self.accepted else {
            return false;
        };
        self.logs[self.leader].iter().flatten().any(|batch| {
            batch.producer_id == accepted.producer_id
                && batch.producer_epoch == accepted.producer_epoch
                && batch.base_sequence == accepted.base_sequence
                && batch.offset == accepted.offset
        })
    }

    /// Whether the client can take the acknowledgement of its append. The
    /// leader answers only after the gate resolves and the commit runs.
    pub fn can_ack_committed(&self) -> bool {
        self.batch == BatchState::Appended
            && self.acked_offset.is_none()
            && self.hwm == 1
            && self.live(self.leader)
            && self.leader_contains_accepted()
            && self
                .accepted
                .is_some_and(|accepted| !self.commit_pending(accepted.offset))
    }

    pub fn cached_leader_current(&self) -> bool {
        self.cached_leader == self.leader && self.live(self.cached_leader)
    }

    pub fn mark_failover(&mut self) {
        if self.acked_offset.is_some() {
            self.witnesses.mark(WITNESS_ACKED_BEFORE_FAILOVER);
        }
        self.witnesses.mark(WITNESS_FAILOVER);
    }
}
