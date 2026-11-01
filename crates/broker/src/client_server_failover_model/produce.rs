//! One client produce attempt, from the routing decision the client makes to
//! the dedup verdict the leader returns.
//!
//! This is the composition the model exists to check: the client's own retry
//! and sequence bookkeeping runs against `check_pure`, the real broker-side
//! idempotent-producer check, rather than against a restatement of it. The
//! check reads the leader's tracker, which an append reaches only through a
//! later [`Action::Commit`](super::state::Action::Commit). A batch the check
//! lets through then reaches the writer, which looks it up in the leader's log
//! under the append lock (`duplicate_of` in `partition_writer/append.rs`) and
//! answers a copy with the offset already there.

use {
    super::{
        bounds::{BASE_SEQUENCE, MAX_LOG_LEN, MAX_SEND_ATTEMPTS, PRODUCER_EPOCH, PRODUCER_ID},
        state::{
            AcceptedBatch, BatchState, FailoverState, LogBatch, ProduceResult, RequestOutcome,
        },
        witness::{
            WITNESS_APPENDED_UNACKED, WITNESS_DUPLICATE_AFTER_UNKNOWN, WITNESS_DUPLICATE_RESPONSE,
            WITNESS_FAILOVER, WITNESS_NOT_LEADER, WITNESS_PREPARED_RETRY, WITNESS_RETRY,
            WITNESS_RETRY_AFTER_FAILOVER, WITNESS_RETRY_DURING_PENDING_COMMIT,
            WITNESS_TIMED_OUT_UNKNOWN, WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER,
            WITNESS_WRITER_DUPLICATE,
        },
    },
    crate::producer_state::{Decision, check_pure},
};

/// Whether the writer looks a batch up in the leader's log before it appends.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WriterLookup {
    /// It does, under the append lock, as production does.
    UnderLock,
    /// It appends every batch the check lets through, as the broker did
    /// before #1947.
    Off,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    Send,
    Retry,
}

impl FailoverState {
    pub fn request_base_sequence(&mut self, kind: SendKind) -> Option<i32> {
        let base_sequence = match self.batch {
            BatchState::Empty => {
                if kind == SendKind::Retry {
                    return None;
                }
                BASE_SEQUENCE
            }
            BatchState::Prepared => {
                if kind == SendKind::Retry {
                    self.witnesses.mark(WITNESS_RETRY);
                    self.witnesses.mark(WITNESS_PREPARED_RETRY);
                    if self.witnesses.seen(WITNESS_FAILOVER) {
                        self.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                    }
                }
                BASE_SEQUENCE
            }
            BatchState::Appended | BatchState::Acked => {
                if kind == SendKind::Send {
                    return None;
                }
                self.witnesses.mark(WITNESS_RETRY);
                if self.witnesses.seen(WITNESS_FAILOVER) {
                    self.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                }
                BASE_SEQUENCE
            }
            BatchState::Failed => return None,
        };
        Some(base_sequence)
    }

    /// The offset of the batch's copy in the leader's log, which the writer's
    /// lookup answers with, or `None` when the writer appends.
    fn writer_duplicate(&self, lookup: WriterLookup) -> Option<i64> {
        if lookup == WriterLookup::Off {
            return None;
        }
        self.logs[self.leader]
            .iter()
            .flatten()
            .find(|batch| {
                batch.producer_id == PRODUCER_ID
                    && batch.producer_epoch == PRODUCER_EPOCH
                    && batch.base_sequence == BASE_SEQUENCE
            })
            .map(|batch| batch.offset)
    }

    pub fn apply_client_send(
        &self,
        lookup: WriterLookup,
        (kind, outcome): (SendKind, RequestOutcome),
    ) -> Option<Self> {
        if self.send_attempts >= MAX_SEND_ATTEMPTS {
            return None;
        }

        let mut s = self.clone();
        let base_sequence = s.request_base_sequence(kind)?;
        s.send_attempts = s.send_attempts.saturating_add(1);
        if s.batch == BatchState::Empty {
            s.batch = BatchState::Prepared;
        }

        if !s.cached_leader_current() {
            if outcome != RequestOutcome::NotLeader {
                return None;
            }
            s.refresh_needed = true;
            s.last_result = Some(ProduceResult::NotLeader);
            s.witnesses.mark(WITNESS_NOT_LEADER);
            return Some(s);
        } else if outcome == RequestOutcome::NotLeader {
            return None;
        }

        if kind == SendKind::Retry
            && s.leader_contains_accepted()
            && s.accepted
                .is_some_and(|accepted| s.commit_pending(accepted.offset))
        {
            s.witnesses.mark(WITNESS_RETRY_DURING_PENDING_COMMIT);
        }
        let checked = check_pure(s.tracker.as_ref(), PRODUCER_EPOCH, base_sequence, 0);
        let writer_offset = (checked == Decision::Append)
            .then(|| s.writer_duplicate(lookup))
            .flatten();
        let writer_duplicate = writer_offset.is_some();
        match writer_offset.map_or(checked, |base_offset| Decision::Duplicate { base_offset }) {
            Decision::Append => {
                if !matches!(
                    outcome,
                    RequestOutcome::AppendedUnacked | RequestOutcome::TimedOutUnknown
                ) {
                    return None;
                }
                let slot = s.log_len(s.leader);
                if slot >= MAX_LOG_LEN {
                    return None;
                }
                let batch = LogBatch::at(i64::try_from(slot).ok()?);
                s.logs[s.leader][slot] = Some(batch);
                // The commit waits for the `acks=all` gate: `PendingAck::finish`
                // runs `AppendCommit::record` after it resolves.
                s.pending_commit[slot] = true;
                // The client's batch is the first copy in the leader's log.
                if !s.leader_contains_accepted() {
                    s.accepted = Some(AcceptedBatch {
                        producer_id: PRODUCER_ID,
                        producer_epoch: PRODUCER_EPOCH,
                        base_sequence,
                        offset: batch.offset,
                    });
                }
                s.batch = BatchState::Appended;
                s.next_sequence = base_sequence + 1;
                match outcome {
                    RequestOutcome::AppendedUnacked => {
                        s.last_result = Some(ProduceResult::AppendedUnacked);
                        s.witnesses.mark(WITNESS_APPENDED_UNACKED);
                    }
                    RequestOutcome::TimedOutUnknown => {
                        s.last_result = Some(ProduceResult::TimedOutUnknown);
                        s.witnesses.mark(WITNESS_TIMED_OUT_UNKNOWN);
                    }
                    RequestOutcome::NotLeader | RequestOutcome::Duplicate => return None,
                }
            }
            Decision::Duplicate { base_offset } => {
                if outcome != RequestOutcome::Duplicate {
                    return None;
                }
                let accepted = s.accepted?;
                if accepted.producer_id != PRODUCER_ID
                    || accepted.producer_epoch != PRODUCER_EPOCH
                    || accepted.base_sequence != base_sequence
                {
                    return None;
                }
                if !s.leader_contains_accepted() {
                    return None;
                }
                s.witnesses.mark(WITNESS_DUPLICATE_RESPONSE);
                if writer_duplicate {
                    // `finalize_ack` commits the writer's answer before the
                    // `acks=all` gate, as it does a fresh append's.
                    s.witnesses.mark(WITNESS_WRITER_DUPLICATE);
                    *s.pending_commit
                        .get_mut(usize::try_from(base_offset).ok()?)? = true;
                }
                if self.last_result == Some(ProduceResult::TimedOutUnknown) {
                    s.witnesses.mark(WITNESS_DUPLICATE_AFTER_UNKNOWN);
                    if self.witnesses.seen(WITNESS_FAILOVER) {
                        s.witnesses.mark(WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER);
                    }
                }
                // A duplicate under `acks=all` waits for the high watermark to
                // cover the batch it repeats.
                if i64::from(s.hwm) > base_offset {
                    s.acked_offset = Some(base_offset);
                    s.batch = BatchState::Acked;
                    s.last_result = Some(ProduceResult::Acked);
                } else {
                    s.batch = BatchState::Appended;
                    s.last_result = Some(ProduceResult::AppendedUnacked);
                    s.witnesses.mark(WITNESS_APPENDED_UNACKED);
                }
            }
            Decision::OutOfOrder | Decision::Fenced => {
                s.batch = BatchState::Failed;
            }
        }
        Some(s)
    }
}
