//! One client produce attempt, from the routing decision the client makes to
//! the dedup verdict the leader returns.
//!
//! This is the composition the model exists to check: the client's own retry
//! and sequence bookkeeping runs against `check_pure`, the real broker-side
//! idempotent-producer check, rather than against a restatement of it. The
//! check reads the leader's tracker, which an answer reaches only through a
//! later [`Action::Commit`](super::state::Action::Commit). A batch the check
//! passes goes to the leader's writer, which since #1947 looks it up among
//! the producer's retained batches in the leader's log through
//! [`LoggedProducer::duplicate_of`](crate::producer_state::LoggedProducer::duplicate_of)
//! and answers a repeat with the retained batch's offset.

use {
    super::{
        bounds::{BASE_SEQUENCE, MAX_LOG_LEN, MAX_SEND_ATTEMPTS, PRODUCER_EPOCH, PRODUCER_ID},
        model::Orchestration,
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

    pub fn apply_client_send(
        &self,
        kind: SendKind,
        outcome: RequestOutcome,
        orchestration: Orchestration,
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
        match check_pure(s.tracker.as_ref(), PRODUCER_EPOCH, base_sequence, 0) {
            Decision::Append => {
                if !matches!(
                    outcome,
                    RequestOutcome::AppendedUnacked | RequestOutcome::TimedOutUnknown
                ) {
                    return None;
                }
                let duplicate = match orchestration {
                    Orchestration::WriterLookup => s
                        .logged_producer(s.leader)
                        .duplicate_of(PRODUCER_EPOCH, base_sequence),
                    Orchestration::GateOnly => None,
                };
                let offset = if let Some(offset) = duplicate {
                    // The writer answers a repeat of a retained batch with its
                    // offset and appends nothing.
                    s.witnesses.mark(WITNESS_WRITER_DUPLICATE);
                    offset
                } else {
                    let slot = s.log_len(s.leader);
                    if slot >= MAX_LOG_LEN {
                        return None;
                    }
                    let batch = LogBatch::at(i64::try_from(slot).ok()?);
                    s.logs[s.leader][slot] = Some(batch);
                    batch.offset
                };
                // The request owes the tracker the writer's answer:
                // `finalize_ack` runs `AppendCommit::record`.
                s.pending_commit[usize::try_from(offset).ok()?] = true;
                // The client's batch is the first copy in the leader's log.
                if !s.leader_contains_accepted() {
                    s.accepted = Some(AcceptedBatch {
                        producer_id: PRODUCER_ID,
                        producer_epoch: PRODUCER_EPOCH,
                        base_sequence,
                        offset,
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
