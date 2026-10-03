use super::*;

impl TxnModel {
    /// `InitProducerId` over the live entry, in the handler's order.
    pub(super) fn init(&self, s: &mut TxnProj) -> Option<()> {
        let mut entry = rebuild(s);
        // `pending_completion_response`: a durable `Prepare*` answers
        // `CONCURRENT_TRANSACTIONS` to a request that names no identity, and
        // nothing is written.
        if completion_for(entry.state).is_some() {
            return None;
        }
        if entry.state == TxnState::Ongoing {
            // `prepareFenceProducerEpoch` and the server's abort at the
            // cluster's version: the epoch moves once, whichever version. The
            // client is answered `CONCURRENT_TRANSACTIONS` and retries after
            // the completion.
            let held = entry.producer_epoch;
            entry.state = TxnState::PrepareAbort;
            prepare_server_abort_identities_with_fresh(&mut entry, self.fence_version, None)
                .expect("model epochs never reach the rotation boundary");
            if !fenced_as_kafka(&entry, held, self.fence_version) {
                s.violations.insert(Violation::FenceDiverged);
            }
        } else {
            // `prepareIncrementProducerEpoch` on a terminal or empty entry.
            let (pid, epoch) = krabka_verified::transaction::next_producer_identity(
                true,
                false,
                entry.producer_id.get(),
                entry.producer_epoch,
                None,
            )
            .expect("model epochs never reach the rotation boundary");
            entry = TxnEntry::new_empty("tid".to_string(), ProducerId(pid), epoch, 60_000, 1);
        }
        project(s, &entry);
        Some(())
    }

    pub(super) fn begin(s: &mut TxnProj) -> Option<()> {
        let prior = st(s.state);
        if !prior.can_transition_to(TxnState::Ongoing) {
            return None;
        }
        if prior != TxnState::Ongoing {
            s.generation = s.epoch;
        }
        s.state = TxnState::Ongoing.to_kafka_status();
        Some(())
    }

    pub(super) fn end_txn_phase1(s: &mut TxnProj, committed: bool) -> Option<()> {
        if s.pending.is_some() {
            return None;
        }
        let mut entry = rebuild(s);
        let (prepare, complete) = decide_phase1_transition(&mut entry, committed).ok()?;
        prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        let (completion_pid, completion_epoch) = completion_producer_identity(&entry);
        s.pending = Some(PendingEnd {
            generation: s.generation,
            expected_pid: entry.producer_id.get(),
            expected_epoch: entry.producer_epoch,
            completion_pid: completion_pid.get(),
            completion_epoch,
            prepare: prepare.to_kafka_status(),
            complete: complete.to_kafka_status(),
        });
        project(s, &entry);
        Some(())
    }

    pub(super) fn end_txn_phase3(s: &mut TxnProj) -> Option<()> {
        let p = s.pending.take()?;
        let entry = rebuild(s);
        // Independent of the decision core: is the entry still exactly the
        // snapshot Phase 1 persisted, for the generation it prepared?
        let still_prepared = s.pid == p.expected_pid
            && s.epoch == p.expected_epoch
            && s.state == p.prepare
            && s.generation == p.generation;
        match decide_end_txn_completion(
            &entry,
            ProducerId(p.expected_pid),
            p.expected_epoch,
            ProducerId(p.completion_pid),
            p.completion_epoch,
            st(p.prepare),
            st(p.complete),
        ) {
            CompletionDecision::Proceed {
                next_state,
                response_pid,
                response_epoch,
            } => {
                if !still_prepared {
                    s.violations.insert(Violation::FencedEndTxnFinalized);
                }
                finalize(s, p.generation, next_state);
                s.pid = response_pid.get();
                s.epoch = response_epoch;
                s.state = next_state.to_kafka_status();
            }
            // Idempotent retry or a lost race with the completion task: the
            // handler answers success and writes nothing.
            CompletionDecision::AlreadyComplete { .. } => {}
            CompletionDecision::Reject(_) => {
                if still_prepared {
                    s.violations.insert(Violation::UnjustifiedReject);
                }
            }
        }
        Some(())
    }

    pub(super) fn complete(s: &mut TxnProj) -> Option<()> {
        let entry = rebuild(s);
        let (_, complete) = completion_for(entry.state)?;
        // The model's completion runs atomically, so the prepared snapshot is
        // the live entry.
        match completion_decision(&entry, &entry, (entry.state, complete)) {
            TransactionReaperCompletionDecision::Proceed => {
                let mut completed = entry.clone();
                let identity = completion_producer_identity(&completed);
                apply_completion(&mut completed, complete, identity, 1);
                finalize(s, s.generation, complete);
                project(s, &completed);
                Some(())
            }
            TransactionReaperCompletionDecision::AlreadyComplete
            | TransactionReaperCompletionDecision::RejectMalformed
            | TransactionReaperCompletionDecision::RejectStaleIdentity
            | TransactionReaperCompletionDecision::RejectChangedPreparedState => None,
        }
    }
}
