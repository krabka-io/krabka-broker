use super::*;

impl TwoPcModel {
    pub(super) fn init(
        &self,
        s: &mut TwoPcProj,
        enable_2pc: bool,
        requested_ms: i32,
    ) -> Option<()> {
        // The handler resolves the timeout before it reads the entry, and
        // answers INVALID_TRANSACTION_TIMEOUT when Kafka refuses it.
        let timeout_ms = resolve_txn_timeout(enable_2pc, requested_ms, MAX_TIMEOUT_MS).ok()?;
        let mut entry = rebuild(s);
        // `pending_completion_response`: CONCURRENT_TRANSACTIONS, no write.
        if completion_for(entry.state).is_some() {
            return None;
        }
        if entry.state == TxnState::Ongoing {
            // `prepareFenceProducerEpoch` and the server's abort at the
            // cluster's version. The client retries after the completion, so
            // this call does not apply its own timeout.
            let held = entry.producer_epoch;
            entry.state = TxnState::PrepareAbort;
            prepare_server_abort_identities_with_fresh(&mut entry, self.fence_version, None)
                .expect("model epochs never reach the rotation boundary");
            if !fenced_as_kafka(&entry, held, self.fence_version) {
                s.violations.insert(Violation::FenceDiverged);
            }
        } else {
            let (pid, epoch) = krabka_verified::transaction::next_producer_identity(
                true,
                false,
                entry.producer_id.get(),
                entry.producer_epoch,
                None,
            )
            .expect("model epochs never reach the rotation boundary");
            entry = TxnEntry::new_empty(
                "tid".to_string(),
                ProducerId(pid),
                epoch,
                timeout_ms,
                CLOCK[s.clock],
            );
            s.enable_2pc = enable_2pc;
            s.requested_ms = requested_ms;
        }
        project(s, &entry);
        Some(())
    }

    pub(super) fn begin(s: &mut TwoPcProj) -> Option<()> {
        let prior = st(s.state);
        if !prior.can_transition_to(TxnState::Ongoing) {
            return None;
        }
        if prior != TxnState::Ongoing {
            s.start_ms = CLOCK[s.clock];
            s.generation = s.epoch;
        }
        s.state = TxnState::Ongoing.to_kafka_status();
        Some(())
    }

    pub(super) fn end_txn(s: &mut TwoPcProj, committed: bool) -> Option<()> {
        let mut entry = rebuild(s);
        decide_phase1_transition(&mut entry, committed).ok()?;
        prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        project(s, &entry);
        Some(())
    }

    pub(super) fn complete(s: &mut TwoPcProj) -> Option<()> {
        let entry = rebuild(s);
        let (_, complete) = completion_for(entry.state)?;
        match completion_decision(&entry, &entry, (entry.state, complete)) {
            TransactionReaperCompletionDecision::Proceed => {}
            TransactionReaperCompletionDecision::AlreadyComplete
            | TransactionReaperCompletionDecision::RejectMalformed
            | TransactionReaperCompletionDecision::RejectStaleIdentity
            | TransactionReaperCompletionDecision::RejectChangedPreparedState => return None,
        }
        let mut completed = entry.clone();
        let identity = completion_producer_identity(&completed);
        apply_completion(&mut completed, complete, identity, CLOCK[s.clock]);
        if s.last_finalized.is_some_and(|last| s.generation <= last) {
            s.violations.insert(Violation::FinalizedOutOfOrder);
        }
        s.last_finalized = Some(s.generation);
        if complete == TxnState::CompleteCommit {
            s.witnesses.insert(Witness::Committed);
        }
        project(s, &completed);
        Some(())
    }

    pub(super) fn sweep(&self, s: &mut TwoPcProj) -> Option<()> {
        let now_ms = CLOCK[s.clock];
        let mut entry = rebuild(s);
        let reaps =
            should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms);
        let kafka = kafka_timed_out(s, now_ms);
        if !reaps {
            if kafka {
                s.violations.insert(Violation::MissedReap);
                return Some(());
            }
            if st(s.state) == TxnState::Ongoing
                && !s.enable_2pc
                && i128::from(s.start_ms) + i128::from(s.requested_ms) == i128::from(now_ms)
                && !s.witnesses.contains(&Witness::SparedAtTimeout)
            {
                s.witnesses.insert(Witness::SparedAtTimeout);
                return Some(());
            }
            return None;
        }
        if s.enable_2pc {
            s.violations.insert(Violation::ReapedTwoPc);
        }
        if !kafka {
            s.violations.insert(Violation::ReapedEarly);
        }
        if !s.enable_2pc {
            s.witnesses.insert(Witness::ReapedClassic);
        }
        if i128::from(s.start_ms) + i128::from(s.requested_ms) + 1 == i128::from(now_ms) {
            s.witnesses.insert(Witness::ReapedOnePastTimeout);
        }
        // `apply_prepare_abort`, then the server's abort at the cluster's
        // version.
        let held = entry.producer_epoch;
        entry.state = TxnState::PrepareAbort;
        prepare_server_abort_identities_with_fresh(&mut entry, self.fence_version, None)
            .expect("model epochs never reach the rotation boundary");
        if !fenced_as_kafka(&entry, held, self.fence_version) {
            s.violations.insert(Violation::FenceDiverged);
        }
        project(s, &entry);
        Some(())
    }
}
