use super::*;

impl Model for TxnModel {
    type State = TxnProj;
    type Action = TxnAction;

    fn init_states(&self) -> Vec<Self::State> {
        // A tid that has completed its first InitProducerId: epoch 0, Empty.
        vec![TxnProj {
            pid: PID.get(),
            epoch: 0,
            state: TxnState::Empty.to_kafka_status(),
            generation: 0,
            pending: None,
            finalized: vec![],
            violations: BTreeSet::new(),
        }]
    }

    fn actions(&self, _: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every action is offered in every state; the production decisions
        // in `next_state` refuse the ones that do not apply.
        actions.extend([
            TxnAction::Init,
            TxnAction::BeginTxn,
            TxnAction::EndTxnPhase1(true),
            TxnAction::EndTxnPhase1(false),
            TxnAction::EndTxnPhase3,
            TxnAction::Complete,
        ]);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            TxnAction::Init => self.init(&mut s)?,
            TxnAction::BeginTxn => Self::begin(&mut s)?,
            TxnAction::EndTxnPhase1(committed) => Self::end_txn_phase1(&mut s, committed)?,
            TxnAction::EndTxnPhase3 => Self::end_txn_phase3(&mut s)?,
            TxnAction::Complete => Self::complete(&mut s)?,
        }
        if action == TxnAction::Init && is_prepared(st(last.state)) {
            s.violations.insert(Violation::InitOverwrotePrepared);
        }
        if s.epoch < last.epoch {
            s.violations.insert(Violation::EpochRegressed);
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: a fenced or overtaken EndTxn never writes Complete*.
            Property::always("fenced_end_txn_never_finalizes", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::FencedEndTxnFinalized)
            }),
            // HEADLINE: each generation finalizes at most once, so it is never
            // both committed and aborted.
            Property::always("finalized_at_most_once", |_, s: &TxnProj| {
                s.finalized
                    .windows(2)
                    .all(|pair| pair[0].generation != pair[1].generation)
            }),
            // HEADLINE: InitProducerId answers CONCURRENT_TRANSACTIONS to a
            // prepared transaction instead of moving it.
            Property::always("init_never_overwrites_prepared", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::InitOverwrotePrepared)
            }),
            // The fence of an `Ongoing` transaction raises the epoch once and
            // stamps what Kafka does at the cluster's version.
            Property::always("fence_matches_kafka", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::FenceDiverged)
            }),
            // Phase 3 rejects only an entry that moved underneath it.
            Property::always("reject_is_justified", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::UnjustifiedReject)
            }),
            Property::always("epoch_never_regresses", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::EpochRegressed)
            }),
            // Non-vacuity: an EndTxn commit and an InitProducerId fence-abort
            // both finalize.
            Property::sometimes("can_commit", |_, s: &TxnProj| {
                s.finalized.iter().any(|f| f.committed)
            }),
            Property::sometimes("can_abort", |_, s: &TxnProj| {
                s.finalized.iter().any(|f| !f.committed)
            }),
            // Non-vacuity: the entry's epoch moves past a pending EndTxn's
            // prepared epoch while it waits for Phase 3 -- the zombie window.
            Property::sometimes("fence_in_window", |_, s: &TxnProj| {
                s.pending.is_some_and(|p| p.expected_epoch < s.epoch)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.epoch <= self.max_epoch
    }
}
