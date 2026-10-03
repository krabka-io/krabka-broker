use super::*;

impl Model for TwoPcModel {
    type State = TwoPcProj;
    type Action = TwoPcAction;

    fn init_states(&self) -> Vec<Self::State> {
        // A tid that completed its first, classic InitProducerId at time 0.
        let timeout_ms = resolve_txn_timeout(false, FIRST_TIMEOUT_MS, MAX_TIMEOUT_MS)
            .expect("the first timeout is valid");
        let entry = TxnEntry::new_empty("tid".to_string(), PID, 0, timeout_ms, CLOCK[0]);
        let mut s = TwoPcProj {
            pid: 0,
            epoch: 0,
            state: 0,
            timeout_ms: 0,
            start_ms: 0,
            clock: 0,
            enable_2pc: false,
            requested_ms: FIRST_TIMEOUT_MS,
            generation: 0,
            last_finalized: None,
            violations: BTreeSet::new(),
            witnesses: BTreeSet::new(),
        };
        project(&mut s, &entry);
        vec![s]
    }

    fn actions(&self, _: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every action is offered in every state; the production decisions
        // in `next_state` refuse the ones that do not apply.
        for requested in REQUESTS {
            actions.push(TwoPcAction::Init(false, requested));
        }
        // The requested timeout is not read under 2PC, valid or not.
        actions.push(TwoPcAction::Init(true, FIRST_TIMEOUT_MS));
        actions.push(TwoPcAction::Init(true, 0));
        actions.extend([
            TwoPcAction::BeginTxn,
            TwoPcAction::EndTxn(true),
            TwoPcAction::EndTxn(false),
            TwoPcAction::Complete,
            TwoPcAction::TimeoutSweep,
            TwoPcAction::Tick,
        ]);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            TwoPcAction::Init(enable_2pc, requested) => self.init(&mut s, enable_2pc, requested)?,
            TwoPcAction::BeginTxn => Self::begin(&mut s)?,
            TwoPcAction::EndTxn(committed) => Self::end_txn(&mut s, committed)?,
            TwoPcAction::Complete => Self::complete(&mut s)?,
            TwoPcAction::TimeoutSweep => self.sweep(&mut s)?,
            TwoPcAction::Tick => {
                if s.clock + 1 >= CLOCK.len() {
                    return None;
                }
                s.clock += 1;
            }
        }
        if s.epoch < last.epoch {
            s.violations.insert(Violation::EpochRegressed);
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE (KIP-939): the timeout reaper never aborts a 2PC txn.
            Property::always("two_pc_never_reaped", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::ReapedTwoPc)
            }),
            // The reaper aborts exactly when Kafka's rule times out.
            Property::always("reaper_never_early", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::ReapedEarly)
            }),
            Property::always("reaper_never_misses", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::MissedReap)
            }),
            // Generations finalize once each, in order.
            Property::always("finalized_at_most_once", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::FinalizedOutOfOrder)
            }),
            Property::always("epoch_never_regresses", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::EpochRegressed)
            }),
            // The abort the coordinator runs on an `Ongoing` transaction raises
            // the epoch once and stamps what Kafka does at the cluster's
            // version.
            Property::always("fence_matches_kafka", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::FenceDiverged)
            }),
            // Non-vacuity: the reaper aborts classic transactions, and does so
            // one millisecond past the timeout but not at it.
            Property::sometimes("reaper_aborts_classic", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::ReapedClassic)
            }),
            Property::sometimes("reaped_one_past_timeout", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::ReapedOnePastTimeout)
            }),
            Property::sometimes("spared_at_timeout", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::SparedAtTimeout)
            }),
            // Non-vacuity: a 2PC transaction stays open past the instant its
            // sentinel timeout would have expired.
            Property::sometimes("two_pc_open_past_timeout", |_, s: &TwoPcProj| {
                s.enable_2pc
                    && st(s.state) == TxnState::Ongoing
                    && i128::from(s.start_ms) + i128::from(i32::MAX) < i128::from(CLOCK[s.clock])
            }),
            Property::sometimes("can_commit", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::Committed)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.epoch <= self.max_epoch
    }
}
