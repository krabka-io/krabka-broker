use super::*;

impl Model for BucketModel {
    type State = BucketState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        let Config { rate, burst } = self.configs[0];
        vec![BucketState {
            rate,
            burst,
            available: burst,
            last_refill: 0,
            mirror: rate,
            locked: false,
            generation: 0,
            now: 0,
            resets: 0,
            consumers: vec![Consumer::Idle; self.consumers],
            resetter: Resetter::Idle,
            epoch: 0,
            t0: 0,
            base: burst,
            granted: 0,
            capped: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.now < self.max_time {
            actions.push(Act::Tick);
        }
        for (consumer, pc) in s.consumers.iter().enumerate() {
            if matches!(pc, Consumer::Idle) {
                for req in 0..=self.max_req {
                    actions.push(Act::StartConsume { consumer, req });
                }
            } else {
                actions.push(Act::StepConsumer(consumer));
            }
        }
        if matches!(s.resetter, Resetter::Idle) {
            if s.resets < self.max_resets {
                for config in 0..self.configs.len() {
                    actions.push(Act::StartReset { config });
                }
            }
        } else {
            actions.push(Act::StepResetter);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::Tick => s.now += 1,
            Act::StartConsume { consumer, req } => {
                s.consumers[consumer] = Consumer::FastRate { req };
            }
            Act::StartReset { config } => {
                let config = self.configs[config];
                s.resets += 1;
                s.resetter = match self.algorithm {
                    Algorithm::Locked => Resetter::Acquire { config },
                    Algorithm::SeqlockCas => Resetter::Enter { config },
                };
            }
            Act::StepConsumer(t) => self.step_consumer(&mut s, t)?,
            Act::StepResetter => Self::step_resetter(&mut s)?,
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("available_within_burst", |_, s: &BucketState| {
                available_within_burst(s)
            }),
            Property::always("claimed_refill_conserved", |_, s: &BucketState| {
                claimed_refill_conserved(s)
            }),
            Property::always("grants_whole_tokens", |m: &BucketModel, s: &BucketState| {
                s.granted.is_multiple_of(m.units_per_token)
            }),
            Property::sometimes("consumers_overlap", |_, s: &BucketState| {
                s.consumers
                    .iter()
                    .filter(|c| !matches!(c, Consumer::Idle))
                    .count()
                    >= 2
            }),
            Property::sometimes("reset_overlaps_consume", |_, s: &BucketState| {
                !matches!(s.resetter, Resetter::Idle)
                    && s.consumers.iter().any(|c| !matches!(c, Consumer::Idle))
            }),
            Property::sometimes("refill_in_flight", |_, s: &BucketState| s.in_flight() > 0),
            Property::sometimes("refill_granted", |_, s: &BucketState| s.granted > s.burst),
            Property::sometimes("refill_capped", |_, s: &BucketState| s.capped > 0),
            Property::sometimes("burst_shrunk", |m: &BucketModel, s: &BucketState| {
                s.resets > 0 && s.burst < m.configs[0].burst
            }),
            Property::sometimes("bucket_drained", |_, s: &BucketState| {
                s.rate > 0 && s.available == 0
            }),
        ];
        // A reset that carries a balance under the new burst instead of
        // refilling to it. Whole-token grants at two storage units to a token
        // never leave a balance under the smaller burst of that search, so it
        // has no such reset to show.
        if self.units_per_token == 1 {
            properties.push(Property::sometimes(
                "reset_keeps_the_balance",
                |_, s: &BucketState| s.epoch > 0 && s.base < s.burst,
            ));
        }
        properties
    }
}
