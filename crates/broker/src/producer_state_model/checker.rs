use super::*;

impl Model for ProducerModel {
    type State = ProdState;
    type Action = ProdAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProdState {
            host: None,
            window: None,
            witness: None,
            violation: None,
        }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        for epoch in 0..=self.max_epoch {
            for base in 0..=self.max_seq {
                for delta in 0..=1 {
                    actions.push(ProdAction::Submit(
                        epoch,
                        Range {
                            base,
                            last: base + delta,
                        },
                    ));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let ProdAction::Submit(epoch, batch) = action;
        let entry = last.host.as_ref().map(HostEntry::entry);
        let host = Answer::of(check_retained(
            entry.as_ref(),
            SequenceContext::RELEASED,
            epoch,
            batch.base,
            batch.last - batch.base,
        ));
        let kafka = kafka_decision(last.window.as_ref(), epoch, batch);
        let mut s = ProdState {
            witness: None,
            ..last.clone()
        };
        if host != kafka {
            s.violation.get_or_insert((epoch, batch, host, kafka));
            return Some(s);
        }
        match host {
            Answer::Append => {
                s.host = Some(HostEntry::after_append(last.host.as_ref(), epoch, batch));
                let mut window = last
                    .window
                    .clone()
                    .filter(|window| window.epoch == epoch)
                    .unwrap_or(Window {
                        epoch,
                        last_sequence: -1,
                        retained: VecDeque::new(),
                    });
                window.last_sequence = batch.last;
                window.retained.push_back(batch);
                if window.retained.len() > NUM_BATCHES_TO_RETAIN {
                    window.retained.pop_front();
                }
                s.window = Some(window);
                Some(s)
            }
            Answer::Duplicate(range) => {
                let retained = &last.window.as_ref()?.retained;
                s.witness = Some(if retained.back() == Some(&range) {
                    Witness::DuplicateOfLast
                } else if retained.len() == NUM_BATCHES_TO_RETAIN
                    && retained.front() == Some(&range)
                {
                    Witness::DuplicateOfOldest
                } else {
                    Witness::DuplicateOfOlder
                });
                Some(s)
            }
            Answer::OutOfOrder
                if last.window.as_ref().is_some_and(|w| {
                    w.epoch == epoch
                        && w.retained.len() == NUM_BATCHES_TO_RETAIN
                        && w.retained
                            .front()
                            .is_some_and(|oldest| batch.last < oldest.base)
                }) =>
            {
                s.witness = Some(Witness::RetryBelowWindowRefused);
                Some(s)
            }
            Answer::OutOfOrder | Answer::Fenced => None,
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: the host answers every submit as Kafka does over the
            // ghost window. That covers dedup of any of the five retained
            // batches, contiguity within an epoch, a fresh start at a new
            // epoch, and fencing of a stale epoch.
            Property::always("decision_matches_kafka", |_, s: &ProdState| {
                s.violation.is_none()
            }),
            Property::sometimes("duplicate_of_last_batch", |_, s: &ProdState| {
                s.witness == Some(Witness::DuplicateOfLast)
            }),
            Property::sometimes("duplicate_of_older_retained_batch", |_, s: &ProdState| {
                s.witness == Some(Witness::DuplicateOfOlder)
            }),
            Property::sometimes("duplicate_of_oldest_retained_batch", |_, s: &ProdState| {
                s.witness == Some(Witness::DuplicateOfOldest)
            }),
            Property::sometimes("retry_below_window_refused", |_, s: &ProdState| {
                s.witness == Some(Witness::RetryBelowWindowRefused)
            }),
            Property::sometimes("can_bump_epoch", |_, s: &ProdState| {
                s.window.as_ref().is_some_and(|w| w.epoch >= 1)
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.window
            .as_ref()
            .is_none_or(|w| w.epoch <= self.max_epoch && w.last_sequence <= self.max_seq)
    }
}
