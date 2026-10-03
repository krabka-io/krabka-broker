use super::*;

impl Model for EosModel {
    type State = EosState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![EosState {
            log: vec![],
            prod: (0..self.producers)
                .map(|_| Prod {
                    state: TxnState::Empty.to_kafka_status(),
                    epoch: 0,
                    generation: 0,
                })
                .collect(),
            hw: Offset(0),
            violations: Violations::default(),
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        // A follower replicating: advance the HWM toward the log end.
        if s.hw < model_offset(s.log.len()) {
            acts.push(Act::Ack);
        }
        if s.log.len() >= self.max_log {
            return;
        }
        for p in 0..self.producers {
            let pr = s.prod[usize::from(p)];
            // `AddPartitionsToTxn` opens a NEW transaction only from a state
            // that is not already `Ongoing`; on an `Ongoing` one it extends
            // the open transaction, which `Append` covers.
            if pr.generation < self.max_gen
                && pr.state != TxnState::Ongoing.to_kafka_status()
                && tstate(pr.state).can_transition_to(TxnState::Ongoing)
            {
                acts.push(Act::Begin(p));
            }
            if pr.state == TxnState::Ongoing.to_kafka_status() {
                let n = s
                    .log
                    .iter()
                    .filter(|b| {
                        b.producer == p && b.generation == pr.generation && b.kind == Kind::Data
                    })
                    .count();
                if n < self.max_data_per_txn {
                    acts.push(Act::Append(p));
                }
                if n >= 1 {
                    acts.push(Act::End(p, true));
                    acts.push(Act::End(p, false));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match a {
            Act::Begin(p) => {
                let pr = &mut s.prod[usize::from(p)];
                if pr.state == TxnState::Ongoing.to_kafka_status()
                    || !tstate(pr.state).can_transition_to(TxnState::Ongoing)
                {
                    return None;
                }
                pr.generation += 1;
                pr.state = TxnState::Ongoing.to_kafka_status();
            }
            Act::Append(p) => {
                let g = s.prod[usize::from(p)].generation;
                s.log.push(Batch {
                    producer: p,
                    generation: g,
                    kind: Kind::Data,
                });
            }
            Act::End(p, commit) => {
                let pr = s.prod[usize::from(p)];
                let mut entry = rebuild(usize::from(p), pr);
                let Ok((prepare, complete)) = decide_phase1_transition(&mut entry, commit) else {
                    return None; // illegal transition
                };
                crate::txn::handlers::end_txn::prepare_completion_identities_with_fresh(
                    &mut entry,
                    TxnVersion::Verified,
                    None,
                )
                .expect("model epochs never reach the rotation boundary");
                let completion =
                    crate::txn::handlers::end_txn::completion_producer_identity(&entry);
                match decide_end_txn_completion(
                    &entry,
                    ProducerId(PID0 + i64::from(p)),
                    entry.producer_epoch,
                    completion.0,
                    completion.1,
                    prepare,
                    complete,
                ) {
                    CompletionDecision::Proceed {
                        next_state,
                        response_epoch,
                        ..
                    } => {
                        s.log.push(Batch {
                            producer: p,
                            generation: pr.generation,
                            kind: if commit { Kind::Commit } else { Kind::Abort },
                        });
                        let np = &mut s.prod[usize::from(p)];
                        np.state = next_state.to_kafka_status();
                        np.epoch = response_epoch; // TV_2 bumps the epoch on completion
                    }
                    CompletionDecision::AlreadyComplete { .. } | CompletionDecision::Reject(_) => {
                        s.violations.end_not_proceed = true;
                    }
                }
            }
            Act::Ack => {
                s.hw += 1; // a follower replicated one more offset
            }
        }
        // The HWM and the LSO never regress: offsets only grow, and the
        // oldest open transaction's base only advances.
        if s.hw < last.hw {
            s.violations.hw_regressed = true;
        }
        if lso(&s.log) < lso(&last.log) {
            s.violations.lso_regressed = true;
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: every visible batch belongs to a COMMITTED txn — no
            // open/uncommitted and no aborted record is ever visible. Catches a
            // real `compute_visibility_window` returning effective_lso ABOVE
            // min(lso, hw) (which would expose open-txn or above-HWM data).
            Property::always("only_committed_visible", |_, s: &EosState| {
                visible(&s.log, s.hw).into_iter().all(|off| {
                    let b = s.log[model_index(off)];
                    txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // The read_committed window is exactly min(first unstable offset,
            // HWM), with the first unstable offset recomputed here from the
            // log's markers, not from `lso()`. Catches a window above OR
            // below min(lso, hw), and an `lso()` that drifts from Kafka's rule.
            Property::always("window_is_min_lso_hw", |_, s: &EosState| {
                effective_lso(&s.log, s.hw) == first_unstable_offset(&s.log).min(s.hw)
            }),
            // Every committed Data batch below min(first unstable offset, HWM)
            // is visible: no committed, durable, stable record is hidden.
            Property::always("committed_prefix_complete", |_, s: &EosState| {
                let v = visible(&s.log, s.hw);
                let window = first_unstable_offset(&s.log).min(s.hw);
                s.log.iter().enumerate().all(|(off, b)| {
                    !(b.kind == Kind::Data
                        && model_offset(off) < window
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit))
                        || v.contains(&model_offset(off))
                })
            }),
            // Nothing at or above the HWM is visible.
            Property::always("nothing_visible_above_hw", |_, s: &EosState| {
                visible(&s.log, s.hw).into_iter().all(|off| off < s.hw)
            }),
            // No visible offset lies in an aborted range as Kafka's consumer
            // derives it from the Abort markers alone (see
            // `aborted_by_markers`), independently of `visible()`'s filter.
            Property::always("no_visible_aborted", |_, s: &EosState| {
                let aborted = aborted_by_markers(&s.log);
                visible(&s.log, s.hw)
                    .into_iter()
                    .all(|off| !aborted.contains(&off))
            }),
            // The HWM stays within the log.
            Property::always("hw_within_log", |_, s: &EosState| {
                s.hw <= model_offset(s.log.len())
            }),
            // Every transition keeps the LSO and the HWM from regressing, and
            // every End drives the decision cores to Proceed.
            Property::always("no_transition_violation", |_, s: &EosState| {
                s.violations == Violations::default()
            }),
            // ----- non-vacuity witnesses -----
            Property::sometimes("committed_visible", |_, s: &EosState| {
                !visible(&s.log, s.hw).is_empty()
            }),
            Property::sometimes("aborted_filtered", |_, s: &EosState| {
                let eff = effective_lso(&s.log, s.hw);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (model_offset(off)) < eff
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Abort)
                })
            }),
            // The key EOS subtlety: a COMMITTED batch sits ABOVE the LSO, held
            // back by an older still-open transaction (out-of-order commit).
            Property::sometimes("interleaved_held_back", |_, s: &EosState| {
                let l = lso(&s.log);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (model_offset(off)) >= l
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // The visibility CORE actively clamps: the HWM holds the effective LSO
            // BELOW the LSO (an open txn's records sit above the not-yet-replicated
            // HWM). Proves `compute_visibility_window`'s `lso.min(hw)` is exercised
            // non-trivially, not as an identity pass-through.
            Property::sometimes("hwm_clamp_active", |_, s: &EosState| {
                effective_lso(&s.log, s.hw) < lso(&s.log)
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.len() <= self.max_log
    }
}
