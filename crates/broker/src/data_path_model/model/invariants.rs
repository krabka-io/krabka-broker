use super::*;

impl DpModel {
    pub(super) fn model_properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("committed_durable", |_, s: &DpState| {
                let lg = &s.log[usize::from(s.leader)];
                s.committed
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("wal_acked_durable", |_, s: &DpState| {
                let lg = &s.log[s.leader as usize];
                s.wal_acked
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("hwm_within_leader_log", |_, s: &DpState| {
                s.hwm <= s.leader_leo()
            }),
        ];
        if self.diskless {
            props.extend([
                Property::always("diskless_hw_released_by_wal_sync", |_, s: &DpState| {
                    s.hwm == model_offset(s.wal_acked.len())
                }),
                Property::sometimes("wal_acked_progress", |_, s: &DpState| {
                    !s.wal_acked.is_empty()
                }),
                Property::sometimes("wal_acked_survives_broker_down", |_, s: &DpState| {
                    !has(s.live, s.leader) && !s.wal_acked.is_empty()
                }),
                Property::always("offsets_contiguous_and_unique", |_, s: &DpState| {
                    let mut next = 0;
                    for &(start, end) in &s.assigned {
                        if start != next || end <= start {
                            return false;
                        }
                        next = end;
                    }
                    next == s.seq_next
                }),
            ]);
        } else {
            props.extend([
                Property::sometimes("committed_progress", |_, s: &DpState| {
                    !s.committed.is_empty()
                }),
                Property::sometimes("full_replication", |_, s: &DpState| {
                    s.hwm == s.leader_leo() && s.hwm > 0
                }),
                // A leader change occurred.
                Property::sometimes("leader_changed", |_, s: &DpState| s.leader_epoch >= 2),
                // The ISR shrank below the full replica set.
                Property::sometimes("isr_shrunk", |_, s: &DpState| {
                    u32::from(s.isr).count_ones() < u32::from(NB_U8)
                }),
                // Two brokers hold different epochs at one offset — truncation
                // territory (a follower must truncate to reconcile).
                Property::sometimes("divergence_present", |_, s: &DpState| {
                    (0..MAX_LEN).any(|off| {
                        let mut seen: Option<u8> = None;
                        for b in 0..NB {
                            if let Some(&e) = s.log[b].get(off) {
                                match seen {
                                    None => seen = Some(e),
                                    Some(x) if x != e => return true,
                                    _ => {}
                                }
                            }
                        }
                        false
                    })
                }),
            ]);
        }
        if self.tracks_elr() {
            props.extend([
                // THE CLAIM. `failover_one` and `select_leader` elect a
                // surviving eligible leader replica ahead of a longer log and
                // report that election as losing nothing -- the
                // unclean-election counter does not count it, the audit reason
                // says no committed record is lost, and KFC-9's `require` gate
                // lets it through. All of that rests on the published set
                // naming only replicas that hold every committed record, which
                // KIP-966 gets from the leader's high watermark standing still
                // while the ISR is under min ISR. This is that, stated over a
                // set the model did not choose but computed with the real
                // maintenance rule, and over a watermark the real core moved.
                Property::always("elr_holds_every_committed_record", |_, s: &DpState| {
                    (0..NB_U8).filter(|&b| has(s.elr, b)).all(|b| {
                        let log = &s.log[usize::from(b)];
                        s.committed
                            .iter()
                            .enumerate()
                            .all(|(off, &e)| log.get(off) == Some(&e))
                    })
                }),
                // The same claim at the moment it is cashed in: the election
                // that took the ELR rule did not drop a committed record.
                Property::always(
                    "elr_election_keeps_every_committed_record",
                    |_, s: &DpState| s.elr_trace & ELR_DROPPED_COMMITTED == 0,
                ),
                // Anti-vacuity for the rule the claim rests on: a state in
                // which every ISR member holds a record past the HWM, so a
                // watermark that ignored min ISR would have committed it, but
                // the ISR is under min ISR and the watermark stayed.
                Property::sometimes("hwm_held_by_min_isr", |m: &DpModel, s: &DpState| {
                    let leader_log = &s.log[usize::from(s.leader)];
                    has(s.isr, s.leader)
                        && usize::try_from(u32::from(s.isr).count_ones())
                            .expect("a bitmask over three brokers counts low")
                            < m.min_isr
                        && (0..NB_U8)
                            .filter(|&b| has(s.isr, b))
                            .all(|b| consistent_leo(&s.log[usize::from(b)], leader_log) > s.hwm)
                }),
                // Anti-vacuity. Without these three the two `always`
                // properties above would pass on a model that never publishes
                // an ELR, never elects out of one, or only ever elects the
                // replica the fallback would have picked anyway.
                Property::sometimes("elr_published", |_, s: &DpState| s.elr != 0),
                Property::sometimes("elr_election_taken", |_, s: &DpState| {
                    s.elr_trace & ELR_ELECTED != 0
                }),
                Property::sometimes("elr_election_beat_a_longer_log", |_, s: &DpState| {
                    s.elr_trace & ELR_BEAT_LONGER_LOG != 0
                }),
            ]);
        }
        if self.unclean {
            // Loss characterization: an unclean-election data loss is reachable
            // (and `committed_durable` above still holds — `committed` is the LIVE
            // durability obligation, truncated when an unclean election drops it).
            props.push(Property::sometimes("unclean_loss", |_, s: &DpState| s.lost));
        } else {
            // Clean config: NO committed-data loss ever occurs.
            props.push(Property::always("no_loss_when_clean", |_, s: &DpState| {
                !s.lost
            }));
        }
        props
    }
}
