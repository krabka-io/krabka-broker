use super::*;

impl ConsensusModel {
    pub(super) fn model_properties() -> Vec<Property<Self>> {
        vec![
            // Anti-vacuity witness: a leader is actually elected in some state.
            Property::sometimes("leader_elected", |_, s: &ModelState| {
                s.nodes.values().any(is_leader)
            }),
            // Safety: a leader whose check-quorum window expires must step
            // down. Without it, an old leader isolated by a partition holds its
            // epoch indefinitely — KIP-996 pre-vote never bumps its epoch, so
            // nothing else would ever tell it the majority side has moved on.
            // `election_safety` cannot see that: the two leaders hold different
            // epochs.
            Property::always(
                "check_quorum_expiry_ends_leadership",
                |_, s: &ModelState| !s.check_quorum_violation,
            ),
            // Anti-vacuity witness for the property above: the resignation is
            // actually reached, rather than the check holding because no
            // check-quorum expiry was ever explored.
            Property::sometimes("leader_resigns", |m: &ConsensusModel, s: &ModelState| {
                // Only required where the expiry is offered; a config that
                // does not explore it satisfies this trivially.
                !m.enable_check_quorum || s.leader_resigned
            }),
            // Safety: at most one leader per leader-epoch.
            Property::always("election_safety", |_, s: &ModelState| {
                let mut by_epoch: BTreeMap<Epoch, NodeId> = BTreeMap::new();
                for (&id, n) in &s.nodes {
                    if is_leader(n) {
                        let epoch = n.machine.quorum_state().leader_epoch;
                        if let Some(&other) = by_epoch.get(&epoch)
                            && other != id
                        {
                            return false;
                        }
                        by_epoch.insert(epoch, id);
                    }
                }
                true
            }),
            // Safety: the committed log is linearizable — there exists a single
            // total order of client appends consistent with every observed
            // invoke/return. A lost or reordered committed entry has no such
            // serialization.
            Property::always("linearizable", |_, s: &ModelState| {
                s.linz.serialized_history().is_some()
            }),
            Property::always("assigned_offsets_gap_free", |_, s: &ModelState| {
                s.committed
                    .iter()
                    .enumerate()
                    .all(|(offset, value)| u64::try_from(offset + 1).is_ok_and(|v| *value == v))
            }),
            Property::always("committed_values_unique", |_, s: &ModelState| {
                s.committed.iter().copied().collect::<BTreeSet<_>>().len() == s.committed.len()
            }),
            // Anti-vacuity witness: a CLIENT append is actually committed.
            // Without this, `linearizable` could hold vacuously because no
            // client value ever committed (a control-record-only HWM advance
            // would not count).
            Property::sometimes("entry_committed", |m: &ConsensusModel, s: &ModelState| {
                // Only required when client appends are enabled; a no-append
                // config (election focus) satisfies this trivially.
                m.max_appends == 0 || !s.committed.is_empty()
            }),
            Property::sometimes(
                "two_appenders_concurrent",
                |m: &ConsensusModel, s: &ModelState| {
                    !m.enable_append_via
                        || (s.appenders_seen.len() == usize::from(APPENDER_COUNT)
                            && s.pending.len() == usize::from(APPENDER_COUNT))
                },
            ),
            // Safety (Raft log matching): two logs may diverge only as an
            // uncommitted suffix — if they disagree on the epoch at some offset
            // `k`, they must not agree again at any later offset (equal entries
            // imply equal prefixes). Re-agreement after disagreement is a true
            // matching violation.
            Property::always("log_matching", |_, s: &ModelState| {
                let logs: Vec<&Vec<Epoch>> = s.nodes.values().map(|n| &n.log.epochs).collect();
                for i in 0..logs.len() {
                    for j in (i + 1)..logs.len() {
                        let (a, b) = (logs[i], logs[j]);
                        let common = a.len().min(b.len());
                        for k in 0..common {
                            if a[k] != b[k] && (k + 1..common).any(|m| a[m] == b[m]) {
                                return false;
                            }
                        }
                    }
                }
                true
            }),
            // Safety (Raft leader completeness, Figure 8): a leader of epoch `e`
            // holds every entry committed in an epoch at or below `e`, stamped
            // with the epoch it committed under. Entries of a LATER epoch are
            // excluded: a leader that has been superseded and not yet heard so
            // legitimately lags behind its successor's commits, and holding it
            // to them would flag ordinary staleness rather than a safety
            // failure.
            //
            // A vote granted to a candidate whose log is behind the granting
            // majority is exactly what breaks this: the winner would open a new
            // epoch missing an already-acknowledged entry, and its replication
            // would then overwrite it everywhere.
            Property::always("leader_completeness", |_, s: &ModelState| {
                s.nodes.values().filter(|n| is_leader(n)).all(|n| {
                    let leader_epoch = n.machine.quorum_state().leader_epoch;
                    s.committed_epochs
                        .iter()
                        .enumerate()
                        .filter(|&(_, &epoch)| epoch <= leader_epoch)
                        .all(|(offset, &epoch)| {
                            i64::try_from(offset)
                                .is_ok_and(|offset| n.log.epoch_at(offset) == Some(epoch))
                        })
                })
            }),
            // Anti-vacuity witness for the property above: a voter actually
            // refuses a candidate for log recency alone, which is the only
            // reason `leader_completeness` can hold under a majority that has
            // fallen behind. The transition that delivers the request
            // establishes the reason by running the voter's real machine on
            // the same request with only the candidate's log end changed.
            Property::sometimes(
                "stale_candidate_refused",
                |m: &ConsensusModel, s: &ModelState| {
                    // Only reachable where a majority can fall behind a
                    // committed prefix: three voters that take client appends.
                    if m.voter_ids.len() < 3 || m.max_appends == 0 {
                        return true;
                    }
                    s.step_witness == Some(StepWitness::StaleCandidateRefused)
                },
            ),
            // Anti-vacuity witness for `log_matching`: a follower whose log
            // disagrees with the leader's is cut back by the production
            // truncation path, since the model never overwrites a log.
            Property::sometimes(
                "divergent_log_truncated",
                |m: &ConsensusModel, s: &ModelState| {
                    // Required where a node can be cut off from a leader it
                    // then outlives: the crash configs. Without a crash the
                    // bounded configs never leave a follower holding an entry
                    // the next leader lacks.
                    m.max_crashes == 0 || s.step_witness == Some(StepWitness::DivergentLogTruncated)
                },
            ),
            // Safety: no node's committed high-watermark exceeds its own log end
            // (a node cannot have committed past what it physically holds).
            Property::always("hwm_within_log", |_, s: &ModelState| {
                s.nodes
                    .values()
                    .all(|n| node_high_watermark(n) <= n.log.end_offset())
            }),
        ]
    }
}
