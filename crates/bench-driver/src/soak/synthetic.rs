//! Synthetic evidence: the run a contract describes, as if every record
//! arrived and every recovery took half its deadline.
//!
//! This is the hermetic half of the self-check. It needs no cluster, so it
//! runs on every pull request, and it proves two things the live self-check
//! cannot prove alone: that every checked-in contract is internally consistent
//! (a perfect run of it passes), and that the judge's verdict depends on the
//! evidence rather than on the contract alone (the same run with one record
//! removed fails, naming that record).

use {
    super::{
        contract::{Contract, FaultKind, Isolation, LatencyBudget, Semantics, Target},
        evidence::{
            CommitCheck, Evidence, FINAL_COMMIT_CHECK, FaultRecord, InjectedLoss, LatencySummary,
            PathMeasurement, ProducerLedger, ReaderLedger, RecoveryRecord, TxnLedger, TxnOutcome,
        },
        ledger::SeqSet,
        lifecycle::{Checkpoint, CheckpointRead, Side, is_transition, rollback},
        recovery::{PathRecovery, RecoveryEvidence},
        schedule::{ScheduledFault, expand},
    },
    krabka_units::prelude::*,
    num_traits::ToPrimitive as _,
    std::collections::BTreeMap,
};

/// Records each synthetic producer acknowledges.
const RECORDS: u64 = 200;

/// The wall-clock instant a synthetic run starts at: 2026-10-15T00:00:00Z.
const EPOCH_MS: i64 = 1_792_022_400_000;

/// A run of `contract` in which nothing went wrong.
#[must_use]
pub fn clean_run(contract: &Contract) -> Evidence {
    let mut evidence = Evidence {
        contract: contract.name.clone(),
        contract_digest: contract.digest(),
        seed: contract.seed,
        broker_image: "synthetic".to_owned(),
        started_unix_ms: EPOCH_MS,
        ended_unix_ms: EPOCH_MS + contract.duration.millis_i64(),
        ..Evidence::default()
    };
    let schedule = expand(contract).unwrap_or_default();
    for path in &contract.paths {
        let producer = format!("{}-0", path.name);
        let mut ledger = ProducerLedger {
            path: path.name.clone(),
            producer: producer.clone(),
            semantics: path.semantics,
            attempted: RECORDS,
            acked: SeqSet::range(0, RECORDS),
            failed: SeqSet::new(),
            retried: SeqSet::new(),
            transactions: Vec::new(),
        };
        let mut committed = SeqSet::range(0, RECORDS);
        if path.semantics == Semantics::Transactional {
            committed = SeqSet::new();
            let size = u64::from(contract.clients.transactions.records_per_transaction);
            let abort_every = u64::from(contract.clients.transactions.abort_every);
            for (id, start) in (0..RECORDS)
                .step_by(usize::try_from(size).unwrap_or(1))
                .enumerate()
            {
                let id = u64::try_from(id).unwrap_or(u64::MAX);
                let end = (start + size).min(RECORDS);
                let seqs = SeqSet::range(start, end);
                let outcome = if (id + 1) % abort_every == 0 {
                    TxnOutcome::Aborted
                } else {
                    committed.insert_range(start, end);
                    TxnOutcome::Committed
                };
                ledger.transactions.push(TxnLedger { id, seqs, outcome });
            }
        }
        for &isolation in &path.consumers {
            let seen = match (path.semantics, isolation) {
                (Semantics::Transactional, Isolation::ReadCommitted) => committed.clone(),
                _ => SeqSet::range(0, RECORDS),
            };
            let mut latest_by_key = BTreeMap::new();
            if let Some(space) = path.key_space {
                for seq in 0..RECORDS {
                    latest_by_key.insert(seq % space, seq);
                }
            }
            evidence.readers.push(ReaderLedger {
                path: path.name.clone(),
                isolation,
                producer: producer.clone(),
                seen,
                duplicates: Vec::new(),
                latest_by_key,
            });
        }
        if path.commit_offsets {
            let checkpoints = schedule
                .iter()
                .map(|fault| fault.id.clone())
                .chain([FINAL_COMMIT_CHECK.to_owned()]);
            for fault in checkpoints {
                evidence.commits.push(CommitCheck {
                    group: format!("{}-committer", path.name),
                    topic: path.name.clone(),
                    partition: 0,
                    fault,
                    acknowledged: 10,
                    fetched: Some(10),
                    log_end: 20,
                });
            }
        }
        evidence.producers.push(ledger);
        let half = |budget: &LatencyBudget| LatencySummary {
            p50: budget.p50 * 0.5,
            p99: budget.p99 * 0.5,
            p999: budget.p999 * 0.5,
            count: RECORDS,
        };
        evidence.paths.push(PathMeasurement {
            path: path.name.clone(),
            produce_steady: half(&path.produce_latency),
            produce_disturbed: half(&path.produce_latency),
            consume_steady: half(&path.consume_latency),
            consume_disturbed: half(&path.consume_latency),
            steady_rate: path.rate,
        });
    }
    for fault in &schedule {
        let injected = EPOCH_MS + fault.start.millis_i64();
        let cleared = EPOCH_MS + fault.end().millis_i64();
        evidence.faults.push(FaultRecord {
            id: fault.id.clone(),
            kind: fault.kind.name().to_owned(),
            target: "synthetic-broker".to_owned(),
            role: match fault.target {
                Target::Controller => "controller",
                Target::Leader => "leader",
                Target::Follower => "follower",
                Target::Any | Target::Broker(_) => "broker",
            }
            .to_owned(),
            node: synthetic_node(fault.target),
            injected_unix_ms: injected,
            cleared_unix_ms: Some(cleared),
            cleanup_error: None,
            detail: String::new(),
        });
        for &check in &fault.checks {
            let deadline = check.deadline(&contract.deadlines);
            evidence.recoveries.push(RecoveryRecord {
                fault: fault.id.clone(),
                check,
                from_unix_ms: cleared,
                recovered_unix_ms: Some(cleared + (deadline * 0.5).millis_i64()),
            });
        }
    }
    if let Some(objectives) = &contract.disaster_recovery {
        // The capture starts `post_capture_load` before the end of the load,
        // the source is lost at its end, and the restored cluster serves half
        // the recovery time objective later. Every record comes back.
        let disaster = evidence.ended_unix_ms;
        let held = objectives.post_capture_load.secs_f64();
        evidence.recovery = Some(RecoveryEvidence {
            capture_started_unix_ms: disaster - objectives.post_capture_load.millis_i64(),
            disaster_unix_ms: disaster,
            serving_unix_ms: Some(disaster + (objectives.rto * 0.5).millis_i64()),
            paths: evidence
                .producers
                .iter()
                .map(|producer| PathRecovery {
                    path: producer.path.clone(),
                    producer: producer.producer.clone(),
                    // The load kept its paced rate after the capture, and every
                    // record of it came back.
                    acknowledged: producer.acked.len()
                        + after_boundary(contract, &producer.path, held),
                    before_boundary: producer.acked.len(),
                    lost_beyond_boundary: SeqSet::new(),
                    oldest_loss_unix_ms: None,
                })
                .collect(),
        });
    }
    lifecycle_run(contract, &schedule, &mut evidence);
    evidence
}

/// The node id a synthetic run gives broker index `i`: `i + 1`, as the
/// executor's fixture numbers its nodes.
fn synthetic_node(target: Target) -> Option<i32> {
    match target {
        Target::Broker(index) => i32::try_from(index).ok().map(|index| index + 1),
        Target::Controller | Target::Leader | Target::Follower | Target::Any => None,
    }
}

/// The releases, the source's advertised ranges and one clean checkpoint per
/// transition of a lifecycle contract: every broker starts on the source, a
/// roll moves its broker to that side's image, nothing is finalized, and every
/// acknowledged record is read back.
fn lifecycle_run(contract: &Contract, schedule: &[ScheduledFault], evidence: &mut Evidence) {
    let Some(lifecycle) = &contract.lifecycle else {
        return;
    };
    let image = |side: Side| format!("synthetic-{side}");
    for side in [Side::Source, Side::Target] {
        evidence.releases.insert(side.to_string(), image(side));
    }
    evidence
        .source_supports
        .clone_from(&lifecycle.source.supports);
    let brokers = i32::try_from(contract.cluster.brokers).unwrap_or(i32::MAX);
    let mut images: BTreeMap<i32, String> = (1..=brokers)
        .map(|node| (node, image(Side::Source)))
        .collect();
    for fault in schedule.iter().filter(|fault| is_transition(&fault.kind)) {
        if let (FaultKind::Roll { to }, Some(node)) = (&fault.kind, synthetic_node(fault.target)) {
            images.insert(node, image(*to));
        }
        let finalized = BTreeMap::new();
        evidence.checkpoints.push(Checkpoint {
            fault: fault.id.clone(),
            unix_ms: EPOCH_MS + fault.end().millis_i64(),
            images: images.clone(),
            rollback: rollback(&lifecycle.source, &finalized),
            finalized,
            records: contract
                .paths
                .iter()
                .map(|path| CheckpointRead {
                    path: path.name.clone(),
                    semantics: path.semantics,
                    key_space: path.key_space,
                    acknowledged: SeqSet::range(0, RECORDS),
                    reader: ReaderLedger {
                        path: path.name.clone(),
                        isolation: Isolation::ReadUncommitted,
                        producer: format!("{}-0", path.name),
                        seen: SeqSet::range(0, RECORDS),
                        duplicates: Vec::new(),
                        latest_by_key: path.key_space.map_or_else(BTreeMap::new, |space| {
                            (0..RECORDS).map(|seq| (seq % space, seq)).collect()
                        }),
                    },
                })
                .collect(),
            config_mismatches: Vec::new(),
        });
    }
}

/// Records `path` acknowledges at its paced rate over `seconds`.
fn after_boundary(contract: &Contract, path: &str, seconds: f64) -> u64 {
    let rate = contract
        .path(path)
        .map_or(0.0, |path| path.rate.per_sec_f64());
    (rate * seconds).ceil().to_u64().unwrap_or(0)
}

/// Remove the first acknowledged record of the self-check's path from every
/// reader, and record that it was removed: the synthetic stand-in for the
/// `DeleteRecords` the live self-check issues.
pub fn inject_missing_record(contract: &Contract, evidence: &mut Evidence) {
    let Some(self_check) = &contract.self_check else {
        return;
    };
    let Some(producer) = evidence
        .producers
        .iter()
        .find(|producer| producer.path == self_check.missing_record_path)
    else {
        return;
    };
    let Some(seq) = producer.acked.iter().next() else {
        return;
    };
    let producer = producer.producer.clone();
    for reader in &mut evidence.readers {
        if reader.producer == producer {
            reader.seen.remove(seq);
        }
    }
    evidence.injected.push(InjectedLoss {
        path: self_check.missing_record_path.clone(),
        producer,
        seq,
        partition: 0,
        offset: 0,
    });
}
