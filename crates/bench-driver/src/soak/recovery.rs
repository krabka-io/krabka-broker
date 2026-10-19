//! A disaster-recovery drill, judged: which records the capture had to hold,
//! which it lost, and how long recovery took.
//!
//! A drill runs a contract's load against a source cluster, captures the
//! restore inputs while the load runs, keeps the load going for the declared
//! `post_capture_load`, and then loses the whole source cluster. A fresh
//! cluster is restored from the capture alone, and the records are read back.
//!
//! The capture boundary is the instant the capture started. Every record the
//! source acknowledged before it must be restored: [`bound_evidence`] cuts each
//! producer's ledger there, so the reconciler reports a missing one as lost,
//! by the same rules as a soak. A record acknowledged after the boundary may
//! or may not be restored, which is exactly how the reconciler already treats
//! a send whose outcome is unknown. The ones that are missing are the drill's
//! data loss: [`PathRecovery`] counts them, and the recovery point is the age,
//! at the loss, of the oldest acknowledged record that did not come back.
//!
//! [`check_recovery`] holds the measured recovery point and recovery time
//! against the contract's [`RecoveryObjectives`].

use {
    super::{
        contract::{Contract, Profile, RecoveryObjectives},
        evidence::{CommitCheck, Evidence, ProducerLedger, ReaderLedger},
        ledger::SeqSet,
    },
    krabka_units::{fmt::Human as _, prelude::*},
    num_traits::ToPrimitive as _,
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
};

/// When one send was acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub seq: u64,
    pub unix_ms: i64,
}

/// One commit the group coordinator acknowledged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitAck {
    pub unix_ms: i64,
    /// The committed next offset of each partition the commit named.
    pub offsets: BTreeMap<i32, i64>,
}

/// One producer's acknowledgement times.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerAcks {
    pub producer: String,
    pub acks: Vec<Ack>,
}

/// One committing group's acknowledged commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitHistory {
    pub group: String,
    pub topic: String,
    pub partitions: i32,
    pub acks: Vec<CommitAck>,
}

/// What a drill's load recorded against the source cluster, before the loss.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadRecord {
    /// The run as a soak records it: its identity, its producers' ledgers and
    /// its paths' measurements. Readers and commit checks come after the loss.
    pub run: Evidence,
    pub acks: Vec<ProducerAcks>,
    pub commits: Vec<CommitHistory>,
}

/// The two instants a drill is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Boundary {
    /// When the capture started: the capture boundary.
    pub capture_started_unix_ms: i64,
    /// When the source cluster was lost.
    pub disaster_unix_ms: i64,
}

/// How one path came through the loss.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathRecovery {
    pub path: String,
    pub producer: String,
    /// Records the source acknowledged, before the loss.
    pub acknowledged: u64,
    /// Of those, the ones acknowledged before the capture boundary.
    pub before_boundary: u64,
    /// Records acknowledged after the boundary that no reader of the restored
    /// cluster found.
    pub lost_beyond_boundary: SeqSet,
    /// When the oldest acknowledged record that no reader found was
    /// acknowledged. `None` when every acknowledged record came back.
    pub oldest_loss_unix_ms: Option<i64>,
}

/// The recovery half of a drill's evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveryEvidence {
    pub capture_started_unix_ms: i64,
    pub disaster_unix_ms: i64,
    /// When the restored cluster first served authenticated produce and
    /// consume of the captured data. `None` when it never did.
    pub serving_unix_ms: Option<i64>,
    pub paths: Vec<PathRecovery>,
}

/// The records of `acks` acknowledged at or before `instant`.
#[must_use]
pub fn acknowledged_by(acks: &[Ack], instant: i64) -> SeqSet {
    acks.iter()
        .filter(|ack| ack.unix_ms <= instant)
        .map(|ack| ack.seq)
        .collect()
}

/// The last commit of each partition acknowledged at or before `instant`.
#[must_use]
pub fn committed_by(history: &[CommitAck], instant: i64) -> BTreeMap<i32, i64> {
    let mut committed = BTreeMap::new();
    for ack in history.iter().filter(|ack| ack.unix_ms <= instant) {
        committed.extend(&ack.offsets);
    }
    committed
}

/// The records every one of `readers` found. A record counts as restored only
/// when each declared reader read it.
fn restored(readers: &[&ReaderLedger]) -> SeqSet {
    let mut readers = readers.iter();
    let Some(first) = readers.next() else {
        return SeqSet::new();
    };
    readers.fold(first.seen.clone(), |found, reader| {
        found.intersection(&reader.seen)
    })
}

/// How `ledger`'s path came through the loss, given when each of its sends was
/// acknowledged and what the restored cluster's readers found.
#[must_use]
pub fn path_recovery(
    ledger: &ProducerLedger,
    acks: &[Ack],
    readers: &[&ReaderLedger],
    boundary: Boundary,
) -> PathRecovery {
    let before = acknowledged_by(acks, boundary.capture_started_unix_ms);
    let found = restored(readers);
    let missing = ledger.acked.difference(&found);
    let oldest_loss_unix_ms = acks
        .iter()
        .filter(|ack| missing.contains(ack.seq))
        .map(|ack| ack.unix_ms)
        .min();
    PathRecovery {
        path: ledger.path.clone(),
        producer: ledger.producer.clone(),
        acknowledged: ledger.acked.len(),
        before_boundary: before.intersection(&ledger.acked).len(),
        lost_beyond_boundary: missing.difference(&before),
        oldest_loss_unix_ms,
    }
}

/// What the restored cluster served, after the loss.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoredReadings {
    pub readers: Vec<ReaderLedger>,
    /// Committed-offset checks of the restored cluster, each against the last
    /// commit acknowledged before the capture boundary.
    pub commits: Vec<CommitCheck>,
    pub serving_unix_ms: Option<i64>,
    pub ended_unix_ms: i64,
}

/// The evidence the judge grades a drill by.
///
/// Each producer's ledger keeps only the acknowledgements from before the
/// capture boundary, so the reconciler requires exactly those to be restored
/// and permits the rest either way. The recovery section records what was lost
/// beyond the boundary.
#[must_use]
pub fn bound_evidence(
    load: &LoadRecord,
    boundary: Boundary,
    restored: RestoredReadings,
) -> Evidence {
    let no_acks = Vec::new();
    let acks_of = |producer: &str| {
        load.acks
            .iter()
            .find(|acks| acks.producer == producer)
            .map_or(&no_acks, |acks| &acks.acks)
    };
    let mut paths = Vec::new();
    let mut producers = Vec::new();
    for ledger in &load.run.producers {
        let acks = acks_of(&ledger.producer);
        let readers: Vec<&ReaderLedger> = restored
            .readers
            .iter()
            .filter(|reader| reader.producer == ledger.producer)
            .collect();
        paths.push(path_recovery(ledger, acks, &readers, boundary));
        let mut cut = ledger.clone();
        cut.acked = ledger
            .acked
            .intersection(&acknowledged_by(acks, boundary.capture_started_unix_ms));
        producers.push(cut);
    }
    Evidence {
        ended_unix_ms: restored.ended_unix_ms,
        producers,
        readers: restored.readers,
        commits: restored.commits,
        recovery: Some(RecoveryEvidence {
            capture_started_unix_ms: boundary.capture_started_unix_ms,
            disaster_unix_ms: boundary.disaster_unix_ms,
            serving_unix_ms: restored.serving_unix_ms,
            paths,
        }),
        ..load.run.clone()
    }
}

/// A drill's recovery point and recovery time, held against its objectives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveryVerdict {
    pub objectives: RecoveryObjectives,
    /// The age at the loss, in milliseconds, of the oldest acknowledged
    /// record that did not come back. Zero when every acknowledged record came
    /// back.
    pub rpo_ms: i64,
    /// Milliseconds from the loss until the restored cluster served. `None`
    /// when it never did.
    pub rto_ms: Option<i64>,
    /// Records acknowledged after the capture boundary and not restored.
    pub lost_beyond_boundary: u64,
    /// Every way the drill missed its objectives or its evidence falls short.
    pub problems: Vec<String>,
}

fn human(time: Time) -> String { time.human().to_string() }

/// Judge a drill's recovery against the contract's objectives. `None` for a
/// contract that is not a drill.
#[must_use]
pub fn check_recovery(contract: &Contract, evidence: &Evidence) -> Option<RecoveryVerdict> {
    if contract.profile != Profile::DisasterRecovery {
        return None;
    }
    let objectives = contract.disaster_recovery.clone()?;
    let mut problems = Vec::new();
    let Some(recovery) = &evidence.recovery else {
        problems.push("the evidence records no recovery".to_owned());
        return Some(RecoveryVerdict {
            objectives,
            rpo_ms: 0,
            rto_ms: None,
            lost_beyond_boundary: 0,
            problems,
        });
    };
    let held = recovery.disaster_unix_ms - recovery.capture_started_unix_ms;
    if held < objectives.post_capture_load.millis_i64() {
        problems.push(format!(
            "the source was lost {} after the capture started, before the declared \
             post_capture_load of {}",
            human(Time::from_millis(held.max(0))),
            human(objectives.post_capture_load)
        ));
    }
    for path in &contract.paths {
        match recovery
            .paths
            .iter()
            .find(|measured| measured.path == path.name)
        {
            None => problems.push(format!("path `{}` has no recovery measurement", path.name)),
            Some(measured) if measured.before_boundary == 0 => problems.push(format!(
                "path `{}` acknowledged nothing before the capture boundary, so the capture \
                 proved nothing about it",
                path.name
            )),
            Some(measured) => {
                // The load must keep writing at its floor between the capture and
                // the loss, or there is no tail for the recovery point to measure.
                let after = measured
                    .acknowledged
                    .saturating_sub(measured.before_boundary);
                let seconds = Time::from_millis(held.max(0)).secs_f64();
                let rate = if seconds > 0.0 {
                    after.to_f64().unwrap_or(f64::MAX) / seconds
                } else {
                    0.0
                };
                if rate < path.throughput_floor.per_sec_f64() {
                    problems.push(format!(
                        "path `{}` acknowledged {after} record(s) between the capture boundary \
                         and the loss, {rate:.1}/s against its floor of {}/s, so the loss window \
                         was not exercised",
                        path.name,
                        path.throughput_floor.per_sec_f64()
                    ));
                }
            }
        }
    }
    let rpo = Time::from_millis(
        recovery
            .paths
            .iter()
            .filter_map(|path| path.oldest_loss_unix_ms)
            .map(|oldest| (recovery.disaster_unix_ms - oldest).max(0))
            .max()
            .unwrap_or(0),
    );
    if rpo > objectives.rpo {
        problems.push(format!(
            "recovery point: the oldest lost record was acknowledged {} before the loss, past \
             the objective of {}",
            human(rpo),
            human(objectives.rpo)
        ));
    }
    let rto = recovery
        .serving_unix_ms
        .filter(|serving| *serving >= recovery.disaster_unix_ms)
        .map(|serving| Time::from_millis(serving - recovery.disaster_unix_ms));
    match rto {
        None => problems.push(
            "recovery time: the restored cluster never served authenticated produce and consume"
                .to_owned(),
        ),
        Some(rto) if rto > objectives.rto => problems.push(format!(
            "recovery time: the restored cluster served {} after the loss, past the objective \
             of {}",
            human(rto),
            human(objectives.rto)
        )),
        Some(_) => {}
    }
    Some(RecoveryVerdict {
        objectives,
        rpo_ms: rpo.millis_i64(),
        rto_ms: rto.map(Time::millis_i64),
        lost_beyond_boundary: recovery
            .paths
            .iter()
            .map(|path| path.lost_beyond_boundary.len())
            .sum(),
        problems,
    })
}

#[cfg(test)]
mod tests;
