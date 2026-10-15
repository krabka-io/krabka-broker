//! One scheduled fault, from target to recovery: resolve the target against
//! the cluster as it is, inject, hold, clean up, measure each recovery the
//! fault is checked for, and check the committed offsets across it.
//!
//! Recoveries are measured from the instant the contract names: a failover
//! from the injection, concurrently with the hold, and a rejoin or a catch-up
//! from the end of the cleanup. Each polls every quarter second and gives up
//! at twice the contract's own deadline, recording no recovery. The verdict,
//! not the executor, decides whether a measured recovery was in time.

use {
    crate::{
        faults::{Injected, inject},
        host::{FaultLog, now_ms},
        load::CommitTracker,
        readback::commit_checks,
        topology::{
            Nodes, TopicPartition, isr_complete, led_by, not_failed_over, resolve_target, role_of,
        },
        windows::{Span, Windows, recovery_budget},
    },
    krabka_bench_driver::soak::{
        contract::{Contract, FaultKind, RecoveryCheck},
        evidence::{CommitCheck, FaultRecord, RecoveryRecord},
        schedule::ScheduledFault,
    },
    krabka_units::prelude::*,
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
        time::Duration,
    },
    tokio::task::JoinHandle,
};

/// How often a recovery is polled.
const POLL: Duration = Duration::from_millis(250);

/// The recoveries `fault` is measured for, once its target's `role` is known.
///
/// A fault that moves leadership off the active controller is measured for a
/// controller failover too, whatever its plan targeted: a `leader` or an
/// `any` plan can land on the controller.
pub(crate) fn checks_for(fault: &ScheduledFault, role: &str) -> Vec<RecoveryCheck> {
    let mut checks = fault.checks.clone();
    let moves_leadership = matches!(
        fault.kind,
        FaultKind::Restart { .. }
            | FaultKind::Pause {
                intends_failover: true
            }
    );
    if role == "controller"
        && moves_leadership
        && !checks.contains(&RecoveryCheck::ControllerFailover)
    {
        checks.push(RecoveryCheck::ControllerFailover);
    }
    checks
}

/// Whether `check` is measured from the injection rather than the cleanup.
pub(crate) fn measured_from_injection(check: RecoveryCheck) -> bool {
    matches!(
        check,
        RecoveryCheck::LeaderFailover | RecoveryCheck::ControllerFailover
    )
}

/// Everything one fault produced for the evidence.
pub(crate) struct FaultOutcome {
    pub(crate) record: FaultRecord,
    pub(crate) recoveries: Vec<RecoveryRecord>,
    pub(crate) commits: Vec<CommitCheck>,
}

/// What every fault is executed against.
pub(crate) struct Executor {
    pub(crate) contract: Contract,
    pub(crate) nodes: Arc<Nodes>,
    pub(crate) windows: Arc<Windows>,
    pub(crate) log: Arc<FaultLog>,
    pub(crate) trackers: Vec<Arc<CommitTracker>>,
    pub(crate) bootstrap: String,
}

impl Executor {
    /// The topics every rejoin and catch-up waits on, with their partition
    /// counts.
    fn topics(&self) -> Vec<(String, i32)> {
        self.contract
            .paths
            .iter()
            .map(|path| (path.name.clone(), path.partitions))
            .collect()
    }

    /// Execute `fault`.
    pub(crate) async fn run(&self, fault: &ScheduledFault) -> FaultOutcome {
        // Taken before anything else, so a commit acknowledged while the
        // target resolves still counts as before the fault.
        let before: Vec<BTreeMap<i32, i64>> = self
            .trackers
            .iter()
            .map(|tracker| tracker.snapshot())
            .collect();
        let (record, recoveries) = self.execute(fault).await;
        // Every committing group is checked across every fault, whether or
        // not it ran cleanly: a fault that did not run is the verdict's
        // finding, and the offsets still had to survive whatever did happen.
        let mut commits = Vec::new();
        for (tracker, acknowledged) in self.trackers.iter().zip(&before) {
            commits.extend(commit_checks(&self.bootstrap, tracker, &fault.id, acknowledged).await);
        }
        FaultOutcome {
            record,
            recoveries,
            commits,
        }
    }

    /// Inject, hold, clear and measure `fault`.
    async fn execute(&self, fault: &ScheduledFault) -> (FaultRecord, Vec<RecoveryRecord>) {
        let first_topic = self
            .contract
            .paths
            .first()
            .map(|path| path.name.clone())
            .unwrap_or_default();
        let roles = self.nodes.roles(&first_topic).await;
        let mut record = FaultRecord {
            id: fault.id.clone(),
            kind: fault.kind.name().to_owned(),
            target: format!("{:?}", fault.target),
            role: "broker".to_owned(),
            injected_unix_ms: now_ms(),
            cleared_unix_ms: None,
            cleanup_error: None,
            detail: String::new(),
        };
        let target = match resolve_target(fault.target, &self.nodes.ids(), roles) {
            Ok(target) => target,
            Err(error) => {
                record.cleanup_error =
                    Some(format!("not injected: the target did not resolve: {error}"));
                self.log.append(&record);
                return (record, Vec::new());
            }
        };
        let Some(node) = self.nodes.node(target).cloned() else {
            record.cleanup_error = Some(format!("not injected: no node {target}"));
            self.log.append(&record);
            return (record, Vec::new());
        };
        record.target = node.container.clone();
        role_of(target, roles).clone_into(&mut record.role);
        let led = self
            .nodes
            .metadata_from_any(Some(target))
            .await
            .map(|(_, states)| led_by(&states, target))
            .unwrap_or_default();
        let checks = checks_for(fault, &record.role);

        let injected = now_ms();
        record.injected_unix_ms = injected;
        self.windows.injected(injected);
        let kind = fault.kind.clone();
        let host = self.contract.host.clone();
        let (container, root) = (node.container.clone(), node.root.clone());
        let injection =
            tokio::task::spawn_blocking(move || inject(&kind, &container, &root, &host))
                .await
                .unwrap_or_else(|error| Err(format!("injection task: {error}")));
        let Injected { guard, detail } = match injection {
            Ok(injected) => injected,
            Err(error) => {
                record.detail.clone_from(&error);
                record.cleanup_error = Some(format!("not injected: {error}"));
                self.windows.cleared(Span {
                    start: injected,
                    end: now_ms() + recovery_budget(&self.contract, fault).millis_i64(),
                });
                self.log.append(&record);
                return (record, Vec::new());
            }
        };
        record.detail = detail;
        self.log.append(&record);
        eprintln!(
            "soak-contract: {} injected on {} ({}): {}",
            fault.id, record.target, record.role, record.detail
        );

        let mut measuring: Vec<JoinHandle<RecoveryRecord>> = checks
            .iter()
            .filter(|check| measured_from_injection(**check))
            .map(|check| self.measure(&fault.id, *check, target, led.clone(), injected))
            .collect();

        let hold_until = injected + fault.duration.millis_i64();
        let hold = hold_until - now_ms();
        if hold > 0 {
            tokio::time::sleep(Time::from_millis(hold).to_std()).await;
        }
        let cleanup = tokio::task::spawn_blocking(move || guard.clear())
            .await
            .unwrap_or_else(|error| Err(format!("cleanup task: {error}")));
        let cleared = now_ms();
        match cleanup {
            Ok(how) => {
                record.cleared_unix_ms = Some(cleared);
                record.detail = format!("{}; cleared: {how}", record.detail);
            }
            Err(error) => record.cleanup_error = Some(error),
        }
        self.windows.cleared(Span {
            start: injected,
            end: cleared + recovery_budget(&self.contract, fault).millis_i64(),
        });
        self.log.append(&record);
        eprintln!(
            "soak-contract: {} cleared after {}ms{}",
            fault.id,
            cleared - injected,
            record
                .cleanup_error
                .as_ref()
                .map(|error| format!(": {error}"))
                .unwrap_or_default()
        );

        measuring.extend(
            checks
                .iter()
                .filter(|check| !measured_from_injection(**check))
                .map(|check| self.measure(&fault.id, *check, target, led.clone(), cleared)),
        );
        let mut recoveries = Vec::new();
        for task in measuring {
            if let Ok(recovery) = task.await {
                eprintln!(
                    "soak-contract: {} {}: {}",
                    recovery.fault,
                    recovery.check,
                    recovery.took().map_or_else(
                        || "never observed".to_owned(),
                        |took| format!("{}ms", took.millis_i64())
                    )
                );
                recoveries.push(recovery);
            }
        }

        (record, recoveries)
    }

    /// Measure `check` after `fault`, from `from`, on a task of its own.
    fn measure(
        &self,
        fault: &str,
        check: RecoveryCheck,
        target: i32,
        led: BTreeSet<TopicPartition>,
        from: i64,
    ) -> JoinHandle<RecoveryRecord> {
        let nodes = Arc::clone(&self.nodes);
        let topics = self.topics();
        let give_up = from + (check.deadline(&self.contract.deadlines) * 2.0).millis_i64();
        let fault = fault.to_owned();
        tokio::spawn(async move {
            let recovered = loop {
                let waiting = match recovered(&nodes, check, target, &led, &topics).await {
                    Ok(()) => break Some(now_ms()),
                    Err(waiting) => waiting,
                };
                if now_ms() >= give_up {
                    eprintln!("soak-contract: {fault} {check}: gave up; last seen: {waiting}");
                    break None;
                }
                tokio::time::sleep(POLL).await;
            };
            RecoveryRecord {
                fault,
                check,
                from_unix_ms: from,
                recovered_unix_ms: recovered,
            }
        })
    }
}

/// Whether the cluster has recovered as `check` asks, after a fault on
/// `target` that led `led` when it was injected, or what it is still waiting
/// for.
///
/// A rejoin or a catch-up also needs the target to answer a request itself:
/// a broker that comes back inside the replica lag window may never have
/// left the ISR, and the ISR alone would then call a broker recovered before
/// it has even started.
async fn recovered(
    nodes: &Nodes,
    check: RecoveryCheck,
    target: i32,
    led: &BTreeSet<TopicPartition>,
    topics: &[(String, i32)],
) -> Result<(), String> {
    match check {
        RecoveryCheck::LeaderFailover => match nodes.metadata_from_any(Some(target)).await {
            Some((from, states)) if from != target => {
                let waiting = not_failed_over(&states, led, target);
                if waiting.is_empty() {
                    Ok(())
                } else {
                    Err(format!("node {from} reports {waiting:?}"))
                }
            }
            Some(_) => Err(format!("only node {target} answers Metadata")),
            None => Err("no node answers Metadata".to_owned()),
        },
        RecoveryCheck::ControllerFailover => match nodes.controller(Some(target)).await {
            Some(controller) if controller != target => Ok(()),
            other => Err(format!("active controller among the others: {other:?}")),
        },
        RecoveryCheck::IsrRejoin | RecoveryCheck::CatchUpAfterPause => {
            nodes
                .metadata(target)
                .await
                .map_err(|error| format!("node {target} does not answer: {error}"))?;
            match nodes.metadata_from_any(Some(target)).await {
                Some((_, states)) if isr_complete(&states, topics) => Ok(()),
                Some((from, _)) => Err(format!("node {from} reports an incomplete ISR")),
                None => Err("no node answers Metadata".to_owned()),
            }
        }
    }
}

/// Wait until every partition of `topics` has its full ISR, as a boot does
/// before the load starts.
pub(crate) async fn await_full_isr(
    nodes: &Nodes,
    topics: &[(String, i32)],
    limit: Time,
) -> Result<(), String> {
    let deadline = now_ms() + limit.millis_i64();
    loop {
        if let Some((_, states)) = nodes.metadata_from_any(None).await
            && isr_complete(&states, topics)
        {
            return Ok(());
        }
        if now_ms() >= deadline {
            return Err(format!(
                "the ISR of {topics:?} was not complete within {limit:?}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{checks_for, measured_from_injection},
        assert2::check,
        krabka_bench_driver::soak::{
            contract::{FaultKind, RecoveryCheck, Target},
            schedule::ScheduledFault,
        },
        krabka_units::prelude::*,
    };

    fn fault(kind: FaultKind, target: Target) -> ScheduledFault {
        ScheduledFault {
            id: "f#0".into(),
            plan: "f".into(),
            checks: kind.checks(target),
            kind,
            target,
            start: secs(10),
            duration: secs(5),
        }
    }

    #[yare::parameterized(
        leader_restart_on_the_controller = { FaultKind::Restart { graceful: false }, Target::Leader, "controller", true },
        leader_restart_on_a_leader = { FaultKind::Restart { graceful: false }, Target::Leader, "leader", false },
        any_failover_pause_on_the_controller = { FaultKind::Pause { intends_failover: true }, Target::Broker(0), "controller", true },
        quiet_pause_on_the_controller = { FaultKind::Pause { intends_failover: false }, Target::Broker(0), "controller", false },
        controller_restart_measured_once = { FaultKind::Restart { graceful: true }, Target::Controller, "controller", true },
    )]
    fn a_fault_that_lands_on_the_controller_is_measured_for_its_failover(
        kind: FaultKind,
        target: Target,
        role: &str,
        measured: bool,
    ) {
        let checks = checks_for(&fault(kind, target), role);
        let failovers = checks
            .iter()
            .filter(|check| **check == RecoveryCheck::ControllerFailover)
            .count();
        check!(failovers == usize::from(measured));
    }

    #[yare::parameterized(
        leader_failover = { RecoveryCheck::LeaderFailover, true },
        controller_failover = { RecoveryCheck::ControllerFailover, true },
        isr_rejoin = { RecoveryCheck::IsrRejoin, false },
        catch_up = { RecoveryCheck::CatchUpAfterPause, false },
    )]
    fn a_failover_runs_from_the_injection_and_a_rejoin_from_the_cleanup(
        check: RecoveryCheck,
        from_injection: bool,
    ) {
        check!(measured_from_injection(check) == from_injection);
    }
}
