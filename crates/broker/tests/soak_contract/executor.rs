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
        lifecycle_run::LifecycleRun,
        topology::{
            Node, Nodes, PartitionState, TopicPartition, isr_complete, led_by, not_failed_over,
            resolve_target, role_of,
        },
        transitions::{self, Expect},
    },
    krabka_bench_driver::soak::{
        client::{
            Endpoint,
            load::CommitTracker,
            readback::commit_checks,
            windows::{Span, Windows},
        },
        contract::{Contract, FaultKind, RecoveryCheck},
        deadline::checks_for,
        evidence::{CommitCheck, FaultRecord, RecoveryRecord},
        schedule::ScheduledFault,
    },
    krabka_units::prelude::*,
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
        time::Duration,
    },
    tokio::{task, task::JoinHandle, time},
};

/// How often a recovery is polled.
const POLL: Duration = Duration::from_millis(250);

/// Whether `check` is measured from the injection rather than the cleanup.
pub fn measured_from_injection(check: RecoveryCheck) -> bool {
    matches!(
        check,
        RecoveryCheck::LeaderFailover
            | RecoveryCheck::ControllerFailover
            | RecoveryCheck::CertificateServed
            | RecoveryCheck::ReassignmentComplete
    )
}

/// Everything one fault produced for the evidence.
pub struct FaultOutcome {
    pub(crate) record: FaultRecord,
    pub(crate) recoveries: Vec<RecoveryRecord>,
    pub(crate) commits: Vec<CommitCheck>,
}

/// What every fault is executed against.
pub struct Executor {
    pub(crate) contract: Contract,
    pub(crate) nodes: Arc<Nodes>,
    pub(crate) windows: Arc<Windows>,
    pub(crate) log: Arc<FaultLog>,
    pub(crate) trackers: Vec<Arc<CommitTracker>>,
    pub(crate) endpoint: Endpoint,
    /// The release images and the rotation state of a lifecycle run.
    pub(crate) lifecycle: Option<Arc<LifecycleRun>>,
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
            commits.extend(commit_checks(&self.endpoint, tracker, &fault.id, acknowledged).await);
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
            node: None,
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
        record.node = Some(target);
        role_of(target, roles).clone_into(&mut record.role);
        // `None` when no node answered: what the target led is then unknown,
        // not empty, and a leader failover cannot be measured.
        let led = self
            .nodes
            .metadata_from_any(Some(target))
            .await
            .map(|(_, states)| led_by(&states, target));
        if led.is_none() {
            eprintln!(
                "soak-contract: {}: no node answered Metadata before the injection, so what node {target} led is unknown",
                fault.id
            );
        }
        let checks = checks_for(fault, &record.role);

        let injected = now_ms();
        record.injected_unix_ms = injected;
        self.windows.injected(injected);
        let (injection, expect) = self.inject(fault, &node).await;
        let Injected { guard, detail } = match injection {
            Ok(injected) => injected,
            Err(error) => {
                record.detail.clone_from(&error);
                record.cleanup_error = Some(format!("not injected: {error}"));
                self.windows.cleared(Span {
                    start: injected,
                    end: now_ms() + fault.recovery_budget(&self.contract.deadlines).millis_i64(),
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
            .map(|check| {
                self.measure(
                    &fault.id,
                    *check,
                    target,
                    led.clone(),
                    expect.clone(),
                    injected,
                )
            })
            .collect();

        let hold_until = injected + fault.duration.millis_i64();
        let hold = hold_until - now_ms();
        if hold > 0 {
            time::sleep(Time::from_millis(hold).to_std()).await;
        }
        let cleanup = task::spawn_blocking(move || guard.clear())
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
            end: cleared + fault.recovery_budget(&self.contract.deadlines).millis_i64(),
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
                .map(|check| {
                    self.measure(
                        &fault.id,
                        *check,
                        target,
                        led.clone(),
                        expect.clone(),
                        cleared,
                    )
                }),
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

    /// Inject `fault` on `node`: a request to the cluster for a rotation or
    /// a reassignment, otherwise a Docker operation off the async workers.
    /// Returns what a lifecycle check then waits to observe.
    async fn inject(
        &self,
        fault: &ScheduledFault,
        node: &Node,
    ) -> (Result<Injected, String>, Option<Expect>) {
        if matches!(
            fault.kind,
            FaultKind::CertificateRotation | FaultKind::Reassignment { .. }
        ) {
            let Some(run) = &self.lifecycle else {
                return (
                    Err("a lifecycle transition outside a lifecycle run".to_owned()),
                    None,
                );
            };
            return match transitions::inject(&fault.kind, node, &self.nodes, run).await {
                Ok((injected, expect)) => (Ok(injected), Some(expect)),
                Err(error) => (Err(error), None),
            };
        }
        let relaunch = match (&fault.kind, &self.lifecycle) {
            (FaultKind::Roll { to }, Some(run)) => Some(node.launch.argv(run.image(*to))),
            _ => None,
        };
        let kind = fault.kind.clone();
        let host = self.contract.host.clone();
        let (container, root) = (node.container.clone(), node.root.clone());
        let injected =
            task::spawn_blocking(move || inject(&kind, &container, &root, &host, relaunch))
                .await
                .unwrap_or_else(|error| Err(format!("injection task: {error}")));
        (injected, None)
    }

    /// Measure `check` after `fault`, from `from`, on a task of its own.
    fn measure(
        &self,
        fault: &str,
        check: RecoveryCheck,
        target: i32,
        led: Option<BTreeSet<TopicPartition>>,
        expect: Option<Expect>,
        from: i64,
    ) -> JoinHandle<RecoveryRecord> {
        let nodes = Arc::clone(&self.nodes);
        let topics = self.topics();
        let give_up = from + (check.deadline(&self.contract.deadlines) * 2.0).millis_i64();
        let fault = fault.to_owned();
        tokio::spawn(async move {
            let recovered = loop {
                let waiting = match recovered(
                    &nodes,
                    check,
                    target,
                    led.as_ref(),
                    expect.as_ref(),
                    &topics,
                )
                .await
                {
                    Ok(()) => break Some(now_ms()),
                    Err(Waiting::Unmeasurable(why)) => {
                        eprintln!("soak-contract: {fault} {check}: gave up: {why}");
                        break None;
                    }
                    Err(Waiting::For(waiting)) => waiting,
                };
                if now_ms() >= give_up {
                    eprintln!("soak-contract: {fault} {check}: gave up; last seen: {waiting}");
                    break None;
                }
                time::sleep(POLL).await;
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

/// Why a recovery has not been observed yet.
#[derive(Debug)]
enum Waiting {
    /// Still waiting, for what the message says.
    For(String),
    /// It can never be observed, for the reason the message gives.
    Unmeasurable(String),
}

/// Whether leadership has failed over off `target`, given what it `led` when
/// the fault was injected (`None` when that is unknown) and the `answer` of
/// the node that served `Metadata` (`None` when none did).
///
/// An unknown `led` is never failed over: an empty set would pass at once,
/// and record a failover that was never seen.
fn leader_failover(
    led: Option<&BTreeSet<TopicPartition>>,
    answer: Option<(i32, &[PartitionState])>,
    target: i32,
) -> Result<(), Waiting> {
    let Some(led) = led else {
        return Err(Waiting::Unmeasurable(format!(
            "what node {target} led before the injection is unknown"
        )));
    };
    match answer {
        Some((from, states)) if from != target => {
            let waiting = not_failed_over(states, led, target);
            if waiting.is_empty() {
                Ok(())
            } else {
                Err(Waiting::For(format!("node {from} reports {waiting:?}")))
            }
        }
        Some(_) => Err(Waiting::For(format!("only node {target} answers Metadata"))),
        None => Err(Waiting::For("no node answers Metadata".to_owned())),
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
    led: Option<&BTreeSet<TopicPartition>>,
    expect: Option<&Expect>,
    topics: &[(String, i32)],
) -> Result<(), Waiting> {
    match check {
        RecoveryCheck::CertificateServed | RecoveryCheck::ReassignmentComplete => match expect {
            Some(expect) => transitions::observed(nodes, expect)
                .await
                .map_err(Waiting::For),
            None => Err(Waiting::Unmeasurable(
                "the transition was not injected, so there is nothing to observe".to_owned(),
            )),
        },
        RecoveryCheck::LeaderFailover => {
            let answer = match led {
                Some(_) => nodes.metadata_from_any(Some(target)).await,
                None => None,
            };
            leader_failover(
                led,
                answer
                    .as_ref()
                    .map(|(from, states)| (*from, states.as_slice())),
                target,
            )
        }
        RecoveryCheck::ControllerFailover => match nodes.controller(Some(target)).await {
            Some(controller) if controller != target => Ok(()),
            other => Err(Waiting::For(format!(
                "active controller among the others: {other:?}"
            ))),
        },
        RecoveryCheck::IsrRejoin | RecoveryCheck::CatchUpAfterPause => {
            nodes
                .metadata(target)
                .await
                .map_err(|error| Waiting::For(format!("node {target} does not answer: {error}")))?;
            match nodes.metadata_from_any(Some(target)).await {
                Some((_, states)) if isr_complete(&states, topics) => Ok(()),
                Some((from, _)) => Err(Waiting::For(format!(
                    "node {from} reports an incomplete ISR"
                ))),
                None => Err(Waiting::For("no node answers Metadata".to_owned())),
            }
        }
    }
}

/// Wait until every partition of `topics` has its full ISR, as a boot does
/// before the load starts.
pub async fn await_full_isr(
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
        time::sleep(POLL).await;
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use {
        super::{
            PartitionState, TopicPartition, Waiting, checks_for, leader_failover,
            measured_from_injection,
        },
        assert2::check,
        krabka_bench_driver::soak::{
            contract::{FaultKind, RecoveryCheck, Target},
            schedule::ScheduledFault,
        },
        krabka_units::prelude::*,
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
        std::collections::BTreeSet,
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

    #[derive(Debug, PartialEq, Eq)]
    enum Seen {
        FailedOver,
        Waiting,
        Unmeasurable,
    }

    #[derive(Default)]
    struct RecoveryScenario {
        fault: Option<ScheduledFault>,
        role: String,
        checks: Vec<RecoveryCheck>,
        check: Option<RecoveryCheck>,
        from_injection: Slot<bool>,
        led: Option<BTreeSet<TopicPartition>>,
        answer: Option<(i32, Vec<PartitionState>)>,
        outcome: Option<Seen>,
    }

    #[fixture]
    fn recovery() -> RecoveryScenario {
        // A recovery judgment uses one fault and the metadata available for it.
        RecoveryScenario {
            fault: None,
            ..Default::default()
        }
    }

    #[given("fault {kind} targets {target} and lands on a node holding role {role}")]
    fn fault_landing(recovery: &mut RecoveryScenario, kind: String, target: String, role: String) {
        let kind = match kind.as_str() {
            "ungraceful-restart" => FaultKind::Restart { graceful: false },
            "graceful-restart" => FaultKind::Restart { graceful: true },
            "failover-pause" => FaultKind::Pause {
                intends_failover: true,
            },
            "quiet-pause" => FaultKind::Pause {
                intends_failover: false,
            },
            _ => panic!("a supported fault kind"),
        };
        let target = match target.as_str() {
            "leader" => Target::Leader,
            "controller" => Target::Controller,
            "broker-zero" => Target::Broker(0),
            _ => panic!("a supported fault target"),
        };
        recovery.fault = Some(fault(kind, target));
        recovery.role = role;
    }

    #[when("the executor selects recovery checks for the role the fault actually hit")]
    fn select_recovery_checks(recovery: &mut RecoveryScenario) {
        recovery.checks = checks_for(
            recovery.fault.as_ref().expect("the fault was supplied"),
            &recovery.role,
        );
    }

    #[then("controller failover is measured exactly once: {measured}")]
    fn controller_measurement(recovery: &RecoveryScenario, measured: bool) {
        let failovers = recovery
            .checks
            .iter()
            .filter(|check| **check == RecoveryCheck::ControllerFailover)
            .count();
        check!(failovers == usize::from(measured));
    }

    #[given("the recovery check is {check}")]
    fn named_recovery_check(recovery: &mut RecoveryScenario, check: String) {
        recovery.check = Some(match check.as_str() {
            "leader-failover" => RecoveryCheck::LeaderFailover,
            "controller-failover" => RecoveryCheck::ControllerFailover,
            "isr-rejoin" => RecoveryCheck::IsrRejoin,
            "catch-up" => RecoveryCheck::CatchUpAfterPause,
            "certificate-served" => RecoveryCheck::CertificateServed,
            "reassignment-complete" => RecoveryCheck::ReassignmentComplete,
            _ => panic!("a supported recovery check"),
        });
    }

    #[when("the executor chooses whether the recovery clock starts at injection")]
    fn choose_recovery_clock(recovery: &RecoveryScenario) {
        recovery.from_injection.set(measured_from_injection(
            recovery.check.expect("the recovery check was supplied"),
        ));
    }

    #[then("the clock starts at injection rather than cleanup: {injection}")]
    fn recovery_clock_origin(recovery: &RecoveryScenario, injection: bool) {
        check!(recovery.from_injection.get() == Some(injection));
    }

    #[given("before injection node one led partition set {led}")]
    fn previously_led_partitions(recovery: &mut RecoveryScenario, led: String) {
        recovery.led = match led.as_str() {
            "unknown" => None,
            "none" => Some(BTreeSet::new()),
            _ => Some(
                led.split(',')
                    .map(|partition| {
                        (
                            "t".to_owned(),
                            partition.parse().expect("a previously led partition"),
                        )
                    })
                    .collect(),
            ),
        };
    }

    #[given("metadata comes from node {from} with partition leader {leader}")]
    fn reported_partition_leader(recovery: &mut RecoveryScenario, from: String, leader: String) {
        recovery.answer = if from == "none" {
            None
        } else {
            Some((from.parse().expect("a responding node"), vec![
                PartitionState {
                    topic: "t".to_owned(),
                    partition: 0,
                    leader: leader.parse().expect("a reported leader"),
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2, 3],
                },
            ]))
        };
    }

    #[when("the executor judges failover away from node one using only known metadata")]
    fn observe_leader_failover(recovery: &mut RecoveryScenario) {
        recovery.outcome = Some(
            match leader_failover(
                recovery.led.as_ref(),
                recovery
                    .answer
                    .as_ref()
                    .map(|(from, states)| (*from, states.as_slice())),
                1,
            ) {
                Ok(()) => Seen::FailedOver,
                Err(Waiting::For(_)) => Seen::Waiting,
                Err(Waiting::Unmeasurable(_)) => Seen::Unmeasurable,
            },
        );
    }

    #[then("the observed leader recovery is {outcome}")]
    fn observed_leader_recovery(recovery: &RecoveryScenario, outcome: String) {
        let expected = match outcome.as_str() {
            "failed-over" => Seen::FailedOver,
            "waiting" => Seen::Waiting,
            "unmeasurable" => Seen::Unmeasurable,
            _ => panic!("a recovery verdict"),
        };
        check!(recovery.outcome.as_ref().expect("the failover was judged") == &expected);
    }

    scenarios!("tests/features/migrated/tests/soak_contract/executor.feature", fixtures = [recovery: RecoveryScenario]);
}
