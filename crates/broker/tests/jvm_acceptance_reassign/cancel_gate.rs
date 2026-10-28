//! `kafka-reassign-partitions --cancel` against the KFC-9 break-glass gate, in
//! all three states the gate has.
//!
//! # The claim
//!
//! KFC-9 makes cancelling a reassignment one of the five privileged
//! transitions behind a two-person rule. A cancel discards the replica the
//! move had already built, so the cluster it leaves behind is less durable
//! than the one the operator started with, and KFC-9's answer is that a second
//! person must have said so first.
//!
//! The gate refuses with `POLICY_VIOLATION` (44) and an `error_message` naming
//! the action and the partition. Both halves are claims about somebody else's
//! code: `Errors.forCode(44)` is Kafka's, and whether an `error_message` on an
//! `AlterPartitionReassignments` row reaches the operator at all is
//! `ReassignPartitionsCommand`'s. This is the only place either is checked
//! against Apache Kafka's own client rather than against krabka's.
//!
//! # The three states
//!
//! A gate is only a gate if it is not a wall, so all three states run against
//! the same cluster, the same topic and the same command:
//!
//! 1. **Off.** `break_glass.approvers` is empty, and a cancel behaves as it
//!    does on a cluster with no `[break_glass]` section at all. This is the
//!    control: without it, a broker that refused every cancel would pass the
//!    case below.
//! 2. **On, unapproved.** The same command is refused, and the refusal reaches
//!    the JVM tool with Kafka's own exception class and krabka's own sentence.
//! 3. **On, approved.** Two distinct operators approve a proposal through the
//!    krabka-private API, and the same command succeeds again.
//!
//! # Why the gate is turned on by a restart
//!
//! `break_glass` is file configuration and is read when a broker starts, so
//! state 1 and states 2-3 are two different clusters. They are the *same*
//! cluster here: the three brokers are shut down and started again on their
//! own log directories in `Rejoin` mode, with the configuration the shared
//! harness handed back and one field changed. Rebuilding from scratch would
//! lose the topic and the in-flight reassignment the cancel is about.
//!
//! # What this file does not cover
//!
//! Operator signatures. `break_glass.signed_actions` is emptied, so the
//! approvals are unsigned. The canonical byte layout and the verification
//! rules are the in-process suites' work; what a container can say is what the
//! JVM tool printed and what it exited with.

use {
    crate::{
        cluster::offline_for_partition,
        jvm_acceptance::{
            break_glass::{Approvals, approve, propose},
            broker0_advertised, plain_client_properties,
            start_three_broker_sasl_plaintext_jvm_cluster, wait_three_brokers_registered,
        },
        oracle::{AssignedTopicCommandSetup, CliRun, Side, ToolFile},
        tool_output::{Assignment, TopicPartition, parse_cancelled, reassignment_json},
    },
    assert2::{assert, check},
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle, authorizer::SimpleAclAuthorizer,
    },
    std::sync::Arc,
    tempfile::TempDir,
};

/// The super-user the tools authenticate as.
const ADMIN: (&str, &str) = ("admin", "admin-secret");
/// The operator who opens the proposal. They may not also approve it.
const PROPOSER: (&str, &str) = ("alice", "alice-secret");
/// The two approvers. Two distinct principals is what makes the rule a
/// two-person rule rather than a two-click rule.
const APPROVER_ONE: (&str, &str) = ("bob", "bob-secret");
const APPROVER_TWO: (&str, &str) = ("carol", "carol-secret");

/// The topic whose reassignment is started and then cancelled, three times.
const TOPIC: &str = "krabka-cancel-gate-itest";

/// The wire value of `BreakGlassAction::CancelReassignment` on the
/// krabka-private `ProposeBreakGlass` request.
///
/// The broker's own mapping is crate-private, so the value is written out
/// here. It is part of the private API's contract, and a change to it that
/// this constant did not follow would show up as a proposal that authorizes
/// nothing.
const WIRE_CANCEL_REASSIGNMENT: i8 = 5;

/// Where the tool's files are placed inside its container.
const PLAN_JSON: &str = "/tmp/krabka-cancel-plan.json";
const CLIENT_PROPS: &str = "/client.properties";

/// One `kafka-reassign-partitions` invocation as the super-user.
fn reassign(side: &Side<'_>, props: &str, args: &[&str], plan: &str) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    full.extend_from_slice(&[
        "--reassignment-json-file",
        PLAN_JSON,
        "--command-config",
        CLIENT_PROPS,
    ]);
    side.run_with_files(
        "kafka-reassign-partitions",
        &full,
        &[
            ToolFile::new(PLAN_JSON, plan),
            ToolFile::new(CLIENT_PROPS, props),
        ],
        None,
    )
}

/// Start a reassignment that will not finish on its own, and answer with the
/// document that names it.
///
/// `target` is a registered broker stopped by the caller, so the move stays in
/// flight -- which is the premise `--cancel` needs: the JVM tool asks the
/// broker which partitions are actually reassigning and sends nothing at all
/// for the ones that are not.
async fn start_a_reassignment(
    handle: &BrokerHandle,
    side: &Side<'_>,
    props: &str,
    target: i32,
) -> String {
    let current = handle
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let staying = i32::try_from(current.replicas[0].0).expect("a node id fits");
    let plan = reassignment_json(&[Assignment {
        partition: TopicPartition::new(TOPIC, 0),
        replicas: vec![staying, target],
    }]);
    let started = reassign(side, props, &["--execute"], &plan);
    assert!(
        started.succeeded(),
        "starting a reassignment is not gated:\n{}",
        started.text(),
    );
    handle
        .wait_for_image(|image| {
            image
                .partition(TOPIC, 0)
                .is_some_and(|record| !record.adding_replicas.is_empty())
        })
        .await;
    plan
}

/// Boot the three brokers again on the directories they already hold.
///
/// `Rejoin` rather than `Bootstrap`: the metadata log is already there, and
/// this is the same cluster with one configuration field changed, not a new
/// one. All three starts are awaited together, because a voter's start blocks
/// until its controller sees a committed leader and a leader needs a majority
/// of the static voter set up and dialable.
async fn restart(configs: [BrokerConfig; 3]) -> [BrokerHandle; 3] {
    let starts = configs.map(|mut config| {
        config.bootstrap_mode = BootstrapMode::Rejoin;
        tokio::spawn(async move { Broker::start(config).await })
    });
    let mut handles = Vec::with_capacity(3);
    for start in starts {
        handles.push(
            start
                .await
                .expect("broker spawn join")
                .expect("broker start"),
        );
    }
    handles.try_into().ok().expect("three handles")
}

/// Open a `cancel_reassignment` proposal on the topic and have both approvers
/// sign off.
///
/// The target is the bare topic name: KFC-9 lets a proposal on a topic cover
/// every partition of it for the actions that name one, and a cancel is one of
/// those, so this also checks that widening on the way through.
async fn approve_a_cancel(bootstrap: &str) -> [Approvals; 2] {
    let proposal = propose(
        bootstrap,
        PROPOSER,
        WIRE_CANCEL_REASSIGNMENT,
        TOPIC,
        "the move is making the incident worse",
    )
    .await;
    [
        approve(bootstrap, APPROVER_ONE, proposal).await,
        approve(bootstrap, APPROVER_TWO, proposal).await,
    ]
}

struct CancelPermission {
    handles: [Option<BrokerHandle>; 3],
    configs: Option<[BrokerConfig; 3]>,
    dirs: Option<[TempDir; 3]>,
    plan: String,
    output: Option<CliRun>,
    approvals: Option<[Approvals; 2]>,
}
#[rstest::fixture]
fn cancel() -> CancelPermission {
    // Start this scenario with fresh inputs and observations.

    CancelPermission {
        handles: [None, None, None],
        configs: None,
        dirs: None,
        plan: String::new(),
        output: None,
        approvals: None,
    }
}
#[rstest_bdd_macros::given(
    "a SASL cluster with admin and operators alice, bob, carol has an offline-target reassignment for krabka-cancel-gate-itest-0"
)]
async fn pending_move(cancel: &mut CancelPermission) {
    let (h1, h2, h3, cfg1, cfg2, cfg3, d1, d2, d3) = Box::pin(
        start_three_broker_sasl_plaintext_jvm_cluster()
            .admin(ADMIN.0)
            .admin_pass(ADMIN.1)
            .extra_users(&[PROPOSER, APPROVER_ONE, APPROVER_TWO])
            .wait_until_registered(true)
            .call(),
    )
    .await;

    let props = plain_client_properties(ADMIN.0, ADMIN.1);
    let advertised = broker0_advertised().to_owned();
    let side = Side::Krabka {
        bootstrap: &advertised,
    };

    side.create_assigned_topic(AssignedTopicCommandSetup {
        topic: TOPIC,
        props_path: CLIENT_PROPS,
        files: &[ToolFile::new(CLIENT_PROPS, &props)],
        ..Default::default()
    });
    h1.wait_until_partition_present(TOPIC, 0).await;

    // Keep one valid target offline so every cancel below observes a real
    // in-flight reassignment instead of racing replica catch-up.
    let OfflineReassignment { handles, plan } =
        offline_reassignment([h1, h2, h3], &side, &props, "partition record").await;
    cancel.handles = handles;
    cancel.plan = plan;
    cancel.configs = Some([cfg1, cfg2, cfg3]);
    cancel.dirs = Some([d1, d2, d3]);
}
#[rstest_bdd_macros::when("the administrator cancels the reassignment while preserving throttles")]
fn cancel_move(cancel: &mut CancelPermission) {
    let props = plain_client_properties(ADMIN.0, ADMIN.1);
    let side = Side::Krabka {
        bootstrap: broker0_advertised(),
    };
    cancel.output = Some(reassign(
        &side,
        &props,
        &["--cancel", "--preserve-throttles"],
        &cancel.plan,
    ));
}
#[rstest_bdd_macros::then(
    "the cancellation succeeds and reports partition krabka-cancel-gate-itest-0"
)]
fn allowed(cancel: &CancelPermission) {
    let output = cancel.output.as_ref().expect("cancel attempted");
    check!(output.succeeded(), "{}", output.text());
    check!(parse_cancelled(&output.stdout) == vec![TopicPartition::new(TOPIC, 0)]);
}
#[rstest_bdd_macros::when(
    "the same cluster restarts on its existing logs with unsigned two-person approvals and starts another offline-target reassignment"
)]
async fn enable_approvals(cancel: &mut CancelPermission) {
    let approvers: Vec<String> = [PROPOSER, APPROVER_ONE, APPROVER_TWO]
        .iter()
        .map(|(user, _)| format!("User:{user}"))
        .collect();
    let gated = cancel
        .configs
        .take()
        .expect("original configurations")
        .map(|mut config| {
            // An empty approver set turns the whole workflow off, so naming the
            // set is what puts the cancel behind an approval. `signed_actions` is
            // emptied explicitly rather than left to the file default, which names
            // three actions this suite has no key material to sign for.
            config.break_glass.approvers = approvers.clone();
            config.break_glass.signed_actions = Vec::new();
            // The cluster runs a `SimpleAclAuthorizer` whose only super user is
            // the admin the tools authenticate as, and the break-glass RPCs are
            // cluster operations. Without this the operators are refused at the
            // authorizer with `CLUSTER_AUTHORIZATION_FAILED` and never reach the
            // two-person rule, which is the thing under test.
            for (user, _) in [PROPOSER, APPROVER_ONE, APPROVER_TWO] {
                config.super_users.insert(user.to_owned());
            }
            // The authorizer is built from the super-user set, so it has to be
            // rebuilt after widening it; the one the cluster helper constructed
            // captured the old set.
            config.authorizer = Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
            config
        });
    for handle in cancel.handles.iter_mut().filter_map(Option::take) {
        handle.shutdown().await;
    }
    let [h1, h2, h3] = restart(gated).await;
    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    // ── 2. the gate on, with no proposal ───────────────────────────────────
    let props = plain_client_properties(ADMIN.0, ADMIN.1);
    let advertised = broker0_advertised().to_owned();
    let side = Side::Krabka {
        bootstrap: &advertised,
    };
    let OfflineReassignment { handles, plan } = offline_reassignment(
        [h1, h2, h3],
        &side,
        &props,
        "partition record after restart",
    )
    .await;
    cancel.handles = handles;
    cancel.plan = plan;
}
#[rstest_bdd_macros::then(
    "cancellation is refused with this exact policy sentence and the reassignment remains in flight"
)]
fn refused(cancel: &CancelPermission, docstring: String) {
    let output = cancel.output.as_ref().expect("cancel attempted");
    check!(!output.succeeded(), "{}", output.text());
    check!(
        output.text().contains(docstring.trim()),
        "{}",
        output.text()
    );
    let record = cancel.handles[0]
        .as_ref()
        .expect("bootstrap broker")
        .partition_record_for_test(TOPIC, 0)
        .expect("partition");
    check!(!record.adding_replicas.is_empty());
}
#[rstest_bdd_macros::when(
    "alice proposes cancel_reassignment and distinct operators bob and carol approve it"
)]
async fn authorize_cancel(cancel: &mut CancelPermission) {
    cancel.approvals = Some(approve_a_cancel(broker0_advertised()).await);
}
#[rstest_bdd_macros::then("one approval is insufficient and the second reaches the required count")]
fn two_people(cancel: &CancelPermission) {
    let [first, second] = cancel.approvals.as_ref().expect("approvals sent");
    check!(first.held == 1 && first.held < first.required);
    check!(second.held == second.required);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/cancel_gate.feature",
    name = "reassign partitions cancel reports the break glass gate to the jvm tool"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reassign_partitions_cancel_reports_the_break_glass_gate_to_the_jvm_tool(
    cancel: CancelPermission,
) {
    for handle in cancel.handles.iter_mut().filter_map(Option::take) {
        handle.shutdown().await;
    }
    drop(cancel);
}

/// The brokers still live while one target is offline, and the plan whose
/// reassignment is in flight.
struct OfflineReassignment {
    handles: [Option<BrokerHandle>; 3],
    plan: String,
}

async fn offline_reassignment(
    brokers: [BrokerHandle; 3],
    side: &Side<'_>,
    props: &str,
    context: &str,
) -> OfflineReassignment {
    let (target, handles, _current) = offline_for_partition(brokers, TOPIC, context).await;
    let handle = handles[0].as_ref().expect("bootstrap broker stays live");
    let plan = start_a_reassignment(
        handle,
        side,
        props,
        i32::try_from(target).expect("a node id fits"),
    )
    .await;
    OfflineReassignment { handles, plan }
}
