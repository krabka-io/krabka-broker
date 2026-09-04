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

use assert2::{assert, check};
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use krabka_protocol::krabka::break_glass::{ApproveBreakGlassRequest, ProposeBreakGlassRequest};

use crate::{
    jvm_acceptance::{
        broker0_advertised, nc_check_connectivity, plain_jaas,
        start_three_broker_sasl_plaintext_jvm_cluster_with_users, wait_three_brokers_registered,
    },
    oracle::{CliRun, Side, ToolFile},
    support,
    tool_output::{Assignment, TopicPartition, parse_cancelled, reassignment_json},
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

/// The JVM exception `Errors.forCode` builds for `POLICY_VIOLATION` (44).
///
/// The fully-qualified name is the assertion and not the bare class: a bare
/// name would also match a sentence that merely mentioned it, and the point is
/// that Kafka's own client constructed this class out of the number krabka
/// sent.
const POLICY_VIOLATION_EXCEPTION: &str = "org.apache.kafka.common.errors.PolicyViolationException";

/// Where the tool's files are placed inside its container.
const PLAN_JSON: &str = "/krabka-cancel-plan.json";
const CLIENT_PROPS: &str = "/client.properties";

/// The refusal the gate words when no proposal covers the request.
///
/// Rebuilt here rather than imported, because it is the sentence an operator
/// reads: a broker that still answered 44 while dropping this text would leave
/// the on-call with a policy violation and no next step, and the code alone
/// cannot see that.
fn no_proposal_refusal(partition: i32) -> String {
    format!(
        "break-glass refused cancel_reassignment on {TOPIC}-{partition}: \
         no approved proposal covers the request"
    )
}

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
/// Nothing in this harness makes the new replica catch up, so the move stays
/// in flight -- which is the premise `--cancel` needs: the JVM tool asks the
/// broker which partitions are actually reassigning and sends nothing at all
/// for the ones that are not.
async fn start_a_reassignment(handle: &BrokerHandle, side: &Side<'_>, props: &str) -> String {
    let current = handle
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let held: std::collections::BTreeSet<u64> =
        current.replicas.iter().map(|node| node.0).collect();
    let targets: Vec<i32> = (1_u64..=3)
        .filter(|node| !held.contains(node))
        .map(|node| i32::try_from(node).expect("a node id fits"))
        .collect();
    let plan = reassignment_json(&[Assignment {
        partition: TopicPartition::new(TOPIC, 0),
        replicas: targets,
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
async fn approve_a_cancel(bootstrap: &str) {
    let proposer = support::sasl_client(bootstrap, PROPOSER.0, PROPOSER.1).await;
    let opened = proposer
        .send(ProposeBreakGlassRequest {
            action: WIRE_CANCEL_REASSIGNMENT,
            target: TOPIC.to_owned(),
            reason: "the move is making the incident worse".to_owned(),
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass");
    let code = opened.error_code;
    let message = opened.error_message;
    assert!(code == 0, "propose: code={code} message={message:?}");

    let first = approve(bootstrap, APPROVER_ONE, opened.proposal_id).await;
    check!(
        first.0 == 1 && first.0 < first.1,
        "one approval is one distinct principal and must not be enough: {first:?}",
    );
    let second = approve(bootstrap, APPROVER_TWO, opened.proposal_id).await;
    check!(
        second.0 == second.1,
        "two distinct principals must satisfy the rule: {second:?}",
    );
}

/// Add one approval as `operator`, and answer with `(held, required)`.
async fn approve(
    bootstrap: &str,
    operator: (&str, &str),
    proposal_id: krabka_protocol::primitives::uuid::Uuid,
) -> (i32, i32) {
    let client = support::sasl_client(bootstrap, operator.0, operator.1).await;
    let response = client
        .send(ApproveBreakGlassRequest {
            proposal_id,
            withdraw: false,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass");
    let code = response.error_code;
    let message = response.error_message;
    let who = operator.0;
    assert!(
        code == 0,
        "approve as {who}: code={code} message={message:?}"
    );
    (response.approvals_held, response.approvals_required)
}

/// The gate off, then on and unapproved, then on and approved -- the same
/// `--cancel` each time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn reassign_partitions_cancel_reports_the_break_glass_gate_to_the_jvm_tool() {
    let (h1, h2, h3, cfg1, cfg2, cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN.0,
            ADMIN.1,
            &[PROPOSER, APPROVER_ONE, APPROVER_TWO],
        )
        .await;
    nc_check_connectivity();
    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN.0, ADMIN.1),
    );
    let advertised = broker0_advertised().to_owned();
    let side = Side::Krabka {
        bootstrap: &advertised,
    };

    side.run_with_files(
        "kafka-topics",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--command-config",
            CLIENT_PROPS,
        ],
        &[ToolFile::new(CLIENT_PROPS, &props)],
        None,
    )
    .expect_success();
    h1.wait_until_partition_present(TOPIC, 0).await;

    // ── 1. the gate off ────────────────────────────────────────────────────
    let plan = start_a_reassignment(&h1, &side, &props).await;
    let ungated = reassign(&side, &props, &["--cancel"], &plan);
    check!(
        ungated.succeeded(),
        "with no approver set a cancel is not gated at all:\n{}",
        ungated.text(),
    );
    check!(
        parse_cancelled(&ungated.stdout) == vec![TopicPartition::new(TOPIC, 0)],
        "the ungated cancel must name the partition it cancelled:\n{}",
        ungated.stdout,
    );

    // ── the same cluster, with the two-person rule turned on ───────────────
    let approvers: Vec<String> = [PROPOSER, APPROVER_ONE, APPROVER_TWO]
        .iter()
        .map(|(user, _)| format!("User:{user}"))
        .collect();
    let gated = [cfg1, cfg2, cfg3].map(|mut config| {
        // An empty approver set turns the whole workflow off, so naming the
        // set is what puts the cancel behind an approval. `signed_actions` is
        // emptied explicitly rather than left to the file default, which names
        // three actions this suite has no key material to sign for.
        config.break_glass.approvers = approvers.clone();
        config.break_glass.signed_actions = Vec::new();
        config
    });
    for handle in [h1, h2, h3] {
        handle.shutdown().await;
    }
    let [h1, h2, h3] = restart(gated).await;
    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    // ── 2. the gate on, with no proposal ───────────────────────────────────
    let plan = start_a_reassignment(&h1, &side, &props).await;
    let refused = reassign(&side, &props, &["--cancel"], &plan);
    check!(
        !refused.succeeded(),
        "a gated cancel with no approval must exit non-zero:\n{}",
        refused.text(),
    );
    check!(
        refused.text().contains(POLICY_VIOLATION_EXCEPTION),
        "the refusal must reach the tool as Kafka's policy violation:\n{}",
        refused.text(),
    );
    check!(
        refused.text().contains(&no_proposal_refusal(0)),
        "the refusal must carry the broker's own sentence, {:?}:\n{}",
        no_proposal_refusal(0),
        refused.text(),
    );
    let record = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after the refusal");
    check!(
        !record.adding_replicas.is_empty(),
        "the refusal must have left the reassignment running: {record:?}",
    );

    // ── 3. the gate on, with an approved proposal ──────────────────────────
    // The krabka-private clients run on the host and dial the same
    // `host.docker.internal` name the JVM tools bootstrap against, which CI
    // maps to loopback in `/etc/hosts`, so both halves address one broker.
    approve_a_cancel(side.bootstrap()).await;
    let approved = reassign(&side, &props, &["--cancel"], &plan);
    check!(
        approved.succeeded(),
        "an approved cancel must succeed:\n{}",
        approved.text(),
    );
    check!(
        parse_cancelled(&approved.stdout) == vec![TopicPartition::new(TOPIC, 0)],
        "the approved cancel must name the partition it cancelled:\n{}",
        approved.stdout,
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
