//! Every gated Kafka transition must reject absent authority and honor valid approvals.
use {
    crate::{
        cluster::{Cluster, boot},
        principals::ALICE,
        proposals::{
            ACTION_CANCEL_REASSIGNMENT, ACTION_DELETE_RECORDS, ACTION_DELETE_TOPIC,
            ACTION_UNCLEAN_ELECT_LEADERS, ACTION_UNREGISTER_BROKER, approved_proposal, stored,
        },
        topics::create_topic,
        transitions::{Transition, refusals},
    },
    assert2::check,
    krabka_client_core::Client,
    krabka_metadata::BreakGlassAction,
    krabka_protocol::primitives::uuid::Uuid,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct GatedTransition {
    cluster: Option<Cluster>,
    alice: Option<Client>,
    topic: String,
    transition: Option<Transition>,
    metric: Option<BreakGlassAction>,
    action: i8,
    refusals_before: Option<u64>,
    answered: Option<i16>,
    proposal: Option<Uuid>,
}

#[fixture]
fn gate() -> GatedTransition {
    // Start this scenario with fresh inputs and observations.

    GatedTransition {
        cluster: None,
        alice: None,
        topic: String::new(),
        transition: None,
        metric: None,
        action: 0,
        refusals_before: None,
        answered: None,
        proposal: None,
    }
}

#[given("a two-person-rule broker with topic {topic} and Kafka transition action {action}")]
async fn gated_broker(gate: &mut GatedTransition, topic: String, action: i8) {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    create_topic(&alice, &topic, 1).await;
    let (transition, metric) = match action {
        ACTION_UNCLEAN_ELECT_LEADERS => (
            Transition::UncleanElection,
            BreakGlassAction::UncleanElectLeaders,
        ),
        ACTION_UNREGISTER_BROKER => (Transition::Unregister, BreakGlassAction::UnregisterBroker),
        ACTION_CANCEL_REASSIGNMENT => (
            Transition::CancelReassignment,
            BreakGlassAction::CancelReassignment,
        ),
        ACTION_DELETE_TOPIC => (Transition::DeleteTopic, BreakGlassAction::DeleteTopic),
        ACTION_DELETE_RECORDS => (Transition::TrimRecords, BreakGlassAction::DeleteRecords),
        _ => panic!("unknown wire action {action}"),
    };
    gate.cluster = Some(cluster);
    gate.alice = Some(alice);
    gate.topic = topic;
    gate.transition = Some(transition);
    gate.metric = Some(metric);
    gate.action = action;
}

#[when("alice invokes the Kafka transition")]
async fn invoke(gate: &mut GatedTransition) {
    gate.refusals_before = Some(refusals(
        &gate.cluster.as_ref().unwrap().broker,
        gate.metric.unwrap(),
    ));
    gate.answered = Some(
        gate.transition
            .unwrap()
            .run(gate.alice.as_ref().unwrap(), &gate.topic)
            .await,
    );
}

#[then("the transition returns error {error} and its refusal counter increases by {increase}")]
fn refused(gate: &GatedTransition, error: i16, increase: u64) {
    check!(gate.answered == Some(error));
    check!(
        refusals(&gate.cluster.as_ref().unwrap().broker, gate.metric.unwrap())
            == gate.refusals_before.unwrap() + increase
    );
}

#[when("alice proposes the transition target and bob and carol approve")]
async fn authorize(gate: &mut GatedTransition) {
    gate.proposal = Some(
        approved_proposal(
            gate.cluster.as_ref().unwrap(),
            gate.action,
            &gate.transition.unwrap().target(&gate.topic),
        )
        .await,
    );
}

#[then("the transition returns error {error} and the proposal consumed state is {consumed}")]
async fn authorized_result(gate: &GatedTransition, error: i16, consumed: bool) {
    check!(gate.answered == Some(error));
    let proposal = stored(gate.alice.as_ref().unwrap(), gate.proposal.unwrap()).await;
    check!((proposal.consumed_at_ms != 0) == consumed);
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/gated_transitions.feature",
    name = "every gated transition refuses without a proposal and runs with one"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_gated_transition_refuses_without_a_proposal_and_runs_with_one(
    gate: GatedTransition,
    topic: String,
    action: i8,
    approved: i16,
    consumed: bool,
) {
    let _ = (topic, action, approved, consumed);
    drop(gate.alice);
    gate.cluster.unwrap().broker.shutdown().await;
}
