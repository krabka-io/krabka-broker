//! Independent operator approvals change what the same stock unclean-election request may do.
use {
    crate::{
        control_plane::{approved_unclean_election, create_topics},
        host_broker::{JvmBroker, gate_on, sasl_listener, sasl_props, start_jvm_broker},
        jvm_acceptance::{ClientPropsFile, break_glass::Approvals, write_client_props},
        jvm_tool::{ToolRun, jvm_unclean_election},
        support,
        vocabulary::{APPROVER_ONE, APPROVER_TWO, ELECT_TOPIC, PROPOSER},
    },
    assert2::check,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct ProtectedElection {
    broker: Option<JvmBroker>,
    properties: Option<ClientPropsFile>,
    result: Option<ToolRun>,
    approvals: Option<[Approvals; 2]>,
}
#[fixture]
fn election() -> ProtectedElection {
    // Start this scenario with fresh inputs and observations.

    ProtectedElection {
        broker: None,
        properties: None,
        result: None,
        approvals: None,
    }
}
#[given(
    "a gated SASL broker hosts topic kfc9-elect for proposer alice and distinct approvers bob and carol"
)]
async fn operators(election: &mut ProtectedElection) {
    let users = [PROPOSER, APPROVER_ONE, APPROVER_TWO];
    let broker = start_jvm_broker(|config| {
        sasl_listener(config, &users);
        gate_on(config);
    })
    .await;
    let properties = write_client_props(&sasl_props(PROPOSER.0, PROPOSER.1));
    create_topics(
        &broker.host,
        Some(support::sasl_plain_security(PROPOSER.0, PROPOSER.1)),
        &[ELECT_TOPIC],
    )
    .await;
    election.broker = Some(broker);
    election.properties = Some(properties);
}
#[when("alice invokes the stock unclean election command for kfc9-elect partition 0")]
fn request(election: &mut ProtectedElection) {
    election.result = Some(jvm_unclean_election(
        &election
            .broker
            .as_ref()
            .expect("gated SASL broker started")
            .container,
        election
            .properties
            .as_ref()
            .expect("alice client properties"),
    ));
}
#[then("the unapproved election exits nonzero and prints these policy observations")]
fn refused(election: &ProtectedElection, datatable: Vec<Vec<String>>) {
    let result = election.result.as_ref().expect("stock election completed");
    check!(
        !result.succeeded(),
        "unapproved unclean election must exit nonzero"
    );
    for row in datatable.into_iter().skip(1) {
        let text = row.into_iter().next().expect("policy observation");
        check!(result.says(&text), "election missing{text:?}: {result:?}");
    }
}
#[when(
    "alice proposes that election and bob then carol approve it through their own authenticated sessions"
)]
async fn approve(election: &mut ProtectedElection) {
    election.approvals = Some(
        approved_unclean_election(
            &election
                .broker
                .as_ref()
                .expect("gated SASL broker started")
                .host,
            ELECT_TOPIC,
        )
        .await,
    );
}
#[then("one distinct approver is insufficient and the second satisfies the required count")]
fn approval_count(election: &ProtectedElection) {
    let [first, second] = election
        .approvals
        .as_ref()
        .expect("two approval responses received");
    check!(
        first.held == 1,
        "first approval must represent one distinct principal: {first:?}"
    );
    check!(
        first.held < first.required,
        "one approver must not suffice: {first:?}"
    );
    check!(
        second.held == second.required,
        "two principals must satisfy rule: {second:?}"
    );
}
#[then("the approved election exits zero and has these required and forbidden observations")]
fn allowed(election: &ProtectedElection, datatable: Vec<Vec<String>>) {
    let result = election
        .result
        .as_ref()
        .expect("stock approved election completed");
    check!(
        result.succeeded(),
        "approved election must exit zero: {result:?}"
    );
    for row in datatable.into_iter().skip(1) {
        let [text, presence]: [String; 2] =
            row.try_into().expect("observation and expected presence");
        let presence: bool = presence.parse().expect("boolean observation expectation");
        check!(
            result.says(&text) == presence,
            "election observation {text:?} expected presence{presence}: {result:?}"
        );
    }
}
#[ignore = "requires docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_break_glass_tools.feature",
    name = "Two distinct approvals permit the unchanged stock unclean election request"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_leader_election_needs_an_approved_proposal_for_an_unclean_election(
    election: ProtectedElection,
) {
    drop(election);
}
