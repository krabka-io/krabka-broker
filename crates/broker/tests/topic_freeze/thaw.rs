//! Lifting a freeze, which takes two people and a signature.
//!
//! The reverse direction is its own case because a freeze that could not be
//! lifted would be a broken cluster rather than a safe one, and no refusal case
//! can tell a working thaw from a registry entry that nothing removes. It runs
//! over SASL because the two-person rule needs three distinct principals, and
//! every connection on a plaintext listener is the same anonymous one.

use {
    crate::{
        control_plane::{cluster_id, freeze_scope, set_freeze, wait_for_registry_len},
        signing::{SignedFreeze, signed_request},
        support::{self, OperatorKey, OperatorKeysSetup},
        wire::{CONTROL, ProduceOutcome, accepted, create_topic, now_ms, produce_outcome, refused},
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, codes},
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::{
            break_glass::{ApproveBreakGlassRequest, ProposeBreakGlassRequest},
            freeze::{PATTERN_TYPE_LITERAL, SetTopicFreezeResponse},
        },
        primitives::uuid::Uuid as WireUuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

/// The `ThawTopicFreeze` break-glass action, on the wire.
const ACTION_THAW: i8 = 1;

/// Open a break-glass proposal to thaw `target`, and return its id.
async fn propose_thaw(client: &Client, target: &str) -> uuid::Uuid {
    let response = client
        .send(ProposeBreakGlassRequest {
            action: ACTION_THAW,
            target: target.to_owned(),
            reason: "the cutover finished".to_owned(),
            // Zero asks for `break_glass.proposal_ttl`.
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "ProposeBreakGlass: {response:?}"
    );
    uuid::Uuid::from_bytes(response.proposal_id.0)
}

/// Add one approval, and return how many the proposal now holds.
async fn approve(client: &Client, proposal_id: uuid::Uuid) -> i32 {
    let response = client
        .send(ApproveBreakGlassRequest {
            proposal_id: WireUuid(*proposal_id.as_bytes()),
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "ApproveBreakGlass: {response:?}"
    );
    response.approvals_held
}

#[derive(Default)]
struct ThawScenario {
    keys: Option<TempDir>,
    logs: Option<TempDir>,
    key: Option<OperatorKey>,
    broker: Option<BrokerHandle>,
    alice: Option<Client>,
    bootstrap: String,
    frozen: WireUuid,
    control: WireUuid,
    proposal: Option<uuid::Uuid>,
    approvals: Vec<i32>,
    thaw: Option<SetTopicFreezeResponse>,
    frozen_write: Option<ProduceOutcome>,
    control_write: Option<ProduceOutcome>,
}

impl ThawScenario {
    fn alice(&self) -> &Client { self.alice.as_ref().expect("Alice is connected") }

    fn broker(&self) -> &BrokerHandle {
        self.broker
            .as_ref()
            .expect("the signed-thaw broker is running")
    }
}

#[fixture]
fn thawing() -> ThawScenario {
    // Each scenario owns fresh services and observations.
    ThawScenario {
        keys: None,
        ..Default::default()
    }
}

#[given("Alice froze orders after one record and Bob and Carol are independent approvers")]
async fn approved_thaw_fixture(thawing: &mut ThawScenario) {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    let key = support::mint_operator_key(keys.path(), "alice-yubi", "User:alice");
    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl()
        .dir(logs.path())
        .trust(OperatorKeysSetup {
            keys: &[&key],
            approvers: &["User:alice", "User:bob", "User:carol"],
        })
        .users(&[("alice", "pw"), ("bob", "pw"), ("carol", "pw")])
        .call()
        .await;
    let alice = support::sasl_client(&bootstrap, "alice", "pw").await;
    let frozen = create_topic(&broker, &alice, "orders").await;
    let control = create_topic(&broker, &alice, CONTROL).await;
    check!(produce_outcome(&broker, &alice, "orders", frozen).await == accepted(1));
    freeze_scope(&alice, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&broker, &alice, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );
    thawing.keys = Some(keys);
    thawing.logs = Some(logs);
    thawing.key = Some(key);
    thawing.broker = Some(broker);
    thawing.alice = Some(alice);
    thawing.bootstrap = bootstrap;
    thawing.frozen = frozen;
    thawing.control = control;
}

#[when("Alice proposes thawing the literal orders scope")]
async fn alice_proposes_thaw(thawing: &mut ThawScenario) {
    thawing.proposal = Some(propose_thaw(thawing.alice(), "literal:orders").await);
}

#[when("approver {principal} approves Alice's thaw proposal")]
async fn independent_approval(thawing: &mut ThawScenario, principal: String) {
    let approver = support::sasl_client(&thawing.bootstrap, &principal, "pw").await;
    let held = approve(
        &approver,
        thawing.proposal.expect("Alice proposed the thaw"),
    )
    .await;
    thawing.approvals.push(held);
}

#[then("the thaw proposal holds {count} independent approvals")]
fn counted_approvals(thawing: &ThawScenario, count: i32) {
    check!(thawing.approvals.last() == Some(&count));
}

#[when("Alice submits a signed thaw naming the approved proposal")]
async fn alice_signs_thaw(thawing: &mut ThawScenario) {
    let cluster = cluster_id(thawing.alice()).await;
    let request = signed_request(&SignedFreeze {
        key: thawing.key.as_ref().expect("Alice's key was installed"),
        cluster_id: &cluster,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: "orders",
        frozen: false,
        reason: "the cutover finished",
        set_at_ms: now_ms(),
        proposal_id: thawing.proposal.expect("Alice proposed the thaw"),
    });
    thawing.thaw = Some(set_freeze(thawing.alice(), request).await);
}

#[then("the approved thaw succeeds and the freeze registry becomes empty")]
async fn approved_thaw_succeeds(thawing: &ThawScenario) {
    let response = thawing
        .thaw
        .as_ref()
        .expect("the signed thaw was submitted");
    check!(response.error_code == codes::NONE, "thaw: {response:?}");
    wait_for_registry_len(thawing.alice(), 0).await;
}

#[when("clients write to thawed orders and its unfrozen control")]
async fn write_after_thaw(thawing: &mut ThawScenario) {
    thawing.frozen_write =
        Some(produce_outcome(thawing.broker(), thawing.alice(), "orders", thawing.frozen).await);
    thawing.control_write =
        Some(produce_outcome(thawing.broker(), thawing.alice(), CONTROL, thawing.control).await);
}

#[then("orders accepts its second record and the control accepts its first")]
fn thaw_restores_writes(thawing: &ThawScenario) {
    check!(thawing.frozen_write.as_ref().expect("orders was written") == &accepted(2));
    check!(thawing.control_write.as_ref().expect("control was written") == &accepted(1));
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/thaw.feature",
    name = "a thaw restores writes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thaw_restores_writes(thawing: ThawScenario) {
    thawing
        .broker
        .expect("the signed-thaw broker is running")
        .shutdown()
        .await;
    drop(thawing.logs);
    drop(thawing.keys);
}
