//! Principal-bound break-glass proposal, approval, and consumption behavior.
use {
    crate::{
        cluster::{Cluster, boot, boot_with_signed_actions},
        principals::principal,
        proposals::{
            approval_signing_bytes, approve, approve_signed, approve_with, now_ms, propose, stored,
        },
        support,
        topics::{create_topic, delete_topic, topic_exists},
    },
    assert2::{assert, check},
    krabka_client_core::Client,
    krabka_protocol::krabka::break_glass::{
        ApproveBreakGlassResponse, BreakGlassApproval, DescribedBreakGlassProposal,
        ProposeBreakGlassResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{collections::BTreeMap, time::Duration},
    tokio::time,
};

#[derive(Default)]
pub struct BreakGlass {
    cluster: Option<Cluster>,
    clients: BTreeMap<String, Client>,
    topic: String,
    proposed: Option<ProposeBreakGlassResponse>,
    approval: Option<ApproveBreakGlassResponse>,
    described: Option<DescribedBreakGlassProposal>,
    before_transition: Option<DescribedBreakGlassProposal>,
    deletion: Option<i16>,
    audit: Vec<serde_json::Value>,
}

#[fixture]
pub(super) fn glass() -> BreakGlass {
    // Start this scenario with fresh inputs and observations.

    BreakGlass {
        cluster: None,
        clients: BTreeMap::new(),
        topic: String::new(),
        proposed: None,
        approval: None,
        described: None,
        before_transition: None,
        deletion: None,
        audit: Vec::new(),
    }
}

impl BreakGlass {
    pub(super) async fn shutdown(mut self) {
        self.clients.clear();
        self.cluster.take().unwrap().broker.shutdown().await;
    }
}

#[given("a two-approval broker with users {users}, topic {topic}, and signed actions {actions}")]
async fn broker(glass: &mut BreakGlass, users: String, topic: String, actions: String) {
    let cluster = if actions == "none" {
        boot().await
    } else {
        boot_with_signed_actions(&actions.split(',').collect::<Vec<_>>()).await
    };
    for user in users.split(',') {
        glass
            .clients
            .insert(user.to_owned(), cluster.client(user).await);
    }
    create_topic(&glass.clients["alice"], &topic, 1).await;
    glass.topic = topic;
    glass.cluster = Some(cluster);
}

#[when("{user} proposes action {action} on {target} with lifetime {ttl} ms")]
async fn proposal(glass: &mut BreakGlass, user: String, action: i8, target: String, ttl: i64) {
    glass.proposed = Some(propose(&glass.clients[&user], action, &target, ttl).await);
}

#[then("the proposal response has error {error}")]
fn proposed(glass: &BreakGlass, error: i16) {
    assert!(glass.proposed.as_ref().unwrap().error_code == error);
}

#[when("{user} approves the current proposal without a signature")]
async fn unsigned(glass: &mut BreakGlass, user: String) {
    glass.approval = Some(
        approve(
            &glass.clients[&user],
            glass.proposed.as_ref().unwrap().proposal_id,
        )
        .await,
    );
}

#[when("{user} approves with their operator signature")]
async fn signed(glass: &mut BreakGlass, user: String) {
    glass.approval = Some(
        approve_signed(
            &glass.clients[&user],
            glass.proposed.as_ref().unwrap().proposal_id,
            glass.cluster.as_ref().unwrap().key(&user),
        )
        .await,
    );
}

#[when("{user} approves with their signature first byte XOR {mask}")]
async fn tampered(glass: &mut BreakGlass, user: String, mask: u8) {
    let id = glass.proposed.as_ref().unwrap().proposal_id;
    let key = glass.cluster.as_ref().unwrap().key(&user);
    let mut signature = key
        .pair()
        .sign(&approval_signing_bytes(
            &stored(&glass.clients[&user], id).await,
        ))
        .as_ref()
        .to_vec();
    signature[0] ^= mask;
    glass.approval = Some(approve_with(&glass.clients[&user], id, &key.key_id, signature).await);
}

#[then("the approval is exactly success with {held} approvals held and {required} required")]
fn full_approval(glass: &BreakGlass, held: i32, required: i32) {
    check!(
        glass.approval.as_ref().unwrap()
            == &ApproveBreakGlassResponse {
                approvals_held: held,
                approvals_required: required,
                ..Default::default()
            }
    );
}

#[then("the approval response has error {error}")]
fn approval_error(glass: &BreakGlass, error: i16) {
    check!(glass.approval.as_ref().unwrap().error_code == error);
}

#[then("the approval response holds {held} approvals")]
fn held_count(glass: &BreakGlass, held: i32) {
    check!(glass.approval.as_ref().unwrap().approvals_held == held);
}

#[then("the approval retains {held} approvals and its error names principal {user}")]
fn refused_principal(glass: &BreakGlass, held: i32, user: String) {
    let response = glass.approval.as_ref().unwrap();
    check!(response.approvals_held == held);
    check!(
        response
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&principal(&user)))
    );
}

#[then("the approval error message contains {text}")]
fn error_text(glass: &BreakGlass, text: String) {
    check!(
        glass
            .approval
            .as_ref()
            .unwrap()
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&text))
    );
}

#[when("alice describes the current proposal")]
async fn describe(glass: &mut BreakGlass) {
    glass.described = Some(
        stored(
            &glass.clients["alice"],
            glass.proposed.as_ref().unwrap().proposal_id,
        )
        .await,
    );
}

#[then("the stored proposal holds {count} approvals and is unconsumed")]
fn count(glass: &BreakGlass, count: usize) {
    let response = glass.described.as_ref().unwrap();
    assert!(response.approvals.len() == count);
    check!(response.consumed_at_ms == 0);
}

#[then("the proposer is {proposer} and unsigned approvals name {users} in order")]
fn unsigned_order(glass: &BreakGlass, proposer: String, users: String) {
    let ready = glass.described.as_ref().unwrap();
    check!(ready.proposer == principal(&proposer));
    let expected: Vec<BreakGlassApproval> = users
        .split(',')
        .zip(&ready.approvals)
        .map(|(user, actual)| BreakGlassApproval {
            principal: principal(user),
            approved_at_ms: actual.approved_at_ms,
            ..Default::default()
        })
        .collect();
    check!(ready.approvals == expected);
}

#[then("stored approvals carry nonempty signatures and the operator key ids for {users} in order")]
fn signature_evidence(glass: &BreakGlass, users: String) {
    let approvals = &glass.described.as_ref().unwrap().approvals;
    let expected: Vec<String> = users
        .split(',')
        .map(|user| glass.cluster.as_ref().unwrap().key(user).key_id.clone())
        .collect();
    check!(
        approvals
            .iter()
            .map(|approval| approval.key_id.clone())
            .collect::<Vec<_>>()
            == expected
    );
    check!(
        approvals
            .iter()
            .all(|approval| !approval.signature.is_empty())
    );
}

#[when("alice deletes the topic")]
async fn delete(glass: &mut BreakGlass) {
    glass.before_transition = glass.described.clone();
    glass.deletion = Some(delete_topic(&glass.clients["alice"], &glass.topic).await);
}

#[then("the deletion has error {error} and topic existence is {exists}")]
async fn topic_result(glass: &BreakGlass, error: i16, exists: bool) {
    check!(glass.deletion == Some(error));
    check!(topic_exists(&glass.clients["alice"], &glass.topic).await == exists);
}

#[then("only the stored proposal consumption timestamp changes to a nonzero value")]
fn spent(glass: &BreakGlass) {
    let spent = glass.described.as_ref().unwrap();
    check!(spent.consumed_at_ms != 0);
    check!(
        *spent
            == DescribedBreakGlassProposal {
                consumed_at_ms: spent.consumed_at_ms,
                ..glass.before_transition.as_ref().unwrap().clone()
            }
    );
}

#[when("the current proposal expiry passes with safety margin {margin} ms")]
async fn expire(glass: &BreakGlass, margin: u64) {
    let remaining = glass
        .proposed
        .as_ref()
        .unwrap()
        .expires_at_ms
        .saturating_sub(now_ms())
        .max(0);
    time::sleep(Duration::from_millis(
        u64::try_from(remaining).unwrap() + margin,
    ))
    .await;
}

#[when("alice reads audit records through operation {operation}")]
async fn audit(glass: &mut BreakGlass, operation: String) {
    glass.audit = support::wait_for_audit_record(&glass.clients["alice"], &operation, |value| {
        value["class_uid"] == 6003 && value["api"]["operation"] == operation
    })
    .await;
}

#[then(
    "the {operation} privileged action is attributed to {user} and joins a successful login by principal and peer"
)]
fn audit_login(glass: &BreakGlass, operation: String, user: String) {
    let approval = glass
        .audit
        .iter()
        .find(|value| value["class_uid"] == 6003 && value["api"]["operation"] == operation)
        .expect("approval audit record");
    check!(approval["actor"]["user"]["name"] == principal(&user));
    let login = glass.audit.iter().find(|value| {
        value["class_uid"] == 3002
            && value["status_id"] == 1
            && value["actor"]["user"]["name"] == approval["actor"]["user"]["name"]
            && value["src_endpoint"] == approval["src_endpoint"]
    });
    assert!(
        login.is_some(),
        "no Authentication row joins to {approval:?}; rows: {:?}",
        glass.audit
    );
}
