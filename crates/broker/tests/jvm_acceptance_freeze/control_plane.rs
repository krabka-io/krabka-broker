//! The setup a case performs through krabka's own APIs before any JVM tool
//! runs: the topics, the freezes, and the break-glass proposal two operators
//! approve.
//!
//! Every call here travels a krabka-private API that a JVM tool cannot reach,
//! so it is fixture rather than finding: a setup step that fails stops the case
//! with `assert!`. The approval count is the one exception, because the
//! two-person rule is what the case is about, and it is checked the way the
//! suite checks everything else.

use assert2::{assert, check};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_core::{Client, security::ClientSecurity};
use krabka_protocol::krabka::{
    break_glass::{ApproveBreakGlassRequest, ProposeBreakGlassRequest},
    freeze::SetTopicFreezeRequest,
};

use crate::{
    support,
    vocabulary::{APPROVER_ONE, APPROVER_TWO, PROPOSER, WIRE_UNCLEAN_ELECT_LEADERS},
};

/// A plaintext host-side client for the krabka-private APIs.
pub(super) async fn plain_client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("kfc9-jvm-acceptance")
        .build()
        .await
        .expect("client build")
}

/// Create every topic a case needs, and fail the case when one does not open.
pub(super) async fn create_topics(
    bootstrap: &str,
    security: Option<ClientSecurity>,
    names: &[&str],
) {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_owned()], security)
        .await
        .expect("admin connect");
    let specs: Vec<CreateTopicSpec> = names
        .iter()
        .map(|name| CreateTopicSpec {
            name: (*name).to_owned(),
            partitions: 1,
            replicas: 1,
            configs: std::collections::BTreeMap::default(),
        })
        .collect();
    let outcomes = admin
        .create_topics(&specs, krabka_units::secs(30))
        .await
        .expect("create topics");
    for outcome in outcomes {
        let name = outcome.name;
        let error = outcome.error;
        assert!(error.is_none(), "create topic {name}: {error:?}");
    }
}

/// Freeze one scope through the krabka-private `SetTopicFreeze` (api key
/// 1015).
///
/// The request is unsigned, which the broker accepts for a freeze while
/// `freeze.require_signature` is off. A freeze is the safe direction, and
/// KFC-9 keeps it reachable in one command on a cluster with no key material.
pub(super) async fn freeze(client: &Client, scope: &str, pattern_type: i8, reason: &str) {
    let response = client
        .send(SetTopicFreezeRequest {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: reason.to_owned(),
            ..SetTopicFreezeRequest::default()
        })
        .await
        .expect("SetTopicFreeze");
    let code = response.error_code;
    let message = response.error_message;
    assert!(code == 0, "freeze {scope}: code={code} message={message:?}");
}

/// How far along a proposal is after one approval.
#[derive(Debug)]
struct Approvals {
    /// Distinct principals that have approved it.
    held: i32,
    /// Distinct principals it needs. The broker refuses a configured value
    /// below two.
    required: i32,
}

/// Open a break-glass proposal as `PROPOSER`, and have both approvers sign off.
///
/// The target is the bare topic name rather than `<topic>-<partition>`. KFC-9
/// lets a proposal on a topic cover every partition of it for the actions that
/// name a partition, and an unclean election is one of those, so this also
/// checks that widening on the way through.
pub(super) async fn approved_unclean_election(bootstrap: &str, target: &str) {
    let proposer = support::sasl_client(bootstrap, PROPOSER.0, PROPOSER.1).await;
    let opened = proposer
        .send(ProposeBreakGlassRequest {
            action: WIRE_UNCLEAN_ELECT_LEADERS,
            target: target.to_owned(),
            reason: "the whole ISR is gone and the site has to come back".to_owned(),
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
        first.held == 1,
        "one approval is one distinct principal, not {first:?}"
    );
    check!(
        first.held < first.required,
        "one person must not be enough: {first:?}"
    );

    let second = approve(bootstrap, APPROVER_TWO, opened.proposal_id).await;
    check!(
        second.held == second.required,
        "two distinct principals must satisfy the rule: {second:?}"
    );
}

/// Add one approval to a proposal as `operator`.
async fn approve(
    bootstrap: &str,
    operator: (&str, &str),
    proposal_id: krabka_protocol::primitives::uuid::Uuid,
) -> Approvals {
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
    Approvals {
        held: response.approvals_held,
        required: response.approvals_required,
    }
}
