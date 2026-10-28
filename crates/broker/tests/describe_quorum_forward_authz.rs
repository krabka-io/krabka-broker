//! `DescribeQuorum` forwarding carries the CALLER's own principal to the
//! active controller, not this node's inter-broker identity (review of
//! #1034).
//!
//! `krabka_raft::ControllerHandle::forward_raw` dials the leader's
//! controller listener as this node's own inter-broker identity -- on this
//! test's plaintext controller listener, `ANONYMOUS`. Before the fix,
//! `describe_quorum::handle` sent the bare `DescribeQuorumRequest` bytes
//! over that dial, so the leader's controller listener authorized `Describe`
//! against `ANONYMOUS`, not the caller who already passed
//! `cluster_describe_denied` on the follower -- a caller holding `Describe`
//! would be fenced out the moment it landed on a follower instead of the
//! leader. The fix wraps the forward in a KIP-590 `Envelope` carrying the
//! caller's own principal, so the leader authorizes and answers under the
//! identity that actually asked.
#![cfg(test)]

mod support;

use {
    crate::support::{
        listeners,
        quorum::{follower_config, two_node_leader},
        sasl_client, shutdown_cluster, start_n_node_with,
    },
    assert2::check,
    krabka_broker::{
        BrokerConfig,
        authorizer::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer},
    },
    krabka_metadata::AclOperation,
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::sync::Arc,
};

const ALICE: &str = "alice";
const ALICE_PASSWORD: &str = "alice-secret";

/// Grants `ClusterAction` to EVERY principal -- raft consensus and the
/// KIP-590 envelope's own outer gate both need it from the inter-broker
/// identity (`ANONYMOUS` on this test's plaintext controller listener) just
/// to run at all -- and `Describe` to `alice` alone.
///
/// No ACL log entry is ever submitted: the rule is a static, config-time
/// decision, so it is in force from the very first bootstrap RPC. An
/// ACL-log-backed authorizer cannot grant anything before a leader is
/// elected to commit the grant, and granting the inter-broker identity a
/// blanket bypass (a super user, as other controller-listener authorization
/// tests do) would make it hold `Describe` too, which defeats this test:
/// the whole point is that the inter-broker identity must NOT be able to
/// answer `Describe` on the caller's behalf.
#[derive(Debug)]
struct ClusterActionForEveryoneDescribeForAlice;

impl Authorizer for ClusterActionForEveryoneDescribeForAlice {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        match req.operation {
            AclOperation::ClusterAction => AuthorizationResult::Allow,
            AclOperation::Describe if req.principal.name == ALICE => AuthorizationResult::Allow,
            _ => AuthorizationResult::Deny,
        }
    }
}

use crate::support::quorum::metadata_quorum_request as describe_quorum_request;

#[derive(Default)]
struct AuthorizedQuorumForwarding {
    cluster: Vec<(krabka_broker::BrokerHandle, BrokerConfig, tempfile::TempDir)>,
    leader: i32,
    response: Option<DescribeQuorumResponse>,
}
#[rstest::fixture]
fn authorized_quorum_forwarding() -> AuthorizedQuorumForwarding {
    let cluster = Vec::new();
    AuthorizedQuorumForwarding {
        cluster,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a two-voter SASL PLAIN cluster grants ClusterAction to everyone and Describe only to alice"
)]
async fn caller_authorized_quorum(authorized_quorum_forwarding: &mut AuthorizedQuorumForwarding) {
    let cluster = start_n_node_with(2, |_, config: &mut BrokerConfig| {
        config.listeners = vec![listeners::listener(
            "SASL_PLAINTEXT",
            config.listen_addr,
            ListenerProtocol::SaslPlaintext,
        )];
        config.inter_broker_listener_name = "SASL_PLAINTEXT".to_owned();
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        config
            .plain_credentials
            .insert(ALICE.to_owned(), ALICE_PASSWORD.to_owned());
        config.authorizer = Arc::new(ClusterActionForEveryoneDescribeForAlice);
    })
    .await
    .expect("two-voter cluster");
    let client = sasl_client(&cluster[0].1.listen_addr.to_string(), ALICE, ALICE_PASSWORD).await;
    let initial = client
        .send(describe_quorum_request())
        .await
        .expect("initial quorum description");
    check!(initial.error_code == 0, "initial top-level error code");
    authorized_quorum_forwarding.leader = two_node_leader(&initial, (1, 2));
    authorized_quorum_forwarding.cluster = cluster;
}
#[rstest_bdd_macros::when("alice sends DescribeQuorum to the elected leader's follower")]
async fn describe_quorum_as_original_caller(
    authorized_quorum_forwarding: &mut AuthorizedQuorumForwarding,
) {
    let follower = follower_config(
        &authorized_quorum_forwarding.cluster,
        authorized_quorum_forwarding.leader,
    );
    let client = sasl_client(&follower.listen_addr.to_string(), ALICE, ALICE_PASSWORD).await;
    authorized_quorum_forwarding.response = Some(
        client
            .send(describe_quorum_request())
            .await
            .expect("forwarded quorum description"),
    );
}
#[rstest_bdd_macros::then(
    "the forwarded quorum response succeeds at both levels and identifies the elected leader"
)]
async fn authorized_forwarded_quorum(
    authorized_quorum_forwarding: &mut AuthorizedQuorumForwarding,
) {
    let response = authorized_quorum_forwarding
        .response
        .as_ref()
        .expect("forwarded response");
    check!(response.error_code == 0, "top-level error code");
    let partition = &response.topics[0].partitions[0];
    check!(
        partition.error_code == 0,
        "forwarding must authorize alice rather than the inter-broker principal: {partition:?}"
    );
    check!(partition.leader_id == authorized_quorum_forwarding.leader);
    shutdown_cluster(mem::take(&mut authorized_quorum_forwarding.cluster)).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/caller_quorum_forward.feature",
    name = "Forwarding preserves the Describe-authorized caller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_describe_authorized_caller_succeeds_through_a_forwarding_follower(
    authorized_quorum_forwarding: AuthorizedQuorumForwarding,
) {
}

use {krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse, std::mem};
