//! KFC-9 break-glass, driven over the wire by more than one principal.
//!
//! The broker half of the two-person rule is unit-tested, and the tool half is
//! covered by `crates/guard-cli/tests/guard_cli.rs`. Neither of those can prove
//! the thing the feature promises. A `PLAINTEXT` listener authenticates every
//! connection as one principal, `User:ANONYMOUS`, so a suite that speaks over
//! one can show that a proposer may not approve their own proposal and can
//! never show two distinct approvers completing a proposal. The guard-cli
//! module doc says so, and defers the completion cases here.
//!
//! This suite boots the broker behind a `SASL_PLAINTEXT` listener with four
//! credentials, so `User:alice` proposes, `User:bob` and `User:carol` approve,
//! and `User:mallory` stands outside the approver set. Every case below rests
//! on that: the refusals prove the rule bites, and the completions prove the
//! rule can be satisfied at all.
//!
//! # What each tier covers
//!
//! * The loop — a proposal, two approvals, the transition, the spent proposal.
//! * The three distinct-principal refusals — a second approval by one person, a
//!   proposer approving themselves, and a principal outside the set.
//! * Expiry, on the approve path and on the consume path.
//! * A signature, demanded by `break_glass.signed_actions`.
//! * Atomicity: the consume and the transition in one raft append, read back
//!   out of the committed metadata log.
//! * Durability: an approved proposal across a controller failover.
//! * Every gated transition, refused with no proposal and run with one.
//! * The background unclean-recovery path, under all three settings.
//!
//! # What this suite does not cover, and why
//!
//! The thaw of a topic freeze is the sixth gated transition, and it is the one
//! that names its proposal on the request rather than out of band. It needs the
//! freeze signing layout as well as this one, so it belongs with the freeze
//! suite. The transaction admission path is gated by the freeze and not by
//! break-glass, so it belongs there too.

use std::time::Duration;

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{
    Broker, BrokerConfig, BrokerHandle, NodeId, codes,
    config::{BackgroundUncleanRecovery, ListenerSpec},
    metrics::{BreakGlassAction as ActionLabel, BreakGlassActionLabel},
    operator_keys::OperatorKeys,
};
use krabka_client_core::{Client, Connection, ConnectionOptions};
use krabka_metadata::{
    BreakGlassAction as GatedAction, BreakGlassApproval as StoredApproval,
    BreakGlassProposalRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord,
    UnregisterBrokerRecord,
};
use krabka_protocol::{
    krabka::break_glass::{
        ApproveBreakGlassRequest, ApproveBreakGlassResponse, BreakGlassApproval as WireApproval,
        DescribeBreakGlassRequest, DescribedBreakGlassProposal, ProposeBreakGlassRequest,
    },
    owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        unregister_broker_request::UnregisterBrokerRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

mod support;

// ── the principals ───────────────────────────────────────────────────────────

/// The PLAIN credentials the listener knows. Each authenticates as
/// `User:<name>`, which is the spelling `approvers` and `[[operator_keys]]`
/// use.
const USERS: &[(&str, &str)] = &[
    ("alice", "alice-secret"),
    ("bob", "bob-secret"),
    ("carol", "carol-secret"),
    ("mallory", "mallory-secret"),
];

/// The proposer in every case. She is an approver too, because the broker
/// refuses a proposal from a principal outside the set.
const ALICE: &str = "alice";
/// The first approver.
const BOB: &str = "bob";
/// The second approver. `required_approvals` is two and a proposer may not
/// approve, so a completed loop needs three people.
const CAROL: &str = "carol";
/// Authenticated, and outside `break_glass.approvers`.
const MALLORY: &str = "mallory";

/// The principals that may approve. `mallory` is deliberately absent.
const APPROVERS: &[&str] = &[ALICE, BOB, CAROL];

/// `principal` in the `KafkaPrincipal` string form every KFC-9 surface uses.
fn principal(user: &str) -> String {
    format!("User:{user}")
}

// ── the action wire values ───────────────────────────────────────────────────
//
// KFC-9 pins these as the wire values of the broker's action enum. They are
// written out here rather than derived from the broker, so a renumbering that
// kept the broker self-consistent still fails this suite.

/// `unclean_elect_leaders`.
const ACTION_UNCLEAN_ELECT_LEADERS: i8 = 2;
/// `unregister_broker`.
const ACTION_UNREGISTER_BROKER: i8 = 4;
/// `cancel_reassignment`.
const ACTION_CANCEL_REASSIGNMENT: i8 = 5;
/// `delete_topic`.
const ACTION_DELETE_TOPIC: i8 = 6;
/// `delete_records`.
const ACTION_DELETE_RECORDS: i8 = 7;

/// The `ttl_ms` value that asks the broker for its configured lifetime.
const TTL_CONFIGURED: i64 = 0;

/// The one broker id a single-node cluster registers.
const BROKER_ID: i32 = 1;

// ── the fixture ──────────────────────────────────────────────────────────────

/// A live broker behind SASL, the operator keys it trusts, and the directory
/// both live in.
///
/// The `TempDir` is held so the log directory and the public key files outlive
/// the broker, exactly as `guard_cli.rs` holds its own.
struct Cluster {
    broker: BrokerHandle,
    bootstrap: String,
    keys: Vec<support::OperatorKey>,
    _dir: TempDir,
}

impl Cluster {
    /// A client the broker authenticates as `User:<user>`.
    async fn client(&self, user: &str) -> Client {
        let password = USERS
            .iter()
            .find(|(name, _)| *name == user)
            .map(|(_, password)| *password)
            .expect("a configured credential");
        support::sasl_client(&self.bootstrap, user, password).await
    }

    /// The operator key bound to `user`.
    fn key(&self, user: &str) -> &support::OperatorKey {
        let want = principal(user);
        self.keys
            .iter()
            .find(|key| key.principal == want)
            .expect("a minted operator key")
    }
}

/// Mint one operator key per approver under `dir`.
fn mint_keys(dir: &std::path::Path) -> Vec<support::OperatorKey> {
    APPROVERS
        .iter()
        .map(|user| support::mint_operator_key(dir, &format!("{user}-yubi"), &principal(user)))
        .collect()
}

/// Boot the suite's single-node broker on the shared helper.
///
/// `break_glass` keeps its defaults: two required approvals, a thirty-minute
/// lifetime, and no signed action. Every case that needs one of those changed
/// says so at its own call site.
async fn boot() -> Cluster {
    let dir = TempDir::new().expect("tempdir");
    let keys = mint_keys(dir.path());
    let borrowed: Vec<&support::OperatorKey> = keys.iter().collect();
    let approvers: Vec<String> = APPROVERS.iter().copied().map(principal).collect();
    let approver_refs: Vec<&str> = approvers.iter().map(String::as_str).collect();
    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl(
        &dir.path().join("data"),
        &borrowed,
        &approver_refs,
        USERS,
    )
    .await;
    Cluster {
        broker,
        bootstrap,
        keys,
        _dir: dir,
    }
}

/// [`boot`], with `break_glass.signed_actions` naming `actions`.
///
/// The broker reads `signed_actions` when it starts, and
/// [`support::start_with_operator_keys_sasl`] takes no hook for it, so the one
/// case that needs a signed action rebuilds the same SASL broker here. Keeping
/// the field at its default in the shared helper is right: a suite that changed
/// it there would make every other case demand signatures it does not test.
async fn boot_with_signed_actions(actions: &[&str]) -> Cluster {
    let dir = TempDir::new().expect("tempdir");
    let keys = mint_keys(dir.path());
    let entries: Vec<_> = keys.iter().map(support::OperatorKey::entry).collect();

    let mut config = BrokerConfig::for_tests(dir.path().join("data"));
    config.operator_keys = OperatorKeys::load(&entries).expect("load the operator trust set");
    config.break_glass.approvers = APPROVERS.iter().copied().map(principal).collect();
    config.break_glass.signed_actions = actions.iter().map(|a| (*a).to_owned()).collect();
    config.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_owned(),
        bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
        advertised: "127.0.0.1:0".to_owned(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    "SASL_PLAINTEXT".clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, password) in USERS {
        config
            .plain_credentials
            .insert((*name).to_owned(), (*password).to_owned());
    }

    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    Cluster {
        broker,
        bootstrap,
        keys,
        _dir: dir,
    }
}

// ── the private wire ─────────────────────────────────────────────────────────

/// Open a proposal, and answer the whole response so a caller can assert on a
/// refusal as easily as on a success.
async fn propose(
    client: &Client,
    action: i8,
    target: &str,
    ttl_ms: i64,
) -> krabka_protocol::krabka::break_glass::ProposeBreakGlassResponse {
    client
        .send(ProposeBreakGlassRequest {
            action,
            target: target.to_owned(),
            reason: "an integration case".to_owned(),
            ttl_ms,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass")
}

/// Open a proposal that the broker must accept, and answer its id.
async fn open(client: &Client, action: i8, target: &str) -> WireUuid {
    let response = propose(client, action, target, TTL_CONFIGURED).await;
    assert!(
        response.error_code == codes::NONE,
        "propose {action} on {target}: {response:?}"
    );
    response.proposal_id
}

/// Read one stored proposal back.
async fn stored(client: &Client, id: WireUuid) -> DescribedBreakGlassProposal {
    let response = client
        .send(DescribeBreakGlassRequest {
            proposal_id: id,
            ..DescribeBreakGlassRequest::default()
        })
        .await
        .expect("DescribeBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "describe {id:?}: {response:?}"
    );
    response
        .proposals
        .into_iter()
        .find(|proposal| proposal.proposal_id == id)
        .expect("the proposal the cluster holds")
}

/// Add one unsigned approval.
async fn approve(client: &Client, id: WireUuid) -> ApproveBreakGlassResponse {
    client
        .send(ApproveBreakGlassRequest {
            proposal_id: id,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass")
}

/// Add one approval signed by `key`, over the proposal the broker holds.
///
/// The signature covers the stored record and not one the caller made up, so
/// this reads the proposal back first, exactly as `krabka-guard` does.
async fn approve_signed(
    client: &Client,
    id: WireUuid,
    key: &support::OperatorKey,
) -> ApproveBreakGlassResponse {
    let held = stored(client, id).await;
    let signature = key
        .pair()
        .sign(&approval_signing_bytes(&held))
        .as_ref()
        .to_vec();
    approve_with(client, id, &key.key_id, signature).await
}

/// Add one approval carrying `key_id` and `signature` verbatim.
async fn approve_with(
    client: &Client,
    id: WireUuid,
    key_id: &str,
    signature: Vec<u8>,
) -> ApproveBreakGlassResponse {
    client
        .send(ApproveBreakGlassRequest {
            proposal_id: id,
            key_id: key_id.to_owned(),
            signature,
            withdraw: false,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass")
}

/// The canonical bytes an approval signature covers.
///
/// This is the layout KFC-9 documents, built here by hand. The broker rebuilds
/// the same bytes from its own record, so a signature this file makes verifies
/// inside the broker only when the two agree. Deriving these bytes from the
/// broker's own encoder would make the check vacuous.
fn approval_signing_bytes(proposal: &DescribedBreakGlassProposal) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"krabka-break-glass-v1\0");
    out.extend_from_slice(&proposal.proposal_id.0);
    out.extend_from_slice(&proposal.action.to_be_bytes());
    push_len_prefixed(&mut out, proposal.target.as_bytes());
    push_len_prefixed(&mut out, proposal.proposer.as_bytes());
    out.extend_from_slice(&proposal.created_at_ms.to_be_bytes());
    out.extend_from_slice(&proposal.expires_at_ms.to_be_bytes());
    out
}

/// Append `bytes` behind its `u32` big-endian length.
fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("a compact field");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Alice proposes, Bob and Carol approve. The proposal is then usable.
async fn approved_proposal(cluster: &Cluster, action: i8, target: &str) -> WireUuid {
    let alice = cluster.client(ALICE).await;
    let bob = cluster.client(BOB).await;
    let carol = cluster.client(CAROL).await;
    let id = open(&alice, action, target).await;
    let first = approve(&bob, id).await;
    assert!(first.error_code == codes::NONE, "bob approves: {first:?}");
    let second = approve(&carol, id).await;
    assert!(
        second.error_code == codes::NONE,
        "carol approves: {second:?}"
    );
    id
}

// ── the Kafka wire ───────────────────────────────────────────────────────────

/// Create `name` with one partition and the given replication factor.
async fn create_topic(client: &Client, name: &str, replication_factor: i16) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_owned(),
                num_partitions: 1,
                replication_factor,
                ..CreatableTopic::default()
            }],
            timeout_ms: 10_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    let code = response.topics.first().map(|topic| topic.error_code);
    assert!(code == Some(codes::NONE), "create {name}: {response:?}");
}

/// Delete `name`, and answer the row's error code.
async fn delete_topic(client: &Client, name: &str) -> i16 {
    let response = client
        .send(DeleteTopicsRequest {
            // The encoder writes `topics` at version 6 and later and
            // `topic_names` below it, so filling both leaves the negotiated
            // version to pick.
            topics: vec![DeleteTopicState {
                name: Some(name.to_owned()),
                ..DeleteTopicState::default()
            }],
            topic_names: vec![name.to_owned()],
            timeout_ms: 10_000,
            ..DeleteTopicsRequest::default()
        })
        .await
        .expect("DeleteTopics");
    response
        .responses
        .first()
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// Whether `client`'s cluster still knows `name`.
async fn topic_exists(client: &Client, name: &str) -> bool {
    client
        .send(krabka_protocol::owned::metadata_request::MetadataRequest::default())
        .await
        .expect("Metadata")
        .topics
        .iter()
        .any(|topic| topic.name.as_deref() == Some(name) && topic.error_code == codes::NONE)
}

// ── the gated transitions ────────────────────────────────────────────────────

/// One privileged transition, in the shape a request makes it.
///
/// The enum exists so a table can name a transition without carrying a boxed
/// future per row. Each variant answers the error code the broker returned for
/// the one row that the request names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transition {
    /// `ElectLeaders` with the unclean election type. Preferred election is not
    /// gated.
    UncleanElection,
    /// `UnregisterBroker` on this cluster's one broker.
    Unregister,
    /// `AlterPartitionReassignments` with a null replica list, which is the
    /// cancel. A start is not gated, and a completion is not a cancel.
    CancelReassignment,
    /// `DeleteTopics`.
    DeleteTopic,
    /// `DeleteRecords` at offset zero.
    TrimRecords,
}

impl Transition {
    /// The break-glass target a proposal must name to authorize this
    /// transition on `topic`.
    fn target(self, topic: &str) -> String {
        match self {
            Transition::UncleanElection
            | Transition::CancelReassignment
            | Transition::TrimRecords => format!("{topic}-0"),
            Transition::Unregister => BROKER_ID.to_string(),
            Transition::DeleteTopic => topic.to_owned(),
        }
    }

    /// Send the request, and answer the error code of the row it names.
    async fn run(self, client: &Client, topic: &str) -> i16 {
        match self {
            Transition::UncleanElection => unclean_election(client, topic).await,
            Transition::Unregister => unregister(client, BROKER_ID).await,
            Transition::CancelReassignment => cancel_reassignment(client, topic).await,
            Transition::DeleteTopic => delete_topic(client, topic).await,
            Transition::TrimRecords => trim_records(client, topic).await,
        }
    }
}

/// `ElectLeaders(UNCLEAN)` on partition 0 of `topic`.
async fn unclean_election(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(ElectLeadersRequest {
            election_type: 1,
            topic_partitions: Some(vec![TopicPartitions {
                topic: topic.to_owned(),
                partitions: vec![0],
                ..TopicPartitions::default()
            }]),
            timeout_ms: 10_000,
            ..ElectLeadersRequest::default()
        })
        .await
        .expect("ElectLeaders");
    response
        .replica_election_results
        .first()
        .and_then(|result| result.partition_result.first())
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// `UnregisterBroker(broker_id)`.
async fn unregister(client: &Client, broker_id: i32) -> i16 {
    client
        .send(UnregisterBrokerRequest {
            broker_id,
            ..UnregisterBrokerRequest::default()
        })
        .await
        .expect("UnregisterBroker")
        .error_code
}

/// `AlterPartitionReassignments` with a null replica list: the cancel.
async fn cancel_reassignment(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(AlterPartitionReassignmentsRequest {
            timeout_ms: 10_000,
            topics: vec![ReassignableTopic {
                name: topic.to_owned(),
                partitions: vec![ReassignablePartition {
                    partition_index: 0,
                    replicas: None,
                    ..ReassignablePartition::default()
                }],
                ..ReassignableTopic::default()
            }],
            ..AlterPartitionReassignmentsRequest::default()
        })
        .await
        .expect("AlterPartitionReassignments");
    response
        .responses
        .first()
        .and_then(|row| row.partitions.first())
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// `DeleteRecords` at offset zero on partition 0 of `topic`.
async fn trim_records(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: topic.to_owned(),
                partitions: vec![DeleteRecordsPartition {
                    partition_index: 0,
                    offset: 0,
                    ..DeleteRecordsPartition::default()
                }],
                ..DeleteRecordsTopic::default()
            }],
            timeout_ms: 10_000,
            ..DeleteRecordsRequest::default()
        })
        .await
        .expect("DeleteRecords");
    response
        .topics
        .first()
        .and_then(|row| row.partitions.first())
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// How many gated transitions this broker refused for `action`.
fn refusals(broker: &BrokerHandle, action: GatedAction) -> u64 {
    broker
        .metrics()
        .break_glass_refusals
        .get_or_create(&BreakGlassActionLabel {
            action: ActionLabel(action),
        })
        .get()
}

/// How many privileged transitions this broker ran with no approval at all.
fn bypassed(broker: &BrokerHandle, action: GatedAction) -> u64 {
    broker
        .metrics()
        .break_glass_bypassed
        .get_or_create(&BreakGlassActionLabel {
            action: ActionLabel(action),
        })
        .get()
}

// ── the loop ─────────────────────────────────────────────────────────────────

/// The whole two-person loop: Alice opens a proposal, Bob and Carol approve it,
/// and the transition it authorizes then runs.
///
/// This is the case no other suite in the workspace can write. Every existing
/// break-glass test over the wire ends at a refusal, because one `PLAINTEXT`
/// listener has one principal. Delete this and the feature keeps every refusal
/// it promises and loses the only proof that an approval is ever honoured: a
/// gate that refused a fully approved proposal, or an approve handler that
/// stored nothing, would pass the rest of this file untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proposal_two_approvals_and_the_transition_they_authorize() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let bob = cluster.client(BOB).await;
    let carol = cluster.client(CAROL).await;
    create_topic(&alice, "doomed", 1).await;

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;

    // Each approval reports the running count, so an operator knows how many
    // more people the transition still needs.
    check!(
        approve(&bob, id).await
            == ApproveBreakGlassResponse {
                approvals_held: 1,
                approvals_required: 2,
                ..ApproveBreakGlassResponse::default()
            },
        "the first approval"
    );
    check!(
        approve(&carol, id).await
            == ApproveBreakGlassResponse {
                approvals_held: 2,
                approvals_required: 2,
                ..ApproveBreakGlassResponse::default()
            },
        "the second approval"
    );

    // The record names both people, in the order they agreed, and neither
    // approval carries a signature because `delete_topic` is not in this
    // broker's `signed_actions`.
    let ready = stored(&alice, id).await;
    assert!(ready.approvals.len() == 2, "{ready:?}");
    check!(
        ready.approvals
            == vec![
                WireApproval {
                    principal: principal(BOB),
                    approved_at_ms: ready.approvals[0].approved_at_ms,
                    ..WireApproval::default()
                },
                WireApproval {
                    principal: principal(CAROL),
                    approved_at_ms: ready.approvals[1].approved_at_ms,
                    ..WireApproval::default()
                },
            ]
    );
    check!(ready.proposer == principal(ALICE));
    check!(ready.consumed_at_ms == 0, "nothing has spent it yet");

    check!(delete_topic(&alice, "doomed").await == codes::NONE);
    check!(!topic_exists(&alice, "doomed").await, "the topic is gone");

    // Spending the approval stamps `consumed_at_ms` and changes nothing else.
    // That single-field difference is what makes the proposal unusable twice.
    let spent = stored(&alice, id).await;
    check!(spent.consumed_at_ms != 0);
    check!(
        spent
            == DescribedBreakGlassProposal {
                consumed_at_ms: spent.consumed_at_ms,
                ..ready
            }
    );

    cluster.broker.shutdown().await;
}

/// A second approval from one principal is not a second person.
///
/// `required_approvals = 2` is a rule about people, not about rows. A handler
/// that appended Bob's approval twice would satisfy the count while one
/// credential authorized a data-losing transition on its own, which is the
/// exact failure the feature exists to stop. The refusal is asserted, and so is
/// the consequence: the transition still refuses afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_principal_cannot_stand_in_for_two() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let bob = cluster.client(BOB).await;
    create_topic(&alice, "doomed", 1).await;

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;
    check!(approve(&bob, id).await.error_code == codes::NONE);

    let again = approve(&bob, id).await;
    check!(
        again.error_code == codes::BREAK_GLASS_DUPLICATE_APPROVER,
        "{again:?}"
    );
    check!(again.approvals_held == 1, "the second try added nothing");
    check!(
        again
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&principal(BOB))),
        "{again:?}"
    );

    check!(stored(&alice, id).await.approvals.len() == 1);
    check!(
        delete_topic(&alice, "doomed").await == codes::POLICY_VIOLATION,
        "one approval is not two"
    );
    check!(topic_exists(&alice, "doomed").await, "the topic survives");

    cluster.broker.shutdown().await;
}

/// The proposer may not approve their own proposal.
///
/// Without this check the rule is a two-click rule: one person opens a proposal
/// and approves it, and only one more click stands between one credential and
/// an unclean election. The broker answers `BREAK_GLASS_DUPLICATE_APPROVER`
/// (1007), because the proposer already counts as one of the two people.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proposer_cannot_approve_their_own_proposal() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    create_topic(&alice, "doomed", 1).await;

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;
    let refused = approve(&alice, id).await;

    check!(
        refused.error_code == codes::BREAK_GLASS_DUPLICATE_APPROVER,
        "{refused:?}"
    );
    check!(refused.approvals_held == 0);
    check!(stored(&alice, id).await.approvals.is_empty());

    cluster.broker.shutdown().await;
}

/// A principal outside `break_glass.approvers` is refused on both ends of the
/// workflow.
///
/// The approver set comes from `broker.toml` and not from the metadata log, so
/// an attacker who can write the log still cannot add themselves to it. That
/// only holds if the broker actually consults the set. Mallory holds a valid
/// SASL credential and every Kafka right the cluster asks for, and the broker
/// refuses her `BREAK_GLASS_NOT_AN_APPROVER` (1008) whether she proposes or
/// approves. The proposer check matters as much as the approver one: a
/// proposer outside the set turns a rule about three people into a rule about
/// two people and a stranger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principal_outside_the_approver_set_is_refused() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let mallory = cluster.client(MALLORY).await;
    create_topic(&alice, "doomed", 1).await;

    let opened = propose(&mallory, ACTION_DELETE_TOPIC, "doomed", TTL_CONFIGURED).await;
    check!(
        opened.error_code == codes::BREAK_GLASS_NOT_AN_APPROVER,
        "{opened:?}"
    );

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;
    let refused = approve(&mallory, id).await;
    check!(
        refused.error_code == codes::BREAK_GLASS_NOT_AN_APPROVER,
        "{refused:?}"
    );
    check!(
        refused
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&principal(MALLORY))),
        "{refused:?}"
    );
    check!(stored(&alice, id).await.approvals.is_empty());

    cluster.broker.shutdown().await;
}

// ── expiry ───────────────────────────────────────────────────────────────────

/// This process's wall clock in epoch milliseconds, which is the clock the
/// broker stamps a proposal against.
fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_millis(),
    )
    .expect("a clock inside i64 milliseconds")
}

/// Sleep until the wall clock is past `expires_at_ms`.
///
/// A real sleep, because the thing under test is a wall-clock expiry that the
/// broker reads from its own clock. There is no image change and no metric to
/// await: the proposal does not move, the clock does.
async fn sleep_past(expires_at_ms: i64) {
    let remaining = expires_at_ms.saturating_sub(now_ms()).max(0);
    let wait = u64::try_from(remaining).expect("a non-negative remainder") + 400;
    tokio::time::sleep(Duration::from_millis(wait)).await;
}

/// A proposal past its lifetime authorizes nothing, on either end.
///
/// `proposal_ttl` is the whole safety bound on an approver who was removed from
/// the set: the design says explicitly that the broker does not re-check the
/// approver set when it spends an approval, and that waiting out the lifetime
/// is what kills a pending approval by a person who has since gone. If an
/// expired proposal could still be approved, or a fully approved one could
/// still be spent after its expiry, that bound would not exist and a removed
/// approver's agreement would stay live for as long as the proposal sat in the
/// image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_proposal_authorizes_nothing() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let bob = cluster.client(BOB).await;
    let carol = cluster.client(CAROL).await;
    create_topic(&alice, "doomed", 1).await;

    // The approve path. Nobody approved it before it ran out.
    let short = propose(&alice, ACTION_DELETE_TOPIC, "doomed", 500).await;
    assert!(short.error_code == codes::NONE, "{short:?}");
    sleep_past(short.expires_at_ms).await;
    let late = approve(&bob, short.proposal_id).await;
    check!(late.error_code == codes::POLICY_VIOLATION, "{late:?}");
    check!(
        late.error_message
            .as_deref()
            .is_some_and(|message| message.contains("expired")),
        "{late:?}"
    );

    // The consume path. Two people agreed in time, and the window closed
    // before anybody spent the agreement.
    let ready = propose(&alice, ACTION_DELETE_TOPIC, "doomed", 3_000).await;
    assert!(ready.error_code == codes::NONE, "{ready:?}");
    check!(approve(&bob, ready.proposal_id).await.error_code == codes::NONE);
    check!(approve(&carol, ready.proposal_id).await.error_code == codes::NONE);
    check!(
        stored(&alice, ready.proposal_id).await.approvals.len() == 2,
        "both approvals landed inside the window"
    );

    sleep_past(ready.expires_at_ms).await;
    check!(delete_topic(&alice, "doomed").await == codes::POLICY_VIOLATION);
    check!(topic_exists(&alice, "doomed").await, "the topic survives");
    check!(
        stored(&alice, ready.proposal_id).await.consumed_at_ms == 0,
        "a refused transition spends nothing"
    );

    cluster.broker.shutdown().await;
}

// ── signatures ───────────────────────────────────────────────────────────────

/// An action in `break_glass.signed_actions` takes a detached operator
/// signature on every approval.
///
/// The name on an approval is the broker's word for it, and the whole point of
/// the signature is that the record stays provable without trusting the broker
/// that minted it. Three outcomes have to stay apart, and this case pins all
/// three: no signature is `OPERATOR_SIGNATURE_REQUIRED` (1010), a signature
/// that does not verify is `OPERATOR_SIGNATURE_INVALID` (1009), and a good one
/// is accepted and stored. The bytes are built in this file from the layout
/// KFC-9 documents, so the pass also proves the broker rebuilds the same bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_action_takes_a_signature_on_every_approval() {
    let cluster = boot_with_signed_actions(&["delete_topic"]).await;
    let alice = cluster.client(ALICE).await;
    let bob = cluster.client(BOB).await;
    let carol = cluster.client(CAROL).await;
    create_topic(&alice, "doomed", 1).await;

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;

    let unsigned = approve(&bob, id).await;
    check!(
        unsigned.error_code == codes::OPERATOR_SIGNATURE_REQUIRED,
        "{unsigned:?}"
    );

    // One flipped bit anywhere in the signature, presented under a real key id
    // by the principal that key is bound to.
    let key = cluster.key(BOB);
    let mut tampered = key
        .pair()
        .sign(&approval_signing_bytes(&stored(&bob, id).await))
        .as_ref()
        .to_vec();
    tampered[0] ^= 0x01;
    let bad = approve_with(&bob, id, &key.key_id, tampered).await;
    check!(
        bad.error_code == codes::OPERATOR_SIGNATURE_INVALID,
        "{bad:?}"
    );

    check!(
        stored(&alice, id).await.approvals.is_empty(),
        "neither refusal stored an approval"
    );

    let first = approve_signed(&bob, id, cluster.key(BOB)).await;
    check!(first.error_code == codes::NONE, "{first:?}");
    let second = approve_signed(&carol, id, cluster.key(CAROL)).await;
    check!(second.error_code == codes::NONE, "{second:?}");

    // The broker keeps what it verified, so an auditor can re-verify it later
    // against the operator public keys alone.
    let ready = stored(&alice, id).await;
    assert!(ready.approvals.len() == 2, "{ready:?}");
    check!(ready.approvals[0].key_id == cluster.key(BOB).key_id);
    check!(ready.approvals[1].key_id == cluster.key(CAROL).key_id);
    check!(ready.approvals.iter().all(|a| !a.signature.is_empty()));

    check!(delete_topic(&alice, "doomed").await == codes::NONE);

    cluster.broker.shutdown().await;
}

// ── atomicity ────────────────────────────────────────────────────────────────

/// Read the committed metadata log back off the controller listener from
/// `from`, and answer the records of each batch, batch by batch.
///
/// `image` has to be the image that was current when the records were written:
/// a Kafka metadata record names a topic by id, and translating one back needs
/// an image that still knows the id.
async fn metadata_batches(
    broker: &BrokerHandle,
    from: i64,
    image: &MetadataImage,
) -> Vec<Vec<MetadataRecord>> {
    let connection = Connection::connect(
        broker.controller_addr(),
        ConnectionOptions {
            client_id: "break-glass-test".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("dial the controller listener");
    let mut body = Vec::new();
    krabka_raft::KrabkaMetadataFetchRequest {
        fetch_offset: from,
        max_bytes: 4 << 20,
    }
    .encode_v0(&mut body);
    let raw = connection
        .raw_request(krabka_raft::API_KEY_METADATA_FETCH, 0, Bytes::from(body))
        .await
        .expect("metadata fetch");
    connection.close();

    let mut cursor: &[u8] = &raw;
    let response = krabka_raft::KrabkaMetadataFetchResponse::decode_v0(&mut cursor)
        .expect("decode the metadata fetch response");
    assert!(response.error_code == 0, "the controller served the fetch");

    let mut bytes: &[u8] = &response.records;
    let mut batches = Vec::new();
    while !bytes.is_empty() {
        let batch = RecordBatch::decode(&mut bytes).expect("decode a metadata batch");
        if batch.attributes.is_control_batch() {
            continue;
        }
        batches.push(
            batch
                .records
                .iter()
                .filter_map(|record| record.value.as_ref())
                .filter_map(|value| krabka_metadata::from_kraft_value(value, image).ok())
                .collect(),
        );
    }
    batches
}

/// Whether `record` unregisters a broker.
fn is_unregister(record: &MetadataRecord) -> bool {
    matches!(record, MetadataRecord::V1UnregisterBroker(_))
}

/// Whether `record` is the spent form of proposal `id`.
fn is_consume_of(record: &MetadataRecord, id: WireUuid) -> bool {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => {
            proposal.proposal_id.as_bytes() == &id.0 && proposal.consumed_at_ms != 0
        }
        _ => false,
    }
}

/// The consumed approval and the transition it authorizes commit together, in
/// one raft append.
///
/// This is the reason a proposal lives in the metadata log at all rather than
/// in an internal topic or a separate service. Two appends would let a crash
/// between them either spend one approval twice or lose it. Nothing in the type
/// system enforces the rule — each gated handler prepends the consumed record
/// to its own — so a handler that called `submit_change` twice would keep every
/// other assertion in this file green and quietly break the guarantee. The case
/// reads the committed log back and asserts on the batch itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_consume_and_the_transition_land_in_one_raft_append() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let id = approved_proposal(&cluster, ACTION_UNREGISTER_BROKER, &BROKER_ID.to_string()).await;

    // Snapshot the log position and the image before the transition runs.
    let image = cluster.broker.controller_image_for_test();
    let from = i64::try_from(
        cluster
            .broker
            .controller_quorum_state_for_test()
            .last_applied_index,
    )
    .expect("a log offset inside i64");
    let held = image
        .break_glass_proposal(uuid::Uuid::from_bytes(id.0))
        .expect("the approved proposal is in the image")
        .clone();
    check!(held.consumed_at_ms == 0);

    check!(unregister(&alice, BROKER_ID).await == codes::NONE);
    cluster
        .broker
        .wait_for_image(|img| img.broker(NodeId(1)).is_none())
        .await;

    let batches = metadata_batches(&cluster.broker, from, &image).await;
    let carrying: Vec<&Vec<MetadataRecord>> = batches
        .iter()
        .filter(|batch| batch.iter().any(is_unregister))
        .collect();
    assert!(
        carrying.len() == 1,
        "exactly one append carries the unregistration, found {}",
        carrying.len()
    );

    let consumed_at_ms = match carrying[0].first() {
        Some(MetadataRecord::V1BreakGlassProposal(proposal)) => proposal.consumed_at_ms,
        other => panic!("the consume leads the append; found {other:?}"),
    };
    check!(consumed_at_ms != 0);
    check!(
        carrying[0]
            == &vec![
                MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
                    consumed_at_ms,
                    ..held
                }),
                MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id: NodeId(1) }),
            ],
        "the append is the consume followed by the transition, and nothing else"
    );
    check!(
        batches
            .iter()
            .filter(|batch| batch.iter().any(|record| is_consume_of(record, id)))
            .count()
            == 1,
        "the approval is spent in one append and no other"
    );

    cluster.broker.shutdown().await;
}

// ── every gated transition ───────────────────────────────────────────────────

/// One row of the gated-transition table.
struct GatedCase {
    /// What the row is about. First, so a failure names it.
    label: &'static str,
    /// The transition, as a request makes it.
    transition: Transition,
    /// The `action` wire value a proposal must carry to authorize it.
    action: i8,
    /// The same action, as the metric families label it.
    metric: GatedAction,
    /// What the broker answers once an approved proposal covers the request.
    ///
    /// Two of the five complete on a one-broker cluster. The other three then
    /// answer a question about the partition's own state instead — there is no
    /// reassignment to cancel and no election to hold — and that change of
    /// answer is itself the proof that the authority gate opened. The gate runs
    /// before any of those state questions, exactly so that "does this need an
    /// approval" cannot depend on state a concurrent request can move.
    approved: i16,
}

/// Every transition the two-person rule gates through a Kafka API.
///
/// The unregistration is last on purpose: it drops the one broker registration
/// this cluster has, and a later row would then find no broker to place a
/// replica on.
const GATED: [GatedCase; 5] = [
    GatedCase {
        label: "an unclean leader election",
        transition: Transition::UncleanElection,
        action: ACTION_UNCLEAN_ELECT_LEADERS,
        metric: GatedAction::UncleanElectLeaders,
        approved: codes::ELECTION_NOT_NEEDED,
    },
    GatedCase {
        label: "a reassignment cancel",
        transition: Transition::CancelReassignment,
        action: ACTION_CANCEL_REASSIGNMENT,
        metric: GatedAction::CancelReassignment,
        approved: codes::NO_REASSIGNMENT_IN_PROGRESS,
    },
    GatedCase {
        label: "a record trim",
        transition: Transition::TrimRecords,
        action: ACTION_DELETE_RECORDS,
        metric: GatedAction::DeleteRecords,
        approved: codes::NONE,
    },
    GatedCase {
        label: "a topic deletion",
        transition: Transition::DeleteTopic,
        action: ACTION_DELETE_TOPIC,
        metric: GatedAction::DeleteTopic,
        approved: codes::NONE,
    },
    GatedCase {
        label: "a broker unregistration",
        transition: Transition::Unregister,
        action: ACTION_UNREGISTER_BROKER,
        metric: GatedAction::UnregisterBroker,
        approved: codes::NONE,
    },
];

/// Run one row: refuse it with no proposal, then run it with one.
async fn run_gated_case(cluster: &Cluster, alice: &Client, case: &GatedCase, topic: &str) {
    let label = case.label;
    let before = refusals(&cluster.broker, case.metric);
    check!(
        case.transition.run(alice, topic).await == codes::POLICY_VIOLATION,
        "case {label}: refused while no proposal covers it"
    );
    check!(
        refusals(&cluster.broker, case.metric) == before + 1,
        "case {label}: the refusal is counted"
    );

    let id = approved_proposal(cluster, case.action, &case.transition.target(topic)).await;
    check!(
        case.transition.run(alice, topic).await == case.approved,
        "case {label}: the approval opened the gate"
    );

    // The approval is spent exactly when the transition ran. A request that
    // gets past the gate and then fails on the partition's own state leaves the
    // proposal usable, so an operator does not have to gather two signatures
    // again over a typo.
    check!(
        (stored(alice, id).await.consumed_at_ms != 0) == (case.approved == codes::NONE),
        "case {label}: an approval is spent exactly when the transition ran"
    );
}

/// Every gated transition refuses without an approved proposal and runs with
/// one, and each refusal reaches the caller as `POLICY_VIOLATION` (44).
///
/// The five handlers each carry their own copy of the gate call, their own
/// target spelling, and their own refusal row, so a gate that was dropped from
/// one of them would leave the other four green. Code 44 is load-bearing too:
/// the design chose an existing Kafka code precisely so a JVM client classifies
/// the refusal as non-retriable, and an unassigned code would map to
/// `UNKNOWN_SERVER_ERROR` and be retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_gated_transition_refuses_without_a_proposal_and_runs_with_one() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;

    for (index, case) in GATED.iter().enumerate() {
        let topic = format!("gated-{index}");
        create_topic(&alice, &topic, 1).await;
        run_gated_case(&cluster, &alice, case, &topic).await;
    }

    cluster.broker.shutdown().await;
}

// ── multi-node ───────────────────────────────────────────────────────────────

/// A client on a `PLAINTEXT` listener, which authenticates as `User:ANONYMOUS`.
async fn plain_client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("break-glass-test")
        .build()
        .await
        .expect("client build")
}

/// Boot an `n`-node cluster whose every node runs the two-person rule.
///
/// [`support::start_n_node_with_retry`] takes no configuration hook, so this
/// wraps [`support::start_n_node_with`] in the same retry: short raft timings
/// split-vote on a slow runner, and a fresh port set usually wins on the second
/// attempt.
async fn start_gated_cluster(
    n: u64,
    background: BackgroundUncleanRecovery,
) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    for attempt in 1..=3_u32 {
        let started = support::start_n_node_with(n, |_, config| {
            config.break_glass.approvers = vec![support::ANONYMOUS.to_owned()];
            config.break_glass.background_unclean_recovery = background;
        })
        .await;
        match started {
            Ok(cluster) => return cluster,
            Err(error) => eprintln!("{n}-node cluster attempt {attempt} failed: {error:?}"),
        }
    }
    panic!("no {n}-node break-glass cluster after 3 attempts")
}

/// Where `node` sits in `cluster`.
fn index_of(cluster: &[(BrokerHandle, BrokerConfig, TempDir)], node: NodeId) -> usize {
    cluster
        .iter()
        .position(|(_, config, _)| config.node_id == node)
        .expect("the node is one of the cluster's")
}

/// Await a controller leader that is not `gone`.
async fn wait_for_new_leader(handle: &BrokerHandle, gone: NodeId) -> NodeId {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(leader) = handle.controller_leader_id()
            && leader != gone
        {
            return leader;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no controller leader replaced {gone:?} within 30s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// An approved proposal outlives the controller that recorded it.
///
/// The approver set is a per-node file value, and the design leans on the
/// proposal itself living in the metadata log so that any node which becomes
/// controller can spend it. If an approval only existed on the node that took
/// it, an incident response would die with the controller that a rolling
/// restart or a crash removed — which is exactly when a break-glass approval
/// matters most.
///
/// A `PLAINTEXT` listener has one principal, so the two approvals are written
/// straight into the metadata log here. What has to survive the failover is the
/// record; the wire path that produces it is proved on the SASL broker above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_approved_proposal_survives_a_controller_failover() {
    let mut cluster = start_gated_cluster(3, BackgroundUncleanRecovery::Off).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let leader = cluster[0].0.wait_until_controller_leader().await;
    let leader_index = index_of(&cluster, leader);
    let follower_index = (0..cluster.len())
        .find(|index| *index != leader_index)
        .expect("a follower");
    let client = plain_client(&cluster[follower_index].1.listen_addr.to_string()).await;

    create_topic(&client, "doomed", 3).await;
    let id = open(&client, ACTION_DELETE_TOPIC, "doomed").await;
    let wanted = uuid::Uuid::from_bytes(id.0);
    cluster[leader_index]
        .0
        .wait_for_image(|image| image.break_glass_proposal(wanted).is_some())
        .await;

    let held = cluster[leader_index]
        .0
        .controller_image_for_test()
        .break_glass_proposal(wanted)
        .expect("the proposal reached the leader's image")
        .clone();
    cluster[leader_index]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1BreakGlassProposal(
            BreakGlassProposalRecord {
                approvals: vec![approval_by(BOB), approval_by(CAROL)],
                ..held
            },
        ))
        .await
        .expect("the approvals commit");
    for (handle, _, _) in &cluster {
        handle
            .wait_for_image(|image| {
                image
                    .break_glass_proposal(wanted)
                    .is_some_and(|proposal| proposal.approvals.len() == 2)
            })
            .await;
    }
    let before = stored(&client, id).await;

    let (old_controller, _config, _dir) = cluster.remove(leader_index);
    old_controller.shutdown().await;
    let elected = wait_for_new_leader(&cluster[0].0, leader).await;
    check!(elected != leader, "a different node holds the quorum now");

    let after_client = plain_client(&cluster[0].1.listen_addr.to_string()).await;
    check!(
        stored(&after_client, id).await == before,
        "the proposal crossed the failover unchanged"
    );
    check!(
        delete_topic(&after_client, "doomed").await == codes::NONE,
        "the surviving controller spends the approval"
    );
    check!(stored(&after_client, id).await.consumed_at_ms != 0);

    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}

/// One unsigned approval by `user`, in the metadata form.
fn approval_by(user: &str) -> StoredApproval {
    StoredApproval {
        principal: principal(user),
        approved_at_ms: now_ms(),
        key_id: String::new(),
        signature: Vec::new(),
    }
}

// ── the background recovery path ─────────────────────────────────────────────

/// One row of the background-recovery table.
struct BackgroundCase {
    /// What the row is about. First, so a failure names it.
    label: &'static str,
    /// The `break_glass.background_unclean_recovery` setting under test.
    mode: BackgroundUncleanRecovery,
    /// Whether the recovery runs and elects a survivor at all.
    recovers: bool,
    /// Whether the broker counts the election as a bypass of a rule that
    /// nobody on this path could have satisfied.
    counts_bypass: bool,
}

/// The three answers the design gives to a recovery with no caller.
const BACKGROUND: [BackgroundCase; 3] = [
    BackgroundCase {
        label: "off keeps today's behaviour",
        mode: BackgroundUncleanRecovery::Off,
        recovers: true,
        counts_bypass: false,
    },
    BackgroundCase {
        label: "audit-only recovers and accounts for it",
        mode: BackgroundUncleanRecovery::AuditOnly,
        recovers: true,
        counts_bypass: true,
    },
    BackgroundCase {
        label: "require leaves the partition offline",
        mode: BackgroundUncleanRecovery::Require,
        recovers: false,
        counts_bypass: false,
    },
];

/// The topic the background cases take offline.
const RECOVERED: &str = "recovered";

/// Take partition 0 of [`RECOVERED`] offline behind the dead broker `victim`.
///
/// Every live ISR member is gone, so the controller's failover sweep hands the
/// partition to the offset-aware recovery manager with no proposal and nobody
/// to ask for one. That is the path this table is about, and it is the one path
/// a two-person rule cannot sit on.
async fn take_offline(controller: &BrokerHandle, victim: NodeId) {
    let current = controller
        .partition_record_for_test(RECOVERED, 0)
        .expect("a partition record");
    controller
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
            leader: victim,
            isr: vec![victim],
            leader_epoch: current.leader_epoch.next(),
            partition_epoch: current.partition_epoch + 1,
            ..current.clone()
        }))
        .await
        .expect("take the partition offline");
}

/// Assert what `case` says the broker does with a recovery nobody approved.
async fn check_background_outcome(
    controller: &BrokerHandle,
    case: &BackgroundCase,
    victim: NodeId,
) {
    let label = case.label;
    if case.recovers {
        controller
            .wait_for_image(|image| {
                image
                    .partition(RECOVERED, 0)
                    .is_some_and(|record| record.leader != victim)
            })
            .await;
    } else {
        // A negative outcome, so there is no event to await. The partition has
        // to still be leaderless after the recovery manager has had every
        // chance to elect, which a bounded wait is the only way to state.
        tokio::time::sleep(Duration::from_secs(5)).await;
        check!(
            controller
                .partition_record_for_test(RECOVERED, 0)
                .map(|record| record.leader)
                == Some(victim),
            "case {label}: the partition stays leaderless and visibly offline"
        );
    }

    if case.counts_bypass {
        controller
            .wait_for_metrics("a counted unclean-recovery bypass", |_| {
                bypassed(controller, GatedAction::UncleanRecovery) >= 1
            })
            .await;
    } else {
        check!(
            bypassed(controller, GatedAction::UncleanRecovery) == 0,
            "case {label}: nothing was recorded as bypassed"
        );
    }
}

/// Drive one background-recovery row on its own three-node cluster.
async fn background_recovery_case(case: &BackgroundCase) {
    let mut cluster = start_gated_cluster(3, case.mode).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let leader = cluster[0].0.wait_until_controller_leader().await;
    let victim_index = (0..cluster.len())
        .find(|index| cluster[*index].1.node_id != leader)
        .expect("a non-controller node");
    let victim = cluster[victim_index].1.node_id;

    let client = plain_client(
        &cluster[index_of(&cluster, leader)]
            .1
            .listen_addr
            .to_string(),
    )
    .await;
    create_topic(&client, RECOVERED, 3).await;
    for (handle, _, _) in &cluster {
        handle.wait_until_partition_present(RECOVERED, 0).await;
    }
    // Only a topic that opted into an offset-aware strategy reaches the
    // recovery manager. Without one the failover path elects directly and never
    // consults the background rule at all.
    cluster[index_of(&cluster, leader)]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: RECOVERED.to_owned(),
            overrides: [(
                "unclean.recovery.strategy".to_owned(),
                "Aggressive".to_owned(),
            )]
            .into_iter()
            .collect(),
        }))
        .await
        .expect("set unclean.recovery.strategy");

    let (dead, _config, _dir) = cluster.remove(victim_index);
    dead.shutdown().await;
    let controller = &cluster[index_of(&cluster, leader)].0;
    // The ISR shrink is the controller's own signal that liveness has marked
    // the broker dead, which is what the failover sweep reads.
    controller
        .wait_for_image(|image| {
            image
                .partition(RECOVERED, 0)
                .is_some_and(|record| !record.isr.contains(&victim))
        })
        .await;

    take_offline(controller, victim).await;
    check_background_outcome(controller, case, victim).await;

    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}

/// The background unclean-recovery path under each of the three settings it
/// takes.
///
/// This path runs from leader election and from a broker heartbeat, with no
/// request, no connection, and no principal, so a two-person rule cannot exist
/// on it. The design says that plainly rather than leaving a silent gap, and
/// this case is what keeps the statement true: `off` is today's behaviour,
/// `audit-only` recovers and counts the bypass so an operator can prove after
/// the fact that a data-losing election happened with nobody's approval, and
/// `require` fails closed and leaves the partition visibly offline. A setting
/// that quietly collapsed into another would turn the documented three-way
/// choice into a lie, and `break_glass_bypassed` is the series an operator is
/// told to alert on.
///
/// The rows run one cluster at a time rather than three at once: each needs
/// three brokers and a broker death, and the sequence keeps the peak cost of
/// the case to one cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_unclean_recovery_follows_its_configured_rule() {
    for case in &BACKGROUND {
        background_recovery_case(case).await;
    }
}
