mod support;

use assert2::{assert, check};
use krabka_broker::coordinator::AUDIT_TOPIC;
use krabka_protocol::{
    krabka::{
        break_glass::{
            ApproveBreakGlassRequest, ApproveBreakGlassResponse, ProposeBreakGlassRequest,
            ProposeBreakGlassResponse,
        },
        freeze::{
            PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED, SetTopicFreezeRequest,
            SetTopicFreezeResponse,
        },
    },
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};

#[tokio::test]
async fn audit_topic_exists_after_startup() {
    let p = support::start().await;
    p.broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    // Send a Metadata request for `__krabka_audit` and assert the broker
    // returns it with `error_code == 0` and at least one partition.
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(AUDIT_TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("MetadataRequest failed");

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(AUDIT_TOPIC))
        .expect("__krabka_audit not in Metadata response");

    assert2::check!(
        topic.error_code == 0,
        "unexpected error code: {}",
        topic.error_code
    );
    assert2::check!(
        !topic.partitions.is_empty(),
        "__krabka_audit has no partitions"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn broker_started_event_is_written_to_audit_topic() {
    let p = support::start().await;

    // Wait for the BrokerStarted event to be durably written to the audit topic
    // (the sink increments `audit_events_total` on each successful produce).
    p.broker
        .wait_for_metrics("audit event written", |m| m.audit_events_total.get() >= 1)
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "BrokerStarted", |j| {
        j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted"
    })
    .await;

    p.broker.shutdown().await;
}

/// Verifies that a successful `CreateTopics` call emits an `AdminOperation`
/// audit record. That record must carry `class_uid == 6003`,
/// `api.operation == "CreateTopics"`, `status_id == 1`, and the topic name in
/// `resources[0].name`.
#[tokio::test]
async fn successful_create_topics_is_audited() {
    let p = support::start().await;

    let audit_before = p.broker.metrics().audit_events_total.get();
    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "audited-orders".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::check!(cr.topics[0].error_code == 0);

    // Wait for the CreateTopics AdminOperation audit record to be durable.
    p.broker
        .wait_for_metrics("audit event written", |m| {
            m.audit_events_total.get() > audit_before
        })
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "CreateTopics admin audit", |j| {
        j["class_uid"] == 6003
            && j["api"]["operation"] == "CreateTopics"
            && j["status_id"] == 1
            && j["resources"][0]["name"] == "audited-orders"
    })
    .await;

    p.broker.shutdown().await;
}

/// Verifies the checkpoint path. The broker is configured with an audit
/// signing key and a checkpoint cadence of `every_n = 1`. A `CreateTopics`
/// request must then put a `checkpoint` record on the audit topic with the
/// expected `key_id`.
#[tokio::test]
async fn signed_checkpoints_appear_on_audit_topic() {
    use ring::signature::Ed25519KeyPair;

    // Generate a key, write it to a temp file, start a broker configured to use it.
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let keydir = tempfile::tempdir().unwrap();
    let keypath = keydir.path().join("audit.pk8");
    std::fs::write(&keypath, pkcs8.as_ref()).unwrap();

    // Start a broker with audit signing + a tiny checkpoint cadence (every 1 record).
    let p = support::start_with_audit_key(&keypath, "k-test", 1).await;

    // Cause some audit events (a create succeeds; super-user path).
    let audit_before = p.broker.metrics().audit_events_total.get();
    let _ = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "cp-topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for the create's chained record AND its signed checkpoint to be
    // durable: with `every_n = 1`, each audit event triggers a checkpoint, so
    // the counter advances by 2 (chained record + checkpoint) per create.
    p.broker
        .wait_for_metrics("audit checkpoint written", |m| {
            m.audit_events_total.get() >= audit_before + 2
        })
        .await;

    let recs = support::wait_for_audit_record(&p.client, "signed checkpoint", |j| {
        j["type"] == "checkpoint" && j["key_id"] == "k-test"
    })
    .await;
    let saw_checkpoint = recs
        .iter()
        .any(|j| j["type"] == "checkpoint" && j["key_id"] == "k-test");
    assert2::check!(saw_checkpoint);

    p.broker.shutdown().await;
}

/// Verifies that the authorizer-decorator path denies an unauthorized
/// operation.
///
/// This test asserts that:
///   1. The broker denies a `CreateTopics` request with
///      `CLUSTER_AUTHORIZATION_FAILED`.
///   2. The broker stays healthy and does not crash.
///
/// This test does NOT assert that the broker emitted an `AuthorizationDenied`
/// audit record to the audit topic.
///
/// The full end-to-end path, which sends a denied request and then observes
/// the `AuthorizationDenied` record in the audit topic through the same
/// client, is impractical for these reasons:
///   - The test client connects anonymously, with the principal
///     `"ANONYMOUS"`.
///   - `SimpleAclAuthorizer` with no ACLs and no super-users denies every
///     request, including the `Fetch` that reads the audit topic back.
///   - There is no plaintext SASL path that would give the anonymous reader a
///     higher principal without SCRAM credentials.
///
/// The unit test `deny_decision_emits_audit_record` in
/// `crates/broker/src/audit_authorizer.rs` already proves the audit emit on a
/// deny.
#[tokio::test]
async fn denied_operation_returns_cluster_authorization_failed() {
    // Start a broker with a deny-all authorizer.
    let p = support::start_with_deny_all_authz().await;

    // Attempt a create that will be denied.
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "denied-topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();

    // Verify the broker actually denied the request (error_code
    // CLUSTER_AUTHORIZATION_FAILED = 31).
    let denied = resp
        .topics
        .iter()
        .any(|t| t.error_code == krabka_broker::codes::CLUSTER_AUTHORIZATION_FAILED);
    assert2::check!(denied, "expected CreateTopics to be denied; resp: {resp:?}");

    // Verify the broker is still alive by checking the audit topic is reachable.
    let topic_id = support::topic_id_for(&p.client, AUDIT_TOPIC).await;
    let fr = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 0,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    // The broker responded to the Fetch request without crashing.
    let _ = fr;

    p.broker.shutdown().await;
}

/// Verifies that the audit hash-chain sequence numbers are contiguous, and
/// that none repeats, across a broker restart. That shows that chain recovery
/// worked, and that the second boot did NOT reset the chain to seq 0.
#[tokio::test]
async fn audit_chain_continues_across_restart() {
    let dir = tempfile::tempdir().unwrap();

    // First boot: generate some audit events, then shut down cleanly.
    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        let audit_before = broker.metrics().audit_events_total.get();
        let _ = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "r1".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .unwrap();
        // Ensure the r1 CreateTopics audit record is durable before shutdown.
        broker
            .wait_for_metrics("audit event written", |m| {
                m.audit_events_total.get() > audit_before
            })
            .await;
        broker.shutdown().await;
    }

    // Second boot on the SAME data dir: more events.
    let (broker, client) = support::start_with_dir(dir.path()).await;
    let audit_before = broker.metrics().audit_events_total.get();
    let _ = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "r2".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    // Ensure the r2 CreateTopics audit record is durable before consuming.
    broker
        .wait_for_metrics("audit event written", |m| {
            m.audit_events_total.get() > audit_before
        })
        .await;

    // Consume the audit topic and assert seqs are a contiguous, duplicate-free
    // chain (recovery worked — no reset to 0 on the second boot).
    let seqs = support::wait_for_audit_seq_count(&client, 4).await;
    assert2::check!(seqs.len() >= 4); // 2 BrokerStarted + 2 CreateTopics (at least)
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert2::check!(sorted.len() == seqs.len()); // no duplicate seqs
    assert2::check!(sorted == (0..seqs.len() as u64).collect::<Vec<_>>()); // contiguous from 0

    broker.shutdown().await;
}

// ── KFC-9: the audit log as evidence in its own right ────────────────────────
//
// KFC-9's load-bearing claim is that the audit log *alone* answers who did
// what. An auditor holding this topic and the operator public keys has to be
// able to say who froze a topic, who approved the thaw, and which approval
// authorized which transition — with no metadata image to read and no broker
// to take anybody's word for. Nothing above tests that claim, and the unit
// tests cannot: they observe the event on its way into the log rather than on
// its way back out of it.
//
// The cases below drive one whole workflow over the wire and then read the
// answer back off `__krabka_audit`.
//
// # Why these run over SASL
//
// A two-person rule needs two people. A plaintext listener authenticates every
// connection as one name, so a proposer over such a listener is also the only
// available approver and the broker refuses them by design. These cases speak
// `SASL_PLAINTEXT` so that alice, bob and carol are three real principals and
// no broker-side shortcut stands in for a second person.

/// The proposer. She freezes the topic, opens the proposal, and thaws it.
const ALICE: &str = "User:alice";
/// The first approver.
const BOB: &str = "User:bob";
/// The second approver. Two are needed: `break_glass.required_approvals`
/// defaults to two, and the proposer may not approve her own proposal.
const CAROL: &str = "User:carol";

/// The operator key alice signs a freeze under.
const ALICE_KEY_ID: &str = "alice-yubi";

/// The scope alice freezes, and the scope the proposal names.
const FROZEN_SCOPE: &str = "orders";
/// The same scope as a break-glass target, which is `"<pattern>:<scope>"`.
const FROZEN_TARGET: &str = "literal:orders";
/// The target of the second proposal, the one that is withdrawn rather than
/// spent. It names a different scope so that a case joining on the first
/// proposal cannot match it by accident.
const WITHDRAWN_TARGET: &str = "literal:invoices";

const FREEZE_REASON: &str = "DR cutover: stop writes before the promotion";
const PROPOSE_REASON: &str = "the promotion finished; hand the topic back";
const THAW_REASON: &str = "promotion complete";
const WITHDRAW_REASON: &str = "raised against the wrong scope";

/// `BreakGlassAction::ThawTopicFreeze` as the wire spells it.
///
/// The broker keeps that mapping `pub(crate)`, so this is a copy of it. It is
/// not a copy taken on trust: the proposal's own audit record names the action
/// in words, and [`every_kfc9_phase_writes_one_chained_audit_record`] asserts
/// that this byte reaches the log as `thaw_topic_freeze`.
const ACTION_THAW_TOPIC_FREEZE: i8 = 1;

/// The domain separator in front of a freeze record's signed bytes.
///
/// KFC-9 publishes this constant and the layout below it, which is what lets
/// an auditor write their own verifier. This is that auditor's copy, written
/// from the specification rather than reached for inside the broker.
const FREEZE_DOMAIN: &[u8] = b"krabka-topic-freeze-v1\0";

/// Milliseconds since the Unix epoch, as the operator's own machine reads them.
fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("epoch milliseconds fit in an i64")
}

/// Append `bytes` behind its `u32` big-endian length.
fn put_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("a fixture field is far below u32::MAX");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// The fields a freeze record's Ed25519 signature covers.
#[derive(Clone, Copy)]
struct FreezeBytes<'a> {
    cluster_id: &'a str,
    pattern_type: i8,
    scope: &'a str,
    frozen: bool,
    reason: &'a str,
    set_by: &'a str,
    set_at_ms: i64,
    proposal_id: uuid::Uuid,
}

/// The canonical bytes of a freeze record, built the way an auditor builds
/// them: from the layout KFC-9 publishes, with no broker code in the loop.
///
/// A test that reached for the broker's own `freeze_signing_bytes` would prove
/// only that the broker agrees with itself. The point of this second
/// implementation is that it agrees with the *document*.
fn freeze_signing_bytes(input: &FreezeBytes<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(FREEZE_DOMAIN);
    put_len_prefixed(&mut out, input.cluster_id.as_bytes());
    out.extend_from_slice(&input.pattern_type.to_be_bytes());
    put_len_prefixed(&mut out, input.scope.as_bytes());
    out.push(u8::from(input.frozen));
    put_len_prefixed(&mut out, input.reason.as_bytes());
    put_len_prefixed(&mut out, input.set_by.as_bytes());
    out.extend_from_slice(&input.set_at_ms.to_be_bytes());
    out.extend_from_slice(input.proposal_id.as_bytes());
    out
}

/// The `pattern_type` byte behind the `"<pattern>:"` prefix of an audit
/// event's target.
fn pattern_type_byte(name: &str) -> i8 {
    match name {
        "literal" => PATTERN_TYPE_LITERAL,
        "prefixed" => PATTERN_TYPE_PREFIXED,
        other => panic!("no freeze pattern type is spelled {other:?}"),
    }
}

/// One `SetTopicFreeze` request as the operator's own machine builds it: the
/// signature is made here, from the private key that never reaches a broker.
struct SignedFreeze<'a> {
    key: &'a support::OperatorKey,
    cluster_id: &'a str,
    frozen: bool,
    reason: &'a str,
    proposal_id: uuid::Uuid,
    set_at_ms: i64,
}

async fn send_signed_freeze(
    client: &krabka_client_core::Client,
    freeze: &SignedFreeze<'_>,
) -> SetTopicFreezeResponse {
    let message = freeze_signing_bytes(&FreezeBytes {
        cluster_id: freeze.cluster_id,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: FROZEN_SCOPE,
        frozen: freeze.frozen,
        reason: freeze.reason,
        set_by: &freeze.key.principal,
        set_at_ms: freeze.set_at_ms,
        proposal_id: freeze.proposal_id,
    });
    client
        .send(SetTopicFreezeRequest {
            scope: FROZEN_SCOPE.into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: freeze.frozen,
            reason: freeze.reason.into(),
            proposal_id: WireUuid(*freeze.proposal_id.as_bytes()),
            set_at_ms: freeze.set_at_ms,
            key_id: freeze.key.key_id.clone(),
            signature: freeze.key.pair().sign(&message).as_ref().to_vec(),
            ..SetTopicFreezeRequest::default()
        })
        .await
        .expect("SetTopicFreeze")
}

/// Open a proposal to thaw `target`.
async fn propose_thaw(
    client: &krabka_client_core::Client,
    target: &str,
    reason: &str,
) -> ProposeBreakGlassResponse {
    client
        .send(ProposeBreakGlassRequest {
            action: ACTION_THAW_TOPIC_FREEZE,
            target: target.into(),
            reason: reason.into(),
            // Zero asks for `break_glass.proposal_ttl`.
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass")
}

/// Approve a proposal, or withdraw it.
async fn settle_proposal(
    client: &krabka_client_core::Client,
    proposal_id: WireUuid,
    withdraw: bool,
) -> ApproveBreakGlassResponse {
    client
        .send(ApproveBreakGlassRequest {
            proposal_id,
            withdraw,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass")
}

/// The cluster id, as any client reads it.
///
/// The signed freeze bytes cover it, so an auditor needs it. It is a
/// client-visible fact about the cluster and not a reading of the metadata
/// image's freeze registry, which is the thing these cases refuse to consult.
async fn cluster_id_of(client: &krabka_client_core::Client) -> String {
    client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata")
        .cluster_id
        .expect("the broker reports a cluster id")
}

/// The `PrivilegedAction` records whose `api.operation` is `operation`.
///
/// The OCSF body spells that field `"<action>.<phase>"`, so one string selects
/// both the transition and the step of the workflow.
fn privileged<'a>(records: &'a [serde_json::Value], operation: &str) -> Vec<&'a serde_json::Value> {
    records
        .iter()
        .filter(|j| j["class_uid"] == 6003 && j["api"]["operation"] == operation)
        .collect()
}

/// The one `PrivilegedAction` record for `operation`.
fn one_privileged<'a>(records: &'a [serde_json::Value], operation: &str) -> &'a serde_json::Value {
    let rows = privileged(records, operation);
    assert!(
        rows.len() == 1,
        "expected exactly one {operation} record, got {}",
        rows.len()
    );
    rows[0]
}

/// One broker that trusts alice's operator key, takes all three principals as
/// its break-glass approvers, and holds a `PLAIN` credential for each.
///
/// alice is in the approver set because a proposer has to be: a proposer from
/// outside it could open a proposal that two approvers then sign, which turns a
/// rule about three people into a rule about two and a stranger. She still
/// cannot approve her own proposal, which is why bob and carol both do.
struct Cluster {
    broker: krabka_broker::BrokerHandle,
    bootstrap: String,
    /// alice's key. Only the public half matters once the workflow has run:
    /// that is the half an auditor holds.
    alice_key: support::OperatorKey,
    /// Where the audit partition's segment files live, for the offline reader.
    log_dir: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn boot_workflow_cluster() -> Cluster {
    let dir = tempfile::tempdir().expect("tempdir");
    // The keys live beside the log directory rather than inside it, so no
    // stray file lands where the broker enumerates partition directories.
    let keys = dir.path().join("keys");
    std::fs::create_dir_all(&keys).expect("create the operator key directory");
    let alice_key = support::mint_operator_key(&keys, ALICE_KEY_ID, ALICE);
    let log_dir = dir.path().join("data");

    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl(
        &log_dir,
        &[&alice_key],
        &[ALICE, BOB, CAROL],
        &[
            ("alice", "alice-pw"),
            ("bob", "bob-pw"),
            ("carol", "carol-pw"),
        ],
    )
    .await;
    broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;
    Cluster {
        broker,
        bootstrap,
        alice_key,
        log_dir,
        _dir: dir,
    }
}

/// One complete freeze-and-thaw workflow, and the facts a case needs to join
/// its records back together.
struct ThawWorkflow {
    cluster: Cluster,
    /// alice's client. Every case reads the audit topic back through it.
    alice: krabka_client_core::Client,
    /// The cluster id inside the signed freeze bytes.
    cluster_id: String,
    /// The proposal that authorized the thaw.
    proposal_id: uuid::Uuid,
    /// The timestamp alice signed into the freeze record.
    ///
    /// This is the one field of the signed bytes that the audit event does not
    /// carry; see
    /// [`a_signed_freeze_reverifies_from_the_audit_topic_with_no_metadata_image`].
    freeze_set_at_ms: i64,
}

/// Run every KFC-9 phase once, in the order an incident produces them.
///
/// alice freezes `orders` under her own signature, proposes the thaw, bob and
/// carol approve, and alice thaws — which spends the approval in the same raft
/// append that removes the entry. A second proposal is then withdrawn rather
/// than spent, because a withdrawal is the one path that records the
/// `consumed` phase without a Kafka transition behind it.
async fn run_thaw_workflow() -> ThawWorkflow {
    let cluster = boot_workflow_cluster().await;
    let alice = support::sasl_client(&cluster.bootstrap, "alice", "alice-pw").await;
    let bob = support::sasl_client(&cluster.bootstrap, "bob", "bob-pw").await;
    let carol = support::sasl_client(&cluster.bootstrap, "carol", "carol-pw").await;
    let cluster_id = cluster_id_of(&alice).await;

    let freeze_set_at_ms = now_ms();
    let frozen = send_signed_freeze(
        &alice,
        &SignedFreeze {
            key: &cluster.alice_key,
            cluster_id: &cluster_id,
            frozen: true,
            reason: FREEZE_REASON,
            proposal_id: uuid::Uuid::nil(),
            set_at_ms: freeze_set_at_ms,
        },
    )
    .await;
    assert!(frozen.error_code == 0, "the freeze was refused: {frozen:?}");

    let proposed = propose_thaw(&alice, FROZEN_TARGET, PROPOSE_REASON).await;
    assert!(
        proposed.error_code == 0,
        "the proposal was refused: {proposed:?}"
    );
    for (label, client) in [("bob", &bob), ("carol", &carol)] {
        let approved = settle_proposal(client, proposed.proposal_id, false).await;
        assert!(
            approved.error_code == 0,
            "{label}'s approval was refused: {approved:?}"
        );
    }

    // A signed record's timestamp must beat the entry it replaces. The two
    // requests are several round trips apart, so wall time already does that;
    // the `max` makes it true whatever the clock's resolution.
    let thawed = send_signed_freeze(
        &alice,
        &SignedFreeze {
            key: &cluster.alice_key,
            cluster_id: &cluster_id,
            frozen: false,
            reason: THAW_REASON,
            proposal_id: uuid::Uuid::from_bytes(proposed.proposal_id.0),
            set_at_ms: now_ms().max(freeze_set_at_ms + 1),
        },
    )
    .await;
    assert!(thawed.error_code == 0, "the thaw was refused: {thawed:?}");

    let withdrawn = propose_thaw(&alice, WITHDRAWN_TARGET, WITHDRAW_REASON).await;
    assert!(
        withdrawn.error_code == 0,
        "the second proposal was refused: {withdrawn:?}"
    );
    let settled = settle_proposal(&bob, withdrawn.proposal_id, true).await;
    assert!(
        settled.error_code == 0,
        "the withdrawal was refused: {settled:?}"
    );

    ThawWorkflow {
        cluster,
        alice,
        cluster_id,
        proposal_id: uuid::Uuid::from_bytes(proposed.proposal_id.0),
        freeze_set_at_ms,
    }
}

/// Verifies that every KFC-9 phase reaches the audit topic, and that the
/// hash chain still runs unbroken across all of them.
///
/// The unit tests prove that each handler hands a `PrivilegedAction` event to
/// the audit log. They cannot prove that the event survives the sink, the
/// chain, and the produce into `__krabka_audit`, and a phase that reaches no
/// audit topic is a phase an auditor cannot count.
///
/// The chain assertion is the second half. `krabka-audit`'s offline verifier
/// is the reader an auditor would actually run: it walks the segment files and
/// recomputes `SHA256(prev ‖ seq ‖ value)` for each record, with no broker in
/// the loop. The broker is shut down first, so what the verifier reads is the
/// durable log rather than a live writer's buffer.
#[tokio::test]
async fn every_kfc9_phase_writes_one_chained_audit_record() {
    let w = run_thaw_workflow().await;
    let records = support::wait_for_audit_record(&w.alice, "the withdrawal", |j| {
        j["api"]["operation"] == "thaw_topic_freeze.consumed"
    })
    .await;

    // One row per phase the workflow produced. `status_id` is 1 for a success,
    // and every phase here succeeded: a refusal would be 2.
    for (label, operation) in [
        ("a freeze", "set_topic_freeze.applied"),
        ("a proposal", "thaw_topic_freeze.proposed"),
        ("an approval", "thaw_topic_freeze.approved"),
        ("a thaw", "thaw_topic_freeze.applied"),
        ("a consumed proposal", "thaw_topic_freeze.consumed"),
    ] {
        let rows = privileged(&records, operation);
        check!(
            !rows.is_empty(),
            "case {label}: no {operation} on the topic"
        );
        check!(
            rows.iter().all(|j| j["status_id"] == 1),
            "case {label}: {operation} did not record a success"
        );
    }

    // The seqs the topic carries are contiguous and never repeat, so no phase
    // slipped in without taking a chain slot.
    let seqs = support::audit_record_seqs(&w.alice).await;
    check!(
        seqs.len() >= 5,
        "five phases wrote fewer than five chained records: {seqs:?}"
    );
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    check!(sorted.len() == seqs.len(), "a seq repeats: {seqs:?}");
    let contiguous: Vec<u64> =
        (0..u64::try_from(seqs.len()).expect("a test-sized chain")).collect();
    check!(sorted == contiguous, "the chain has a hole: {seqs:?}");

    w.cluster.broker.shutdown().await;

    let partition = krabka_log::name::partition_dir(&w.cluster.log_dir, AUDIT_TOPIC, 0);
    let report =
        krabka_audit::verify_partition_dir(&partition, &krabka_audit::TrustedKeys::default())
            .expect("read the audit partition off disk");
    check!(
        report.ok,
        "the audit hash chain broke: {:?}",
        report.first_break
    );
    check!(
        report.records.0 >= u64::try_from(seqs.len()).expect("a test-sized chain"),
        "the offline reader saw fewer records than the fetch did"
    );
}

/// Verifies the join an auditor actually runs: take the proposal id off the
/// transition row, and it selects exactly the approvals that authorized it.
///
/// Presence is not the property that matters here. A log holding an approve
/// row and a transition row proves nothing unless the two can be tied
/// together, and the proposal id is the only thing that ties them. This case
/// starts from the transition, follows the id, and asserts that the principals
/// it reaches are the two people who actually approved.
///
/// It also asserts the shape of an approval row. A two-person rule whose
/// record names one person is not a two-person rule, so carol's approval has
/// to name carol as the actor and bob among the counterparties.
#[tokio::test]
async fn the_proposal_id_joins_each_approval_to_the_transition_it_authorized() {
    let w = run_thaw_workflow().await;
    let records = support::wait_for_audit_record(&w.alice, "the thaw", |j| {
        j["api"]["operation"] == "thaw_topic_freeze.applied"
    })
    .await;

    // Start where an auditor starts: the row that says a thaw happened.
    let thaw = one_privileged(&records, "thaw_topic_freeze.applied");
    let proposal = thaw["privileged_action"]["proposal_id"]
        .as_str()
        .expect("the transition names the proposal it spent")
        .to_owned();
    check!(proposal == w.proposal_id.to_string());

    // Follow that id into the approvals. Two proposals exist on this cluster,
    // so a join that matched on anything looser would pull in the wrong rows.
    let mut approvers: Vec<&str> = privileged(&records, "thaw_topic_freeze.approved")
        .into_iter()
        .filter(|j| j["privileged_action"]["proposal_id"] == serde_json::json!(proposal))
        .filter_map(|j| j["actor"]["user"]["name"].as_str())
        .collect();
    approvers.sort_unstable();
    check!(
        approvers == vec![BOB, CAROL],
        "the proposal id did not reach both approvers"
    );

    // And into the proposal, which names the person who asked for the thaw.
    let proposed: Vec<&str> = privileged(&records, "thaw_topic_freeze.proposed")
        .into_iter()
        .filter(|j| j["privileged_action"]["proposal_id"] == serde_json::json!(proposal))
        .filter_map(|j| j["actor"]["user"]["name"].as_str())
        .collect();
    check!(proposed == vec![ALICE]);

    // The approval row itself names both people. `counterparties` is the
    // approval list as it stood after this approval landed, so carol's row
    // carries bob as well as carol; the whole array is compared rather than a
    // membership test, so a lost or reordered name fails here.
    let carols = privileged(&records, "thaw_topic_freeze.approved")
        .into_iter()
        .find(|j| j["actor"]["user"]["name"] == serde_json::json!(CAROL))
        .expect("carol's approval reached the audit topic");
    check!(
        carols["privileged_action"]["counterparties"]
            == serde_json::json!([
                { "name": BOB, "type": "" },
                { "name": CAROL, "type": "" },
            ]),
        "carol's approval does not name bob"
    );

    w.cluster.broker.shutdown().await;
}

/// Verifies the claim the whole feature rests on: a signed freeze re-verifies
/// from the audit topic alone, against the operator's public key, with no
/// metadata image read and no broker asked.
///
/// This is the case that makes the audit log independent evidence rather than
/// a second copy of the broker's opinion. The record in the metadata log
/// carries the same signature, but reading it means trusting the broker that
/// serves it. Here the event is fetched off `__krabka_audit`, the signed bytes
/// are rebuilt out of that event's own fields, and the signature is checked
/// against the public key file — by an Ed25519 verifier this test drives
/// directly, so no broker code decides the answer.
///
/// # What the event does not carry
///
/// One of the eight signed fields does not come out of the event: the cluster
/// id, which is a property of the cluster whose log the auditor is holding and
/// so is theirs to know. Every other field, `set_at_ms` included, is read here
/// out of the record itself.
///
/// `set_at_ms` needs its own field because the event's `time` is the moment the
/// broker emitted the record, not the moment the operator signed, and it is the
/// signed instant that is inside the preimage. An earlier version of this case
/// took it from the operator's own note of what they had signed and said so,
/// because the event did not carry it -- which meant the audit topic alone was
/// not enough, contradicting what KFC-9 claims for it.
///
/// The tampering check is what keeps the verification from being vacuous: the
/// same signature over the same record with `frozen` flipped must fail, which
/// is the replay-a-freeze-as-a-thaw attack that KFC-9 signs that byte to stop.
#[tokio::test]
async fn a_signed_freeze_reverifies_from_the_audit_topic_with_no_metadata_image() {
    let w = run_thaw_workflow().await;
    let records = support::wait_for_audit_record(&w.alice, "the freeze", |j| {
        j["api"]["operation"] == "set_topic_freeze.applied"
    })
    .await;
    let freeze = one_privileged(&records, "set_topic_freeze.applied");
    let action = &freeze["privileged_action"];

    // Everything below this line comes out of the event.
    let target = action["target"]
        .as_str()
        .expect("the event names its scope");
    let (pattern, scope) = target
        .split_once(':')
        .expect("a freeze target is \"<pattern>:<scope>\"");
    let set_by = freeze["actor"]["user"]["name"]
        .as_str()
        .expect("the event names its actor");
    let reason = freeze["status_detail"]
        .as_str()
        .expect("the event carries the operator's reason");
    let proposal_id = uuid::Uuid::parse_str(
        action["proposal_id"]
            .as_str()
            .expect("the event carries a proposal id"),
    )
    .expect("a uuid");
    let key_id = action["key_id"].as_str().expect("the event names the key");
    let set_at_ms = action["signed_at_ms"]
        .as_i64()
        .expect("the event carries the stamp the signature covers");
    let signature = hex::decode(
        action["signature"]
            .as_str()
            .expect("the event carries the raw signature"),
    )
    .expect("the signature is lowercase hex");

    let signed = FreezeBytes {
        cluster_id: &w.cluster_id,
        pattern_type: pattern_type_byte(pattern),
        scope,
        // `set_topic_freeze` is the freeze direction; `thaw_topic_freeze` is
        // the other one. The action name is what tells the two apart.
        frozen: true,
        reason,
        set_by,
        set_at_ms,
        proposal_id,
    };

    check!(key_id == ALICE_KEY_ID);
    check!(action["signature_verified"] == true);
    // The stamp the event carries is the one the operator signed, and it is a
    // different instant from the one the broker logged. Asserting both halves
    // is what stops a future change from quietly setting `signed_at_ms` to the
    // emit time, which would verify here and be wrong.
    check!(set_at_ms == w.freeze_set_at_ms);
    check!(freeze["time"].as_i64() != Some(set_at_ms));
    let public = std::fs::read(&w.cluster.alice_key.public_path).expect("the operator public key");
    let key = ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &public);
    check!(
        key.verify(&freeze_signing_bytes(&signed), &signature)
            .is_ok(),
        "the freeze on the audit topic does not verify under alice's key"
    );

    // The same signature must not carry a thaw. One byte separates the two
    // records, and it is inside the signed bytes precisely so that a captured
    // freeze signature cannot be replayed in the dangerous direction.
    let as_thaw = FreezeBytes {
        frozen: false,
        ..signed
    };
    check!(
        key.verify(&freeze_signing_bytes(&as_thaw), &signature)
            .is_err(),
        "a freeze signature verified as a thaw"
    );

    // The join the fix to the freeze principal made possible: the freeze names
    // its author in the same Kafka form the break-glass events use, so an
    // auditor can tie a freeze to the proposal its author later opened. Two
    // spellings of one person would break this and nothing else would notice.
    let proposed = privileged(&records, "thaw_topic_freeze.proposed");
    check!(set_by == ALICE);
    check!(
        proposed
            .iter()
            .any(|j| j["actor"]["user"]["name"] == serde_json::json!(set_by)),
        "the freeze and the proposal spell their author differently"
    );

    w.cluster.broker.shutdown().await;
}

/// Verifies that the KFC-9 metric families move on a real request.
///
/// A metric that is defined but never fed passes every unit test, and the
/// KFC-7 suite found exactly that gap the hard way. These four series are the
/// ones an operator alerts on, so each is watched across the request that is
/// supposed to move it: the registry gauge up on the freeze and back down on
/// the thaw, and a proposal walking `pending` → `approved` → `consumed`.
///
/// The refusal counter is the fourth, and it needs a Kafka transition rather
/// than a private one: `DeleteTopics` on a gated broker with no approval is
/// refused with `POLICY_VIOLATION`, bumps `break_glass_refusals`, and writes
/// its own refused row to the audit topic. Counting the refusal and auditing
/// it are two different promises, and this asserts both of them at once.
#[tokio::test]
async fn the_kfc9_gauges_and_counters_move_on_real_requests() {
    use krabka_broker::metrics::{
        BreakGlassAction as ActionLabel, BreakGlassActionLabel, BreakGlassState,
        BreakGlassStateLabel, BrokerMetrics,
    };

    fn proposals(metrics: &BrokerMetrics, state: BreakGlassState) -> i64 {
        metrics
            .break_glass_proposals
            .get_or_create(&BreakGlassStateLabel { state })
            .get()
    }
    fn refusals(metrics: &BrokerMetrics, action: krabka_metadata::BreakGlassAction) -> u64 {
        metrics
            .break_glass_refusals
            .get_or_create(&BreakGlassActionLabel {
                action: ActionLabel(action),
            })
            .get()
    }

    let cluster = boot_workflow_cluster().await;
    let alice = support::sasl_client(&cluster.bootstrap, "alice", "alice-pw").await;
    let bob = support::sasl_client(&cluster.bootstrap, "bob", "bob-pw").await;
    let carol = support::sasl_client(&cluster.bootstrap, "carol", "carol-pw").await;
    let cluster_id = cluster_id_of(&alice).await;
    check!(cluster.broker.metrics().topic_freezes_active.get() == 0);

    let set_at_ms = now_ms();
    let freeze = SignedFreeze {
        key: &cluster.alice_key,
        cluster_id: &cluster_id,
        frozen: true,
        reason: FREEZE_REASON,
        proposal_id: uuid::Uuid::nil(),
        set_at_ms,
    };
    check!(send_signed_freeze(&alice, &freeze).await.error_code == 0);
    cluster
        .broker
        .wait_for_metrics("the registry gauge counts the freeze", |m| {
            m.topic_freezes_active.get() == 1
        })
        .await;

    let proposed = propose_thaw(&alice, FROZEN_TARGET, PROPOSE_REASON).await;
    check!(proposed.error_code == 0);
    cluster
        .broker
        .wait_for_metrics("the proposal counts as pending", |m| {
            proposals(m, BreakGlassState::Pending) == 1
        })
        .await;

    for client in [&bob, &carol] {
        check!(
            settle_proposal(client, proposed.proposal_id, false)
                .await
                .error_code
                == 0
        );
    }
    cluster
        .broker
        .wait_for_metrics("two approvals move it to approved", |m| {
            proposals(m, BreakGlassState::Approved) == 1
                && proposals(m, BreakGlassState::Pending) == 0
        })
        .await;

    let thaw = SignedFreeze {
        frozen: false,
        reason: THAW_REASON,
        proposal_id: uuid::Uuid::from_bytes(proposed.proposal_id.0),
        set_at_ms: now_ms().max(set_at_ms + 1),
        ..freeze
    };
    check!(send_signed_freeze(&alice, &thaw).await.error_code == 0);
    cluster
        .broker
        .wait_for_metrics("the thaw drops the gauge and spends the proposal", |m| {
            m.topic_freezes_active.get() == 0 && proposals(m, BreakGlassState::Consumed) == 1
        })
        .await;

    // A gated Kafka transition with no approval behind it.
    let created = alice
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "doomed".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    check!(created.topics[0].error_code == 0);

    let before = refusals(
        cluster.broker.metrics(),
        krabka_metadata::BreakGlassAction::DeleteTopic,
    );
    let deleted = alice
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some("doomed".into()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    check!(deleted.responses[0].error_code == krabka_broker::codes::POLICY_VIOLATION);
    check!(
        refusals(
            cluster.broker.metrics(),
            krabka_metadata::BreakGlassAction::DeleteTopic
        ) == before + 1,
        "the refusal counter did not move"
    );
    support::wait_for_audit_record(&alice, "the refused deletion", |j| {
        j["api"]["operation"] == "delete_topic.refused" && j["status_id"] == 2
    })
    .await;

    cluster.broker.shutdown().await;
}
