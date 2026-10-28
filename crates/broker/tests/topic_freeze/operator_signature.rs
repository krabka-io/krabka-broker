//! The operator signature, from the command that signs it to the registry entry
//! an auditor re-verifies after a restart.
//!
//! `freeze.require_signature` governs the freeze direction alone: a freeze is
//! the safe direction and has to be reachable in one command, while a thaw
//! always demands the proof. These cases hold that asymmetry in place, and they
//! hold the replay defence with it -- a signature captured from a freeze must
//! not authorize the thaw that lifts it.

use {
    crate::{
        control_plane::{cluster_id, freeze_request, set_freeze, wait_for_registry_len},
        signing::{SignedFreeze, signed_request, verifies_locally},
        support::{
            self, OperatorKey,
            client::{BrokerWithClient, connect_to_broker},
        },
        wire::{
            CONTROL, ProduceOutcome, accepted, create_controlled_topic, create_topic, now_ms,
            produce_outcome, refused, wait_controlled_partitions,
        },
    },
    assert2::check,
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle, codes, operator_keys::OperatorKeys,
    },
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::freeze::{
            DescribedTopicFreeze, PATTERN_TYPE_LITERAL, SetTopicFreezeRequest,
            SetTopicFreezeResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::path::Path,
};

/// [`support::start_with_operator_key`] with `freeze.require_signature` on.
///
/// The harness helper takes the default, and a running broker's configuration
/// is not mutable, so the cases that need the strict setting build it here.
/// Everything else is what the harness builds: the same trust set, the same
/// single-approver break-glass set, the same plaintext listener.
async fn start_requiring_signatures(dir: &Path, key: &OperatorKey) -> BrokerWithClient {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = OperatorKeys::load(&[key.entry()]).expect("load the operator trust set");
    config.break_glass.approvers = vec![support::ANONYMOUS.to_owned()];
    config.freeze.require_signature = true;
    let broker = Broker::start(config).await.expect("broker start");
    let client = connect_to_broker(&broker).await;
    BrokerWithClient { broker, client }
}

/// A running broker, its client, the operator key it trusts and the ids of the
/// frozen and control topics.
struct OperatorFixture {
    broker: BrokerHandle,
    client: Client,
    key: OperatorKey,
    frozen: WireUuid,
    control: WireUuid,
}

async fn operator_fixture(keys: &Path, logs: &Path, require_signature: bool) -> OperatorFixture {
    let key = support::mint_operator_key(keys, "alice-yubi", support::ANONYMOUS);
    let BrokerWithClient { broker, client } = if require_signature {
        start_requiring_signatures(logs, &key).await
    } else {
        let started = support::start_with_operator_key(logs, &key).await;
        BrokerWithClient {
            broker: started.broker,
            client: started.client,
        }
    };
    let (frozen, control) = create_controlled_topic(&broker, &client, "orders").await;
    OperatorFixture {
        broker,
        client,
        key,
        frozen,
        control,
    }
}

/// The incident fixture shared by the signed freeze, replay and recovery cases.
fn incident_request(key: &OperatorKey, cluster: &str, set_at_ms: i64) -> SetTopicFreezeRequest {
    signed_request(&SignedFreeze {
        key,
        cluster_id: cluster,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: "orders",
        frozen: true,
        reason: "incident",
        set_at_ms,
        proposal_id: uuid::Uuid::nil(),
    })
}

/// The cluster id, signing time and signed freeze request of a fresh incident.
struct FreshIncident {
    cluster: String,
    set_at_ms: i64,
    request: SetTopicFreezeRequest,
}

async fn fresh_incident_request(client: &Client, key: &OperatorKey) -> FreshIncident {
    let cluster = cluster_id(client).await;
    let set_at_ms = now_ms();
    let request = incident_request(key, &cluster, set_at_ms);
    FreshIncident {
        cluster,
        set_at_ms,
        request,
    }
}

/// The signing keys directory and the persistent broker logs directory.
struct OperatorDirectories {
    keys: tempfile::TempDir,
    logs: tempfile::TempDir,
}

/// Keep the signing keys and persistent broker logs alive for the whole case.
fn operator_directories() -> OperatorDirectories {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    OperatorDirectories { keys, logs }
}

/// Both directories beside the operator fixture, in the original local drop
/// order.
struct OperatorCase {
    keys: tempfile::TempDir,
    logs: tempfile::TempDir,
    broker: BrokerHandle,
    client: Client,
    key: OperatorKey,
    frozen: WireUuid,
    control: WireUuid,
}

/// Own both directories beside the operator fixture, in the original local drop order.
async fn operator_case(require_signature: bool) -> OperatorCase {
    let OperatorDirectories { keys, logs } = operator_directories();
    let OperatorFixture {
        broker,
        client,
        key,
        frozen,
        control,
    } = operator_fixture(keys.path(), logs.path(), require_signature).await;
    OperatorCase {
        keys,
        logs,
        broker,
        client,
        key,
        frozen,
        control,
    }
}

#[derive(Default)]
struct OperatorScenario {
    case: Option<OperatorCase>,
    fresh: Option<FreshIncident>,
    response: Option<SetTopicFreezeResponse>,
    replays: Vec<(&'static str, SetTopicFreezeResponse)>,
    write: Option<ProduceOutcome>,
    control_write: Option<ProduceOutcome>,
    config: Option<BrokerConfig>,
    before: Vec<DescribedTopicFreeze>,
    after: Vec<DescribedTopicFreeze>,
    cluster_after: Option<String>,
}

impl OperatorScenario {
    fn case(&self) -> &OperatorCase { self.case.as_ref().expect("the operator broker is running") }

    async fn shutdown(self) {
        self.case
            .expect("the operator broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn operator() -> OperatorScenario {
    // Each scenario owns fresh services and observations.
    OperatorScenario {
        case: None,
        ..Default::default()
    }
}

#[given("an operator broker with require_signature set to {require_signature}")]
async fn signature_policy(operator: &mut OperatorScenario, require_signature: bool) {
    operator.case = Some(operator_case(require_signature).await);
}

#[given("the operator has already signed an incident freeze for orders")]
async fn signed_incident_fixture(operator: &mut OperatorScenario) {
    let fresh = fresh_incident_request(&operator.case().client, &operator.case().key).await;
    let response = set_freeze(&operator.case().client, fresh.request.clone()).await;
    check!(
        response.error_code == codes::NONE,
        "signed freeze: {response:?}"
    );
    wait_for_registry_len(&operator.case().client, 1).await;
    operator.fresh = Some(fresh);
}

#[given("a persistent operator broker has recorded a signed incident freeze")]
async fn persistent_signed_incident(operator: &mut OperatorScenario) {
    let OperatorDirectories { keys, logs } = operator_directories();
    let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
    let started = support::start_with_operator_key(logs.path(), &key).await;
    let broker = started.broker;
    let client = started.client;
    create_topic(&broker, &client, "orders").await;
    create_topic(&broker, &client, CONTROL).await;
    let fresh = fresh_incident_request(&client, &key).await;
    let response = set_freeze(&client, fresh.request.clone()).await;
    check!(
        response.error_code == codes::NONE,
        "signed freeze: {response:?}"
    );
    operator.before = wait_for_registry_len(&client, 1).await;
    operator.config = Some(started.config);
    operator.fresh = Some(fresh);
    operator.case = Some(OperatorCase {
        keys,
        logs,
        broker,
        client,
        key,
        frozen: WireUuid::ZERO,
        control: WireUuid::ZERO,
    });
}

#[when("the operator sends an unsigned incident freeze for orders")]
async fn unsigned_incident(operator: &mut OperatorScenario) {
    operator.response = Some(
        set_freeze(
            &operator.case().client,
            freeze_request(PATTERN_TYPE_LITERAL, "orders", "incident"),
        )
        .await,
    );
}

#[when("the operator submits a freshly signed incident freeze for orders")]
async fn signed_incident(operator: &mut OperatorScenario) {
    let fresh = fresh_incident_request(&operator.case().client, &operator.case().key).await;
    operator.response = Some(set_freeze(&operator.case().client, fresh.request.clone()).await);
    operator.fresh = Some(fresh);
}

#[when("an unsigned thaw attempts to lift the incident freeze")]
async fn unsigned_thaw(operator: &mut OperatorScenario) {
    operator.response = Some(
        set_freeze(&operator.case().client, SetTopicFreezeRequest {
            scope: "orders".to_owned(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: false,
            reason: "let me back in".to_owned(),
            proposal_id: WireUuid(*uuid::Uuid::new_v4().as_bytes()),
            set_at_ms: now_ms(),
            ..SetTopicFreezeRequest::default()
        })
        .await,
    );
}

#[then("the operator mutation returns Kafka error {code}")]
fn mutation_code(operator: &OperatorScenario, code: i16) {
    let response = operator
        .response
        .as_ref()
        .expect("an operator mutation was sent");
    check!(
        response.error_code == code,
        "operator mutation: {response:?}"
    );
}

#[then("the registry contains {count} incident freeze entries")]
async fn registry_size(operator: &OperatorScenario, count: usize) {
    wait_for_registry_len(&operator.case().client, count).await;
}

#[then("the incident registry entry preserves the signed fields and verifies locally")]
async fn signed_fields_round_trip(operator: &OperatorScenario) {
    let fresh = operator.fresh.as_ref().expect("the incident was signed");
    let case = operator.case();
    let entries = wait_for_registry_len(&case.client, 1).await;
    check!(
        entries[0]
            == DescribedTopicFreeze {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                reason: "incident".to_owned(),
                set_by: support::ANONYMOUS.to_owned(),
                set_at_ms: fresh.set_at_ms,
                proposal_id: WireUuid::ZERO,
                key_id: case.key.key_id.clone(),
                signature: fresh.request.signature.clone(),
                ..DescribedTopicFreeze::default()
            }
    );
    check!(verifies_locally(&case.key, &fresh.cluster, &entries[0]));
}

#[when("an attacker replays the freeze signature as a thaw at original and fresh timestamps")]
async fn replay_freeze_as_thaw(operator: &mut OperatorScenario) {
    let fresh = operator
        .fresh
        .as_ref()
        .expect("the signed incident was recorded");
    let captured = fresh.request.signature.clone();
    let times = [
        ("the otherwise identical thaw", fresh.set_at_ms),
        ("the same signature on a fresh timestamp", now_ms() + 1_000),
    ];
    for (label, timestamp) in times {
        let response = set_freeze(&operator.case().client, SetTopicFreezeRequest {
            scope: "orders".to_owned(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: false,
            reason: "incident".to_owned(),
            proposal_id: WireUuid::ZERO,
            set_at_ms: timestamp,
            key_id: operator.case().key.key_id.clone(),
            signature: captured.clone(),
            ..SetTopicFreezeRequest::default()
        })
        .await;
        operator.replays.push((label, response));
    }
}

#[then("both replayed thaws return OPERATOR_SIGNATURE_INVALID")]
fn replayed_signatures_invalid(operator: &OperatorScenario) {
    for (label, response) in &operator.replays {
        check!(
            response.error_code == codes::OPERATOR_SIGNATURE_INVALID,
            "{label}: {response:?}"
        );
    }
    check!(operator.replays.len() == 2);
}

#[then("the incident registry still protects the orders scope")]
async fn incident_still_registered(operator: &OperatorScenario) {
    check!(wait_for_registry_len(&operator.case().client, 1).await[0].scope == "orders");
}

#[when("the signed-freeze controller restarts in rejoin mode")]
async fn restart_signed_controller(operator: &mut OperatorScenario) {
    let OperatorCase {
        keys,
        logs,
        broker,
        client,
        key,
        ..
    } = operator
        .case
        .take()
        .expect("the operator broker is running");
    drop(client);
    broker.shutdown().await;
    let config = operator
        .config
        .as_mut()
        .expect("the persistent configuration was retained");
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = support::start_reusing_addrs(config, "the signed-freeze restart").await;
    let client = connect_to_broker(&broker).await;
    wait_controlled_partitions(&broker, "orders").await;
    operator.after = wait_for_registry_len(&client, 1).await;
    operator.cluster_after = Some(cluster_id(&client).await);
    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    operator.case = Some(OperatorCase {
        keys,
        logs,
        broker,
        client,
        key,
        frozen,
        control,
    });
}

#[then("the recovered entry and cluster identity are unchanged and the signature still verifies")]
fn recovered_signature_intact(operator: &OperatorScenario) {
    let cluster = &operator
        .fresh
        .as_ref()
        .expect("the incident was signed")
        .cluster;
    check!(operator.after == operator.before);
    check!(
        operator
            .cluster_after
            .as_ref()
            .expect("the controller rejoined")
            == cluster
    );
    check!(verifies_locally(
        &operator.case().key,
        cluster,
        &operator.after[0]
    ));
}

#[when("a client probes writes to incident topic orders")]
async fn probe_incident_write(operator: &mut OperatorScenario) {
    let case = operator.case();
    operator.write = Some(produce_outcome(&case.broker, &case.client, "orders", case.frozen).await);
}

#[then("the incident topic write is {outcome} at log end {offset}")]
fn incident_write_outcome(operator: &OperatorScenario, outcome: String, offset: i64) {
    let expected = match outcome.as_str() {
        "accepted" => accepted(offset),
        "refused" => refused("literal", "orders", "incident", offset),
        _ => panic!("unsupported incident write outcome {outcome}"),
    };
    check!(operator.write.as_ref().expect("orders was written") == &expected);
}

#[when("a client writes to the signed-freeze control topic")]
async fn probe_operator_control(operator: &mut OperatorScenario) {
    let case = operator.case();
    operator.control_write =
        Some(produce_outcome(&case.broker, &case.client, CONTROL, case.control).await);
}

#[then("the signed-freeze control accepts its first record")]
fn operator_control_accepts(operator: &OperatorScenario) {
    check!(
        operator
            .control_write
            .as_ref()
            .expect("control was written")
            == &accepted(1)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/operator_signature.feature",
    name = "require signature decides whether an unsigned freeze is accepted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn require_signature_decides_whether_an_unsigned_freeze_is_accepted(
    operator: OperatorScenario,
    require_signature: bool,
    code: i16,
    entries: usize,
    outcome: String,
    offset: i64,
) {
    let _ = (require_signature, code, entries, outcome, offset);
    operator.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/operator_signature.feature",
    name = "a signed freeze round trips with its key id and signature intact"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_freeze_round_trips_with_its_key_id_and_signature_intact(
    operator: OperatorScenario,
) {
    operator.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/operator_signature.feature",
    name = "an unsigned thaw is refused whatever require signature says"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsigned_thaw_is_refused_whatever_require_signature_says(
    operator: OperatorScenario,
    example: String,
    require_signature: bool,
) {
    let _ = (example, require_signature);
    operator.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/operator_signature.feature",
    name = "a signature captured from a freeze is refused as a thaw"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_captured_from_a_freeze_is_refused_as_a_thaw(operator: OperatorScenario) {
    operator.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/operator_signature.feature",
    name = "a signature survives a controller restart and still verifies"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_survives_a_controller_restart_and_still_verifies(operator: OperatorScenario) {
    operator.shutdown().await;
}
