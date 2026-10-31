//! End-to-end tests for the `InitProducerId` handler entry point.
//!
//! They drive a live broker, because the transactional path only becomes
//! reachable once the coordinator owns the `__transaction_state` partition for
//! the transactional id. KIP-939 needs the broker config
//! `transaction.two.phase.commit.enable`, not a `transaction.version` level.
//! Keeping them out of the module root leaves the request flow readable.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::{handle as init_producer_id_handle, identity::ProducerIdentity, *},
    crate::{
        api_catalog::UnstableApiVersions,
        authorizer::SimpleAclAuthorizer,
        broker::{Broker, BrokerHandle},
        error::BrokerError,
        partition_registry::PartitionRegistry,
        producer_id_manager::ProducerIdManager,
        handlers::{
            acl_test_support as acl_fixtures, acl_wire, context::RequestContext,
            find_coordinator::handle as find_coordinator_handle,
        },
        test_support::{
            ControllerPeerAllowed, start_broker_no_audit, start_broker_no_audit_with,
            start_broker_with_authorizer_no_audit, wait_for_controller_leader,
        },
        txn::{
            coordinator::TxnCoordinator,
            handlers::end_txn::handle as end_txn_handle,
            state::{TxnEntry, TxnState},
            version::{TxnVersion, resolve_txn_version},
        },
    },
    assert2::assert,
    krabka_metadata::{
        AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
        transaction_version,
    },
    krabka_protocol::owned::{
        end_txn_request::EndTxnRequest,
        end_txn_response::{self, EndTxnResponse},
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{self, FindCoordinatorResponse},
        init_producer_id_response,
    },
    krabka_units::secs,
    std::{collections::HashSet, sync::Arc},
    tokio::sync::Mutex,
};

/// Persist the held transaction as ongoing after releasing its entry lock.
async fn persist_ongoing(
    coordinator: &TxnCoordinator,
    cell: &Arc<Mutex<TxnEntry>>,
    expectation: &str,
) -> ProducerIdentity {
    let (producer_id, producer_epoch, snapshot) = {
        let mut entry = cell.lock().await;
        entry.state = TxnState::Ongoing;
        (entry.producer_id, entry.producer_epoch, entry.clone())
    };
    coordinator
        .put(snapshot, TxnVersion::Verified)
        .await
        .expect(expectation);
    ProducerIdentity {
        id: producer_id,
        epoch: producer_epoch,
    }
}

/// Waits for the controller, and checks the cluster finalized `TV_2`, the
/// highest `transaction.version` Kafka defines, which a self-bootstrapped
/// broker finalizes. KIP-939 needs no higher level.
async fn wait_for_transaction_version_2(broker_handle: &BrokerHandle) {
    wait_for_controller_leader(&broker_handle.broker_arc_for_test()).await;
    broker_handle
        .wait_for_image(|image| {
            image.finalized_feature(transaction_version::TRANSACTION_VERSION_FEATURE)
                == Some(transaction_version::TRANSACTION_VERSION_MAX)
        })
        .await;
    let image = broker_handle
        .broker_arc_for_test()
        .controller
        .current_image();
    assert!(resolve_txn_version(&image) == TxnVersion::Verified);
}

async fn wait_for_transaction_v2_coordinator(handle: &BrokerHandle) {
    wait_for_transaction_version_2(handle).await;
    handle.wait_until_transaction_coordinator_ready().await;
}

/// The KIP-939 test configuration precedes coordinator readiness at each caller.
async fn transaction_v2_broker() -> HandlerStartedBroker {
    start_broker_no_audit_with(|config| {
        config.transaction_state_num_partitions = 7;
        config.transaction_max_timeout = secs(8);
        config.features.transaction_two_phase_commit_enable = true;
        config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    })
    .await
}
#[derive(Default)]
struct TransactionInitializationScenario {
    started: Option<HandlerStartedBroker>,
    transaction: String,
    initialized: Option<InitProducerIdResponse>,
    ongoing: Option<ProducerIdentity>,
    recovered: Vec<InitProducerIdResponse>,
    completed: Vec<EndTxnResponse>,
}
impl TransactionInitializationScenario {
    fn initialization_request(
        &self,
        transaction_timeout_ms: i32,
        enable2_pc: bool,
        keep_prepared_txn: bool,
    ) -> InitProducerIdRequest {
        InitProducerIdRequest {
            transactional_id: Some(self.transaction.clone()),
            transaction_timeout_ms,
            enable2_pc,
            keep_prepared_txn,
            ..Default::default()
        }
    }

    fn coordinator_broker(&self) -> Arc<Broker> {
        self.started
            .as_ref()
            .expect("coordinator")
            .broker
            .broker_arc_for_test()
    }
}
#[rstest::fixture]
fn transaction_initialization() -> TransactionInitializationScenario {
    let started = None;
    TransactionInitializationScenario {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a transaction-version-2 coordinator with two-phase commit enabled and 8000ms maximum timeout owns {transaction}"
)]
async fn transaction_initialization_coordinator(
    transaction_initialization: &mut TransactionInitializationScenario,
    transaction: String,
) {
    let started = transaction_v2_broker().await;
    wait_for_transaction_v2_coordinator(&started.broker).await;
    let broker = started.broker.broker_arc_for_test();
    krabka_macros::request_test_identity! { bind(principal, peer, context); user ["admin"]; client ["txn-client"]; };
    let request = FindCoordinatorRequest {
        key_type: 1,
        coordinator_keys: vec![transaction.clone()],
        ..Default::default()
    };
    let response = find_coordinator_handle(
        &broker,
        request,
        find_coordinator_response::MAX_VERSION,
        &context,
    )
    .await
    .expect("find transaction coordinator");
    assert!(
        response
            .coordinators
            .iter()
            .all(|coordinator| coordinator.error_code == codes::NONE)
    );
    transaction_initialization.started = Some(started);
    transaction_initialization.transaction = transaction;
}
#[rstest_bdd_macros::when(
    "InitProducerId requests timeout {timeout} with two-phase commit {two_phase}"
)]
async fn initialize_transaction_identity(
    transaction_initialization: &mut TransactionInitializationScenario,
    timeout: i32,
    two_phase: bool,
) {
    let broker = transaction_initialization.coordinator_broker();
    krabka_macros::request_test_identity! { bind(principal, peer, context); user ["admin"]; client ["txn-client"]; };
    let request = transaction_initialization.initialization_request(timeout, two_phase, false);
    transaction_initialization.initialized = Some(
        init_producer_id_handle(
            &broker,
            request,
            init_producer_id_response::MAX_VERSION,
            &context,
        )
        .await
        .expect("initialize transaction"),
    );
}
#[rstest_bdd_macros::then(
    "the complete initialization response has code {code} and persisted timeout {timeout}"
)]
async fn transaction_initialization_timeout(
    transaction_initialization: &mut TransactionInitializationScenario,
    code: i16,
    timeout: String,
) {
    let response = transaction_initialization
        .initialized
        .as_ref()
        .expect("initialization response");
    let broker = transaction_initialization.coordinator_broker();
    if code == codes::NONE {
        assert!(
            response
                == &InitProducerIdResponse {
                    producer_id: response.producer_id,
                    producer_epoch: 0,
                    ..Default::default()
                }
        );
        let entry = broker
            .txn_coordinator
            .get(&transaction_initialization.transaction)
            .expect("persisted transaction");
        assert!(
            entry.lock().await.txn_timeout_ms == timeout.parse::<i32>().expect("stored timeout")
        );
    } else {
        assert!(
            response
                == &InitProducerIdResponse {
                    error_code: code,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }
        );
        assert!(
            broker
                .txn_coordinator
                .get(&transaction_initialization.transaction)
                .is_none()
        );
    }
    transaction_initialization
        .started
        .take()
        .expect("coordinator")
        .broker
        .shutdown()
        .await;
}
#[rstest_bdd_macros::given(
    "the 500ms two-phase transaction has a persisted ongoing producer identity"
)]
async fn ongoing_two_phase_transaction(
    transaction_initialization: &mut TransactionInitializationScenario,
) {
    initialize_transaction_identity(transaction_initialization, 500, true).await;
    assert!(
        transaction_initialization
            .initialized
            .as_ref()
            .expect("initialized transaction")
            .error_code
            == codes::NONE
    );
    let broker = transaction_initialization.coordinator_broker();
    let ongoing = broker
        .txn_coordinator
        .get(&transaction_initialization.transaction)
        .expect("two-phase transaction entry");
    transaction_initialization.ongoing = Some(
        persist_ongoing(
            &broker.txn_coordinator,
            &ongoing,
            "persist ongoing two-phase transaction",
        )
        .await,
    );
}
#[rstest_bdd_macros::when(
    "two clients recover the prepared transaction with a 500ms timeout and keepPreparedTxn enabled"
)]
async fn recover_two_phase_transaction(
    transaction_initialization: &mut TransactionInitializationScenario,
) {
    let broker = transaction_initialization.coordinator_broker();
    krabka_macros::request_test_identity! { bind(principal, peer, context); user ["admin"]; client ["txn-client"]; };
    let request = transaction_initialization.initialization_request(500, true, true);
    for _ in 0..2 {
        transaction_initialization.recovered.push(
            init_producer_id_handle(
                &broker,
                request.clone(),
                init_producer_id_response::MAX_VERSION,
                &context,
            )
            .await
            .expect("recover prepared transaction"),
        );
    }
}
#[rstest_bdd_macros::then(
    "both recoveries retain the ongoing identity and the second recovery increments the recovery producer epoch"
)]
fn recovered_two_phase_identities(transaction_initialization: &TransactionInitializationScenario) {
    let [first, second] = transaction_initialization.recovered.as_slice() else {
        panic!("two recovery responses required")
    };
    let ongoing = transaction_initialization
        .ongoing
        .as_ref()
        .expect("ongoing producer identity");
    assert!(first.error_code == codes::NONE);
    assert!(first.ongoing_txn_producer_id == ongoing.id.get());
    assert!(first.ongoing_txn_producer_epoch == ongoing.epoch);
    assert!(second.error_code == codes::NONE);
    assert!(second.producer_id == first.producer_id);
    assert!(second.producer_epoch == first.producer_epoch + 1);
    assert!(second.ongoing_txn_producer_id == ongoing.id.get());
    assert!(second.ongoing_txn_producer_epoch == ongoing.epoch);
}
#[rstest_bdd_macros::when(
    "the older recovery client commits, then the current recovery client commits and retries"
)]
async fn complete_recovered_transaction(
    transaction_initialization: &mut TransactionInitializationScenario,
) {
    let broker = transaction_initialization.coordinator_broker();
    krabka_macros::request_test_identity! { bind(principal, peer, context); user ["admin"]; client ["txn-client"]; };
    let [first, second] = transaction_initialization.recovered.as_slice() else {
        panic!("two recovery responses required")
    };
    for identity in [first, second, second] {
        let request = EndTxnRequest {
            transactional_id: transaction_initialization.transaction.clone(),
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            committed: true,
            ..Default::default()
        };
        transaction_initialization.completed.push(
            end_txn_handle(&broker, request, end_txn_response::MAX_VERSION, &context)
                .await
                .expect("complete recovered transaction"),
        );
    }
}
#[rstest_bdd_macros::then(
    "the older client is fenced, the current commit is idempotent and the persisted state is CompleteCommit without a next producer identity"
)]
async fn completed_recovered_transaction(
    transaction_initialization: &mut TransactionInitializationScenario,
) {
    let [fenced, completed, retry] = transaction_initialization.completed.as_slice() else {
        panic!("three commit responses required")
    };
    assert!(
        fenced
            == &krabka_macros::unthrottled_wire!(EndTxnResponse {
                error_code: codes::PRODUCER_FENCED,
                producer_id: -1,
                producer_epoch: -1
            })
    );
    let identity = &transaction_initialization.recovered[1];
    assert!(
        completed
            == &krabka_macros::unthrottled_wire!(EndTxnResponse {
                error_code: codes::NONE,
                producer_id: identity.producer_id,
                producer_epoch: identity.producer_epoch + 1
            })
    );
    assert!(retry == completed);
    let broker = transaction_initialization.coordinator_broker();
    let entry = broker
        .txn_coordinator
        .get(&transaction_initialization.transaction)
        .expect("completed transaction");
    {
        let state = entry.lock().await;
        assert!(state.state == TxnState::CompleteCommit);
        assert!(state.next_producer_id.is_none());
        assert!(state.next_producer_epoch == -1);
        drop(state);
    }
    transaction_initialization
        .started
        .take()
        .expect("coordinator")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_transaction_initialization.feature",
    name = "Positive client timeout is stored unchanged"
)]
#[tokio::test]
async fn transaction_initialization_case_0(
    transaction_initialization: TransactionInitializationScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_transaction_initialization.feature",
    name = "Timeout above the broker maximum is refused"
)]
#[tokio::test]
async fn transaction_initialization_case_1(
    transaction_initialization: TransactionInitializationScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_transaction_initialization.feature",
    name = "Two-phase commit stores the sentinel timeout"
)]
#[tokio::test]
async fn transaction_initialization_case_2(
    transaction_initialization: TransactionInitializationScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_transaction_initialization.feature",
    name = "Zero timeout is refused"
)]
#[tokio::test]
async fn transaction_initialization_case_3(
    transaction_initialization: TransactionInitializationScenario,
) {
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_transaction_initialization.feature",
    name = "Prepared recovery fences stale identities and commits idempotently"
)]
#[tokio::test]
async fn transaction_initialization_case_4(
    transaction_initialization: TransactionInitializationScenario,
) {
}

/// krabka-io/krabka-broker#784: Kafka 4.3.1 gates KIP-939 on
/// `transaction.two.phase.commit.enable`, not on a `transaction.version`
/// level. With the config off (its default),
/// `TransactionCoordinator.handleInitProducerId` answers `enable2Pc` with
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` and `keepPreparedTxn` with
/// `UNSUPPORTED_VERSION`. At `TV_2`, with the config on, neither is refused on
/// version grounds, and a broker that coordinates nothing answers the
/// coordinator check.
#[tokio::test]
async fn kip939_fields_follow_the_two_phase_commit_config_at_transaction_version_2() {
    use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};
    let refused = |error_code| InitProducerIdResponse {
        error_code,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    // `keepPreparedTxn` is refused as Kafka 4.3.1 refuses it unless 2PC and
    // `unstable.api.versions.enable` are both on (#784).
    for (two_phase_commit, unstable, enable_2pc, keep_prepared_txn, want) in [
        (
            false,
            Enabled,
            true,
            false,
            refused(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        ),
        (
            false,
            Enabled,
            false,
            true,
            refused(codes::UNSUPPORTED_VERSION),
        ),
        (true, Disabled, true, false, refused(codes::NOT_COORDINATOR)),
        (
            true,
            Disabled,
            false,
            true,
            refused(codes::UNSUPPORTED_VERSION),
        ),
        (true, Enabled, true, false, refused(codes::NOT_COORDINATOR)),
        (true, Enabled, false, true, refused(codes::NOT_COORDINATOR)),
    ] {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker_no_audit_with(|config| {
            config.features.transaction_two_phase_commit_enable = two_phase_commit;
            config.features.unstable_api_versions = unstable;
        })
        .await;
        wait_for_transaction_version_2(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(principal, peer, context);
            user ["admin"];
            client ["txn-client"];
        };
        let version = init_producer_id_response::MAX_VERSION;
        // No FindCoordinator ran, so this broker coordinates nothing.
        let request = InitProducerIdRequest {
            transactional_id: Some("txn-tv2".to_owned()),
            transaction_timeout_ms: 500,
            enable2_pc: enable_2pc,
            keep_prepared_txn,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("answer the KIP-939 request");
        assert!(
            response == want,
            "config {two_phase_commit}, {unstable:?}, enable2Pc {enable_2pc}, \
             keepPreparedTxn {keep_prepared_txn}: {response:?}"
        );
        broker_handle.shutdown().await;
    }
}

#[tokio::test]
async fn keep_prepared_txn_without_enable_2pc_preserves_finite_timeout() {
    krabka_macros::broker_test_setup!(
        bind(broker_handle, _dir, broker);
        boot [transaction_v2_broker()];
    );
    wait_for_transaction_v2_coordinator(&broker_handle).await;
    krabka_macros::request_test_identity! {
        bind(principal, peer, context);
        user ["admin"];
        client ["txn-client"];
    };
    let tid = "txn-recover-finite";

    let response = find_transaction_coordinator(&broker, &context, tid)
        .await
        .expect("find transaction coordinator");
    assert!(response.coordinators[0].error_code == codes::NONE);

    let version = init_producer_id_response::MAX_VERSION;
    let request = InitProducerIdRequest {
        transactional_id: Some(tid.to_owned()),
        transaction_timeout_ms: 500,
        ..Default::default()
    };
    let response = init_producer_id_handle(&broker, request, version, &context)
        .await
        .expect("initialize finite-timeout transaction");
    assert!(response.error_code == codes::NONE);

    let finite = broker
        .txn_coordinator
        .get(tid)
        .expect("finite-timeout transaction entry");
    let ProducerIdentity {
        id: finite_pid,
        epoch: finite_epoch,
    } = persist_ongoing(
        &broker.txn_coordinator,
        &finite,
        "persist finite ongoing transaction",
    )
    .await;

    let recovery_request = InitProducerIdRequest {
        transactional_id: Some(tid.to_owned()),
        transaction_timeout_ms: 500,
        keep_prepared_txn: true,
        ..Default::default()
    };
    let response = init_producer_id_handle(&broker, recovery_request, version, &context)
        .await
        .expect("recover finite-timeout transaction without enable2Pc");
    assert!(response.error_code == codes::NONE);
    assert!(response.ongoing_txn_producer_id == finite_pid.get());
    assert!(response.ongoing_txn_producer_epoch == finite_epoch);
    assert!(finite.lock().await.txn_timeout_ms == 500);
    broker_handle.shutdown().await;
}

/// Kafka validates the timeout in `TransactionCoordinator.handleInitProducerId`
/// before it looks the coordinator up, so a broker that does not coordinate the
/// id answers `INVALID_TRANSACTION_TIMEOUT` too.
#[tokio::test]
async fn the_timeout_check_runs_before_the_coordinator_check() {
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_no_audit_with(|config| config.transaction_max_timeout = secs(8)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_transaction_version_2(&broker_handle).await;
    krabka_macros::request_test_identity! {
        bind(principal, peer, context);
        user ["admin"];
        client ["txn-client"];
    };
    let version = init_producer_id_response::MAX_VERSION;
    // No FindCoordinator ran, so `__transaction_state` does not exist and this
    // broker coordinates nothing.
    let tid = "txn-no-coordinator";

    let answers = [
        (
            "a valid timeout reaches the coordinator check",
            5_000,
            codes::NOT_COORDINATOR,
        ),
        (
            "an invalid timeout answers before it",
            9_000,
            codes::INVALID_TRANSACTION_TIMEOUT,
        ),
    ];
    for (name, requested_ms, expected) in answers {
        let request = InitProducerIdRequest {
            transactional_id: Some(tid.to_owned()),
            transaction_timeout_ms: requested_ms,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("initialize transactional producer");
        assert!(
            response
                == InitProducerIdResponse {
                    error_code: expected,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                },
            "{name}: {response:?}"
        );
    }
    broker_handle.shutdown().await;
}

/// Kafka `KafkaApis.handleInitProducerIdRequest` refuses half an identity with
/// `INVALID_REQUEST`, and answers a client below version 4 with
/// `INVALID_PRODUCER_EPOCH` in place of `PRODUCER_FENCED`.
#[tokio::test]
async fn half_an_identity_is_invalid_and_an_old_client_gets_invalid_producer_epoch() {
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_transaction_v2_coordinator(&broker_handle).await;
    krabka_macros::request_test_identity! {
        bind(principal, peer, context);
        user ["admin"];
        client ["txn-client"];
    };
    let tid = "txn-half-identity";
    let max = init_producer_id_response::MAX_VERSION;
    let found = find_transaction_coordinator(&broker, &context, tid)
        .await
        .expect("find the transaction coordinator");
    assert!(found.coordinators[0].error_code == codes::NONE);
    // An entry to be fenced against.
    let created = init_producer_id_handle(
        &broker,
        InitProducerIdRequest {
            transactional_id: Some(tid.to_owned()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        },
        max,
        &context,
    )
    .await
    .expect("create the transaction entry");
    assert!(created.error_code == codes::NONE, "{created:?}");
    let zombie = (created.producer_id + 1_000, created.producer_epoch);

    // (name, version, transactional id, request identity, expected code)
    let cases = [
        (
            "a producer id with no epoch",
            max,
            Some(tid),
            (7, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "an epoch with no producer id",
            max,
            Some(tid),
            (-1, 3),
            codes::INVALID_REQUEST,
        ),
        (
            "an idempotent producer with an epoch and no producer id",
            max,
            None,
            (-1, 5),
            codes::INVALID_REQUEST,
        ),
        (
            "an idempotent producer with a producer id and no epoch",
            max,
            None,
            (42, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "an empty transactional id with half an identity",
            max,
            Some(""),
            (42, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "a fenced identity at the newest version",
            max,
            Some(tid),
            zombie,
            codes::PRODUCER_FENCED,
        ),
        (
            "a fenced identity below version 4",
            3,
            Some(tid),
            zombie,
            codes::INVALID_PRODUCER_EPOCH,
        ),
    ];
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for (name, version, transactional_id, (producer_id, producer_epoch), code) in cases {
        let request = InitProducerIdRequest {
            transactional_id: transactional_id.map(str::to_owned),
            transaction_timeout_ms: 60_000,
            producer_id,
            producer_epoch,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("initialize transactional producer");
        expected.push((name, InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: code,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        }));
        actual.push((name, response));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

/// A stripped-down [`AclEntry`] builder for the ACL-preamble table below: only
/// the resource type/name/pattern, operation, and Allow/Deny vary per row.
fn acl(
    permission_type: PermissionType,
    resource_type: ResourceType,
    resource_name: &str,
    pattern_type: PatternType,
    operation: AclOperation,
) -> AclEntry {
    AclEntry {
        resource_type,
        resource_name: resource_name.into(),
        pattern_type,
        principal: "User:alice".into(),
        host: "*".into(),
        operation,
        permission_type,
    }
}

/// #685: the null-transactional-id and empty-transactional-id branches of the
/// `InitProducerId` ACL preamble, matching Kafka's
/// `KafkaApis.handleInitProducerIdRequest`.
///
/// Neither branch reaches the transaction coordinator (null allocates
/// directly; empty is rejected before dispatch), so each case starts its own
/// broker with only the authorizer configured, with no coordinator bring-up.
#[tokio::test]
async fn acl_preamble_for_null_and_empty_transactional_id() {
    struct Case {
        label: &'static str,
        transactional_id: Option<&'static str>,
        acls: Vec<AclEntry>,
        expected_error: i16,
    }

    let cases = [
        Case {
            label: "null id: cluster IdempotentWrite allows",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Cluster,
                acl_wire::CLUSTER_RESOURCE_NAME,
                PatternType::Literal,
                AclOperation::IdempotentWrite,
            )],
            expected_error: codes::NONE,
        },
        Case {
            label: "null id: literal Write on one topic allows",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                AclOperation::Write,
            )],
            expected_error: codes::NONE,
        },
        Case {
            label: "null id: prefixed Write on topics allows",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Topic,
                "ord",
                PatternType::Prefixed,
                AclOperation::Write,
            )],
            expected_error: codes::NONE,
        },
        Case {
            label: "null id: topic Write fully covered by a Deny on Topic \"*\" denies",
            transactional_id: None,
            acls: vec![
                acl(
                    PermissionType::Allow,
                    ResourceType::Topic,
                    "orders",
                    PatternType::Literal,
                    AclOperation::Write,
                ),
                acl(
                    PermissionType::Deny,
                    ResourceType::Topic,
                    "*",
                    PatternType::Literal,
                    AclOperation::Write,
                ),
            ],
            expected_error: codes::CLUSTER_AUTHORIZATION_FAILED,
        },
        Case {
            label: "null id: Read (not Write) on a topic denies",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                AclOperation::Read,
            )],
            expected_error: codes::CLUSTER_AUTHORIZATION_FAILED,
        },
        Case {
            label: "empty id: no ACLs denies with TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            transactional_id: Some(""),
            acls: vec![],
            expected_error: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        },
        Case {
            label: "empty id: Write on TransactionalId \"*\" passes the ACL gate, \
                     but the coordinator still rejects the empty id",
            transactional_id: Some(""),
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::TransactionalId,
                "*",
                PatternType::Literal,
                AclOperation::Write,
            )],
            expected_error: codes::INVALID_REQUEST,
        },
    ];

    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for case in cases {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker_with_authorizer_no_audit(Arc::new(SimpleAclAuthorizer::new(
                        HashSet::new()
                    ),))];
        };
        if !case.acls.is_empty() {
            acl_fixtures::seed_case_acls(&broker, case.acls).await;
        }

        krabka_macros::request_test_identity! {
            bind(principal, peer, context);
            user ["alice"];
            client ["idempotent-client"];
        };
        let version = init_producer_id_response::MAX_VERSION;
        let request = InitProducerIdRequest {
            transactional_id: case.transactional_id.map(str::to_owned),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("handle InitProducerId");

        expected.push((case.label, case.expected_error));
        actual.push((case.label, response.error_code));
        if case.expected_error != codes::NONE {
            assert!(response.producer_id == -1, "{}", case.label);
        }
        broker_handle.shutdown().await;
    }
    assert!(actual == expected);
}

/// #685: `keep_prepared_txn` alone must not run the KIP-939 two-phase-commit
/// gate -- only `enable_2pc` does. Both cases share a Write ACL on the
/// transactional id (which the ACL preamble above already covers) and differ
/// only in which KIP-939 flag is set and whether a `TwoPhaseCommit` ACL
/// exists.
#[tokio::test]
async fn two_phase_commit_gate_is_scoped_to_enable_2pc_not_keep_prepared_txn() {
    struct Case {
        label: &'static str,
        tid: &'static str,
        enable_2pc: bool,
        keep_prepared_txn: bool,
        two_phase_commit_acl: bool,
        // `None` means "any success code (not TRANSACTIONAL_ID_AUTHORIZATION_FAILED)".
        expected_error: Option<i16>,
    }

    let cases = [
        Case {
            label: "keep_prepared_txn alone skips the 2PC ACL/config gate",
            tid: "tx-keep-only",
            enable_2pc: false,
            keep_prepared_txn: true,
            two_phase_commit_acl: false,
            expected_error: None,
        },
        Case {
            label: "enable_2pc without a TwoPhaseCommit ACL is denied",
            tid: "tx-enable-2pc",
            enable_2pc: true,
            keep_prepared_txn: false,
            two_phase_commit_acl: false,
            expected_error: Some(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        },
    ];

    for case in cases {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker_no_audit_with(|config| {
            config.transaction_state_num_partitions = 7;
            config.transaction_max_timeout = secs(8);
            config.features.transaction_two_phase_commit_enable = true;
            config.features.unstable_api_versions = UnstableApiVersions::Enabled;
            config.authorizer = Arc::new(ControllerPeerAllowed(SimpleAclAuthorizer::new(
                HashSet::new(),
            )));
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_transaction_v2_coordinator(&broker_handle).await;

        let mut acls = vec![acl(
            PermissionType::Allow,
            ResourceType::TransactionalId,
            case.tid,
            PatternType::Literal,
            AclOperation::Write,
        )];
        if case.two_phase_commit_acl {
            acls.push(acl(
                PermissionType::Allow,
                ResourceType::TransactionalId,
                case.tid,
                PatternType::Literal,
                AclOperation::TwoPhaseCommit,
            ));
        }
        broker
            .controller
            .submit_change(
                acls.into_iter()
                    .map(MetadataRecord::V1AccessControlEntry)
                    .collect(),
            )
            .await
            .expect("seed acls");

        krabka_macros::request_test_identity! {
            bind(principal, peer, context);
            user ["alice"];
            client ["txn-client"];
        };

        let find_response = find_transaction_coordinator(&broker, &context, case.tid)
            .await
            .expect("find transaction coordinator");
        assert!(
            find_response.coordinators[0].error_code == codes::NONE,
            "{}",
            case.label
        );

        let version = init_producer_id_response::MAX_VERSION;
        let request = InitProducerIdRequest {
            transactional_id: Some(case.tid.to_owned()),
            transaction_timeout_ms: 500,
            enable2_pc: case.enable_2pc,
            keep_prepared_txn: case.keep_prepared_txn,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("handle InitProducerId");

        match case.expected_error {
            Some(code) => assert!(response.error_code == code, "{}", case.label),
            None => assert!(
                response.error_code != codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                "{}: {:?}",
                case.label,
                response
            ),
        }
        broker_handle.shutdown().await;
    }
}

/// #1015: Kafka's `RPCProducerIdManager.generateProducerId` throws
/// `COORDINATOR_LOAD_IN_PROGRESS` while it has no producer ID block, and
/// `TransactionCoordinator.handleInitProducerId` answers that code for an
/// idempotent producer and for a new transactional id alike. The client
/// retries; the broker keeps the connection.
#[tokio::test]
async fn a_failed_block_allocation_answers_coordinator_load_in_progress() {
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_controller_leader(&broker).await;
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;
    krabka_macros::request_test_identity! {
        bind(principal, peer, context);
        user ["admin"];
        client ["txn-client"];
    };
    let tid = "txn-no-block";

    let find_response = find_transaction_coordinator(&broker, &context, tid)
        .await
        .expect("find transaction coordinator");
    assert!(
        find_response
            .coordinators
            .iter()
            .all(|coordinator| coordinator.error_code == codes::NONE)
    );

    // The last frontier that cannot fit another block: the controller refuses
    // every allocation from here on.
    let broker_epoch = broker
        .controller
        .current_image()
        .broker_epoch(broker.config.node_id)
        .expect("registered broker epoch");
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1ProducerIds(
            krabka_metadata::ProducerIdsRecord {
                broker_id: broker.config.node_id,
                broker_epoch,
                next_producer_id: i64::MAX - 999,
            },
        )])
        .await
        .expect("seed exhausted producer ID space");

    let version = init_producer_id_response::MAX_VERSION;
    let cases = [
        ("idempotent producer", None),
        ("new transactional id", Some(tid)),
    ];
    for (name, transactional_id) in cases {
        let request = InitProducerIdRequest {
            transactional_id: transactional_id.map(str::to_owned),
            transaction_timeout_ms: 5_000,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("answered, not a closed connection");
        assert!(
            response
                == InitProducerIdResponse {
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                },
            "{name}"
        );
    }
    broker_handle.shutdown().await;
}

/// Route one transaction id through the real coordinator handler at its latest version.
async fn find_transaction_coordinator(
    broker: &Broker,
    context: &RequestContext<'_>,
    tid: &str,
) -> Result<FindCoordinatorResponse, BrokerError> {
    find_coordinator_handle(
        broker,
        FindCoordinatorRequest {
            key_type: 1,
            coordinator_keys: vec![tid.to_owned()],
            ..Default::default()
        },
        find_coordinator_response::MAX_VERSION,
        context,
    )
    .await
}

/// A coordinator with no `__transaction_state` partition for the id answers
/// `COORDINATOR_NOT_AVAILABLE`, at every request version. Config validation
/// rejects a non-positive partition count, so a started broker never gets
/// here, and the coordinator is built directly.
#[test]
fn a_coordinator_without_state_partitions_is_not_available() {
    let coordinator = TxnCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::new(PartitionRegistry::new()),
        Arc::new(ProducerIdManager::new()),
        0,
        krabka_units::mebibytes(1),
    );
    for version in [0, init_producer_id_response::MAX_VERSION] {
        assert!(
            coordinator_partition(&coordinator, "tid", version)
                == Err(InitProducerIdResponse {
                    error_code: codes::COORDINATOR_NOT_AVAILABLE,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }),
            "version {version}"
        );
    }
}
