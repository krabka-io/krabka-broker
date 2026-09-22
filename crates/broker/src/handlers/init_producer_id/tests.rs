//! End-to-end tests for the `InitProducerId` handler entry point.
//!
//! They drive a live broker, because the transactional path only becomes
//! reachable once the coordinator owns the `__transaction_state` partition for
//! the transactional id and the cluster has finalised `transaction.version` 3.
//! Keeping them out of the module root leaves the request flow readable.

use assert2::assert;
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_units::secs;

use super::*;
use crate::{
    test_support::{peer, principal, start_broker_with},
    txn::state::TxnState,
};

async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|node| node == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn enable_transaction_version_3(broker: &Broker) {
    wait_for_leader(broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE.into(),
            level: 3,
        })])
        .await
        .expect("enable transaction.version 3");
    assert!(
        broker
            .controller
            .current_image()
            .finalized_feature(krabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE)
            == Some(3)
    );
}

/// The timeout table of Kafka's `validateTransactionTimeoutMs`, checked
/// against a live broker whose `transaction.max.timeout.ms` is 8 s.
async fn check_timeout_answers(
    broker: &std::sync::Arc<Broker>,
    context: &crate::handlers::RequestContext<'_>,
    tids: [&str; 4],
) {
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    for (tid, requested_ms, enable_2pc, expected) in [
        (tids[0], 500, false, Ok(500)),
        (
            tids[1],
            10_000,
            false,
            Err(codes::INVALID_TRANSACTION_TIMEOUT),
        ),
        (tids[2], 500, true, Ok(i32::MAX)),
        (tids[3], 0, false, Err(codes::INVALID_TRANSACTION_TIMEOUT)),
    ] {
        let request = InitProducerIdRequest {
            transactional_id: Some(tid.to_string()),
            transaction_timeout_ms: requested_ms,
            enable2_pc: enable_2pc,
            ..Default::default()
        };
        let response = handle(
            broker,
            version,
            2,
            &crate::test_support::encode_request(&request, version),
            context,
        )
        .await
        .expect("initialize transactional producer");
        let response: InitProducerIdResponse =
            crate::test_support::decode_response(&response, version);
        match expected {
            Ok(expected_ms) => {
                assert!(
                    response
                        == InitProducerIdResponse {
                            producer_id: response.producer_id,
                            producer_epoch: 0,
                            ..Default::default()
                        },
                    "{tid}: {response:?}"
                );
                let entry = broker
                    .txn_coordinator
                    .get(tid)
                    .expect("persisted transaction entry");
                assert!(entry.lock().await.txn_timeout_ms == expected_ms, "{tid}");
            }
            Err(error_code) => {
                assert!(
                    response
                        == InitProducerIdResponse {
                            error_code,
                            producer_id: -1,
                            producer_epoch: -1,
                            ..Default::default()
                        },
                    "{tid}: {response:?}"
                );
                assert!(broker.txn_coordinator.get(tid).is_none(), "{tid}");
            }
        }
    }
}

#[tokio::test]
async fn handler_refuses_a_timeout_kafka_refuses_and_stores_the_rest_as_sent() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.transaction_state_num_partitions = 7;
        config.transaction_max_timeout = secs(8);
        config.features.transaction_two_phase_commit_enable = true;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("admin");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let tids = ["txn-small", "txn-above-max", "txn-2pc", "txn-zero"];

    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    enable_transaction_version_3(&broker).await;

    let find_version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let find_request = krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
        key_type: 1,
        coordinator_keys: tids.iter().map(ToString::to_string).collect(),
        ..Default::default()
    };
    let find_response = crate::handlers::find_coordinator::handle(
        &broker,
        find_version,
        1,
        &crate::test_support::encode_request(&find_request, find_version),
        &context,
    )
    .await
    .expect("find transaction coordinators");
    let find_response: krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse =
        crate::test_support::decode_response(&find_response, find_version);
    assert!(
        find_response
            .coordinators
            .iter()
            .all(|coordinator| coordinator.error_code == codes::NONE)
    );

    // Kafka `validateTransactionTimeoutMs`: a timeout above
    // `transaction.max.timeout.ms`, or one that is not positive, is refused.
    // Every other value is stored as the client sent it, and 2PC stores the
    // sentinel whatever the request asks for.
    check_timeout_answers(&broker, &context, tids).await;

    let ongoing = broker
        .txn_coordinator
        .get(tids[2])
        .expect("2PC transaction entry");
    let (ongoing_pid, ongoing_epoch, snapshot) = {
        let mut entry = ongoing.lock().await;
        entry.state = TxnState::Ongoing;
        (entry.producer_id, entry.producer_epoch, entry.clone())
    };
    broker
        .txn_coordinator
        .put(snapshot, crate::txn::version::TxnVersion::TwoPhase)
        .await
        .expect("persist ongoing 2PC transaction");

    let recovery_request = InitProducerIdRequest {
        transactional_id: Some(tids[2].to_string()),
        transaction_timeout_ms: 500,
        enable2_pc: true,
        keep_prepared_txn: true,
        ..Default::default()
    };
    let recovery_response = handle(
        &broker,
        version,
        3,
        &crate::test_support::encode_request(&recovery_request, version),
        &context,
    )
    .await
    .expect("recover prepared transaction");
    let recovery_response: InitProducerIdResponse =
        crate::test_support::decode_response(&recovery_response, version);
    assert!(recovery_response.error_code == codes::NONE);
    assert!(recovery_response.ongoing_txn_producer_id == ongoing_pid.get());
    assert!(recovery_response.ongoing_txn_producer_epoch == ongoing_epoch);

    let second_recovery_response = handle(
        &broker,
        version,
        4,
        &crate::test_support::encode_request(&recovery_request, version),
        &context,
    )
    .await
    .expect("recover prepared transaction again");
    let second_recovery_response: InitProducerIdResponse =
        crate::test_support::decode_response(&second_recovery_response, version);
    assert!(second_recovery_response.error_code == codes::NONE);
    assert!(second_recovery_response.producer_id == recovery_response.producer_id);
    assert!(second_recovery_response.producer_epoch == recovery_response.producer_epoch + 1);
    assert!(second_recovery_response.ongoing_txn_producer_id == ongoing_pid.get());
    assert!(second_recovery_response.ongoing_txn_producer_epoch == ongoing_epoch);

    let end_version = krabka_protocol::owned::end_txn_response::MAX_VERSION;
    let fenced_end_request = krabka_protocol::owned::end_txn_request::EndTxnRequest {
        transactional_id: tids[2].to_string(),
        producer_id: recovery_response.producer_id,
        producer_epoch: recovery_response.producer_epoch,
        committed: true,
        ..Default::default()
    };
    let fenced_end_response = crate::txn::handlers::end_txn::handle(
        &broker,
        end_version,
        5,
        &crate::test_support::encode_request(&fenced_end_request, end_version),
        &context,
    )
    .await
    .expect("reject fenced recovery client");
    let fenced_end_response: krabka_protocol::owned::end_txn_response::EndTxnResponse =
        crate::test_support::decode_response(&fenced_end_response, end_version);
    assert!(fenced_end_response.error_code == codes::INVALID_PRODUCER_EPOCH);

    let end_request = krabka_protocol::owned::end_txn_request::EndTxnRequest {
        transactional_id: tids[2].to_string(),
        producer_id: second_recovery_response.producer_id,
        producer_epoch: second_recovery_response.producer_epoch,
        committed: true,
        ..Default::default()
    };
    let end_response = crate::txn::handlers::end_txn::handle(
        &broker,
        end_version,
        6,
        &crate::test_support::encode_request(&end_request, end_version),
        &context,
    )
    .await
    .expect("complete recovered transaction");
    let end_response: krabka_protocol::owned::end_txn_response::EndTxnResponse =
        crate::test_support::decode_response(&end_response, end_version);
    assert!(end_response.error_code == codes::NONE);
    assert!(end_response.producer_id == second_recovery_response.producer_id);
    assert!(end_response.producer_epoch == second_recovery_response.producer_epoch + 1);

    let retry_response = crate::txn::handlers::end_txn::handle(
        &broker,
        end_version,
        7,
        &crate::test_support::encode_request(&end_request, end_version),
        &context,
    )
    .await
    .expect("retry recovered transaction completion");
    let retry_response: krabka_protocol::owned::end_txn_response::EndTxnResponse =
        crate::test_support::decode_response(&retry_response, end_version);
    assert!(retry_response == end_response);
    let completed = broker
        .txn_coordinator
        .get(tids[2])
        .expect("completed 2PC transaction entry");
    let completed = completed.lock().await;
    assert!(completed.state == TxnState::CompleteCommit);
    assert!(completed.next_producer_id.is_none());
    assert!(completed.next_producer_epoch == -1);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn kip939_fields_require_transaction_version_3() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.features.transaction_two_phase_commit_enable = true;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("admin");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;

    for (enable_2pc, keep_prepared_txn) in [(true, false), (false, true)] {
        let request = InitProducerIdRequest {
            transactional_id: Some("txn-tv2".to_string()),
            transaction_timeout_ms: 500,
            enable2_pc: enable_2pc,
            keep_prepared_txn,
            ..Default::default()
        };
        let response = handle(
            &broker,
            version,
            1,
            &crate::test_support::encode_request(&request, version),
            &context,
        )
        .await
        .expect("reject KIP-939 request before TV3");
        let response: InitProducerIdResponse =
            crate::test_support::decode_response(&response, version);
        assert!(
            response.error_code == codes::UNSUPPORTED_VERSION,
            "{response:?}"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn keep_prepared_txn_without_enable_2pc_preserves_finite_timeout() {
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.transaction_state_num_partitions = 7;
        config.transaction_max_timeout = secs(8);
        config.features.transaction_two_phase_commit_enable = true;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    enable_transaction_version_3(&broker).await;
    let principal = principal("admin");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let tid = "txn-recover-finite";

    let find_version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let find_request = krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
        key_type: 1,
        coordinator_keys: vec![tid.to_string()],
        ..Default::default()
    };
    let response = crate::handlers::find_coordinator::handle(
        &broker,
        find_version,
        1,
        &crate::test_support::encode_request(&find_request, find_version),
        &context,
    )
    .await
    .expect("find transaction coordinator");
    let response: krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse =
        crate::test_support::decode_response(&response, find_version);
    assert!(response.coordinators[0].error_code == codes::NONE);

    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let request = InitProducerIdRequest {
        transactional_id: Some(tid.to_string()),
        transaction_timeout_ms: 500,
        ..Default::default()
    };
    let response = handle(
        &broker,
        version,
        2,
        &crate::test_support::encode_request(&request, version),
        &context,
    )
    .await
    .expect("initialize finite-timeout transaction");
    let response: InitProducerIdResponse = crate::test_support::decode_response(&response, version);
    assert!(response.error_code == codes::NONE);

    let finite = broker
        .txn_coordinator
        .get(tid)
        .expect("finite-timeout transaction entry");
    let (finite_pid, finite_epoch, snapshot) = {
        let mut entry = finite.lock().await;
        entry.state = TxnState::Ongoing;
        (entry.producer_id, entry.producer_epoch, entry.clone())
    };
    broker
        .txn_coordinator
        .put(snapshot, crate::txn::version::TxnVersion::TwoPhase)
        .await
        .expect("persist finite ongoing transaction");

    let recovery_request = InitProducerIdRequest {
        transactional_id: Some(tid.to_string()),
        transaction_timeout_ms: 500,
        keep_prepared_txn: true,
        ..Default::default()
    };
    let response = handle(
        &broker,
        version,
        3,
        &crate::test_support::encode_request(&recovery_request, version),
        &context,
    )
    .await
    .expect("recover finite-timeout transaction without enable2Pc");
    let response: InitProducerIdResponse = crate::test_support::decode_response(&response, version);
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
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.transaction_max_timeout = secs(8);
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    enable_transaction_version_3(&broker).await;
    let principal = principal("admin");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
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
            transactional_id: Some(tid.to_string()),
            transaction_timeout_ms: requested_ms,
            ..Default::default()
        };
        let response = handle(
            &broker,
            version,
            2,
            &crate::test_support::encode_request(&request, version),
            &context,
        )
        .await
        .expect("initialize transactional producer");
        let response: InitProducerIdResponse =
            crate::test_support::decode_response(&response, version);
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
    let (broker_handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    enable_transaction_version_3(&broker).await;
    let principal = principal("admin");
    let peer = peer();
    let context = crate::test_support::request_context(&principal, &peer, "txn-client");
    let tid = "txn-half-identity";
    let max = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let find_version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let find_request = krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
        key_type: 1,
        coordinator_keys: vec![tid.to_string()],
        ..Default::default()
    };
    let found = crate::handlers::find_coordinator::handle(
        &broker,
        find_version,
        1,
        &crate::test_support::encode_request(&find_request, find_version),
        &context,
    )
    .await
    .expect("find the transaction coordinator");
    let found: krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse =
        crate::test_support::decode_response(&found, find_version);
    assert!(found.coordinators[0].error_code == codes::NONE);
    // An entry to be fenced against.
    let created = handle(
        &broker,
        max,
        1,
        &crate::test_support::encode_request(
            &InitProducerIdRequest {
                transactional_id: Some(tid.to_string()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            },
            max,
        ),
        &context,
    )
    .await
    .expect("create the transaction entry");
    let created: InitProducerIdResponse = crate::test_support::decode_response(&created, max);
    assert!(created.error_code == codes::NONE, "{created:?}");
    let zombie = (created.producer_id + 1_000, created.producer_epoch);

    // (name, version, request identity, expected code)
    let cases = [
        (
            "a producer id with no epoch",
            max,
            (7, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "an epoch with no producer id",
            max,
            (-1, 3),
            codes::INVALID_REQUEST,
        ),
        (
            "a fenced identity at the newest version",
            max,
            zombie,
            codes::PRODUCER_FENCED,
        ),
        (
            "a fenced identity below version 4",
            3,
            zombie,
            codes::INVALID_PRODUCER_EPOCH,
        ),
    ];
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for (name, version, (producer_id, producer_epoch), code) in cases {
        let request = InitProducerIdRequest {
            transactional_id: Some(tid.to_string()),
            transaction_timeout_ms: 60_000,
            producer_id,
            producer_epoch,
            ..Default::default()
        };
        let response = handle(
            &broker,
            version,
            2,
            &crate::test_support::encode_request(&request, version),
            &context,
        )
        .await
        .expect("initialize transactional producer");
        let response: InitProducerIdResponse =
            crate::test_support::decode_response(&response, version);
        expected.push((name, code));
        actual.push((name, response.error_code));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}
