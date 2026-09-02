//! Staging of the requested deletions and upsertions into one plan.
//!
//! The rows are walked in Kafka's order, the deletions and then the
//! upsertions, and each user keeps the first validation or resource error it
//! hit. A second alteration for a user whose earlier row is still accepted
//! becomes `DUPLICATE_RESOURCE` instead. The stage returns the per-user
//! result rows in first-seen order, and the metadata records of the accepted
//! rows.

use std::collections::HashMap;

use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::{
    alter_user_scram_credentials_request::{
        AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
    },
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};
use krabka_security::SaslMechanism;
use krabka_verified::{ScramAlterationDecision, ScramPriorState};

use super::{
    records::{delete_record, upsertion_record},
    response::{err_result, ok_result},
    validation::{
        AlterationError, decide_deletion, decide_upsertion, decision_error, decision_mechanism,
    },
};
use crate::broker::Broker;

pub(super) struct AlterationPlan {
    pub(super) user_results: Vec<AlterUserScramCredentialsResult>,
    pub(super) records: Vec<MetadataRecord>,
}

pub(super) fn plan_alterations(
    broker: &Broker,
    req: AlterUserScramCredentialsRequest,
    authorized: bool,
) -> AlterationPlan {
    let mut user_order = Vec::new();
    let mut deletions = HashMap::new();
    let mut upsertions = HashMap::new();
    let mut errors = HashMap::new();

    for deletion in req.deletions {
        remember_user(&mut user_order, &deletion.name);
        stage_deletion(broker, deletion, authorized, &mut deletions, &mut errors);
    }

    for upsertion in req.upsertions {
        remember_user(&mut user_order, &upsertion.name);
        stage_upsertion(
            upsertion,
            authorized,
            &mut deletions,
            &mut upsertions,
            &mut errors,
        );
    }

    let mut user_results = Vec::with_capacity(user_order.len());
    let mut records = Vec::new();
    for user in user_order {
        if let Some(error) = errors.remove(&user) {
            user_results.push(err_result(user, error.code, error.message));
            continue;
        }

        if let Some((deletion, mechanism)) = deletions.remove(&user) {
            records.push(delete_record(&deletion, mechanism));
            user_results.push(ok_result(deletion.name));
            continue;
        }

        if let Some((upsertion, mechanism)) = upsertions.remove(&user) {
            records.push(upsertion_record(&upsertion, mechanism));
            user_results.push(ok_result(upsertion.name));
        }
    }

    AlterationPlan {
        user_results,
        records,
    }
}

fn remember_user(user_order: &mut Vec<String>, user: &str) {
    if user_order.iter().any(|seen| seen == user) {
        return;
    }
    user_order.push(user.to_string());
}

pub(super) fn distinct_requested_users(req: &AlterUserScramCredentialsRequest) -> Vec<String> {
    let mut users = Vec::new();
    for deletion in &req.deletions {
        remember_user(&mut users, &deletion.name);
    }
    for upsertion in &req.upsertions {
        remember_user(&mut users, &upsertion.name);
    }
    users
}

fn stage_deletion(
    broker: &Broker,
    deletion: ScramCredentialDeletion,
    authorized: bool,
    deletions: &mut HashMap<String, (ScramCredentialDeletion, SaslMechanism)>,
    errors: &mut HashMap<String, AlterationError>,
) {
    let prior = prior_state(
        &deletion.name,
        deletions.contains_key(&deletion.name),
        false,
        errors,
    );
    let decision = decide_deletion(broker, &deletion, authorized, prior);
    match decision {
        ScramAlterationDecision::KeepPriorError => {}
        ScramAlterationDecision::Duplicate => {
            deletions.remove(&deletion.name);
            let error = decision_error(decision).expect("duplicate has an error");
            errors.insert(deletion.name, error);
        }
        ScramAlterationDecision::AcceptSha256 | ScramAlterationDecision::AcceptSha512 => {
            let mechanism =
                decision_mechanism(decision).expect("accepted decision has a mechanism");
            deletions.insert(deletion.name.clone(), (deletion, mechanism));
        }
        _ => {
            let error = decision_error(decision).expect("rejected decision has an error");
            errors.insert(deletion.name, error);
        }
    }
}

fn prior_state(
    user: &str,
    has_deletion: bool,
    has_upsertion: bool,
    errors: &HashMap<String, AlterationError>,
) -> ScramPriorState {
    if errors.contains_key(user) {
        ScramPriorState::Rejected
    } else if has_deletion || has_upsertion {
        ScramPriorState::Accepted
    } else {
        ScramPriorState::Unseen
    }
}

fn stage_upsertion(
    upsertion: ScramCredentialUpsertion,
    authorized: bool,
    deletions: &mut HashMap<String, (ScramCredentialDeletion, SaslMechanism)>,
    upsertions: &mut HashMap<String, (ScramCredentialUpsertion, SaslMechanism)>,
    errors: &mut HashMap<String, AlterationError>,
) {
    let prior = prior_state(
        &upsertion.name,
        deletions.contains_key(&upsertion.name),
        upsertions.contains_key(&upsertion.name),
        errors,
    );
    let decision = decide_upsertion(&upsertion, authorized, prior);
    match decision {
        ScramAlterationDecision::KeepPriorError => {}
        ScramAlterationDecision::Duplicate => {
            deletions.remove(&upsertion.name);
            upsertions.remove(&upsertion.name);
            let error = decision_error(decision).expect("duplicate has an error");
            errors.insert(upsertion.name, error);
        }
        ScramAlterationDecision::AcceptSha256 | ScramAlterationDecision::AcceptSha512 => {
            let mechanism =
                decision_mechanism(decision).expect("accepted decision has a mechanism");
            upsertions.insert(upsertion.name.clone(), (upsertion, mechanism));
        }
        _ => {
            let error = decision_error(decision).expect("rejected decision has an error");
            errors.insert(upsertion.name, error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_metadata::ScramCredentialRecord;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
    };
    use krabka_security::{AuthMethod, Principal, scram::MIN_SCRAM_ITERATIONS};

    use super::*;
    use crate::{
        codes,
        handlers::alter_user_scram_credentials::{
            handle,
            test_support::{
                KAFKA_DUPLICATE_RESOURCE, expected_result, start_broker, test_context,
                valid_upsertion, valid_upsertion_for_mechanism, wait_for_leader,
            },
        },
    };

    #[tokio::test]
    async fn plan_emits_one_record_per_success_and_is_retry_stable() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut overflow = valid_upsertion("overflow");
        overflow.iterations = i32::MAX;
        let req = AlterUserScramCredentialsRequest {
            upsertions: vec![
                valid_upsertion("accepted"),
                overflow,
                valid_upsertion_for_mechanism("duplicate", 1, SaslMechanism::ScramSha256),
                valid_upsertion_for_mechanism("duplicate", 2, SaslMechanism::ScramSha512),
            ],
            ..Default::default()
        };

        let first = plan_alterations(&broker, req.clone(), true);
        let retry = plan_alterations(&broker, req, true);

        assert!(first.user_results.len() == 3);
        assert!(first.user_results[0].error_code == 0);
        assert!(first.user_results[1].error_code == codes::UNACCEPTABLE_CREDENTIAL);
        assert!(first.user_results[2].error_code == codes::DUPLICATE_RESOURCE);
        assert!(first.records.len() == 1);
        let MetadataRecord::V1ScramCredential(record) = &first.records[0] else {
            panic!("accepted upsertion must emit a SCRAM credential record");
        };
        assert!(record.user == "accepted");
        assert!(retry.user_results == first.user_results);
        assert!(retry.records == first.records);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_across_upsertion_mechanisms_returns_one_error_row() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            upsertions: vec![
                valid_upsertion_for_mechanism("alice", 1, SaslMechanism::ScramSha256),
                valid_upsertion_for_mechanism("alice", 2, SaslMechanism::ScramSha512),
            ],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                KAFKA_DUPLICATE_RESOURCE,
                Some("A user credential cannot be altered twice in the same request"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        let image = broker.controller.current_image();
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha256)
                .is_none()
        );
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_between_deletion_and_upsertion_returns_one_error_row() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1ScramCredential(
                ScramCredentialRecord {
                    user: "alice".into(),
                    mechanism: SaslMechanism::ScramSha512,
                    salt: b"salt".to_vec(),
                    stored_key: vec![1; 64],
                    server_key: vec![2; 64],
                    iterations: u32::try_from(MIN_SCRAM_ITERATIONS).expect("min fits"),
                },
            )])
            .await
            .expect("seed alice SCRAM credential");
        broker_handle
            .wait_for_image(|image| {
                image
                    .scram_credential("alice", SaslMechanism::ScramSha512)
                    .is_some()
            })
            .await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: 2,
                ..Default::default()
            }],
            upsertions: vec![valid_upsertion_for_mechanism(
                "alice",
                1,
                SaslMechanism::ScramSha256,
            )],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                KAFKA_DUPLICATE_RESOURCE,
                Some("A user credential cannot be altered twice in the same request"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        let image = broker.controller.current_image();
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        );
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_after_missing_deletion_preserves_resource_not_found() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: 2,
                ..Default::default()
            }],
            upsertions: vec![valid_upsertion_for_mechanism(
                "alice",
                1,
                SaslMechanism::ScramSha256,
            )],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                codes::RESOURCE_NOT_FOUND,
                Some("credential not found"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        let image = broker.controller.current_image();
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha256)
                .is_none()
        );
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_none()
        );
        broker_handle.shutdown().await;
    }
}
