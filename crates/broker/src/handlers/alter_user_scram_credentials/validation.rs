//! Per-row validation of one SCRAM deletion or one SCRAM upsertion.
//!
//! Each row is checked on its own, and in the order Kafka checks it, because
//! the first failing check decides the per-user error code that the response
//! carries. The module also maps the KIP-554 wire mechanism byte to a
//! [`SaslMechanism`], because an unknown byte is itself a per-row error.

use krabka_protocol::owned::alter_user_scram_credentials_request::{
    ScramCredentialDeletion, ScramCredentialUpsertion,
};
use krabka_security::{
    SaslMechanism,
    scram::{MAX_SCRAM_ITERATIONS, MIN_SCRAM_ITERATIONS},
};

use crate::{broker::Broker, codes};

const EMPTY_USERNAME_MESSAGE: &str = "Username must not be empty";

/// KIP-554 wire byte that identifies a SCRAM mechanism. See
/// [`wire_to_mech`].
type MechanismWireByte = i8;

#[derive(Debug)]
pub(super) struct AlterationError {
    pub(super) code: i16,
    pub(super) message: &'static str,
}

pub(super) fn validate_deletion(
    broker: &Broker,
    deletion: &ScramCredentialDeletion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    if !authorized {
        return Err(AlterationError {
            code: codes::CLUSTER_AUTHORIZATION_FAILED,
            message: "not super-user",
        });
    }
    if deletion.name.is_empty() {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: EMPTY_USERNAME_MESSAGE,
        });
    }
    let Some(mech) = wire_to_mech(deletion.mechanism) else {
        return Err(AlterationError {
            code: codes::UNSUPPORTED_SASL_MECHANISM,
            message: "unknown mechanism",
        });
    };
    if broker
        .controller
        .current_image()
        .scram_credential(&deletion.name, mech)
        .is_none()
    {
        return Err(AlterationError {
            code: codes::RESOURCE_NOT_FOUND,
            message: "credential not found",
        });
    }
    Ok(mech)
}

pub(super) fn validate_upsertion(
    upsertion: &ScramCredentialUpsertion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    if !authorized {
        return Err(AlterationError {
            code: codes::CLUSTER_AUTHORIZATION_FAILED,
            message: "not super-user",
        });
    }
    if upsertion.name.is_empty() {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: EMPTY_USERNAME_MESSAGE,
        });
    }
    let Some(mech) = wire_to_mech(upsertion.mechanism) else {
        return Err(AlterationError {
            code: codes::UNSUPPORTED_SASL_MECHANISM,
            message: "unknown mechanism",
        });
    };
    if upsertion.iterations < MIN_SCRAM_ITERATIONS {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "iterations < 4096",
        });
    }
    if upsertion.iterations > MAX_SCRAM_ITERATIONS {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "iterations > 16384",
        });
    }
    Ok(mech)
}

/// Maps the KIP-554 wire mechanism byte to a [`SaslMechanism`].
///
/// Per KIP-554:
/// - `0`: unknown, reserved
/// - `1`: SCRAM-SHA-256
/// - `2`: SCRAM-SHA-512
fn wire_to_mech(wire: MechanismWireByte) -> Option<SaslMechanism> {
    match wire {
        1 => Some(SaslMechanism::ScramSha256),
        2 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, ScramCredentialRecord};
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
        },
    };
    use krabka_security::{AuthMethod, Principal};

    use super::*;
    use crate::handlers::alter_user_scram_credentials::{
        handle,
        test_support::{
            KAFKA_MAX_SCRAM_ITERATIONS, KAFKA_UNACCEPTABLE_CREDENTIAL,
            KAFKA_UNSUPPORTED_SASL_MECHANISM, deletion, expected_result, process_deletion,
            process_upsertion, start_broker, test_context, valid_upsertion, wait_for_leader,
        },
    };

    #[test]
    fn wire_to_mech_maps_both_scram_variants() {
        let cases = [
            (1, Some(SaslMechanism::ScramSha256)),
            (2, Some(SaslMechanism::ScramSha512)),
            (0, None),
            (99, None),
        ];
        for (wire, expected) in cases {
            assert!(wire_to_mech(wire) == expected, "wire {wire}");
        }
    }

    #[test]
    fn process_upsertion_rejects_unknown_mechanism_with_unsupported_sasl_mechanism() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.mechanism = 99;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNSUPPORTED_SASL_MECHANISM,
                Some("unknown mechanism"),
            )
        );
        assert!(records.is_empty());
    }

    #[test]
    fn process_upsertion_rejects_iterations_above_kafka_maximum() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.iterations = KAFKA_MAX_SCRAM_ITERATIONS + 1;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("iterations > 16384"),
            )
        );
        assert!(records.is_empty());
    }

    #[test]
    fn process_upsertion_allows_kafka_maximum_iterations() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.iterations = KAFKA_MAX_SCRAM_ITERATIONS;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("alice", 0, None));
        assert!(records.len() == 1);
    }

    #[test]
    fn process_upsertion_validates_boundaries_and_records_success() {
        let mut records = Vec::new();

        let rejections = [(
            {
                let mut u = valid_upsertion("too-few");
                u.iterations = MIN_SCRAM_ITERATIONS - 1;
                u
            },
            "iterations < 4096",
        )];
        for (upsertion, msg) in rejections {
            let user = upsertion.name.clone();
            let r = process_upsertion(upsertion, true, &mut records);
            assert!(
                r == expected_result(&user, codes::UNACCEPTABLE_CREDENTIAL, Some(msg)),
                "case: {user}"
            );
            assert!(records.is_empty(), "case: {user}");
        }

        let r = process_upsertion(valid_upsertion("alice"), true, &mut records);
        assert!(r == expected_result("alice", 0, None));
        let (stored_key, server_key) = krabka_security::derive_keys_from_salted(
            SaslMechanism::ScramSha256,
            &valid_upsertion("alice").salted_password,
        );
        let expected_records = vec![MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha256,
            salt: b"salt".to_vec(),
            stored_key,
            server_key,
            iterations: u32::try_from(MIN_SCRAM_ITERATIONS).expect("min fits"),
        })];
        assert!(records == expected_records);
    }

    #[test]
    fn process_upsertion_rejects_unauthorized_users() {
        let mut records = Vec::new();

        let r = process_upsertion(valid_upsertion("bob"), false, &mut records);
        let expected = expected_result(
            "bob",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("not super-user"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn process_deletion_rejects_missing_credentials() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut records = Vec::new();

        let r = process_deletion(&broker, deletion("alice"), true, &mut records);
        assert!(
            r.error_code == 91,
            "missing SCRAM deletion target must use Kafka RESOURCE_NOT_FOUND (91), got {}",
            r.error_code
        );
        let expected = expected_result(
            "alice",
            codes::RESOURCE_NOT_FOUND,
            Some("credential not found"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn process_deletion_rejects_unauthorized_users() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut records = Vec::new();

        let r = process_deletion(&broker, deletion("alice"), false, &mut records);
        let expected = expected_result(
            "alice",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("not super-user"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn process_deletion_rejects_unknown_mechanism_with_unsupported_sasl_mechanism() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut records = Vec::new();
        let mut deletion = deletion("alice");
        deletion.mechanism = 99;

        let r = process_deletion(&broker, deletion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNSUPPORTED_SASL_MECHANISM,
                Some("unknown mechanism"),
            )
        );
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_empty_deletion_username_is_unacceptable_credential() {
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
            deletions: vec![deletion("")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("Username must not be empty"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        assert!(
            broker
                .controller
                .current_image()
                .scram_credential("", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_empty_upsertion_username_is_unacceptable_credential() {
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
            upsertions: vec![valid_upsertion("")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("Username must not be empty"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        assert!(
            broker
                .controller
                .current_image()
                .scram_credential("", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }
}
