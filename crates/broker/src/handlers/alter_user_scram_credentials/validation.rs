//! Per-row validation of one SCRAM deletion or one SCRAM upsertion.
//!
//! Each row is checked on its own, and in the order Kafka checks it, because
//! the first failing check decides the per-user error code that the response
//! carries. The module also maps the KIP-554 wire mechanism byte to a
//! [`SaslMechanism`], because an unknown byte is itself a per-row error.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{broker::Broker, codes},
    krabka_protocol::owned::alter_user_scram_credentials_request::{
        ScramCredentialDeletion, ScramCredentialUpsertion,
    },
    krabka_security::{
        SaslMechanism,
        scram::{MAX_SCRAM_ITERATIONS, MIN_SCRAM_ITERATIONS},
    },
    krabka_verified::{
        ScramAlterationDecision, ScramAlterationFacts, ScramAlterationKind, ScramPriorState,
        scram_alteration_decision,
    },
};

const EMPTY_USERNAME_MESSAGE: &str = "Username must not be empty";

/// The row message of a cluster `Alter` refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs ALTER permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
pub(super) const CLUSTER_ALTER_DENIED_MESSAGE: &str =
    "Request AlterUserScramCredentials needs ALTER permission.";

/// `ScramControlManager`'s refusal when `metadata.version` predates SCRAM.
pub(super) const SCRAM_UNSUPPORTED_MESSAGE: &str =
    "The current metadata.version does not support SCRAM";

/// KIP-554 wire byte that identifies a SCRAM mechanism. See
/// [`wire_to_mech`].
type MechanismWireByte = i8;

#[derive(Debug)]
pub(super) struct AlterationError {
    pub(super) code: i16,
    pub(super) message: &'static str,
}

#[cfg(test)]
pub(super) fn validate_deletion(
    broker: &Broker,
    deletion: &ScramCredentialDeletion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    let decision = decide_deletion(broker, deletion, authorized, ScramPriorState::Unseen);
    decision_mechanism(decision)
        .ok_or_else(|| decision_error(decision).expect("a first-row rejection has an error"))
}

pub(super) fn decide_deletion(
    broker: &Broker,
    deletion: &ScramCredentialDeletion,
    authorized: bool,
    prior: ScramPriorState,
) -> ScramAlterationDecision {
    let deletion_target_exists = wire_to_mech(deletion.mechanism).is_some_and(|mechanism| {
        broker
            .controller
            .current_image()
            .scram_credential(&deletion.name, mechanism)
            .is_some()
    });
    scram_alteration_decision(ScramAlterationFacts {
        kind: ScramAlterationKind::Delete,
        prior,
        authorized,
        name_empty: deletion.name.is_empty(),
        mechanism: deletion.mechanism,
        iterations: 0,
        min_iterations: MIN_SCRAM_ITERATIONS,
        max_iterations: MAX_SCRAM_ITERATIONS,
        deletion_target_exists,
    })
}

#[cfg(test)]
pub(super) fn validate_upsertion(
    upsertion: &ScramCredentialUpsertion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    let decision = decide_upsertion(upsertion, authorized, ScramPriorState::Unseen);
    decision_mechanism(decision)
        .ok_or_else(|| decision_error(decision).expect("a first-row rejection has an error"))
}

pub(super) fn decide_upsertion(
    upsertion: &ScramCredentialUpsertion,
    authorized: bool,
    prior: ScramPriorState,
) -> ScramAlterationDecision {
    scram_alteration_decision(ScramAlterationFacts {
        kind: ScramAlterationKind::Upsert,
        prior,
        authorized,
        name_empty: upsertion.name.is_empty(),
        mechanism: upsertion.mechanism,
        iterations: upsertion.iterations,
        min_iterations: MIN_SCRAM_ITERATIONS,
        max_iterations: MAX_SCRAM_ITERATIONS,
        deletion_target_exists: false,
    })
}

pub(super) fn decision_mechanism(decision: ScramAlterationDecision) -> Option<SaslMechanism> {
    match decision {
        ScramAlterationDecision::AcceptSha256 => Some(SaslMechanism::ScramSha256),
        ScramAlterationDecision::AcceptSha512 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

pub(super) fn decision_error(decision: ScramAlterationDecision) -> Option<AlterationError> {
    let error = match decision {
        ScramAlterationDecision::KeepPriorError
        | ScramAlterationDecision::AcceptSha256
        | ScramAlterationDecision::AcceptSha512 => return None,
        ScramAlterationDecision::Duplicate => AlterationError {
            code: codes::DUPLICATE_RESOURCE,
            message: "A user credential cannot be altered twice in the same request",
        },
        ScramAlterationDecision::Unauthorized => AlterationError {
            code: codes::CLUSTER_AUTHORIZATION_FAILED,
            message: CLUSTER_ALTER_DENIED_MESSAGE,
        },
        ScramAlterationDecision::EmptyName => AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: EMPTY_USERNAME_MESSAGE,
        },
        ScramAlterationDecision::UnsupportedMechanism => AlterationError {
            code: codes::UNSUPPORTED_SASL_MECHANISM,
            message: "Unknown SCRAM mechanism",
        },
        ScramAlterationDecision::TooFewIterations => AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "Too few iterations",
        },
        ScramAlterationDecision::TooManyIterations => AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "Too many iterations",
        },
        ScramAlterationDecision::MissingCredential => AlterationError {
            code: codes::RESOURCE_NOT_FOUND,
            message: "Attempt to delete a user credential that does not exist",
        },
    };
    Some(error)
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

    use {
        super::*,
        crate::handlers::{
            alter_user_scram_credentials::{
                answer,
                test_support::{
                    KAFKA_UNACCEPTABLE_CREDENTIAL, deletion, expected_response, expected_result,
                    process_deletion, process_upsertion, start_broker, test_context,
                    valid_upsertion,
                },
            },
            test_support::allow_all_authorizer,
        },
        assert2::assert,
        krabka_bdd::{Given, Outcome, ScenarioSpec, World},
        krabka_metadata::{MetadataRecord, ScramCredentialRecord},
        krabka_protocol::owned::{
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
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

    /// Whether the alteration holds cluster `ALTER` permission.
    #[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
    enum ClusterAlter {
        With,
        Without,
    }

    /// The one credential alteration a scenario validates.
    enum Alteration {
        Upsertion(ScramCredentialUpsertion),
        Deletion(ScramCredentialDeletion),
    }

    /// One alteration, the broker a deletion is checked against, and what
    /// validating it answered and staged.
    struct ScramValidation {
        alteration: Given<Alteration>,
        broker: Given<HandlerStartedBroker>,
        records: Vec<MetadataRecord>,
        result: Given<AlterUserScramCredentialsResult>,
    }

    #[rstest::fixture]
    fn scram_validation() -> ScramValidation {
        ScramValidation {
            alteration: Given::new(
                "alteration",
                "Given alice supplies a credential upsertion of mechanism type <mechanism> with \
                 <iterations> iterations",
            ),
            broker: Given::new(
                "broker",
                "Given alice asks to delete her mechanism type <mechanism> credential, which \
                 does not exist",
            ),
            records: Vec::new(),
            result: Given::new(
                "result",
                "When the controller validates the alteration <permission> cluster ALTER \
                 permission",
            ),
        }
    }

    impl World for ScramValidation {
        async fn after(&mut self, _scenario: &ScenarioSpec, _outcome: &Outcome) {
            if self.broker.is_set() {
                self.broker.take().broker.shutdown().await;
            }
        }
    }

    #[krabka_bdd::given(
        "alice supplies a credential upsertion of mechanism type {mechanism} with {iterations} iterations"
    )]
    fn alice_supplies_upsertion(
        scram_validation: &mut ScramValidation,
        mechanism: i8,
        iterations: i32,
    ) {
        let mut upsertion = valid_upsertion("alice");
        upsertion.mechanism = mechanism;
        upsertion.iterations = iterations;
        scram_validation
            .alteration
            .set(Alteration::Upsertion(upsertion));
    }

    #[krabka_bdd::given(
        "alice asks to delete her mechanism type {mechanism} credential, which does not exist"
    )]
    async fn alice_asks_deletion(scram_validation: &mut ScramValidation, mechanism: i8) {
        let mut deletion = deletion("alice");
        deletion.mechanism = mechanism;
        scram_validation
            .alteration
            .set(Alteration::Deletion(deletion));
        scram_validation
            .broker
            .set(start_broker(allow_all_authorizer()).await);
    }

    #[krabka_bdd::when(
        "the controller validates the alteration {permission} cluster ALTER permission"
    )]
    fn validate_alteration(scram_validation: &mut ScramValidation, permission: ClusterAlter) {
        let authorized = matches!(permission, ClusterAlter::With);
        let result = match scram_validation.alteration.take() {
            Alteration::Upsertion(upsertion) => {
                process_upsertion(upsertion, authorized, &mut scram_validation.records)
            }
            Alteration::Deletion(deletion) => process_deletion(
                &scram_validation.broker.get().broker.broker_arc_for_test(),
                deletion,
                authorized,
                &mut scram_validation.records,
            ),
        };
        scram_validation.result.set(result);
    }

    #[krabka_bdd::then("alice's SCRAM alteration is refused with error {code} saying {message}")]
    fn alteration_refused(scram_validation: &ScramValidation, code: i16, message: String) {
        assert!(scram_validation.result.get() == &expected_result("alice", code, Some(&message)));
    }

    #[krabka_bdd::then("no credential metadata record is staged")]
    fn nothing_staged(scram_validation: &ScramValidation) {
        assert!(scram_validation.records.is_empty());
    }

    #[krabka_bdd::then("alice's SCRAM alteration is accepted with one staged credential record")]
    fn alteration_accepted(scram_validation: &ScramValidation) {
        assert!(
            (
                scram_validation.result.get(),
                scram_validation.records.len()
            ) == (&expected_result("alice", 0, None), 1)
        );
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/handlers/alter_user_scram_credentials/validation.feature",
        fixtures = [scram_validation: ScramValidation],
        world = scram_validation,
        runtime = "tokio-multi-thread",
        workers = 2,
    );

    /// An accepted upsertion stages the credential Kafka derives from the
    /// salted password: its stored and server keys, salt and iterations.
    #[test]
    fn process_upsertion_stages_the_derived_credential() {
        let mut records = Vec::new();

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

    #[tokio::test]
    async fn handle_empty_deletion_username_is_unacceptable_credential() {
        check_empty_username(AlterUserScramCredentialsRequest {
            deletions: vec![deletion("")],
            ..Default::default()
        })
        .await;
    }

    #[tokio::test]
    async fn handle_empty_upsertion_username_is_unacceptable_credential() {
        check_empty_username(AlterUserScramCredentialsRequest {
            upsertions: vec![valid_upsertion("")],
            ..Default::default()
        })
        .await;
    }

    async fn check_empty_username(req: AlterUserScramCredentialsRequest) {
        krabka_broker_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            ready controller_leader;
            user ["admin"];
        };
        let resp = answer(&broker, req, &ctx).await;
        let expected = expected_response(vec![expected_result(
            "",
            KAFKA_UNACCEPTABLE_CREDENTIAL,
            Some("Username must not be empty"),
        )]);
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
