//! SCRAM credential alteration planning decisions.

use creusot_std::prelude::*;

/// Credential operation represented by one KIP-554 request row.
#[krabka_macros::model_type]
pub enum ScramAlterationKind {
    Delete,
    Upsert,
}

/// State left by an earlier row for the same user.
#[krabka_macros::model_type]
pub enum ScramPriorState {
    Unseen,
    Accepted,
    Rejected,
}

/// Scalar facts projected from one request row and the current metadata image.
#[krabka_macros::model_type]
pub struct ScramAlterationFacts {
    pub kind: ScramAlterationKind,
    pub prior: ScramPriorState,
    pub authorized: bool,
    pub name_empty: bool,
    pub mechanism: i8,
    pub iterations: i32,
    pub min_iterations: i32,
    pub max_iterations: i32,
    pub deletion_target_exists: bool,
}

/// The first applicable Kafka error, or the mechanism of one accepted row.
#[krabka_macros::model_type]
pub enum ScramAlterationDecision {
    KeepPriorError,
    Duplicate,
    Unauthorized,
    EmptyName,
    UnsupportedMechanism,
    TooFewIterations,
    TooManyIterations,
    MissingCredential,
    AcceptSha256,
    AcceptSha512,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn alteration_mechanism_admitted(facts: ScramAlterationFacts) -> bool {
    pearlite! { alteration_name_admitted(facts)
    && (facts.mechanism@ == 1 || facts.mechanism@ == 2) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn alteration_name_admitted(facts: ScramAlterationFacts) -> bool {
    pearlite! { facts.prior == ScramPriorState::Unseen
    && facts.authorized
    && !facts.name_empty }
}

/// Apply KIP-554 validation and conflict rules in their response precedence.
///
/// A prior failure is sticky. A prior accepted row becomes a duplicate and
/// cancels that pending success. Only the two `Accept` variants authorize the
/// adapter to create one metadata record.
#[ensures(match result {
    ScramAlterationDecision::KeepPriorError => facts.prior == ScramPriorState::Rejected,
    ScramAlterationDecision::Duplicate => facts.prior == ScramPriorState::Accepted,
    ScramAlterationDecision::Unauthorized => {
        facts.prior == ScramPriorState::Unseen && !facts.authorized
    }
    ScramAlterationDecision::EmptyName => {
        facts.prior == ScramPriorState::Unseen && facts.authorized && facts.name_empty
    }
    ScramAlterationDecision::UnsupportedMechanism => {
        alteration_name_admitted(facts)
            && facts.mechanism@ != 1
            && facts.mechanism@ != 2
    }
    ScramAlterationDecision::TooFewIterations => {
        alteration_mechanism_admitted(facts)
            && facts.kind == ScramAlterationKind::Upsert
            && facts.iterations@ < facts.min_iterations@
    }
    ScramAlterationDecision::TooManyIterations => {
        alteration_mechanism_admitted(facts)
            && facts.kind == ScramAlterationKind::Upsert
            && facts.iterations@ >= facts.min_iterations@
            && facts.iterations@ > facts.max_iterations@
    }
    ScramAlterationDecision::MissingCredential => {
        alteration_mechanism_admitted(facts)
            && facts.kind == ScramAlterationKind::Delete
            && !facts.deletion_target_exists
    }
    ScramAlterationDecision::AcceptSha256 | ScramAlterationDecision::AcceptSha512 => {
        alteration_mechanism_admitted(facts)
            && match facts.kind {
                ScramAlterationKind::Delete => facts.deletion_target_exists,
                ScramAlterationKind::Upsert => {
                    facts.iterations@ >= facts.min_iterations@
                        && facts.iterations@ <= facts.max_iterations@
                }
            }
    }
})]
#[ensures(result == ScramAlterationDecision::AcceptSha256 ==> facts.mechanism@ == 1)]
#[ensures(result == ScramAlterationDecision::AcceptSha512 ==> facts.mechanism@ == 2)]
#[must_use]
pub fn scram_alteration_decision(facts: ScramAlterationFacts) -> ScramAlterationDecision {
    match facts.prior {
        ScramPriorState::Rejected => return ScramAlterationDecision::KeepPriorError,
        ScramPriorState::Accepted => return ScramAlterationDecision::Duplicate,
        ScramPriorState::Unseen => {}
    }

    if !facts.authorized {
        return ScramAlterationDecision::Unauthorized;
    }
    if facts.name_empty {
        return ScramAlterationDecision::EmptyName;
    }
    if facts.mechanism != 1 && facts.mechanism != 2 {
        return ScramAlterationDecision::UnsupportedMechanism;
    }

    match facts.kind {
        ScramAlterationKind::Delete if !facts.deletion_target_exists => {
            ScramAlterationDecision::MissingCredential
        }
        ScramAlterationKind::Upsert if facts.iterations < facts.min_iterations => {
            ScramAlterationDecision::TooFewIterations
        }
        ScramAlterationKind::Upsert if facts.iterations > facts.max_iterations => {
            ScramAlterationDecision::TooManyIterations
        }
        ScramAlterationKind::Delete | ScramAlterationKind::Upsert => {
            if facts.mechanism == 1 {
                ScramAlterationDecision::AcceptSha256
            } else {
                ScramAlterationDecision::AcceptSha512
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ScramAlterationDecision, ScramAlterationFacts, ScramAlterationKind, ScramPriorState,
        scram_alteration_decision,
    };

    fn valid_upsert() -> ScramAlterationFacts {
        ScramAlterationFacts {
            kind: ScramAlterationKind::Upsert,
            prior: ScramPriorState::Unseen,
            authorized: true,
            name_empty: false,
            mechanism: 1,
            iterations: 4096,
            min_iterations: 4096,
            max_iterations: 16384,
            deletion_target_exists: false,
        }
    }

    use std::vec;

    struct ScramValidation {
        facts: Option<ScramAlterationFacts>,
        decision: Option<ScramAlterationDecision>,
    }

    #[rstest::fixture]
    fn alteration() -> ScramValidation {
        // Start this scenario with fresh inputs and observations.

        ScramValidation {
            facts: None,
            decision: None,
        }
    }

    #[rstest_bdd_macros::given(
        "a SCRAM upsert with prior result {prior}, authorization {authorized}, empty name {empty_name}, mechanism {mechanism}, and iterations {iterations}"
    )]
    fn upsert_facts(
        alteration: &mut ScramValidation,
        prior: String,
        authorized: bool,
        empty_name: bool,
        mechanism: i8,
        iterations: String,
    ) {
        let prior = match prior.as_str() {
            "rejected" => ScramPriorState::Rejected,
            "accepted" => ScramPriorState::Accepted,
            "unseen" => ScramPriorState::Unseen,
            _ => panic!("unknown prior SCRAM result: {prior}"),
        };
        let iterations = match iterations.as_str() {
            "minimum-integer" => i32::MIN,
            "maximum-integer" => i32::MAX,
            _ => iterations.parse().expect("iteration count is an integer"),
        };
        alteration.facts = Some(ScramAlterationFacts {
            prior,
            authorized,
            name_empty: empty_name,
            mechanism,
            iterations,
            ..valid_upsert()
        });
    }

    #[rstest_bdd_macros::when("the SCRAM upsert is validated")]
    fn validate_upsert(alteration: &mut ScramValidation) {
        alteration.decision = Some(scram_alteration_decision(
            alteration.facts.expect("a SCRAM upsert was given"),
        ));
    }

    #[rstest_bdd_macros::then("the SCRAM response preserves {outcome}")]
    fn scram_response(alteration: &ScramValidation, outcome: String) {
        let expected = match outcome.as_str() {
            "the-prior-error" => ScramAlterationDecision::KeepPriorError,
            "a-duplicate-error" => ScramAlterationDecision::Duplicate,
            "an-authorization-error" => ScramAlterationDecision::Unauthorized,
            "an-empty-name-error" => ScramAlterationDecision::EmptyName,
            "an-unsupported-mechanism-error" => ScramAlterationDecision::UnsupportedMechanism,
            "a-too-few-iterations-error" => ScramAlterationDecision::TooFewIterations,
            "a-too-many-iterations-error" => ScramAlterationDecision::TooManyIterations,
            _ => panic!("unknown SCRAM response outcome: {outcome}"),
        };
        assert2::check!(alteration.decision == Some(expected));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/scram.feature", fixtures = [alteration: ScramValidation]);

    #[test]
    fn accepts_only_supported_complete_rows() {
        let facts = valid_upsert();
        assert2::check!(scram_alteration_decision(facts) == ScramAlterationDecision::AcceptSha256);

        let sha512 = ScramAlterationFacts {
            mechanism: 2,
            iterations: 16384,
            ..facts
        };
        assert2::check!(scram_alteration_decision(sha512) == ScramAlterationDecision::AcceptSha512);

        let missing = ScramAlterationFacts {
            kind: ScramAlterationKind::Delete,
            deletion_target_exists: false,
            ..facts
        };
        assert2::check!(
            scram_alteration_decision(missing) == ScramAlterationDecision::MissingCredential
        );

        let deletion = ScramAlterationFacts {
            deletion_target_exists: true,
            ..missing
        };
        assert2::check!(
            scram_alteration_decision(deletion) == ScramAlterationDecision::AcceptSha256
        );
        // A deletion carries no iteration count, so neither bound applies.
        let sha512_deletion = ScramAlterationFacts {
            mechanism: 2,
            iterations: 0,
            ..deletion
        };
        assert2::check!(
            scram_alteration_decision(sha512_deletion) == ScramAlterationDecision::AcceptSha512
        );
    }
}
