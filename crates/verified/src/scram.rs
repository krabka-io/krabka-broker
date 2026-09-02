//! SCRAM credential alteration planning decisions.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Credential operation represented by one KIP-554 request row.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ScramAlterationKind {
    Delete,
    Upsert,
}

/// State left by an earlier row for the same user.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ScramPriorState {
    Unseen,
    Accepted,
    Rejected,
}

/// Scalar facts projected from one request row and the current metadata image.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
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
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
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
        facts.prior == ScramPriorState::Unseen
            && facts.authorized
            && !facts.name_empty
            && facts.mechanism@ != 1
            && facts.mechanism@ != 2
    }
    ScramAlterationDecision::TooFewIterations => {
        facts.prior == ScramPriorState::Unseen
            && facts.authorized
            && !facts.name_empty
            && (facts.mechanism@ == 1 || facts.mechanism@ == 2)
            && facts.kind == ScramAlterationKind::Upsert
            && facts.iterations@ < facts.min_iterations@
    }
    ScramAlterationDecision::TooManyIterations => {
        facts.prior == ScramPriorState::Unseen
            && facts.authorized
            && !facts.name_empty
            && (facts.mechanism@ == 1 || facts.mechanism@ == 2)
            && facts.kind == ScramAlterationKind::Upsert
            && facts.iterations@ >= facts.min_iterations@
            && facts.iterations@ > facts.max_iterations@
    }
    ScramAlterationDecision::MissingCredential => {
        facts.prior == ScramPriorState::Unseen
            && facts.authorized
            && !facts.name_empty
            && (facts.mechanism@ == 1 || facts.mechanism@ == 2)
            && facts.kind == ScramAlterationKind::Delete
            && !facts.deletion_target_exists
    }
    ScramAlterationDecision::AcceptSha256 | ScramAlterationDecision::AcceptSha512 => {
        facts.prior == ScramPriorState::Unseen
            && facts.authorized
            && !facts.name_empty
            && (facts.mechanism@ == 1 || facts.mechanism@ == 2)
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

    #[test]
    fn errors_follow_kafka_precedence() {
        let mut facts = valid_upsert();
        facts.prior = ScramPriorState::Rejected;
        facts.authorized = false;
        facts.name_empty = true;
        facts.mechanism = 0;
        facts.iterations = i32::MIN;
        assert2::check!(
            scram_alteration_decision(facts) == ScramAlterationDecision::KeepPriorError
        );

        facts.prior = ScramPriorState::Accepted;
        assert2::check!(scram_alteration_decision(facts) == ScramAlterationDecision::Duplicate);

        facts.prior = ScramPriorState::Unseen;
        assert2::check!(scram_alteration_decision(facts) == ScramAlterationDecision::Unauthorized);

        facts.authorized = true;
        assert2::check!(scram_alteration_decision(facts) == ScramAlterationDecision::EmptyName);

        facts.name_empty = false;
        assert2::check!(
            scram_alteration_decision(facts) == ScramAlterationDecision::UnsupportedMechanism
        );

        facts.mechanism = 1;
        assert2::check!(
            scram_alteration_decision(facts) == ScramAlterationDecision::TooFewIterations
        );

        facts.iterations = i32::MAX;
        assert2::check!(
            scram_alteration_decision(facts) == ScramAlterationDecision::TooManyIterations
        );
    }

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
        assert2::check!(scram_alteration_decision(deletion) == scram_alteration_decision(deletion));
    }
}
