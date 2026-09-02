//! Topic-freeze signature, scope, and replacement decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

/// How far a signed record passed through the principal-binding rules.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeIdentityState {
    UnknownKey,
    WrongKeyPrincipal,
    WrongConnectionPrincipal,
    Bound,
}

/// Facts used by the ordered six-rule signature admission.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FreezeSignatureFacts {
    pub identity: FreezeIdentityState,
    pub set_at_ms: i64,
    pub now_ms: i64,
    pub max_skew_ms: i64,
    pub replaces: bool,
    pub replaced_set_at_ms: i64,
    pub signature_valid: bool,
}

/// The first failed rule, or admission after all six pass.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeSignatureDecision {
    UnknownKey,
    AuthorIsNotKeyPrincipal,
    AuthorIsNotConnectionPrincipal,
    TimestampOutsideSkewWindow,
    TimestampNotNewer,
    SignatureInvalid,
    Admit,
}

/// Mathematical skew-window predicate, without machine-integer overflow.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn freeze_timestamp_in_window_model(set_at_ms: i64, now_ms: i64, max_skew_ms: i64) -> bool {
    pearlite! {
        max_skew_ms@ >= 0
            && set_at_ms@ >= now_ms@ - max_skew_ms@
            && set_at_ms@ <= now_ms@ + max_skew_ms@
    }
}

/// Check the symmetric skew window with saturating machine bounds.
#[ensures(result == freeze_timestamp_in_window_model(set_at_ms, now_ms, max_skew_ms))]
#[must_use]
pub const fn freeze_timestamp_in_window(set_at_ms: i64, now_ms: i64, max_skew_ms: i64) -> bool {
    if max_skew_ms < 0 {
        return false;
    }
    let earliest = now_ms.saturating_sub(max_skew_ms);
    let latest = now_ms.saturating_add(max_skew_ms);
    set_at_ms >= earliest && set_at_ms <= latest
}

/// Apply the six signature rules in their security-sensitive order.
#[ensures(match result {
    FreezeSignatureDecision::UnknownKey => facts.identity == FreezeIdentityState::UnknownKey,
    FreezeSignatureDecision::AuthorIsNotKeyPrincipal => {
        facts.identity == FreezeIdentityState::WrongKeyPrincipal
    }
    FreezeSignatureDecision::AuthorIsNotConnectionPrincipal => {
        facts.identity == FreezeIdentityState::WrongConnectionPrincipal
    }
    FreezeSignatureDecision::TimestampOutsideSkewWindow => {
        facts.identity == FreezeIdentityState::Bound
            && !freeze_timestamp_in_window_model(
                facts.set_at_ms,
                facts.now_ms,
                facts.max_skew_ms,
            )
    }
    FreezeSignatureDecision::TimestampNotNewer => {
        facts.identity == FreezeIdentityState::Bound
            && freeze_timestamp_in_window_model(
                facts.set_at_ms,
                facts.now_ms,
                facts.max_skew_ms,
            )
            && facts.replaces
            && facts.set_at_ms@ <= facts.replaced_set_at_ms@
    }
    FreezeSignatureDecision::SignatureInvalid => {
        facts.identity == FreezeIdentityState::Bound
            && freeze_timestamp_in_window_model(
                facts.set_at_ms,
                facts.now_ms,
                facts.max_skew_ms,
            )
            && (!facts.replaces || facts.set_at_ms@ > facts.replaced_set_at_ms@)
            && !facts.signature_valid
    }
    FreezeSignatureDecision::Admit => {
        facts.identity == FreezeIdentityState::Bound
            && freeze_timestamp_in_window_model(
                facts.set_at_ms,
                facts.now_ms,
                facts.max_skew_ms,
            )
            && (!facts.replaces || facts.set_at_ms@ > facts.replaced_set_at_ms@)
            && facts.signature_valid
    }
})]
#[must_use]
pub fn freeze_signature_decision(facts: FreezeSignatureFacts) -> FreezeSignatureDecision {
    match facts.identity {
        FreezeIdentityState::UnknownKey => FreezeSignatureDecision::UnknownKey,
        FreezeIdentityState::WrongKeyPrincipal => FreezeSignatureDecision::AuthorIsNotKeyPrincipal,
        FreezeIdentityState::WrongConnectionPrincipal => {
            FreezeSignatureDecision::AuthorIsNotConnectionPrincipal
        }
        FreezeIdentityState::Bound => {
            if !freeze_timestamp_in_window(facts.set_at_ms, facts.now_ms, facts.max_skew_ms) {
                FreezeSignatureDecision::TimestampOutsideSkewWindow
            } else if facts.replaces && facts.set_at_ms <= facts.replaced_set_at_ms {
                FreezeSignatureDecision::TimestampNotNewer
            } else if !facts.signature_valid {
                FreezeSignatureDecision::SignatureInvalid
            } else {
                FreezeSignatureDecision::Admit
            }
        }
    }
}

/// The precedence of one scope that matches a topic.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeScopeRank {
    NoMatch,
    Prefix { length: u64 },
    Literal,
}

/// Whether a matching scope replaces the current resolver candidate.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeScopeDecision {
    Keep,
    Replace,
}

/// Make every caller use literal-over-prefix and longest-prefix precedence.
#[ensures((result == FreezeScopeDecision::Replace) == match (current, candidate) {
    (_, FreezeScopeRank::NoMatch) | (FreezeScopeRank::Literal, _) => false,
    (FreezeScopeRank::NoMatch, _) => true,
    (FreezeScopeRank::Prefix { .. }, FreezeScopeRank::Literal) => true,
    (
        FreezeScopeRank::Prefix { length: current_length },
        FreezeScopeRank::Prefix { length: candidate_length },
    ) => candidate_length@ > current_length@,
})]
#[must_use]
pub fn freeze_scope_decision(
    current: FreezeScopeRank,
    candidate: FreezeScopeRank,
) -> FreezeScopeDecision {
    match (current, candidate) {
        (_, FreezeScopeRank::NoMatch) | (FreezeScopeRank::Literal, _) => FreezeScopeDecision::Keep,
        (FreezeScopeRank::NoMatch, _)
        | (FreezeScopeRank::Prefix { .. }, FreezeScopeRank::Literal) => {
            FreezeScopeDecision::Replace
        }
        (
            FreezeScopeRank::Prefix {
                length: current_length,
            },
            FreezeScopeRank::Prefix {
                length: candidate_length,
            },
        ) => {
            if candidate_length > current_length {
                FreezeScopeDecision::Replace
            } else {
                FreezeScopeDecision::Keep
            }
        }
    }
}

/// Every operation whose interaction with a topic freeze is deliberate.
///
/// Keeping the allowed operations in the same closed enum as the refused
/// ones makes additions visible to the proof and to its exhaustive tests.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeMutationKind {
    Produce,
    TransactionEnlistment,
    DeleteRecords,
    DeleteTopic,
    ReassignmentAlter,
    ReassignmentCompletion,
    Compaction,
    Retention,
    TransactionCompletion,
    OffsetCommit,
    Replication,
    BarrierMarker,
    TieringCopy,
}

/// The single externally observable result of authorization plus freeze
/// admission.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeMutationDecision {
    AuthorizationDenied,
    Frozen,
    Admit,
}

/// Classify the complete topic-mutation inventory under a live freeze.
#[ensures(result == freeze_refusal_model(kind))]
#[must_use]
pub const fn freeze_refuses(kind: FreezeMutationKind) -> bool {
    match kind {
        FreezeMutationKind::Produce
        | FreezeMutationKind::TransactionEnlistment
        | FreezeMutationKind::DeleteRecords
        | FreezeMutationKind::DeleteTopic
        | FreezeMutationKind::ReassignmentAlter
        | FreezeMutationKind::Compaction
        | FreezeMutationKind::Retention => true,
        FreezeMutationKind::TransactionCompletion
        | FreezeMutationKind::ReassignmentCompletion
        | FreezeMutationKind::OffsetCommit
        | FreezeMutationKind::Replication
        | FreezeMutationKind::BarrierMarker
        | FreezeMutationKind::TieringCopy => false,
    }
}

/// Logical mirror of [`freeze_refuses`] for the mutation contract.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn freeze_refusal_model(kind: FreezeMutationKind) -> bool {
    match kind {
        FreezeMutationKind::Produce
        | FreezeMutationKind::TransactionEnlistment
        | FreezeMutationKind::DeleteRecords
        | FreezeMutationKind::DeleteTopic
        | FreezeMutationKind::ReassignmentAlter
        | FreezeMutationKind::Compaction
        | FreezeMutationKind::Retention => true,
        FreezeMutationKind::TransactionCompletion
        | FreezeMutationKind::ReassignmentCompletion
        | FreezeMutationKind::OffsetCommit
        | FreezeMutationKind::Replication
        | FreezeMutationKind::BarrierMarker
        | FreezeMutationKind::TieringCopy => false,
    }
}

/// Rank authorization ahead of freeze detail, then apply the one refusal
/// classification shared by every mutation adapter.
#[ensures((result == FreezeMutationDecision::AuthorizationDenied) == !authorized)]
#[ensures((result == FreezeMutationDecision::Frozen) == (
    authorized && frozen && freeze_refusal_model(kind)
))]
#[ensures((result == FreezeMutationDecision::Admit) == (
    authorized && (!frozen || !freeze_refusal_model(kind))
))]
#[must_use]
pub const fn freeze_mutation_decision(
    authorized: bool,
    frozen: bool,
    kind: FreezeMutationKind,
) -> FreezeMutationDecision {
    if !authorized {
        FreezeMutationDecision::AuthorizationDenied
    } else if frozen && freeze_refuses(kind) {
        FreezeMutationDecision::Frozen
    } else {
        FreezeMutationDecision::Admit
    }
}

/// The committed entry at the exact incoming scope key.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeStoredState {
    Missing,
    Present { set_at_ms: i64 },
}

/// Facts checked before a freeze mutation enters the metadata log.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FreezeReplacementFacts {
    pub stored: FreezeStoredState,
    pub incoming_frozen: bool,
    pub incoming_set_at_ms: i64,
    pub uncommitted_tail: bool,
}

/// Why a freeze mutation may or may not enter the metadata log.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FreezeReplacementDecision {
    Missing,
    Stale,
    InFlight,
    Append,
}

/// Admit only a new freeze or a strictly newer exact-key replacement, with no
/// uncommitted metadata tail.
#[ensures((result == FreezeReplacementDecision::Append) == (
    !facts.uncommitted_tail
        && match facts.stored {
            FreezeStoredState::Missing => facts.incoming_frozen,
            FreezeStoredState::Present { set_at_ms } => facts.incoming_set_at_ms@ > set_at_ms@,
        }
))]
#[must_use]
pub fn freeze_replacement_decision(facts: FreezeReplacementFacts) -> FreezeReplacementDecision {
    match facts.stored {
        FreezeStoredState::Missing if !facts.incoming_frozen => FreezeReplacementDecision::Missing,
        FreezeStoredState::Present { set_at_ms } if facts.incoming_set_at_ms <= set_at_ms => {
            FreezeReplacementDecision::Stale
        }
        FreezeStoredState::Missing | FreezeStoredState::Present { .. } => {
            if facts.uncommitted_tail {
                FreezeReplacementDecision::InFlight
            } else {
                FreezeReplacementDecision::Append
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FreezeIdentityState, FreezeMutationDecision, FreezeMutationKind, FreezeReplacementDecision,
        FreezeReplacementFacts, FreezeScopeDecision, FreezeScopeRank, FreezeSignatureDecision,
        FreezeSignatureFacts, FreezeStoredState, freeze_mutation_decision,
        freeze_replacement_decision, freeze_scope_decision, freeze_signature_decision,
        freeze_timestamp_in_window,
    };

    #[test]
    fn skew_window_is_symmetric_and_overflow_safe() {
        assert2::check!(freeze_timestamp_in_window(i64::MAX, i64::MAX, i64::MAX));
        assert2::check!(!freeze_timestamp_in_window(i64::MIN, i64::MAX, i64::MAX));
        assert2::check!(!freeze_timestamp_in_window(i64::MAX, i64::MIN, i64::MAX));
        assert2::check!(!freeze_timestamp_in_window(0, 0, -1));
    }

    #[test]
    fn signature_rules_return_the_first_failure() {
        let valid = FreezeSignatureFacts {
            identity: FreezeIdentityState::Bound,
            set_at_ms: 100,
            now_ms: 100,
            max_skew_ms: 10,
            replaces: true,
            replaced_set_at_ms: 99,
            signature_valid: true,
        };
        for (facts, expected) in [
            (
                FreezeSignatureFacts {
                    identity: FreezeIdentityState::UnknownKey,
                    ..valid
                },
                FreezeSignatureDecision::UnknownKey,
            ),
            (
                FreezeSignatureFacts {
                    identity: FreezeIdentityState::WrongKeyPrincipal,
                    ..valid
                },
                FreezeSignatureDecision::AuthorIsNotKeyPrincipal,
            ),
            (
                FreezeSignatureFacts {
                    identity: FreezeIdentityState::WrongConnectionPrincipal,
                    ..valid
                },
                FreezeSignatureDecision::AuthorIsNotConnectionPrincipal,
            ),
            (
                FreezeSignatureFacts {
                    set_at_ms: 111,
                    ..valid
                },
                FreezeSignatureDecision::TimestampOutsideSkewWindow,
            ),
            (
                FreezeSignatureFacts {
                    set_at_ms: 99,
                    ..valid
                },
                FreezeSignatureDecision::TimestampNotNewer,
            ),
            (
                FreezeSignatureFacts {
                    signature_valid: false,
                    ..valid
                },
                FreezeSignatureDecision::SignatureInvalid,
            ),
            (valid, FreezeSignatureDecision::Admit),
        ] {
            assert2::check!(freeze_signature_decision(facts) == expected);
        }
    }

    #[test]
    fn scope_precedence_is_literal_then_longest_prefix() {
        assert2::check!(
            freeze_scope_decision(
                FreezeScopeRank::NoMatch,
                FreezeScopeRank::Prefix { length: 3 },
            ) == FreezeScopeDecision::Replace
        );
        assert2::check!(
            freeze_scope_decision(
                FreezeScopeRank::Prefix { length: 3 },
                FreezeScopeRank::Prefix { length: 4 },
            ) == FreezeScopeDecision::Replace
        );
        assert2::check!(
            freeze_scope_decision(
                FreezeScopeRank::Prefix { length: 4 },
                FreezeScopeRank::Prefix { length: 3 },
            ) == FreezeScopeDecision::Keep
        );
        assert2::check!(
            freeze_scope_decision(
                FreezeScopeRank::Prefix { length: 4 },
                FreezeScopeRank::Literal
            ) == FreezeScopeDecision::Replace
        );
        assert2::check!(
            freeze_scope_decision(
                FreezeScopeRank::Literal,
                FreezeScopeRank::Prefix { length: 5 }
            ) == FreezeScopeDecision::Keep
        );
    }

    #[test]
    fn mutation_inventory_ranks_authorization_then_freeze() {
        use FreezeMutationDecision::{Admit, AuthorizationDenied, Frozen};
        use FreezeMutationKind::{
            BarrierMarker, Compaction, DeleteRecords, DeleteTopic, OffsetCommit, Produce,
            ReassignmentAlter, ReassignmentCompletion, Replication, Retention, TieringCopy,
            TransactionCompletion, TransactionEnlistment,
        };

        let refused = [
            Produce,
            TransactionEnlistment,
            DeleteRecords,
            DeleteTopic,
            ReassignmentAlter,
            Compaction,
            Retention,
        ];
        let allowed = [
            TransactionCompletion,
            ReassignmentCompletion,
            OffsetCommit,
            Replication,
            BarrierMarker,
            TieringCopy,
        ];

        for kind in refused {
            assert2::check!(freeze_mutation_decision(false, true, kind) == AuthorizationDenied);
            assert2::check!(freeze_mutation_decision(true, true, kind) == Frozen);
            assert2::check!(freeze_mutation_decision(true, false, kind) == Admit);
        }
        for kind in allowed {
            assert2::check!(freeze_mutation_decision(false, true, kind) == AuthorizationDenied);
            assert2::check!(freeze_mutation_decision(true, true, kind) == Admit);
            assert2::check!(freeze_mutation_decision(true, false, kind) == Admit);
        }
    }

    #[test]
    fn replacements_require_a_live_thaw_target_a_newer_stamp_and_no_tail() {
        let valid = FreezeReplacementFacts {
            stored: FreezeStoredState::Present { set_at_ms: 9 },
            incoming_frozen: true,
            incoming_set_at_ms: 10,
            uncommitted_tail: false,
        };
        for (facts, expected) in [
            (
                FreezeReplacementFacts {
                    stored: FreezeStoredState::Missing,
                    incoming_frozen: false,
                    ..valid
                },
                FreezeReplacementDecision::Missing,
            ),
            (
                FreezeReplacementFacts {
                    incoming_set_at_ms: 9,
                    ..valid
                },
                FreezeReplacementDecision::Stale,
            ),
            (
                FreezeReplacementFacts {
                    uncommitted_tail: true,
                    ..valid
                },
                FreezeReplacementDecision::InFlight,
            ),
            (valid, FreezeReplacementDecision::Append),
            (
                FreezeReplacementFacts {
                    stored: FreezeStoredState::Missing,
                    incoming_frozen: true,
                    ..valid
                },
                FreezeReplacementDecision::Append,
            ),
        ] {
            assert2::check!(freeze_replacement_decision(facts) == expected);
        }
    }
}
