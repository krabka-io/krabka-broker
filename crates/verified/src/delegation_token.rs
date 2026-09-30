//! KIP-48 delegation-token deadline decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Delegation-token API whose admission policy is being evaluated.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenApi {
    Create,
    Renew,
    Expire,
    Describe,
}

/// Whether a connection may invoke a delegation-token API.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenApiAdmission {
    Reject,
    Allow,
}

/// Credential source selected for the first SCRAM round.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ScramCredentialSource {
    Regular,
    DelegationToken,
    ExpiredDelegationToken,
    Unknown,
}

/// Absolute deadlines stored on a freshly created delegation token.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TokenDeadlines {
    pub max_timestamp_ms: i64,
    pub initial_expiry_ms: i64,
}

/// Whether the host configuration admits a create and, if so, its deadlines.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenCreateDecision {
    Invalid,
    Create(TokenDeadlines),
}

/// Whether a token can be renewed and, if so, its next expiry.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenRenewDecision {
    Invalid,
    Expired,
    Renew(i64),
}

/// Mutation selected by `ExpireDelegationToken`.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenExpireDecision {
    Expired,
    Delete,
    Update(i64),
}

/// Delegation-token mutation whose committed-state precondition is checked.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenMutationKind {
    Renew,
    Expire,
    Delete,
}

/// Relationship between the committed token and a guarded mutation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenMutationState {
    Missing,
    Expected,
    Applied,
    Stale,
}

/// Controller action for a generation-bound delegation-token mutation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TokenMutationDecision {
    Append,
    Retry,
    Reject,
}

/// Scalar facts projected by the controller before it mutates token state.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TokenMutationFacts {
    pub kind: TokenMutationKind,
    pub state: TokenMutationState,
    pub now_ms: i64,
    pub expected_expiry_ms: i64,
    pub incoming_expiry_ms: i64,
    pub max_timestamp_ms: i64,
    pub uncommitted_tail: bool,
}

mod token_mutation_decision;
pub use token_mutation_decision::{
    scram_credential_source, token_api_admission, token_describe_visible, token_mutation_decision,
};

mod token_is_active;
pub use token_is_active::{
    create_token_deadlines, expire_token_deadline, renew_token_expiry, token_is_active,
};

#[cfg(test)]
mod tests;
