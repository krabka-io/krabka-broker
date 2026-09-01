//! Schema-registry failure classification and fail-open admission.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Security-relevant class of a failed schema-registry lookup.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum SchemaFailureKind {
    /// The registry answered that the schema ID does not exist.
    Unknown,
    /// No authoritative answer is available yet: transport, throttling, or 5xx.
    Transient,
    /// The registry definitively rejected the request, such as with another 4xx.
    Permanent,
    /// A successful response could not be decoded into the required shape.
    Malformed,
}

/// Whether a failed lookup rejects the record or admits it without validation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum SchemaFailureDecision {
    Reject,
    AllowUnvalidated,
}

/// Apply the configured fail-open policy to a classified registry failure.
///
/// Only a transient failure may be admitted, and only when the operator opted
/// into fail-open behavior. Definite and malformed answers always fail closed.
#[ensures((result == SchemaFailureDecision::AllowUnvalidated)
    == (fail_open && failure == SchemaFailureKind::Transient))]
#[must_use]
pub fn schema_failure_decision(
    fail_open: bool,
    failure: SchemaFailureKind,
) -> SchemaFailureDecision {
    match failure {
        SchemaFailureKind::Transient if fail_open => SchemaFailureDecision::AllowUnvalidated,
        _ => SchemaFailureDecision::Reject,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn fail_open_admits_only_transient_failures() {
        use SchemaFailureDecision::{AllowUnvalidated, Reject};
        use SchemaFailureKind::{Malformed, Permanent, Transient, Unknown};

        for (failure, open_decision) in [
            (Unknown, Reject),
            (Transient, AllowUnvalidated),
            (Permanent, Reject),
            (Malformed, Reject),
        ] {
            check!(schema_failure_decision(false, failure) == Reject);
            check!(schema_failure_decision(true, failure) == open_decision);
        }
    }
}
