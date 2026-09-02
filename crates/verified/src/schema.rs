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

/// A schema-validated record field's role in subject selection.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum SchemaFieldRole {
    Key,
    Value,
}

/// Whether the schema gate skips a field or validates it under one role.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum SchemaFieldAction {
    Skip,
    CheckKey,
    CheckValue,
}

/// Whether every applicable field in a record batch was admitted.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum SchemaBatchAdmission {
    Admit,
    Reject,
}

/// Decode the exact big-endian schema ID from a complete Confluent prefix.
///
/// The adapter supplies the five prefix bytes only after establishing that
/// they exist. Magic zero is the sole admitted framing version.
#[ensures((result == None) == (magic != 0u8))]
#[ensures(forall<id: u32> result == Some(id) ==>
    id@ == id_0@ * 16_777_216
        + id_1@ * 65_536
        + id_2@ * 256
        + id_3@)]
#[must_use]
pub fn schema_frame_id(magic: u8, id_0: u8, id_1: u8, id_2: u8, id_3: u8) -> Option<u32> {
    if magic != 0 {
        return None;
    }
    Some(
        u32::from(id_0) * 16_777_216
            + u32::from(id_1) * 65_536
            + u32::from(id_2) * 256
            + u32::from(id_3),
    )
}

/// Select exactly the configured, non-null key or value field.
#[ensures((result == SchemaFieldAction::CheckKey)
    == (key_enabled && present && role == SchemaFieldRole::Key))]
#[ensures((result == SchemaFieldAction::CheckValue)
    == (value_enabled && present && role == SchemaFieldRole::Value))]
#[ensures((result == SchemaFieldAction::Skip)
    == (!present
        || (role == SchemaFieldRole::Key && !key_enabled)
        || (role == SchemaFieldRole::Value && !value_enabled)))]
#[must_use]
pub fn schema_field_action(
    key_enabled: bool,
    value_enabled: bool,
    role: SchemaFieldRole,
    present: bool,
) -> SchemaFieldAction {
    if present {
        match role {
            SchemaFieldRole::Key if key_enabled => SchemaFieldAction::CheckKey,
            SchemaFieldRole::Value if value_enabled => SchemaFieldAction::CheckValue,
            SchemaFieldRole::Key | SchemaFieldRole::Value => SchemaFieldAction::Skip,
        }
    } else {
        SchemaFieldAction::Skip
    }
}

/// Admit a batch only after a complete walk where every applicable field was admitted.
#[ensures((result == SchemaBatchAdmission::Admit)
    == (walk_complete && applicable == admitted))]
#[ensures((result == SchemaBatchAdmission::Reject)
    == (!walk_complete || applicable != admitted))]
#[must_use]
pub fn schema_batch_admission(
    walk_complete: bool,
    applicable: u64,
    admitted: u64,
) -> SchemaBatchAdmission {
    if walk_complete && applicable == admitted {
        SchemaBatchAdmission::Admit
    } else {
        SchemaBatchAdmission::Reject
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

    #[test]
    fn framing_field_selection_and_batch_admission_are_exact() {
        check!(schema_frame_id(0, 0x01, 0x23, 0x45, 0x67) == Some(0x0123_4567));
        check!(schema_frame_id(1, 0x01, 0x23, 0x45, 0x67) == None);

        for key_enabled in [false, true] {
            for value_enabled in [false, true] {
                for present in [false, true] {
                    let key = schema_field_action(
                        key_enabled,
                        value_enabled,
                        SchemaFieldRole::Key,
                        present,
                    );
                    let value = schema_field_action(
                        key_enabled,
                        value_enabled,
                        SchemaFieldRole::Value,
                        present,
                    );
                    let expected_key = if key_enabled && present {
                        SchemaFieldAction::CheckKey
                    } else {
                        SchemaFieldAction::Skip
                    };
                    let expected_value = if value_enabled && present {
                        SchemaFieldAction::CheckValue
                    } else {
                        SchemaFieldAction::Skip
                    };
                    check!(key == expected_key);
                    check!(value == expected_value);
                }
            }
        }

        check!(schema_batch_admission(true, 0, 0) == SchemaBatchAdmission::Admit);
        check!(schema_batch_admission(true, 4, 4) == SchemaBatchAdmission::Admit);
        check!(schema_batch_admission(true, 4, 3) == SchemaBatchAdmission::Reject);
        check!(schema_batch_admission(true, 3, 4) == SchemaBatchAdmission::Reject);
        check!(schema_batch_admission(false, 4, 4) == SchemaBatchAdmission::Reject);
    }
}
