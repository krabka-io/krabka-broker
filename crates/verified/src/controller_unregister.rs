//! Ordered metadata-image admission of controller unregistration.

use creusot_std::prelude::*;

/// The first metadata-image refusal, or permission to submit the record.
#[krabka_macros::model_type]
pub enum ControllerUnregisterDecision {
    Voter,
    MetadataTooOld,
    NotRegistered,
    Append,
}

/// Authorization and active-controller routing occur before this kernel.
/// The flags describe one captured metadata image; submission and changes
/// after capture remain host obligations.
#[ensures((result == ControllerUnregisterDecision::Voter) == voter)]
#[ensures((result == ControllerUnregisterDecision::MetadataTooOld) == (!voter && !feature_enabled))]
#[ensures((result == ControllerUnregisterDecision::NotRegistered) == (!voter && feature_enabled && !registered))]
#[ensures((result == ControllerUnregisterDecision::Append) == (!voter && feature_enabled && registered))]
#[must_use]
pub fn controller_unregister_decision(
    voter: bool,
    feature_enabled: bool,
    registered: bool,
) -> ControllerUnregisterDecision {
    if voter {
        ControllerUnregisterDecision::Voter
    } else if !feature_enabled {
        ControllerUnregisterDecision::MetadataTooOld
    } else if !registered {
        ControllerUnregisterDecision::NotRegistered
    } else {
        ControllerUnregisterDecision::Append
    }
}
