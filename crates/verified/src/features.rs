//! Feature-finalization admission and downgrade record sequencing.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Record plan selected after all feature-finalization facts are established.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FeatureUpdateDecision {
    Reject,
    EmitFeature,
    EmitCleanupThenFeature,
}

/// Admit a feature update only after every registry, direction, compatibility,
/// dependency, and delete-semantic check succeeds.
///
/// A lossy downgrade additionally requires the explicit unsafe-downgrade mode.
/// The distinct accepted result forces the host to emit every cleanup record
/// before the feature-level record that makes the older format authoritative.
#[ensures((result == FeatureUpdateDecision::Reject) == (
    !request.0
        || !request.1
        || !request.2
        || !compatibility.0
        || !compatibility.1
        || !compatibility.2
        || !transition.0
        || (transition.1 && !transition.2)
))]
#[ensures((result == FeatureUpdateDecision::EmitFeature) == (
    request.0
        && request.1
        && request.2
        && compatibility.0
        && compatibility.1
        && compatibility.2
        && transition.0
        && !transition.1
))]
#[ensures((result == FeatureUpdateDecision::EmitCleanupThenFeature) == (
    request.0
        && request.1
        && request.2
        && compatibility.0
        && compatibility.1
        && compatibility.2
        && transition.0
        && transition.1
        && transition.2
))]
#[must_use]
pub fn feature_update_decision(
    request: (bool, bool, bool),
    compatibility: (bool, bool, bool),
    transition: (bool, bool, bool),
) -> FeatureUpdateDecision {
    let (update_type_valid, target_supported, direction_valid) = request;
    let (all_registered_nodes_support, state_representable, dependencies_met) = compatibility;
    let (delete_semantics_valid, cleanup_required, unsafe_downgrade) = transition;
    if !update_type_valid
        || !target_supported
        || !direction_valid
        || !all_registered_nodes_support
        || !state_representable
        || !dependencies_met
        || !delete_semantics_valid
        || (cleanup_required && !unsafe_downgrade)
    {
        FeatureUpdateDecision::Reject
    } else if cleanup_required {
        FeatureUpdateDecision::EmitCleanupThenFeature
    } else {
        FeatureUpdateDecision::EmitFeature
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{FeatureUpdateDecision, feature_update_decision};

    #[test]
    fn finalization_fails_closed_and_requires_unsafe_cleanup() {
        use FeatureUpdateDecision::{EmitCleanupThenFeature, EmitFeature, Reject};

        check!(
            feature_update_decision((true, true, true), (true, true, true), (true, false, false))
                == EmitFeature
        );
        check!(
            feature_update_decision((true, true, true), (true, true, true), (true, true, true))
                == EmitCleanupThenFeature
        );
        check!(
            feature_update_decision((true, true, true), (true, true, true), (true, true, false))
                == Reject
        );

        for rejected in [
            feature_update_decision(
                (false, true, true),
                (true, true, true),
                (true, false, false),
            ),
            feature_update_decision(
                (true, false, true),
                (true, true, true),
                (true, false, false),
            ),
            feature_update_decision(
                (true, true, false),
                (true, true, true),
                (true, false, false),
            ),
            feature_update_decision(
                (true, true, true),
                (false, true, true),
                (true, false, false),
            ),
            feature_update_decision(
                (true, true, true),
                (true, false, true),
                (true, false, false),
            ),
            feature_update_decision(
                (true, true, true),
                (true, true, false),
                (true, false, false),
            ),
            feature_update_decision(
                (true, true, true),
                (true, true, true),
                (false, false, false),
            ),
        ] {
            check!(rejected == Reject);
        }
    }
}
