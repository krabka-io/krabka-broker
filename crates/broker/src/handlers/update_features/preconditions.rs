//! Predicates over the live metadata image that gate a feature finalize.
//!
//! Each function answers one question about the cluster as the image records
//! it: whether the KIP-1022 dependencies of a level hold, whether every
//! registered node supports a level, and whether the quorum is fully
//! registered. They are read-only and share no state, so they live apart from
//! the validation loop that calls them.

/// True when the target image already meets every KIP-1022 dependency for a
/// feature finalize. `deps` is the feature's `dependencies(level)` slice, which
/// holds `(dependency_feature_name, min_finalized_level)` pairs.
pub(super) fn dependencies_met(
    image: &krabka_metadata::MetadataImage,
    deps: &[(&str, i16)],
) -> bool {
    deps.iter().all(|(dep, min_level)| {
        image
            .finalized_features()
            .get(*dep)
            .is_some_and(|finalized| finalized >= min_level)
    })
}

pub(super) fn unsupported_registered_node(
    image: &krabka_metadata::MetadataImage,
    feature: &str,
    level: i16,
) -> Option<String> {
    for broker in image.brokers() {
        if !broker
            .features
            .get(feature)
            .is_some_and(|&(min, max)| min <= level && level <= max)
        {
            return Some(format!(
                "Broker {} does not support {feature} level {level}.",
                broker.node_id
            ));
        }
    }
    for controller in image.controllers() {
        if !controller
            .features
            .get(feature)
            .is_some_and(|&(min, max)| min <= level && level <= max)
        {
            return Some(format!(
                "Controller {} does not support {feature} level {level}.",
                controller.node_id
            ));
        }
    }
    None
}

pub(super) fn registered_node_without_metadata_downgrade_capability(
    image: &krabka_metadata::MetadataImage,
) -> Option<String> {
    let supports_downgrade = |features: &std::collections::BTreeMap<String, (i16, i16)>| {
        features
            .get(krabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_FEATURE)
            .is_some_and(|&(min, max)| {
                min <= krabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_LEVEL
                    && krabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_LEVEL <= max
            })
    };
    for broker in image.brokers() {
        if !supports_downgrade(&broker.features) {
            return Some(format!(
                "Broker {} does not support online metadata.version downgrade.",
                broker.node_id
            ));
        }
    }
    for controller in image.controllers() {
        if !supports_downgrade(&controller.features) {
            return Some(format!(
                "Controller {} does not support online metadata.version downgrade.",
                controller.node_id
            ));
        }
    }
    None
}

pub(super) fn unregistered_controller(
    image: &krabka_metadata::MetadataImage,
) -> Option<krabka_metadata::NodeId> {
    image
        .voters()
        .iter()
        .map(|voter| voter.id)
        .find(|&id| image.controller(id).is_none())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn dependencies_met_checks_finalized_levels() {
        use krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        // No deps → trivially met.
        assert!(dependencies_met(&image, &[]));
        // Unmet: metadata.version not finalized at all.
        assert!(!dependencies_met(&image, &[("metadata.version", 22)]));
        // Finalize metadata.version=25 → a >=22 dependency is now met, >=26 not.
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        }));
        assert!(dependencies_met(&image, &[("metadata.version", 22)]));
        assert!(!dependencies_met(&image, &[("metadata.version", 26)]));
    }
}
