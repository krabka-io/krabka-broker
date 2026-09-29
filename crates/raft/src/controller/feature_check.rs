//! Kafka's guard against replaying a feature level this controller does not
//! support.
//!
//! `FeatureControlManager.replay(FeatureLevelRecord)` looks the feature up in
//! the controller's own supported features and throws when the record's level
//! lies outside that range:
//!
//! ```text
//! Tried to apply FeatureLevelRecord ..., but this controller only supports versions <range>
//! ```
//!
//! The exception is fatal, so a controller never runs at a level that it did
//! not advertise. That is what protects a cluster whose log finalized an
//! unstable `metadata.version` from a node that started with
//! `unstable.feature.versions.enable` off. A feature the controller does not
//! know is accepted, as `QuorumFeatures.localSupportedFeature` answers
//! `VersionRange.ALL` for it.
//!
//! The engine applies a record and publishes the resulting image, so the guard
//! reads the finalized levels of a published image: the one the engine
//! recovered at start-up, and each one it publishes afterwards. The image keeps
//! the last level of each feature, so a record that a later record superseded
//! is not named. Kafka would name it, and refuses the same logs.

use krabka_metadata::MetadataImage;

use crate::config::UnstableFeatureVersions;

/// Kafka's `VersionRange.toString()`: a single level as itself, and a span as
/// `min-max`.
fn range_text(min: i16, max: i16) -> String {
    if min == max {
        min.to_string()
    } else {
        format!("{min}-{max}")
    }
}

/// The refusal for the first finalized feature of `image` whose level this
/// controller does not support under `unstable`, or `None` when it supports
/// them all.
pub(super) fn unsupported_feature_level(
    image: &MetadataImage,
    unstable: UnstableFeatureVersions,
) -> Option<String> {
    image.finalized_features().iter().find_map(|(name, level)| {
        let feature = krabka_metadata::feature(name)?;
        let (min, max) = crate::supported_feature_range(feature, unstable);
        (!(min..=max).contains(level)).then(|| {
            format!(
                "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='{name}', \
                 featureLevel={level}), but this controller only supports versions {}",
                range_text(min, max)
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use uuid::Uuid;

    use super::*;

    /// An image that finalized each of `levels`.
    fn image_at(levels: &[(&str, i16)]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for (name, level) in levels {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: (*name).into(),
                level: *level,
            }));
        }
        image
    }

    fn refusal(name: &str, level: i16, range: &str) -> String {
        format!(
            "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='{name}', \
             featureLevel={level}), but this controller only supports versions {range}"
        )
    }

    /// Kafka 4.3's latest production `metadata.version` is `4.3-IV0` (30), and
    /// `4.4-IV2` (33) is one of trunk's unstable levels.
    #[test]
    fn a_level_outside_the_supported_range_is_refused_with_kafkas_message() {
        use UnstableFeatureVersions::{Disabled, Enabled};

        for (what, levels, unstable, expected) in [
            (
                "the latest production level",
                vec![("metadata.version", 30)],
                Disabled,
                None,
            ),
            (
                "an unstable level with the flag off",
                vec![("metadata.version", 33)],
                Disabled,
                Some(refusal("metadata.version", 33, "7-30")),
            ),
            (
                "an unstable level with the flag on",
                vec![("metadata.version", 33)],
                Enabled,
                None,
            ),
            (
                "a feature above its range",
                vec![("share.version", 5)],
                Enabled,
                Some(refusal("share.version", 5, "0-2")),
            ),
            (
                "a level below the range",
                vec![("metadata.version", 3)],
                Enabled,
                Some(refusal("metadata.version", 3, "7-34")),
            ),
            (
                "the first of two refused features",
                vec![("metadata.version", 33), ("share.version", 5)],
                Disabled,
                Some(refusal("metadata.version", 33, "7-30")),
            ),
            (
                "a feature this controller does not know",
                vec![("some.future.feature", 9)],
                Disabled,
                None,
            ),
            ("no finalized feature", vec![], Disabled, None),
        ] {
            check!(
                unsupported_feature_level(&image_at(&levels), unstable) == expected,
                "{what}"
            );
        }
    }
}
