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
//! know is supported at level 0 only, as `QuorumFeatures.localSupportedFeature`
//! answers `DISABLED`, the range `0-0`, for it: a log that finalized a feature
//! from a newer release at any level above 0 is refused, and the message names
//! the range as `0`.
//!
//! Kafka's fatal fault halts the process (`ProcessTerminatingFaultHandler`,
//! exit status 1). A library does not halt its host, so the controller
//! reports the fault and the embedder halts. A controller that starts over such
//! a log refuses to start with a [`RaftError::FatalFault`](crate::RaftError),
//! and one that applies such a level from its leader stops itself: it publishes the
//! refusal on [`ControllerHandle::watch_fatal`](crate::ControllerHandle), then
//! cancels the listener and the engine, and every later submit fails with
//! [`RaftError::Shutdown`](crate::RaftError). A combined broker turns that
//! signal into a failed `Broker::start` or, once started, into its
//! self-shutdown, and `krabka-broker` exits non-zero with the refusal, so the
//! process never runs on without its controller.
//!
//! At startup the guard reads the fully recovered image before the engine
//! starts or repairs a downgrade checkpoint, and then that image with every
//! feature level that the log holds above its recovered high watermark
//! applied. That high watermark comes from a cache file and can lag, so a
//! level it misses would otherwise reach the engine, which would write to the
//! log before it refused the level. At runtime the guard reads each image the
//! engine publishes.
//! The image keeps
//! the last level of each feature, so a record that a later record superseded
//! is not named. Kafka would name it, and refuses the same logs.

use {
    crate::{config::UnstableFeatureVersions, supported_feature_range},
    krabka_metadata::MetadataImage,
};

/// Kafka's `VersionRange.toString()`: a single level as itself, and a span as
/// `min-max`.
fn range_text(min: i16, max: i16) -> String {
    if min == max {
        min.to_string()
    } else {
        format!("{min}-{max}")
    }
}

/// Kafka's `QuorumFeatures.DISABLED`, the range of a feature that the
/// controller does not support.
const UNKNOWN_FEATURE_RANGE: LevelRange = LevelRange { min: 0, max: 0 };

/// The levels a controller supports of one feature, inclusive.
struct LevelRange {
    min: i16,
    max: i16,
}

/// The refusal for the first finalized feature of `image` whose level this
/// controller does not support under `unstable`, or `None` when it supports
/// them all.
pub(super) fn unsupported_feature_level(
    image: &MetadataImage,
    unstable: UnstableFeatureVersions,
) -> Option<String> {
    image.finalized_features().iter().find_map(|(name, level)| {
        // `QuorumFeatures.localSupportedFeature`: a feature this controller
        // does not know is `DISABLED`, supported at level 0 only.
        let LevelRange { min, max } =
            krabka_metadata::feature(name).map_or(UNKNOWN_FEATURE_RANGE, |feature| {
                let (min, max) = supported_feature_range(feature, unstable);
                LevelRange { min, max }
            });
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
    use {
        super::*,
        assert2::check,
        krabka_metadata::{FeatureLevelRecord, MetadataRecord},
        uuid::Uuid,
    };

    /// An image that finalized each of `levels`.
    fn image_at(levels: &[FeatureLevelRecord]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for level in levels {
            image.apply(&MetadataRecord::V1FeatureLevel(level.clone()));
        }
        image
    }

    fn finalized(name: &str, level: i16) -> FeatureLevelRecord {
        FeatureLevelRecord {
            name: name.into(),
            level,
        }
    }

    fn refusal(name: &str, level: i16, range: &str) -> String {
        format!(
            "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='{name}', \
             featureLevel={level}), but this controller only supports versions {range}"
        )
    }

    use {
        rstest::fixture,
        rstest_bdd::{Slot, datatable::Rows},
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    };

    struct RecoveredFeatures {
        image: Option<MetadataImage>,
        unstable: Option<UnstableFeatureVersions>,
        refusal: Slot<Option<String>>,
    }

    #[fixture]
    fn recovered() -> RecoveredFeatures {
        // Start this scenario with fresh inputs and observations.

        RecoveredFeatures {
            image: None,
            unstable: None,
            refusal: Slot::default(),
        }
    }

    #[given("unstable feature versions are enabled: {enabled}")]
    fn feature_policy(recovered: &mut RecoveredFeatures, enabled: bool) {
        recovered.unstable = Some(if enabled {
            UnstableFeatureVersions::Enabled
        } else {
            UnstableFeatureVersions::Disabled
        });
    }

    #[derive(DataTableRow)]
    struct FinalizedFeature {
        name: String,
        level: i16,
    }

    #[given("the recovered metadata image finalizes these feature levels:")]
    fn recovered_image(
        recovered: &mut RecoveredFeatures,
        #[datatable] rows: Rows<FinalizedFeature>,
    ) {
        let levels: Vec<_> = rows
            .into_iter()
            .map(|row| finalized(&row.name, row.level))
            .collect();
        recovered.image = Some(image_at(&levels));
    }

    #[when("the controller checks recovered feature compatibility")]
    fn check_recovered_features(recovered: &mut RecoveredFeatures) {
        recovered.refusal.set(unsupported_feature_level(
            recovered
                .image
                .as_ref()
                .expect("a recovered image was given"),
            recovered.unstable.expect("feature policy was configured"),
        ));
    }

    #[then("every recovered feature level is supported")]
    fn supported_recovered_image(recovered: &RecoveredFeatures) {
        check!(recovered.refusal.get() == Some(None));
    }

    #[then("feature {name} at level {level} is refused with Kafka supported range {range}")]
    fn refused_recovered_image(
        recovered: &RecoveredFeatures,
        name: String,
        level: i16,
        range: String,
    ) {
        check!(recovered.refusal.get() == Some(Some(refusal(&name, level, &range))));
    }

    scenarios!("tests/features/migrated/src/controller/feature_check.feature", fixtures = [recovered: RecoveredFeatures]);
}
