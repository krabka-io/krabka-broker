//! The KIP-584 feature rows that an `ApiVersions` response carries: the
//! `supported_features` range this broker compiles in, and the
//! `finalized_features` levels it reads out of the live metadata image.

use krabka_protocol::owned::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey};

/// First `ApiVersions` version whose JVM client accepts `kraft.version` minimum zero.
const KRAFT_ZERO_MIN_API_VERSION: i16 = 4;

// KIP-584 feature surface. `supported_features` advertises `metadata.version`
// over the full Kafka-faithful range MIN=7 (3.3-IV3) .. MAX=25 (4.0-IV3),
// sourced from the `krabka_metadata::metadata_version` table via
// `crate::features`. `finalized_features` + the epoch are read from the live
// metadata image: a fresh (unformatted) broker surfaces no finalized features
// and epoch `-1` (`MetadataVersion.UNKNOWN` to JVM clients) until a
// `V1FeatureLevel` is seeded by `krabka format --release-version` or
// `UpdateFeatures` (api_key 57) lands one.

pub(super) fn supported_feature_keys(api_version: i16) -> Vec<SupportedFeatureKey> {
    crate::features::supported_features()
        .iter()
        .map(|f| SupportedFeatureKey {
            name: f.name.to_string(),
            // `kraft.version` is the KIP-853 exception whose supported range
            // includes level zero. Other disable-at-zero features keep the
            // legacy JVM-compatible clamp.
            min_version: if f.name == krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                && api_version >= KRAFT_ZERO_MIN_API_VERSION
            {
                f.min_version
            } else {
                f.min_version.max(1)
            },
            max_version: f.max_version,
            ..Default::default()
        })
        .collect()
}

pub(super) fn finalized_feature_keys(
    image: &krabka_metadata::MetadataImage,
) -> Vec<FinalizedFeatureKey> {
    let mut features: Vec<_> = image
        .finalized_features()
        .iter()
        .map(|(name, level)| FinalizedFeatureKey {
            name: name.clone(),
            // Kafka reports the finalized level as both the min and max
            // finalized version level.
            max_version_level: *level,
            min_version_level: *level,
            ..Default::default()
        })
        .collect();
    let kraft_version = i16::try_from(image.kraft_version()).unwrap_or(i16::MAX);
    features.push(FinalizedFeatureKey {
        name: krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE.into(),
        max_version_level: kraft_version,
        min_version_level: kraft_version,
        ..Default::default()
    });
    features
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};

    use super::*;

    // ── KIP-584 feature surface ────────────────────────────────────────────

    #[test]
    fn supported_features_advertise_version_compatible_kraft_range() {
        let keys = supported_feature_keys(4);
        let mv = keys
            .iter()
            .find(|k| k.name == "metadata.version")
            .expect("metadata.version advertised");
        assert!(mv.min_version == crate::features::METADATA_VERSION_MIN);
        assert!(mv.max_version == crate::features::METADATA_VERSION_MAX);
        let kraft = keys
            .iter()
            .find(|key| key.name == "kraft.version")
            .expect("kraft.version advertised");
        assert!((kraft.min_version, kraft.max_version) == (0, 1));

        let legacy_kraft = supported_feature_keys(3)
            .into_iter()
            .find(|key| key.name == "kraft.version")
            .expect("kraft.version advertised");
        assert!((legacy_kraft.min_version, legacy_kraft.max_version) == (1, 1));
    }

    #[test]
    fn fresh_image_surfaces_finalized_kraft_version_zero() {
        // A fresh metadata image (no `UpdateFeatures` ever applied) has no
        // finalized features and the schema sentinel epoch `-1`, which JVM
        // clients consume as `MetadataVersion.UNKNOWN`.
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let features = finalized_feature_keys(&image);
        assert!(features.len() == 1);
        assert!(features[0].name == "kraft.version");
        assert!(features[0].min_version_level == 0 && features[0].max_version_level == 0);
        assert!(image.finalized_features_epoch() == -1);
    }

    #[test]
    fn finalized_feature_keys_preserve_names_and_levels() {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 24,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));

        let keys = finalized_feature_keys(&image);

        assert!(keys.len() == 3, "{keys:?}");
        let mv = keys
            .iter()
            .find(|k| k.name == "metadata.version")
            .expect("metadata.version finalized");
        assert!(mv.max_version_level == 24, "{keys:?}");
        assert!(mv.min_version_level == 24, "{keys:?}");
        let gv = keys
            .iter()
            .find(|k| k.name == "group.version")
            .expect("group.version finalized");
        assert!(gv.max_version_level == 1, "{keys:?}");
        assert!(gv.min_version_level == 1, "{keys:?}");
    }
}
