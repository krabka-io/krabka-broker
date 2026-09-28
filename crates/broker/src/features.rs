//! KIP-584 supported-feature surface for the broker. This module re-exports
//! the `krabka_metadata` feature registry and derives the `ApiVersions`
//! advertisement rows from it, so the advertised and the validated feature
//! sets can never disagree. The behavioral gating helper `require_feature`
//! lives here because it returns broker error codes.

use krabka_metadata::MetadataImage;
/// The `eligible.leader.replicas.version` feature name (KIP-966). It gates the
/// controller's ELR bookkeeping, which `ElrPublisher::extend` reads with
/// `feature_enabled`, and `UpdateFeatures` clears the published state when it
/// is finalized back to 0.
pub(crate) use krabka_metadata::metadata_version::ELR_VERSION_FEATURE as ELR_VERSION;
pub(crate) use krabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
// Re-exported for `ApiVersions` tests / range-bound assertions; consumed only
// from `#[cfg(test)]` modules, so the non-test lib target sees them as unused.
#[cfg(test)]
pub(crate) use krabka_metadata::metadata_version::METADATA_VERSION_MAX;
#[cfg(test)]
pub(crate) use krabka_metadata::metadata_version::METADATA_VERSION_MIN;
/// The `share.version` feature name (KIP-932). Only the `#[cfg(test)]`
/// module that asserts share.version is advertised uses it.
#[cfg(test)]
pub(crate) use krabka_metadata::metadata_version::SHARE_VERSION_FEATURE as SHARE_VERSION;
/// The `streams.version` feature name (KIP-1071). It gates
/// `StreamsGroupHeartbeat` and `StreamsGroupDescribe`. Those handlers read it
/// with `feature_enabled`.
pub(crate) use krabka_metadata::metadata_version::STREAMS_VERSION_FEATURE as STREAMS_VERSION;

/// The `metadata.version` a self-bootstrapped cluster finalizes: Kafka 4.3's
/// `MetadataVersion.LATEST_PRODUCTION`, `4.3-IV0`. It is the level
/// `krabka format` picks when no release is named, and the two must agree.
///
/// The feature table also carries Kafka trunk's unstable `4.4-IV0` and
/// `4.4-IV1`. A stock 4.3 node or tool does not know them, so a cluster
/// reaches them only through `UpdateFeatures` or an explicit
/// `krabka format --release-version`.
pub(crate) const LATEST_PRODUCTION_METADATA_VERSION: i16 = 30;

/// The `metadata.version` level at which CIDR-based ACL host patterns
/// (KIP-1276) are accepted: upstream Kafka's `4.4-IV1`,
/// `MetadataVersion.isCidrAclSupported`.
pub(crate) const CIDR_ACL_HOST_MIN_LEVEL: i16 =
    krabka_metadata::metadata_version::CIDR_ACL_MIN_LEVEL;

/// Kafka's `MetadataVersion.didMetadataChange`, one entry per level from
/// `METADATA_VERSION_MIN` (7, `3.3-IV3`) upward. A `true` level added or
/// changed a metadata record, so a downgrade across it may lose metadata.
///
/// Levels 7 to 30 are transcribed from `MetadataVersion.java` at Kafka 4.3.1.
/// Levels 31 (`4.4-IV0`) and 32 (`4.4-IV1`) are from Kafka trunk, the only
/// place they are defined.
const DID_METADATA_CHANGE: [bool; 26] = [
    true,  // 7   3.3-IV3: InControlledShutdown in broker registration (KIP-841)
    true,  // 8   3.4-IV0: ZK migration records
    false, // 9   3.5-IV0: tiered storage (KIP-405)
    false, // 10  3.5-IV1: replica epoch in Fetch (KIP-903)
    true,  // 11  3.5-IV2: KRaft SCRAM
    false, // 12  3.6-IV0: no leader epoch bump on ISR shrink
    true,  // 13  3.6-IV1: metadata transactions
    true,  // 14  3.6-IV2: KRaft delegation tokens
    true,  // 15  3.7-IV0: controller registration (KIP-919)
    false, // 16  3.7-IV1: reserved
    true,  // 17  3.7-IV2: JBOD in KRaft
    false, // 18  3.7-IV3: reserved
    false, // 19  3.7-IV4: Fetch version for KIP-951
    false, // 20  3.8-IV0
    false, // 21  3.9-IV0: ListOffsets v9 (KIP-1005)
    false, // 22  4.0-IV0: group.version 1 bootstrap (KIP-848)
    true,  // 23  4.0-IV1: ELR fields and ClearElrRecord (KIP-966)
    false, // 24  4.0-IV2: transaction.version bootstrap (KIP-890)
    false, // 25  4.0-IV3: async remote LIST_OFFSETS (KIP-1075)
    false, // 26  4.1-IV0: ELR on by default (KIP-966)
    false, // 27  4.1-IV1: replica fetcher FETCH v18
    false, // 28  4.2-IV0: share groups by default (KIP-932)
    false, // 29  4.2-IV1: streams groups by default (KIP-1071)
    true,  // 30  4.3-IV0: cordoned log dirs in broker registration
    false, // 31  4.4-IV0: share-group dead-letter queue
    true,  // 32  4.4-IV1: CIDR ACL host patterns (KIP-1276)
];

/// Kafka's `MetadataVersion.checkIfMetadataChanged`: whether any level in
/// `(low, high]` changed metadata, where `low` and `high` are the two
/// arguments in ascending order. Equal levels never changed metadata.
///
/// Kafka walks down from the higher level until it meets a changed level or
/// the lower one. A level outside the table has no predecessor for that walk,
/// which Kafka reads as a change, so it counts as one here too.
pub(crate) fn metadata_changed_between(source: i16, target: i16) -> bool {
    let (low, high) = if source <= target {
        (source, target)
    } else {
        (target, source)
    };
    (low.saturating_add(1)..=high).any(|level| {
        level
            .checked_sub(krabka_metadata::metadata_version::METADATA_VERSION_MIN)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| DID_METADATA_CHANGE.get(index))
            .copied()
            .unwrap_or(true)
    })
}

/// One row of the `ApiVersions.supported_features` advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features this broker supports finalizing. They come from the
/// `krabka_metadata` registry, the single source of truth.
pub(crate) fn supported_features() -> Vec<SupportedFeature> {
    krabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let (min_version, max_version) = f.supported_range();
            SupportedFeature {
                name: f.name(),
                min_version,
                max_version,
            }
        })
        .collect()
}

/// Look up a supported feature by name. It pairs with `supported_features` as
/// the module's feature-surface API. The `UpdateFeatures` handler resolves the
/// registry feature directly, so the non-test lib target sees this as unused.
#[cfg(test)]
pub(crate) fn lookup(name: &str) -> Option<SupportedFeature> {
    krabka_metadata::feature(name).map(|f| {
        let (min_version, max_version) = f.supported_range();
        SupportedFeature {
            name: f.name(),
            min_version,
            max_version,
        }
    })
}

/// KIP-584 admission gate. Returns `Err(UNSUPPORTED_VERSION)` when `name` is
/// finalized below `required_level`. It is permissive when the feature is
/// unfinalized, because there is no level to gate against. This matches how
/// the range guard treats a missing level.
///
/// This permissiveness is correct only for a feature whose own presence in
/// the registry is what changed -- a legacy image predates the feature
/// entirely, so there is nothing to compare against and the RPC must still
/// work. It is the wrong helper for [`CIDR_ACL_HOST_MIN_LEVEL`]: an
/// unfinalized `metadata.version` there is a real (if legacy or
/// pre-bootstrap) image whose effective level is at most
/// [`krabka_metadata::metadata_version::METADATA_VERSION_MAX`], never a
/// blank check. See [`cidr_hosts_supported`].
pub(crate) fn require_feature(
    image: &MetadataImage,
    name: &str,
    required_level: i16,
) -> Result<(), i16> {
    let finalized = image.finalized_features().get(name).copied();
    if finalized.is_some_and(|level| level < required_level) {
        Err(crate::codes::UNSUPPORTED_VERSION)
    } else {
        Ok(())
    }
}

/// KIP-1276 admission gate: whether `image` supports CIDR-range ACL hosts.
///
/// Unlike [`require_feature`], an unfinalized `metadata.version` is treated
/// as [`krabka_metadata::metadata_version::METADATA_VERSION_MAX`], the level
/// this binary supports, not as an unconditional pass.
pub(crate) fn cidr_hosts_supported(image: &MetadataImage) -> bool {
    image
        .finalized_features()
        .get(METADATA_VERSION)
        .copied()
        .unwrap_or(krabka_metadata::metadata_version::METADATA_VERSION_MAX)
        >= CIDR_ACL_HOST_MIN_LEVEL
}

/// True when `name` is finalized at >= `level`. It treats an UNFINALIZED
/// feature as level 0, which is disabled. Use it for features where absence
/// means "off", for example `group.version` → next-gen disabled. This differs
/// from `require_feature`, which is permissive on absence and serves
/// metadata.version-gated RPCs on legacy images.
pub(crate) fn feature_enabled(
    image: &krabka_metadata::MetadataImage,
    name: &str,
    level: i16,
) -> bool {
    image.finalized_features().get(name).copied().unwrap_or(0) >= level
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn feature_enabled_treats_absence_as_disabled() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        assert!(!feature_enabled(&image, "group.version", 1)); // absent → disabled
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));
        assert!(feature_enabled(&image, "group.version", 1)); // present at 1 → enabled
    }

    /// KIP-1276's gate: an image with no `metadata.version` is judged against
    /// this binary's highest level, 4.4-IV1, which admits CIDR hosts; a
    /// finalized level admits them only from 4.4-IV1.
    #[test]
    fn cidr_hosts_supported_follows_the_finalized_metadata_version() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        for (finalized, want) in [
            (None, true),
            (Some(LATEST_PRODUCTION_METADATA_VERSION), false),
            (Some(CIDR_ACL_HOST_MIN_LEVEL - 1), false),
            (Some(CIDR_ACL_HOST_MIN_LEVEL), true),
        ] {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = finalized {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: METADATA_VERSION.into(),
                    level,
                }));
            }
            assert!(cidr_hosts_supported(&image) == want, "{finalized:?}");
        }
    }

    /// Kafka's `checkIfMetadataChanged` walks from the higher level down
    /// and stops at the first level whose `didMetadataChange` is set, so the
    /// higher level's own flag counts and the lower level's does not.
    #[test]
    fn metadata_changed_between_walks_like_kafka() {
        for (source, target, want) in [
            (25, 25, false),
            (25, 24, false),
            (24, 25, false),
            (29, 24, false),
            (25, 22, true),
            (25, 23, false),
            (23, 22, true),
            (17, 15, true),
            (16, 15, false),
            (13, 12, true),
            (12, 11, false),
            (30, 29, true),
            (32, 31, true),
            (31, 30, false),
            (8, 7, true),
            (7, 6, true),
            (33, 32, true),
        ] {
            assert!(
                metadata_changed_between(source, target) == want,
                "{source} -> {target}"
            );
        }
    }

    /// The table covers exactly the levels this binary supports.
    #[test]
    fn did_metadata_change_covers_every_supported_level() {
        let levels = usize::try_from(METADATA_VERSION_MAX - METADATA_VERSION_MIN + 1).unwrap();
        assert!(DID_METADATA_CHANGE.len() == levels);
    }

    #[test]
    fn self_bootstrap_and_format_default_to_the_same_metadata_version() {
        assert!(
            LATEST_PRODUCTION_METADATA_VERSION == krabka_format::LATEST_PRODUCTION_METADATA_VERSION
        );
    }

    #[test]
    fn supported_features_include_metadata_version() {
        let expected = SupportedFeature {
            name: METADATA_VERSION,
            min_version: METADATA_VERSION_MIN,
            max_version: METADATA_VERSION_MAX,
        };
        assert!(lookup(METADATA_VERSION) == Some(expected));
        assert!(lookup("not.a.feature").is_none());
    }

    #[test]
    fn share_version_is_supported() {
        let expected = SupportedFeature {
            name: SHARE_VERSION,
            min_version: 0,
            max_version: 1,
        };
        assert!(lookup(SHARE_VERSION) == Some(expected));
        // Advertised via the registry-derived supported-feature table.
        assert!(
            supported_features()
                .iter()
                .any(|f| f.name == SHARE_VERSION && f.min_version == 0 && f.max_version == 1)
        );
    }

    #[test]
    fn streams_version_is_supported() {
        let expected = SupportedFeature {
            name: STREAMS_VERSION,
            min_version: 0,
            max_version: 1,
        };
        assert!(lookup(STREAMS_VERSION) == Some(expected));
        // Advertised via the registry-derived supported-feature table.
        assert!(
            supported_features()
                .iter()
                .any(|f| f.name == STREAMS_VERSION && f.min_version == 0 && f.max_version == 1)
        );
    }

    #[test]
    fn require_feature_is_permissive_on_unfinalized() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        assert!(require_feature(&image, METADATA_VERSION, 11).is_ok());
    }

    #[test]
    fn require_feature_gates_below_level() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: METADATA_VERSION.to_string(),
            level: 10,
        }));
        for (required_level, want) in [
            (11, Err(crate::codes::UNSUPPORTED_VERSION)),
            (10, Ok(())),
            (7, Ok(())),
        ] {
            assert!(
                require_feature(&image, METADATA_VERSION, required_level) == want,
                "level {required_level}"
            );
        }
    }
}
