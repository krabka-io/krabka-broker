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

/// The `metadata.version` level at which CIDR-based ACL host patterns
/// (KIP-1276) are accepted: upstream Kafka's `4.4-IV1`,
/// `MetadataVersion.isCidrAclSupported`.
///
/// `krabka_metadata::metadata_version`'s table -- the canonical
/// level<->`X.Y-IVn` mapping this broker advertises and negotiates -- ends at
/// `METADATA_VERSION_MAX` (`4.0-IV3`, level 25) and has no `4.4-IV1` entry
/// yet, so this constant sits one level past that ceiling rather than naming
/// a table entry that does not exist. `require_feature` only compares
/// integers, so the gate below is already correct: no real cluster can
/// finalize a level this high today (`UpdateFeatures` rejects any level
/// outside `[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`), so `CreateAcls`
/// answers every CIDR host with the same `UNSUPPORTED_VERSION` Kafka gives
/// below `4.4-IV1` -- correct present-day behavior, matching the "Kafka does
/// not support this yet either, at this metadata version" reality. The day
/// `krabka_metadata`'s table grows a real `4.4-IV1` entry, this constant
/// should be redefined against it instead of `METADATA_VERSION_MAX + 1`.
pub(crate) const CIDR_ACL_HOST_MIN_LEVEL: i16 =
    krabka_metadata::metadata_version::METADATA_VERSION_MAX + 1;

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
/// as at most [`krabka_metadata::metadata_version::METADATA_VERSION_MAX`]
/// (today's real ceiling), not as an unconditional pass. A pre-bootstrap or
/// legacy image that has never finalized `metadata.version` is still bound
/// by whatever level the broker binary itself actually supports, and this
/// binary supports at most `METADATA_VERSION_MAX`, below
/// [`CIDR_ACL_HOST_MIN_LEVEL`].
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

    #[test]
    fn supported_features_include_metadata_version() {
        let expected = SupportedFeature {
            name: METADATA_VERSION,
            min_version: METADATA_VERSION_MIN,
            max_version: METADATA_VERSION_MAX,
        };
        assert!(lookup(METADATA_VERSION) == Some(expected));
        assert!(lookup("not.a.feature").is_none());
        assert!(
            lookup(krabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_FEATURE)
                .is_none()
        );
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
