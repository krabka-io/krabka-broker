//! Resolution of the bootstrap `metadata.version` and of the KIP-1022 feature
//! levels a format finalizes.
//!
//! `--release-version` and `--feature` both decide what the seed
//! `FeatureLevelRecord`s say, and the two interact: one sets the base release
//! and the other overrides individual features, except for `metadata.version`
//! where they conflict. The rules and the validation `kafka-storage format`
//! performs live here, apart from the flag definitions they read.

use std::collections::BTreeMap;

use krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE;
use krabka_raft::UnstableFeatureVersions;

/// The highest `metadata.version` this format accepts under `unstable`:
/// Kafka 4.3's latest production level, or with unstable feature versions the
/// highest level krabka knows (Kafka's `MetadataVersion.latestTesting()`).
fn metadata_version_ceiling(unstable: UnstableFeatureVersions) -> i16 {
    krabka_metadata::feature(krabka_metadata::metadata_version::METADATA_VERSION_FEATURE)
        .map_or(LATEST_PRODUCTION_METADATA_VERSION, |feature| {
            krabka_raft::supported_feature_range(feature, unstable).1
        })
}

/// Map a release string to a supported `metadata.version` feature level, as
/// Kafka's `MetadataVersion.fromVersionString(versionString,
/// unstableFeatureVersionsEnabled)` does: an unknown release, or one past the
/// latest production level while unstable feature versions are off, is
/// refused with Kafka's message, which lists the releases that are accepted.
fn resolve_release_level(s: &str, unstable: UnstableFeatureVersions) -> Result<i16, String> {
    let ceiling = metadata_version_ceiling(unstable);
    krabka_metadata::metadata_version::from_version_string(s)
        .map(krabka_metadata::metadata_version::MetadataVersion::feature_level)
        .filter(|level| {
            krabka_metadata::metadata_version::is_supported_level(*level) && *level <= ceiling
        })
        .ok_or_else(|| {
            let supported: Vec<&str> = (krabka_metadata::metadata_version::METADATA_VERSION_MIN
                ..=ceiling)
                .filter_map(krabka_metadata::metadata_version::from_feature_level)
                .map(krabka_metadata::metadata_version::MetadataVersion::ivn)
                .collect();
            format!(
                "Unknown metadata.version '{s}'. Supported metadata.version are: {}",
                supported.join(", ")
            )
        })
}

/// Parse one `--feature NAME=LEVEL` spec into `(name, level)`.
pub(super) fn parse_feature_spec(s: &str) -> Result<(String, i16), String> {
    let (name, level) = s
        .split_once('=')
        .ok_or("--feature must be NAME=LEVEL, e.g. transaction.version=2")?;
    let name = name.trim();
    if name.is_empty() {
        return Err("feature name must not be empty".into());
    }
    let level: i16 = level
        .trim()
        .parse()
        .map_err(|e| format!("feature level: {e}"))?;
    Ok((name.to_string(), level))
}

/// The `metadata.version` a cluster formats at when neither `--release-version`
/// nor `--feature metadata.version` names one: Kafka 4.3's
/// `MetadataVersion.LATEST_PRODUCTION`, `4.3-IV0`.
///
/// The levels above it that the feature table carries, `4.4-IV0` to
/// `4.4-IV2`, are Kafka trunk's unstable versions. A stock 4.3 node or tool
/// does not know them, so a format names one only under
/// `--unstable-feature-versions-enable`, Kafka's
/// `unstable.feature.versions.enable`.
pub const LATEST_PRODUCTION_METADATA_VERSION: i16 = krabka_raft::LATEST_PRODUCTION_METADATA_VERSION;

/// Resolve `krabka format`'s KIP-1022 feature flags into the bootstrap
/// `metadata.version` level and the per-feature override map, applying the
/// validation `kafka-storage format` performs:
///
/// - every `--feature` names a registered feature, finalized in its supported
///   range (else reject);
/// - `--feature metadata.version=X` conflicts with `--release-version`, and a
///   level past the latest production one is refused as `not yet stable`
///   unless `unstable` allows it (`Formatter.verifyReleaseVersion`);
/// - `bootstrap_mv` = `--feature metadata.version` if set, else
///   `--release-version`, else [`LATEST_PRODUCTION_METADATA_VERSION`], or the
///   highest level krabka knows under `unstable` (`latestTesting`);
/// - the fully-resolved feature set satisfies every KIP-1022 dependency.
pub(super) fn resolve_format_features(
    release_version: Option<&str>,
    features: &[(String, i16)],
    unstable: UnstableFeatureVersions,
) -> Result<(i16, BTreeMap<String, i16>), String> {
    use krabka_metadata::metadata_version::METADATA_VERSION_FEATURE;

    let mut overrides: BTreeMap<String, i16> = BTreeMap::new();
    let mut feature_mv: Option<i16> = None;

    for (name, level) in features {
        // KIP-853 persists kraft.version as a raft control record, never as a
        // FeatureLevelRecord. Its mode-specific validation happens separately.
        if name == KRAFT_VERSION_FEATURE {
            continue;
        }
        let Some(feat) = krabka_metadata::feature(name) else {
            let mut known: Vec<&str> = krabka_metadata::feature_registry()
                .iter()
                .map(|f| f.name())
                .collect();
            known.sort_unstable();
            return Err(format!(
                "Unsupported feature: {name}. Supported features are: {}",
                known.join(", ")
            ));
        };
        let (min, max) = feat.supported_range();
        if name == METADATA_VERSION_FEATURE
            && (min..=max).contains(level)
            && *level > metadata_version_ceiling(unstable)
        {
            let ivn = krabka_metadata::metadata_version::from_feature_level(*level)
                .map_or_else(|| level.to_string(), |mv| mv.ivn().to_owned());
            return Err(format!("metadata.version {ivn} is not yet stable."));
        }
        if *level < min || *level > max {
            return Err(format!(
                "feature {name}={level} is outside the supported range {min}..={max}"
            ));
        }
        if name == METADATA_VERSION_FEATURE {
            if release_version.is_some() {
                return Err(
                    "Use --release-version instead of --feature metadata.version=X to avoid ambiguity.".into(),
                );
            }
            feature_mv = Some(*level);
        }
        if overrides.insert(name.clone(), *level).is_some() {
            return Err(format!("feature {name} specified more than once"));
        }
    }

    let bootstrap_mv = if let Some(mv) = feature_mv {
        mv
    } else if let Some(rv) = release_version {
        resolve_release_level(rv, unstable)?
    } else {
        metadata_version_ceiling(unstable)
    };

    // KIP-1022 dependency validation over the fully-resolved feature set
    // (every registered feature at its override-or-default level).
    let resolved: BTreeMap<String, i16> = krabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let level = overrides
                .get(f.name())
                .copied()
                .unwrap_or_else(|| f.default_level(bootstrap_mv));
            (f.name().to_string(), level)
        })
        .collect();
    krabka_metadata::validate_feature_dependencies(&resolved)?;

    Ok((bootstrap_mv, overrides))
}

#[cfg(test)]
mod tests {

    use assert2::check;
    use krabka_metadata::MetadataRecord;

    use super::*;

    /// `unstable.feature.versions.enable` at Kafka's default.
    const STRICT: UnstableFeatureVersions = UnstableFeatureVersions::Disabled;

    /// A level equal to the supported minimum is in range. Every other case
    /// sits strictly inside the range or well outside it, so relaxing the
    /// guard from `<` to `<=` -- which rejects the minimum itself -- changed
    /// nothing any test looked at.
    #[test]
    fn resolve_features_accepts_a_level_at_the_supported_minimum() {
        // group.version supports 0..=1; metadata.version 7..=25.
        check!(resolve_format_features(None, &[("group.version".into(), 0)], STRICT).is_ok());
        check!(resolve_format_features(None, &[("metadata.version".into(), 7)], STRICT).is_ok());
    }

    #[test]
    fn release_version_maps_to_feature_level() {
        for (input, want) in [
            ("4.0", Some(25)),
            ("3.7-IV4", Some(19)),
            ("2.8", None),     // below MIN / unknown
            ("9.9-IV0", None), // unknown
        ] {
            assert2::assert!(resolve_release_level(input, STRICT).ok() == want);
        }
    }

    #[test]
    fn bootstrap_seeds_every_nonzero_feature_at_release_default() {
        let bootstrap_mv = krabka_metadata::metadata_version::from_version_string("4.0")
            .unwrap()
            .feature_level();
        // Exercises the exact helper `run()` uses, so it tracks the registry
        // as features are added in later tasks. Features whose release default
        // is 0 are omitted (KIP-1022: level 0 = absent = disabled), matching
        // `kafka-storage format`.
        let records = krabka_metadata::bootstrap_feature_records(bootstrap_mv);
        for feat in krabka_metadata::feature_registry() {
            let found = records.iter().find_map(|r| match r {
                MetadataRecord::V1FeatureLevel(f) if f.name == feat.name() => Some(f.level),
                _ => None,
            });
            let expected = feat.default_level(bootstrap_mv);
            if expected > 0 {
                assert2::assert!(found == Some(expected));
            } else {
                assert2::assert!(found.is_none());
            }
        }
    }

    // The no-flag default path (`LATEST_PRODUCTION_METADATA_VERSION`) is
    // covered end-to-end by `format_smoke.rs`, which formats without
    // `--release-version` and asserts the FeatureLevel record is present.
    #[test]
    fn short_release_string_resolves_to_its_last_iv() {
        assert2::assert!(resolve_release_level("4.0", STRICT).unwrap() == 25);
    }

    #[test]
    fn parse_feature_spec_happy_path() {
        assert2::assert!(
            parse_feature_spec("group.version=1").unwrap() == ("group.version".to_string(), 1)
        );
        assert2::assert!(
            parse_feature_spec("metadata.version=20").unwrap()
                == ("metadata.version".to_string(), 20)
        );
    }

    #[test]
    fn parse_feature_spec_error_branches() {
        for bad in [
            "noequals",          // missing '='
            "group.version=abc", // non-integer level
            "group.version=",    // empty level
            "=1",                // empty name
        ] {
            assert2::assert!(parse_feature_spec(bad).is_err());
        }
    }

    #[test]
    fn resolve_features_defaults_bootstrap_mv_to_latest_production() {
        // No --release-version, no metadata.version override → bootstrap at
        // 4.3-IV0, not at trunk's unstable levels; an explicit non-metadata
        // feature becomes an override.
        let (mv, ov) =
            resolve_format_features(None, &[("group.version".into(), 1)], STRICT).expect("resolve");
        assert2::assert!(mv == LATEST_PRODUCTION_METADATA_VERSION);
        assert2::assert!(ov.get("group.version") == Some(&1));
    }

    #[test]
    fn resolve_features_metadata_version_feature_sets_bootstrap_mv() {
        let (mv, ov) = resolve_format_features(None, &[("metadata.version".into(), 20)], STRICT)
            .expect("resolve");
        assert2::assert!(mv == 20);
        assert2::assert!(ov.get("metadata.version") == Some(&20));
    }

    #[test]
    fn resolve_features_release_version_sets_bootstrap_mv() {
        let (mv, ov) = resolve_format_features(Some("4.0-IV0"), &[], STRICT).expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.is_empty());
    }

    #[test]
    fn resolve_features_release_and_feature_combine() {
        // --release-version sets the base; a non-metadata --feature overrides it.
        let (mv, ov) = resolve_format_features(
            Some("4.0-IV0"),
            &[("transaction.version".into(), 2)],
            STRICT,
        )
        .expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.get("transaction.version") == Some(&2));
    }

    #[test]
    fn resolve_features_rejects_release_plus_metadata_version_feature() {
        // Ambiguity: both --release-version and --feature metadata.version set MV.
        let err =
            resolve_format_features(Some("4.0-IV0"), &[("metadata.version".into(), 24)], STRICT)
                .unwrap_err();
        assert2::assert!(err.contains("metadata.version"));
    }

    #[test]
    fn resolve_features_rejects_unknown_feature() {
        let err =
            resolve_format_features(None, &[("bogus.version".into(), 1)], STRICT).unwrap_err();
        assert2::assert!(err.contains("Unsupported feature"));
        assert2::assert!(err.contains("bogus.version"));
    }

    #[test]
    fn resolve_features_rejects_out_of_range_level() {
        for (name, level) in [
            ("group.version", 5),     // group.version supports 0..=1
            ("metadata.version", 99), // metadata.version supports 7..=25
            ("metadata.version", 1),
        ] {
            assert2::assert!(
                resolve_format_features(None, &[(name.into(), level)], STRICT).is_err()
            );
        }
    }

    /// #784: a Kafka trunk `metadata.version` (4.4-IV0 to 4.4-IV2) is
    /// refused with Kafka 4.3.1's `kafka-storage format` messages unless
    /// unstable feature versions are enabled, and the default release is
    /// `latestTesting` when they are.
    #[test]
    fn unstable_metadata_versions_follow_unstable_feature_versions_enable() {
        type Case<'a> = (
            Option<&'a str>,
            Vec<(String, i16)>,
            UnstableFeatureVersions,
            Result<i16, String>,
        );
        let trunk = UnstableFeatureVersions::Enabled;
        let known = "Unknown metadata.version '4.4'. Supported metadata.version are: \
                     3.3-IV3, 3.4-IV0, 3.5-IV0, 3.5-IV1, 3.5-IV2, 3.6-IV0, 3.6-IV1, 3.6-IV2, \
                     3.7-IV0, 3.7-IV1, 3.7-IV2, 3.7-IV3, 3.7-IV4, 3.8-IV0, 3.9-IV0, 4.0-IV0, \
                     4.0-IV1, 4.0-IV2, 4.0-IV3, 4.1-IV0, 4.1-IV1, 4.2-IV0, 4.2-IV1, 4.3-IV0";
        let mv = |level| vec![("metadata.version".to_string(), level)];
        let cases: Vec<Case<'_>> = vec![
            (None, vec![], STRICT, Ok(30)),
            (None, vec![], trunk, Ok(33)),
            (Some("4.4"), vec![], STRICT, Err(known.to_owned())),
            (Some("4.4-IV0"), vec![], trunk, Ok(31)),
            (Some("4.4"), vec![], trunk, Ok(33)),
            (
                None,
                mv(31),
                STRICT,
                Err("metadata.version 4.4-IV0 is not yet stable.".to_owned()),
            ),
            (
                None,
                mv(33),
                STRICT,
                Err("metadata.version 4.4-IV2 is not yet stable.".to_owned()),
            ),
            (None, mv(30), STRICT, Ok(30)),
            (None, mv(32), trunk, Ok(32)),
        ];
        for (release, features, unstable, want) in cases {
            check!(
                resolve_format_features(release, &features, unstable).map(|(mv, _)| mv) == want,
                "{release:?} {features:?} {unstable:?}"
            );
        }
    }

    #[test]
    fn resolve_features_rejects_bad_release_string() {
        assert2::assert!(resolve_format_features(Some("2.8"), &[], STRICT).is_err());
    }
}
