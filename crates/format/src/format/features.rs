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

/// Map a release string to a supported `metadata.version` feature level,
/// erroring if it is unknown or outside `[MIN, MAX]`.
fn resolve_release_level(s: &str) -> Result<i16, String> {
    let mv = krabka_metadata::metadata_version::from_version_string(s)
        .ok_or_else(|| format!("unknown metadata.version {s:?}"))?;
    let level = mv.feature_level();
    if !krabka_metadata::metadata_version::is_supported_level(level) {
        return Err(format!(
            "metadata.version {s:?} (level {level}) is outside the supported range"
        ));
    }
    Ok(level)
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

/// Resolve `krabka format`'s KIP-1022 feature flags into the bootstrap
/// `metadata.version` level and the per-feature override map, applying the
/// validation `kafka-storage format` performs:
///
/// - every `--feature` names a registered feature, finalized in its supported
///   range (else reject);
/// - `--feature metadata.version=X` conflicts with `--release-version`;
/// - `bootstrap_mv` = `--feature metadata.version` if set, else
///   `--release-version`, else the newest supported level (latest stable);
/// - the fully-resolved feature set satisfies every KIP-1022 dependency.
pub(super) fn resolve_format_features(
    release_version: Option<&str>,
    features: &[(String, i16)],
) -> Result<(i16, BTreeMap<String, i16>), String> {
    use krabka_metadata::metadata_version::{METADATA_VERSION_FEATURE, METADATA_VERSION_MAX};

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
        resolve_release_level(rv)?
    } else {
        METADATA_VERSION_MAX
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

    /// A level equal to the supported minimum is in range. Every other case
    /// sits strictly inside the range or well outside it, so relaxing the
    /// guard from `<` to `<=` -- which rejects the minimum itself -- changed
    /// nothing any test looked at.
    #[test]
    fn resolve_features_accepts_a_level_at_the_supported_minimum() {
        // group.version supports 0..=1; metadata.version 7..=25.
        check!(resolve_format_features(None, &[("group.version".into(), 0)]).is_ok());
        check!(resolve_format_features(None, &[("metadata.version".into(), 7)]).is_ok());
    }

    #[test]
    fn release_version_maps_to_feature_level() {
        for (input, want) in [
            ("4.0", Some(25)),
            ("3.7-IV4", Some(19)),
            ("2.8", None),     // below MIN / unknown
            ("9.9-IV0", None), // unknown
        ] {
            assert2::assert!(resolve_release_level(input).ok() == want);
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

    // The no-flag default path (`Ok(None) => METADATA_VERSION_MAX` in `run()`)
    // is covered end-to-end by `format_smoke.rs`, which formats without
    // `--release-version` and asserts the FeatureLevel record is present.
    #[test]
    fn max_version_string_resolves_to_max() {
        assert2::assert!(
            resolve_release_level("4.0").unwrap()
                == krabka_metadata::metadata_version::METADATA_VERSION_MAX
        );
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
    fn resolve_features_defaults_bootstrap_mv_to_max() {
        // No --release-version, no metadata.version override → bootstrap at MAX;
        // an explicit non-metadata feature becomes an override.
        let (mv, ov) =
            resolve_format_features(None, &[("group.version".into(), 1)]).expect("resolve");
        assert2::assert!(mv == krabka_metadata::metadata_version::METADATA_VERSION_MAX);
        assert2::assert!(ov.get("group.version") == Some(&1));
    }

    #[test]
    fn resolve_features_metadata_version_feature_sets_bootstrap_mv() {
        let (mv, ov) =
            resolve_format_features(None, &[("metadata.version".into(), 20)]).expect("resolve");
        assert2::assert!(mv == 20);
        assert2::assert!(ov.get("metadata.version") == Some(&20));
    }

    #[test]
    fn resolve_features_release_version_sets_bootstrap_mv() {
        let (mv, ov) = resolve_format_features(Some("4.0-IV0"), &[]).expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.is_empty());
    }

    #[test]
    fn resolve_features_release_and_feature_combine() {
        // --release-version sets the base; a non-metadata --feature overrides it.
        let (mv, ov) =
            resolve_format_features(Some("4.0-IV0"), &[("transaction.version".into(), 2)])
                .expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.get("transaction.version") == Some(&2));
    }

    #[test]
    fn resolve_features_rejects_release_plus_metadata_version_feature() {
        // Ambiguity: both --release-version and --feature metadata.version set MV.
        let err = resolve_format_features(Some("4.0-IV0"), &[("metadata.version".into(), 24)])
            .unwrap_err();
        assert2::assert!(err.contains("metadata.version"));
    }

    #[test]
    fn resolve_features_rejects_unknown_feature() {
        let err = resolve_format_features(None, &[("bogus.version".into(), 1)]).unwrap_err();
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
            assert2::assert!(resolve_format_features(None, &[(name.into(), level)]).is_err());
        }
    }

    #[test]
    fn resolve_features_rejects_bad_release_string() {
        assert2::assert!(resolve_format_features(Some("2.8"), &[]).is_err());
    }
}
