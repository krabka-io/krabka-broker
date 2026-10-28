//! The KIP-584 feature rows that an `ApiVersions` response carries: the
//! `supported_features` range this broker compiles in, and the
//! `finalized_features` levels it reads out of the live metadata image, with
//! Kafka's minimum `metadata.version` before the first feature delta.
//!
//! The rules are Kafka 4.3.1's, and the controller listener answers with the
//! same ones: both call `krabka_raft`'s shared helpers, which hold the Kafka
//! references. In short, every feature is advertised from Kafka's minimum
//! production level -- 0 for all but `metadata.version` -- a request below v4
//! does not see a zero minimum (`alterFeatureLevel0`), and no finalized row
//! is at level 0, `kraft.version` included.

use {
    crate::features::supported_features,
    krabka_protocol::owned::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey},
};

/// The supported-feature rows of this broker's feature table at `api_version`,
/// under `unstable`.
pub(super) fn supported_feature_keys(
    api_version: i16,
    unstable: krabka_raft::UnstableFeatureVersions,
) -> Vec<SupportedFeatureKey> {
    supported_features(unstable)
        .iter()
        .filter_map(|feature| {
            krabka_raft::supported_feature_key(
                feature.name,
                feature.min_version,
                feature.max_version,
                api_version,
            )
        })
        .collect()
}

/// The finalized-feature rows of `image`: every level above 0.
pub(super) fn finalized_feature_keys(
    image: &krabka_metadata::MetadataImage,
) -> Vec<FinalizedFeatureKey> {
    krabka_raft::finalized_feature_keys(image)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::features::{LATEST_PRODUCTION_METADATA_VERSION, METADATA_VERSION_MIN},
        assert2::check,
        krabka_metadata::{FeatureLevelRecord, KRaftVersionRecord, MetadataImage, MetadataRecord},
        rstest_bdd::datatable::Rows,
        uuid::Uuid,
    };

    krabka_macros::supported_feature_fixture!(supported);

    krabka_macros::finalized_feature_fixture!(finalized);

    struct FeatureDiscoveryContract {
        version: i16,
        image: Option<MetadataImage>,
        supported: Option<Vec<SupportedFeatureKey>>,
        finalized: Option<Vec<FinalizedFeatureKey>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ExpectedFeatureRange {
        feature: String,
        minimum: String,
        maximum: String,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ExpectedFinalizedLevel {
        feature: String,
        level: String,
    }

    #[rstest::fixture]
    fn feature_discovery() -> FeatureDiscoveryContract {
        // Start this scenario with fresh inputs and observations.

        FeatureDiscoveryContract {
            version: 0,
            image: None,
            supported: None,
            finalized: None,
        }
    }

    fn feature_contract_level(value: &str) -> i16 {
        match value {
            "minimum-metadata" => METADATA_VERSION_MIN,
            "production-metadata" => LATEST_PRODUCTION_METADATA_VERSION,
            _ => value.parse().expect("an explicit feature level"),
        }
    }

    #[rstest_bdd_macros::given("the client uses ApiVersions version {version}")]
    fn feature_request_version(feature_discovery: &mut FeatureDiscoveryContract, version: i16) {
        feature_discovery.version = version;
    }

    #[rstest_bdd_macros::given("the broker has a fresh metadata image")]
    fn fresh_feature_image(feature_discovery: &mut FeatureDiscoveryContract) {
        feature_discovery.image = Some(MetadataImage::new(Uuid::nil()));
    }

    #[rstest_bdd_macros::when("the broker advertises production supported feature ranges")]
    fn advertise_supported_ranges(feature_discovery: &mut FeatureDiscoveryContract) {
        feature_discovery.supported = Some(supported_feature_keys(
            feature_discovery.version,
            krabka_raft::UnstableFeatureVersions::Disabled,
        ));
    }

    #[rstest_bdd_macros::when("the broker advertises the finalized feature levels")]
    fn advertise_finalized_levels(feature_discovery: &mut FeatureDiscoveryContract) {
        feature_discovery.finalized = Some(finalized_feature_keys(
            feature_discovery
                .image
                .as_ref()
                .expect("a metadata image was given"),
        ));
    }

    #[rstest_bdd_macros::then("the complete supported feature ranges are:")]
    fn feature_ranges(
        feature_discovery: &FeatureDiscoveryContract,
        #[datatable] rows: Rows<ExpectedFeatureRange>,
    ) {
        let expected: Vec<_> = rows
            .into_iter()
            .map(|row| {
                supported(
                    &row.feature,
                    feature_contract_level(&row.minimum),
                    feature_contract_level(&row.maximum),
                )
            })
            .collect();
        check!(feature_discovery.supported.as_ref() == Some(&expected));
    }

    #[rstest_bdd_macros::then("the complete finalized feature levels are:")]
    fn feature_levels(
        feature_discovery: &FeatureDiscoveryContract,
        #[datatable] rows: Rows<ExpectedFinalizedLevel>,
    ) {
        let expected: Vec<_> = rows
            .into_iter()
            .map(|row| finalized(&row.feature, feature_contract_level(&row.level)))
            .collect();
        check!(feature_discovery.finalized.as_ref() == Some(&expected));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/handlers/api_versions/feature_keys.feature", fixtures = [feature_discovery: FeatureDiscoveryContract]);

    #[test]
    fn finalized_feature_keys_keep_levels_above_zero() {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (name, level) in [
            ("metadata.version", 24),
            ("group.version", 1),
            ("share.version", 0),
        ] {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.into(),
                level,
            }));
        }
        check!(
            finalized_feature_keys(&image)
                == vec![
                    finalized("group.version", 1),
                    finalized("metadata.version", 24)
                ]
        );
        image.apply(&MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
            kraft_version: 1,
        }));
        check!(
            finalized_feature_keys(&image)
                == vec![
                    finalized("group.version", 1),
                    finalized("kraft.version", 1),
                    finalized("metadata.version", 24)
                ]
        );
        // The krabka-owned `krabka.version` is a finalized row like any other.
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "krabka.version".into(),
            level: 1,
        }));
        check!(
            finalized_feature_keys(&image)
                == vec![
                    finalized("group.version", 1),
                    finalized("krabka.version", 1),
                    finalized("kraft.version", 1),
                    finalized("metadata.version", 24)
                ]
        );
    }
}
