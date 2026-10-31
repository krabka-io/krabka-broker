//! Tests for the request planner, kept in their own file because they build
//! whole metadata images and outweigh the module they cover.

use {
    super::*,
    crate::{
        features::ELR_VERSION,
        handlers::update_features::test_support::{
            elr_update, metadata_update, min_isr_config_record, named_update, validate_only,
        },
        test_support::broker_registration,
    },
    assert2::assert,
    krabka_metadata::{NodeId, krabka_version},
    krabka_raft::UnstableFeatureVersions,
    std::collections::BTreeMap,
};

/// The local controller at the defaults: `unstable.feature.versions.enable`
/// off, as a stock Kafka 4.3.1 controller runs.
const LOCAL: LocalController = LocalController {
    node_id: NodeId(1),
    unstable_features: UnstableFeatureVersions::Disabled,
};
const UPGRADE: i8 = 1;
const SAFE: i8 = 2;
const UNSAFE: i8 = 3;

#[derive(derive_more::From, derive_more::Into)]
/// The advertised feature ranges are wire data, including unsupported levels.
struct RegisteredFeatureRanges(BTreeMap<String, (i16, i16)>);

impl Default for RegisteredFeatureRanges {
    fn default() -> Self { Self(krabka_metadata::supported_feature_ranges()) }
}

#[derive(krabka_macros::FieldDefaults)]
struct RemoteFeatureSetup {
    #[default(NodeId(2))]
    node_id: NodeId,
    ranges: RegisteredFeatureRanges,
}

#[bon::builder]
fn remote_feature_image(
    mut image: krabka_metadata::MetadataImage,
    #[builder(into, default = NodeId (2))] node_id: NodeId,
    #[builder(into, default)] ranges: RegisteredFeatureRanges,
) -> krabka_metadata::MetadataImage {
    let setup = RemoteFeatureSetup { node_id, ranges };
    image.apply(&MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            host: String::new(),
            port: 0,
            features: setup.ranges.0,
            ..broker_registration(setup.node_id)
        },
    ));
    image
}

fn feature_record(name: &str, level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: name.into(),
        level,
    })
}

/// A feature and the level it is finalized at.
#[derive(Clone, Copy)]
struct FeatureLevel<'a> {
    name: &'a str,
    level: i16,
}

/// An image finalized at `levels`.
fn image(levels: &[FeatureLevel<'_>]) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for &FeatureLevel { name, level } in levels {
        image.apply(&feature_record(name, level));
    }
    image
}

fn invalid(feature: &str, level: i16, reason: &str) -> UpdateError {
    UpdateError {
        code: codes::INVALID_UPDATE_VERSION,
        message: format!("Invalid update version {level} for feature {feature}. {reason}"),
    }
}

fn plan(records: Vec<MetadataRecord>, features: &[&str]) -> UpdatePlan {
    UpdatePlan {
        records,
        features: features.iter().map(ToString::to_string).collect(),
        ..UpdatePlan::default()
    }
}

#[derive(Default)]
struct FeaturePlanningScenario {
    image: Option<krabka_metadata::MetadataImage>,
    updates: Vec<FeatureUpdateKey>,
    result: Option<Result<UpdatePlan, UpdateError>>,
    static_min_isr: i32,
    safety: Option<Vec<MetadataRecord>>,
}
#[rstest::fixture]
fn feature_planning() -> FeaturePlanningScenario {
    let image = None;
    FeaturePlanningScenario {
        image,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("the finalized feature levels are {levels}")]
fn finalized_planning_levels(feature_planning: &mut FeaturePlanningScenario, levels: String) {
    let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for spec in levels.split(',') {
        let (name, level) = spec.split_once('=').unwrap();
        img.apply(&feature_record(name, level.parse().unwrap()));
    }
    feature_planning.image = Some(img);
}
#[rstest_bdd_macros::given("the requested feature rows are {updates}")]
fn planning_update_rows(feature_planning: &mut FeaturePlanningScenario, updates: String) {
    feature_planning.updates = if updates == "none" {
        Vec::new()
    } else {
        updates
            .split(';')
            .map(|spec| {
                let columns: Vec<_> = spec.split(':').collect();
                let [name, level, kind] = columns.as_slice() else {
                    panic!("feature level and upgrade type required")
                };
                named_update(name, level.parse().unwrap(), kind.parse().unwrap())
            })
            .collect()
    };
}
#[rstest_bdd_macros::given("registered broker 2 advertises krabka.version range {range}")]
fn planning_remote_broker(feature_planning: &mut FeaturePlanningScenario, range: String) {
    let mut ranges = krabka_metadata::supported_feature_ranges();
    if range == "missing" {
        ranges.remove(krabka_version::KRABKA_VERSION_FEATURE);
    } else {
        let (min, max) = range.split_once('-').unwrap();
        ranges.insert(
            krabka_version::KRABKA_VERSION_FEATURE.into(),
            (min.parse().unwrap(), max.parse().unwrap()),
        );
    }
    feature_planning.image = Some(
        remote_feature_image()
            .image(feature_planning.image.take().unwrap())
            .ranges(ranges)
            .call(),
    );
}
#[rstest_bdd_macros::when("the local controller plans the validate-only feature request")]
fn plan_requested_features(feature_planning: &mut FeaturePlanningScenario) {
    feature_planning.result = Some(plan_updates(
        &validate_only(feature_planning.updates.clone()),
        feature_planning.image.as_ref().unwrap(),
        LOCAL,
    ));
}
#[rstest_bdd_macros::then(
    "the complete feature plan writes {records} reports features {features} upgrades kraft to {kraft} and enables ELR {elr}"
)]
fn check_complete_feature_plan(
    feature_planning: &FeaturePlanningScenario,
    records: String,
    features: String,
    kraft: String,
    elr: bool,
) {
    let records = if records == "none" {
        Vec::new()
    } else {
        records
            .split(',')
            .map(|spec| {
                let (name, level) = spec.split_once('=').unwrap();
                feature_record(name, level.parse().unwrap())
            })
            .collect()
    };
    let features: Vec<_> = if features == "none" {
        Vec::new()
    } else {
        features.split(',').collect()
    };
    let kraft_upgrade = if kraft == "none" {
        None
    } else {
        Some(kraft.parse::<u16>().unwrap())
    };
    let expected = UpdatePlan {
        kraft_upgrade,
        enables_elr: elr,
        ..plan(records, &features)
    };
    assert!(feature_planning.result.as_ref().unwrap() == &Ok(expected));
}
#[rstest_bdd_macros::then("feature {feature} level {level} is refused because {reason}")]
fn check_feature_plan_refusal(
    feature_planning: &FeaturePlanningScenario,
    feature: String,
    level: i16,
    reason: String,
) {
    assert!(feature_planning.result.as_ref().unwrap() == &Err(invalid(&feature, level, &reason)));
}
#[rstest_bdd_macros::given(
    "ELR is disabled with cluster minimum ISR {cluster} broker {broker} minimum ISR {value} and static minimum ISR {static_value}"
)]
fn planning_elr_safety_inputs(
    feature_planning: &mut FeaturePlanningScenario,
    cluster: String,
    broker: u64,
    value: String,
    static_value: i32,
) {
    let mut image = elr_image(None, false);
    if cluster != "unset" {
        image.apply(&min_isr_config_record(
            krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            Some(&cluster),
        ));
    }
    if value != "unset" {
        image.apply(&min_isr_config_record(NodeId(broker), Some(&value)));
    }
    feature_planning.image = Some(image);
    feature_planning.static_min_isr = static_value;
}
#[rstest_bdd_macros::when("enabling ELR generates its minimum ISR safety records")]
fn generate_elr_safety_records(feature_planning: &mut FeaturePlanningScenario) {
    feature_planning.safety = Some(elr_safety_records(
        feature_planning.image.as_ref().unwrap(),
        feature_planning.static_min_isr,
    ));
}
#[rstest_bdd_macros::then("the ordered minimum ISR safety records are {records}")]
fn check_elr_safety_records(feature_planning: &FeaturePlanningScenario, records: String) {
    let expected: Vec<_> = if records == "none" {
        Vec::new()
    } else {
        records
            .split(',')
            .map(|spec| {
                let (node, value) = spec.split_once('=').unwrap();
                let node = if node == "cluster" {
                    krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                } else {
                    NodeId(node.parse().unwrap())
                };
                min_isr_config_record(node, if value == "null" { None } else { Some(value) })
            })
            .collect()
    };
    assert!(feature_planning.safety.as_ref().unwrap() == &expected);
}
rstest_bdd_macros::scenarios!("tests/features/handler_feature_planning",fixtures=[feature_planning:FeaturePlanningScenario]);

/// A version 0 row carries `AllowDowngrade` rather than `UpgradeType`, and
/// Kafka reads the `UpgradeType` default, UPGRADE, so the flag never
/// authorizes a downgrade.
/// #784: a Kafka trunk `metadata.version` (31-33) is past what the local
/// controller supports unless `unstable.feature.versions.enable` is set, and
/// is refused with `QuorumFeatures.reasonNotSupported`'s text, as a 4.3.1
/// controller refuses it. With the flag set, trunk's levels are accepted.
#[yare::parameterized(
    latest_stable_level = {
        LOCAL,
        30,
        Ok(plan(vec![feature_record("metadata.version", 30)], &["metadata.version"])),
    },
    first_unstable_level_is_refused = {
        LOCAL,
        31,
        Err(invalid("metadata.version", 31, "Local controller 1 only supports versions 7-30")),
    },
    higher_unstable_level_is_refused = {
        LOCAL,
        33,
        Err(invalid("metadata.version", 33, "Local controller 1 only supports versions 7-30")),
    },
    enabled_unstable_level = {
        LocalController { unstable_features: UnstableFeatureVersions::Enabled, ..LOCAL },
        31,
        Ok(plan(vec![feature_record("metadata.version", 31)], &["metadata.version"])),
    },
    enabled_higher_unstable_level = {
        LocalController { unstable_features: UnstableFeatureVersions::Enabled, ..LOCAL },
        33,
        Ok(plan(vec![feature_record("metadata.version", 33)], &["metadata.version"])),
    },
)]
fn an_unstable_metadata_version_needs_unstable_feature_versions(
    local: LocalController,
    level: i16,
    expected: Result<UpdatePlan, UpdateError>,
) {
    let image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 30,
    }]);
    let request = validate_only(vec![metadata_update(level, UPGRADE)]);
    assert2::assert!(
        plan_updates(&request, &image, local) == expected,
        "{:?} level {level}",
        local.unstable_features
    );
}

#[test]
fn version_zero_allow_downgrade_is_read_as_upgrade() {
    let request = UpdateFeaturesRequest {
        feature_updates: vec![FeatureUpdateKey {
            feature: "group.version".into(),
            max_version_level: 0,
            allow_downgrade: true,
            ..Default::default()
        }],
        ..Default::default()
    };
    let image = image(&[
        FeatureLevel {
            name: "metadata.version",
            level: 25,
        },
        FeatureLevel {
            name: "group.version",
            level: 1,
        },
    ]);
    assert!(
        plan_updates(&request, &image, LOCAL)
            == Err(invalid(
                "group.version",
                0,
                "Can't downgrade the version of this feature without setting the upgrade type \
                 to either safe or unsafe downgrade.",
            ))
    );
}

/// The first failing entry in Kafka's `HashMap` order names the error, and
/// the valid rows ahead of it write nothing.
#[test]
fn the_first_failure_in_map_order_fails_the_whole_request() {
    let image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 25,
    }]);
    let request = validate_only(vec![
        named_update("share.version", 9, UPGRADE),
        named_update("group.version", 9, UPGRADE),
    ]);
    assert!(
        plan_updates(&request, &image, LOCAL)
            == Err(invalid(
                "group.version",
                9,
                "Local controller 1 only supports versions 0-1",
            ))
    );
}

/// krabka-io/krabka-broker#781: Kafka's `didMetadataChange` walk decides a
/// `metadata.version` downgrade, whatever the downgrade type, and an accepted
/// downgrade writes only its feature record.
#[test]
fn metadata_version_downgrades_follow_did_metadata_change() {
    let refused = |current: i16, target: i16, reason: &str| {
        Err(UpdateError {
            code: codes::INVALID_UPDATE_VERSION,
            message: format!(
                "Unsupported metadata.version downgrade from {current} to {target}. {reason}"
            ),
        })
    };
    let lossy = "Refusing to perform the requested downgrade because it might delete metadata \
                 information.";
    let unsafe_reason = "Unsafe metadata downgrade is not supported in this version.";
    let accepted = |target: i16| {
        Ok(plan(vec![feature_record("metadata.version", target)], &[
            "metadata.version",
        ]))
    };
    let cases = [
        (25, 24, SAFE, accepted(24)),
        (25, 24, UNSAFE, accepted(24)),
        (25, 22, SAFE, refused(25, 22, lossy)),
        (25, 17, UNSAFE, refused(25, 17, unsafe_reason)),
        (17, 15, UNSAFE, refused(17, 15, unsafe_reason)),
        // 3.6-IV1 (13) changed metadata, so leaving it is lossy; 3.6-IV0
        // (12) did not.
        (13, 12, SAFE, refused(13, 12, lossy)),
        (12, 11, SAFE, accepted(11)),
        (29, 24, SAFE, accepted(24)),
        (30, 29, SAFE, refused(30, 29, lossy)),
        (
            25,
            24,
            UPGRADE,
            Err(invalid(
                "metadata.version",
                24,
                "Can't downgrade the version of this feature without setting the upgrade type \
                 to either safe or unsafe downgrade.",
            )),
        ),
        (
            25,
            6,
            SAFE,
            Err(invalid(
                "metadata.version",
                6,
                "Local controller 1 only supports versions 7-30",
            )),
        ),
    ];
    for (current, target, upgrade_type, expected) in cases {
        let image = image(&[FeatureLevel {
            name: "metadata.version",
            level: current,
        }]);
        let request = validate_only(vec![metadata_update(target, upgrade_type)]);
        assert!(
            plan_updates(&request, &image, LOCAL) == expected,
            "{current} -> {target} type {upgrade_type}"
        );
    }
}

/// An image at 4.3-IV0, with `eligible.leader.replicas.version` finalized at
/// `elr_level` when one is given and a published ELR on topic `orders` when
/// `published` is.
fn elr_image(elr_level: Option<i16>, published: bool) -> krabka_metadata::MetadataImage {
    let mut image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 30,
    }]);
    if let Some(level) = elr_level {
        image.apply(&feature_record(ELR_VERSION, level));
    }
    if published {
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: "orders".into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 3,
        }));
        image.apply(&MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "orders".into(),
                partition: 0,
                ..Default::default()
            },
        ));
        image.apply(&MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "orders".into(),
                partition: 0,
                eligible_leader_replicas: vec![NodeId(2), NodeId(3)],
                last_known_elr: vec![],
            },
        ));
    }
    image
}

/// KIP-966: finalizing the feature back to 0 clears the memberships it
/// published, ahead of the feature record; a cluster that never published
/// any writes the feature record alone.
#[yare::parameterized(published = { true }, never_published = { false })]
fn an_elr_downgrade_clears_the_published_state_before_the_feature_record(published: bool) {
    let elr = ELR_VERSION;
    let mut records = Vec::new();
    if published {
        records.push(MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "orders".into(),
                partition: 0,
                eligible_leader_replicas: vec![],
                last_known_elr: vec![],
            },
        ));
    }
    records.push(feature_record(elr, 0));
    let image = elr_image(Some(1), published);
    let request = validate_only(vec![elr_update(0, SAFE)]);
    assert2::assert!(
        plan_updates(&request, &image, LOCAL) == Ok(plan(records, &[elr])),
        "{published}"
    );
}

/// A broker that did not register a feature supports only level 0, so it
/// blocks turning the feature on but not off.
#[test]
fn a_feature_unaware_broker_blocks_only_enabling() {
    let elr = ELR_VERSION;
    let unaware = |elr_level| {
        let image = elr_image(elr_level, false);
        let mut features = krabka_metadata::supported_feature_ranges();
        features.remove(elr);
        remote_feature_image().image(image).ranges(features).call()
    };
    let cases = [
        (
            "enabling",
            unaware(None),
            elr_update(1, UPGRADE),
            Err(invalid(elr, 1, "Broker 2 does not support this feature.")),
        ),
        (
            "disabling",
            unaware(Some(1)),
            elr_update(0, SAFE),
            Ok(plan(vec![feature_record(elr, 0)], &[elr])),
        ),
    ];
    for (case, image, update, expected) in cases {
        assert!(
            plan_updates(&validate_only(vec![update]), &image, LOCAL) == expected,
            "{case}"
        );
    }
}

/// One `UpdateFeatures` case: its name, the image it plans against, the one
/// update it carries, and the plan or error it must produce.
type PlanCase<'a> = (
    &'a str,
    krabka_metadata::MetadataImage,
    FeatureUpdateKey,
    Result<UpdatePlan, UpdateError>,
);

/// Plan every case's update on its own, as a validate-only request, against
/// the local controller, and compare all of them at once.
fn assert_planned<const N: usize>(cases: [PlanCase<'_>; N]) {
    let actual: Vec<_> = cases
        .iter()
        .map(|(case, image, update, _)| {
            (
                *case,
                plan_updates(&validate_only(vec![update.clone()]), image, LOCAL),
            )
        })
        .collect();
    let expected: Vec<_> = cases
        .into_iter()
        .map(|(case, _, _, want)| (case, want))
        .collect();
    assert!(actual == expected);
}

/// krabka-io/krabka-broker#1763: `krabka.version` level 2 turns on diskless
/// WAL object reservations. The controller finalizes it only once every
/// registered broker advertises it, so no 1.0.0 broker, which writes no
/// reservations, runs beside it. Lowering it again is an ordinary downgrade:
/// every broker that supports level 2 reads reservations at every level.
#[test]
fn krabka_version_reservation_level_finalizes_only_when_every_broker_supports_it() {
    let kv = krabka_version::KRABKA_VERSION_FEATURE;
    let level = krabka_version::DISKLESS_OBJECT_RESERVATION_LEVEL;
    let mv = FeatureLevel {
        name: "metadata.version",
        level: 30,
    };
    let with_broker = |levels: &[FeatureLevel<'_>], range| {
        let mut features = krabka_metadata::supported_feature_ranges();
        features.insert(kv.to_owned(), range);
        remote_feature_image()
            .image(image(levels))
            .ranges(features)
            .call()
    };
    let at_level = [mv, FeatureLevel { name: kv, level }];
    let cases = [
        (
            "every broker advertises the reservation level",
            with_broker(&[mv], (0, level)),
            named_update(kv, level, UPGRADE),
            Ok(plan(vec![feature_record(kv, level)], &[kv])),
        ),
        (
            "a 1.0.0 broker advertises only level 1",
            with_broker(&[mv], (0, 1)),
            named_update(kv, level, UPGRADE),
            Err(invalid(kv, level, "Broker 2 only supports versions 0-1")),
        ),
        (
            "a safe downgrade below the reservation level",
            with_broker(&at_level, (0, level)),
            named_update(kv, 1, SAFE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
    ];
    assert_planned(cases);
}
