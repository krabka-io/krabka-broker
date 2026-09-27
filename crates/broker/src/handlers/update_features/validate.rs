//! Per-feature validation of an `UpdateFeatures` request.
//!
//! This module holds the loop that turns each requested feature update into a
//! result row and, where the update is accepted, into the metadata records
//! that persist it. It is the bulk of the handler's logic and the only part
//! that decides whether an update is legal, so it sits apart from the request
//! plumbing in the module root.

use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::owned::{
    update_features_request::UpdateFeaturesRequest,
    update_features_response::UpdatableFeatureResult,
};
use krabka_verified::features::{
    FeatureClusterFacts, FeatureLevels, FeatureUpdateDecision, FeatureUpdateFacts,
    FeatureUpdateType, MetadataVersionFacts, feature_update_decision,
};

use super::{
    preconditions::{
        dependencies_met, registered_node_without_metadata_downgrade_capability,
        unregistered_controller, unsupported_registered_node,
    },
    response::row,
    upgrade_type::update_type,
};
use crate::codes;

/// KIP-584: a requested `max_version_level` of `0` asks to *delete* the
/// finalized feature rather than move it to another level.
const DELETE_FINALIZED_LEVEL: i16 = 0;

pub(super) fn validate_updates(
    request: &UpdateFeaturesRequest,
    image: &krabka_metadata::MetadataImage,
    version: i16,
) -> (Vec<UpdatableFeatureResult>, Vec<MetadataRecord>) {
    let mut seen = std::collections::HashSet::new();
    let mut results = Vec::new();
    let mut records = Vec::new();
    let mut metadata_version_records = None;
    for upd in &request.feature_updates {
        let name = upd.feature.clone();
        if !seen.insert(name.clone()) {
            results.push(row(
                name,
                codes::INVALID_REQUEST,
                "Provided feature can not be updated more than once in the request.",
            ));
            continue;
        }
        let Some(feat) = krabka_metadata::feature(&name) else {
            results.push(row(
                name,
                codes::INVALID_REQUEST,
                "Could not apply finalized feature update because the provided feature is not supported.",
            ));
            continue;
        };

        let level = upd.max_version_level;
        if name == krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE {
            let current = i16::try_from(image.kraft_version()).unwrap_or(i16::MAX);
            if level != 1 || current > level {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "kraft.version can only be upgraded from 0 to 1.",
                ));
            } else {
                results.push(row(name, codes::NONE, ""));
            }
            continue;
        }
        let current = image.finalized_features().get(&name).copied();
        let update_type = update_type(version, upd.allow_downgrade, upd.upgrade_type);
        let planned_cleanup =
            match plan_feature_update(image, feat, &name, level, current, update_type) {
                Ok(planned_cleanup) => planned_cleanup,
                Err(message) => {
                    results.push(row(name, codes::INVALID_UPDATE_VERSION, &message));
                    continue;
                }
            };

        // Accepted. The verified cleanup result is kept with the deferred
        // metadata.version record so no intervening append can reverse them.
        let feature_record = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: name.clone(),
            level,
        });
        if name == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE {
            metadata_version_records = Some((planned_cleanup, feature_record));
        } else {
            // KIP-966: turning the feature off clears the memberships it
            // published, the way Kafka's controller emits its own cleaning
            // records. The clearing records go in first, so a replay that
            // stops between them and the feature record has already forgotten
            // the memberships rather than kept them under a feature that is
            // still on.
            if name == crate::features::ELR_VERSION
                && level == DELETE_FINALIZED_LEVEL
                && current.is_some_and(|cur| cur >= 1)
            {
                records.extend(crate::elr::clear_published_elr(image));
            }
            records.push(feature_record);
        }
        results.push(row(name, codes::NONE, ""));
    }
    // KIP-1155: the metadata.version record is always emitted last, after any
    // records that remove fields unavailable at the target version.
    if let Some((cleanup_records, feature_record)) = metadata_version_records {
        records.extend(cleanup_records);
        records.push(feature_record);
    }
    (results, records)
}

/// Why a registered node blocks a feature row, as the rejection names it.
struct NodeBlockers {
    unsupported_level: Option<String>,
    unregistered_controller: Option<krabka_metadata::NodeId>,
    no_downgrade_capability: Option<String>,
}

/// Establish every fact one feature row is decided on, let the verified
/// kernel decide it, and return the cleanup records an admitted row emits
/// before its feature-level record, or the rejection message.
///
/// A `metadata.version` downgrade's floor and dependencies are read from the
/// image its cleanup records would leave, so an unsafe downgrade is judged
/// on the state the caller authorized.
fn plan_feature_update(
    image: &krabka_metadata::MetadataImage,
    feat: &dyn krabka_metadata::Feature,
    name: &str,
    level: i16,
    current: Option<i16>,
    update_type: Option<FeatureUpdateType>,
) -> Result<Vec<MetadataRecord>, String> {
    let is_metadata_version = name == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE;
    let metadata_downgrade = is_metadata_version && current.is_some_and(|cur| level < cur);
    let cleanup_records = if metadata_downgrade {
        image.metadata_version_downgrade_records(level)
    } else {
        Vec::new()
    };
    let projected_image = (!cleanup_records.is_empty()).then(|| {
        let mut projected = image.clone();
        for record in &cleanup_records {
            projected.apply(record);
        }
        projected
    });
    let target_image = projected_image.as_ref().unwrap_or(image);
    let blockers = NodeBlockers {
        unsupported_level: unsupported_registered_node(image, name, level),
        unregistered_controller: is_metadata_version
            .then(|| unregistered_controller(image))
            .flatten(),
        no_downgrade_capability: is_metadata_version
            .then(|| registered_node_without_metadata_downgrade_capability(image))
            .flatten(),
    };
    let facts = FeatureUpdateFacts {
        update_type,
        levels: FeatureLevels {
            requested: level,
            finalized: current,
            max_supported: feat.supported_range().1,
            floor: feat.min_required_floor(target_image),
        },
        cluster: FeatureClusterFacts {
            all_nodes_support: blockers.unsupported_level.is_none(),
            dependencies_met: dependencies_met(target_image, feat.dependencies(level)),
        },
        metadata_version: is_metadata_version.then_some(MetadataVersionFacts {
            online_downgrade_min_level:
                krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL,
            all_controllers_registered: blockers.unregistered_controller.is_none(),
            all_nodes_downgrade_capable: blockers.no_downgrade_capability.is_none(),
            cleanup_required: !cleanup_records.is_empty(),
        }),
    };
    row_outcome(feature_update_decision(facts), cleanup_records, blockers)
}

/// The records an admitted feature row emits before its feature-level
/// record, or the `INVALID_UPDATE_VERSION` message of a rejected one.
fn row_outcome(
    decision: FeatureUpdateDecision,
    cleanup_records: Vec<MetadataRecord>,
    blockers: NodeBlockers,
) -> Result<Vec<MetadataRecord>, String> {
    Err(match decision {
        FeatureUpdateDecision::EmitFeature => return Ok(Vec::new()),
        FeatureUpdateDecision::EmitCleanupThenFeature => return Ok(cleanup_records),
        FeatureUpdateDecision::UnknownUpdateType => {
            "The controller does not support the given upgrade type.".into()
        }
        FeatureUpdateDecision::OutOfSupportedRange => {
            "Provided version level is not in the supported range.".into()
        }
        FeatureUpdateDecision::UnsupportedByNode => blockers
            .unsupported_level
            .unwrap_or_else(|| "A registered node does not support the provided level.".into()),
        FeatureUpdateDecision::DowngradeWithoutFlag => {
            "Can not downgrade a finalized feature without setting the downgrade flag.".into()
        }
        FeatureUpdateDecision::DowngradeToNewerLevel => {
            "Can not downgrade to a newer feature version.".into()
        }
        FeatureUpdateDecision::OnlineDowngradeUnsupported => {
            "Online metadata.version downgrade requires 3.7-IV0 or newer.".into()
        }
        FeatureUpdateDecision::UnregisteredController => blockers.unregistered_controller.map_or_else(
            || "A controller has not registered.".into(),
            |controller| {
                format!(
                    "Controller {controller} has not registered, so its metadata.version support cannot be verified."
                )
            },
        ),
        FeatureUpdateDecision::NodeCannotDowngrade => blockers
            .no_downgrade_capability
            .unwrap_or_else(|| "A registered node does not support online metadata.version downgrade.".into()),
        FeatureUpdateDecision::BelowFloor => {
            "Can not downgrade the feature below the level required by existing cluster state."
                .into()
        }
        FeatureUpdateDecision::DependencyUnmet => {
            "Can not finalize feature: a required dependency feature is not finalized at a high enough level."
                .into()
        }
        FeatureUpdateDecision::DeleteMissingFeature => {
            "Can not delete a finalized feature that does not exist.".into()
        }
        FeatureUpdateDecision::DeleteWithoutFlag => {
            "Can not delete a finalized feature without setting the downgrade flag.".into()
        }
        FeatureUpdateDecision::LossyDowngradeNotUnsafe => {
            "Refusing a lossy metadata.version downgrade; retry with UNSAFE_DOWNGRADE to discard incompatible metadata."
                .into()
        }
    })
}

/// `true` when the request finalizes `eligible.leader.replicas.version` above
/// 0 and that row was accepted.
pub(super) fn enables_elr(
    request: &UpdateFeaturesRequest,
    results: &[UpdatableFeatureResult],
) -> bool {
    request
        .feature_updates
        .iter()
        .zip(results)
        .any(|(update, result)| {
            update.feature == crate::features::ELR_VERSION
                && update.max_version_level > 0
                && result.error_code == codes::NONE
        })
}

/// Kafka's `ConfigurationControlManager.maybeGenerateElrSafetyRecords`: the
/// config records that make it safe to turn ELR on, written in the same batch
/// as, and ahead of, the feature record. The cluster-level
/// `min.insync.replicas` is set to the broker's static value when it has
/// none, and every broker-level `min.insync.replicas` is removed, including
/// one left behind by a broker that has since unregistered.
pub(super) fn elr_safety_records(
    image: &krabka_metadata::MetadataImage,
    static_min_insync_replicas: i32,
) -> Vec<MetadataRecord> {
    let key = crate::config_keys::MIN_INSYNC_REPLICAS;
    let mut records = Vec::new();
    if !image
        .default_broker_config()
        .is_some_and(|configs| configs.contains_key(key))
    {
        records.push(MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: key.into(),
                config_value: Some(static_min_insync_replicas.to_string()),
            },
        ));
    }
    // Every broker config resource, registered broker or not, as Kafka walks
    // `brokersWithConfigs`: unregistering a broker leaves its configs behind.
    // The image names those resources only through its record form.
    let mut nodes: Vec<krabka_metadata::NodeId> = image
        .to_records()
        .into_iter()
        .filter_map(|record| match record {
            MetadataRecord::V1BrokerConfig(config)
                if config.node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                    && config.config_name == key =>
            {
                Some(config.node_id)
            }
            _ => None,
        })
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    records.extend(nodes.into_iter().map(|node_id| {
        MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
            node_id,
            config_name: key.into(),
            config_value: None,
        })
    }));
    records
}

#[cfg(test)]
mod tests;
