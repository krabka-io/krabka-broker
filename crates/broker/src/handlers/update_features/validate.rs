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

use super::{
    preconditions::{
        dependencies_met, registered_node_without_metadata_downgrade_capability,
        unregistered_controller, unsupported_registered_node,
    },
    response::row,
    upgrade_type::{UpdateType, update_type},
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
        let Some(update_type) = update_type(version, upd.allow_downgrade, upd.upgrade_type) else {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "The controller does not support the given upgrade type.",
            ));
            continue;
        };
        let allow_dg = update_type != UpdateType::Upgrade;

        let (_min, max) = feat.supported_range();
        if level < 0 || level > max {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Provided version level is not in the supported range.",
            ));
            continue;
        }
        if let Some(cur) = current {
            if level < cur && !allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not downgrade a finalized feature without setting the downgrade flag.",
                ));
                continue;
            }
            if level > cur && allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not downgrade to a newer feature version.",
                ));
                continue;
            }
        }
        let mut downgrade_records = Vec::new();
        let mut projected_image = None;
        if name == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
            && current.is_some_and(|cur| level < cur)
        {
            if level < krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Online metadata.version downgrade requires 3.7-IV0 or newer.",
                ));
                continue;
            }
            if let Some(controller) = unregistered_controller(image) {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    &format!(
                        "Controller {controller} has not registered, so its metadata.version support cannot be verified."
                    ),
                ));
                continue;
            }
            if let Some(message) = registered_node_without_metadata_downgrade_capability(image) {
                results.push(row(name, codes::INVALID_UPDATE_VERSION, &message));
                continue;
            }
            downgrade_records = image.metadata_version_downgrade_records(level);
            if !downgrade_records.is_empty() {
                let mut projected = image.clone();
                for record in &downgrade_records {
                    projected.apply(record);
                }
                projected_image = Some(projected);
            }
        }
        if let Some(message) = unsupported_registered_node(image, &name, level) {
            results.push(row(name, codes::INVALID_UPDATE_VERSION, &message));
            continue;
        }
        // Per-feature downgrade-safety floor (KIP-584 unsafe downgrade): a
        // finalize below the level the live image requires is rejected even
        // with the downgrade flag set. `level == 0` (delete) is handled by the
        // tombstone path below, not the floor.
        // Unsafe metadata.version downgrades validate against the image that
        // will exist after their explicit cleanup records apply. Computing the
        // floor from the pre-cleanup image would reject the very state removal
        // the caller authorized.
        let target_image = projected_image.as_ref().unwrap_or(image);
        let floor = feat.min_required_floor(target_image);
        if level > 0 && level < floor {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not downgrade the feature below the level required by existing cluster state.",
            ));
            continue;
        }
        // KIP-1022 dependencies: every dependency must already be finalized
        // at >= its required level in the target image.
        if !dependencies_met(target_image, feat.dependencies(level)) {
            results.push(row(
                name,
                codes::INVALID_UPDATE_VERSION,
                "Can not finalize feature: a required dependency feature is not finalized at a high enough level.",
            ));
            continue;
        }
        if level == DELETE_FINALIZED_LEVEL {
            // Delete the finalized feature; only valid if it exists and a
            // downgrade is permitted.
            if current.is_none() {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not delete a finalized feature that does not exist.",
                ));
                continue;
            }
            if !allow_dg {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not delete a finalized feature without setting the downgrade flag.",
                ));
                continue;
            }
        }

        let cleanup_required = !downgrade_records.is_empty();
        let decision = krabka_verified::feature_update_decision(
            (true, true, true),
            (true, true, true),
            (
                true,
                cleanup_required,
                update_type == UpdateType::UnsafeDowngrade,
            ),
        );
        let planned_cleanup = match decision {
            krabka_verified::FeatureUpdateDecision::Reject => {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Refusing a lossy metadata.version downgrade; retry with UNSAFE_DOWNGRADE to discard incompatible metadata.",
                ));
                continue;
            }
            krabka_verified::FeatureUpdateDecision::EmitFeature => Vec::new(),
            krabka_verified::FeatureUpdateDecision::EmitCleanupThenFeature => downgrade_records,
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

#[cfg(test)]
mod tests;
