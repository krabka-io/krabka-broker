//! The break-glass approval gate shared by destructive partition operations.

use krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord};

use crate::{
    break_glass::gate::{self, BreakGlassDenial},
    config::BreakGlassConfig,
};

pub(super) fn partition_target(topic: &str, partition: i32) -> String {
    format!("{topic}-{partition}")
}

pub(super) fn authorize_partition(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    action: BreakGlassAction,
    topic: &str,
    partition: i32,
) -> Result<Option<MetadataRecord>, BreakGlassDenial> {
    if !gate::is_gated(config) {
        return Ok(None);
    }
    gate::authorize(
        image,
        config,
        action,
        &partition_target(topic, partition),
        crate::time_util::now_ms(),
    )
    .map(Some)
}

/// Name a partition operation while sharing its complete approval gate.
macro_rules! authorizer {
    ($(#[$meta:meta])* $visibility:vis fn $name:ident = $action:ident;) => {
        $(#[$meta])*
        $visibility fn $name(
            image: &krabka_metadata::MetadataImage,
            config: &crate::config::BreakGlassConfig,
            topic: &str,
            partition: i32,
        ) -> Result<Option<krabka_metadata::MetadataRecord>, crate::break_glass::gate::BreakGlassDenial> {
            crate::handlers::partition_transition::authorize_partition(image, config, krabka_metadata::BreakGlassAction::$action, topic, partition)
        }
    };
}
pub(super) use authorizer;

/// Count a refusal before resolving its target and emitting the audit event.
pub(super) fn audit_refusal<T: AsRef<str>>(
    broker: &crate::broker::Broker,
    context: &crate::handlers::RequestContext<'_>,
    action: BreakGlassAction,
    target: impl FnOnce() -> T,
    denial: &BreakGlassDenial,
    reason: &str,
) {
    crate::break_glass::metrics::record_refusal(&broker.metrics, denial.action);
    crate::break_glass::handlers::audit::audit_transition(
        &broker.audit_log,
        &broker.config.break_glass,
        context,
        &crate::break_glass::handlers::audit::GatedTransition {
            action,
            target: target().as_ref(),
            phase: krabka_audit::PrivilegedPhase::Refused,
            proposal_id: denial.proposal_id(),
            reason,
        },
    );
}
