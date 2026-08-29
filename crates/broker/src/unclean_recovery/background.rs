//! KFC-9: the break-glass rule the URM applies to a recovery nobody asked for.
//!
//! The rule reads `[break_glass]` once and then answers two questions per job:
//! whether the recovery may run at all, and whether the election that followed
//! has to be recorded as a bypass. Both answers, and the audit events that
//! carry them, live here so the manager keeps only the control flow.

use std::sync::Arc;

use krabka_audit::{
    AuditEndpoint, AuditEvent, AuditLog, AuditOutcome, AuditPrincipal, PrivilegedPhase,
};
use krabka_metadata::BreakGlassAction;
use krabka_raft::NodeId;

use super::RecoveryJob;
use crate::{
    break_glass::{action_name, metrics as break_glass_metrics},
    config::{BackgroundUncleanRecovery, BreakGlassConfig},
    operator_keys::approver_set_fingerprint,
    time_util::now_ms,
};

/// KFC-9: the break-glass rule for a recovery that nobody asked for.
///
/// # This path has no caller to refuse
///
/// Unclean recovery loses committed data exactly as an operator-typed unclean
/// election does, so the two-person rule looks as if it belongs on both. It
/// cannot be here. Leader election and the broker-heartbeat path start a
/// recovery with no request, no connection, and no principal, so a refusal has
/// no recipient and an approval has nobody to ask. An operator who types an
/// unclean election can be asked for a second signature, and a controller that
/// reacts to a dead broker at 03:00 cannot.
///
/// [`BackgroundUncleanRecovery`] is the three-valued answer to that, and
/// `audit-only` is the default for the reason the split states.
/// [`BackgroundUncleanRecovery::Require`] is the fail-closed option, and it
/// costs every partition whose leader dies at 03:00 its availability, and not
/// only the ones an incident touches.
///
/// A job that carries a proposal took the operator path, where the handler
/// already spent an approved proposal. None of this applies to it.
#[derive(Debug, Clone)]
pub(crate) struct BackgroundRecovery {
    /// Whether this broker runs the two-person rule at all.
    ///
    /// An empty `break_glass.approvers` turns the workflow off, and nobody can
    /// approve anything. Every recovery would then be unapproved, so `require`
    /// would make unclean recovery impossible and `audit-only` would count
    /// every failover as a bypass of a rule that does not exist. A cluster with
    /// no `[break_glass]` section behaves exactly as it does today, which is
    /// the rule the whole feature follows.
    enabled: bool,
    /// The configured `break_glass.background_unclean_recovery`.
    mode: BackgroundUncleanRecovery,
    /// Where the bypass and the refusal events go.
    audit_log: Arc<AuditLog>,
    /// A fingerprint of the sorted approver set, as every other break-glass
    /// event carries. Two brokers that disagree about `break_glass.approvers`
    /// are then visible after the fact.
    approver_set_fingerprint: String,
}

impl BackgroundRecovery {
    /// Read the rule out of `[break_glass]`.
    pub(crate) fn new(config: &BreakGlassConfig, audit_log: Arc<AuditLog>) -> Self {
        Self {
            enabled: crate::break_glass::gate::is_gated(config),
            mode: config.background_unclean_recovery,
            audit_log,
            approver_set_fingerprint: approver_set_fingerprint(&config.approvers),
        }
    }

    /// Whether this job must not run at all.
    ///
    /// Only [`BackgroundUncleanRecovery::Require`] refuses, and only a job that
    /// no operator approved on a broker that runs the two-person rule.
    pub(super) fn refuses(&self, job: &RecoveryJob) -> bool {
        self.enabled && job.proposal.is_none() && self.mode == BackgroundUncleanRecovery::Require
    }

    /// Audit a recovery this rule refused. The partition stays leaderless and
    /// visibly offline, so the audit log is the only place that says why.
    pub(super) fn audit_refusal(&self, job: &RecoveryJob, node_id: NodeId) {
        self.emit(
            PrivilegedPhase::Refused,
            AuditOutcome::Failure,
            job,
            node_id,
            format!(
                "break_glass.background_unclean_recovery is require, and no proposal approved \
                 this recovery; the partition stays offline (strategy {:?})",
                job.strategy
            ),
        );
    }

    /// Account one recovery that elected a leader with no approval behind it.
    ///
    /// `audit-only` is the default, so this is the ordinary path on a cluster
    /// that turned the two-person rule on. The counter is the series to alert
    /// on, and the event is the after-the-fact proof that a data-losing
    /// election happened that no second person agreed to.
    pub(super) fn audit_bypass(
        &self,
        job: &RecoveryJob,
        node_id: NodeId,
        winner: NodeId,
        metrics: &crate::metrics::BrokerMetrics,
    ) {
        if !self.enabled
            || job.proposal.is_some()
            || self.mode != BackgroundUncleanRecovery::AuditOnly
        {
            return;
        }
        break_glass_metrics::record_bypass(metrics, BreakGlassAction::UncleanRecovery);
        self.emit(
            PrivilegedPhase::Bypassed,
            AuditOutcome::Success,
            job,
            node_id,
            format!(
                "unclean recovery elected broker {} with no break-glass approval (strategy {:?})",
                winner.0, job.strategy
            ),
        );
    }

    /// Emit one `PrivilegedAction` event for this path.
    ///
    /// The event names the controller that acted rather than a person, and its
    /// source endpoint is empty, because no connection carried this recovery.
    /// That absence is the whole reason the path cannot have a gate.
    fn emit(
        &self,
        phase: PrivilegedPhase,
        outcome: AuditOutcome,
        job: &RecoveryJob,
        node_id: NodeId,
        reason: String,
    ) {
        self.audit_log.emit(AuditEvent::PrivilegedAction {
            outcome,
            phase,
            action: action_name(BreakGlassAction::UncleanRecovery).to_owned(),
            target: format!("{}-{}", job.topic, job.partition),
            proposal_id: String::new(),
            principal: AuditPrincipal {
                name: format!("Controller:{}", node_id.0),
                auth_method: "Internal".to_owned(),
            },
            counterparties: Vec::new(),
            approver_set_fingerprint: self.approver_set_fingerprint.clone(),
            key_id: String::new(),
            signature: Vec::new(),
            signature_verified: false,
            // The controller runs this with no caller and no signature.
            signed_at_ms: 0,
            source: AuditEndpoint {
                ip: String::new(),
                port: 0,
            },
            reason,
            time_ms: now_ms(),
        });
    }
}
