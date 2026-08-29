//! The KIP-966 path that hands an unclean election to the Unclean Recovery
//! Manager instead of electing from the metadata image.
//!
//! A topic with an offset-aware recovery strategy needs the log state of its
//! surviving replicas before a leader can be picked, and the manager owns the
//! append that elects one. This module spends the break-glass approval first,
//! queues the job, and waits out the operator deadline for its outcome.

use krabka_audit::PrivilegedPhase;
use krabka_metadata::{BreakGlassAction, MetadataRecord};
use krabka_protocol::owned::elect_leaders_response::PartitionResult;
use krabka_units::convert::TimeExt as _;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    batch::ElectionBatch,
    env::ElectionEnv,
    unclean_gate::{consumed_proposal_id, unclean_target},
};
use crate::{
    break_glass::handlers::audit::{GatedTransition, audit_transition},
    broker::Broker,
    codes,
    config_keys::RecoveryStrategy,
    unclean_recovery::{RecoveryJob, RecoveryOutcome},
};

/// Hand one partition to the Unclean Recovery Manager and wait for its outcome.
///
/// # KFC-9: the approval is spent before the recovery starts
///
/// The URM owns the `submit_change` that elects the leader, and it runs after
/// this handler answers, so there is no batch to carry the consume in. The
/// broker appends the consume on its own first instead. Consume-then-transition
/// is the safe order of the two: a crash between them loses the approval, where
/// the reverse order would leave an unconsumed proposal that a second unclean
/// election could spend again.
///
/// The job carries the proposal id, which is what takes the recovery out of the
/// background rule in [`crate::unclean_recovery::BackgroundRecovery`]. A person
/// asked for this one.
pub(super) async fn run_offset_aware_recovery(
    env: &ElectionEnv<'_>,
    batch: &mut ElectionBatch,
    topic: &str,
    partition: i32,
    strategy: RecoveryStrategy,
    consumed: Option<MetadataRecord>,
) -> PartitionResult {
    let broker = env.broker;
    let proposal = match spend_before_recovery(broker, batch, consumed).await {
        Ok(proposal) => proposal,
        Err(message) => {
            return PartitionResult {
                partition_id: partition,
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some(message),
                ..Default::default()
            };
        }
    };
    if let Some(proposal_id) = proposal {
        audit_transition(
            &broker.audit_log,
            &broker.config.break_glass,
            env.ctx,
            &GatedTransition {
                action: BreakGlassAction::UncleanElectLeaders,
                target: &unclean_target(topic, partition),
                phase: PrivilegedPhase::Consumed,
                proposal_id: Some(proposal_id),
                reason: "approval spent on an offset-aware unclean recovery",
            },
        );
    }
    let (tx, rx) = oneshot::channel();
    broker
        .unclean_recovery
        .enqueue(RecoveryJob {
            topic: topic.to_string(),
            partition,
            strategy,
            reply: Some(tx),
            proposal,
        })
        .await;
    let (error_code, error_message) =
        match tokio::time::timeout(broker.config.operator_recovery_deadline.to_std(), rx).await {
            Ok(Ok(RecoveryOutcome::Elected(_))) => (codes::NONE, None),
            Ok(Ok(RecoveryOutcome::NoEligibleReplica)) => (
                codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                Some("no eligible replica responded".into()),
            ),
            Ok(Ok(RecoveryOutcome::NotNeeded)) => (
                codes::ELECTION_NOT_NEEDED,
                Some("partition already has a leader".into()),
            ),
            Ok(Ok(RecoveryOutcome::BreakGlassRequired)) => (
                codes::POLICY_VIOLATION,
                Some("break_glass.background_unclean_recovery is require".into()),
            ),
            _ => (
                codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                Some("unclean recovery in progress".into()),
            ),
        };
    PartitionResult {
        partition_id: partition,
        error_code,
        error_message,
        ..Default::default()
    }
}

/// Append the consumed proposal for a partition the URM will elect, and answer
/// the proposal it names.
///
/// # Errors
///
/// Returns the submit failure text when the quorum did not take the consume. No
/// recovery starts in that case, so the approval stays unspent and the operator
/// can retry.
async fn spend_before_recovery(
    broker: &Broker,
    batch: &mut ElectionBatch,
    consumed: Option<MetadataRecord>,
) -> Result<Option<Uuid>, String> {
    let Some(consumed) = consumed else {
        return Ok(None);
    };
    let Some(proposal_id) = consumed_proposal_id(&consumed) else {
        return Ok(None);
    };
    if !batch.spent.insert(proposal_id) {
        return Ok(Some(proposal_id));
    }
    match broker.controller.submit_change(vec![consumed]).await {
        Ok(_) => Ok(Some(proposal_id)),
        Err(error) => {
            tracing::warn!(%error, "elect-leaders could not spend the break-glass approval");
            Err(format!("submit failed: {error}"))
        }
    }
}
