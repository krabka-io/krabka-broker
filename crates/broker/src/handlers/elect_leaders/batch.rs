//! The records one `ElectLeaders` request accumulates before it appends them,
//! and the audit trail that append owes once its outcome is known.
//!
//! A request elects many partitions and spends at most one break-glass approval
//! per proposal, so the consumed proposals and the leader changes gather here
//! and reach the metadata log as a single raft append.

use std::collections::HashSet;

use krabka_audit::PrivilegedPhase;
use krabka_metadata::{BreakGlassAction, MetadataRecord};
use uuid::Uuid;

use super::unclean_gate::consumed_proposal_id;
use crate::{
    break_glass::handlers::audit::{GatedTransition, audit_transition},
    broker::Broker,
    handlers::RequestContext,
};

/// What one `ElectLeaders` request accumulates across its partitions.
///
/// `records` is the single raft append that carries every consumed proposal
/// beside every leader change the request makes. That one append is why a
/// proposal lives in the metadata log at all: the approval and the transition
/// it authorizes commit together, so a crash between them cannot spend one
/// approval twice.
#[derive(Default)]
pub(super) struct ElectionBatch {
    /// The consumed proposals first, then the partition records.
    pub(super) records: Vec<MetadataRecord>,
    /// The proposals this request already spent. One approved proposal on a
    /// bare topic name covers every partition of that topic, so a request that
    /// elects ten of them reads one proposal ten times and spends it once.
    pub(super) spent: HashSet<Uuid>,
    /// The transitions waiting on the append, to audit once it commits.
    pub(super) applied: Vec<(String, Option<Uuid>)>,
}

impl ElectionBatch {
    /// Take a consumed proposal into the append, and answer the proposal it
    /// names.
    ///
    /// The record goes in ahead of every partition record, and only the first
    /// time this request sees the proposal.
    pub(super) fn spend(&mut self, consumed: Option<MetadataRecord>) -> Option<Uuid> {
        let consumed = consumed?;
        let proposal_id = consumed_proposal_id(&consumed)?;
        if self.spent.insert(proposal_id) {
            self.records.insert(0, consumed);
        }
        Some(proposal_id)
    }

    /// Audit every transition this append carried.
    ///
    /// `failure` is the submit error when the append did not commit, and the
    /// event then records a refusal with that text rather than an application
    /// that never happened.
    pub(super) fn audit_applied(
        &self,
        broker: &Broker,
        ctx: &RequestContext<'_>,
        failure: Option<&str>,
    ) {
        for (target, proposal_id) in &self.applied {
            let (phase, reason) = match failure {
                None => (
                    PrivilegedPhase::Applied,
                    "unclean leader election committed",
                ),
                Some(error) => (PrivilegedPhase::Refused, error),
            };
            audit_transition(
                &broker.audit_log,
                &broker.config.break_glass,
                ctx,
                &GatedTransition {
                    action: BreakGlassAction::UncleanElectLeaders,
                    target,
                    phase,
                    proposal_id: *proposal_id,
                    reason,
                },
            );
        }
    }
}
