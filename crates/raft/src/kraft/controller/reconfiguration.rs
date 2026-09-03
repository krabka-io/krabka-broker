//! KIP-853 reconfiguration: validation and append of one voter or
//! `kraft.version` control operation, and the control-record bookkeeping that
//! tracks it from append through truncation to commitment.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_metadata::{MetadataRecord, VotersRecord};
use krabka_protocol::{
    owned::k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
    records::{RecordBatch, metadata::control::ControlRecord},
};
use krabka_verified::{
    VoterChangeKind, VoterReconfigurationDecision, voter_reconfiguration_decision,
};
use tokio::sync::oneshot;

use super::{
    Engine, PendingReconfig,
    control_state::{voter_set_to_wire, voter_supports_version},
    offsets::{hwm_reaches_waiter, is_single_voter_majority, validate_append_result},
    records::{decode_control_record, leader_change_batch, typed_control_batch},
};
use crate::{
    NodeId,
    error::RaftError,
    kraft::{role::Role, types::Epoch},
    reconfig::ReconfigOutcome,
};

fn rejected_reconfiguration(
    decision: VoterReconfigurationDecision,
    leader: Option<NodeId>,
    target_id: Option<NodeId>,
    current_version: u16,
    requested_version: u16,
    lag: u64,
) -> Result<ReconfigOutcome, RaftError> {
    match decision {
        VoterReconfigurationDecision::NotLeader => Ok(ReconfigOutcome::NotLeader { leader }),
        VoterReconfigurationDecision::InProgress
        | VoterReconfigurationDecision::EpochUncommitted => Err(RaftError::ReconfigInProgress),
        VoterReconfigurationDecision::EmptyCurrentVoterSet => Err(RaftError::ReconfigRejected(
            "cannot reconfigure an empty voter set".into(),
        )),
        VoterReconfigurationDecision::UnsupportedKraftVersion => {
            Err(RaftError::UnsupportedKraftVersion(current_version))
        }
        VoterReconfigurationDecision::DuplicateVoter => Err(target_id.map_or_else(
            || RaftError::ReconfigRejected("duplicate voter without a voter id".into()),
            RaftError::DuplicateVoter,
        )),
        VoterReconfigurationDecision::IncompatibleVoter => {
            let message = target_id.map_or_else(
                || format!("not every voter supports kraft.version {requested_version}"),
                |id| format!("voter {id} does not support kraft.version {current_version}"),
            );
            Err(RaftError::InvalidVoterUpdate(message))
        }
        VoterReconfigurationDecision::VoterNotCaughtUp => target_id.map_or_else(
            || {
                Err(RaftError::ReconfigRejected(
                    "caught-up voter check did not identify a voter".into(),
                ))
            },
            |id| Err(RaftError::VoterNotCaughtUp { id, lag }),
        ),
        VoterReconfigurationDecision::VoterNotFound
        | VoterReconfigurationDecision::DirectoryMismatch => target_id.map_or_else(
            || {
                Err(RaftError::ReconfigRejected(
                    "voter lookup did not identify a voter".into(),
                ))
            },
            |id| Err(RaftError::VoterNotFound(id)),
        ),
        VoterReconfigurationDecision::LastVoter => Err(RaftError::ReconfigRejected(
            "cannot remove the last voter".into(),
        )),
        VoterReconfigurationDecision::InvalidVersionTransition => {
            Err(RaftError::InvalidVoterUpdate(format!(
                "kraft.version transition {current_version} -> {requested_version} is not supported"
            )))
        }
        VoterReconfigurationDecision::Admit(_) => Err(RaftError::ReconfigRejected(
            "admitted voter change was handled as a rejection".into(),
        )),
    }
}

impl Engine {
    /// Append the leader's `LeaderChange` control marker for `epoch`.
    #[tracing::instrument(level = "info", skip_all, fields(node = self.me.0, epoch), err)]
    pub fn append_leader_change(&mut self, epoch: Epoch) -> Result<Offset, RaftError> {
        let mut batch = leader_change_batch(
            epoch,
            self.me,
            &self.core.quorum_state().voters,
            self.controls.latest_version(),
        );
        let expected_base = self.log.log_end_offset();
        let base = self.log.append(&mut batch, Self::wall_clock_ms())?;
        validate_append_result(
            "leader-change",
            expected_base,
            base,
            self.log.log_end_offset(),
        )?;
        Ok(base)
    }

    /// Validate and append one KIP-853 control operation. The voter set is
    /// applied to the core immediately after append; completion waits for the
    /// resulting batch to cross the HWM unless `AddRaftVoter` v1 requested an
    /// append-only acknowledgement.
    pub fn on_reconfigure(
        &mut self,
        change: crate::reconfig::VoterChange,
        reply: oneshot::Sender<Result<crate::reconfig::ReconfigOutcome, RaftError>>,
    ) {
        use crate::reconfig::VoterChange;

        let current = self.controls.committed_voters.clone();
        let current_version = self.controls.committed_version;
        let is_leader = self.core.role().is_leader();
        let single_flight_clear = self.pending_reconfig.is_none()
            && self.controls.latest_voters() == &current
            && self.controls.latest_version() == current_version;
        let epoch_committed = match self.core.role() {
            Role::Leader {
                epoch_start_offset, ..
            } => self.log.hwm().0 > *epoch_start_offset,
            _ => false,
        };
        let (
            kind,
            requested_version,
            target_id,
            target_present,
            directory_matches,
            target_version_compatible,
            target_caught_up,
            all_voters_support_v1,
            lag,
        ) = match &change {
            VoterChange::Add(request) => {
                let leader_end = self.log.log_end_offset().0;
                let observer_end = self
                    .replica_fetch_offsets
                    .get(&request.voter.id)
                    .copied()
                    .unwrap_or(0);
                (
                    VoterChangeKind::Add,
                    current_version,
                    Some(request.voter.id),
                    current.contains(request.voter.id),
                    true,
                    voter_supports_version(&request.voter, current_version),
                    observer_end >= leader_end,
                    true,
                    u64::try_from(leader_end.saturating_sub(observer_end)).unwrap_or(u64::MAX),
                )
            }
            VoterChange::Remove(request) => (
                VoterChangeKind::Remove,
                current_version,
                Some(request.id),
                current.contains(request.id),
                current
                    .get(request.id)
                    .is_some_and(|voter| voter.directory_id == request.directory_id),
                true,
                true,
                true,
                0,
            ),
            VoterChange::Update(request) => (
                VoterChangeKind::Update,
                current_version,
                Some(request.voter.id),
                current.contains(request.voter.id),
                current.get(request.voter.id).is_some_and(|voter| {
                    voter.directory_id == uuid::Uuid::nil()
                        || voter.directory_id == request.voter.directory_id
                }),
                voter_supports_version(&request.voter, current_version),
                true,
                true,
                0,
            ),
            VoterChange::FinalizeKraftVersion(version) => (
                VoterChangeKind::FinalizeKraftVersion,
                *version,
                None,
                false,
                true,
                true,
                true,
                current
                    .iter()
                    .all(|voter| voter_supports_version(voter, *version)),
                0,
            ),
        };

        let decision = voter_reconfiguration_decision(
            (is_leader, single_flight_clear, epoch_committed),
            (current.len(), current_version, all_voters_support_v1),
            (kind, requested_version, target_present),
            (
                directory_matches,
                target_version_compatible,
                target_caught_up,
            ),
        );
        let plan = match decision {
            VoterReconfigurationDecision::Admit(plan) => plan,
            rejected => {
                let _ = reply.send(rejected_reconfiguration(
                    rejected,
                    self.core.quorum_state().leader_id,
                    target_id,
                    current_version,
                    requested_version,
                    lag,
                ));
                return;
            }
        };

        let (next, ack_when_committed, removed_local_leader) = match change {
            VoterChange::Add(request) => (
                current.with_voter(request.voter),
                request.ack_when_committed,
                false,
            ),
            VoterChange::Remove(request) => (
                current.without_voter(request.id),
                true,
                request.id == self.me,
            ),
            VoterChange::Update(request) => (current.with_voter(request.voter), true, false),
            VoterChange::FinalizeKraftVersion(_) => (current.clone(), true, false),
        };
        if next.len() != plan.next_voter_count {
            let _ = reply.send(Err(RaftError::ReconfigRejected(
                "constructed voter set does not match the proved result".into(),
            )));
            return;
        }

        if plan.preflight_only {
            // At level 0 UpdateVoter supplies upgrade preflight data; no
            // VotersRecord may be written yet.
            self.controls.committed_voters = next.clone();
            self.controls.voter_history.insert(-1, next.clone());
            let actions = self.core.apply_voter_set(next.clone(), self.now());
            self.peers.update_voters(&next);
            self.execute(actions);
            self.publish_leader();
            let _ = reply.send(Ok(ReconfigOutcome::Committed));
            return;
        }

        let mut records = Vec::with_capacity(2);
        if plan.write_kraft_version {
            records.push(ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                version: 0,
                k_raft_version: i16::try_from(plan.next_kraft_version).unwrap_or(i16::MAX),
                ..Default::default()
            }));
        }
        if plan.write_voters {
            records.push(ControlRecord::Voters(voter_set_to_wire(&next)));
        }

        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut batch = match typed_control_batch(leader_epoch, &records) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let base = match self.log.append(&mut batch, Self::wall_clock_ms()) {
            Ok(base) => base,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        if let Err(error) = self.apply_control_batch(&batch) {
            let _ = reply.send(Err(error));
            return;
        }
        let need_offset = Offset(
            base.0
                .saturating_add(i64::try_from(records.len()).unwrap_or(i64::MAX)),
        );
        let waiter_reply = if ack_when_committed {
            Some(reply)
        } else {
            let _ = reply.send(Ok(ReconfigOutcome::Committed));
            None
        };
        self.pending_reconfig = Some(PendingReconfig {
            need_offset,
            reply: waiter_reply,
            removed_local_leader,
        });

        if is_single_voter_majority(self.core.quorum_state().majority()) {
            self.advance_and_apply(self.log.log_end_offset());
        }
        self.publish_leader();
    }

    /// Apply KIP-853 controls as soon as their batch is appended or fetched.
    /// Consensus always uses the latest local view, even before commitment.
    pub fn apply_control_batch(&mut self, batch: &RecordBatch) -> Result<(), RaftError> {
        if !batch.attributes.is_control_batch() {
            return Ok(());
        }
        let previous = self.controls.latest_voters().clone();
        for record in &batch.records {
            let Some(control) = decode_control_record(record)? else {
                continue;
            };
            let offset = batch
                .base_offset
                .saturating_add(i64::from(record.offset_delta));
            self.controls.apply(offset, &control)?;
        }
        let latest = self.controls.latest_voters().clone();
        if latest != previous {
            let actions = self.core.apply_voter_set(latest.clone(), self.now());
            self.peers.update_voters(&latest);
            self.execute(actions);
        }
        Ok(())
    }

    pub fn restore_control_state_after_truncation(&mut self, offset: i64) {
        let previous = self.controls.latest_voters().clone();
        self.controls.truncate_to(offset);
        let latest = self.controls.latest_voters().clone();
        if latest != previous {
            let actions = self.core.apply_voter_set(latest.clone(), self.now());
            self.peers.update_voters(&latest);
            self.execute(actions);
        }
        if self
            .pending_reconfig
            .as_ref()
            .is_some_and(|pending| pending.need_offset.0 > offset)
            && let Some(mut pending) = self.pending_reconfig.take()
            && let Some(reply) = pending.reply.take()
        {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
        }
    }

    pub fn commit_control_state(&mut self, high_watermark: Offset) {
        if !self.controls.commit_to(high_watermark.0) {
            return;
        }
        self.core.commit_voter_set();
        self.core.set_kraft_version(self.controls.committed_version);
        self.image.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord {
                kraft_version: self.controls.committed_version,
            },
        ));
        self.image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: self.controls.committed_voters.clone(),
        }));
        if self.downgrade_snapshot_pending.is_none() {
            let _ = self.image_tx.send(Arc::new(self.image.clone()));
        }
    }

    pub fn try_resolve_reconfiguration(&mut self) {
        let Some(pending) = self.pending_reconfig.as_ref() else {
            return;
        };
        if !hwm_reaches_waiter(self.log.hwm(), pending.need_offset) {
            return;
        }
        let Some(mut pending) = self.pending_reconfig.take() else {
            return;
        };
        if let Some(reply) = pending.reply.take() {
            let _ = reply.send(Ok(crate::reconfig::ReconfigOutcome::Committed));
        }
        if pending.removed_local_leader {
            let actions = self.core.finish_local_leader_removal(self.now());
            self.execute(actions);
            self.reconcile_timers("leader");
        }
    }
}
