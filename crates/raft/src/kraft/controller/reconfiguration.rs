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
use tokio::sync::oneshot;

use super::{
    Engine, PendingReconfig,
    control_state::{voter_set_to_wire, voter_supports_version},
    offsets::{hwm_reaches_waiter, is_single_voter_majority, validate_append_result},
    records::{decode_control_record, leader_change_batch, typed_control_batch},
};
use crate::{
    error::RaftError,
    kraft::{role::Role, types::Epoch},
};

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
        let base = self.log.append(&mut batch)?;
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
        use crate::reconfig::{ReconfigOutcome, VoterChange};

        if !self.core.role().is_leader() {
            let _ = reply.send(Ok(ReconfigOutcome::NotLeader {
                leader: self.core.quorum_state().leader_id,
            }));
            return;
        }
        if self.pending_reconfig.is_some()
            || self.controls.latest_voters() != &self.controls.committed_voters
            || self.controls.latest_version() != self.controls.committed_version
        {
            let _ = reply.send(Err(RaftError::ReconfigInProgress));
            return;
        }
        if let Role::Leader {
            epoch_start_offset, ..
        } = self.core.role()
            && self.log.hwm().0 <= *epoch_start_offset
        {
            let _ = reply.send(Err(RaftError::ReconfigInProgress));
            return;
        }

        let current = self.controls.committed_voters.clone();
        let current_version = self.controls.committed_version;
        let (records, ack_when_committed, removed_local_leader) = match change {
            VoterChange::Add(request) => {
                if current_version < 1 {
                    let _ = reply.send(Err(RaftError::UnsupportedKraftVersion(current_version)));
                    return;
                }
                if current.contains(request.voter.id) {
                    let _ = reply.send(Err(RaftError::DuplicateVoter(request.voter.id)));
                    return;
                }
                if !voter_supports_version(&request.voter, current_version) {
                    let _ = reply.send(Err(RaftError::InvalidVoterUpdate(format!(
                        "voter {} does not support kraft.version {current_version}",
                        request.voter.id
                    ))));
                    return;
                }
                let leader_end = self.log.log_end_offset().0;
                let observer_end = self
                    .replica_fetch_offsets
                    .get(&request.voter.id)
                    .copied()
                    .unwrap_or(0);
                if observer_end < leader_end {
                    let lag =
                        u64::try_from(leader_end.saturating_sub(observer_end)).unwrap_or(u64::MAX);
                    let _ = reply.send(Err(RaftError::VoterNotCaughtUp {
                        id: request.voter.id,
                        lag,
                    }));
                    return;
                }
                let next = current.with_voter(request.voter);
                (
                    vec![ControlRecord::Voters(voter_set_to_wire(&next))],
                    request.ack_when_committed,
                    false,
                )
            }
            VoterChange::Remove(request) => {
                if current_version < 1 {
                    let _ = reply.send(Err(RaftError::UnsupportedKraftVersion(current_version)));
                    return;
                }
                let Some(existing) = current.get(request.id) else {
                    let _ = reply.send(Err(RaftError::VoterNotFound(request.id)));
                    return;
                };
                if existing.directory_id != request.directory_id {
                    let _ = reply.send(Err(RaftError::VoterNotFound(request.id)));
                    return;
                }
                if current.len() == 1 {
                    let _ = reply.send(Err(RaftError::ReconfigRejected(
                        "cannot remove the last voter".into(),
                    )));
                    return;
                }
                let next = current.without_voter(request.id);
                (
                    vec![ControlRecord::Voters(voter_set_to_wire(&next))],
                    true,
                    request.id == self.me,
                )
            }
            VoterChange::Update(request) => {
                let Some(existing) = current.get(request.voter.id) else {
                    let _ = reply.send(Err(RaftError::VoterNotFound(request.voter.id)));
                    return;
                };
                if existing.directory_id != uuid::Uuid::nil()
                    && existing.directory_id != request.voter.directory_id
                {
                    let _ = reply.send(Err(RaftError::VoterNotFound(request.voter.id)));
                    return;
                }
                if !voter_supports_version(&request.voter, current_version) {
                    let _ = reply.send(Err(RaftError::InvalidVoterUpdate(format!(
                        "voter {} does not support kraft.version {current_version}",
                        request.voter.id
                    ))));
                    return;
                }
                if current_version == 0 {
                    // At level 0 UpdateVoter supplies upgrade preflight data;
                    // no VotersRecord may be written yet.
                    let next = current.with_voter(request.voter);
                    self.controls.committed_voters = next.clone();
                    self.controls.voter_history.insert(-1, next.clone());
                    let actions = self.core.apply_voter_set(next.clone(), self.now());
                    self.peers.update_voters(&next);
                    self.execute(actions);
                    self.publish_leader();
                    let _ = reply.send(Ok(ReconfigOutcome::Committed));
                    return;
                }
                let next = current.with_voter(request.voter);
                (
                    vec![ControlRecord::Voters(voter_set_to_wire(&next))],
                    true,
                    false,
                )
            }
            VoterChange::FinalizeKraftVersion(version) => {
                if version != 1 || current_version != 0 {
                    let _ = reply.send(Err(RaftError::InvalidVoterUpdate(format!(
                        "kraft.version transition {current_version} -> {version} is not supported"
                    ))));
                    return;
                }
                if current
                    .iter()
                    .any(|voter| !voter_supports_version(voter, version))
                {
                    let _ = reply.send(Err(RaftError::InvalidVoterUpdate(
                        "not every voter supports kraft.version 1".into(),
                    )));
                    return;
                }
                (
                    vec![
                        ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                            version: 0,
                            k_raft_version: 1,
                            ..Default::default()
                        }),
                        ControlRecord::Voters(voter_set_to_wire(&current)),
                    ],
                    true,
                    false,
                )
            }
        };

        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut batch = match typed_control_batch(leader_epoch, &records) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let base = match self.log.append(&mut batch) {
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
