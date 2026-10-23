//! Serialized metadata submission: admission delegation, consensus append, and
//! commit waiters that the high watermark later resolves or fails.

use {
    super::{
        CommitWaiter, Engine, KraftController,
        mutation_policy::{
            MutationAdmission, PreparedRecords, created_topic_names, prepare_records,
        },
        offsets::{hwm_reaches_waiter, submit_waiter_need_offset, validate_append_result},
        records::{
            decode_committed_value, is_kip835_noop, metadata_record_batch, next_batch_offset,
        },
    },
    crate::{DelegationTokenMutation, SubmitChangeResult, error::RaftError, kraft::role::Role},
    krabka_ids::Offset,
    krabka_metadata::{MetadataImage, MetadataRecord, to_kraft_values},
    std::sync::Arc,
    tokio::sync::oneshot,
};

impl Engine {
    fn mutation_admission(&self) -> MutationAdmission<'_> {
        MutationAdmission {
            image: &self.image,
            uncommitted_tail: self.log.hwm() < self.log.log_end_offset(),
        }
    }

    /// Checks a handler's admission image in the same engine turn as the append.
    /// No precomputed mutation crosses a leadership change or an uncommitted tail.
    pub fn on_submit_change_if_image(
        &mut self,
        records: &[MetadataRecord],
        expected_image: Option<&Arc<MetadataImage>>,
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        if let Some(expected) = expected_image {
            let error = if !self.core.role().is_leader() {
                Some(RaftError::NotLeader {
                    current_leader: self.core.quorum_state().leader_id,
                })
            } else if !Arc::ptr_eq(expected, &self.image.published()) {
                Some(RaftError::StaleMetadataImage)
            } else if !self.uncommitted_tail_is_idle() {
                Some(RaftError::UncommittedTail)
            } else {
                None
            };
            if let Some(error) = error {
                let _ = reply.send(Err(error));
                return;
            }
        }
        self.on_submit_change(records, reply);
    }

    pub fn on_submit_delegation_token_mutations(
        &mut self,
        mutations: &[DelegationTokenMutation],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        if !self.core.role().is_leader() {
            let current_leader = self.core.quorum_state().leader_id;
            let _ = reply.send(Err(RaftError::NotLeader { current_leader }));
            return;
        }

        let records = match self
            .mutation_admission()
            .delegation_token_mutations(mutations, KraftController::wall_clock_ms())
        {
            Ok(records) => records,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        if records.is_empty() {
            let _ = reply.send(Ok(SubmitChangeResult::default()));
            return;
        }
        self.on_submit_change_guarded(&records, reply, true, Submitter::Request);
    }

    /// Replay the uncommitted records of earlier leader epochs into a copy of
    /// the committed image, and give each record to `visit` before it
    /// applies.
    ///
    /// The records between the high watermark and this leader's epoch start
    /// offset were appended by an earlier leader, possibly this node, and are
    /// not committed. They commit when this epoch's first record does. Each
    /// value is decoded against the image that the values before it produce,
    /// as a replica replays it. A value that names state the image does not
    /// hold, or that does not validate, is skipped, as replay skips it (see
    /// [`decode_committed_value`]). A node that does not lead has no such
    /// records, and gets the committed image.
    ///
    /// # Errors
    ///
    /// Returns the error of a log read that fails, and
    /// [`RaftError::MetadataReplay`] for a value that does not decode. `visit`
    /// has then seen the records before it. The value commits with this
    /// epoch's first record, and its apply then stops the controller.
    pub(super) fn replay_earlier_epoch_tail(
        &self,
        mut visit: impl FnMut(&MetadataImage, &MetadataRecord),
    ) -> Result<MetadataImage, RaftError> {
        let mut image = self.image.clone();
        let Role::Leader {
            epoch_start_offset, ..
        } = self.core.role()
        else {
            return Ok(image);
        };
        let end = Offset(*epoch_start_offset);
        let mut cursor = self.log.hwm();
        while cursor < end {
            let batches = self
                .log
                .read_decoded(cursor, self.metadata_raft_fetch_max.size())?;
            let Some(next) = next_batch_offset(&batches).filter(|next| *next > cursor) else {
                break;
            };
            for batch in &batches {
                if batch.base_offset >= end.0 || batch.attributes.is_control_batch() {
                    continue;
                }
                for (offset, value) in batch.records.iter().filter_map(|record| {
                    let offset = batch
                        .base_offset
                        .saturating_add(i64::from(record.offset_delta));
                    record.value.as_ref().map(|value| (offset, value))
                }) {
                    let Some(record) = decode_committed_value(value, &image, offset)? else {
                        continue;
                    };
                    if image.validate(&record).is_err() {
                        continue;
                    }
                    visit(&image, &record);
                    image.apply(&record);
                }
            }
            cursor = next;
        }
        Ok(image)
    }

    /// The names of the topics that uncommitted records from earlier leader
    /// epochs create, as [`Self::replay_earlier_epoch_tail`] replays them.
    fn earlier_epoch_topic_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        // A tail that does not read adds no more names, the same as a tail
        // that ends there.
        let _ = self.replay_earlier_epoch_tail(|image, record| {
            if let MetadataRecord::V1Topic(topic) = record
                && image.topic(&topic.name).is_none()
            {
                names.push(topic.name.clone());
            }
        });
        names
    }

    /// Whether the log between the high watermark and the log end holds
    /// nothing that changes the metadata image: only KIP-835 no-ops and
    /// control batches, or nothing at all.
    ///
    /// An admission image is the committed image, so a plan built from it
    /// still describes the log end when the uncommitted tail changes nothing.
    /// The leader appends a no-op every `metadata.max.idle.interval.ms`
    /// whatever else it does, and a check that refused every uncommitted
    /// tail lost to it whenever the no-op landed just before the plan: the
    /// liveness ticker's failover, which retries once a second, could then be
    /// refused for as long as the two timers stayed in step. Kafka's
    /// controller plans against its uncommitted state and has no such window.
    ///
    /// A tail that does not read counts as changing the image.
    fn uncommitted_tail_is_idle(&self) -> bool {
        let end = self.log.log_end_offset();
        let mut cursor = self.log.hwm();
        let mut idle = true;
        while idle && cursor < end {
            let batches = self
                .log
                .read_decoded(cursor, self.metadata_raft_fetch_max.size())
                .unwrap_or_default();
            let next = next_batch_offset(&batches).filter(|next| *next > cursor);
            idle = next.is_some()
                && batches
                    .iter()
                    .filter(|batch| !batch.attributes.is_control_batch())
                    .flat_map(|batch| &batch.records)
                    .filter_map(|record| record.value.as_ref())
                    .all(|value| is_kip835_noop(value));
            cursor = next.unwrap_or(end);
        }
        idle
    }

    /// Whether a change to the registration of `node_id` must wait for the
    /// log to commit before the leader can decide it.
    ///
    /// A change names the broker epoch of the registration it was built from,
    /// and applies only while the broker is still registered at that epoch,
    fn leader_epoch_ready(&self) -> bool {
        match self.core.role() {
            Role::Leader {
                epoch_start_offset, ..
            } => {
                krabka_verified::wal_reservation_epoch_ready(self.log.hwm().0, *epoch_start_offset)
            }
            _ => false,
        }
    }

    /// as Kafka's `ClusterControlManager.replayRegistrationChange` refuses a
    /// `BrokerRegistrationChangeRecord` for any other epoch. The leader
    /// decides that against its committed image. A registration of the same
    /// broker this leader appended but has not committed yet is not in that
    /// image, and neither is anything a previous leader left uncommitted until
    /// this leader's own epoch commits; either could replace the registration
    /// the change names. The refusal clears when the tail commits, and the
    /// caller builds the change again from the image that holds it.
    fn registration_change_must_wait(&self, node_id: krabka_metadata::NodeId) -> bool {
        let epoch_ready = self.leader_epoch_ready();
        let leader_epoch = self.core.quorum_state().leader_epoch;
        !epoch_ready
            || self
                .registration_writes
                .get(&node_id)
                .is_some_and(|&(epoch, end)| {
                    epoch == leader_epoch && !hwm_reaches_waiter(self.log.hwm(), end)
                })
    }

    /// Handle a `submit_change`: leader appends + parks a waiter; non-leader
    /// rejects immediately with the leader hint.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            node = self.me.0,
            epoch = self.core.quorum_state().leader_epoch,
            is_leader = self.core.role().is_leader(),
            records = records.len()
        )
    )]
    pub fn on_submit_change(
        &mut self,
        records: &[krabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        self.on_submit_change_guarded(records, reply, false, Submitter::Request);
    }

    /// Append the activation batch of a log that holds no `metadata.version`.
    ///
    /// It is [`Self::on_submit_change`] except that it may reserve diskless
    /// offsets before this leader's epoch commits. See [`Submitter::Activation`].
    pub(super) fn on_activation_change(
        &mut self,
        records: &[krabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        self.on_submit_change_guarded(records, reply, false, Submitter::Activation);
    }

    fn on_submit_change_guarded(
        &mut self,
        records: &[krabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
        delegation_token_guarded: bool,
        submitter: Submitter,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let epoch_ready = self.leader_epoch_ready();
        // A new leader reserves no offsets until its own epoch commits. The
        // refusal is transient, as the refusal of a compare-and-set behind
        // an uncommitted tail is, so it is the same error. A broker then
        // tells the refusal apart from a failed reservation, and the forward
        // path keeps it. The activation batch is the exception.
        if records
            .iter()
            .any(|record| matches!(record, MetadataRecord::V1PartitionOffsetAdvance(_)))
            && !epoch_ready
            && submitter == Submitter::Request
        {
            let _ = reply.send(Err(RaftError::UncommittedTail));
            return;
        }

        if let Err(error) = self.mutation_admission().check_compare_and_set_records(
            records,
            delegation_token_guarded,
            |node_id| self.registration_change_must_wait(node_id),
        ) {
            let _ = reply.send(Err(error));
            return;
        }

        // Kafka's `QuorumController` replays each record into its in-memory
        // state before the record commits. Thus `ReplicationControlManager`
        // `createTopics` sees a pending topic name as an existing topic and
        // answers `TOPIC_ALREADY_EXISTS`. This leader validates against the
        // applied image only, so it checks the names that parked waiters
        // create.
        //
        // A leadership loss drops the waiters, but their records can stay in
        // the log. This leader never truncates its own log, so a record from
        // an earlier epoch commits with this epoch's first record. Its topic
        // names count as pending too.
        let creates = created_topic_names(&self.image, records);
        let earlier_epoch_creates = if creates.is_empty() {
            Vec::new()
        } else {
            self.earlier_epoch_topic_names()
        };
        if let Some(name) = creates.iter().find(|name| {
            earlier_epoch_creates.contains(name)
                || self
                    .commit_waiters
                    .iter()
                    .any(|waiter| waiter.creates.contains(name))
        }) {
            let _ = reply.send(Err(RaftError::Metadata(
                krabka_metadata::MetadataError::TopicExists(name.clone()),
            )));
            return;
        }

        // Pre-validate and translate to KIP-631 value blobs in ONE pass against
        // an evolving scratch image, so config-diff / ACL-resolution in
        // `to_kraft_values` see in-batch prior records (a batch mixing
        // topic+partition is validated and encoded as a sequence).

        // KIP-903: broker epoch = the offset this batch commits at. The i-th
        // value blob lands at `assign_base + i`; a V1BrokerRegistration fans
        // out to exactly one blob, so its offset delta equals the number of
        // blobs already allocated. Single-writer leader: the current log end
        // offset is the base `append` will return.
        let assign_base = self.log.log_end_offset();

        let PreparedRecords {
            value_blobs,
            result,
            registration_nodes,
        } = match prepare_records(
            records,
            self.image.overlay(),
            self.image.overlay(),
            assign_base,
            leader_epoch,
            &self
                .commit_waiters
                .iter()
                .flat_map(|waiter| &waiter.result.offset_reservations),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };

        // Every record fanned out to nothing (e.g. an empty config clear): the
        // submit is a committed no-op. Reply success without appending a batch.
        if value_blobs.is_empty() {
            let _ = reply.send(Ok(result));
            return;
        }

        let mut batch = match metadata_record_batch(leader_epoch, &value_blobs) {
            Ok(batch) => batch,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let base = match self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())
        {
            Ok(off) => off,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if let Err(e) = validate_append_result(
            "submit-change",
            assign_base,
            base,
            self.log.log_end_offset(),
        ) {
            let _ = reply.send(Err(e));
            return;
        }
        let need_offset = submit_waiter_need_offset(base, value_blobs.len());
        for node_id in registration_nodes {
            self.registration_writes
                .insert(node_id, (leader_epoch, need_offset));
        }
        // Park the waiter, then try to advance the HWM immediately: a single
        // voter commits its own append with no peer fetch.
        self.commit_waiters.push(CommitWaiter {
            base_offset: base,
            need_offset,
            rejection: None,
            creates,
            result,
            reply,
            _processing: self.turn.take(),
        });
        // Drive a self-fetch so the core recomputes the HWM (single voter
        // commits immediately; multi-voter commits when followers fetch).
        self.advance_if_single_voter();
        self.try_resolve_waiters();
    }

    /// Test-only: append a metadata batch and commit it through the real apply
    /// pipeline. Returns the appended base offset (or -1 on failure).
    #[cfg(test)]
    pub fn test_append_and_commit(&mut self, records: &[krabka_metadata::MetadataRecord]) -> i64 {
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut scratch = self.image.clone();
        let mut blobs: Vec<bytes::Bytes> = Vec::new();
        for r in records {
            if let Ok(mut bs) = to_kraft_values(r, &scratch) {
                blobs.append(&mut bs);
            }
            scratch.apply(r);
        }
        let mut batch = match metadata_record_batch(leader_epoch, &blobs) {
            Ok(batch) => batch,
            Err(e) => {
                tracing::error!(?e, "kraft: test batch construction failed");
                return -1;
            }
        };
        let expected_base = self.log.log_end_offset();
        let base = match self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())
        {
            Ok(off) => off,
            Err(e) => {
                tracing::error!(?e, "kraft: test append failed");
                return -1;
            }
        };
        if let Err(e) = validate_append_result(
            "test append",
            expected_base,
            base,
            self.log.log_end_offset(),
        ) {
            tracing::error!(?e, "kraft: test append invariant failed");
            return -1;
        }
        self.advance_and_apply(self.log.log_end_offset());
        // Test helper returns the raw base offset (compared against `-1` sentinel).
        base.0
    }

    /// Attach a rejection to the waiter whose appended range
    /// `[base_offset, need_offset)` actually contains `record_offset`. Gating on
    /// both bounds (not just `need_offset > record_offset`) prevents a failing
    /// record from bleeding its rejection onto later, unrelated waiters whose
    /// own records committed fine (FIX 2).
    pub fn note_rejection(&mut self, record_offset: Offset, err: &krabka_metadata::MetadataError) {
        for w in &mut self.commit_waiters {
            if w.base_offset <= record_offset
                && record_offset < w.need_offset
                && w.rejection.is_none()
            {
                w.rejection = Some(RaftError::Metadata(err.clone()));
            }
        }
    }

    /// Resolve every waiter whose target offset is now committed.
    pub fn try_resolve_waiters(&mut self) {
        let hwm = self.log.hwm();
        let mut still = Vec::new();
        for w in self.commit_waiters.drain(..) {
            if hwm_reaches_waiter(hwm, w.need_offset) {
                let result = w.rejection.map_or(Ok(w.result), Err);
                let _ = w.reply.send(result);
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }

    pub fn fail_waiters_reached_by(&mut self, hwm: Offset, reason: &str) {
        let mut still = Vec::new();
        for w in self.commit_waiters.drain(..) {
            if hwm_reaches_waiter(hwm, w.need_offset) {
                let _ = w
                    .reply
                    .send(Err(RaftError::ChangeRejected(reason.to_string())));
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }
}

/// Who appends a batch, which decides when the batch may reserve diskless
/// offsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Submitter {
    /// A broker or client request. It reserves offsets only once this
    /// leader's epoch commits, so every reservation an earlier leader left
    /// uncommitted is committed and folded in, or truncated, first.
    Request,
    /// The activation batch: the bootstrap records of a log that holds no
    /// `metadata.version`. It is this epoch's first write after its
    /// `LeaderChange`, on a log that holds no earlier record, so no earlier
    /// leader left a reservation for it to fold in. A restored cluster's
    /// bootstrap starts each diskless partition's next offset at its
    /// materialized end with a `V1PartitionOffsetAdvance` (#1524), and the
    /// activation batch has to carry it.
    Activation,
}

#[cfg(test)]
mod tests {
    use super::KraftController;

    #[test]
    fn wall_clock_ms_is_real_timestamp() {
        assert2::check!(KraftController::wall_clock_ms() > 1_700_000_000_000);
    }
}
