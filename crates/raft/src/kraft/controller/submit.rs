//! Metadata submission: pre-validation and KIP-631 encoding of a caller's
//! records against a scratch image, the append that assigns their offsets, and
//! the parked commit waiters that the high watermark later resolves or fails.

use krabka_ids::Offset;
use krabka_metadata::{
    BreakGlassProposalRecord, DelegationToken, DelegationTokenRecord, DeleteDelegationTokenRecord,
    MetadataImage, MetadataRecord, TopicFreezeRecord, to_kraft_values,
};
use tokio::sync::oneshot;

use super::{
    CommitWaiter, Engine,
    offsets::{
        assigned_record_offset, hwm_reaches_waiter, is_single_voter_majority,
        submit_waiter_need_offset, validate_append_result,
    },
    records::metadata_record_batch,
};
use crate::{
    DelegationTokenMutation, OffsetReservation, SubmitChangeResult, error::RaftError,
    kraft::role::Role,
};

impl Engine {
    fn delegation_token_record(token: &DelegationToken) -> DelegationTokenRecord {
        DelegationTokenRecord {
            token_id: token.token_id.clone(),
            owner: token.owner.clone(),
            hmac: token.hmac.clone(),
            issue_timestamp_ms: token.issue_timestamp_ms,
            expiry_timestamp_ms: token.expiry_timestamp_ms,
            max_timestamp_ms: token.max_timestamp_ms,
            renewers: token.renewers.clone(),
        }
    }

    /// Epoch milliseconds. Delegation-token deadlines are wall-clock by
    /// definition, and so is the create-time stamped on every batch the leader
    /// appends: `Engine::now` is monotonic from this process's own start, and
    /// a snapshot header timestamp has to mean the same instant on every node
    /// that reads it.
    pub(super) fn wall_clock_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};

        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
            })
    }

    fn token_generation_matches(
        expected: &DelegationTokenRecord,
        replacement: &DelegationTokenRecord,
    ) -> bool {
        expected.token_id == replacement.token_id
            && expected.owner == replacement.owner
            && expected.hmac == replacement.hmac
            && expected.issue_timestamp_ms == replacement.issue_timestamp_ms
            && expected.max_timestamp_ms == replacement.max_timestamp_ms
            && expected.renewers == replacement.renewers
    }

    fn token_mutation_decision(
        image: &MetadataImage,
        mutation: &DelegationTokenMutation,
        now_ms: i64,
        uncommitted_tail: bool,
    ) -> krabka_verified::TokenMutationDecision {
        let (kind, expected, replacement) = match mutation {
            DelegationTokenMutation::Renew {
                expected,
                replacement,
            } => (
                krabka_verified::TokenMutationKind::Renew,
                expected,
                Some(replacement),
            ),
            DelegationTokenMutation::Expire {
                expected,
                replacement,
            } => (
                krabka_verified::TokenMutationKind::Expire,
                expected,
                Some(replacement),
            ),
            DelegationTokenMutation::Delete { expected } => {
                (krabka_verified::TokenMutationKind::Delete, expected, None)
            }
        };
        let stored = image
            .delegation_token_by_id(&expected.token_id)
            .map(Self::delegation_token_record);
        let generation_matches = replacement
            .is_none_or(|replacement| Self::token_generation_matches(expected, replacement));
        let state = match (&stored, replacement, generation_matches) {
            (None, _, true) => krabka_verified::TokenMutationState::Missing,
            (Some(stored), Some(replacement), true) if stored == replacement => {
                krabka_verified::TokenMutationState::Applied
            }
            (Some(stored), _, true) if stored == expected => {
                krabka_verified::TokenMutationState::Expected
            }
            (_, _, false) | (Some(_), _, true) => krabka_verified::TokenMutationState::Stale,
        };
        krabka_verified::token_mutation_decision(krabka_verified::TokenMutationFacts {
            kind,
            state,
            now_ms,
            expected_expiry_ms: expected.expiry_timestamp_ms,
            incoming_expiry_ms: replacement.map_or(expected.expiry_timestamp_ms, |record| {
                record.expiry_timestamp_ms
            }),
            max_timestamp_ms: expected.max_timestamp_ms,
            uncommitted_tail,
        })
    }

    fn token_mutation_record(mutation: &DelegationTokenMutation) -> MetadataRecord {
        match mutation {
            DelegationTokenMutation::Renew { replacement, .. }
            | DelegationTokenMutation::Expire { replacement, .. } => {
                MetadataRecord::V1DelegationToken(replacement.clone())
            }
            DelegationTokenMutation::Delete { expected } => {
                MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
                    token_id: expected.token_id.clone(),
                })
            }
        }
    }

    fn token_mutation_id(mutation: &DelegationTokenMutation) -> &str {
        match mutation {
            DelegationTokenMutation::Renew { expected, .. }
            | DelegationTokenMutation::Expire { expected, .. }
            | DelegationTokenMutation::Delete { expected } => &expected.token_id,
        }
    }

    pub fn on_submit_delegation_token_mutations(
        &mut self,
        mutations: &[DelegationTokenMutation],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }

        let uncommitted_tail = self.log.hwm() < self.log.log_end_offset();
        let now_ms = Self::wall_clock_ms();
        let mut scratch = self.image.clone();
        let mut records = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let decision =
                Self::token_mutation_decision(&scratch, mutation, now_ms, uncommitted_tail);
            match decision {
                krabka_verified::TokenMutationDecision::Append => {
                    let record = Self::token_mutation_record(mutation);
                    scratch.apply(&record);
                    records.push(record);
                }
                krabka_verified::TokenMutationDecision::Retry => {}
                krabka_verified::TokenMutationDecision::Reject => {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                        "delegation-token mutation {} rejected",
                        Self::token_mutation_id(mutation)
                    ))));
                    return;
                }
            }
        }
        if records.is_empty() {
            let _ = reply.send(Ok(SubmitChangeResult::default()));
            return;
        }
        self.on_submit_change_guarded(&records, reply, true);
    }

    fn consumption_matches_stored(
        stored: &BreakGlassProposalRecord,
        consumed: &BreakGlassProposalRecord,
    ) -> bool {
        let mut expected = stored.clone();
        expected.consumed_at_ms = consumed.consumed_at_ms;
        expected == *consumed
    }

    fn break_glass_consumption_decision(
        &self,
        consumed: &BreakGlassProposalRecord,
    ) -> krabka_verified::BreakGlassConsumptionDecision {
        let stored = self.image.break_glass_proposal(consumed.proposal_id);
        let proposal = match stored {
            None => krabka_verified::BreakGlassProposalState::Missing,
            Some(stored)
                if Self::consumption_matches_stored(stored, consumed)
                    && stored.consumed_at_ms == 0
                    && !stored.withdrawn =>
            {
                krabka_verified::BreakGlassProposalState::ExactPending
            }
            Some(_) => krabka_verified::BreakGlassProposalState::Stale,
        };
        krabka_verified::break_glass_consumption_decision(
            krabka_verified::BreakGlassConsumptionFacts {
                proposal,
                consumed_at_ms: consumed.consumed_at_ms,
                // A consume is a security-sensitive compare-and-set. Require
                // the committed image to cover the whole log prefix before
                // appending it. This conservative fence survives leadership
                // loss, where a waiter can disappear while its uncommitted
                // log entry remains and may later commit.
                uncommitted_tail: self.log.hwm() < self.log.log_end_offset(),
            },
        )
    }

    fn freeze_replacement_decision(
        &self,
        incoming: &TopicFreezeRecord,
        another_freeze_in_batch: bool,
    ) -> krabka_verified::FreezeReplacementDecision {
        let stored = self
            .image
            .topic_freezes()
            .find(|stored| {
                stored.pattern_type == incoming.pattern_type && stored.scope == incoming.scope
            })
            .map_or(krabka_verified::FreezeStoredState::Missing, |stored| {
                krabka_verified::FreezeStoredState::Present {
                    set_at_ms: stored.set_at_ms,
                }
            });
        krabka_verified::freeze_replacement_decision(krabka_verified::FreezeReplacementFacts {
            stored,
            incoming_frozen: incoming.frozen,
            incoming_set_at_ms: incoming.set_at_ms,
            // Like proposal consumption, replacement is a compare-and-set
            // against the committed image. A retained tail or a second
            // freeze in this batch must commit or fail before retry.
            uncommitted_tail: another_freeze_in_batch || self.log.hwm() < self.log.log_end_offset(),
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
        self.on_submit_change_guarded(records, reply, false);
    }

    fn on_submit_change_guarded(
        &mut self,
        records: &[krabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
        delegation_token_guarded: bool,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let epoch_ready = match self.core.role() {
            Role::Leader {
                epoch_start_offset, ..
            } => {
                krabka_verified::wal_reservation_epoch_ready(self.log.hwm().0, *epoch_start_offset)
            }
            _ => false,
        };
        if records
            .iter()
            .any(|record| matches!(record, MetadataRecord::V1PartitionOffsetAdvance(_)))
            && !epoch_ready
        {
            let _ = reply.send(Err(RaftError::ChangeRejected(
                "leader epoch must commit before reserving offsets".to_string(),
            )));
            return;
        }

        let mut freeze_in_batch = false;
        let mut token_create_in_batch = std::collections::HashSet::new();
        for record in records {
            match record {
                MetadataRecord::V1BreakGlassProposal(consumed) if consumed.consumed_at_ms != 0 => {
                    let decision = self.break_glass_consumption_decision(consumed);
                    if decision != krabka_verified::BreakGlassConsumptionDecision::Append {
                        let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                            "break-glass consume {} rejected: {decision:?}",
                            consumed.proposal_id
                        ))));
                        return;
                    }
                }
                MetadataRecord::V1TopicFreeze(freeze) => {
                    let decision = self.freeze_replacement_decision(freeze, freeze_in_batch);
                    if decision != krabka_verified::FreezeReplacementDecision::Append {
                        let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                            "topic-freeze mutation {:?}:{} rejected: {decision:?}",
                            freeze.pattern_type, freeze.scope
                        ))));
                        return;
                    }
                    freeze_in_batch = true;
                }
                MetadataRecord::V1DelegationToken(token) if !delegation_token_guarded => {
                    let create_is_unique =
                        self.image.delegation_token_by_id(&token.token_id).is_none()
                            && token_create_in_batch.insert(token.token_id.clone());
                    if !create_is_unique || self.log.hwm() < self.log.log_end_offset() {
                        let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                            "delegation-token create {} rejected: replacement requires a guarded mutation",
                            token.token_id
                        ))));
                        return;
                    }
                }
                MetadataRecord::V1DeleteDelegationToken(token) if !delegation_token_guarded => {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                        "delegation-token delete {} rejected: mutation is not generation-bound",
                        token.token_id
                    ))));
                    return;
                }
                _ => {}
            }
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

        let mut scratch = self.image.clone();
        let mut result = SubmitChangeResult::default();
        let mut value_blobs: Vec<bytes::Bytes> = Vec::new();
        for r in records {
            // Stamp the registration epoch = its committed offset.
            let stamped;
            let r: &MetadataRecord = match r {
                MetadataRecord::V1BrokerRegistration(b) => {
                    let rewrites_existing = scratch.broker(b.node_id).is_some_and(|existing| {
                        existing.incarnation_id == b.incarnation_id
                            && existing.broker_epoch == b.broker_epoch
                    });
                    if rewrites_existing {
                        r
                    } else {
                        let delta = i64::try_from(value_blobs.len()).unwrap_or(i64::MAX);
                        let mut b = b.clone();
                        b.broker_epoch = assigned_record_offset(assign_base, delta);
                        stamped = MetadataRecord::V1BrokerRegistration(b);
                        &stamped
                    }
                }
                other => other,
            };
            if let Err(e) = scratch.validate(r) {
                let _ = reply.send(Err(RaftError::Metadata(e)));
                return;
            }
            if let MetadataRecord::V1PartitionOffsetAdvance(r) = r {
                let mut next_offset = scratch
                    .partition_next_offset(&r.topic, r.partition)
                    .unwrap_or(0);
                // A multi-voter leader may have earlier reservations appended
                // but not committed into `scratch` yet. Fold their exact
                // contiguous ends so concurrent submissions cannot reuse the
                // same committed base.
                for pending in self.commit_waiters.iter().flat_map(|waiter| {
                    waiter.result.offset_reservations.iter().filter(|pending| {
                        pending.topic == r.topic && pending.partition == r.partition
                    })
                }) {
                    let Some(frontier) = krabka_verified::wal_reservation_frontier(
                        next_offset,
                        pending.base_offset,
                        pending.count,
                    ) else {
                        let _ = reply.send(Err(RaftError::ChangeRejected(
                            "pending offset reservation chain is invalid".to_string(),
                        )));
                        return;
                    };
                    next_offset = frontier;
                }
                // `reserve_offsets` is proved only for positive counts whose
                // sum fits in i64, so reject untrusted metadata first.
                if r.count <= 0 || next_offset.checked_add(r.count).is_none() {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                        "partition offset advance count {} is out of range at next offset {next_offset}",
                        r.count
                    ))));
                    return;
                }
                let (base_offset, _next_offset) =
                    krabka_verified::reserve_offsets(next_offset, r.count);
                result.offset_reservations.push(OffsetReservation {
                    topic: r.topic.clone(),
                    partition: r.partition,
                    base_offset,
                    count: r.count,
                    leader_epoch: u64::from(leader_epoch),
                });
            }
            match to_kraft_values(r, &scratch) {
                Ok(mut blobs) => value_blobs.append(&mut blobs),
                Err(e) => {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!("encode: {e}"))));
                    return;
                }
            }
            scratch.apply(r);
        }

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
        let base = match self.log.append(&mut batch, Self::wall_clock_ms()) {
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
        // Park the waiter, then try to advance the HWM immediately: a single
        // voter commits its own append with no peer fetch.
        self.commit_waiters.push(CommitWaiter {
            base_offset: base,
            need_offset,
            rejection: None,
            result,
            reply,
        });
        // Drive a self-fetch so the core recomputes the HWM (single voter
        // commits immediately; multi-voter commits when followers fetch).
        if is_single_voter_majority(self.core.quorum_state().majority()) {
            self.advance_and_apply(self.log.log_end_offset());
        }
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
        let base = match self.log.append(&mut batch, Self::wall_clock_ms()) {
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
