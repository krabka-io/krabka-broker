//! Metadata submission: pre-validation and KIP-631 encoding of a caller's
//! records against a scratch image, the append that assigns their offsets, and
//! the parked commit waiters that the high watermark later resolves or fails.

use krabka_ids::Offset;
use krabka_metadata::{MetadataRecord, to_kraft_values};
use tokio::sync::oneshot;

use super::{
    CommitWaiter, Engine,
    offsets::{
        assigned_record_offset, hwm_reaches_waiter, is_single_voter_majority,
        submit_waiter_need_offset, validate_append_result,
    },
    records::metadata_record_batch,
};
use crate::{OffsetReservation, SubmitChangeResult, error::RaftError};

impl Engine {
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
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
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
                let (base_offset, _next_offset) = krabka_verified::reserve_offsets(
                    scratch
                        .partition_next_offset(&r.topic, r.partition)
                        .unwrap_or(0),
                    r.count,
                );
                result.offset_reservations.push(OffsetReservation {
                    topic: r.topic.clone(),
                    partition: r.partition,
                    base_offset,
                    count: r.count,
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

        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut batch = metadata_record_batch(leader_epoch, &value_blobs);
        let base = match self.log.append(&mut batch) {
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
        let mut batch = metadata_record_batch(leader_epoch, &blobs);
        let expected_base = self.log.log_end_offset();
        let base = match self.log.append(&mut batch) {
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
