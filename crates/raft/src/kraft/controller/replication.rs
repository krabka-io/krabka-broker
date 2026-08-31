//! The follower side of replication: the rules that classify a fetched batch
//! or a snapshot hint, the apply of a leader's Fetch response, and the
//! reassembly and install of a KIP-630 snapshot fetched from the leader.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord};

use super::{
    Engine, KraftControlState,
    checkpoint::{retain_latest_checkpoint, write_checkpoint},
    checkpoint_dir,
    offsets::fetch_offset_has_records,
    records::{decode_batches, encode_batches},
};
use crate::{
    error::RaftError,
    kraft::{
        event::Event,
        snapshot_fetch::{SnapshotFetchState, SnapshotFetchStep},
        transport::wire,
        types::{Epoch, NodeId},
    },
};

pub fn should_serve_fetch_records(
    has_snapshot: bool,
    has_divergence: bool,
    is_leader: bool,
) -> bool {
    matches!(
        (has_snapshot, has_divergence, is_leader),
        (false, false, true)
    )
}

pub fn fetch_epoch_for_request(
    installed_snapshot_epoch: Option<Epoch>,
    log_start: Offset,
    log_end: Offset,
    last_epoch: Epoch,
) -> Epoch {
    match installed_snapshot_epoch {
        Some(epoch) if log_end.cmp(&log_start).is_eq() => epoch,
        _ => last_epoch,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchBatchDisposition {
    AlreadyPresent,
    Append,
    Gap,
}

pub fn classify_fetch_batch(at: Offset, log_end: Offset) -> FetchBatchDisposition {
    match at.cmp(&log_end) {
        std::cmp::Ordering::Less => FetchBatchDisposition::AlreadyPresent,
        std::cmp::Ordering::Equal => FetchBatchDisposition::Append,
        std::cmp::Ordering::Greater => FetchBatchDisposition::Gap,
    }
}

pub fn should_start_snapshot_fetch(
    snapshot_id: (i64, i32),
    log_end: Offset,
    active_snapshot_id: Option<(i64, i32)>,
) -> bool {
    snapshot_id.0.cmp(&log_end.0).is_gt()
        && !matches!(active_snapshot_id, Some(id) if id == snapshot_id)
}

pub fn snapshot_fetch_response_invalid(error_code: i16, from: NodeId, leader_id: NodeId) -> bool {
    !matches!(
        (error_code.cmp(&0), from.cmp(&leader_id)),
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal)
    )
}

impl Engine {
    /// (Leader side) serialize every log batch at/after `fetch_offset` up to our
    /// log end into a length-prefixed run of `RecordBatch::encode` blobs for the
    /// fetching follower. `KRaft` replicates up to the leader's log end (not just
    /// the HWM — the HWM is carried separately in the response and gates apply on
    /// the follower); this is what moves real record bytes so multi-voter
    /// `submit_change` waiters can commit once a majority has fetched.
    pub fn serve_fetch_records(&self, fetch_offset: Offset) -> bytes::Bytes {
        let log_end = self.log.log_end_offset();
        if !fetch_offset_has_records(fetch_offset, log_end) {
            return bytes::Bytes::new();
        }
        let batches = match self
            .log
            .read_decoded(fetch_offset, self.metadata_raft_fetch_max.size())
        {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(?e, "kraft: serve_fetch read failed");
                return bytes::Bytes::new();
            }
        };
        encode_batches(&batches)
    }

    /// (Follower side) apply the leader's Fetch response: truncate on a
    /// divergence hint, append the carried batches at their leader-assigned
    /// offsets, advance our HWM to `min(leader_hwm, own log_end)`, apply the
    /// newly-committed records to the image, then feed the core
    /// `ReceiveFetchResponse` (which re-arms the fetch timer / re-fetches).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, from = from.0, log_end = self.log.log_end_offset().0)
    )]
    pub fn on_fetch_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::Fetch {
            leader_id,
            leader_epoch,
            diverging,
            snapshot_id,
            hwm,
            records,
        }) = wire::PeerResponse::decode_fetch(body)
        else {
            return;
        };
        self.peers.remember_peer(from, leader_id);

        // The leader signalled our fetch offset is below its pruned log-start:
        // we must fetch the snapshot instead of replicating from the log. Start
        // (or continue) a snapshot transfer, feed the core for liveness/epoch
        // bookkeeping, then stop — no append/apply on this response.
        if let Some(id) = snapshot_id {
            let active_id = self.snapshot_fetch.as_ref().map(|s| s.snapshot_id);
            if should_start_snapshot_fetch(id, self.log.log_end_offset(), active_id) {
                self.snapshot_fetch = Some(SnapshotFetchState::with_max(
                    id,
                    leader_id,
                    self.metadata_snapshot_fetch_max,
                ));
                self.send_fetch_snapshot(leader_id, id, 0);
            }
            self.on_event(Event::ReceiveFetchResponse {
                leader_id,
                leader_epoch,
                diverging,
            });
            return;
        }

        // `hwm` arrives raw on the KIP-595 Fetch response wire; wrap into the
        // log offset domain.
        //
        // Record the leader's watermark before the clamp below drops it to our
        // own log end: it is the quorum's committed offset, and the readiness
        // probe needs the gap between it and where this node has got to.
        self.leader_reported_hwm = self.leader_reported_hwm.max(hwm);
        let hwm = Offset(hwm);
        if let Some(point) = diverging {
            // Diverged: truncate to the leader's hint. The follower will
            // re-fetch from the truncation point on the next cycle. We still
            // feed the core event below so it processes the divergence too.
            // `point.offset` is the core's raw i64 divergence point.
            if let Err(e) = self.log.truncate_to(Offset(point.offset)) {
                tracing::error!(?e, "kraft: follower truncate failed");
            } else {
                self.restore_control_state_after_truncation(point.offset);
            }
        } else if !records.is_empty() {
            // Append the carried batches at their leader-assigned offsets. A
            // batch already present (base_offset < our log end) is skipped:
            // `append_at` requires the offset to equal our current log end.
            match decode_batches(&records) {
                Ok(batches) => {
                    for mut batch in batches {
                        // `base_offset` is a raw record-format field; wrap into
                        // the log offset domain for the append.
                        let at = Offset(batch.base_offset);
                        let log_end = self.log.log_end_offset();
                        match classify_fetch_batch(at, log_end) {
                            FetchBatchDisposition::AlreadyPresent => {
                                continue; // already have it
                            }
                            FetchBatchDisposition::Append => {}
                            FetchBatchDisposition::Gap => {
                                // Gap: we are missing earlier records. Stop; the next
                                // fetch (from our true log end) will refill in order.
                                break;
                            }
                        }
                        if let Err(e) = self.log.append_at(&mut batch, at) {
                            tracing::error!(?e, at = at.0, "kraft: follower append_at failed");
                            break;
                        }
                        if let Err(e) = self.apply_control_batch(&batch) {
                            tracing::error!(?e, at = at.0, "kraft: invalid fetched control batch");
                            break;
                        }
                        // Appended past the snapshot boundary: the log now has a
                        // real epoch of its own, so drop the post-install epoch
                        // override (see `send_fetch`).
                        self.installed_snapshot_epoch = None;
                    }
                }
                Err(e) => tracing::error!(?e, "kraft: follower decode batches failed"),
            }
            // Advance the HWM to the leader's, clamped to our log end, and apply
            // newly-committed records to the image.
            let target = hwm.min(self.log.log_end_offset());
            self.advance_and_apply(target);
        } else {
            // No records but the leader's HWM may have moved past what we already
            // have (e.g. the leader committed entries we already replicated).
            let target = hwm.min(self.log.log_end_offset());
            self.advance_and_apply(target);
        }

        // Feed the core so it re-arms its fetch timer / issues the next fetch.
        self.on_event(Event::ReceiveFetchResponse {
            leader_id,
            leader_epoch,
            diverging,
        });
    }

    /// (Follower side) handle a `FetchSnapshot` response chunk: reassemble via
    /// the [`SnapshotFetchState`], requesting the next range until complete, then
    /// install the assembled snapshot and resume normal fetching. Any error /
    /// abort falls back to a plain Fetch against the same peer.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, from = from.0))]
    pub fn on_fetch_snapshot_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::FetchSnapshot {
            snapshot_id,
            size,
            position,
            bytes,
            error_code,
        }) = wire::PeerResponse::decode_fetch_snapshot(body)
        else {
            return;
        };
        let Some(state) = self.snapshot_fetch.as_mut() else {
            return;
        };
        if snapshot_fetch_response_invalid(error_code, from, state.leader_id) {
            self.snapshot_fetch = None;
            self.send_fetch(from);
            return;
        }
        match state.on_chunk(snapshot_id, size, position, &bytes) {
            SnapshotFetchStep::Continue { next_position } => {
                self.send_fetch_snapshot(from, snapshot_id, next_position);
            }
            SnapshotFetchStep::Restart => {
                self.snapshot_fetch = None;
                self.send_fetch(from);
            }
            SnapshotFetchStep::Complete(assembled) => {
                let id = state.snapshot_id;
                self.snapshot_fetch = None;
                if let Err(e) = self.install_fetched_snapshot(id, &assembled) {
                    tracing::error!(?e, "kraft: snapshot install failed; will re-fetch");
                }
                self.send_fetch(from);
            }
        }
    }

    /// Validate, persist, and install a fetched snapshot: rebuild the image from
    /// its records, write the checkpoint, install it into the log (resetting the
    /// log-start/end to `end_offset`), publish the new image, and arm the
    /// post-install fetch epoch (see `send_fetch`).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, end_offset = id.0, snapshot_epoch = id.1, bytes = bytes.len()),
        err
    )]
    pub fn install_fetched_snapshot(
        &mut self,
        id: (i64, i32),
        bytes: &[u8],
    ) -> Result<(), RaftError> {
        if self.downgrade_snapshot_pending.is_some() {
            return Err(RaftError::ChangeRejected(
                "mandatory metadata downgrade snapshot is pending".into(),
            ));
        }
        // `end_offset` is the snapshot id's raw offset (wire / checkpoint-filename
        // boundary); wrap into the log offset domain where it addresses the log.
        let (end_offset, epoch) = id;
        let end_offset_pos = Offset(end_offset);
        // Validate the bytes decode before mutating any durable state.
        let contents = crate::snapshot::SnapshotReader::read(bytes)?;
        if end_offset_pos <= self.log.log_end_offset() {
            return Ok(()); // stale; we already advanced past this snapshot
        }
        let cluster_id = self.image.cluster_id();
        let mut new_image = MetadataImage::from_records(cluster_id, &contents.metadata_records);
        if let Some(control) = &contents.control_state {
            new_image.apply(&MetadataRecord::V1KRaftVersion(
                krabka_metadata::KRaftVersionRecord {
                    kraft_version: control.kraft_version,
                },
            ));
            new_image.apply(&MetadataRecord::V1Voters(VotersRecord {
                voters: control.voters.clone(),
            }));
        }
        write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset, epoch, bytes)?;
        self.image = new_image;
        if let Some(control) = contents.control_state {
            self.controls = KraftControlState::new(control.voters.clone(), control.kraft_version);
            self.core.set_kraft_version(control.kraft_version);
            let actions = self
                .core
                .apply_voter_set(control.voters.clone(), self.now());
            self.peers.update_voters(&control.voters);
            self.execute(actions);
        }
        self.log.install_snapshot(end_offset_pos)?;
        self.last_snapshot_end_offset = end_offset_pos;
        self.installed_snapshot_epoch = Some(u32::try_from(epoch).unwrap_or(0));
        let _ = self.image_tx.send(Arc::new(self.image.clone()));
        retain_latest_checkpoint(&checkpoint_dir(&self.data_dir));
        Ok(())
    }
}
