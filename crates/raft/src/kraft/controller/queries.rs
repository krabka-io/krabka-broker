//! The read-only views the engine serves: the durable quorum-state write, the
//! structured consensus snapshot behind `DescribeQuorum`, and the committed
//! `__cluster_metadata` slice an observer fetches.

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::ByteSize;

use super::{
    Engine,
    offsets::{fetch_batch_committed_before_hwm, metadata_fetch_offset_in_committed_window},
    quorum_state_file::save_quorum_state,
    records::encode_batches,
};
use crate::{
    error::RaftError,
    kraft::{
        core::QuorumStateMachine,
        role::Role,
        transport::{MetadataFetchSlice, QuorumStateSnapshot},
        types::NodeId,
    },
};

/// Floor on an observer's metadata-fetch budget: at least the first committed
/// batch is always emitted so a zero-budget fetch still makes progress.
const MIN_FETCH_BUDGET: ByteSize = krabka_units::bytes(1);

/// Voter ids from the core's current quorum state (for the initial published
/// snapshot, before the loop runs).
pub fn initial_state_voters(core: &QuorumStateMachine) -> Vec<NodeId> {
    core.quorum_state().voters.ids().into_iter().collect()
}

impl Engine {
    /// Persist the durable quorum state atomically.
    pub fn persist_quorum_state(&self) -> Result<(), RaftError> {
        save_quorum_state(&self.data_dir, self.core.quorum_state())
    }

    /// Highest offset the quorum has committed, as this node knows it: its own
    /// watermark on a leader, and the highest watermark a leader has reported
    /// on a follower, which is the larger of the two while the follower is
    /// still replaying.
    pub fn quorum_high_watermark(&self) -> i64 {
        self.log.hwm().0.max(self.leader_reported_hwm)
    }

    /// Snapshot the consensus state for `DescribeQuorum`.
    pub fn quorum_state_snapshot(&self) -> QuorumStateSnapshot {
        let qs = self.core.quorum_state();
        let mut per_replica_fetch_offset = self.replica_fetch_offsets.clone();
        if let Role::Leader { replicas, .. } = self.core.role() {
            // The leader's own matched index is its log end offset — its local
            // log is, by definition, fully matched against itself. The
            // `replicas` progress map tracks only *peers*, so the leader must
            // insert its own entry explicitly (otherwise a single-voter quorum
            // reports an empty matched-index map and `DescribeQuorum` returns
            // the JVM "unknown" sentinel -1 for the leader).
            // `per_voter_fetch_offset` is a wire-facing DescribeQuorum DTO of raw
            // `i64`s; the peer entries already come from the core as `i64`.
            per_replica_fetch_offset.insert(self.core.me(), self.log.log_end_offset().0);
            for (id, progress) in replicas {
                per_replica_fetch_offset.insert(*id, progress.fetch_offset);
            }
        }
        let observers = per_replica_fetch_offset
            .keys()
            .filter(|id| !qs.voters.contains(**id))
            .copied()
            .collect();
        QuorumStateSnapshot {
            leader_id: qs.leader_id,
            leader_epoch: qs.leader_epoch,
            high_watermark: self.log.hwm().0,
            quorum_high_watermark: self.quorum_high_watermark(),
            log_end_offset: self.log.log_end_offset().0,
            log_start_offset: self.log.log_start_offset().0,
            voters: qs.voters.clone(),
            voted_directory_id: qs.voted_key.as_ref().map(|key| key.directory_id),
            observers,
            per_replica_fetch_offset,
        }
    }

    /// Serve a committed `__cluster_metadata` slice for an observer's metadata
    /// fetch (1004): read committed batches at/after `fetch_offset` up to the
    /// HWM and concatenate their verbatim `RecordBatch` bytes (the engine's
    /// records are already Kafka record batches). At least the first batch is
    /// always emitted so the observer makes progress.
    pub fn metadata_fetch_slice(
        &self,
        fetch_offset: i64,
        max_size: ByteSize,
    ) -> MetadataFetchSlice {
        // `fetch_offset` arrives raw on the observer metadata-fetch wire; wrap it
        // into the `KraftLog` offset domain for the log-bound comparisons/read.
        let fetch_offset = Offset(fetch_offset);
        let high_watermark = self.log.hwm();
        let log_start_offset = self.log.log_start_offset();
        let records = if metadata_fetch_offset_in_committed_window(fetch_offset, high_watermark) {
            match self
                .log
                .read_decoded(fetch_offset, max_size.max(MIN_FETCH_BUDGET))
            {
                Ok(batches) => {
                    let committed: Vec<RecordBatch> = batches
                        .into_iter()
                        .filter(|b| fetch_batch_committed_before_hwm(b.base_offset, high_watermark))
                        .collect();
                    encode_batches(&committed)
                }
                Err(e) => {
                    tracing::error!(?e, "kraft: metadata fetch read failed");
                    bytes::Bytes::new()
                }
            }
        } else {
            bytes::Bytes::new()
        };
        MetadataFetchSlice {
            records,
            // `MetadataFetchSlice` is a wire-facing DTO of raw `i64` offsets.
            log_start_offset: log_start_offset.0,
            high_watermark: high_watermark.0,
            // Every controller serves this fetch, not only the leader, so the
            // observer is told the quorum's committed offset separately: a
            // follower still catching up serves records only up to its own
            // clamped watermark, and an observer that read that as the
            // quorum's would call itself caught up while both were far behind.
            quorum_high_watermark: self.quorum_high_watermark(),
        }
    }
}
