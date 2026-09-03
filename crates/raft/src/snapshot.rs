//! KIP-630 metadata snapshot artifact: `<offset>-<epoch>.checkpoint`.
//!
//! The format layer: image ⇄ record sequence, the `.checkpoint` filename
//! grammar, and the canonical on-disk bytes (header/data/footer Kafka
//! `RecordBatch`es). The engine ([`crate::kraft::KraftController`]) writes the
//! `.checkpoint` directly (no `.meta` sidecar) and recovers from it via
//! [`SnapshotReader::read`].
//!
//! This root holds the batch base offsets and the two decoded value types that
//! both directions share. Serialization lives in `writer`, decoding in
//! `reader`, and the `VotersRecord` translation both of them need in `voters`.

use krabka_metadata::{MetadataImage, MetadataRecord, VoterSet};

mod reader;
mod voters;
mod writer;

pub(crate) use self::{reader::SnapshotReader, writer::SnapshotWriter};

const SNAPSHOT_HEADER_BASE_OFFSET: i64 = 0;
const SNAPSHOT_KRAFT_VERSION_BASE_OFFSET: i64 = 1;
const SNAPSHOT_VOTERS_BASE_OFFSET: i64 = 2;
const SNAPSHOT_DATA_BASE_OFFSET: i64 = 3;

/// KIP-853 control state carried at the front of every metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotControlState {
    pub(crate) kraft_version: u16,
    pub(crate) voters: VoterSet,
}

impl SnapshotControlState {
    fn from_image(image: &MetadataImage) -> Self {
        Self {
            kraft_version: image.kraft_version(),
            voters: image.voters().clone(),
        }
    }
}

/// A decoded snapshot, with Raft control state kept separate from KIP-631
/// metadata records.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SnapshotContents {
    /// KIP-853 controls. Snapshots written before dynamic membership omit
    /// these batches and recover membership from the level-0 configuration.
    pub(crate) control_state: Option<SnapshotControlState>,
    /// The header's `last_contained_log_timestamp`: the create-time of the
    /// last batch this snapshot contains. A node that installs or recovers
    /// from the artifact carries it forward, because the record it names is
    /// below the boundary and no longer in any log to re-read.
    pub(crate) last_contained_log_timestamp: i64,
    pub(crate) metadata_records: Vec<MetadataRecord>,
}
