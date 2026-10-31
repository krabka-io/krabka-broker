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

use {
    krabka_metadata::{KRaftVersionRecord, MetadataImage, MetadataRecord, VoterSet, VotersRecord},
    uuid::Uuid,
};

mod reader;
mod voters;
mod writer;

pub use self::{reader::SnapshotReader, writer::SnapshotWriter};

const SNAPSHOT_HEADER_BASE_OFFSET: i64 = 0;
const SNAPSHOT_KRAFT_VERSION_BASE_OFFSET: i64 = 1;
const SNAPSHOT_VOTERS_BASE_OFFSET: i64 = 2;
const SNAPSHOT_DATA_BASE_OFFSET: i64 = 3;

/// KIP-853 control state carried at the front of every metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotControlState {
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

    /// The controls as image records: the `V1KRaftVersion` and then the
    /// `V1Voters`, the order that every mirror into an image applies them in.
    pub(crate) fn into_records(self) -> [MetadataRecord; 2] {
        [
            MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
                kraft_version: self.kraft_version,
            }),
            MetadataRecord::V1Voters(VotersRecord {
                voters: self.voters,
            }),
        ]
    }
}

/// Mirror the KIP-853 controls into `image`, so its readers observe the live
/// quorum membership. The controls are raft control records with no KIP-631
/// counterpart, so the metadata records alone never set them.
pub(crate) fn mirror_controls(image: &mut MetadataImage, kraft_version: u16, voters: VoterSet) {
    let controls = SnapshotControlState {
        kraft_version,
        voters,
    };
    for record in controls.into_records() {
        image.apply(&record);
    }
}

/// A decoded snapshot, with Raft control state kept separate from KIP-631
/// metadata records.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotContents {
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

impl SnapshotContents {
    /// The image this snapshot holds: its metadata records, and then its
    /// controls when it has them.
    pub(crate) fn to_image(&self, cluster_id: Uuid) -> MetadataImage {
        let mut image = MetadataImage::from_records(cluster_id, &self.metadata_records);
        if let Some(control) = &self.control_state {
            mirror_controls(&mut image, control.kraft_version, control.voters.clone());
        }
        image
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        krabka_metadata::{NodeId, TopicRecord, Voter, VoterEndpoint, voters::KRaftVersionRange},
    };

    fn voters() -> VoterSet {
        VoterSet::from_voters([Voter {
            id: NodeId(1),
            directory_id: Uuid::from_u128(1),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9_093,
            }],
            kraft_version: KRaftVersionRange { min: 0, max: 1 },
        }])
    }

    fn topic() -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::from_u128(7),
            partitions: 0,
            replication_factor: 1,
        })
    }

    /// A snapshot's image is its metadata records followed by the version
    /// and then the voters of its controls, and only the records without
    /// controls.
    #[yare::parameterized(
        with_controls = { Some(SnapshotControlState { kraft_version: 1, voters: voters() }) },
        without_controls = { None },
    )]
    fn to_image_applies_records_then_controls(control_state: Option<SnapshotControlState>) {
        let cluster_id = Uuid::from_u128(42);
        let contents = SnapshotContents {
            control_state: control_state.clone(),
            last_contained_log_timestamp: 0,
            metadata_records: vec![topic()],
        };
        let mut expected = MetadataImage::from_records(cluster_id, &[topic()]);
        if let Some(control) = control_state {
            expected.apply(&MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
                kraft_version: control.kraft_version,
            }));
            expected.apply(&MetadataRecord::V1Voters(VotersRecord {
                voters: control.voters,
            }));
        }
        assert2::assert!(contents.to_image(cluster_id) == expected);
    }
}
