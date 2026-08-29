//! The [`RemoteStorageManager`] SPI: copy / fetch / delete of segment data
//! and indexes to and from the remote tier.

use std::path::PathBuf;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use uuid::Uuid;

use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
};

/// The kinds of index a segment carries alongside its `.log` data.
///
/// Mirrors Kafka's `RemoteStorageManager.IndexType`. A
/// [`RemoteStorageManager`] copies all of these on
/// [`RemoteStorageManager::copy_log_segment_data`] and serves any of them
/// back on [`RemoteStorageManager::fetch_index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// Sparse offset → byte-position index (`.index`).
    Offset,
    /// Sparse timestamp → relative-offset index (`.timeindex`).
    Timestamp,
    /// Producer id snapshot (`.snapshot`).
    ProducerSnapshot,
    /// Leader-epoch checkpoint (`.leader_epoch_checkpoint` in Kafka's
    /// `LocalTieredStorage`).
    LeaderEpoch,
    /// Aborted-transaction index (`.txnindex`). It is optional. A segment
    /// with no aborted transactions has none.
    Transaction,
}

impl IndexType {
    /// The Kafka `LocalTieredStorage` filename suffix for this index type.
    /// Its remote leader-epoch artifact uses `.leader_epoch_checkpoint`,
    /// distinct from a partition log's local `leader-epoch-checkpoint` file.
    #[must_use]
    pub fn suffix(self) -> &'static str {
        match self {
            IndexType::Offset => ".index",
            IndexType::Timestamp => ".timeindex",
            IndexType::ProducerSnapshot => ".snapshot",
            IndexType::LeaderEpoch => ".leader_epoch_checkpoint",
            IndexType::Transaction => ".txnindex",
        }
    }

    /// The index type that a filename suffix names.
    ///
    /// It is the inverse of [`IndexType::suffix`]. A reader that discovers an
    /// archive from the object store alone identifies each artifact by the
    /// suffix of its key, and every key that is not an index ends in
    /// [`LOG_FILE_SUFFIX`], so `None` means "not an index".
    #[must_use]
    pub fn from_suffix(suffix: &str) -> Option<Self> {
        match suffix {
            ".index" => Some(IndexType::Offset),
            ".timeindex" => Some(IndexType::Timestamp),
            ".snapshot" => Some(IndexType::ProducerSnapshot),
            ".leader_epoch_checkpoint" => Some(IndexType::LeaderEpoch),
            ".txnindex" => Some(IndexType::Transaction),
            _ => None,
        }
    }
}

/// Filename suffix of a segment's data, as distinct from one of its indexes.
pub const LOG_FILE_SUFFIX: &str = ".log";

/// Character count of a Kafka Base64-rendered UUID.
///
/// The 16 raw bytes encode to 22 URL-safe unpadded Base64 characters. The
/// width is fixed, which is what lets [`parse_partition_dir_name`] find the
/// topic id in a name whose topic component may itself contain `-`.
const KAFKA_UUID_LEN: usize = 22;

/// Character count of the zero-padded base offset that starts a segment
/// filename. `i64::MAX` is 19 digits, so the field never overflows its width.
const BASE_OFFSET_LEN: usize = 20;

/// Kafka renders UUIDs in URL-safe, unpadded Base64 for remote-tier paths.
#[must_use]
pub fn kafka_uuid(uuid: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(uuid.as_bytes())
}

/// Reads back a UUID that [`kafka_uuid`] rendered.
///
/// Returns `None` unless the input is URL-safe unpadded Base64 that decodes to
/// exactly the 16 bytes of a UUID. The engine rejects a non-canonical encoding,
/// so one UUID has one accepted spelling and the round trip is exact.
#[must_use]
pub fn decode_kafka_uuid(encoded: &str) -> Option<Uuid> {
    let bytes: [u8; 16] = URL_SAFE_NO_PAD.decode(encoded).ok()?.try_into().ok()?;
    Some(Uuid::from_bytes(bytes))
}

/// Directory name used by Kafka's `LocalTieredStorage` for a partition.
#[must_use]
pub fn partition_dir_name(metadata: &RemoteLogSegmentMetadata) -> String {
    let tp = &metadata.remote_log_segment_id().topic_id_partition;
    format!("{}-{}-{}", tp.topic, tp.partition, kafka_uuid(tp.topic_id))
}

/// The partition that a remote-tier directory name identifies.
///
/// A reader that discovers an archive from object storage alone has nothing but
/// the keys, so the directory component of a key is where the partition comes
/// from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionDirName {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Stable topic UUID, as assigned at topic creation.
    pub topic_id: Uuid,
}

/// Reads back the partition that [`partition_dir_name`] rendered.
///
/// The name is parsed from the right, because a topic name may contain `-`
/// while neither the partition index nor the fixed-width topic id can. Returns
/// `None` when a component is absent or malformed, which includes a topic that
/// is empty or that holds the `/` key separator.
#[must_use]
pub fn parse_partition_dir_name(name: &str) -> Option<PartitionDirName> {
    let (head, topic_id) = name.split_at_checked(name.len().checked_sub(KAFKA_UUID_LEN)?)?;
    let topic_id = decode_kafka_uuid(topic_id)?;
    let (topic, partition) = head.strip_suffix('-')?.rsplit_once('-')?;
    if topic.is_empty() || topic.contains('/') || !partition.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(PartitionDirName {
        topic: topic.to_owned(),
        partition: partition.parse().ok()?,
        topic_id,
    })
}

/// Filename used by Kafka's `LocalTieredStorage` for one segment artifact.
#[must_use]
pub fn segment_file_name(metadata: &RemoteLogSegmentMetadata, suffix: &str) -> String {
    format!(
        "{:020}-{}{}",
        metadata.start_offset(),
        kafka_uuid(metadata.remote_log_segment_id().id),
        suffix
    )
}

/// The segment artifact that a remote-tier filename identifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentFileName<'a> {
    /// First offset the segment holds.
    pub base_offset: i64,
    /// Random per-segment UUID.
    pub segment_id: Uuid,
    /// Which artifact of the segment this is: [`LOG_FILE_SUFFIX`] for the data,
    /// or an [`IndexType::suffix`] for one of the indexes.
    pub suffix: &'a str,
}

/// Reads back the segment artifact that [`segment_file_name`] rendered.
///
/// Both leading components have a fixed width, so the name parses from the
/// left even though the Base64 alphabet contains the `-` separator. Returns
/// `None` when a component is absent or malformed.
#[must_use]
pub fn parse_segment_file_name(name: &str) -> Option<SegmentFileName<'_>> {
    let (base_offset, rest) = name.split_at_checked(BASE_OFFSET_LEN)?;
    if !base_offset.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (segment_id, suffix) = rest.strip_prefix('-')?.split_at_checked(KAFKA_UUID_LEN)?;
    Some(SegmentFileName {
        base_offset: base_offset.parse().ok()?,
        segment_id: decode_kafka_uuid(segment_id)?,
        suffix,
    })
}

/// The local files, and the in-memory leader-epoch bytes, that make up one
/// log segment for copy to the remote tier.
///
/// Mirrors Kafka's `LogSegmentData`. `transaction_index` is optional; a
/// segment with no aborted transactions has no `.txnindex` file.
/// `producer_snapshot_index` is optional too so third-party callers can copy
/// legacy segments that predate snapshots. krabka log exports provide one.
/// The broker passes the leader-epoch index as bytes and not as a path,
/// because it holds the relevant slice in memory at copy time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSegmentData {
    /// Path to the `.log` data file.
    pub log_segment: PathBuf,
    /// Path to the `.index` (offset index) file.
    pub offset_index: PathBuf,
    /// Path to the `.timeindex` file.
    pub time_index: PathBuf,
    /// Path to the `.txnindex` file, when present.
    pub transaction_index: Option<PathBuf>,
    /// Path to the producer-id `.snapshot` file, when present. Older segment
    /// sources can omit it; krabka log exports always provide one.
    pub producer_snapshot_index: Option<PathBuf>,
    /// Serialized leader-epoch index bytes for this segment's offset range.
    pub leader_epoch_index: Bytes,
}

/// SPI for the remote object store that holds offloaded segment data.
///
/// Implementations are **synchronous and blocking**. They mirror Kafka's
/// `RemoteStorageManager`, which the broker drives from a dedicated thread
/// pool, and the broker wraps these calls in `spawn_blocking`.
/// Implementations must be `Send + Sync` so the broker can share one instance
/// across tasks.
pub trait RemoteStorageManager: Send + Sync {
    /// Copies a segment's data and all of its indexes to the remote tier.
    ///
    /// Returns optional [`CustomMetadata`], for example an object-store key
    /// or a version id. The broker records it on the segment and passes it
    /// back on every later fetch or delete for that segment.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] if any underlying store operation
    /// fails.
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError>;

    /// Fetches a byte range of a segment's `.log` data.
    ///
    /// `start_position` is the inclusive starting byte offset within the
    /// segment. `end_position`, when `Some`, is the inclusive last byte
    /// offset; when `None`, the read runs to the end of the segment.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment is
    /// not present, or [`RemoteStorageError::Io`] on a store failure.
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Fetches one of a segment's indexes in full.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment or the
    /// requested index is not present, or [`RemoteStorageError::Io`] on a
    /// store failure.
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Deletes a segment's data and all of its indexes from the remote tier.
    ///
    /// Implementations must be idempotent: a delete of an absent segment
    /// succeeds — or return an error, when the backend is an immutable
    /// archive.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::Io`] on a store failure.
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use krabka_ids::LeaderEpoch;
    use proptest::prelude::*;

    use super::*;
    use crate::metadata::{
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState, TopicIdPartition,
    };

    fn metadata(
        topic: &str,
        partition: i32,
        topic_id: Uuid,
        segment_id: Uuid,
        base_offset: i64,
    ) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(topic_id, topic, partition),
                segment_id,
            ),
            base_offset,
            base_offset,
            0,
            1,
            0,
            RemoteLogSegmentDetails::new(
                1,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), base_offset)]),
            ),
        )
        .unwrap()
    }

    fn sample() -> RemoteLogSegmentMetadata {
        metadata("orders", 7, Uuid::from_u128(1), Uuid::from_u128(0xfe), 11)
    }

    #[test]
    fn index_suffixes_match_kafka() {
        // Filesystem-backed stores key files off these exact suffixes.
        for (index_type, want) in [
            (IndexType::Offset, ".index"),
            (IndexType::Timestamp, ".timeindex"),
            (IndexType::ProducerSnapshot, ".snapshot"),
            (IndexType::LeaderEpoch, ".leader_epoch_checkpoint"),
            (IndexType::Transaction, ".txnindex"),
        ] {
            check!(index_type.suffix() == want, "{index_type:?}");
            check!(IndexType::from_suffix(want) == Some(index_type));
        }
    }

    #[test]
    fn from_suffix_rejects_non_index_suffixes() {
        for suffix in [LOG_FILE_SUFFIX, "", ".INDEX", "index", ".timeindex2"] {
            check!(IndexType::from_suffix(suffix) == None, "{suffix:?}");
        }
    }

    #[test]
    fn local_tiered_storage_names_match_kafka() {
        let metadata = sample();

        check!(partition_dir_name(&metadata) == "orders-7-AAAAAAAAAAAAAAAAAAAAAQ");
        check!(
            segment_file_name(&metadata, IndexType::ProducerSnapshot.suffix())
                == "00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot"
        );
    }

    #[test]
    fn object_store_key_components_match_kafka() {
        // The two halves of the S3 key that the object-store backend writes.
        let metadata = metadata("orders", 0, Uuid::from_u128(1), Uuid::from_u128(30), 0);

        check!(partition_dir_name(&metadata) == "orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        check!(
            segment_file_name(&metadata, LOG_FILE_SUFFIX)
                == "00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log"
        );
    }

    #[test]
    fn parses_the_pinned_kafka_names() {
        check!(
            parse_partition_dir_name("orders-7-AAAAAAAAAAAAAAAAAAAAAQ")
                == Some(PartitionDirName {
                    topic: "orders".to_owned(),
                    partition: 7,
                    topic_id: Uuid::from_u128(1),
                })
        );
        check!(
            parse_segment_file_name("00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot")
                == Some(SegmentFileName {
                    base_offset: 11,
                    segment_id: Uuid::from_u128(0xfe),
                    suffix: ".snapshot",
                })
        );
        check!(
            parse_segment_file_name("00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log")
                == Some(SegmentFileName {
                    base_offset: 0,
                    segment_id: Uuid::from_u128(30),
                    suffix: ".log",
                })
        );
    }

    #[test]
    fn parse_partition_dir_name_splits_a_hyphenated_topic_from_the_right() {
        // "orders-7" is a topic name, not a topic and a partition index.
        check!(
            parse_partition_dir_name("orders-7-3-AAAAAAAAAAAAAAAAAAAAAQ")
                == Some(PartitionDirName {
                    topic: "orders-7".to_owned(),
                    partition: 3,
                    topic_id: Uuid::from_u128(1),
                })
        );
        // A partition index is never negative, so the leading `-` of "-1"
        // belongs to the topic name and the index is 1.
        check!(
            parse_partition_dir_name("orders--1-AAAAAAAAAAAAAAAAAAAAAQ")
                == Some(PartitionDirName {
                    topic: "orders-".to_owned(),
                    partition: 1,
                    topic_id: Uuid::from_u128(1),
                })
        );
    }

    #[test]
    fn parse_partition_dir_name_rejects_malformed_names() {
        for name in [
            // Topic id one character short of the fixed Base64 width.
            "orders-7-AAAAAAAAAAAAAAAAAAAAA",
            // Topic id that decodes to more than 16 bytes.
            "orders-7-AAAAAAAAAAAAAAAAAAAAAQAA",
            // Non-numeric partition index.
            "orders-x-AAAAAAAAAAAAAAAAAAAAAQ",
            // Empty topic.
            "-7-AAAAAAAAAAAAAAAAAAAAAQ",
            // A key separator cannot appear inside one directory name.
            "or/ders-7-AAAAAAAAAAAAAAAAAAAAAQ",
            // No partition index at all.
            "orders-AAAAAAAAAAAAAAAAAAAAAQ",
            "",
        ] {
            check!(parse_partition_dir_name(name) == None, "{name:?}");
        }
    }

    #[test]
    fn parse_segment_file_name_rejects_malformed_names() {
        for name in [
            // Base offset one digit short of the fixed width.
            "0000000000000000001-AAAAAAAAAAAAAAAAAAAA_g.snapshot",
            // Non-numeric base offset.
            "0000000000000000001x-AAAAAAAAAAAAAAAAAAAA_g.snapshot",
            // Segment id one character short of the fixed Base64 width.
            "00000000000000000011-AAAAAAAAAAAAAAAAAAAA_",
            // Missing the separator between base offset and segment id.
            "00000000000000000011xAAAAAAAAAAAAAAAAAAAA_g.snapshot",
            // Segment id that is not Base64 at all.
            "00000000000000000011-**********************.snapshot",
            "",
        ] {
            check!(parse_segment_file_name(name) == None, "{name:?}");
        }
    }

    #[test]
    fn decode_kafka_uuid_rejects_wrong_length_base64() {
        for encoded in ["", "AAAA", "AAAAAAAAAAAAAAAAAAAAAQAA", "A"] {
            check!(decode_kafka_uuid(encoded) == None, "{encoded:?}");
        }
    }

    proptest! {
        #[test]
        fn partition_dir_name_round_trips(
            topic in "[a-z0-9]{1,6}(-[a-z0-9]{1,6}){0,3}",
            partition in 0_i32..=i32::MAX,
            topic_id in any::<u128>(),
        ) {
            let topic_id = Uuid::from_u128(topic_id);
            let metadata = metadata(&topic, partition, topic_id, Uuid::nil(), 0);
            let name = partition_dir_name(&metadata);
            prop_assert!(
                parse_partition_dir_name(&name)
                    == Some(PartitionDirName { topic, partition, topic_id }),
                "{name}"
            );
        }

        #[test]
        fn segment_file_name_round_trips(
            base_offset in 0_i64..=i64::MAX,
            segment_id in any::<u128>(),
            index_type in prop_oneof![
                Just(None),
                Just(Some(IndexType::Offset)),
                Just(Some(IndexType::Timestamp)),
                Just(Some(IndexType::ProducerSnapshot)),
                Just(Some(IndexType::LeaderEpoch)),
                Just(Some(IndexType::Transaction)),
            ],
        ) {
            let segment_id = Uuid::from_u128(segment_id);
            let suffix = index_type.map_or(LOG_FILE_SUFFIX, IndexType::suffix);
            let metadata = metadata("orders", 0, Uuid::nil(), segment_id, base_offset);
            let name = segment_file_name(&metadata, suffix);
            let parsed = parse_segment_file_name(&name);
            prop_assert!(
                parsed == Some(SegmentFileName { base_offset, segment_id, suffix }),
                "{name}"
            );
            prop_assert!(
                parsed.and_then(|p| IndexType::from_suffix(p.suffix)) == index_type,
                "{name}"
            );
        }
    }
}
