//! The archive inventory: what the archive holds, before anything is read.
//!
//! This module owns the scan. It lists the archive under the operator's
//! prefix, decodes every key with the KIP-405 archive key codec
//! (`parse_partition_dir_name` and `parse_segment_file_name`), and groups
//! the result into one [`SegmentInventory`] per archived segment, with the
//! object key and size of each artifact that is present. It keeps every key it
//! cannot attribute rather than discarding it, because an unrecognized key is
//! the first sign that `--archive-prefix` points at the wrong tree. When
//! `--rlmm-snapshot` is given, this module reconciles the scan against it and
//! reports a disagreement instead of choosing a winner; without the snapshot a
//! segment the old cluster had marked for deletion looks exactly like a live
//! one. This module decides nothing about completeness or integrity. It
//! reports presence, and the verify stage judges it.

use std::collections::{HashMap, HashSet};

use krabka_ids::Offset;
use krabka_remote_storage::{
    IndexType, LOG_FILE_SUFFIX, PartitionDump, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    TopicIdPartition, parse_partition_dir_name, parse_segment_file_name,
};
use krabka_remote_storage_topic::Snapshot;
use object_store::path::Path;
use uuid::Uuid;

use crate::{args::RestoreArgs, backend::ArchiveStore, error::RestoreError};

/// One artifact of one archived segment, as the scan found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveObject {
    /// The full object key, prefix included.
    pub key: Path,
    /// The object size the store reports. It stays the store's own `u64`,
    /// because it is compared against byte positions inside the object.
    pub size: u64,
}

/// One archived segment and the artifacts the archive holds for it.
///
/// Kafka copies the log, the offset index, the time index, the producer
/// snapshot, and the leader-epoch checkpoint together. The transaction index
/// is absent for a segment with no aborted transaction, so its absence is not
/// by itself a torn copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInventory {
    /// The per-copy segment id the archive names the artifacts with.
    pub segment_id: Uuid,
    /// First offset the segment holds, as the key encodes it.
    pub base_offset: Offset,
    /// The segment data.
    pub log: Option<ArchiveObject>,
    /// The sparse offset index.
    pub offset_index: Option<ArchiveObject>,
    /// The sparse time index.
    pub time_index: Option<ArchiveObject>,
    /// The producer id snapshot.
    pub producer_snapshot: Option<ArchiveObject>,
    /// The leader-epoch checkpoint.
    pub leader_epoch: Option<ArchiveObject>,
    /// The aborted-transaction index. It is optional.
    pub transaction_index: Option<ArchiveObject>,
}

/// One partition of the archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInventory {
    /// The partition the archive directory names.
    pub partition: TopicIdPartition,
    /// Every segment found for the partition, in base-offset order.
    pub segments: Vec<SegmentInventory>,
}

/// Everything the scan found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveInventory {
    /// The selected partitions, ordered by topic then partition index.
    pub partitions: Vec<PartitionInventory>,
    /// Keys the codec could not attribute to a segment.
    pub unrecognized: Vec<Path>,
}

/// One artifact of a segment, as [`inventory`] is still accumulating it.
///
/// Distinguishes the segment's `.log` data from its indexes so the artifact a
/// key names can be assigned to the right [`SegmentInventory`] field without
/// an intermediate `unrecognized` entry for a key whose suffix is simply not
/// yet known at the point the accumulator slot is chosen.
#[derive(Debug, Clone, Copy)]
enum Artifact {
    /// The segment's `.log` data.
    Log,
    /// One of the segment's indexes.
    Index(IndexType),
}

/// Segments accumulated so far for one partition, keyed by segment id so an
/// artifact found later for a segment already seen joins the same entry.
#[derive(Debug, Default)]
struct PartitionAccum {
    /// The topic name, as the partition directory names it.
    topic: String,
    /// Segments seen so far, keyed by segment id.
    segments: HashMap<Uuid, SegmentInventory>,
}

/// A [`SegmentInventory`] with every artifact absent, the accumulator's
/// starting point for a segment id seen for the first time.
fn empty_segment(segment_id: Uuid, base_offset: Offset) -> SegmentInventory {
    SegmentInventory {
        segment_id,
        base_offset,
        log: None,
        offset_index: None,
        time_index: None,
        producer_snapshot: None,
        leader_epoch: None,
        transaction_index: None,
    }
}

/// Scan `store` and build the inventory of the partitions `args` selects.
///
/// # Errors
///
/// Returns [`RestoreError::ObjectStore`] when the listing fails,
/// [`RestoreError::EmptyArchive`] when no selected topic has a segment, and
/// [`RestoreError::MetadataDisagreement`] when `--rlmm-snapshot` contradicts
/// the scan.
pub async fn inventory(
    store: &ArchiveStore,
    args: &RestoreArgs,
) -> Result<ArchiveInventory, RestoreError> {
    let listed = store.ops().list(store.root()).await?;
    let root = store.root().unwrap_or(Path::ROOT);

    let mut unrecognized = Vec::new();
    let mut accum: HashMap<(Uuid, i32), PartitionAccum> = HashMap::new();

    for object in listed {
        // Collected to owned `String`s immediately, so the parsed pieces
        // below never borrow `object.location` and it stays free to move
        // into `unrecognized` or clone into an `ArchiveObject` right after.
        let Some(relative) = object.location.prefix_match(&root) else {
            unrecognized.push(object.location);
            continue;
        };
        let relative: Vec<String> = relative.map(|part| part.as_ref().to_owned()).collect();
        let [partition_dir, segment_file] = relative.as_slice() else {
            unrecognized.push(object.location);
            continue;
        };

        let Some(dir) = parse_partition_dir_name(partition_dir) else {
            unrecognized.push(object.location);
            continue;
        };
        let Some(file) = parse_segment_file_name(segment_file) else {
            unrecognized.push(object.location);
            continue;
        };

        if !args.selects_topic(&dir.topic) {
            unrecognized.push(object.location);
            continue;
        }

        let artifact = if file.suffix == LOG_FILE_SUFFIX {
            Artifact::Log
        } else if let Some(index_type) = IndexType::from_suffix(file.suffix) {
            Artifact::Index(index_type)
        } else {
            unrecognized.push(object.location);
            continue;
        };

        let archive_object = ArchiveObject {
            key: object.location.clone(),
            size: object.size,
        };

        let partition_accum = accum
            .entry((dir.topic_id, dir.partition))
            .or_insert_with(|| PartitionAccum {
                topic: dir.topic.clone(),
                segments: HashMap::new(),
            });
        let segment_accum = partition_accum
            .segments
            .entry(file.segment_id)
            .or_insert_with(|| empty_segment(file.segment_id, Offset(file.base_offset)));

        match artifact {
            Artifact::Log => segment_accum.log = Some(archive_object),
            Artifact::Index(IndexType::Offset) => segment_accum.offset_index = Some(archive_object),
            Artifact::Index(IndexType::Timestamp) => {
                segment_accum.time_index = Some(archive_object);
            }
            Artifact::Index(IndexType::ProducerSnapshot) => {
                segment_accum.producer_snapshot = Some(archive_object);
            }
            Artifact::Index(IndexType::LeaderEpoch) => {
                segment_accum.leader_epoch = Some(archive_object);
            }
            Artifact::Index(IndexType::Transaction) => {
                segment_accum.transaction_index = Some(archive_object);
            }
        }
    }

    let mut partitions: Vec<PartitionInventory> = accum
        .into_iter()
        .map(|((topic_id, partition), partition_accum)| {
            let mut segments: Vec<SegmentInventory> =
                partition_accum.segments.into_values().collect();
            segments.sort_by_key(|segment| segment.base_offset);
            PartitionInventory {
                partition: TopicIdPartition::new(topic_id, partition_accum.topic, partition),
                segments,
            }
        })
        .collect();
    partitions.sort_by(|a, b| {
        (a.partition.topic.as_str(), a.partition.partition)
            .cmp(&(b.partition.topic.as_str(), b.partition.partition))
    });

    if let Some(snapshot_path) = &args.archive.rlmm_snapshot {
        reconcile_with_snapshot(&mut partitions, args, snapshot_path)?;
    }

    if partitions.is_empty() {
        return Err(RestoreError::EmptyArchive {
            prefix: store.prefix().unwrap_or("").to_owned(),
        });
    }

    Ok(ArchiveInventory {
        partitions,
        unrecognized,
    })
}

/// Load the `--rlmm-snapshot` file, mapping an absent or corrupt file onto
/// [`RestoreError::Io`]: the restore crate defines no dedicated snapshot-error
/// variant, and an operator who passed the flag expects the file to be there
/// and to be readable.
fn load_snapshot(path: &std::path::Path) -> Result<Snapshot, RestoreError> {
    match Snapshot::load(path) {
        Ok(Some(snapshot)) => Ok(snapshot),
        Ok(None) => Err(RestoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("--rlmm-snapshot {} does not exist", path.display()),
        ))),
        Err(error) => Err(RestoreError::Io(std::io::Error::other(format!(
            "--rlmm-snapshot {}: {error}",
            path.display()
        )))),
    }
}

/// Reconcile the bucket scan in `partitions` against the RLMM snapshot at
/// `path`, dropping segments the snapshot has marked deleted and failing on a
/// genuine disagreement between the two sources.
///
/// A segment the snapshot names as [`RemoteLogSegmentState::DeleteSegmentStarted`]
/// is dropped silently: deletion is in flight, and the remote tier has not
/// necessarily caught up, so leftover bytes are expected. A segment the
/// snapshot names as [`RemoteLogSegmentState::DeleteSegmentFinished`] is the
/// opposite case: the metadata says the bytes are gone, so bytes still being
/// in the archive is a genuine disagreement worth stopping for, not a routine
/// lag. The same holds for a segment the scan found that the snapshot does
/// not mention at all, and for a segment the snapshot names as live that the
/// scan did not find.
///
/// # Errors
///
/// Returns [`RestoreError::Io`] when the snapshot file is absent or corrupt,
/// and [`RestoreError::MetadataDisagreement`] when a partition's scanned
/// segments and its snapshot entry disagree about what is live.
fn reconcile_with_snapshot(
    partitions: &mut Vec<PartitionInventory>,
    args: &RestoreArgs,
    path: &std::path::Path,
) -> Result<(), RestoreError> {
    let snapshot = load_snapshot(path)?;

    let mut dumps: HashMap<(Uuid, i32), &PartitionDump> = HashMap::new();
    for dump in &snapshot.dump.partitions {
        if args.selects_topic(&dump.topic_id_partition.topic) {
            dumps.insert(
                (
                    dump.topic_id_partition.topic_id,
                    dump.topic_id_partition.partition,
                ),
                dump,
            );
        }
    }

    let mut scanned_keys: HashSet<(Uuid, i32)> = HashSet::new();
    for partition in partitions.iter_mut() {
        let key = (partition.partition.topic_id, partition.partition.partition);
        scanned_keys.insert(key);
        reconcile_partition(partition, dumps.get(&key).copied())?;
    }

    // A partition the snapshot names with live segments, but the scan found
    // nothing for at all, is also a disagreement: the scan loop above never
    // visits it, because it never became a `PartitionInventory`.
    for (key, dump) in &dumps {
        if scanned_keys.contains(key) {
            continue;
        }
        if dump.segments.iter().any(|segment| is_live(segment.state())) {
            return Err(RestoreError::MetadataDisagreement {
                topic: dump.topic_id_partition.topic.clone(),
                partition: dump.topic_id_partition.partition,
                scanned: "0 segments, bases []".to_owned(),
                snapshot: summarize_dump(Some(dump)),
            });
        }
    }

    partitions.retain(|partition| !partition.segments.is_empty());
    Ok(())
}

/// `true` for a segment state the snapshot considers present in the remote
/// tier: a copy in flight or finished, as opposed to a deletion in flight or
/// finished.
fn is_live(state: RemoteLogSegmentState) -> bool {
    matches!(
        state,
        RemoteLogSegmentState::CopySegmentStarted | RemoteLogSegmentState::CopySegmentFinished
    )
}

/// Reconcile one partition's scanned segments against its snapshot entry, if
/// any, then drop the segments the snapshot says are mid-deletion.
///
/// # Errors
///
/// Returns [`RestoreError::MetadataDisagreement`] under the conditions
/// [`reconcile_with_snapshot`] documents.
fn reconcile_partition(
    partition: &mut PartitionInventory,
    dump: Option<&PartitionDump>,
) -> Result<(), RestoreError> {
    let by_key: HashMap<(Uuid, i64), RemoteLogSegmentState> = dump
        .map(|dump| {
            dump.segments
                .iter()
                .map(|segment| {
                    (
                        (segment.remote_log_segment_id().id, segment.start_offset()),
                        segment.state(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let scan_disagrees = partition.segments.iter().any(|segment| {
        matches!(
            by_key.get(&(segment.segment_id, segment.base_offset.get())),
            None | Some(RemoteLogSegmentState::DeleteSegmentFinished)
        )
    });
    let snapshot_disagrees =
        dump.into_iter()
            .flat_map(|dump| &dump.segments)
            .any(|snapshot_segment| {
                is_live(snapshot_segment.state())
                    && !partition.segments.iter().any(|scanned| {
                        scanned.segment_id == snapshot_segment.remote_log_segment_id().id
                            && scanned.base_offset.get() == snapshot_segment.start_offset()
                    })
            });

    if scan_disagrees || snapshot_disagrees {
        return Err(RestoreError::MetadataDisagreement {
            topic: partition.partition.topic.clone(),
            partition: partition.partition.partition,
            scanned: summarize_scan(&partition.segments),
            snapshot: summarize_dump(dump),
        });
    }

    partition.segments.retain(|segment| {
        !matches!(
            by_key.get(&(segment.segment_id, segment.base_offset.get())),
            Some(RemoteLogSegmentState::DeleteSegmentStarted)
        )
    });
    Ok(())
}

/// One-line summary of what the bucket scan found for a partition, for a
/// [`RestoreError::MetadataDisagreement`] message.
fn summarize_scan(segments: &[SegmentInventory]) -> String {
    let bases: Vec<i64> = segments.iter().map(|s| s.base_offset.get()).collect();
    summarize(bases.len(), &bases)
}

/// One-line summary of what the RLMM snapshot states for a partition, for a
/// [`RestoreError::MetadataDisagreement`] message.
fn summarize_dump(dump: Option<&PartitionDump>) -> String {
    let mut bases: Vec<i64> = dump
        .map(|dump| {
            dump.segments
                .iter()
                .map(RemoteLogSegmentMetadata::start_offset)
                .collect()
        })
        .unwrap_or_default();
    bases.sort_unstable();
    summarize(bases.len(), &bases)
}

/// Render `"N segment(s), bases [...]"`, the shared shape of both summary
/// halves of a [`RestoreError::MetadataDisagreement`] message.
fn summarize(count: usize, bases: &[i64]) -> String {
    let plural = if count == 1 { "" } else { "s" };
    format!("{count} segment{plural}, bases {bases:?}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use clap::Parser as _;
    use krabka_ids::LeaderEpoch;
    use krabka_remote_storage::{
        RemoteLogSegmentDetails, RemoteLogSegmentId, RlmmCacheDump, kafka_uuid,
    };

    use super::*;
    use crate::backend::open_archive;

    /// The five artifacts every complete segment copy carries. The
    /// transaction index is excluded on purpose: it is optional even for a
    /// copy discovery has no reason to call torn.
    const FULL_SEGMENT_SUFFIXES: [&str; 5] = [
        ".log",
        ".index",
        ".timeindex",
        ".snapshot",
        ".leader_epoch_checkpoint",
    ];

    /// Bytes every fixture artifact is written with, so every
    /// [`ArchiveObject::size`] in an expected structure is this length.
    const STUB_BYTES: &[u8] = b"stub";

    fn args_from(archive_dir: &std::path::Path, extra: &[&str]) -> RestoreArgs {
        let mut argv: Vec<String> = vec![
            "krabka-restore".to_owned(),
            "--log-dir".to_owned(),
            "/target".to_owned(),
            "--archive-local".to_owned(),
            archive_dir.display().to_string(),
        ];
        argv.extend(extra.iter().map(|s| (*s).to_owned()));
        crate::Cli::parse_from(argv).args
    }

    /// The directory-naming identity of one partition, factored out of
    /// [`write_artifact`]'s arguments so the helper stays under Clippy's
    /// argument-count limit.
    #[derive(Clone, Copy)]
    struct PartitionKey<'a> {
        topic: &'a str,
        partition: i32,
        topic_id: Uuid,
    }

    /// Write one artifact at the exact KIP-405 archive key layout, by hand.
    fn write_artifact(
        root: &std::path::Path,
        prefix: Option<&str>,
        partition: PartitionKey<'_>,
        base_offset: i64,
        segment_id: Uuid,
        suffix: &str,
    ) {
        let dir_name = format!(
            "{}-{}-{}",
            partition.topic,
            partition.partition,
            kafka_uuid(partition.topic_id)
        );
        let file_name = format!("{base_offset:020}-{}{suffix}", kafka_uuid(segment_id));
        let mut dir = root.to_path_buf();
        if let Some(prefix) = prefix {
            dir.push(prefix);
        }
        dir.push(dir_name);
        std::fs::create_dir_all(&dir).expect("create partition dir");
        std::fs::write(dir.join(file_name), STUB_BYTES).expect("write artifact");
    }

    /// Write every artifact of one complete segment copy.
    fn write_full_segment(
        root: &std::path::Path,
        topic: &str,
        partition: i32,
        topic_id: Uuid,
        base_offset: i64,
        segment_id: Uuid,
    ) {
        let key = PartitionKey {
            topic,
            partition,
            topic_id,
        };
        for suffix in FULL_SEGMENT_SUFFIXES {
            write_artifact(root, None, key, base_offset, segment_id, suffix);
        }
    }

    /// The [`SegmentInventory`] a call to [`write_full_segment`] with the same
    /// arguments produces.
    fn expected_full_segment(
        store: &ArchiveStore,
        topic: &str,
        partition: i32,
        topic_id: Uuid,
        base_offset: i64,
        segment_id: Uuid,
    ) -> SegmentInventory {
        let object = |suffix: &str| {
            Some(ArchiveObject {
                key: store.key(&format!(
                    "{topic}-{partition}-{}/{base_offset:020}-{}{suffix}",
                    kafka_uuid(topic_id),
                    kafka_uuid(segment_id),
                )),
                size: STUB_BYTES.len() as u64,
            })
        };
        SegmentInventory {
            segment_id,
            base_offset: Offset(base_offset),
            log: object(".log"),
            offset_index: object(".index"),
            time_index: object(".timeindex"),
            producer_snapshot: object(".snapshot"),
            leader_epoch: object(".leader_epoch_checkpoint"),
            transaction_index: None,
        }
    }

    /// One RLMM-tracked segment, for a `--rlmm-snapshot` fixture.
    fn snapshot_segment(
        topic: &str,
        partition: i32,
        topic_id: Uuid,
        segment_id: Uuid,
        base_offset: i64,
        state: RemoteLogSegmentState,
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
                i32::try_from(STUB_BYTES.len()).expect("stub length fits in i32"),
                state,
                BTreeMap::from([(LeaderEpoch(0), base_offset)]),
            ),
        )
        .expect("valid segment metadata")
    }

    fn write_snapshot(path: &std::path::Path, dump: RlmmCacheDump) {
        Snapshot {
            committed_offsets: Vec::new(),
            dump,
        }
        .write_atomic(path)
        .expect("write snapshot");
    }

    #[tokio::test]
    async fn a_clean_archive_groups_and_sorts_by_topic_partition_and_base_offset() {
        let archive = tempfile::tempdir().expect("temp dir");
        let orders_id = Uuid::from_u128(1);
        let alerts_id = Uuid::from_u128(2);
        let seg_a = Uuid::from_u128(10);
        let seg_b = Uuid::from_u128(11);
        let seg_c = Uuid::from_u128(12);
        let seg_d = Uuid::from_u128(20);
        let seg_e = Uuid::from_u128(30);

        // orders-0: three segments, written out of base-offset order.
        write_full_segment(archive.path(), "orders", 0, orders_id, 100, seg_b);
        write_full_segment(archive.path(), "orders", 0, orders_id, 0, seg_a);
        write_full_segment(archive.path(), "orders", 0, orders_id, 250, seg_c);
        // orders-1: one segment.
        write_full_segment(archive.path(), "orders", 1, orders_id, 0, seg_d);
        // alerts-0: one segment. "alerts" sorts before "orders".
        write_full_segment(archive.path(), "alerts", 0, alerts_id, 0, seg_e);

        let args = args_from(archive.path(), &[]);
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        let expected = ArchiveInventory {
            partitions: vec![
                PartitionInventory {
                    partition: TopicIdPartition::new(alerts_id, "alerts", 0),
                    segments: vec![expected_full_segment(
                        &store, "alerts", 0, alerts_id, 0, seg_e,
                    )],
                },
                PartitionInventory {
                    partition: TopicIdPartition::new(orders_id, "orders", 0),
                    segments: vec![
                        expected_full_segment(&store, "orders", 0, orders_id, 0, seg_a),
                        expected_full_segment(&store, "orders", 0, orders_id, 100, seg_b),
                        expected_full_segment(&store, "orders", 0, orders_id, 250, seg_c),
                    ],
                },
                PartitionInventory {
                    partition: TopicIdPartition::new(orders_id, "orders", 1),
                    segments: vec![expected_full_segment(
                        &store, "orders", 1, orders_id, 0, seg_d,
                    )],
                },
            ],
            unrecognized: vec![],
        };
        check!(result == expected);

        // `TopicIdPartition` equality ignores the topic name, so pin the
        // names too: a bug that mixed up which name goes with which id would
        // not otherwise be caught by the struct comparison above.
        let names: Vec<(&str, i32)> = result
            .partitions
            .iter()
            .map(|p| (p.partition.topic.as_str(), p.partition.partition))
            .collect();
        check!(names == vec![("alerts", 0), ("orders", 0), ("orders", 1)]);
    }

    #[tokio::test]
    async fn a_topic_filter_narrows_the_selection_and_the_rest_lands_in_unrecognized() {
        let archive = tempfile::tempdir().expect("temp dir");
        let orders_id = Uuid::from_u128(1);
        let payments_id = Uuid::from_u128(2);
        let orders_seg = Uuid::from_u128(10);
        let payments_seg = Uuid::from_u128(20);
        write_full_segment(archive.path(), "orders", 0, orders_id, 0, orders_seg);
        write_full_segment(archive.path(), "payments", 0, payments_id, 0, payments_seg);

        let args = args_from(archive.path(), &["--topic", "orders"]);
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        check!(result.partitions.len() == 1);
        check!(result.partitions[0].partition.topic == "orders");
        // Every key of the topic `--topic` excluded is kept, not dropped.
        check!(result.unrecognized.len() == FULL_SEGMENT_SUFFIXES.len());
        let payments_dir = format!("payments-0-{}", kafka_uuid(payments_id));
        check!(
            result
                .unrecognized
                .iter()
                .all(|key| key.to_string().contains(&payments_dir))
        );
    }

    #[tokio::test]
    async fn malformed_keys_land_in_unrecognized_not_dropped_and_not_an_error() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        write_full_segment(
            archive.path(),
            "orders",
            0,
            topic_id,
            0,
            Uuid::from_u128(10),
        );
        // Wrong number of path components: a key directly under the root.
        std::fs::write(archive.path().join("not-a-valid-key.log"), b"junk").expect("write");
        // Two components, but the directory name does not decode.
        std::fs::create_dir_all(archive.path().join("weird-dir")).expect("mkdir");
        std::fs::write(
            archive
                .path()
                .join("weird-dir")
                .join("00000000000000000005-not-base64.log"),
            b"junk",
        )
        .expect("write");

        let args = args_from(archive.path(), &[]);
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        check!(result.partitions.len() == 1);
        check!(result.unrecognized.len() == 2);
        check!(
            result
                .unrecognized
                .iter()
                .any(|key| key.to_string().contains("not-a-valid-key.log"))
        );
        check!(
            result
                .unrecognized
                .iter()
                .any(|key| key.to_string().contains("weird-dir"))
        );
    }

    #[tokio::test]
    async fn a_torn_copy_still_appears_with_the_missing_artifact_as_none() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        let seg = Uuid::from_u128(10);
        let key = PartitionKey {
            topic: "orders",
            partition: 0,
            topic_id,
        };
        // Only `.log` and `.index` land; the rest of the copy never arrives.
        // Discovery reports presence and leaves judging completeness to
        // `verify`, so this must not become a `TornCopy` error here.
        write_artifact(archive.path(), None, key, 0, seg, ".log");
        write_artifact(archive.path(), None, key, 0, seg, ".index");

        let args = args_from(archive.path(), &[]);
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        check!(result.partitions.len() == 1);
        let segment = &result.partitions[0].segments[0];
        check!(segment.log.is_some());
        check!(segment.offset_index.is_some());
        check!(segment.time_index.is_none());
        check!(segment.producer_snapshot.is_none());
        check!(segment.leader_epoch.is_none());
        check!(segment.transaction_index.is_none());
    }

    #[tokio::test]
    async fn a_key_prefix_does_not_confuse_the_relative_path_split() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        let seg = Uuid::from_u128(10);
        let key = PartitionKey {
            topic: "orders",
            partition: 0,
            topic_id,
        };
        write_artifact(archive.path(), Some("tier"), key, 0, seg, ".log");
        write_artifact(archive.path(), Some("tier"), key, 0, seg, ".index");

        let args = args_from(archive.path(), &["--archive-prefix", "tier"]);
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        check!(result.unrecognized.is_empty());
        check!(result.partitions.len() == 1);
        check!(result.partitions[0].segments.len() == 1);
    }

    #[tokio::test]
    async fn an_archive_with_nothing_in_it_is_an_empty_archive_error() {
        let archive = tempfile::tempdir().expect("temp dir");
        let args = args_from(archive.path(), &[]);
        let store = open_archive(&args).expect("store");
        let err = inventory(&store, &args).await.unwrap_err();
        check!(matches!(err, RestoreError::EmptyArchive { prefix } if prefix.is_empty()));
    }

    #[tokio::test]
    async fn a_topic_filter_that_selects_nothing_is_also_an_empty_archive_error() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        write_full_segment(
            archive.path(),
            "orders",
            0,
            topic_id,
            0,
            Uuid::from_u128(10),
        );

        let args = args_from(archive.path(), &["--topic", "bogus"]);
        let store = open_archive(&args).expect("store");
        let err = inventory(&store, &args).await.unwrap_err();
        check!(matches!(err, RestoreError::EmptyArchive { .. }));
    }

    #[tokio::test]
    async fn a_snapshot_that_agrees_keeps_every_live_segment() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        let seg_a = Uuid::from_u128(10);
        let seg_b = Uuid::from_u128(11);
        write_full_segment(archive.path(), "orders", 0, topic_id, 0, seg_a);
        write_full_segment(archive.path(), "orders", 0, topic_id, 100, seg_b);

        let snap_dir = tempfile::tempdir().expect("temp dir");
        let snap_path = snap_dir.path().join("snapshot");
        write_snapshot(
            &snap_path,
            RlmmCacheDump {
                partitions: vec![PartitionDump {
                    topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                    segments: vec![
                        snapshot_segment(
                            "orders",
                            0,
                            topic_id,
                            seg_a,
                            0,
                            RemoteLogSegmentState::CopySegmentFinished,
                        ),
                        snapshot_segment(
                            "orders",
                            0,
                            topic_id,
                            seg_b,
                            100,
                            RemoteLogSegmentState::CopySegmentFinished,
                        ),
                    ],
                    delete_state: None,
                }],
            },
        );

        let args = args_from(
            archive.path(),
            &["--rlmm-snapshot", &snap_path.display().to_string()],
        );
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        check!(result.partitions.len() == 1);
        check!(result.partitions[0].segments.len() == 2);
    }

    #[tokio::test]
    async fn a_delete_started_segment_is_excluded_from_the_inventory_without_an_error() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        let seg_a = Uuid::from_u128(10);
        let seg_b = Uuid::from_u128(11);
        write_full_segment(archive.path(), "orders", 0, topic_id, 0, seg_a);
        write_full_segment(archive.path(), "orders", 0, topic_id, 100, seg_b);

        let snap_dir = tempfile::tempdir().expect("temp dir");
        let snap_path = snap_dir.path().join("snapshot");
        write_snapshot(
            &snap_path,
            RlmmCacheDump {
                partitions: vec![PartitionDump {
                    topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                    segments: vec![
                        snapshot_segment(
                            "orders",
                            0,
                            topic_id,
                            seg_a,
                            0,
                            RemoteLogSegmentState::CopySegmentFinished,
                        ),
                        // Deletion is in flight: the remote tier may not
                        // have caught up yet, so leftover bytes are
                        // expected and this must not be an error.
                        snapshot_segment(
                            "orders",
                            0,
                            topic_id,
                            seg_b,
                            100,
                            RemoteLogSegmentState::DeleteSegmentStarted,
                        ),
                    ],
                    delete_state: None,
                }],
            },
        );

        let args = args_from(
            archive.path(),
            &["--rlmm-snapshot", &snap_path.display().to_string()],
        );
        let store = open_archive(&args).expect("store");
        let result = inventory(&store, &args).await.expect("inventory");

        check!(result.partitions.len() == 1);
        check!(result.partitions[0].segments.len() == 1);
        check!(result.partitions[0].segments[0].segment_id == seg_a);
    }

    #[tokio::test]
    async fn a_segment_the_snapshot_does_not_mention_is_a_disagreement() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        write_full_segment(
            archive.path(),
            "orders",
            0,
            topic_id,
            0,
            Uuid::from_u128(10),
        );

        let snap_dir = tempfile::tempdir().expect("temp dir");
        let snap_path = snap_dir.path().join("snapshot");
        write_snapshot(
            &snap_path,
            RlmmCacheDump {
                partitions: vec![PartitionDump {
                    topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                    // The snapshot knows nothing about this partition's
                    // segment at all.
                    segments: vec![],
                    delete_state: None,
                }],
            },
        );

        let args = args_from(
            archive.path(),
            &["--rlmm-snapshot", &snap_path.display().to_string()],
        );
        let store = open_archive(&args).expect("store");
        let err = inventory(&store, &args).await.unwrap_err();
        check!(matches!(
            err,
            RestoreError::MetadataDisagreement { topic, partition, .. }
                if topic == "orders" && partition == 0
        ));
    }

    #[tokio::test]
    async fn a_delete_finished_segment_with_bytes_still_present_is_a_disagreement() {
        // `DeleteSegmentFinished` says the bytes should be gone. Bytes still
        // being in the archive is a real inconsistency, unlike
        // `DeleteSegmentStarted`, where a deletion still in flight leaving
        // bytes behind is routine and gets dropped silently instead.
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        let seg = Uuid::from_u128(10);
        write_full_segment(archive.path(), "orders", 0, topic_id, 0, seg);

        let snap_dir = tempfile::tempdir().expect("temp dir");
        let snap_path = snap_dir.path().join("snapshot");
        write_snapshot(
            &snap_path,
            RlmmCacheDump {
                partitions: vec![PartitionDump {
                    topic_id_partition: TopicIdPartition::new(topic_id, "orders", 0),
                    segments: vec![snapshot_segment(
                        "orders",
                        0,
                        topic_id,
                        seg,
                        0,
                        RemoteLogSegmentState::DeleteSegmentFinished,
                    )],
                    delete_state: None,
                }],
            },
        );

        let args = args_from(
            archive.path(),
            &["--rlmm-snapshot", &snap_path.display().to_string()],
        );
        let store = open_archive(&args).expect("store");
        let err = inventory(&store, &args).await.unwrap_err();
        check!(matches!(err, RestoreError::MetadataDisagreement { .. }));
    }

    #[tokio::test]
    async fn a_live_partition_missing_from_the_scan_entirely_is_a_disagreement() {
        let archive = tempfile::tempdir().expect("temp dir");
        let payments_id = Uuid::from_u128(2);
        let payments_seg = Uuid::from_u128(20);
        write_full_segment(archive.path(), "payments", 0, payments_id, 0, payments_seg);

        let orders_id = Uuid::from_u128(1);
        let snap_dir = tempfile::tempdir().expect("temp dir");
        let snap_path = snap_dir.path().join("snapshot");
        write_snapshot(
            &snap_path,
            RlmmCacheDump {
                partitions: vec![
                    // Matches the scan exactly: no disagreement from this one.
                    PartitionDump {
                        topic_id_partition: TopicIdPartition::new(payments_id, "payments", 0),
                        segments: vec![snapshot_segment(
                            "payments",
                            0,
                            payments_id,
                            payments_seg,
                            0,
                            RemoteLogSegmentState::CopySegmentFinished,
                        )],
                        delete_state: None,
                    },
                    // Live in the snapshot, but the scan never found this
                    // partition at all.
                    PartitionDump {
                        topic_id_partition: TopicIdPartition::new(orders_id, "orders", 0),
                        segments: vec![snapshot_segment(
                            "orders",
                            0,
                            orders_id,
                            Uuid::from_u128(10),
                            0,
                            RemoteLogSegmentState::CopySegmentFinished,
                        )],
                        delete_state: None,
                    },
                ],
            },
        );

        let args = args_from(
            archive.path(),
            &["--rlmm-snapshot", &snap_path.display().to_string()],
        );
        let store = open_archive(&args).expect("store");
        let err = inventory(&store, &args).await.unwrap_err();
        check!(matches!(
            err,
            RestoreError::MetadataDisagreement { topic, partition, .. }
                if topic == "orders" && partition == 0
        ));
    }

    #[tokio::test]
    async fn a_missing_snapshot_file_is_reported_as_io_not_found() {
        let archive = tempfile::tempdir().expect("temp dir");
        let topic_id = Uuid::from_u128(1);
        write_full_segment(
            archive.path(),
            "orders",
            0,
            topic_id,
            0,
            Uuid::from_u128(10),
        );

        let missing = archive.path().join("does-not-exist-snapshot");
        let args = args_from(
            archive.path(),
            &["--rlmm-snapshot", &missing.display().to_string()],
        );
        let store = open_archive(&args).expect("store");
        let err = inventory(&store, &args).await.unwrap_err();
        check!(matches!(
            err,
            RestoreError::Io(io_error) if io_error.kind() == std::io::ErrorKind::NotFound
        ));
    }
}
