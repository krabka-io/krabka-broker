use std::{io::Write, path::PathBuf, sync::Arc};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_object_store::{ObjectOps, ObjectStoreError};
use krabka_units::prelude::{ByteSizeExt as _, kibibytes, mebibytes};
use object_store::{memory::InMemory, path::Path as ObjectPath};
use tempfile::TempDir;

use super::object_entry;
use crate::{
    error::RemoteStorageError,
    metadata::RemoteLogSegmentMetadata,
    s3::{
        S3RemoteStorage,
        test_support::{
            WORM_KEY_ID, rsm, sample_data, sample_metadata, stamped_metadata, worm_epoch, worm_rsm,
            write_file,
        },
    },
    storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
    worm::{
        ChainHead, MANIFEST_FORMAT_VERSION, MANIFEST_SUFFIX, ManifestSeq, ObjectEntry,
        SegmentIdentity, SegmentManifest, Sha256Digest, WormChainRecord, WormError, manifest_head,
        verify_manifest_signature,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_then_fetch_full_segment() {
    let store = rsm(None);
    let src = TempDir::new().unwrap();
    let md = sample_metadata(10);
    tokio::task::spawn_blocking(move || {
        store
            .copy_log_segment_data(&md, &sample_data(src.path(), true))
            .unwrap();
        assert!(store.fetch_log_segment(&md, 0, None).unwrap() == b"0123456789");
    })
    .await
    .unwrap();
}

fn write_log_segment(dir: &std::path::Path, len: usize) -> PathBuf {
    let p = dir.join("00.log");
    let mut f = std::fs::File::create(&p).unwrap();
    // Deterministic, position-sensitive bytes so the round-trip
    // assertion catches both reordering bugs and truncation.
    let bytes: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
    f.write_all(&bytes).unwrap();
    p
}

/// Files at or above `multipart_threshold` flow through the `ObjectOps`
/// multipart path. This test picks a chunk size that gives multiple
/// non-trailing parts, so it exercises the inner loop's tail-flush and
/// finish path. The `InMemory` backend implements `put_multipart` and
/// `complete` end-to-end, so a successful round-trip proves that the
/// multipart wire calls are correct, including the per-part offsets and
/// the final concatenation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_path_uses_multipart_above_threshold_and_round_trips() {
    // 100 KiB segment, 8 KiB threshold → multipart, 4 KiB chunks
    // → 25 parts (last one full, no tail).
    let seg_len = kibibytes(100).bytes_usize();
    let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
        .with_multipart_tuning(kibibytes(8), kibibytes(4));
    let src = TempDir::new().unwrap();
    let md = sample_metadata(40);
    let log_path = write_log_segment(src.path(), seg_len);
    let data = LogSegmentData {
        log_segment: log_path,
        offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
        time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
        transaction_index: None,
        producer_snapshot_index: Some(write_file(src.path(), "00.snapshot", b"SNAP")),
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    };
    tokio::task::spawn_blocking(move || {
        store.copy_log_segment_data(&md, &data).unwrap();
        let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
        assert!(fetched.len() == seg_len);
        for (i, b) in fetched.iter().enumerate() {
            assert!(*b == u8::try_from(i % 251).unwrap(), "byte mismatch at {i}");
        }
    })
    .await
    .unwrap();
}

/// Multipart path with a tail chunk strictly smaller than `chunk_size`.
/// `WriteMultipart::finish` flushes the partially-filled buffer as the
/// final part, and this test asserts that it does. If it did not, the
/// uploaded object would silently lose the last `tail_len` bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multipart_flushes_partial_tail_chunk() {
    let chunk = kibibytes(4);
    let seg_len = 3 * chunk.bytes_usize() + 137; // 3 full parts + tail
    let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
        .with_multipart_tuning(kibibytes(1), chunk);
    let src = TempDir::new().unwrap();
    let md = sample_metadata(41);
    let log_path = write_log_segment(src.path(), seg_len);
    let data = LogSegmentData {
        log_segment: log_path,
        offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
        time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
        transaction_index: None,
        producer_snapshot_index: None,
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    };
    tokio::task::spawn_blocking(move || {
        store.copy_log_segment_data(&md, &data).unwrap();
        let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
        assert!(fetched.len() == seg_len);
        assert!(
            fetched.last().copied() == Some(u8::try_from((seg_len - 1) % 251).unwrap()),
            "tail byte was dropped"
        );
    })
    .await
    .unwrap();
}

/// Files strictly below the threshold MUST still take the single-PUT
/// path, even when multipart tuning is configured. This test raises the
/// threshold above the fixture size. A regression that inverted the
/// branch would show as a hang, or as a multipart-specific error against
/// a backend with no multipart support. It would also be a latency
/// regression in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_path_stays_on_single_put_below_threshold() {
    let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
        .with_multipart_tuning(mebibytes(1), kibibytes(4));
    let src = TempDir::new().unwrap();
    let md = sample_metadata(42);
    let log_path = write_log_segment(src.path(), 10); // ten bytes, well under 1 MiB
    let data = LogSegmentData {
        log_segment: log_path,
        offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
        time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
        transaction_index: None,
        producer_snapshot_index: None,
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    };
    tokio::task::spawn_blocking(move || {
        store.copy_log_segment_data(&md, &data).unwrap();
        let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
        assert!(fetched.len() == 10);
    })
    .await
    .unwrap();
}

// ---- WORM archive mode -------------------------------------------------

/// Reads back and decodes the manifest a copy wrote for `md`.
fn read_manifest(store: &S3RemoteStorage, md: &RemoteLogSegmentMetadata) -> SegmentManifest {
    let raw =
        S3RemoteStorage::block_os(store.ops.get(&store.segment_key(md, MANIFEST_SUFFIX))).unwrap();
    serde_json::from_slice(&raw).unwrap()
}

/// The manifest entry a copy must record for an object holding `body`.
fn expected_entry(suffix: &str, key: &ObjectPath, body: &[u8], e_tag: &str) -> ObjectEntry {
    ObjectEntry {
        suffix: suffix.to_string(),
        key: key.to_string(),
        size_bytes: u64::try_from(body.len()).unwrap(),
        sha256: Sha256Digest::of(body),
        e_tag: Some(e_tag.to_string()),
        version_id: None,
        create_precondition: true,
    }
}

#[test]
fn multipart_manifest_entry_requires_a_version() {
    let key = ObjectPath::from("archive/segment.log");
    let error = object_entry(
        ".log",
        &key,
        krabka_object_store::PutOutcome {
            size_bytes: 1,
            sha256: Some(Sha256Digest::of(b"x").0),
            e_tag: None,
            version_id: None,
            create_precondition: false,
        },
        true,
    )
    .unwrap_err();

    check!(
        matches!(error, WormError::MissingVersionId { key: missing } if missing == key.to_string())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worm_copy_writes_a_manifest_next_to_the_segment() {
    let src = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
    let md = stamped_metadata(50, 0, ChainHead::GENESIS);
    tokio::task::spawn_blocking(move || {
        store
            .copy_log_segment_data(&md, &sample_data(src.path(), true))
            .unwrap();

        // The manifest is the log's key with the suffix swapped, so a
        // verifier that can list a partition prefix finds it beside the
        // data it describes.
        let manifest_key = store.segment_key(&md, MANIFEST_SUFFIX);
        check!(
            manifest_key.as_ref().trim_end_matches(MANIFEST_SUFFIX)
                == store.log_key(&md).as_ref().trim_end_matches(".log")
        );

        let manifest = read_manifest(&store, &md);
        check!(manifest.body.segment == SegmentIdentity::from_metadata(&md));
        check!(manifest.body.format_version == MANIFEST_FORMAT_VERSION);
        assert!(let Some(signature) = manifest.signature.as_ref());
        check!(signature.key_id == WORM_KEY_ID);
        check!(verify_manifest_signature(
            &manifest,
            &signature.public_key.0
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worm_manifest_lists_every_object_with_its_digest() {
    let src = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
    let md = stamped_metadata(51, 0, ChainHead::GENESIS);
    tokio::task::spawn_blocking(move || {
        store
            .copy_log_segment_data(&md, &sample_data(src.path(), true))
            .unwrap();

        // `InMemory` hands out etags from a per-store counter, so a fresh
        // store numbers this copy's six objects 0..=5 in upload order.
        // The digests are computed here from the fixture bodies, never
        // from what the store reported.
        let expected = vec![
            expected_entry(".log", &store.log_key(&md), b"0123456789", "0"),
            expected_entry(
                ".index",
                &store.index_key(&md, IndexType::Offset),
                b"OFFSET-IDX",
                "1",
            ),
            expected_entry(
                ".timeindex",
                &store.index_key(&md, IndexType::Timestamp),
                b"TIME-IDX",
                "2",
            ),
            expected_entry(
                ".snapshot",
                &store.index_key(&md, IndexType::ProducerSnapshot),
                b"SNAP",
                "3",
            ),
            expected_entry(
                ".leader_epoch_checkpoint",
                &store.index_key(&md, IndexType::LeaderEpoch),
                b"EPOCH-BYTES",
                "4",
            ),
            expected_entry(
                ".txnindex",
                &store.index_key(&md, IndexType::Transaction),
                b"TXN-IDX",
                "5",
            ),
        ];
        check!(read_manifest(&store, &md).body.objects == expected);
    })
    .await
    .unwrap();
}

/// A segment with no transaction index and no producer snapshot lists
/// exactly the four objects the copy wrote, and no placeholders.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worm_manifest_omits_objects_the_copy_did_not_write() {
    let src = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
    let md = stamped_metadata(52, 0, ChainHead::GENESIS);
    let data = LogSegmentData {
        log_segment: write_file(src.path(), "00.log", b"0123456789"),
        offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
        time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
        transaction_index: None,
        producer_snapshot_index: None,
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    };
    tokio::task::spawn_blocking(move || {
        store.copy_log_segment_data(&md, &data).unwrap();
        let suffixes: Vec<String> = read_manifest(&store, &md)
            .body
            .objects
            .into_iter()
            .map(|object| object.suffix)
            .collect();
        check!(suffixes == [".log", ".index", ".timeindex", ".leader_epoch_checkpoint"]);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worm_copy_returns_a_receipt_with_the_new_head() {
    let src = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
    let prev_head = ChainHead([1u8; 32]);
    let md = stamped_metadata(53, 3, prev_head);
    tokio::task::spawn_blocking(move || {
        assert!(let
            Ok(Some(custom)) =
                store.copy_log_segment_data(&md, &sample_data(src.path(), false))
        );
        assert!(let Ok(receipt) = WormChainRecord::from_custom_metadata(&custom));

        check!(
            receipt
                == WormChainRecord {
                    epoch_id: worm_epoch(),
                    seq: ManifestSeq(3),
                    prev_head,
                    head: Some(manifest_head(&read_manifest(&store, &md).body)),
                    // `InMemory` is unversioned, so the PUT reports none.
                    manifest_version_id: None,
                }
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worm_multipart_copy_refuses_replay() {
    let src = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, false)
        .with_multipart_tuning(krabka_units::bytes(8), krabka_units::bytes(4));
    let md = stamped_metadata(54, 0, ChainHead::GENESIS);
    tokio::task::spawn_blocking(move || {
        let data = sample_data(src.path(), true);
        store.copy_log_segment_data(&md, &data).unwrap();
        let first = read_manifest(&store, &md);
        check!(!first.body.objects[0].create_precondition);

        // A replayed copy stops at the very first object: `PutMode::Create`
        // refuses the `.log` key before the manifest is ever reached.
        assert!(let Err(err) = store.copy_log_segment_data(&md, &data));
        check!(matches!(&err, RemoteStorageError::ObjectExists { key }
                if *key == store.log_key(&md).to_string()));

        // With the data objects gone but the manifest still in place, the
        // conditional create on the manifest key itself is what refuses.
        for suffix in [
            ".log",
            ".index",
            ".timeindex",
            ".snapshot",
            ".leader_epoch_checkpoint",
            ".txnindex",
        ] {
            S3RemoteStorage::block_os(store.ops.delete(&store.segment_key(&md, suffix))).unwrap();
        }
        assert!(let Err(err) = store.copy_log_segment_data(&md, &data));
        check!(matches!(&err, RemoteStorageError::ObjectExists { key }
                if *key == store.segment_key(&md, MANIFEST_SUFFIX).to_string()));
        // The manifest that is there is still the original one.
        check!(read_manifest(&store, &md) == first);
    })
    .await
    .unwrap();
}

/// Regression guard on the default path: no manifest, no receipt, and
/// still an overwriting put rather than a conditional create.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_worm_copy_writes_no_manifest_and_returns_none() {
    let store = rsm(None);
    let src = TempDir::new().unwrap();
    let md = sample_metadata(57);
    tokio::task::spawn_blocking(move || {
        let data = sample_data(src.path(), true);
        check!(store.copy_log_segment_data(&md, &data).unwrap().is_none());
        check!(matches!(
            S3RemoteStorage::block_os(store.ops.get(&store.segment_key(&md, MANIFEST_SUFFIX))),
            Err(ObjectStoreError::NotFound(_))
        ));
        // A second copy overwrites rather than being refused.
        check!(store.copy_log_segment_data(&md, &data).unwrap().is_none());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worm_copy_without_a_chain_stamp_is_refused() {
    let src = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
    // No `with_custom_metadata`: the broker did not stamp this segment.
    let md = sample_metadata(58);
    tokio::task::spawn_blocking(move || {
        assert!(let
            Err(err) = store.copy_log_segment_data(&md, &sample_data(src.path(), false))
        );
        check!(matches!(
            err,
            RemoteStorageError::Worm(WormError::MissingChainStamp)
        ));
        // Nothing was committed: the manifest is the commit point, and the
        // copy failed before it.
        check!(matches!(
            S3RemoteStorage::block_os(store.ops.get(&store.segment_key(&md, MANIFEST_SUFFIX))),
            Err(ObjectStoreError::NotFound(_))
        ));
    })
    .await
    .unwrap();
}
