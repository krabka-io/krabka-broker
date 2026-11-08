//! End-to-end proof that the full restore pipeline reacts correctly to a
//! damaged tiered-storage archive.
//!
//! `crates/restore/src/verify.rs`'s own unit tests already check each
//! corruption case in isolation, against hand-built fixture bytes. This file
//! does not repeat that: it builds a real archive the way the broker's
//! remote-log-manager actually would -- append real batches to a real
//! `krabka_log::Log`, roll a segment, and archive it through
//! `LocalTieredStorage::copy_log_segment_data`, the same path
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` exercises -- then
//! damages one archived artifact on disk and drives the damage through
//! `krabka_restore::restore`, the crate's structured top-level entry point.
//! What's under test is the pipeline's *reaction*: the right
//! [`RestoreError`] variant, the right exit code, the right object named, and
//! -- under `--continue-on-corrupt` -- the right segment skipped while
//! everything else restores intact.
#![cfg(test)]

#[path = "support/archived_log.rs"]
mod archived_log;

use {
    archived_log::{archive_segment, archive_segment_at, restore_args},
    assert2::{assert, check},
    clap::Parser as _,
    krabka_ids::Offset,
    krabka_log::{Log, LogConfig, name},
    krabka_protocol::records::CRC_COVERAGE_START,
    krabka_remote_storage::kafka_uuid,
    krabka_restore::{Cli, EXIT_BAD_ARGUMENTS, EXIT_INTEGRITY, RestoreError, restore},
    std::{
        fs,
        fs::OpenOptions,
        path::{self, Path as StdPath, PathBuf},
    },
    uuid::Uuid,
};

/// Recursively collect every file under `dir`.
fn collect_files(dir: &StdPath, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Find the single file under `root` whose path satisfies `predicate`, or
/// panic with a message naming how many matches were actually found. A
/// non-unique match would make the corruption below ambiguous about which
/// archived object it hits, so this refuses to guess.
fn find_one_file(root: &StdPath, predicate: impl Fn(&StdPath) -> bool) -> PathBuf {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    let mut matches: Vec<_> = files.into_iter().filter(|p| predicate(p)).collect();
    match matches.len() {
        1 => matches.remove(0),
        n => panic!(
            "expected exactly one matching file under {}, found {n}",
            root.display()
        ),
    }
}

fn has_extension(path: &StdPath, extension: &str) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some(extension)
}

/// The relative object key `verify_segment` reports for `path`, matching how
/// `ArchiveObject::key` renders: the path under the archive root, with
/// forward slashes.
fn relative_key(archive_root: &StdPath, path: &StdPath) -> String {
    path.strip_prefix(archive_root)
        .expect("path is under the archive root")
        .to_str()
        .expect("archive paths are UTF-8")
        .replace(path::MAIN_SEPARATOR, "/")
}

/// Flip one byte inside a batch's CRC-covered region: at a fixed offset well
/// past both `CRC_COVERAGE_START` (21, `base_offset` and
/// `partition_leader_epoch` sit below it and could be patched freely without
/// upsetting the CRC) and the fixed 61-byte v2 batch header, landing squarely
/// in the record body. That breaks only the CRC check, not the framing or
/// any header field `verify_segment` reads before it gets to the CRC.
fn corrupt_crc_covered_byte(path: &StdPath) {
    const HEADER_LEN: usize = 61;
    const CORRUPT_OFFSET: usize = 100;
    assert!(CORRUPT_OFFSET >= CRC_COVERAGE_START);
    assert!(CORRUPT_OFFSET >= HEADER_LEN);

    let mut bytes = fs::read(path).expect("read log file");
    assert!(
        bytes.len() > CORRUPT_OFFSET,
        "the test batch must be long enough to corrupt a body byte"
    );
    bytes[CORRUPT_OFFSET] ^= 0xFF;
    fs::write(path, bytes).expect("write corrupted log file");
}

/// Truncate a `.log` file to roughly half its length, landing inside the
/// single batch it holds rather than on a batch boundary.
fn truncate_log_mid_batch(path: &StdPath) {
    let full_len = fs::metadata(path).expect("stat log file").len();
    assert!(
        full_len > 20,
        "the test batch must be long enough to truncate meaningfully"
    );
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open log file for truncation");
    file.set_len(full_len / 2).expect("truncate log file");
}

/// An archive holding one segment, and the directory a restore targets.
struct SingleSegmentFixture {
    archive: tempfile::TempDir,
    target_parent: tempfile::TempDir,
}

fn single_segment_fixture(topic_id: u128, segment_id: u128) -> SingleSegmentFixture {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    archive_segment(
        archive.path(),
        "orders",
        0,
        Uuid::from_u128(topic_id),
        Uuid::from_u128(segment_id),
    );
    SingleSegmentFixture {
        archive,
        target_parent,
    }
}

#[derive(Clone, Copy)]
enum SegmentCorruption {
    Crc,
    Truncated,
}

#[yare::parameterized(
    crc = { 1, 0xC12C, SegmentCorruption::Crc },
    truncated = { 2, 0x7ecc, SegmentCorruption::Truncated },
)]
#[test_macro(tokio::test)]
async fn corrupt_segment_fails_the_whole_restore_and_names_the_object(
    topic_id: u128,
    segment_id: u128,
    corruption: SegmentCorruption,
) {
    let SingleSegmentFixture {
        archive,
        target_parent,
    } = single_segment_fixture(topic_id, segment_id);

    let log_path = find_one_file(archive.path(), |p| has_extension(p, "log"));
    let expected_key = relative_key(archive.path(), &log_path);
    match corruption {
        SegmentCorruption::Crc => corrupt_crc_covered_byte(&log_path),
        SegmentCorruption::Truncated => truncate_log_mid_batch(&log_path),
    }

    let args = restore_args(archive.path(), &target_parent.path().join("restored"), &[]);
    let error = restore(&args)
        .await
        .expect_err("a corrupted segment must fail the restore");

    check!(error.exit_code() == EXIT_INTEGRITY);
    match (&corruption, &error) {
        (SegmentCorruption::Crc, RestoreError::ChecksumMismatch { key, .. })
        | (SegmentCorruption::Truncated, RestoreError::TruncatedSegment { key, .. }) => {
            check!(*key == expected_key);
        }
        _ => panic!("unexpected restore error for the corruption case: {error:?}"),
    }
}

#[tokio::test]
async fn a_missing_timeindex_is_a_torn_copy_naming_the_right_artifact() {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    let segment_id = Uuid::from_u128(0x71E5);
    archive_segment(archive.path(), "orders", 0, Uuid::from_u128(3), segment_id);

    let time_index_path = find_one_file(archive.path(), |p| has_extension(p, "timeindex"));
    fs::remove_file(&time_index_path).expect("remove the .timeindex artifact");

    let args = restore_args(archive.path(), &target_parent.path().join("restored"), &[]);
    let error = restore(&args)
        .await
        .expect_err("a torn copy missing .timeindex must fail the restore");

    let exit_code = error.exit_code();
    let RestoreError::TornCopy {
        topic,
        partition,
        segment_id: reported_id,
        artifact,
    } = &error
    else {
        panic!("expected TornCopy, got {error:?}");
    };
    check!(topic == "orders");
    check!(*partition == 0);
    check!(*reported_id == segment_id);
    check!(artifact == ".timeindex");
    check!(exit_code == EXIT_INTEGRITY);
}

#[tokio::test]
async fn continue_on_corrupt_skips_only_the_damaged_segment_and_restores_the_rest() {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    let log_dir = target_parent.path().join("restored");

    let clean_batches = archive_segment(
        archive.path(),
        "orders-clean",
        0,
        Uuid::from_u128(10),
        Uuid::from_u128(0xC1EA),
    );
    let damaged_id = Uuid::from_u128(0xBAD5);
    archive_segment(
        archive.path(),
        "orders-damaged",
        0,
        Uuid::from_u128(20),
        damaged_id,
    );

    let damaged_log = find_one_file(archive.path(), |p| {
        has_extension(p, "log") && p.to_string_lossy().contains("orders-damaged")
    });
    corrupt_crc_covered_byte(&damaged_log);

    let args = restore_args(archive.path(), &log_dir, &["--continue-on-corrupt"]);
    let report = restore(&args)
        .await
        .expect("--continue-on-corrupt must still succeed overall");

    check!(report.skipped.len() == 1);
    let skipped = &report.skipped[0];
    check!(skipped.topic == "orders-damaged");
    check!(skipped.partition == 0);
    check!(skipped.segment_id == damaged_id);
    check!(
        skipped.reason.contains("checksum"),
        "reason was {:?}",
        skipped.reason
    );

    // The clean partition reads back exactly what was archived for it.
    let clean_dir = name::partition_dir(&log_dir, "orders-clean", 0);
    let restored_clean =
        Log::open(&clean_dir, LogConfig::default()).expect("open restored clean partition");
    let read_back = restored_clean
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read restored clean partition");
    check!(read_back.batches == clean_batches);

    // The damaged partition's only segment was skipped before any write, so
    // `write_segment` never created a directory for it.
    let damaged_dir = name::partition_dir(&log_dir, "orders-damaged", 0);
    check!(!damaged_dir.exists());
}

#[tokio::test]
async fn continue_on_corrupt_preserves_both_sides_of_an_interior_gap() {
    let archive = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let log_dir = target.path().join("restored");
    let topic_id = Uuid::from_u128(10);
    let mut expected = Vec::new();
    let mut damaged_id = Uuid::nil();
    for base in 0..3 {
        let segment_id = Uuid::new_v4();
        let batches = archive_segment_at(archive.path(), "orders", 0, topic_id, segment_id, base);
        if base == 1 {
            damaged_id = segment_id;
            let log = find_one_file(archive.path(), |path| {
                has_extension(path, "log")
                    && path.to_string_lossy().contains(&kafka_uuid(segment_id))
            });
            corrupt_crc_covered_byte(&log);
        } else {
            expected.extend(batches);
        }
    }
    let report = restore(&restore_args(archive.path(), &log_dir, &[
        "--continue-on-corrupt",
    ]))
    .await
    .unwrap();
    check!(report.skipped.len() == 1);
    check!(report.skipped[0].segment_id == damaged_id);
    check!(report.partitions[0].segments.len() == 2);
    let restored = Log::open(
        name::partition_dir(&log_dir, "orders", 0),
        LogConfig::default(),
    )
    .unwrap();
    check!(restored.log_end_offset() == Offset(3));
    check!(
        restored
            .read(Offset(0), LogConfig::default().segment_size)
            .unwrap()
            .batches
            == expected
    );
}

#[tokio::test]
async fn continue_on_corrupt_does_not_rescue_a_format_target_failure() {
    // `--continue-on-corrupt` only turns a `verify_segment` failure into a
    // skipped segment; `format_target` runs before any segment is even
    // looked at and its error propagates unconditionally. Omitting
    // `--node-id` is the cheapest way to make `format_target` fail on a
    // command line that is otherwise completely valid, proving
    // `--continue-on-corrupt` does not paper over a target-side failure.
    let SingleSegmentFixture {
        archive,
        target_parent,
    } = single_segment_fixture(30, 0xF00D);

    let command_line = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive.path().display().to_string(),
        "--log-dir".to_owned(),
        target_parent.path().join("restored").display().to_string(),
        "--continue-on-corrupt".to_owned(),
    ];
    let args = Cli::try_parse_from(command_line)
        .expect("valid command line")
        .args;

    let error = restore(&args)
        .await
        .expect_err("missing --node-id must fail even with --continue-on-corrupt");

    check!(matches!(error, RestoreError::InvalidArgument(_)));
    check!(error.exit_code() == EXIT_BAD_ARGUMENTS);
}
