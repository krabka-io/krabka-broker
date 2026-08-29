//! End-to-end proof that `krabka-restore`'s discover/verify/bound/materialize
//! stages compose correctly against a REAL KIP-405 archive.
//!
//! Every module under `src/` already has thorough unit-level tests, built
//! against a hand-faked `SegmentInventory`/`VerifiedSegment`. This file does
//! not repeat that: it builds a real archive the same way the broker's own
//! tiered-storage copy path builds one -- a real `krabka_log::Log`, appended
//! real batches, sealed into real segments, archived through the real
//! `LocalTieredStorage::copy_log_segment_data` (the pattern
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` uses to prove the JVM
//! reads a Krabka-offloaded segment) -- then drives the whole pipeline
//! through `krabka_restore::run_from_args`/`restore`, the crate's own
//! documented in-process entry points (a subprocess needs a Cargo working
//! tree, which a Bazel sandbox does not have; `crates/format/src/lib.rs`'s
//! `run_from_args` doc gives the same rationale for `krabka format`).
//!
//! The fixture spans two topics, one of them ("orders") with two partitions,
//! and one partition ("orders-0") with two archived segments, so discovery
//! has more than one topic and partition to group and materialize has to
//! continue a partition's log across a segment boundary.

use std::{collections::BTreeMap, path::Path};

use assert2::{assert, check};
use bytes::Bytes;
use clap::Parser as _;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_log::{Log, LogConfig, name};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_remote_storage::{
    LocalTieredStorage, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager as _, TopicIdPartition,
};
use krabka_restore::{
    Cli, EXIT_OK, PartitionReport, ReportFormat, RestoreArgs, RestoreReport, SegmentOutcome,
    restore, run_from_args,
};
use tempfile::TempDir;
use uuid::Uuid;

/// A `LogConfig` whose `segment_size` is shrunk to a little over one byte, so
/// a second `append` always rolls the batch that was active before it into
/// its own sealed segment.
///
/// `krabka-restore` depends on `krabka-log` but not on `krabka-units`
/// directly, so this crate has no path to name `krabka_units::ByteSize` or
/// call `krabka_units::prelude::bytes`. Scaling the existing 1 GiB default
/// down through `Div<f64>` -- implemented on every `uom` quantity
/// `krabka-units` wraps, and reachable here purely through operator syntax
/// without naming the type -- gets the same tiny segment size the sibling
/// `jvm_tiered_storage.rs` fixture builds with `bytes(1)`.
fn tiny_segment_config() -> LogConfig {
    let default = LogConfig::default();
    LogConfig {
        segment_size: default.segment_size / 1_073_741_824.0,
        ..default
    }
}

/// One record with a distinguishable value, at `offset_delta` within its
/// batch.
fn record(offset_delta: i32, value: &str) -> Record {
    Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta),
        offset_delta,
        key: None,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: Vec::new(),
    }
}

/// A batch holding one record per `values` entry, in order. `base_offset` is
/// a placeholder: `Log::append` overwrites it with the log's real end offset
/// when the batch is appended.
fn text_batch(values: &[&str]) -> RecordBatch {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            record(
                i32::try_from(index).expect("fixture batches stay far below i32::MAX"),
                value,
            )
        })
        .collect();
    let last_offset_delta = records.iter().map(|r| r.offset_delta).max().unwrap_or(0);
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes: Attributes::default(),
        last_offset_delta,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000 + i64::from(last_offset_delta),
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    }
}

/// One archived segment: the id it was archived under, and the exact batch
/// (with its real, log-assigned `base_offset`) a correct restore must
/// reproduce verbatim.
struct SegmentFixture {
    segment_id: Uuid,
    batch: RecordBatch,
}

/// One archived partition: its identity, and every segment archived for it,
/// in base-offset order.
struct PartitionFixture {
    topic: &'static str,
    topic_id: Uuid,
    partition: i32,
    segments: Vec<SegmentFixture>,
}

impl PartitionFixture {
    /// The batches a correct restore must reproduce, in offset order.
    fn expected_batches(&self) -> Vec<RecordBatch> {
        self.segments.iter().map(|s| s.batch.clone()).collect()
    }
}

/// What one partition's fixture is built from: its identity, and its record
/// values grouped into batches so that every group but the last seals into
/// its own archived segment (see [`build_partition`]). The last group keeps
/// the log's active segment non-empty but is never archived: a real
/// tiered-storage copy never tiers the still-open active segment either.
#[derive(Clone, Copy)]
struct PartitionSpec<'a> {
    topic: &'static str,
    topic_id: Uuid,
    partition: i32,
    groups: &'a [&'a [&'a str]],
}

/// Build a real, multi-batch `krabka_log::Log` for one partition, roll it so
/// every group but the last seals into its own segment, and archive each
/// sealed segment through the real `LocalTieredStorage::copy_log_segment_data`
/// -- the same call the broker's remote-log-manager makes.
fn build_partition(storage: &LocalTieredStorage, spec: PartitionSpec<'_>) -> PartitionFixture {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), tiny_segment_config()).expect("open local log");

    let mut appended: Vec<RecordBatch> = Vec::with_capacity(spec.groups.len());
    for values in spec.groups {
        let mut batch = text_batch(values);
        log.append(&mut batch).expect("append batch");
        appended.push(batch);
    }

    let exports = log.tierable_segments();
    assert!(
        exports.len() == spec.groups.len() - 1,
        "{}-{}: every append after the first should roll exactly one segment",
        spec.topic,
        spec.partition,
    );

    let segments = exports
        .iter()
        .zip(appended.iter())
        .map(|(export, batch)| {
            let segment_id = Uuid::new_v4();
            let partition_id = TopicIdPartition::new(spec.topic_id, spec.topic, spec.partition);
            let metadata = RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(partition_id, segment_id),
                export.base_offset.0,
                export.last_offset.0,
                export.max_timestamp,
                1,
                0,
                RemoteLogSegmentDetails::new(
                    i32::try_from(
                        std::fs::metadata(&export.log_path)
                            .expect("log metadata")
                            .len(),
                    )
                    .expect("fixture segment fits i32"),
                    RemoteLogSegmentState::CopySegmentFinished,
                    BTreeMap::from([(LeaderEpoch(0), export.base_offset.0)]),
                ),
            )
            .expect("valid remote metadata");
            storage
                .copy_log_segment_data(
                    &metadata,
                    &LogSegmentData {
                        log_segment: export.log_path.clone(),
                        offset_index: export.offset_index_path.clone(),
                        time_index: export.time_index_path.clone(),
                        transaction_index: export.transaction_index_path.clone(),
                        producer_snapshot_index: Some(export.producer_snapshot_path.clone()),
                        leader_epoch_index: Bytes::from_static(b"0\n1\n0 0\n"),
                    },
                )
                .expect("archive the segment");

            SegmentFixture {
                segment_id,
                batch: batch.clone(),
            }
        })
        .collect();

    PartitionFixture {
        topic: spec.topic,
        topic_id: spec.topic_id,
        partition: spec.partition,
        segments,
    }
}

/// The whole archive this file tests against: two topics ("orders" with two
/// partitions, "payments" with one), and one partition ("orders-0") with two
/// archived segments.
struct Fixture {
    archive_root: TempDir,
    orders_id: Uuid,
    payments_id: Uuid,
    orders_0: PartitionFixture,
    orders_1: PartitionFixture,
    payments_0: PartitionFixture,
}

impl Fixture {
    /// Every partition, in the (topic, partition) order discovery sorts to.
    fn partitions(&self) -> [&PartitionFixture; 3] {
        [&self.orders_0, &self.orders_1, &self.payments_0]
    }

    /// Distinct topics the archive holds.
    fn topic_count() -> usize {
        2
    }
}

fn build_fixture() -> Fixture {
    let archive_root = tempfile::tempdir().expect("archive root");
    let storage = LocalTieredStorage::new(archive_root.path());
    let orders_id = Uuid::new_v4();
    let payments_id = Uuid::new_v4();

    let orders_0 = build_partition(
        &storage,
        PartitionSpec {
            topic: "orders",
            topic_id: orders_id,
            partition: 0,
            groups: &[&["o0-0", "o0-1"], &["o0-2"], &["o0-3", "o0-4"]],
        },
    );
    let orders_1 = build_partition(
        &storage,
        PartitionSpec {
            topic: "orders",
            topic_id: orders_id,
            partition: 1,
            groups: &[&["o1-0", "o1-1"], &["o1-2"]],
        },
    );
    let payments_0 = build_partition(
        &storage,
        PartitionSpec {
            topic: "payments",
            topic_id: payments_id,
            partition: 0,
            groups: &[&["p0-0", "p0-1"], &["p0-2"]],
        },
    );

    Fixture {
        archive_root,
        orders_id,
        payments_id,
        orders_0,
        orders_1,
        payments_0,
    }
}

/// Build `RestoreArgs` with a valid target-side flag set (`--node-id`,
/// `--standalone`, `--controller-listener`) so `format_target` succeeds, plus
/// whatever `extra` flags a test needs.
fn restore_args(archive_root: &Path, log_dir: &Path, extra: &[&str]) -> RestoreArgs {
    let mut argv = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive_root.display().to_string(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// 1. Full restore round-trip through the CLI-parsing entry point.
#[tokio::test]
async fn run_from_args_restores_the_archive_and_returns_exit_ok() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");

    let code = run_from_args([
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        fixture.archive_root.path().display().to_string(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ])
    .await;

    check!(code == EXIT_OK);
    check!(log_dir.join("meta.properties.json").exists());
}

/// 2. Read back the restored data: every partition's log holds exactly the
/// batches this fixture archived for it, each at its original absolute
/// offset (part of the whole-`RecordBatch` equality below, since
/// `base_offset` is a field of it).
#[tokio::test]
async fn restored_partitions_read_back_the_original_batches_at_their_original_offsets() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let args = restore_args(fixture.archive_root.path(), &log_dir, &[]);

    restore(&args).await.expect("restore");

    for partition in fixture.partitions() {
        let dir = name::partition_dir(&log_dir, partition.topic, partition.partition);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen restored partition");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read restored partition");
        check!(
            read.batches == partition.expected_batches(),
            "{}-{}",
            partition.topic,
            partition.partition,
        );
    }
}

/// 3. Read back the restored metadata: `meta.properties.json` names the
/// cluster id `format_target` chose, and `bootstrap.records.bin` carries the
/// archive's topics and partitions -- with the SAME topic id the archive
/// used, not a freshly generated one.
#[tokio::test]
async fn restored_bootstrap_metadata_carries_the_archived_topic_ids_and_partition_counts() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let cluster_id = Uuid::new_v4();
    let args = restore_args(
        fixture.archive_root.path(),
        &log_dir,
        &["--cluster-id", &cluster_id.to_string()],
    );

    let report = restore(&args).await.expect("restore");
    check!(report.cluster_id == cluster_id);

    let meta: serde_json::Value = serde_json::from_slice(
        &std::fs::read(log_dir.join("meta.properties.json")).expect("meta.properties.json"),
    )
    .expect("meta.properties.json is JSON");
    check!(meta["cluster_id"] == serde_json::json!(cluster_id.to_string()));

    // `bootstrap.records.bin` is a length-prefixed stream of
    // `serde_wincode::SerdeCompat<MetadataRecord>` payloads (see
    // `crates/format/src/format.rs` and
    // `crates/format/tests/seeded_records.rs`), and `wincode`/`serde-wincode`
    // are dependencies of `krabka-format` alone, not of `krabka-restore` --
    // this crate (and so this test, which may only touch this one file) has
    // no `use` path to either crate. Record survival is checked two ways
    // instead: the record *count* the restore's seeded topics and
    // partitions must have added, against a baseline format with the same
    // target flags and no seeding; and the archived topic id's raw bytes,
    // which a `Uuid` serializes to verbatim (as a 16-byte tuple, with no
    // framing of its own, per the `uuid` crate's `Serialize` impl for a
    // non-self-describing format) in any binary serde format, wincode
    // included.
    let baseline = tempfile::tempdir().expect("baseline tempdir");
    let baseline_dir = baseline.path().join("formatted");
    let baseline_code = krabka_format::run_from_args([
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        baseline_dir.display().to_string(),
        "--cluster-id".to_owned(),
        Uuid::new_v4().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ])
    .await;
    check!(baseline_code == 0);

    let record_count = |dir: &Path| -> u64 {
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.join("bootstrap.json")).expect("bootstrap.json"),
        )
        .expect("bootstrap.json is JSON");
        manifest["record_count"]
            .as_u64()
            .expect("record_count is a JSON number")
    };
    let extra_records =
        u64::try_from(Fixture::topic_count() + fixture.partitions().len()).expect("small count");
    check!(record_count(&log_dir) == record_count(&baseline_dir) + extra_records);

    let bin = std::fs::read(log_dir.join("bootstrap.records.bin")).expect("bootstrap.records.bin");
    for topic_id in [fixture.orders_id, fixture.payments_id] {
        let topic_id_bytes: [u8; 16] = topic_id.into_bytes();
        check!(
            bin.windows(16).any(|window| window == topic_id_bytes),
            "topic id {topic_id} not found in bootstrap.records.bin"
        );
    }
    for topic in ["orders", "payments"] {
        check!(
            bin.windows(topic.len())
                .any(|window| window == topic.as_bytes()),
            "topic name {topic:?} not found in bootstrap.records.bin"
        );
    }
}

/// 4. `--dry-run` reports success but writes no partition data.
///
/// `restore()` still formats the target under `--dry-run` (`format_target`
/// runs unconditionally; only `write_segment`'s log-writing is skipped), so
/// `log_dir` itself, and the bootstrap files inside it, DO exist afterward --
/// exactly the shape `crates/restore/src/materialize.rs`'s own
/// `dry_run_matches_a_real_run_but_writes_nothing` unit test checks. What
/// must be absent is each partition's own data directory.
#[tokio::test]
async fn dry_run_reports_success_but_writes_no_partition_data() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");

    let code = run_from_args([
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        fixture.archive_root.path().display().to_string(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
        "--dry-run".to_owned(),
    ])
    .await;

    check!(code == EXIT_OK);
    for partition in fixture.partitions() {
        let dir = name::partition_dir(&log_dir, partition.topic, partition.partition);
        check!(!dir.exists(), "{}-{}", partition.topic, partition.partition);
    }
}

/// 5. `restore()`'s report -- the structured value `--report json` renders --
/// carries exactly the record and segment counts this fixture should
/// produce, compared as whole structs rather than field by field.
#[tokio::test]
async fn json_report_matches_the_fixtures_exact_record_and_segment_counts() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let cluster_id = Uuid::new_v4();
    let args = restore_args(
        fixture.archive_root.path(),
        &log_dir,
        &["--cluster-id", &cluster_id.to_string()],
    );

    let report = restore(&args).await.expect("restore");

    let expected = RestoreReport {
        dry_run: false,
        log_dir: log_dir.clone(),
        cluster_id,
        partitions: fixture
            .partitions()
            .iter()
            .map(|partition| PartitionReport {
                topic: partition.topic.to_owned(),
                partition: partition.partition,
                topic_id: partition.topic_id,
                segments: partition
                    .segments
                    .iter()
                    .map(|segment| {
                        let base_offset = Offset(segment.batch.base_offset);
                        let end_offset = Offset(
                            segment.batch.base_offset + i64::from(segment.batch.last_offset_delta),
                        );
                        SegmentOutcome {
                            segment_id: segment.segment_id,
                            base_offset,
                            end_offset,
                            batches_kept: 1,
                            batches_rewritten: 0,
                            batches_emptied: 0,
                            records_kept: u64::try_from(segment.batch.records.len())
                                .expect("fixture batches stay tiny"),
                            records_dropped: 0,
                            bytes_written: u64::try_from(segment.batch.encoded_len())
                                .expect("fixture batches stay tiny"),
                        }
                    })
                    .collect(),
            })
            .collect(),
        skipped: Vec::new(),
    };
    check!(report == expected);

    // Also render and reparse as JSON, exercising the `--report json` path
    // itself rather than only the underlying struct it renders.
    let json = report.render(ReportFormat::Json);
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    check!(value["cluster_id"] == serde_json::json!(cluster_id.to_string()));
    check!(value["partitions"][0]["topic"] == serde_json::json!("orders"));
    check!(value["partitions"][0]["partition"] == serde_json::json!(0));
    check!(
        value["partitions"][0]["segments"]
            .as_array()
            .expect("segments array")
            .len()
            == 2
    );
    check!(
        value["skipped"]
            .as_array()
            .expect("skipped array")
            .is_empty()
    );
}
