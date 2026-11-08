//! A real sealed log segment, archived the way the broker's remote log
//! manager archives one, for restore suites that drive the whole pipeline.

#[path = "archive_fixture.rs"]
mod archive_fixture;
#[path = "args.rs"]
mod cli_args;

use {
    assert2::assert,
    bytes::Bytes,
    clap::Parser as _,
    krabka_ids::Offset,
    krabka_log::{Log, LogConfig},
    krabka_protocol::records::{Record, RecordBatch},
    krabka_remote_storage::{LocalTieredStorage, TopicIdPartition},
    krabka_restore::{Cli, RestoreArgs},
    std::path::Path as StdPath,
    uuid::Uuid,
};

/// A `LogConfig` whose `segment_size` is shrunk to a 1-byte equivalent, so a
/// second `append` always rolls the first batch into its own sealed segment
/// that [`Log::tierable_segments`] will hand back.
///
/// `krabka-restore` depends on `krabka-log` but not on `krabka-units`
/// directly, so this crate has no path to name `krabka_units::ByteSize` or
/// call `krabka_units::prelude::bytes`. Scaling the default 1 GiB
/// `segment_size` down through `Div<f64>` -- implemented on every `uom`
/// quantity `krabka-units` wraps, and reachable here purely through operator
/// syntax without naming the type -- gets the same tiny segment size the
/// sibling `jvm_tiered_storage.rs` fixture builds with `bytes(1)`.
pub fn tiny_segment_config() -> LogConfig {
    let default = LogConfig::default();
    LogConfig {
        segment_size: default.segment_size / 1_073_741_824.0,
        ..default
    }
}

/// One record batch with a 256-byte value, long enough that a corruption
/// offset deep in the body is nowhere near the batch's boundaries.
pub fn long_batch() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from(vec![b'x'; 256])),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

/// Build a real, two-batch krabka log, roll it so the first batch seals into
/// its own segment, and archive that sealed segment into `archive_root`
/// through the same `LocalTieredStorage::copy_log_segment_data` call the
/// broker's remote-log-manager uses. The second (still-active) batch is never
/// tiered, matching a real broker: only sealed segments are copied.
///
/// Returns the batches the local log held for that segment, which is what a
/// correct restore reproduces.
pub fn archive_segment(
    archive_root: &StdPath,
    topic: &str,
    partition: i32,
    topic_id: Uuid,
    segment_id: Uuid,
) -> Vec<RecordBatch> {
    archive_segment_at(archive_root, topic, partition, topic_id, segment_id, 0)
}

pub fn archive_segment_at(
    archive_root: &StdPath,
    topic: &str,
    partition: i32,
    topic_id: Uuid,
    segment_id: Uuid,
    base_offset: i64,
) -> Vec<RecordBatch> {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), tiny_segment_config()).expect("open local log");
    log.reset_to(Offset(base_offset)).unwrap();
    log.append(&mut long_batch()).expect("append first batch");
    log.append(&mut long_batch())
        .expect("append second batch, rolling the first into a sealed segment");

    log.sync().unwrap();
    let export = log
        .tierable_segments()
        .into_iter()
        .next()
        .expect("one sealed segment after the roll");

    // The full local log holds both batches; only the sealed one (the first)
    // was archived, so the comparison fixture keeps just that range.
    let full_read = log
        .read(Offset(base_offset), LogConfig::default().segment_size)
        .expect("read back the local log");
    let original_batches: Vec<RecordBatch> = full_read
        .batches
        .into_iter()
        .filter(|batch| {
            let last = batch.base_offset + i64::from(batch.last_offset_delta);
            batch.base_offset >= export.base_offset.0 && last <= export.last_offset.0
        })
        .collect();
    assert!(
        !original_batches.is_empty(),
        "the sealed segment's batch must survive the range filter"
    );

    let storage = LocalTieredStorage::new(archive_root);
    archive_fixture::archive_segment(
        &storage,
        TopicIdPartition::new(topic_id, topic, partition),
        segment_id,
        &export,
    );

    original_batches
}

/// Build `RestoreArgs` with a valid target-side flag set (`--node-id`,
/// `--standalone`, `--controller-listener`) so `format_target` succeeds and
/// the pipeline reaches segment verification, plus whatever `extra` flags a
/// scenario needs.
pub fn restore_args(archive_root: &StdPath, log_dir: &StdPath, extra: &[&str]) -> RestoreArgs {
    let argv = cli_args::restore_argv(archive_root, log_dir, cli_args::RestoreOptions {
        extra,
        ..Default::default()
    });
    Cli::try_parse_from(argv).expect("valid command line").args
}
