//! The round trip against a real S3 implementation.
//!
//! Every other round-trip case archives through `LocalTieredStorage` and
//! restores from a temp directory, so the whole suite only ever walks a local
//! filesystem -- and `krabka restore` exists for the day the archive is a
//! bucket. This suite closes that gap: it copies sealed segments into `MinIO`
//! through the production `S3RemoteStorage` backend, points the restore at the
//! bucket over `--archive-s3-*`, and reads the restored partitions back with a
//! fresh `krabka_log::Log`.
//!
//! The container lifecycle follows the pattern the broker's tiered-storage
//! suites use: a `docker run -d` guard that removes the container on drop, a
//! per-process published port so two container suites can run at once, and
//! `mc` for the one bucket operation the S3 API of `object_store` does not
//! expose.

#[path = "roundtrip/batches.rs"]
mod batches;

use std::process::{Command, Stdio};

use assert2::{assert, check};
use bytes::Bytes;
use clap::Parser as _;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_log::{Log, LogConfig, name};
use krabka_protocol::records::RecordBatch;
use krabka_remote_storage::{
    LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentState, RemoteStorageManager as _, S3Config, S3RemoteStorage, TopicIdPartition,
};
use krabka_restore::{Cli, restore};
use uuid::Uuid;

use crate::batches::{text_batch, tiny_segment_config};

/// The `MinIO` server image, pinned by the same digest table the broker's
/// container suites load from (`//bazel/images`).
const MINIO_IMAGE: &str = "mirror.gcr.io/minio/minio:RELEASE.2025-09-07T16-13-09Z";

/// The `mc` client image, used only to create the bucket.
const MINIO_CLIENT_IMAGE: &str = "mirror.gcr.io/minio/mc:RELEASE.2025-08-13T08-35-41Z";

const MINIO_ACCESS_KEY: &str = "minioadmin";

const MINIO_SECRET_KEY: &str = "minioadmin";

/// This suite's own bucket. It is not the tiered-storage suites' bucket: those
/// hold a live cluster's tier, and a restore reads an archive nobody is
/// writing to.
const BUCKET: &str = "krabka-restore-archive";

/// The archive sits under a prefix, so the restore's `--archive-prefix`
/// handling is exercised against a real bucket rather than only against a
/// temp directory.
const PREFIX: &str = "tier";

const TOPIC: &str = "orders";

/// A free ephemeral port on the loopback interface.
///
/// The port is published by the container, so two container suites running at
/// once do not fight over a fixed 9000.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener
        .local_addr()
        .expect("the bound listener has an address")
        .port()
}

/// A `docker run -d` `MinIO` container, removed on drop so an aborted test
/// leaves nothing behind.
struct MinioContainer {
    name: String,
}

impl MinioContainer {
    fn start(port: u16) -> Self {
        let name = format!("krabka-restore-minio-{}", Uuid::new_v4().simple());
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                &format!("{port}:9000"),
                "-e",
                &format!("MINIO_ROOT_USER={MINIO_ACCESS_KEY}"),
                "-e",
                &format!("MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}"),
                MINIO_IMAGE,
                "server",
                "/data",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .expect("spawn docker run minio");
        assert!(status.success(), "docker run minio failed");
        wait_for_minio_ready(port);
        Self { name }
    }
}

impl Drop for MinioContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Poll the published port until `MinIO`'s listener answers, so the first S3
/// call does not race the container's startup.
fn wait_for_minio_ready(port: u16) {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("static addr");
    for _ in 0..60 {
        if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500))
            .is_ok()
        {
            // A TCP accept is not a fully initialised S3 server; give the
            // bucket API a moment to come up.
            std::thread::sleep(std::time::Duration::from_millis(500));
            return;
        }
        // Intentional: a bounded readiness poll of an external process. No
        // krabka metric reflects its listener coming up.
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("MinIO never accepted TCP on 127.0.0.1:{port}");
}

/// Create the bucket with `mc`, retrying the alias so a slow `MinIO` startup
/// does not fail the run on the first probe.
fn make_bucket(port: u16) {
    let script = format!(
        "for i in 1 2 3 4 5 6 7 8 9 10; do \
           mc alias set local http://host.docker.internal:{port} \
             {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && break; \
           sleep 1; \
         done && mc mb -p local/{BUCKET}"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc mb");
    assert!(
        out.status.success(),
        "mc mb failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// The backend configuration both halves of the test use: the archive writer
/// and, through `--archive-s3-*`, the restore itself.
fn s3_config(port: u16) -> S3Config {
    S3Config {
        bucket: BUCKET.to_owned(),
        prefix: Some(PREFIX.to_owned()),
        region: "us-east-1".to_owned(),
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.to_owned()),
        secret_access_key: Some(MINIO_SECRET_KEY.to_owned()),
        allow_http: true,
        ..S3Config::default()
    }
}

/// One archived partition: what it is, and the batches the archive holds for
/// it in offset order.
struct ArchivedPartition {
    partition: i32,
    batches: Vec<RecordBatch>,
}

impl ArchivedPartition {
    /// The offset the restored log must start at: the base offset of the
    /// oldest batch the archive holds.
    fn base_offset(&self) -> i64 {
        self.batches
            .first()
            .expect("every archived partition holds at least one batch")
            .base_offset
    }
}

/// Append `groups` to a real log, sealing every group but the last into its
/// own segment, and copy each sealed segment into `storage` the way the
/// broker's remote-log manager does.
fn archive_partition(
    storage: &S3RemoteStorage,
    topic_id: Uuid,
    partition: i32,
    groups: &[&[&str]],
) -> ArchivedPartition {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), tiny_segment_config()).expect("open local log");

    let mut appended: Vec<RecordBatch> = Vec::with_capacity(groups.len());
    for values in groups {
        let mut batch = text_batch(values);
        log.append(&mut batch).expect("append batch");
        appended.push(batch);
    }

    let exports = log.tierable_segments();
    assert!(
        exports.len() == groups.len() - 1,
        "every append after the first should roll exactly one segment",
    );

    for export in &exports {
        let metadata = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(topic_id, TOPIC, partition),
                Uuid::new_v4(),
            ),
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
                maplit::btreemap! {LeaderEpoch(0) => export.base_offset.0},
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
                    leader_epoch_index: Bytes::from(
                        format!("0\n1\n0 {}\n", export.base_offset.0).into_bytes(),
                    ),
                },
            )
            .expect("archive the segment into MinIO");
    }

    ArchivedPartition {
        partition,
        // The last group stays in the still-open active segment, which a real
        // tiered copy never tiers either.
        batches: appended[..exports.len()].to_vec(),
    }
}

/// A restore of a `MinIO`-hosted archive reproduces every batch the bucket
/// holds, at the offset it holds it at.
///
/// Multi-thread on purpose: `S3RemoteStorage`'s sync trait methods bridge to
/// the async store with `block_in_place`, which a current-thread runtime
/// cannot do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_minio_hosted_archive_restores_the_batches_it_holds() {
    let port = free_port();
    let _minio = MinioContainer::start(port);
    make_bucket(port);

    let topic_id = Uuid::new_v4();
    let storage =
        S3RemoteStorage::from_s3_config(&s3_config(port)).expect("open the MinIO archive");
    let archived = [
        archive_partition(
            &storage,
            topic_id,
            0,
            &[&["o0-0", "o0-1"], &["o0-2"], &["o0-3", "o0-4"]],
        ),
        archive_partition(&storage, topic_id, 1, &[&["o1-0", "o1-1"], &["o1-2"]]),
    ];

    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let args = Cli::try_parse_from([
        "krabka-restore",
        "--archive-s3-bucket",
        BUCKET,
        "--archive-s3-endpoint",
        &format!("http://127.0.0.1:{port}"),
        "--archive-s3-access-key-id",
        MINIO_ACCESS_KEY,
        "--archive-s3-secret-access-key",
        MINIO_SECRET_KEY,
        "--archive-s3-allow-http",
        "--archive-prefix",
        PREFIX,
        "--log-dir",
        &log_dir.display().to_string(),
        "--node-id",
        "1",
        "--standalone",
        "--controller-listener",
        "127.0.0.1:9093",
    ])
    .expect("valid command line")
    .args;

    let report = restore(&args).await.expect("restore from MinIO");
    check!(report.skipped.is_empty());

    for expected in &archived {
        let dir = name::partition_dir(&log_dir, TOPIC, expected.partition);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen restored partition");
        check!(
            log.log_start_offset() == Offset(expected.base_offset()),
            "{TOPIC}-{}",
            expected.partition,
        );
        let read = log
            .read(
                Offset(expected.base_offset()),
                LogConfig::default().segment_size,
            )
            .expect("read restored partition");
        check!(
            read.batches == expected.batches,
            "{TOPIC}-{}",
            expected.partition
        );
    }
}
