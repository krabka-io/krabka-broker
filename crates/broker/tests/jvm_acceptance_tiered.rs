//! KIP-405 tiered storage against `MinIO`: segment upload and the remote read
//! that follows local eviction, the KIP-950 disable path through
//! `kafka-configs`, remote-log metadata survival across a broker restart, and a
//! point-in-time restore of the bucket read back by the JVM consumer.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.
#![cfg(test)]

mod jvm_acceptance;
mod support;

use {
    assert2::{assert, check},
    jvm_acceptance::*,
    krabka_broker::{BootstrapMode, Broker, BrokerHandle, KafkaRlmmConfig, RlmmKind},
    std::{
        mem,
        path::{Path, PathBuf},
        process::Output,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
};

// 200 records of ~30 bytes seal several segments at TIERED_SEGMENT_SIZE.
const RECORDS: usize = 200;

/// A tiered topic on a running broker: the `MinIO` port and container, the
/// broker, its directory and the config it started from.
struct MinioTopic {
    port: u16,
    minio: MinioContainer,
    broker: krabka_broker::BrokerHandle,
    dir: tempfile::TempDir,
    config: krabka_broker::BrokerConfig,
}

async fn start_minio_topic(topic: &str, rlmm: krabka_broker::RlmmKind) -> MinioTopic {
    let port = minio_port();
    let minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);
    let s3 = mutable_minio_config(port);
    let started = start_host_broker_with_minio_tier(s3, rlmm).await;
    nc_check_connectivity();
    create_tiered_topic(&started.broker, topic).await;
    produce_records(topic, RECORDS);
    MinioTopic {
        port,
        minio,
        broker: started.broker,
        dir: started.dir,
        config: started.config,
    }
}

// Same multi-thread caveat as `console_producer_round_trip`: blocking
// `Command::output()` calls would starve the broker accept loop on a
// single-threaded runtime.

// ---------------------------------------------------------------------------
// Topic-backed RLMM durability test (KIP-405 S3 + durable RLMM restart).
//
// Boots with `RlmmKind::TopicBacked`, produces+tiers records, restarts the
// broker against the same `log.dir` (using `BootstrapMode::Rejoin` to skip
// re-initialization), then consumes from offset 0. All records must come
// back — proving `__remote_log_metadata` + snapshot durability across a
// broker restart.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// KFC-3 point-in-time restore, read back by the JVM console consumer.
//
// The other suites in this file archive segments and read them back through
// the broker that wrote them. This one throws that broker away: after the
// segments are in MinIO it shuts the cluster down, points `krabka-restore` at
// the same bucket, and boots a broker on the directory the restore built. The
// JVM consumer then reads that cluster with `--from-beginning` and no
// knowledge that it is not the original.
//
// That is the client-visible half of KFC-3's "Offsets Are the Contract": a
// stock consumer, not a krabka test client, gets the archived records back at
// the archived offsets. `crates/restore/tests/roundtrip/consume.rs` makes the
// same claim against krabka's own client and checks the exact wire answers;
// this one proves a JVM client is satisfied by them.
// ---------------------------------------------------------------------------

/// The `--archive-s3-*` flags that point `krabka-restore` at the same `MinIO`
/// bucket the broker archived into.
fn restore_argv(minio_port: u16, log_dir: &Path) -> Vec<String> {
    [
        "krabka-restore",
        "--archive-s3-bucket",
        MINIO_BUCKET,
        "--archive-s3-region",
        "us-east-1",
        "--archive-s3-endpoint",
        &format!("http://127.0.0.1:{minio_port}"),
        "--archive-s3-access-key-id",
        MINIO_ACCESS_KEY,
        "--archive-s3-secret-access-key",
        MINIO_SECRET_KEY,
        "--archive-s3-allow-http",
        "--log-dir",
        &log_dir.display().to_string(),
        "--node-id",
        "1",
        "--standalone",
        "--controller-listener",
        controller_addr_0(),
    ]
    .iter()
    .map(|arg| (*arg).to_owned())
    .collect()
}

fn mutable_minio_config(port: u16) -> krabka_remote_storage::S3Config {
    krabka_remote_storage::S3Config {
        bucket: MINIO_BUCKET.into(),
        region: "us-east-1".into(),
        prefix: None,
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.into()),
        secret_access_key: Some(MINIO_SECRET_KEY.into()),
        allow_http: true,
        // Force multipart on these small segment fixtures; MinIO permits small parts.
        multipart_threshold: 4 * 1024,
        multipart_chunk_size: 1024,
        conditional_put: false,
        checksum_sha256: false,
        ..krabka_remote_storage::S3Config::default()
    }
}

#[derive(Default)]
struct JvmTiered {
    source: Option<MinioTopic>,
    topic: String,
    minio: Option<MinioContainer>,
    port: Option<u16>,
    source_directory: Option<TempDir>,
    restarted: Option<BrokerHandle>,
    restored_host: Option<(BrokerHandle, TempDir)>,
    target: Option<TempDir>,
    restored_log_dir: PathBuf,
    consumed: Option<usize>,
    altered: Option<Output>,
    console_output: Option<Output>,
    restored_count: Option<usize>,
    values: Vec<String>,
}

#[rstest::fixture]
fn jvm_tiered() -> JvmTiered {
    // The archive remains alive when the source log is restarted or deliberately discarded.
    JvmTiered::default()
}

impl JvmTiered {
    fn broker(&self) -> &BrokerHandle {
        self.restarted
            .as_ref()
            .or_else(|| self.restored_host.as_ref().map(|(broker, _)| broker))
            .unwrap_or_else(|| &self.source.as_ref().expect("MinIO source").broker)
    }

    async fn shutdown(&mut self) {
        let Self {
            source,
            minio,
            source_directory,
            restarted,
            restored_host,
            target,
            ..
        } = mem::take(self);
        if let Some(source) = source {
            let MinioTopic {
                broker, dir, minio, ..
            } = source;
            broker.shutdown().await;
            drop((dir, minio));
        }
        if let Some(broker) = restarted {
            broker.shutdown().await;
        }
        if let Some((broker, directory)) = restored_host {
            broker.shutdown().await;
            drop(directory);
        }
        drop((source_directory, target, minio));
    }
}

#[rstest_bdd_macros::given(
    "a readable MinIO tier with in-memory metadata has 200 records in topic {topic}"
)]
async fn in_memory_minio_topic(jvm_tiered: &mut JvmTiered, topic: String) {
    jvm_tiered.topic = topic;
    jvm_tiered.source = Some(start_minio_topic(&jvm_tiered.topic, RlmmKind::InMemory).await);
}

#[rstest_bdd_macros::given(
    "a readable MinIO tier has 200 records in topic {topic} with five RF1 metadata partitions, min ISR1, two-second snapshots and auto-derived bootstrap and snapshot directory"
)]
async fn durable_minio_topic(jvm_tiered: &mut JvmTiered, topic: String) {
    jvm_tiered.topic = topic;
    jvm_tiered.source = Some(
        start_minio_topic(
            &jvm_tiered.topic,
            RlmmKind::TopicBacked(KafkaRlmmConfig {
                bootstrap: String::new(),
                num_partitions: 5,
                replication: 1,
                min_isr: 1,
                snapshot_interval: krabka_units::secs(2),
                snapshot_dir: PathBuf::new(),
                security: None,
                ..KafkaRlmmConfig::default()
            }),
        )
        .await,
    );
}

#[rstest_bdd_macros::then("MinIO contains at least two sealed segment log objects")]
async fn copied_minio_segments() { wait_for_minio_segments(MINIO_BUCKET, 2).await; }

#[rstest_bdd_macros::when(
    "the JVM consumer reads up to 200 records from offset 0 with a 20000-millisecond timeout"
)]
fn consume_minio_topic(jvm_tiered: &mut JvmTiered) {
    jvm_tiered.consumed = Some(consume_records(
        &jvm_tiered.topic,
        RECORDS,
        20_000,
        broker0_advertised(),
    ));
}

#[rstest_bdd_macros::then("the JVM consumer returns at least 200 records through the remote tier")]
fn complete_minio_topic(jvm_tiered: &JvmTiered) {
    let consumed = jvm_tiered.consumed.expect("MinIO records consumed");
    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records from remote tier, got {consumed}"
    );
}

#[rstest_bdd_macros::when("kafka-configs alters the topic with {config}")]
fn alter_minio_topic(jvm_tiered: &mut JvmTiered, config: String) {
    jvm_tiered.altered = Some(docker_run_kafka_tool_allowing_failure(&[
        "kafka-configs",
        "--bootstrap-server",
        broker0_advertised(),
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        &jvm_tiered.topic,
        "--add-config",
        &config,
    ]));
}

#[rstest_bdd_macros::then(
    "the bare disable fails and names both remote.log.delete.on.disable=true and remote.log.copy.disable=true"
)]
fn refused_minio_disable(jvm_tiered: &JvmTiered) {
    let refused = jvm_tiered
        .altered
        .as_ref()
        .expect("topic alteration attempted");
    assert!(
        !refused.status.success(),
        "the bare flip must be refused: {}",
        tool_output(refused)
    );
    let refusal = tool_output(refused);
    check!(
        refusal.contains("remote.log.delete.on.disable=true"),
        "the refusal must name the delete-on-disable way out: {refusal}"
    );
    check!(
        refusal.contains("remote.log.copy.disable=true"),
        "the refusal must name the read-only tier: {refusal}"
    );
}

#[rstest_bdd_macros::then("the disable with delete-on-disable succeeds")]
fn accepted_minio_disable(jvm_tiered: &JvmTiered) {
    let accepted = jvm_tiered
        .altered
        .as_ref()
        .expect("topic alteration attempted");
    assert!(
        accepted.status.success(),
        "the flip with delete-on-disable must be accepted: {}",
        tool_output(accepted)
    );
}

#[rstest_bdd_macros::then(
    "within thirty seconds the bucket no longer contains the topic and its global log start is greater than zero"
)]
async fn deleted_disabled_minio_tier(jvm_tiered: &JvmTiered) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut listing = minio_list_objects(MINIO_BUCKET);
    while listing.contains(&jvm_tiered.topic) && Instant::now() < deadline {
        time::sleep(Duration::from_millis(500)).await;
        listing = minio_list_objects(MINIO_BUCKET);
    }
    check!(
        !listing.contains(&jvm_tiered.topic),
        "the disabled topic's segments must be gone from the bucket: {listing}"
    );
    check!(
        jvm_tiered
            .broker()
            .partition_log_start_for_test(&jvm_tiered.topic, 0)
            .is_some_and(|start| start > 0),
        "the log start offset must have risen to the local log start"
    );
}

#[rstest_bdd_macros::when(
    "three seconds allow a metadata snapshot and the broker shuts down then rejoins on the same log directory"
)]
async fn restart_durable_minio_broker(jvm_tiered: &mut JvmTiered) {
    time::sleep(Duration::from_secs(3)).await;
    let MinioTopic {
        port,
        minio,
        broker,
        dir,
        mut config,
    } = jvm_tiered.source.take().expect("MinIO source");
    jvm_tiered.port = Some(port);
    jvm_tiered.minio = Some(minio);
    jvm_tiered.source_directory = Some(dir);
    broker.shutdown().await;
    config.bootstrap_mode = BootstrapMode::Rejoin;
    jvm_tiered.restarted = Some(Broker::start(config).await.expect("restart broker"));
    nc_check_connectivity();
}

#[rstest_bdd_macros::when(
    "the JVM console consumer reads 200 records from the restarted broker's partition 0 beginning with a 30000-millisecond timeout"
)]
fn consume_restarted_minio_topic(jvm_tiered: &mut JvmTiered) {
    jvm_tiered.console_output = Some(docker_run_kafka_tool_with_image(KAFKA_IMAGE_TIERED, &[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        &jvm_tiered.topic,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        &RECORDS.to_string(),
        "--timeout-ms",
        "30000",
    ]));
}

#[rstest_bdd_macros::then(
    "the restarted consumer returns at least 200 records including record-0000,0001,0050,0100,0150,0199"
)]
fn recovered_minio_metadata_records(jvm_tiered: &JvmTiered) {
    let stdout = String::from_utf8_lossy(
        &jvm_tiered
            .console_output
            .as_ref()
            .expect("restarted topic consumed")
            .stdout,
    );
    let consumed = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    for index in [0usize, 1, 50, 100, 150, RECORDS - 1] {
        let needle = format!("record-{index:04}");
        assert!(
            stdout.contains(&needle),
            "consumer missing {needle} post-restart; partial output:\n{}",
            stdout.chars().take(2_000).collect::<String>()
        );
    }
    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records from remote tier after restart, got {consumed}"
    );
}

#[rstest_bdd_macros::then(
    "MinIO contains at least two complete segments and the copy task has no remaining work"
)]
async fn settled_minio_archive() { wait_for_settled_minio_segments(MINIO_BUCKET, 2).await; }

#[rstest_bdd_macros::when(
    "the source broker shuts down and its entire local log directory is discarded"
)]
async fn discard_minio_source(jvm_tiered: &mut JvmTiered) {
    let MinioTopic {
        port,
        minio,
        broker,
        dir,
        ..
    } = jvm_tiered.source.take().expect("MinIO source");
    jvm_tiered.port = Some(port);
    jvm_tiered.minio = Some(minio);
    broker.shutdown().await;
    drop(dir);
}

#[rstest_bdd_macros::when(
    "the restore command reads only the MinIO archive into a new standalone log directory"
)]
async fn restore_minio_archive(jvm_tiered: &mut JvmTiered) {
    jvm_tiered.target = Some(tempfile::tempdir().expect("restore target parent"));
    jvm_tiered.restored_log_dir = jvm_tiered
        .target
        .as_ref()
        .expect("restore parent")
        .path()
        .join("restored");
    let report = krabka_restore::restore(
        &<krabka_restore::Cli as clap::Parser>::try_parse_from(restore_argv(
            jvm_tiered.port.expect("MinIO port"),
            &jvm_tiered.restored_log_dir,
        ))
        .expect("valid restore command line")
        .args,
    )
    .await
    .expect("restore the MinIO archive");
    jvm_tiered.restored_count = Some(
        report
            .partitions
            .iter()
            .flat_map(|partition| &partition.segments)
            .map(|segment| usize::try_from(segment.records_kept).expect("small count"))
            .sum(),
    );
}

#[rstest_bdd_macros::then(
    "the restore recovers between 1 and 200 archived records, excluding any untiered active tail"
)]
fn restored_minio_record_count(jvm_tiered: &JvmTiered) {
    let restored = jvm_tiered.restored_count.expect("archive restored");
    assert!(
        restored > 0 && restored <= RECORDS,
        "restore recovered {restored} records from the bucket, expected 1..={RECORDS}"
    );
}

#[rstest_bdd_macros::when(
    "a new broker boots on the restore output and the JVM consumer reads exactly the archived record count from beginning"
)]
async fn consume_restored_minio_cluster(jvm_tiered: &mut JvmTiered) {
    let restored_log_dir = jvm_tiered.restored_log_dir.clone();
    jvm_tiered.restored_host = Some(
        start_host_broker_with(move |config| {
            config.log_dir = restored_log_dir;
        })
        .await,
    );
    nc_check_connectivity();
    jvm_tiered.values = consume_record_values(
        &jvm_tiered.topic,
        jvm_tiered.restored_count.expect("archive restored"),
        30_000,
        broker0_advertised(),
    );
}

#[rstest_bdd_macros::then(
    "the consumer returns the archive's full record count in uninterrupted record-0000 order"
)]
fn consumed_restored_minio_records(jvm_tiered: &JvmTiered) {
    let restored = jvm_tiered.restored_count.expect("archive restored");
    check!(
        jvm_tiered.values.len() == restored,
        "expected the {restored} archived records from the restored cluster, got {}",
        jvm_tiered.values.len()
    );
    let expected: Vec<String> = (0..restored)
        .map(|index| format!("record-{index:04}"))
        .collect();
    check!(jvm_tiered.values == expected);
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_tiered.feature",
    name = "tiered storage round trip through minio"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiered_storage_round_trip_through_minio(mut jvm_tiered: JvmTiered) {
    jvm_tiered.shutdown().await;
    drop(jvm_tiered);
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_tiered.feature",
    name = "tiered storage disable needs delete on disable"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiered_storage_disable_needs_delete_on_disable(mut jvm_tiered: JvmTiered) {
    jvm_tiered.shutdown().await;
    drop(jvm_tiered);
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_tiered.feature",
    name = "tiered storage topic rlmm survives restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiered_storage_topic_rlmm_survives_restart(mut jvm_tiered: JvmTiered) {
    jvm_tiered.shutdown().await;
    drop(jvm_tiered);
}

#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_tiered.feature",
    name = "restored cluster serves the jvm console consumer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_cluster_serves_the_jvm_console_consumer(mut jvm_tiered: JvmTiered) {
    jvm_tiered.shutdown().await;
    drop(jvm_tiered);
}
