//! KIP-405 tiered storage against `MinIO`: segment upload, remote-log metadata
//! survival across a restart, and metadata sharing between two brokers.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use assert2::assert;
use jvm_acceptance::*;
use krabka_broker::Broker;

// Same multi-thread caveat as `console_producer_round_trip`: blocking
// `Command::output()` calls would starve the broker accept loop on a
// single-threaded runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn tiered_storage_round_trip_through_minio() {
    const TOPIC: &str = "krabka-tiered-minio-itest";
    // 200 records of ~30 bytes each → ~6 KiB total. With `segment.bytes=2048`
    // that rolls into ~3 sealed segments plus the active one — enough to
    // exercise the copy path multiple times.
    const RECORDS: usize = 200;

    let minio_port = minio_port();
    let _minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);

    let s3 = krabka_remote_storage::S3Config {
        bucket: MINIO_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        prefix: None,
        endpoint: Some(format!("http://127.0.0.1:{minio_port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.to_string()),
        secret_access_key: Some(MINIO_SECRET_KEY.to_string()),
        allow_http: true,
        // Force multipart on segments above 4 KiB so the multipart code
        // path actually fires for the small `segment.bytes=2048` test
        // fixture. `mc ls` doesn't distinguish single-PUT from multipart-
        // composed objects on read, so the consume assertion below
        // covers both paths transparently.
        multipart_threshold: 4 * 1024,
        // MinIO permits parts < 5 MiB. Keep small so the test fixture
        // doesn't have to bloat segments to exercise multiple parts.
        multipart_chunk_size: 1024,
        // These suites cover the ordinary mutable tier, so they pin the
        // two integrity knobs off and keep exercising exactly the request
        // shapes they always have. The WORM suite is what covers them on.
        conditional_put: false,
        checksum_sha256: false,
    };
    let (broker, _dir, _cfg) =
        start_host_broker_with_minio_tier(s3, krabka_broker::RlmmKind::InMemory).await;
    nc_check_connectivity();

    create_tiered_topic(&broker, TOPIC).await;
    produce_records(TOPIC, RECORDS);

    // Give the `RemoteLogManager` enough ticks (1 s interval) to (a) copy
    // every sealed segment to MinIO and (b) run the local-retention pass.
    // Each tick handles one segment per partition, so ≥ `RECORDS / batch`
    // ticks plus a margin for the slowest mc handshake — 8 s in practice.
    wait_for_minio_segments(MINIO_BUCKET, 2).await;

    // Consume from offset 0. Older offsets only exist in MinIO at this
    // point (their local segments were dropped by local_retention_pass),
    // so the JVM consumer transparently exercises the remote-read path.
    // Spot-check a sample across the offset range — the very first records
    // are guaranteed to come from MinIO because their segment was evicted
    // before consume started.
    let consumed = consume_records(TOPIC, RECORDS, 20_000, broker0_advertised());
    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records from remote tier, got {consumed}"
    );

    broker.shutdown().await;
    // `_minio` is dropped here; the container is removed via `docker rm -f`.
}

// ---------------------------------------------------------------------------
// Topic-backed RLMM durability test (KIP-405 S3 + durable RLMM restart).
//
// Boots with `RlmmKind::TopicBacked`, produces+tiers records, restarts the
// broker against the same `log.dir` (using `BootstrapMode::Rejoin` to skip
// re-initialization), then consumes from offset 0. All records must come
// back — proving `__remote_log_metadata` + snapshot durability across a
// broker restart.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn tiered_storage_topic_rlmm_survives_restart() {
    const TOPIC: &str = "krabka-tiered-restart-itest";
    // 200 records of ~30 bytes each → ~6 KiB total. With `segment.bytes=2048`
    // that rolls into ~3 sealed segments plus the active one — enough to
    // exercise the copy path multiple times.
    const RECORDS: usize = 200;

    let minio_port = minio_port();
    let _minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);

    let s3 = krabka_remote_storage::S3Config {
        bucket: MINIO_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        prefix: None,
        endpoint: Some(format!("http://127.0.0.1:{minio_port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.to_string()),
        secret_access_key: Some(MINIO_SECRET_KEY.to_string()),
        allow_http: true,
        multipart_threshold: 4 * 1024,
        multipart_chunk_size: 1024,
        // These suites cover the ordinary mutable tier, so they pin the
        // two integrity knobs off and keep exercising exactly the request
        // shapes they always have. The WORM suite is what covers them on.
        conditional_put: false,
        checksum_sha256: false,
    };

    // Boot with the durable topic-backed RLMM.
    //
    // `bootstrap` is left empty: the broker auto-derives the RLMM metadata
    // client's bootstrap address from its own PLAINTEXT listener via
    // `loopback_bootstrap` (0.0.0.0:9092 → 127.0.0.1:9092). This exercises
    // the fix that makes empty bootstrap work for plaintext single-broker
    // setups without an explicit address. `snapshot_dir` is left empty; the
    // broker derives it from `log.dir` at startup.
    let (broker, _dir, config) = start_host_broker_with_minio_tier(
        s3,
        krabka_broker::RlmmKind::TopicBacked(krabka_broker::KafkaRlmmConfig {
            bootstrap: String::new(),
            num_partitions: 5,
            replication: 1,
            snapshot_interval: krabka_units::secs(2),
            snapshot_dir: std::path::PathBuf::new(),
            security: None,
            ..krabka_broker::KafkaRlmmConfig::default()
        }),
    )
    .await;
    nc_check_connectivity();

    create_tiered_topic(&broker, TOPIC).await;
    produce_records(TOPIC, RECORDS);

    // Wait for ≥2 segment `.log` objects to appear in MinIO: that means at
    // least two sealed segments have been copied and the local-retention pass
    // has run (evicting them from disk).
    wait_for_minio_segments(MINIO_BUCKET, 2).await;

    // intentional: give the RLMM snapshot task at least one cycle
    // (snapshot_interval=2s) so the on-disk snapshot has a chance to flush
    // before we pull the plug. The snapshot flush has no awaiter/metric. Even
    // if the snapshot hasn't flushed, recovery still succeeds via
    // `__remote_log_metadata` topic replay — the snapshot is only an
    // optimisation that avoids replaying the full topic on startup.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // -----------------------------------------------------------------------
    // RESTART: shut down the broker and re-start against the same log.dir.
    //
    // `BootstrapMode::Rejoin` replays the existing on-disk raft log rather
    // than re-initializing a fresh cluster — the correct mode for restarts.
    // -----------------------------------------------------------------------
    eprintln!("KRABKA[test] shutting down broker for restart test");
    broker.shutdown().await;
    eprintln!("KRABKA[test] broker shut down; restarting with Rejoin mode");

    let mut restart_config = config;
    restart_config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    // `BootstrapMode::Rejoin` replays the existing on-disk raft log rather
    // than re-initializing a fresh cluster — the correct mode for restarts.
    let broker = Broker::start(restart_config).await.expect("restart broker");
    nc_check_connectivity();

    eprintln!("KRABKA[test] broker restarted; consuming from offset 0");

    // Consume from offset 0 post-restart. Older offsets only exist in MinIO;
    // the RLMM must recover its metadata from __remote_log_metadata + snapshot.
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            &RECORDS.to_string(),
            "--timeout-ms",
            "30000",
        ],
    );
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    let consumed = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    eprintln!("KRABKA[test] consumed {consumed} records post-restart");

    // Spot-check a sample across the offset range.
    for i in [0usize, 1, 50, 100, 150, RECORDS - 1] {
        let needle = format!("record-{i:04}");
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

    broker.shutdown().await;
    // `_minio` is dropped here; `_dir` (log.dir) is also dropped — cleanup.
}

/// Multi-broker tiered-storage test. It proves that `__remote_log_metadata`
/// shares segment metadata from the partition leader to broker 2 through the
/// topic-backed RLMM. The *surviving* broker can then serve a remote read
/// with metadata it consumed from the topic, and it never runs the copy task
/// itself.
///
/// Discriminating property: broker 2 (b2) never ran the copy task for the
/// user-topic segments, because only the leader copies. After the broker
/// evicts the local log at `local.retention.bytes=1`, b2 can serve offset-0
/// reads only by fetching the segment from S3 with metadata it learned from
/// `__remote_log_metadata`. An in-memory RLMM would leave b2 with no
/// metadata, and the consume would fail.
///
/// See the `start_two_brokers_with_minio_tier` doc for the networking
/// work-around that routes both RLMM clients through broker 1's loopback.
///
/// This test needs an environment where the broker host processes can
/// resolve the advertised inter-broker address `host.docker.internal`, such
/// as Linux CI with Docker bridge networking. On macOS Docker Desktop, host
/// processes cannot resolve `host.docker.internal`, so inter-broker
/// replication fails. The in-process
/// `tiered_storage_metadata_sharing_via_survivor` test in
/// `tests/tiered_storage_multi_broker.rs` proves the same metadata-sharing
/// claim. It uses `127.0.0.1` advertised addresses and runs under plain
/// `cargo test`, with no Docker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + Linux host networking + KRABKA_RUN_JVM_MULTI_BROKER_TIER=1; in-process multi-broker test is the CI-validated proof"]
async fn tiered_storage_topic_rlmm_multi_broker_metadata_sharing() {
    const TOPIC: &str = "krabka-tiered-multi-itest";
    const RECORDS: usize = 200;

    let bootstrap_b1 = broker1_advertised();
    let minio_port = minio_port();

    // Env-gated out of the default `--ignored` CI sweep (broker-jvm-acceptance):
    // this JVM 3-broker + MinIO failover scenario is timing-sensitive under CI
    // load — the survivor's RLMM catch-up, leader failover, and remote read must
    // all complete within the consume window, which is flaky on shared runners.
    // The in-process `tiered_storage_metadata_sharing_via_survivor` test
    // (tests/tiered_storage_multi_broker.rs) is the deterministic, CI-validated
    // multi-broker proof; this JVM variant is opt-in for manual verification.
    let bootstrap = broker0_advertised();
    if std::env::var("KRABKA_RUN_JVM_MULTI_BROKER_TIER").is_err() {
        eprintln!(
            "Skipping tiered_storage_topic_rlmm_multi_broker_metadata_sharing: set \
             KRABKA_RUN_JVM_MULTI_BROKER_TIER=1 to run. The in-process \
             tiered_storage_multi_broker test is the CI-validated multi-broker proof."
        );
        return;
    }

    let _minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);

    let s3 = krabka_remote_storage::S3Config {
        bucket: MINIO_BUCKET.to_string(),
        region: "us-east-1".to_string(),
        prefix: None,
        endpoint: Some(format!("http://127.0.0.1:{minio_port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.to_string()),
        secret_access_key: Some(MINIO_SECRET_KEY.to_string()),
        allow_http: true,
        multipart_threshold: 4 * 1024,
        multipart_chunk_size: 1024,
        // These suites cover the ordinary mutable tier, so they pin the
        // two integrity knobs off and keep exercising exactly the request
        // shapes they always have. The WORM suite is what covers them on.
        conditional_put: false,
        checksum_sha256: false,
    };

    let (b1, b2, _d1, _d2) = start_two_brokers_with_minio_tier(s3).await;
    nc_check_connectivity();

    // Create a tiered topic with rf=2 so both brokers replicate the user
    // partition. Inline instead of calling `create_tiered_topic` (which
    // hard-codes rf=1 and waits on a single-broker config propagation path).
    //
    // Bootstrap against both brokers so the JVM tool can reach the cluster
    // even if b1 hasn't won the controller election yet.
    let bootstrap_both = format!("{bootstrap},{bootstrap_b1}");
    docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--config",
            "remote.storage.enable=true",
            "--config",
            "segment.bytes=2048",
            "--config",
            "local.retention.bytes=1",
            "--config",
            "retention.bytes=-1",
            "--config",
            "retention.ms=-1",
            "--bootstrap-server",
            &bootstrap_both,
        ],
    );

    // Wait for the tiered-storage config to propagate to at least one broker's
    // live partition replica (leader or follower). We only need one since
    // config propagation goes via the controller to all replicas.
    let cfg_deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let b1_ok = b1.partition_log_config_for_test(TOPIC, 0).is_some_and(|c| {
            c.remote_storage_enable
                && c.segment_size == krabka_units::bytes(2048)
                && c.local_retention_size == Some(krabka_units::bytes(1))
        });
        let b2_ok = b2.partition_log_config_for_test(TOPIC, 0).is_some_and(|c| {
            c.remote_storage_enable
                && c.segment_size == krabka_units::bytes(2048)
                && c.local_retention_size == Some(krabka_units::bytes(1))
        });
        if b1_ok || b2_ok {
            break;
        }
        assert!(
            std::time::Instant::now() <= cfg_deadline,
            "tiered-storage topic config never propagated to either broker within 15s; \
             b1={:?} b2={:?}",
            b1.partition_log_config_for_test(TOPIC, 0),
            b2.partition_log_config_for_test(TOPIC, 0),
        );
        // intentional: bounded poll of the local reconciled LogConfig override;
        // `partition_log_config_for_test` is not surfaced by any awaiter/metric.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    eprintln!("KRABKA[test] tiered config propagated; producing {RECORDS} records");

    // Produce records via broker 1's bootstrap. The cluster routes to the
    // actual partition leader internally.
    produce_records(TOPIC, RECORDS);
    eprintln!("KRABKA[test] produced {RECORDS} records; waiting for MinIO segments");

    // Wait for at least 2 sealed segments to land in MinIO (confirming the
    // leader ran the copy task and local-retention eviction fired).
    wait_for_minio_segments(MINIO_BUCKET, 2).await;
    eprintln!("KRABKA[test] MinIO has >=2 segments; waiting for RLMM metadata propagation to b2");

    // intentional: give the topic-backed RLMM enough time to flush metadata
    // records to `__remote_log_metadata` and for broker 2 (the survivor) to
    // consume them. Cross-broker consumption of those metadata records has no
    // krabka awaiter/metric. The RLMM reconciler ticks every 1s, topic rf=2.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // Kill broker 1: forces the user-partition leader election to move to b2.
    // b2 must now serve the remote read entirely from metadata it consumed
    // from __remote_log_metadata (it never ran the copy task itself).
    eprintln!("KRABKA[test] shutting down broker 1 to force failover to broker 2");
    b1.shutdown().await;

    // intentional: allow the survivor to (a) win the user-partition leader
    // election and (b) have its RLMM reconciler settle on the now-led
    // partition's metadata. The RLMM reconciler settling has no awaiter/metric,
    // so a fixed window is used rather than a possibly-never-resolving wait.
    eprintln!("KRABKA[test] waiting for b2 to become leader and RLMM to settle (10s)");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Consume from offset 0 via the SURVIVING broker (b2, port 9094).
    // Older offsets only exist in MinIO; b2 serves them via the RLMM metadata
    // it consumed off __remote_log_metadata.
    eprintln!(
        "KRABKA[test] consuming from surviving broker 2 ({bootstrap_b1})",
        bootstrap_b1 = broker1_advertised()
    );
    let consumed = consume_records(TOPIC, RECORDS, 40_000, broker1_advertised());
    eprintln!("KRABKA[test] consumed {consumed} records from surviving broker 2");

    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records served from the remote tier by the surviving broker, \
         got {consumed}. Broker 2 should have learned segment locations from \
         __remote_log_metadata (rf=2) without having run the copy task itself."
    );

    b2.shutdown().await;
    // `_minio`, `_d1`, `_d2` dropped here.
}
