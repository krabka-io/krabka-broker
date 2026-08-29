//! Topic setup and the JVM produce/consume round trip the tiered-storage
//! suites share.
//!
//! Every helper here is shaped by KIP-405: the topic carries the remote-storage
//! overrides, the producer forces a segment roll, and the consumer reads back
//! offsets whose local segments are already evicted.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;

use super::{
    docker::{KAFKA_IMAGE, KAFKA_IMAGE_TIERED, docker_run_kafka_tool_with_image},
    minio::minio_list_objects,
    ports::broker0_advertised,
};

/// Create a KIP-405 tiered topic and wait for the config overrides to propagate
/// into the partition's `LogConfig`.
///
/// This function uses `segment.bytes=2048` and `local.retention.bytes=1`, so
/// a small produce batch seals several segments and the broker evicts every
/// copied segment from local disk at once. Later reads must then go through
/// the remote tier.
///
/// The function waits up to 10 s for `ReplicatorSupervisor::reconcile` to
/// apply the config to the live partition. Without this gate, the producer's
/// first batches land in a default-config `Log` with 1 GiB segments and
/// `remote_storage_enable=false`, and nothing triggers the tier-copy path.
/// See `compact_log_cleaner_round_trip` for the same pattern.
pub(crate) async fn create_tiered_topic(broker: &krabka_broker::BrokerHandle, topic: &str) {
    // Uses the KIP-405-aware `cp-kafka:7.8.8` image — older clients' `TopicCommand`
    // validates `--config` keys client-side and rejects `remote.storage.enable` /
    // `local.retention.bytes` before the request leaves the container.
    docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
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
            broker0_advertised(),
        ],
    );

    let cfg_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(topic, 0)
            && cfg.remote_storage_enable
            && cfg.segment_size == krabka_units::bytes(2048)
            && cfg.local_retention_size == Some(krabka_units::bytes(1))
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= cfg_deadline,
            "tiered-storage topic config never propagated within 10s; saw {:?}",
            broker.partition_log_config_for_test(topic, 0)
        );
        // intentional: bounded poll of the local reconciled LogConfig override;
        // `partition_log_config_for_test` is not surfaced by any awaiter/metric.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Stream `n` records with the format `record-NNNN` into `topic` through the
/// JVM console producer.
///
/// This function forces per-record batches with `batch.size=1` and
/// `linger.ms=0`, so the broker rolls segments at `segment.bytes=2048`.
/// Without that, the JVM producer collects everything into one large batch
/// and writes it into a single segment. Nothing then triggers a segment
/// roll, and the tier-copy path gets no work.
pub(crate) fn produce_records(topic: &str, n: usize) {
    let mut payload = String::with_capacity(n * 12);
    for i in 0..n {
        use std::fmt::Write as _;
        let _ = writeln!(payload, "record-{i:04}");
    }
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            topic,
            "--producer-property",
            "batch.size=1",
            "--producer-property",
            "linger.ms=0",
            "--producer-property",
            "max.in.flight.requests.per.connection=1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );
}

/// Poll `mc ls --recursive local/<bucket>` until it lists at least
/// `min_log_objects` entries whose path ends with `.log`, then return the
/// full listing.
///
/// The poll runs at 500 ms intervals for up to 20 s (40 iterations). It
/// panics if the listing never reaches the threshold.
pub(crate) async fn wait_for_minio_segments(bucket: &str, min_log_objects: usize) -> String {
    let mut bucket_listing = String::new();
    let mut copied_log_objects = 0usize;
    for _ in 0..40 {
        // intentional: bounded poll of an external process (MinIO via `mc ls`);
        // no krabka metric reflects object arrival in the bucket.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        bucket_listing = minio_list_objects(bucket);
        copied_log_objects = bucket_listing
            .lines()
            .filter(|line| {
                std::path::Path::new(line)
                    .extension()
                    .is_some_and(|extension| extension == "log")
            })
            .count();
        if copied_log_objects >= min_log_objects {
            return bucket_listing;
        }
    }
    panic!(
        "expected ≥{min_log_objects} segment `.log` objects in MinIO; \
         saw {copied_log_objects}. Bucket listing:\n{bucket_listing}"
    );
}

/// Consume up to `max` records from `topic` (partition 0, from-beginning)
/// with the JVM console consumer. Returns the number of non-empty output
/// lines.
///
/// `bootstrap_host_port` is the Kafka bootstrap address that is visible from
/// inside the Docker container, for example an allocated port.
/// Single-broker callers should pass `broker0_advertised()`.
pub(crate) fn consume_records(
    topic: &str,
    max: usize,
    timeout_ms: u64,
    bootstrap_host_port: &str,
) -> usize {
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap_host_port,
            "--topic",
            topic,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            &max.to_string(),
            "--timeout-ms",
            &timeout_ms.to_string(),
        ],
    );
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    stdout.lines().filter(|l| !l.trim().is_empty()).count()
}
