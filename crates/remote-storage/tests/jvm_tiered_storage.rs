//! Docker-gated proof that Kafka 4.0's real `LocalTieredStorage` reads a
//! Krabka-offloaded segment and that its `ProducerStateManager` accepts the
//! uploaded producer snapshot.
//!
//! ```text
//! cargo test -p krabka-remote-storage --test jvm_tiered_storage -- --ignored --nocapture
//! ```
#![cfg(test)]

use {
    assert2::{assert, check},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    bytes::Bytes,
    krabka_ids::LeaderEpoch,
    krabka_log::{Log, LogConfig, Offset, SegmentExport},
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    krabka_remote_storage::{
        LocalTieredStorage, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
    },
    krabka_units::prelude::bytes,
    sha2::{Digest as _, Sha256},
    std::{
        fmt::Write as _,
        fs, iter,
        path::{Path, PathBuf},
        process::{Command, Output},
    },
    uuid::Uuid,
};

krabka_macros::export_segment_data_fixture!(export_segment_data);

const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";
const KAFKA_STORAGE_TEST_JAR: &str = concat!(
    "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-storage/4.0.0/",
    "kafka-storage-4.0.0-test.jar"
);
const KAFKA_STORAGE_TEST_JAR_SHA256: &str =
    "62da494e4c19303fc38c0099ee350deee8d137817ffb4b06bf873bfcdae9d56d";

const JAVA_PROBE: &str = r#"
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManager;

public class JvmTieredStorageProbe {
    private static byte[] read(InputStream input) throws Exception {
        try (input) {
            return input.readAllBytes();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void main(String[] args) throws Exception {
        Path storageParent = Path.of(args[0]);
        Uuid topicId = Uuid.fromString(args[1]);
        Uuid segmentId = Uuid.fromString(args[2]);
        int segmentSize = Integer.parseInt(args[3]);
        Path localLog = Path.of(args[4]);
        Path localOffsetIndex = Path.of(args[5]);
        Path localTimeIndex = Path.of(args[6]);
        Path localSnapshot = Path.of(args[7]);
        Path localLeaderEpoch = Path.of(args[8]);

        TopicIdPartition partition = new TopicIdPartition(
            topicId,
            new TopicPartition("orders", 0)
        );
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(partition, segmentId),
            0L,
            1L,
            1234L,
            1,
            0L,
            segmentSize,
            Map.of(0, 0L),
            true
        );

        LocalTieredStorage storage = new LocalTieredStorage();
        Map<String, Object> config = new HashMap<>();
        config.put(LocalTieredStorage.STORAGE_DIR_CONFIG, storageParent.toString());
        config.put(LocalTieredStorage.BROKER_ID, 1);
        storage.configure(config);

        require(
            java.util.Arrays.equals(
                read(storage.fetchLogSegment(metadata, 0)),
                Files.readAllBytes(localLog)
            ),
            "JVM LocalTieredStorage read different segment bytes"
        );
        require(
            java.util.Arrays.equals(
                read(storage.fetchIndex(metadata, IndexType.OFFSET)),
                Files.readAllBytes(localOffsetIndex)
            ),
            "JVM LocalTieredStorage read different offset-index bytes"
        );
        require(
            java.util.Arrays.equals(
                read(storage.fetchIndex(metadata, IndexType.TIMESTAMP)),
                Files.readAllBytes(localTimeIndex)
            ),
            "JVM LocalTieredStorage read different time-index bytes"
        );
        require(
            java.util.Arrays.equals(
                read(storage.fetchIndex(metadata, IndexType.LEADER_EPOCH)),
                Files.readAllBytes(localLeaderEpoch)
            ),
            "JVM LocalTieredStorage read different leader-epoch bytes"
        );

        byte[] snapshot = read(storage.fetchIndex(metadata, IndexType.PRODUCER_SNAPSHOT));
        require(
            java.util.Arrays.equals(snapshot, Files.readAllBytes(localSnapshot)),
            "JVM LocalTieredStorage read different producer-snapshot bytes"
        );
        Path snapshotCopy = Files.createTempFile("krabka-producer-", ".snapshot");
        Files.write(snapshotCopy, snapshot);
        List<ProducerStateEntry> entries = ProducerStateManager.readSnapshot(
            snapshotCopy.toFile()
        );
        require(entries.size() == 1, "expected one producer snapshot entry");
        ProducerStateEntry entry = entries.get(0);
        require(entry.producerId() == 42L, "producer id mismatch");
        require(entry.producerEpoch() == 3, "producer epoch mismatch");
        require(entry.lastSeq() == 8, "last sequence mismatch");
        require(entry.lastDataOffset() == 1L, "last offset mismatch");
        require(entry.lastOffsetDelta() == 1, "offset delta mismatch");
        require(entry.lastTimestamp() == 1234L, "timestamp mismatch");
        require(entry.coordinatorEpoch() == -1, "coordinator epoch mismatch");
        require(
            entry.currentTxnFirstOffset().orElseThrow() == 0L,
            "transaction first offset mismatch"
        );
        Files.delete(snapshotCopy);
        storage.close();
        System.out.println("JVM LocalTieredStorage and ProducerStateManager accepted Krabka data");
    }
}
"#;

struct DockerContainer(String);

impl Drop for DockerContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.0])
            .output();
    }
}

fn run(command: &mut Command, description: &str) -> Output {
    let output = command.output().unwrap_or_else(|error| {
        panic!("failed to start {description}: {error}");
    });
    assert!(
        output.status.success(),
        "{description} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

fn kafka_libs(destination: &Path) {
    let output = run(
        Command::new("docker").args(["create", KAFKA_IMAGE]),
        "docker create Kafka 4.0",
    );
    let id = String::from_utf8(output.stdout)
        .expect("container id is UTF-8")
        .trim()
        .to_owned();
    let container = DockerContainer(id);
    run(
        Command::new("docker").args([
            "cp",
            &format!("{}:/opt/kafka/libs/.", container.0),
            &destination.display().to_string(),
        ]),
        "docker cp Kafka jars",
    );
}

fn download_test_jar(destination: &Path) {
    run(
        Command::new("curl").args([
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            KAFKA_STORAGE_TEST_JAR,
            "--output",
            &destination.display().to_string(),
        ]),
        "download Kafka storage test jar",
    );
    let bytes = fs::read(destination).expect("read downloaded Kafka test jar");
    let mut digest = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(digest, "{byte:02x}").expect("writing to String cannot fail");
    }
    assert!(
        digest == KAFKA_STORAGE_TEST_JAR_SHA256,
        "Kafka storage test jar checksum mismatch: expected {KAFKA_STORAGE_TEST_JAR_SHA256}, got {digest}"
    );
}

fn producer_batch() -> RecordBatch {
    RecordBatch {
        producer_id: 42,
        producer_epoch: 3,
        base_sequence: 7,
        base_timestamp: 1234,
        max_timestamp: 1234,
        last_offset_delta: 1,
        attributes: Attributes::default().with_transactional(true),
        records: vec![
            Record {
                offset_delta: 0,
                value: Some(Bytes::from_static(b"one")),
                ..Record::default()
            },
            Record {
                offset_delta: 1,
                value: Some(Bytes::from_static(b"two")),
                ..Record::default()
            },
        ],
        ..RecordBatch::default()
    }
}

fn ordinary_batch() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"roll")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

#[derive(Default)]
struct JvmExportScenario {
    local: Option<tempfile::TempDir>,
    log: Option<Log>,
    export: Option<SegmentExport>,
    shared: Option<tempfile::TempDir>,
    storage_root: PathBuf,
    leader_epoch_path: PathBuf,
    work: Option<tempfile::TempDir>,
    classpath: String,
    source: PathBuf,
    output: Option<Output>,
}

#[rstest::fixture]
fn jvm_export() -> JvmExportScenario {
    let local = None;
    JvmExportScenario {
        local,
        ..JvmExportScenario::default()
    }
}

#[rstest_bdd_macros::given("a sealed Krabka segment containing an idempotent producer batch")]
fn sealed_producer_segment(jvm_export: &mut JvmExportScenario) {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    })
    .expect("open local log");
    log.append(&mut producer_batch())
        .expect("append producer batch");
    log.append(&mut ordinary_batch())
        .expect("roll producer segment");
    log.sync().expect("sync sealed segment");
    jvm_export.export = Some(
        log.tierable_segments()
            .into_iter()
            .next()
            .expect("one sealed segment"),
    );
    jvm_export.local = Some(local);
    jvm_export.log = Some(log);
}

#[rstest_bdd_macros::given(
    "the segment and producer snapshot are copied to Kafka local tiered storage"
)]
fn copied_producer_segment(jvm_export: &mut JvmExportScenario) {
    let local = jvm_export.local.as_ref().expect("local segment given");
    let export = jvm_export.export.as_ref().expect("sealed segment given");
    let details = RemoteLogSegmentDetails::new(
        i32::try_from(
            export
                .log_path
                .metadata()
                .expect("sealed log metadata")
                .len(),
        )
        .expect("segment size fits metadata"),
        RemoteLogSegmentState::CopySegmentStarted,
        iter::once((LeaderEpoch(0), export.base_offset.0)).collect(),
    );
    let metadata = RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
            Uuid::from_u128(0xfe),
        ),
        export.base_offset.0,
        export.last_offset.0,
        export.max_timestamp,
        1,
        0,
        details,
    )
    .expect("valid remote metadata");
    let shared = tempfile::tempdir().expect("shared storage tempdir");
    jvm_export.storage_root = shared.path().join("kafka-tiered-storage");
    let storage = LocalTieredStorage::new(&jvm_export.storage_root);
    jvm_export.leader_epoch_path = local.path().join("remote-leader-epoch-checkpoint");
    fs::write(&jvm_export.leader_epoch_path, b"0\n1\n0 0\n").expect("leader epoch checkpoint");
    storage
        .copy_log_segment_data(
            &metadata,
            &export_segment_data(export, ProducerSnapshotExport::Include, || {
                Bytes::from_static(b"0\n1\n0 0\n")
            }),
        )
        .expect("Krabka local-tier copy");
    jvm_export.shared = Some(shared);
}

#[rstest_bdd_macros::given("Kafka 4.0 tiered-storage JVM tools")]
fn kafka_tiered_tools(jvm_export: &mut JvmExportScenario) {
    let work = tempfile::tempdir().expect("JVM work tempdir");
    let libs = work.path().join("libs");
    fs::create_dir(&libs).expect("Kafka library directory");
    kafka_libs(&libs);
    let test_jar = work.path().join("kafka-storage-4.0.0-test.jar");
    download_test_jar(&test_jar);
    jvm_export.source = work.path().join("JvmTieredStorageProbe.java");
    fs::write(&jvm_export.source, JAVA_PROBE).expect("JVM compatibility probe source");
    jvm_export.classpath = format!("{}/*:{}", libs.display(), test_jar.display());
    jvm_export.work = Some(work);
}

#[rstest_bdd_macros::when("Kafka reads the offloaded segment and restores its producer state")]
fn kafka_reads_export(jvm_export: &mut JvmExportScenario) {
    let export = jvm_export.export.as_ref().expect("sealed segment given");
    let work = jvm_export.work.as_ref().expect("Kafka JVM tools given");
    let shared = jvm_export.shared.as_ref().expect("local tier copy given");
    let segment_size = fs::metadata(&export.log_path)
        .expect("sealed log metadata")
        .len()
        .to_string();
    jvm_export.output = Some(
        Command::new("java")
            .current_dir(work.path())
            .arg("--class-path")
            .arg(&jvm_export.classpath)
            .arg(&jvm_export.source)
            .arg(shared.path())
            .arg(URL_SAFE_NO_PAD.encode(Uuid::from_u128(1).as_bytes()))
            .arg(URL_SAFE_NO_PAD.encode(Uuid::from_u128(0xfe).as_bytes()))
            .arg(segment_size)
            .arg(&export.log_path)
            .arg(&export.offset_index_path)
            .arg(&export.time_index_path)
            .arg(&export.producer_snapshot_path)
            .arg(&jvm_export.leader_epoch_path)
            .output()
            .expect("launch Kafka JVM tiered-storage probe"),
    );
}

#[rstest_bdd_macros::then("Kafka accepts the segment bytes indexes and producer snapshot")]
fn kafka_accepted_export(jvm_export: &JvmExportScenario) {
    let export = jvm_export.export.as_ref().expect("sealed segment given");
    check!(export.base_offset == Offset(0));
    check!(export.last_offset == Offset(1));
    check!(export.producer_snapshot_path.is_file());
    let output = jvm_export.output.as_ref().expect("JVM probe ran");
    check!(
        output.status.success(),
        "Kafka JVM tiered-storage probe failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    check!(
        stdout.contains("LocalTieredStorage and ProducerStateManager accepted"),
        "unexpected JVM output: {stdout}"
    );
    check!(
        jvm_export
            .storage_root
            .join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ")
            .join(concat!(
                "00000000000000000000-AAAAAAAAAAAAAAAAAAAA_g",
                ".snapshot"
            ))
            .is_file()
    );
}

#[ignore = "requires Docker, a JDK, and Maven Central"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/jvm_tiered_compatibility/jvm_tiered_compatibility.feature",
    name = "Kafka reads Krabka's offloaded producer segment"
)]
fn kafka_reads_krabka_local_tiered_segment_and_producer_snapshot(jvm_export: JvmExportScenario) {}
