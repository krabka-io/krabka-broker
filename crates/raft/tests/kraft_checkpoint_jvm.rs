//! Docker-gated: a Krabka engine-produced KIP-630 snapshot (built from a real
//! `MetadataImage` through the KIP-631 translation boundary) is parsed cleanly
//! by the JVM `kafka-dump-log --cluster-metadata-decoder`, proving the
//! on-checkpoint bytes are genuine KIP-631 records (`RegisterBroker` / `Topic` /
//! `Partition` / `Config`), not Krabka-private wincode, and that the header
//! names the create-time of the last batch the snapshot contains rather than
//! the epoch.
//!
//! ```text
//! cargo test -p krabka-raft --test kraft_checkpoint_jvm -- --ignored --nocapture
//! ```

use std::{io::Write as _, process::Command};

use assert2::check;
use krabka_ids::Offset;
use krabka_metadata::{
    BrokerConfigRecord, BrokerRegistrationRecord, LeaderEpoch, MetadataImage, MetadataRecord,
    NodeId, PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_raft::{kraft::KraftLog, serialize_metadata_snapshot};
use uuid::Uuid;

/// The create-time stamped on the metadata batch the snapshot below contains:
/// a 2023 instant, so a header that still carried the KIP-630 default would
/// print 1970 and fail the check on the dump output.
const APPEND_TIMESTAMP_MS: i64 = 1_700_000_000_123;

/// A metadata log holding one committed batch stamped with
/// [`APPEND_TIMESTAMP_MS`], and the timestamp the engine reads back out of it
/// for a snapshot header taken at the high watermark.
fn committed_log_timestamp(dir: &std::path::Path) -> i64 {
    let mut log = KraftLog::open(dir).expect("open the metadata log");
    let mut batch = RecordBatch {
        partition_leader_epoch: 1,
        records: vec![Record {
            value: Some(bytes::Bytes::from_static(b"a committed metadata batch")),
            ..Default::default()
        }],
        ..Default::default()
    };
    log.append(&mut batch, APPEND_TIMESTAMP_MS)
        .expect("append the batch the snapshot contains");
    log.advance_hwm(Offset(1));
    log.last_committed_timestamp_ms()
        .expect("the committed batch has a create-time")
}

#[test]
#[ignore = "requires Docker"]
fn jvm_dump_log_parses_engine_snapshot() {
    let cid = Uuid::new_v4();
    let mut image = MetadataImage::new(cid);
    // RegisterBroker (apiKey 0).
    image.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            node_id: NodeId(1),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
            host: "broker-1".into(),
            port: 9092,
            rack: Some("rack-a".into()),
            log_dirs: vec![],
            endpoints: vec![],
            features: std::collections::BTreeMap::new(),
        },
    ));
    // Config (apiKey 4), broker scope.
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: NodeId(1),
        config_name: "leader.replication.throttled.rate".into(),
        config_value: Some("1048576".into()),
    }));
    // Topic (apiKey 2) + Partition (apiKey 3) ×2.
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: Uuid::new_v4(),
        partitions: 2,
        replication_factor: 1,
    }));
    for p in 0..2 {
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition: p,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }
    // Config (apiKey 4), topic scope.
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: [("retention.ms".to_string(), "604800000".to_string())].into(),
    }));

    // The header timestamp travels the engine's own route: stamped onto the
    // batch at append, read back off the last batch the snapshot contains, and
    // written into `SnapshotHeaderRecord.lastContainedLogTimestamp`.
    let log_dir = tempfile::tempdir().expect("tempdir");
    let last_contained_log_timestamp = committed_log_timestamp(log_dir.path());
    check!(last_contained_log_timestamp == APPEND_TIMESTAMP_MS);

    let bytes = serialize_metadata_snapshot(&image, last_contained_log_timestamp).unwrap();
    let dir = tempfile::tempdir().expect("tempdir");
    // kafka-dump-log infers the snapshot base offset from the file name.
    let path = dir
        .path()
        .join("00000000000000000000-0000000000.checkpoint");
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{}:/work", dir.path().display()),
            "mirror.gcr.io/apache/kafka:4.0.0",
            "/opt/kafka/bin/kafka-dump-log.sh",
            "--cluster-metadata-decoder",
            "--files",
            "/work/00000000000000000000-0000000000.checkpoint",
        ])
        .output()
        .expect("docker run kafka-dump-log");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("{text}");
    assert2::assert!(out.status.success());
    // The JVM decoder names each record by its KIP-631 type. Their presence
    // (and a clean exit) proves the translated bytes decode as real records.
    // The JVM decoder prints each record's KIP-631 type in SCREAMING_SNAKE.
    for needle in [
        "REGISTER_BROKER_RECORD",
        "TOPIC_RECORD",
        "PARTITION_RECORD",
        "CONFIG_RECORD",
    ] {
        assert2::assert!(text.contains(needle));
    }
    // Four text-shape checks over the dump output:
    // 1. No record may fail the decoder's CRC / schema check.
    // 2. All RegisterBroker records must have a non-nil incarnationId
    //    (kafka-dump-log prints lines like `RegisterBrokerRecord(brokerId=1,
    //    incarnationId=00000000-0000-0000-0000-000000000000, ...)` where a
    //    nil UUID is all-zeros).
    // 3. All Partition records must have partitionEpoch >= 0 after Slice 6
    //    (not -1, the schema default).
    check!(
        !text.contains("isvalid: false") && !text.to_lowercase().contains("could not"),
        "dump-log record-validity check failed: {text}"
    );
    check!(
        !text.contains("incarnationId=00000000-0000-0000-0000-000000000000"),
        "dump-log incarnationId check failed: {text}"
    );
    check!(
        !text
            .lines()
            .any(|l| l.contains("PartitionRecord") && l.contains("partitionEpoch=-1")),
        "dump-log partitionEpoch check failed: {text}"
    );
    // 4. The decoder prints the snapshot header's own
    //    `lastContainedLogTimestamp`, and it is the create-time of the batch
    //    appended above — not 0, which every krabka checkpoint carried while
    //    the engine passed a literal zero to the writer and dump-log printed
    //    1970 for a log written today.
    check!(
        text.contains(&last_contained_log_timestamp.to_string()),
        "dump-log header timestamp check failed, want {last_contained_log_timestamp}: {text}"
    );
}
