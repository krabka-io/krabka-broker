//! The copy pass after a leader change, over a replica whose segment
//! boundaries are not the old leader's.
//!
//! A replica rolls its own segments. Two replicas of one partition hold the
//! same offsets and need not agree on where a segment ends, and after any
//! leader election -- a reassignment, a rolling upgrade -- the new leader's
//! boundaries are the ones the copy pass sees. A copy pass that decides what
//! to copy by matching base offsets then re-uploads every sealed segment whose
//! base does not happen to match one already in the tier, and skips outright a
//! segment whose base matches a copied one but that runs further, which leaves
//! the tier a hole nothing ever fills.
//!
//! This suite gives each broker a different `log.segment.bytes` and creates a
//! topic that overrides none of them, so the two replicas of the partition
//! roll at different offsets over the same records. It then tiers, fails the
//! partition over to the survivor, produces and tiers again, and reads the
//! tier back out of the shared object store.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_broker::{BrokerHandle, NodeId, metrics::TopicLabel};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::{
    multi_client::topic_id_for,
    multi_cluster::{
        await_all_brokers_registered, await_all_rlmm_active,
        start_three_tiered_brokers_with_segment_sizes,
    },
};

/// The topic this suite produces into. It sets no `segment.bytes`, so each
/// replica rolls on its own broker-level default -- which is the whole point.
const TOPIC: &str = "tiered-misaligned-itest";
/// Records produced before the failover, and again after it. Each one is a
/// batch of its own, so both replicas roll many times over the run.
const RECORDS: usize = 140;

/// One segment the shared object store holds, as the offsets its bytes
/// actually carry rather than as the name it was written under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RemoteSegment {
    first_offset: i64,
    last_offset: i64,
}

/// Every `*.log` object under `root` that belongs to `TOPIC`, decoded into the
/// offset range its record batches cover, ascending.
///
/// The ranges come out of the bytes, not out of the metadata that describes
/// them, so a duplicate here is a duplicate object in the store and a hole
/// here is a hole in the only copy that survives local retention.
fn remote_segments(root: &Path) -> Vec<RemoteSegment> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("log")
                || path.file_name().and_then(|n| n.to_str()) == Some("log")
            {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_topic_dir = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| name.starts_with(TOPIC));
        if path.is_dir() && is_topic_dir {
            walk(&path, &mut files);
        }
    }
    let mut segments: Vec<RemoteSegment> = files
        .iter()
        .filter_map(|path| {
            let bytes = std::fs::read(path).ok()?;
            let mut cursor: &[u8] = &bytes;
            let mut first: Option<i64> = None;
            let mut last: Option<i64> = None;
            while !cursor.is_empty() {
                let batch = RecordBatch::decode(&mut cursor).ok()?;
                first.get_or_insert(batch.base_offset);
                last = Some(batch.base_offset + i64::from(batch.last_offset_delta));
            }
            Some(RemoteSegment {
                first_offset: first?,
                last_offset: last?,
            })
        })
        .collect();
    segments.sort_unstable();
    segments
}

/// The base offsets of the `*.log` files in one replica's partition
/// directory, ascending. The highest is the active segment's; everything
/// below it is sealed.
fn local_segment_bases(partition_dir: &Path) -> Vec<i64> {
    let Ok(entries) = std::fs::read_dir(partition_dir) else {
        return Vec::new();
    };
    let mut bases: Vec<i64> = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("log") {
                return None;
            }
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<i64>().ok())
        })
        .collect();
    bases.sort_unstable();
    bases
}

/// Produces `count` single-record batches through `client`, one request each,
/// so both replicas see many small batches and roll on their own byte budget.
async fn produce_records(client: &Client, topic_id: WireUuid, prefix: &str, count: usize) {
    for index in 0..count {
        let batch = RecordBatch {
            records: vec![Record {
                value: Some(bytes::Bytes::from(format!("{prefix}-record-{index}"))),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = client
            .send(ProduceRequest {
                // acks=1: after the failover the partition has one live
                // replica, and this test is about what the leader copies, not
                // about how far the ISR shrank.
                acks: 1,
                timeout_ms: 10_000,
                topic_data: vec![TopicProduceData {
                    name: TOPIC.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(batch.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        assert!(
            response.responses[0].partition_responses[0].error_code == 0,
            "Produce failed: {response:?}"
        );
    }
}

/// Creates the tiered topic, deliberately without a `segment.bytes` override,
/// and waits until both replicas hold the tiered configuration.
async fn create_misaligned_topic(admin: &Client, b1: &BrokerHandle, b2: &BrokerHandle) {
    let response = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 2,
                configs: vec![
                    CreatableTopicConfig {
                        name: "remote.storage.enable".into(),
                        value: Some("true".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "local.retention.bytes".into(),
                        value: Some("1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.bytes".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {response:?}"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ready = |broker: &BrokerHandle| {
            broker
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && config.local_retention_size == Some(krabka_units::bytes(1))
                })
        };
        if ready(b1) && ready(b2) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the tiered configuration did not reach both replicas"
        );
        // intentional: topic-config propagation to both replicas has no
        // image-level signal this test can await, so it polls the snapshot.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Polls the shared store until it holds at least `wanted` segments of
/// `TOPIC`, then returns them.
async fn await_remote_segments(remote_dir: &Path, wanted: usize, what: &str) -> Vec<RemoteSegment> {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let segments = remote_segments(remote_dir);
        if segments.len() >= wanted {
            return segments;
        }
        assert!(
            Instant::now() <= deadline,
            "{what}: the shared store holds {} segment(s), wanted {wanted}",
            segments.len()
        );
        // intentional: the copy task runs on the broker's own 1s interval, and
        // the object store is a directory; there is no in-process signal.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The offset through which the tier's coverage runs unbroken from offset 0,
/// and the segments that are redundant: every offset they hold is held by some
/// other segment, so the object is a second copy of data the tier already had.
fn coverage_and_redundancy(segments: &[RemoteSegment]) -> (i64, Vec<RemoteSegment>) {
    let mut covered_through: i64 = -1;
    for segment in segments {
        if segment.first_offset <= covered_through + 1 {
            covered_through = covered_through.max(segment.last_offset);
        } else {
            break;
        }
    }
    let mut redundant = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let others: BTreeSet<i64> = segments
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .flat_map(|(_, other)| other.first_offset..=other.last_offset)
            .collect();
        if (segment.first_offset..=segment.last_offset).all(|offset| others.contains(&offset)) {
            redundant.push(*segment);
        }
    }
    (covered_through, redundant)
}

/// The tier a new leader inherits is a set of offset ranges, not a set of
/// segment boundaries, and this is the test that says so.
///
/// One replica rolls at 1 KiB and the other at 1.5 KiB, so their segments
/// cover the same offsets with different boundaries. The elected leader
/// produces and tiers; it is then shut down, the survivor is elected, and it
/// produces and tiers over its own boundaries. What the store then holds must
/// be one unbroken run of offsets with no object that merely repeats offsets
/// another object already holds -- which is what a resume point taken from the
/// tier's coverage produces and what a base-offset skip set does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_resumes_the_copy_from_the_tiers_coverage() {
    let (b1, b2, b3, log_dirs, remote_dir) = start_three_tiered_brokers_with_segment_sizes([
        krabka_units::kibibytes(1),
        krabka_units::bytes(1536),
        krabka_units::kibibytes(1),
    ])
    .await;
    await_all_brokers_registered(&b1, &b2, &b3).await;
    await_all_rlmm_active(&b1, &b2, &b3).await;

    let b1_bootstrap = format!("127.0.0.1:{}", b1.listen_addr().port());
    let admin = Client::builder()
        .bootstrap(&b1_bootstrap)
        .client_id("tiered-misaligned-admin")
        .build()
        .await
        .expect("admin client");
    create_misaligned_topic(&admin, &b1, &b2).await;

    // With 3 registered brokers and rf=2 the partition sits on brokers 1 and
    // 2; wait until the image names one of them and read which.
    let (b1_id, b2_id) = (b1.node_id(), b2.node_id());
    b1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|p| p.leader == b1_id || p.leader == b2_id)
    })
    .await;
    let leader_is_b1 = b1.partition_leader_for_test(TOPIC, 0) == Some(b1_id);
    let (leader_index, survivor_index) = if leader_is_b1 { (0, 1) } else { (1, 0) };

    let topic_id = topic_id_for(&admin, TOPIC).await;
    let leader_dir = log_dirs[leader_index].path().join(format!("{TOPIC}-0"));
    let survivor_dir = log_dirs[survivor_index].path().join(format!("{TOPIC}-0"));

    // Produce in two halves and read both replicas' boundaries in between.
    // They must differ: that is the condition this test exists for, and if a
    // future change made the two replicas roll at the same offsets the
    // failover below would prove nothing. The sample is taken mid-produce,
    // while both replicas still hold sealed segments to compare.
    produce_records(&admin, topic_id, "before", RECORDS / 2).await;
    let leader_bases = local_segment_bases(&leader_dir);
    let survivor_bases = local_segment_bases(&survivor_dir);
    assert!(
        leader_bases != survivor_bases,
        "the replicas rolled at the same offsets ({leader_bases:?} vs {survivor_bases:?}); \
         this test needs misaligned segment boundaries"
    );
    produce_records(&admin, topic_id, "before-more", RECORDS - RECORDS / 2).await;

    let before = await_remote_segments(remote_dir.path(), 2, "the first leader's copy").await;
    eprintln!("ITEST: the first leader tiered {before:?}");

    // The survivor has to hold the copy metadata before it is elected, or its
    // first copy pass as leader would resume from its own log start over a
    // tier it cannot see. Its own local retention says it does: a follower
    // evicts only what the shared RLMM reports copied, so its oldest local
    // segment moving off zero is that metadata having arrived.
    let propagation_deadline = Instant::now() + Duration::from_mins(2);
    loop {
        if local_segment_bases(&survivor_dir)
            .first()
            .is_some_and(|base| *base > 0)
        {
            break;
        }
        assert!(
            Instant::now() <= propagation_deadline,
            "the follower never evicted a copied segment, so it never saw the copy metadata"
        );
        // intentional: the follower's RLMM consumer and its retention pass
        // both run on the broker's own 1s interval; no in-process signal.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    drop(admin);
    let mut opt_b1 = Some(b1);
    let mut opt_b2 = Some(b2);
    let (leader, leader_id) = if leader_is_b1 {
        (opt_b1.take().expect("broker 1 handle"), b1_id)
    } else {
        (opt_b2.take().expect("broker 2 handle"), b2_id)
    };
    leader.shutdown().await;
    eprintln!("ITEST: the first leader is down; waiting for the failover");
    let survivor = opt_b1
        .as_ref()
        .or(opt_b2.as_ref())
        .expect("one of the two replicas survives");
    survivor
        .wait_until_partition_leader_changed(TOPIC, 0, NodeId(leader_id))
        .await;

    let survivor_bootstrap = format!("127.0.0.1:{}", survivor.listen_addr().port());
    let survivor_client = Client::builder()
        .bootstrap(&survivor_bootstrap)
        .client_id("tiered-misaligned-survivor")
        .build()
        .await
        .expect("survivor client");
    produce_records(&survivor_client, topic_id, "after", RECORDS).await;

    // The new leader's copy pass has to reach past what the old one tiered.
    let after = await_remote_segments(remote_dir.path(), before.len() + 1, "the new leader's copy")
        .await;
    eprintln!("ITEST: the tier after the failover holds {after:?}");

    // KIP-405's copy-lag gauge on the new leader. It counts the sealed local
    // segments the tier does not hold whole, so a new leader holding segments
    // the previous one never copied has to show them here. This is the series
    // that read zero over a segment the old skip set had marked copied by its
    // base offset while nothing had ever been uploaded for it.
    let lag_label = TopicLabel {
        topic: std::sync::Arc::from(TOPIC),
    };
    survivor
        .wait_for_metrics("the survivor counts its uncopied segments as lag", move |m| {
            m.remote_copy_lag_segments.get_or_create(&lag_label).get() >= 1
        })
        .await;

    // Local retention still evicts on the new leader: the same RLMM coverage
    // that placed the resume point is what lets a replica drop its own copies,
    // and it can only reach the new leader's boundaries if the segments over
    // them were really copied. Waiting for it also settles the state the
    // assertions below read.
    let eviction_deadline = Instant::now() + Duration::from_mins(2);
    loop {
        if local_segment_bases(&survivor_dir).len() == 1 {
            break;
        }
        assert!(
            Instant::now() <= eviction_deadline,
            "the new leader still holds {:?}; nothing evicted them, so the tier does not \
             cover them",
            local_segment_bases(&survivor_dir)
        );
        // intentional: local retention runs on the broker's own interval tick.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let segments = remote_segments(remote_dir.path());
    let (covered_through, redundant) = coverage_and_redundancy(&segments);
    let highest = segments
        .iter()
        .map(|segment| segment.last_offset)
        .max()
        .expect("the tier holds segments");

    // No gap: the tier's coverage runs unbroken from offset 0 through the
    // highest offset any object holds. A skip set keyed on base offsets leaves
    // a hole here whenever a new leader's segment shares a base with a copied
    // one and runs further than it, and nothing ever fills it.
    check!(
        covered_through == highest,
        "the tier's coverage from 0 stops at {covered_through} of {highest}: {segments:?}"
    );
    // No duplicated data: every object carries at least one offset no other
    // object carries. A skip set keyed on base offsets re-uploads whole
    // segments here, once for every sealed segment the new leader still holds
    // whose base does not line up with a copied one.
    check!(
        redundant.is_empty(),
        "the tier holds {} object(s) whose offsets another object already holds: {redundant:?}",
        redundant.len()
    );

    // And the coverage really does reach the new leader's own segments: its
    // active segment's base is one past the last offset it sealed, and every
    // sealed segment below it has been copied and then evicted.
    let active_base = *local_segment_bases(&survivor_dir)
        .last()
        .expect("the survivor holds an active segment");
    check!(
        covered_through >= active_base - 1,
        "the tier covers only through {covered_through} while the survivor's active segment \
         starts at {active_base}"
    );

    drop(survivor_client);
    if let Some(handle) = opt_b1.take() {
        handle.shutdown().await;
    }
    if let Some(handle) = opt_b2.take() {
        handle.shutdown().await;
    }
    b3.shutdown().await;
}
