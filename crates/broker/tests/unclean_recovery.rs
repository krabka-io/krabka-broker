// rustc 1.95 clippy ICEs on `clippy::pedantic` / `clippy::unwrap_in_result`
// for files that build Kafka wire frames with `.expect()` inside a
// `Result`-returning helper — same upstream annotate-snippets bug noted in
// `tests/elect_leaders.rs`. Suppress locally; the rest of the workspace
// still enforces the full lint gate.

//! KIP-966 end-to-end: offset-aware **unclean recovery** elects the survivor
//! that is known to be complete, and only falls back to the most complete
//! surviving log when there is no such replica. Neither answer is "the first
//! alive replica".
//!
//! The tests use a 3-broker PLAINTEXT cluster, for the same reason as
//! `tests/elect_leaders.rs`. A 3-node raft quorum survives one dead node, and
//! the authorizer compat shim lets the wire path through without SASL.
//!
//! Both cases run the same scenario, in `run_unclean_recovery`, and differ
//! only in the eligible-leader-replica set the partition carries and the
//! leader that must win:
//!
//! 1. A 3-broker cluster with topic "t", 1 partition, and RF=3, so the
//!    replicas are `[1, 2, 3]`.
//! 2. Set `unclean.recovery.strategy=Aggressive` on the topic, so the UNCLEAN
//!    election routes through the offset-aware Unclean Recovery Manager (URM).
//! 3. Take the partition offline. The test injects a `PartitionRecord` whose
//!    leader and ISR is a dead phantom node, 99. Liveness reports 99 dead, so
//!    the partition has no live leader, and its ISR holds no live member. A
//!    demotion of the leader to a broker that does not exist also stops the
//!    real replication fetchers, which keeps the next step deterministic.
//! 4. Force the replicas' local logs to **diverge** deterministically with the
//!    `produce_records_for_test` accessor. It appends directly to each
//!    broker's hosted partition log, so the per-broker LEOs differ: broker 2
//!    gets the strictly highest LEO, broker 3 the middle one, and broker 1 the
//!    lowest. All three keep `current_leader_epoch == 0`, so only
//!    `log_end_offset` decides the most-complete-log ranking.
//! 5. Publish the partition's eligible-leader-replica set, when the case has
//!    one. It is written last so that no controller path re-derives it between
//!    the write and the election.
//! 6. Trigger recovery with `ElectLeaders(UNCLEAN)` sent to the raft leader.
//!    The URM polls brokers 1, 2, and 3 over the real `GetReplicaLogInfo` wire
//!    path and elects one of them.
//! 7. Assert the new partition leader, and that the ISR collapses to it.
//!
//! With no ELR (`unclean_recovery_elects_longest_log_replica`) the winner is
//! broker 2, the highest LEO, and not broker 1, the first alive replica. With
//! an ELR of `{3}` (`unclean_recovery_prefers_an_eligible_leader_replica`) the
//! winner is broker 3, whose log is *shorter* than broker 2's: an ELR member
//! held every committed record when it left the ISR, so electing it loses
//! nothing, and a longer non-ELR log is only a guess. Broker 3 is neither the
//! first alive replica nor the most complete log, so the assertion separates
//! the ELR rule from both of the others.
//!
//! These tests are gated to non-Windows, to match the multi-broker test
//! convention. The openraft `debug_assert!` races on the hosted Windows
//! scheduler are unrelated.

use std::{io, net::SocketAddr, time::Duration};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use krabka_broker::BrokerHandle;
use krabka_metadata::{BrokerConfigRecord, MetadataRecord, PartitionRecord, TopicConfigRecord};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

mod support;

const ELECT_LEADERS_VERSION: i16 = 2;

/// The controller-managed topic config that carries KIP-966 ELR state. It is
/// `crate::config_keys::ELIGIBLE_LEADER_REPLICAS`, which a test crate cannot
/// name.
const ELR_CONFIG_KEY: &str = "krabka.elr";

/// The controller-managed broker config that carries the witness role. It is
/// `crate::config_keys::BROKER_WITNESS`, which a test crate cannot name, and a
/// witness node publishes it for itself at registration.
const WITNESS_CONFIG_KEY: &str = "broker.witness";

// ─────────────────────────────────────────────────────────────────────────────
// Minimal wire helpers — bare TCP on PLAINTEXT, no SASL.
// (Copied from tests/elect_leaders.rs; the two test crates compile
// independently so a small duplicate keeps the helper local + simple.)
// ─────────────────────────────────────────────────────────────────────────────

/// One length-prefixed request and response exchange over a **PLAINTEXT**
/// connection. It encodes a Kafka request header, flexible or not, writes the
/// frame, reads one response frame, strips the response header, and returns
/// the body bytes.
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "krabka-unclean-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

/// Creates a topic on a PLAINTEXT broker. The authorizer compat shim, with no
/// `super_users` and no ACLs, lets the request through.
async fn create_topic_plaintext(
    addr: SocketAddr,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Drives `ElectLeaders` over a fresh PLAINTEXT connection. It asserts that
/// the top-level `error_code == 0`, and returns the per-partition
/// `(partition_id, error_code)` rows for `topic`.
async fn drive_elect_leaders(
    addr: SocketAddr,
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = ElectLeadersRequest {
        election_type,
        topic_partitions: Some(vec![TopicPartitions {
            topic: topic.to_string(),
            partitions,
            ..Default::default()
        }]),
        timeout_ms: 30_000,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, ELECT_LEADERS_VERSION)
        .expect("encode ElectLeaders");
    let resp_bytes = round_trip(&mut stream, 43, ELECT_LEADERS_VERSION, 1, true, &body)
        .await
        .expect("ElectLeaders round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ElectLeadersResponse::decode(&mut cur, ELECT_LEADERS_VERSION)
        .expect("decode ElectLeadersResponse");

    assert!(
        resp.error_code == 0,
        "top-level error_code must be 0, got {}",
        resp.error_code
    );

    resp.replica_election_results
        .into_iter()
        .find(|r| r.topic == topic)
        .map(|r| {
            r.partition_result
                .into_iter()
                .map(|p| (p.partition_id, p.error_code))
                .collect()
        })
        .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────────────────
// Polling helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Waits until `handle` sees `(topic, partition)` in its metadata image.
async fn wait_partition_hosted(handle: &BrokerHandle, topic: &str, partition: i32) {
    // Event-driven: `has_partition` reads the committed metadata image, which is
    // exactly the source `wait_until_partition_present` subscribes to.
    handle.wait_until_partition_present(topic, partition).await;
}

/// Waits until `handle`'s metadata image reports `leader` as the leader for
/// `(topic, partition)`.
async fn wait_partition_leader(handle: &BrokerHandle, topic: &str, partition: i32, leader: u64) {
    // Event-driven: await the metadata image whose leader == expected (the image
    // is the same source `partition_leader_for_test` reads; `leader` is non-zero).
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.leader == leader)
        })
        .await;
}

/// Waits until the ISR for `(topic, partition)` is exactly `expected`.
async fn wait_partition_isr_only(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    expected: &[u64],
) {
    let expected_set: std::collections::HashSet<u64> = expected.iter().copied().collect();
    // Event-driven: await the metadata image whose ISR set matches `expected`
    // exactly (length-only `wait_until_isr_len` is too weak for a set assertion).
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition).is_some_and(|p| {
                p.isr
                    .iter()
                    .map(|n| n.get())
                    .collect::<std::collections::HashSet<u64>>()
                    == expected_set
            })
        })
        .await;
}

/// The topic's whole controller-managed override map: the recovery strategy
/// that routes an UNCLEAN election through the URM, plus the published
/// eligible-leader-replica value when the case has one.
fn topic_config(topic: &str, elr: Option<&str>) -> MetadataRecord {
    let mut overrides = std::collections::BTreeMap::new();
    overrides.insert(
        "unclean.recovery.strategy".to_string(),
        "Aggressive".to_string(),
    );
    if let Some(elr) = elr {
        overrides.insert(ELR_CONFIG_KEY.to_string(), elr.to_string());
    }
    MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.to_string(),
        overrides,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

/// The offset-aware unclean-recovery path elects the survivor with the
/// **highest LEO** when nothing better is known about any of them. That result
/// is different from a simple "first alive replica" pick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_elects_longest_log_replica() {
    run_unclean_recovery(None, None, 2).await;
}

/// KIP-966: a surviving eligible leader replica outranks a longer log that is
/// not one. Broker 3 is in the published ELR and holds fewer records than
/// broker 2, and broker 3 must still win: it is known to hold every committed
/// record, and broker 2's longer log is only the longest one that answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_prefers_an_eligible_leader_replica() {
    // `krabka.elr` grammar: partition 0 has ELR {3} and no last-known ELR.
    run_unclean_recovery(Some("0:3:"), None, 3).await;
}

/// A witness replicates the partition and can be published as an eligible
/// leader replica, and it must still never lead: `broker.witness` means the
/// node serves no client, so a partition it leads is as unusable as the
/// offline one. Broker 3 is partition 0's only ELR member here and carries the
/// role, so recovery has to fall back to broker 2's longest surviving log --
/// the answer that is reported as data-losing rather than the one that is
/// reported as free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_never_elects_a_witness_from_the_elr() {
    run_unclean_recovery(Some("0:3:"), Some(3), 2).await;
}

/// Leaves partition 0 of `topic` offline on an RF=3 cluster, with the three
/// surviving replicas holding logs of three different lengths.
///
/// `handles` are brokers 1, 2 and 3 in that order, and `addr` is any broker's
/// listener. On return the partition's leader is a node that never existed, so
/// nothing will elect a leader on its own, and broker 2 holds the strictly
/// highest LEO with broker 3 in the middle and broker 1 lowest.
async fn offline_partition_with_diverged_logs(
    addr: SocketAddr,
    topic: &str,
    handles: [&BrokerHandle; 3],
) {
    let [h1, h2, h3] = handles;
    // ── Create the RF=3 topic. With 3 registered brokers, partition 0's
    //    replica assignment is [1, 2, 3]; broker 1 is the preferred/first. ──
    create_topic_plaintext(addr, topic, 1, 3).await;
    wait_partition_hosted(h1, topic, 0).await;
    wait_partition_hosted(h2, topic, 0).await;
    wait_partition_hosted(h3, topic, 0).await;

    // Event-driven: await the partition's presence in the image, then read the
    // record the loop captured (h1 already saw it via `wait_partition_hosted`).
    h1.wait_until_partition_present(topic, 0).await;
    let pr_before = h1
        .partition_record_for_test(topic, 0)
        .expect("partition record present after wait_until_partition_present");
    eprintln!("partition before divergence: {pr_before:?}");
    assert!(
        pr_before.replicas
            == vec![
                krabka_broker::NodeId(1),
                krabka_broker::NodeId(2),
                krabka_broker::NodeId(3)
            ],
        "expected RF=3 replicas [1,2,3]; got {:?}",
        pr_before.replicas
    );

    // ── Set unclean.recovery.strategy=Aggressive so UNCLEAN routes through
    //    the offset-aware Unclean Recovery Manager. ──
    h1.submit_metadata_record_for_test(topic_config(topic, None))
        .await
        .expect("set unclean.recovery.strategy=Aggressive");

    // ── Take the partition offline FIRST: inject a PartitionRecord whose
    //    leader and ISR are a dead phantom node (99, never heartbeated →
    //    liveness=dead). Replicas stay [1, 2, 3] so the URM polls the three
    //    real survivors. Crucially, demoting the leader to a non-existent
    //    broker stops the real replication fetchers (no broker is leader),
    //    so the direct-append divergence we set up next stays deterministic
    //    — otherwise the leader's replicator races with our test appends. ──
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        replicas: vec![
            krabka_broker::NodeId(1),
            krabka_broker::NodeId(2),
            krabka_broker::NodeId(3),
        ],
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: pr_before.leader_epoch.next(),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    });
    h1.submit_metadata_record_for_test(forged)
        .await
        .expect("inject dead-leader PartitionRecord");
    wait_partition_isr_only(h1, topic, 0, &[99]).await;
    // Give the supervisors a beat to observe the leader change and tear down
    // any in-flight replication fetchers before we diverge the logs.
    // intentional: fetcher teardown is a background reconcile action with no
    // metadata-image or metric signal to await; the barrier keeps the direct
    // appends deterministic.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Force the surviving replicas' local logs to DIVERGE deterministically.
    //    `produce_records_for_test` appends directly to each broker's hosted
    //    partition log, so we set known, distinct LEOs by adding a different
    //    count of records on top of whatever each already holds:
    //        broker 2 gets the most → strictly-highest LEO.
    //    All keep current_leader_epoch == 0, so selection is decided by LEO.
    //    Broker 2 is NOT the first-alive replica (that's broker 1), which is
    //    what makes the assertion meaningful. ──
    h1.produce_records_for_test(topic, 0, 2)
        .await
        .expect("produce to broker 1");
    h3.produce_records_for_test(topic, 0, 4)
        .await
        .expect("produce to broker 3");
    h2.produce_records_for_test(topic, 0, 20)
        .await
        .expect("produce to broker 2");
    let end1 = h1.local_log_end_offset(topic, 0).expect("leo b1");
    let end2 = h2.local_log_end_offset(topic, 0).expect("leo b2");
    let end3 = h3.local_log_end_offset(topic, 0).expect("leo b3");
    eprintln!("LEO end offsets: b1={end1} b2={end2} b3={end3}");
    assert!(
        end2 > end1 && end2 > end3,
        "broker 2 must hold the strictly-highest LEO (b1={end1} b2={end2} b3={end3})"
    );
}

/// A 3-broker PLAINTEXT cluster with an RF=3 topic, whose replicas are
/// `[1, 2, 3]`. It runs the scenario the module docs describe with `elr`
/// published as partition 0's eligible-leader-replica set, and asserts that
/// `expected_leader` wins the `ElectLeaders(UNCLEAN)` that follows.
async fn run_unclean_recovery(elr: Option<&str>, witness: Option<u64>, expected_leader: u64) {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let topic = "t";
    let addr = cluster[0].1.listen_addr;
    let h1 = &cluster[0].0; // broker_id 1
    let h2 = &cluster[1].0; // broker_id 2
    let h3 = &cluster[2].0; // broker_id 3

    offline_partition_with_diverged_logs(addr, topic, [h1, h2, h3]).await;

    // ── Publish `broker.witness=true` for the node this case makes a witness,
    //    which is the path a witness node's own registration takes. It goes in
    //    before the ELR value below: a broker-config record carries no
    //    partition change, so no controller path recomputes the ELR from it,
    //    and this keeps the last write the one the URM reads. ──
    if let Some(id) = witness {
        h1.submit_metadata_record_for_test(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_broker::NodeId(id),
            config_name: WITNESS_CONFIG_KEY.to_string(),
            config_value: Some("true".to_string()),
        }))
        .await
        .expect("publish broker.witness for the witness node");
        // Event-driven: await the role in the image the URM reads.
        h1.wait_for_image(|img| {
            img.broker_config(krabka_broker::NodeId(id))
                .and_then(|configs| configs.get(WITNESS_CONFIG_KEY))
                .is_some_and(|value| value == "true")
        })
        .await;
    }

    // ── Publish the eligible-leader-replica set, if this case has one. It
    //    goes in last: a `V1TopicConfig` replaces the topic's whole override
    //    map, and writing it after the partition is already offline keeps any
    //    controller path that recomputes ELR from clearing it first. The
    //    record carries the recovery strategy along with it for the same
    //    reason. ──
    if let Some(elr) = elr {
        h1.submit_metadata_record_for_test(topic_config(topic, Some(elr)))
            .await
            .expect("publish the eligible leader replicas");
        // Event-driven: await the published value in the image the URM reads.
        h1.wait_for_image(|img| {
            img.topic_config(topic)
                .and_then(|configs| configs.get(ELR_CONFIG_KEY))
                .is_some_and(|value| value == elr)
        })
        .await;
    }

    // ── ElectLeaders(UNCLEAN) must reach the raft leader, the only node that
    //    runs the URM and has authoritative liveness state. Discover it. ──
    // Event-driven: await a non-zero elected controller leader (same watch channel
    // `controller_leader_id` reads), then resolve its listen address.
    let elect_addr = {
        let leader = h1.wait_until_controller_leader().await;
        let pos = cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("elected raft leader must be one of the cluster nodes");
        cluster[pos].1.listen_addr
    };
    eprintln!("sending ElectLeaders UNCLEAN to raft leader at {elect_addr}");

    // ── Trigger offset-aware recovery (election_type = 1 = UNCLEAN). ──
    let result = drive_elect_leaders(elect_addr, topic, vec![0], 1).await;
    assert!(
        result == vec![(0, 0)],
        "expected error_code=0 for UNCLEAN election; got {result:?}"
    );

    // ── The load-bearing assertion: the URM elected the replica the rules
    //    name, and not merely the first alive one. ──
    wait_partition_leader(h1, topic, 0, expected_leader).await;
    let final_leader = h1
        .partition_leader_for_test(topic, 0)
        .expect("leader present");
    assert!(
        final_leader == expected_leader,
        "URM must elect broker {expected_leader} with elr={elr:?}; got \
         leader={final_leader}"
    );
    // ISR collapses to the singleton elected leader.
    wait_partition_isr_only(h1, topic, 0, &[expected_leader]).await;

    // Clean up.
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
