//! KIP-447 transactional consumer offsets become visible to `OffsetFetch`
//! only AFTER the transaction's COMMIT marker, and never on ABORT.
//!
//! A consume-process-produce (EOS) producer folds its source offsets into its
//! transaction: `AddOffsetsToTxn` → `TxnOffsetCommit` (api 28) → COMMIT marker
//! (`EndTxn`). The broker buffers the `TxnOffsetCommit` offsets on the
//! transaction coordinator. When the broker writes the COMMIT marker, it
//! materializes them into the in-memory committed-offset map of the owning
//! group, the map that `OffsetFetch` reads. This matches Kafka's "visible only
//! after the commit marker" semantics. On ABORT the broker drops the buffer,
//! and `OffsetFetch` still reports `-1`, which means absent.
//!
//! The same file covers KIP-447's `require_stable`: while the transaction is
//! open, a `read_committed` consumer that asks for stable offsets is told to
//! retry with `UNSTABLE_OFFSET_COMMIT (88)` rather than handed the offset the
//! transaction is about to replace.
//!
//! This test drives the transaction control plane directly with a low-level
//! client. The cluster has a single broker, so the admin client is itself the
//! txn coordinator and the group coordinator. The test runs
//! `InitProducerId → AddOffsetsToTxn → TxnOffsetCommit → EndTxn`, then reads
//! back with `OffsetFetch`.

use std::time::{Duration, Instant};

use assert2::assert;

mod support;

use krabka_protocol::{
    owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        end_txn_request::EndTxnRequest,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopics,
        },
        txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

/// `NOT_COORDINATOR`. The broker elects the `__transaction_state` partition
/// leader lazily on first access, so an early `InitProducerId` can race ahead
/// of it.
const NOT_COORDINATOR: i16 = 16;

/// Poll `FindCoordinator(TRANSACTION, tid)` until the txn coordinator
/// partition has a real leader, so the later `InitProducerId` does not race
/// the lazy leader election. Mirrors the retry-with-deadline idiom in the
/// marker-fanout test.
async fn await_txn_coordinator(client: &krabka_client_core::Client, tid: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let fc = client
            .send(FindCoordinatorRequest {
                key: tid.into(),
                key_type: 1, // TRANSACTION
                coordinator_keys: vec![tid.into()],
                ..Default::default()
            })
            .await
            .expect("find coordinator");
        let node = fc.coordinators.first().map_or(fc.node_id, |c| c.node_id);
        if node >= 0 {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "txn coordinator never became available: {fc:?}"
        );
        // intentional: the FindCoordinator RPC itself triggers the lazy
        // __transaction_state leader election; coordinator availability is not
        // in the metadata image and has no awaiter/metric, so poll the RPC.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const TOPIC: &str = "src";

/// Create a single-partition topic and return nothing. This test keys offsets
/// by name.
async fn create_topic(client: &krabka_client_core::Client) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");
    assert!(resp.topics[0].error_code == 0, "create topic: {resp:?}");
}

/// Resolve `TOPIC`'s `topic_id` with Metadata. At v10 the v8+ `OffsetFetch`
/// `groups[]` shape keys topics by `topic_id`, and the wire drops the name. So
/// the read must carry the id, not only the name.
async fn topic_id_for(client: &krabka_client_core::Client) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(TOPIC))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// `OffsetFetch` for `(TOPIC, 0)` under `group_id`. This function fills BOTH
/// the legacy v0–7 single-group fields and the v8+ `groups[]` shape, which
/// carries the `topic_id` that v10 keys on. The read is then correct for
/// whatever wire version the client negotiates. Returns the committed offset,
/// or `-1` when it is absent. That is the only signal this test asserts on.
async fn fetch_offset(
    client: &krabka_client_core::Client,
    group_id: &str,
    topic_id: WireUuid,
) -> i64 {
    let resp = client
        .send(OffsetFetchRequest {
            // Legacy v0–7 single-group fields.
            group_id: group_id.into(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: TOPIC.into(),
                partition_indexes: vec![0],
                ..Default::default()
            }]),
            // v8+ groups[] shape; topic_id is required at v10 (name dropped).
            groups: vec![OffsetFetchRequestGroup {
                group_id: group_id.into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: TOPIC.into(),
                    topic_id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset fetch");
    // v8+ response: data lives under groups[].topics[].partitions[].
    for g in &resp.groups {
        for t in &g.topics {
            for p in &t.partitions {
                if p.partition_index == 0 {
                    return p.committed_offset;
                }
            }
        }
    }
    // v0–7 fallback: top-level topics[].
    resp.topics
        .iter()
        .find(|t| t.name == TOPIC)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .map_or(-1, |p| p.committed_offset)
}

/// Drive `InitProducerId → AddOffsetsToTxn → TxnOffsetCommit(offset)` for
/// `(tid, group_id)`. Returns `(producer_id, producer_epoch)` so the caller
/// can finalize the transaction with `EndTxn`.
async fn begin_and_commit_offsets(
    client: &krabka_client_core::Client,
    tid: &str,
    group_id: &str,
    offset: i64,
) -> (i64, i16) {
    await_txn_coordinator(client, tid).await;
    // Even after FindCoordinator resolves, InitProducerId can briefly observe
    // NOT_COORDINATOR while the elected leader installs locally — retry it.
    let deadline = Instant::now() + Duration::from_secs(30);
    let (pid, epoch) = loop {
        let init = client
            .send(InitProducerIdRequest {
                transactional_id: Some(tid.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("init producer id");
        if init.error_code == 0 {
            break (init.producer_id, init.producer_epoch);
        }
        assert!(
            init.error_code == NOT_COORDINATOR && Instant::now() <= deadline,
            "InitProducerId: {init:?}"
        );
        // intentional: NOT_COORDINATOR clears once the elected leader installs
        // the txn coordinator locally — coordinator-local state, not in the
        // metadata image and with no awaiter/metric; bounded RPC-response poll.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // AddOffsetsToTxn registers __consumer_offsets in the txn's partition set,
    // so the COMMIT/ABORT marker fans out to it.
    let add = client
        .send(AddOffsetsToTxnRequest {
            transactional_id: tid.into(),
            producer_id: pid,
            producer_epoch: epoch,
            group_id: group_id.into(),
            ..Default::default()
        })
        .await
        .expect("add offsets to txn");
    assert!(add.error_code == 0, "AddOffsetsToTxn: {add:?}");

    // TxnOffsetCommit: empty member_id + generation_id -1 = simple consumer (no
    // membership fencing). Appends a transactional offset record + buffers it.
    let toc = client
        .send(TxnOffsetCommitRequest {
            transactional_id: tid.into(),
            group_id: group_id.into(),
            producer_id: pid,
            producer_epoch: epoch,
            generation_id: -1,
            member_id: String::new(),
            topics: vec![TxnOffsetCommitRequestTopic {
                name: TOPIC.into(),
                partitions: vec![TxnOffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("txn offset commit");
    assert!(
        toc.topics[0].partitions[0].error_code == 0,
        "TxnOffsetCommit: {toc:?}"
    );

    (pid, epoch)
}

/// The COMMIT marker materializes the buffered txn offsets into the group's
/// committed offsets. Before `EndTxn(commit)`, `OffsetFetch` reads `-1`. After
/// it, `OffsetFetch` reads the committed offset.
#[tokio::test]
async fn txn_offset_commit_visible_via_offset_fetch_after_commit_marker() {
    let p = support::start().await;
    create_topic(&p.client).await;
    let topic_id = topic_id_for(&p.client).await;

    let tid = "tid-commit";
    let group = "g-commit";

    let (pid, epoch) = begin_and_commit_offsets(&p.client, tid, group, 3).await;

    // Pre-commit: the transactional offset is held under the LSO and NOT yet
    // surfaced — Kafka makes it visible to OffsetFetch only after the marker.
    assert!(
        fetch_offset(&p.client, group, topic_id).await == -1,
        "txn offset must be invisible before the COMMIT marker"
    );

    // EndTxn(commit) writes the COMMIT marker → materializes the buffer.
    let end = p
        .client
        .send(EndTxnRequest {
            transactional_id: tid.into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: true,
            ..Default::default()
        })
        .await
        .expect("end txn commit");
    assert!(end.error_code == 0, "EndTxn(commit): {end:?}");

    // Post-commit: the offset is now visible via OffsetFetch.
    assert!(
        fetch_offset(&p.client, group, topic_id).await == 3,
        "txn offset must be visible after the COMMIT marker"
    );

    p.broker.shutdown().await;
}

/// ABORT drops the buffered txn offsets. `OffsetFetch` still reads `-1` after
/// `EndTxn(abort)`. Aborted offsets must never become committed.
#[tokio::test]
async fn txn_offset_commit_dropped_on_abort_marker() {
    let p = support::start().await;
    create_topic(&p.client).await;
    let topic_id = topic_id_for(&p.client).await;

    let tid = "tid-abort";
    let group = "g-abort";

    let (pid, epoch) = begin_and_commit_offsets(&p.client, tid, group, 5).await;

    // EndTxn(abort): the buffer is dropped without applying.
    let end = p
        .client
        .send(EndTxnRequest {
            transactional_id: tid.into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: false,
            ..Default::default()
        })
        .await
        .expect("end txn abort");
    assert!(end.error_code == 0, "EndTxn(abort): {end:?}");

    // Still absent: an aborted transactional offset is never committed.
    assert!(
        fetch_offset(&p.client, group, topic_id).await == -1,
        "txn offset must stay absent after the ABORT marker"
    );

    p.broker.shutdown().await;
}

/// The `OffsetFetch` version that carries both KIP-447's `require_stable` and
/// the KIP-516 `groups[]` response shape this test asserts on. Pinning it
/// makes the expected response struct exact: at v10 the topic name is off the
/// wire and the `topic_id` identifies the topic.
const OFFSET_FETCH_V10: i16 = 10;

/// Commit an ordinary, non-transactional offset for `(TOPIC, 0)`, so that the
/// group has a stable offset for a `require_stable` fetch to be tempted to
/// hand back while the later transaction is still open.
async fn commit_stable_offset(client: &krabka_client_core::Client, group_id: &str, offset: i64) {
    let resp = client
        .send(OffsetCommitRequest {
            group_id: group_id.into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![OffsetCommitRequestTopic {
                name: TOPIC.into(),
                topic_id: topic_id_for(client).await,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset commit");
    assert!(
        resp.topics[0].partitions[0].error_code == 0,
        "OffsetCommit: {resp:?}"
    );
}

/// `OffsetFetch` at v10 for `(TOPIC, 0)`, with `require_stable` as given.
/// `send_at_least` refuses to downgrade, so the decoded response is always the
/// v10 shape.
async fn fetch_at_v10(
    client: &krabka_client_core::Client,
    group_id: &str,
    topic_id: WireUuid,
    require_stable: bool,
) -> OffsetFetchResponse {
    client
        .send_at_least(
            OffsetFetchRequest {
                groups: vec![OffsetFetchRequestGroup {
                    group_id: group_id.into(),
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: TOPIC.into(),
                        topic_id,
                        partition_indexes: vec![0],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                require_stable,
                ..Default::default()
            },
            OFFSET_FETCH_V10,
        )
        .await
        .expect("offset fetch at v10")
}

/// The whole v10 response carrying one group, one topic, and `partition` as
/// its only row. v10 drops the topic name from the wire, so it decodes empty.
fn v10_response(
    group_id: &str,
    topic_id: WireUuid,
    partition: OffsetFetchResponsePartitions,
) -> OffsetFetchResponse {
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: Vec::new(),
        error_code: 0,
        groups: vec![OffsetFetchResponseGroup {
            group_id: group_id.into(),
            topics: vec![OffsetFetchResponseTopics {
                name: String::new(),
                topic_id,
                partitions: vec![partition],
                ..Default::default()
            }],
            error_code: 0,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A stable `(TOPIC, 0)` row at `offset`. Both the plain `OffsetCommit` and
/// the `TxnOffsetCommit` in this file leave the leader epoch at `-1` and the
/// metadata empty.
fn stable_row(offset: i64) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: 0,
        committed_offset: offset,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code: 0,
        ..Default::default()
    }
}

/// KIP-447: a `read_committed` EOS consumer sets `require_stable = true` and
/// must be told to retry -- `UNSTABLE_OFFSET_COMMIT (88)` -- while its own
/// transaction's offset commit is still waiting for a marker.
///
/// The group already has a stable offset of 3 when the transaction commits 9.
/// Handing back 3 while the transaction is open is the rewind this test pins
/// down: on a consume-process-produce restart the consumer would reprocess
/// records the transaction had already committed. Kafka answers 88 with the
/// invalid-offset sentinels instead, and only after `EndTxn` writes the commit
/// marker does the same request read 9.
#[tokio::test]
async fn require_stable_offset_fetch_is_unstable_until_the_commit_marker() {
    let p = support::start().await;
    create_topic(&p.client).await;
    let topic_id = topic_id_for(&p.client).await;

    let tid = "tid-require-stable";
    let group = "g-require-stable";

    commit_stable_offset(&p.client, group, 3).await;
    let (pid, epoch) = begin_and_commit_offsets(&p.client, tid, group, 9).await;

    let unstable = OffsetFetchResponsePartitions {
        partition_index: 0,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code: 88, // UNSTABLE_OFFSET_COMMIT
        ..Default::default()
    };

    // require_stable = true, transaction still open: retry, do not rewind.
    let strict = fetch_at_v10(&p.client, group, topic_id, true).await;
    assert!(strict == v10_response(group, topic_id, unstable));

    // require_stable = false is unchanged by KIP-447: it still reads the
    // stable offset the open transaction is about to replace.
    let relaxed = fetch_at_v10(&p.client, group, topic_id, false).await;
    assert!(relaxed == v10_response(group, topic_id, stable_row(3)));

    let end = p
        .client
        .send(EndTxnRequest {
            transactional_id: tid.into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: true,
            ..Default::default()
        })
        .await
        .expect("end txn commit");
    assert!(end.error_code == 0, "EndTxn(commit): {end:?}");

    // The marker resolved the transaction: the offset is stable again, at the
    // value the transaction committed.
    let settled = fetch_at_v10(&p.client, group, topic_id, true).await;
    assert!(settled == v10_response(group, topic_id, stable_row(9)));

    p.broker.shutdown().await;
}

/// An aborted transaction also stops answering `UNSTABLE_OFFSET_COMMIT`: the
/// abort marker drops the pending marks without publishing anything, so a
/// `require_stable` fetch goes back to the group's stable offset rather than
/// telling the consumer to retry for ever.
#[tokio::test]
async fn require_stable_offset_fetch_becomes_stable_again_after_an_abort_marker() {
    let p = support::start().await;
    create_topic(&p.client).await;
    let topic_id = topic_id_for(&p.client).await;

    let tid = "tid-require-stable-abort";
    let group = "g-require-stable-abort";

    commit_stable_offset(&p.client, group, 3).await;
    let (pid, epoch) = begin_and_commit_offsets(&p.client, tid, group, 9).await;

    let end = p
        .client
        .send(EndTxnRequest {
            transactional_id: tid.into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: false,
            ..Default::default()
        })
        .await
        .expect("end txn abort");
    assert!(end.error_code == 0, "EndTxn(abort): {end:?}");

    let settled = fetch_at_v10(&p.client, group, topic_id, true).await;
    assert!(settled == v10_response(group, topic_id, stable_row(3)));

    p.broker.shutdown().await;
}
