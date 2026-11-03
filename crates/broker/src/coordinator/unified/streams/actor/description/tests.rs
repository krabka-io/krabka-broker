//! The KIP-1331 steps of the streams actor, driven through its handle with no
//! connected `MetadataSource`: a joining member installs the topology, which
//! is all the solicitation needs.

use {
    super::*,
    crate::{
        coordinator::unified::{
            GroupCoordinator, ReplaySeed, StreamsGroupSeed,
            offsets_log::fake::InMemoryOffsetsLog,
            persistence::{Key, parse_key},
            streams::{
                actor::{
                    StreamsGroupActorHandle, StreamsGroupActorMessage,
                    test_support::{
                        coordinator_with_log, describe, heartbeat_result_at, undelayed,
                    },
                },
                description::{
                    BackendFuture, DescriptionEpochs, FileTopologyDescriptionBackend, Node,
                    NodeKind, StoredDescription, Subtopology, TopologyDescriptionBackend,
                    TopologyDescriptionPlugin,
                },
                persistence::{StreamsGroupKey, StreamsGroupMetadataValue},
                topology::to_stored_topology,
            },
        },
        task_util::ask,
    },
    assert2::assert,
    krabka_protocol::owned::{
        common::streams_group_topology_description_update_request::{
            topology_description_node::TopologyDescriptionNode,
            topology_description_subtopology::TopologyDescriptionSubtopology,
        },
        streams_group_heartbeat_request::{Subtopology as WireSubtopology, Topology},
    },
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    tokio::sync::{Notify, oneshot},
};

fn coordinator(plugin: TopologyDescriptionPlugin) -> Arc<GroupCoordinator> {
    coordinator_and_log(plugin).coordinator
}

/// A coordinator whose streams groups use `plugin`, and the offsets log it
/// writes.
struct CoordinatorAndLog {
    coordinator: Arc<GroupCoordinator>,
    log: Arc<InMemoryOffsetsLog>,
}

fn coordinator_and_log(plugin: TopologyDescriptionPlugin) -> CoordinatorAndLog {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coordinator = coordinator_with_log(
        StreamsGroupConfig {
            topology_description_plugin: plugin,
            ..undelayed()
        },
        Arc::clone(&log),
    );
    CoordinatorAndLog { coordinator, log }
}

/// The value of the last group metadata record that `log` holds.
async fn last_group_metadata(log: &InMemoryOffsetsLog) -> StreamsGroupMetadataValue {
    log.batches()
        .await
        .iter()
        .flat_map(|batch| batch.records.iter())
        .filter(|record| {
            record.key.as_deref().is_some_and(|key| {
                matches!(
                    parse_key(key),
                    Ok(Key::Streams(StreamsGroupKey::GroupMetadata { .. }))
                )
            })
        })
        .filter_map(|record| record.value.as_deref())
        .map(|value| StreamsGroupMetadataValue::decode(value).expect("a group metadata value"))
        .next_back()
        .expect("a group metadata record")
}

/// A join of `member_id` with the one-subtopology topology at epoch 0.
fn join(member_id: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: member_id.into(),
        member_epoch: 0,
        topology: Some(Topology {
            epoch: 0,
            subtopologies: vec![WireSubtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The next heartbeat of a member that joined at `member_epoch`.
fn steady(member_id: &str, member_epoch: i32) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: member_id.into(),
        member_epoch,
        ..Default::default()
    }
}

async fn heartbeat(
    handle: &StreamsGroupActorHandle,
    request: StreamsGroupHeartbeatRequest,
    version: i16,
) -> StreamsGroupHeartbeatResponse {
    let response = heartbeat_result_at(handle, request, version).await.response;
    assert!(response.error_code == codes::NONE, "{response:?}");
    response
}

async fn push(handle: &StreamsGroupActorHandle, push: DescriptionPush) -> PushAnswer {
    ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::PushDescription {
            push: Box::new(push),
            reply,
        }
    })
    .await
    .expect("a push answer")
}

/// The source node a Streams client describes for the topology of [`join`].
fn wire_description(node_type: i8) -> WireDescription {
    WireDescription {
        subtopologies: vec![TopologyDescriptionSubtopology {
            subtopology_id: "0".into(),
            nodes: vec![TopologyDescriptionNode {
                name: "KSTREAM-SOURCE-0000000000".into(),
                node_type,
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn description_push(member_id: &str, topology_epoch: i32, node_type: i8) -> DescriptionPush {
    DescriptionPush {
        member_id: member_id.into(),
        topology_epoch,
        description: wire_description(node_type),
    }
}

/// Each row joins two members of a fresh group, one after the other, and
/// sends a heartbeat from the first. Only a broker with a plugin asks, only
/// at version 1, and only the first member: the back-off window holds the
/// others off, as Kafka's `armIfNotActive` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_member_of_a_group_is_asked_for_the_description() {
    let rows = [
        ("no plugin", TopologyDescriptionPlugin::None, 1, [
            false, false, false,
        ]),
        ("version 0", TopologyDescriptionPlugin::InMemory, 0, [
            false, false, false,
        ]),
        (
            "in-memory plugin",
            TopologyDescriptionPlugin::InMemory,
            1,
            [true, false, false],
        ),
    ];
    for (row, plugin, version, want) in rows {
        let coordinator = coordinator(plugin);
        let handle = coordinator.get_or_create_streams("app");

        let first = heartbeat(&handle, join("a"), version).await;
        let second = heartbeat(&handle, join("b"), version).await;
        let again = heartbeat(&handle, steady("a", first.member_epoch), version).await;

        let asked = [
            first.topology_description_required,
            second.topology_description_required,
            again.topology_description_required,
        ];
        assert!(asked == want, "{row}");
    }
}

/// A push stores the description and records its epoch: a describe serves
/// it, the group metadata record carries the epoch in Kafka trunk's tag 2, and
/// no heartbeat asks again. Any member of the group may push, not only the
/// one asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_is_stored_and_ends_the_asking() {
    let CoordinatorAndLog { coordinator, log } =
        coordinator_and_log(TopologyDescriptionPlugin::InMemory);
    let handle = coordinator.get_or_create_streams("app");
    let first = heartbeat(&handle, join("a"), 1).await;
    assert!(first.topology_description_required);
    let second = heartbeat(&handle, join("b"), 1).await;
    assert!(describe(&handle).await.topology_description.is_none());
    let before = last_group_metadata(&log).await;

    let answer = push(&handle, description_push("b", 0, 1)).await;

    assert!(answer == (codes::NONE, None));
    let expected = TopologyDescription {
        subtopologies: vec![Subtopology {
            id: "0".into(),
            nodes: vec![Node {
                name: "KSTREAM-SOURCE-0000000000".into(),
                kind: NodeKind::Source {
                    topics: vec!["in".into()],
                },
                successors: vec![],
            }],
        }],
        global_stores: vec![],
    };
    assert!(describe(&handle).await.topology_description == Some(expected));
    let stored = DescriptionEpochs {
        stored: 0,
        failed: -1,
    };
    assert!(
        last_group_metadata(&log).await
            == StreamsGroupMetadataValue {
                description: stored,
                ..before
            }
    );
    let seed = coordinator
        .cached_streams_seed("app")
        .expect("a cached seed");
    assert!(seed.description_epochs == stored);
    for (member_id, member_epoch) in [("a", first.member_epoch), ("b", second.member_epoch)] {
        let response = heartbeat(&handle, steady(member_id, member_epoch), 1).await;
        assert!(!response.topology_description_required, "{member_id}");
    }
}

/// The pushes Kafka's `validateStreamsGroupTopologyDescriptionUpdate` and
/// converter refuse, with their messages. None of them stores anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_the_group_does_not_accept_stores_nothing() {
    let rows = [
        (
            "an unknown member",
            description_push("z", 0, 1),
            (
                codes::UNKNOWN_MEMBER_ID,
                Some("Member z is not a member of group app.".to_owned()),
            ),
        ),
        (
            "another topology epoch",
            description_push("a", 3, 1),
            (
                codes::INVALID_REQUEST,
                Some(
                    "Topology epoch 3 does not match the group's current topology epoch 0."
                        .to_owned(),
                ),
            ),
        ),
        (
            "an unknown node type",
            description_push("a", 0, 9),
            (
                codes::INVALID_REQUEST,
                Some("Unknown topology node type: 9".to_owned()),
            ),
        ),
    ];
    for (row, refused, want) in rows {
        let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
        let handle = coordinator.get_or_create_streams("app");
        heartbeat(&handle, join("a"), 1).await;

        assert!(push(&handle, refused).await == want, "{row}");
        assert!(
            describe(&handle).await.topology_description.is_none(),
            "{row}"
        );
        let seed = coordinator
            .cached_streams_seed("app")
            .expect("a cached seed");
        assert!(
            seed.description_epochs == DescriptionEpochs::default(),
            "{row}"
        );
    }

    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let empty = coordinator.get_or_create_streams("app");
    assert!(
        push(&empty, description_push("a", 0, 1)).await
            == (
                codes::GROUP_ID_NOT_FOUND,
                Some("Group app not found.".to_owned())
            )
    );
}

/// A group loaded from the log keeps the epochs its record stored, so its
/// members are not asked again, as in Kafka trunk. The in-memory plugin did
/// not survive the load, so a describe finds no description, as Kafka's
/// reference plugin loses its map with the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_group_is_not_asked_for_a_stored_epoch() {
    let epochs = |stored, failed| DescriptionEpochs { stored, failed };
    // (row, the loaded epochs, the topology epoch, whether the member is asked)
    let rows = [
        ("stored", epochs(0, -1), 0, false),
        ("failed for good", epochs(-1, 0), 0, false),
        ("uncertain", epochs(-2, -1), 0, true),
        ("stored at an older epoch", epochs(0, -1), 1, true),
        ("none", epochs(-1, -1), 0, true),
    ];
    for (row, description_epochs, topology_epoch, asked) in rows {
        let mut request = join("a");
        let topology = request
            .topology
            .as_mut()
            .expect("a join sends its topology");
        topology.epoch = topology_epoch;
        let loaded = to_stored_topology(topology);
        let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
        let handle = coordinator.get_or_create_streams("app");
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                description_epochs,
                topology: Some(loaded),
                ..StreamsGroupSeed::default()
            }))
            .await
            .expect("the actor runs");

        let response = heartbeat(&handle, request, 1).await;

        assert!(response.topology_description_required == asked, "{row}");
        assert!(
            describe(&handle).await.topology_description.is_none(),
            "{row}"
        );
    }
}

/// A group loaded from a record that Kafka trunk, or a broker with the
/// plugin, wrote: a broker with the plugin writes its description epochs back
/// in tags 2 and 3, and one without it writes Kafka 4.3.1's record, the same
/// value without them, while it keeps the epochs it read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_description_epochs_are_written_with_the_plugin_only() {
    let replayed = DescriptionEpochs {
        stored: 2,
        failed: 1,
    };
    let mut written = Vec::new();
    for plugin in [
        TopologyDescriptionPlugin::None,
        TopologyDescriptionPlugin::InMemory,
    ] {
        let CoordinatorAndLog { coordinator, log } = coordinator_and_log(plugin);
        let handle = coordinator.get_or_create_streams("app");
        let mut request = join("a");
        let loaded = to_stored_topology(request.topology.as_mut().expect("a topology"));
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                description_epochs: replayed,
                topology: Some(loaded),
                ..StreamsGroupSeed::new_group()
            }))
            .await
            .expect("the actor runs");

        heartbeat(&handle, request, 1).await;

        let seed = coordinator
            .cached_streams_seed("app")
            .expect("a cached seed");
        assert!(seed.description_epochs == replayed, "{plugin:?}");
        written.push(last_group_metadata(&log).await);
    }
    let [without_plugin, with_plugin] = <[_; 2]>::try_from(written).expect("two records");
    assert!(without_plugin.description == DescriptionEpochs::default());
    assert!(
        with_plugin
            == StreamsGroupMetadataValue {
                description: replayed,
                ..without_plugin
            }
    );
}

/// An independent native backend: shared across actor/coordinator instances,
/// and able to fail each SPI operation independently.
#[derive(Debug, Default)]
struct NativeStore {
    intent_log: Option<Arc<InMemoryOffsetsLog>>,
    delete_barrier: Option<(Notify, Notify)>,
    value: Mutex<Option<StoredDescription>>,
    set_failure: Mutex<Option<StorageFailure>>,
    fail_get: AtomicBool,
    fail_delete: AtomicBool,
}

impl TopologyDescriptionBackend for NativeStore {
    fn set<'a>(
        &'a self,
        _: &'a str,
        epoch: i32,
        description: &'a TopologyDescription,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if let Some(log) = &self.intent_log {
                assert!(
                    last_group_metadata(log).await.description.stored
                        == DescriptionEpochs::UNCERTAIN
                );
            }
            let failure = *self.set_failure.lock().expect("lock");
            if let Some(failure) = failure {
                return Err(failure);
            }
            *self.value.lock().expect("lock") = Some(StoredDescription {
                topology_epoch: epoch,
                description: description.clone(),
            });
            Ok(())
        })
    }

    fn get<'a>(&'a self, _: &'a str, epoch: i32) -> BackendFuture<'a, Option<TopologyDescription>> {
        Box::pin(async move {
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(StorageFailure::Transient);
            }
            Ok(self
                .value
                .lock()
                .expect("lock")
                .as_ref()
                .filter(|v| v.topology_epoch == epoch)
                .map(|v| v.description.clone()))
        })
    }

    fn delete<'a>(&'a self, _: &'a str) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if let Some((entered, release)) = &self.delete_barrier {
                entered.notify_one();
                release.notified().await;
            }
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(StorageFailure::Transient);
            }
            *self.value.lock().expect("lock") = None;
            Ok(())
        })
    }
}

fn native_coordinator(
    backend: Arc<NativeStore>,
    log: Arc<InMemoryOffsetsLog>,
) -> Arc<GroupCoordinator> {
    let coordinator = coordinator_with_log(
        StreamsGroupConfig {
            topology_description_backend: Some(backend),
            ..undelayed()
        },
        log,
    );
    coordinator.mark_streams("app");
    coordinator
}

fn native_logging_setup(
    backend: Arc<NativeStore>,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    (native_coordinator(backend, Arc::clone(&log)), log)
}

fn native_setup() -> (Arc<NativeStore>, Arc<GroupCoordinator>) {
    let backend = Arc::new(NativeStore::default());
    let coordinator = native_coordinator(
        Arc::clone(&backend),
        Arc::new(InMemoryOffsetsLog::default()),
    );
    (backend, coordinator)
}

async fn store_native_topology(
    coordinator: &Arc<GroupCoordinator>,
) -> Arc<StreamsGroupActorHandle> {
    let handle = coordinator.get_or_create_streams("app");
    heartbeat(&handle, join("a"), 1).await;
    assert!(push(&handle, description_push("a", 0, 1)).await.0 == codes::NONE);
    handle
}

async fn store_shutdown_and_snapshot(
    coordinator: &Arc<GroupCoordinator>,
) -> (Option<TopologyDescription>, StreamsGroupSeed) {
    let handle = store_native_topology(coordinator).await;
    let snapshot = (
        describe(&handle).await.topology_description,
        coordinator
            .cached_streams_seed("app")
            .expect("durable seed"),
    );
    coordinator.shutdown_all().await;
    snapshot
}

async fn leave_native_member(handle: &StreamsGroupActorHandle) {
    heartbeat(
        handle,
        StreamsGroupHeartbeatRequest {
            group_id: "app".into(),
            member_id: "a".into(),
            member_epoch: -1,
            ..Default::default()
        },
        1,
    )
    .await;
}

#[tokio::test]
async fn native_store_failures_leave_durable_uncertainty_and_permanent_epoch() {
    for failure in [StorageFailure::Transient, StorageFailure::Permanent] {
        let backend = Arc::new(NativeStore::default());
        *backend.set_failure.lock().expect("lock") = Some(failure);
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coordinator = native_coordinator(Arc::clone(&backend), Arc::clone(&log));
        let handle = coordinator.get_or_create_streams("app");
        let joined = heartbeat(&handle, join("a"), 1).await;
        assert!(joined.topology_description_required);
        assert!(
            push(&handle, description_push("a", 0, 1)).await.0
                == codes::STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED
        );
        let metadata = last_group_metadata(&log).await;
        assert!(metadata.description.stored == DescriptionEpochs::UNCERTAIN);
        assert!(
            metadata.description.failed
                == if failure == StorageFailure::Permanent {
                    0
                } else {
                    -1
                }
        );
        assert!(
            !heartbeat(&handle, steady("a", joined.member_epoch), 1)
                .await
                .topology_description_required
        );
        assert!(describe(&handle).await.topology_description.is_none());
    }
}

#[tokio::test]
async fn native_get_failure_is_error_not_missing_and_missing_is_not_error() {
    let (backend, coordinator) = native_setup();
    let handle = store_native_topology(&coordinator).await;
    backend.fail_get.store(true, Ordering::SeqCst);
    assert!(describe(&handle).await.topology_description_error);
    backend.fail_get.store(false, Ordering::SeqCst);
    *backend.value.lock().expect("lock") = None;
    let view = describe(&handle).await;
    assert!(!view.topology_description_error && view.topology_description.is_none());
}

#[tokio::test]
async fn native_cleanup_failure_is_retried_and_never_claims_reliable_storage() {
    let backend = Arc::new(NativeStore::default());
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coordinator = native_coordinator(Arc::clone(&backend), Arc::clone(&log));
    let handle = store_native_topology(&coordinator).await;
    leave_native_member(&handle).await;
    backend.fail_delete.store(true, Ordering::SeqCst);
    assert!(
        !ask(&handle.tx, StreamsGroupActorMessage::DeleteDescription)
            .await
            .expect("answer")
            .expect("log")
    );
    assert!(last_group_metadata(&log).await.description.stored == DescriptionEpochs::UNCERTAIN);
    assert!(describe(&handle).await.topology_description.is_none());
    backend.fail_delete.store(false, Ordering::SeqCst);
    assert!(
        ask(&handle.tx, StreamsGroupActorMessage::DeleteDescription)
            .await
            .expect("answer")
            .expect("log")
    );
    assert!(last_group_metadata(&log).await.description.stored == DescriptionEpochs::NONE);
    assert!(backend.value.lock().expect("lock").is_none());
}

#[tokio::test]
async fn shared_native_backend_survives_coordinator_movement_and_epoch_change() {
    let backend = Arc::new(NativeStore::default());
    let first = native_coordinator(
        Arc::clone(&backend),
        Arc::new(InMemoryOffsetsLog::default()),
    );
    let (expected, seed) = store_shutdown_and_snapshot(&first).await;
    let second = native_coordinator(
        Arc::clone(&backend),
        Arc::new(InMemoryOffsetsLog::default()),
    );
    let moved = second.get_or_create_streams("app");
    moved
        .tx
        .send(StreamsGroupActorMessage::Seed(seed.clone()))
        .await
        .expect("seed moved actor");
    assert!(describe(&moved).await.topology_description == expected);
    // Live topology mutation is rejected by Kafka too. Replay a later
    // topology epoch to exercise storage fencing without changing that rule.
    let mut advanced = seed;
    advanced.topology.as_mut().expect("stored topology").epoch = 1;
    moved
        .tx
        .send(StreamsGroupActorMessage::Seed(advanced))
        .await
        .expect("advance replay");
    let mut changed = join("a");
    changed.topology.as_mut().expect("topology").epoch = 1;
    let response = heartbeat(&moved, changed, 1).await;
    assert!(response.topology_description_required);
    assert!(describe(&moved).await.topology_description.is_none());
    assert!(push(&moved, description_push("a", 1, 1)).await.0 == codes::NONE);
    assert!(
        backend
            .value
            .lock()
            .expect("lock")
            .as_ref()
            .expect("stored")
            .topology_epoch
            == 1
    );
}

#[tokio::test]
async fn native_delete_failure_preserves_group_and_success_allows_clean_recreation() {
    use crate::coordinator::DeleteGroupError;
    let (backend, coordinator) = native_setup();
    let handle = store_native_topology(&coordinator).await;
    let leave = StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: "a".into(),
        member_epoch: -1,
        ..Default::default()
    };
    heartbeat(&handle, leave, 1).await;
    backend.fail_delete.store(true, Ordering::SeqCst);
    assert!(coordinator.delete_group("app").await == Err(DeleteGroupError::TopologyDescription));
    assert!(coordinator.streams_description_runtime.contains_key("app"));
    assert!(coordinator.find_streams("app").is_some());
    backend.fail_delete.store(false, Ordering::SeqCst);
    assert!(coordinator.delete_group("app").await == Ok(()));
    assert!(backend.value.lock().expect("lock").is_none());
    let recreated = coordinator.get_or_create_streams("app");
    assert!(
        heartbeat(&recreated, join("b"), 1)
            .await
            .topology_description_required
    );
    assert!(describe(&recreated).await.topology_description.is_none());
}

#[tokio::test]
async fn failed_classic_conversion_keeps_streams_group_and_throttles_cleanup() {
    use crate::coordinator::unified::streams::migration::DowngradeOutcome;
    let (backend, coordinator) = native_setup();
    let handle = store_native_topology(&coordinator).await;
    leave_native_member(&handle).await;
    backend.fail_delete.store(true, Ordering::SeqCst);
    assert!(
        coordinator
            .try_convert_streams_to_classic("app", "", 0)
            .await
            .expect("conversion")
            == DowngradeOutcome::RetryCleanup
    );
    assert!(coordinator.find_streams("app").is_some());
    // Even if the backend immediately recovers, retries cannot hammer it.
    backend.fail_delete.store(false, Ordering::SeqCst);
    assert!(
        coordinator
            .try_convert_streams_to_classic("app", "", 0)
            .await
            .expect("retry")
            == DowngradeOutcome::RetryCleanup
    );
    assert!(backend.value.lock().expect("lock").is_some());
}

#[tokio::test]
async fn plugin_sees_durable_uncertain_before_external_side_effect() {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let backend = Arc::new(NativeStore {
        intent_log: Some(Arc::clone(&log)),
        ..Default::default()
    });
    let coordinator = native_coordinator(backend, Arc::clone(&log));
    let _handle = store_native_topology(&coordinator).await;
    assert!(last_group_metadata(&log).await.description.stored == 0);
}

/// Reopen the persistent implementation, not a retained Arc, on a new actor.
#[tokio::test]
async fn filesystem_description_survives_coordinator_restart() {
    let directory = tempfile::tempdir().expect("directory");
    let backend = FileTopologyDescriptionBackend::new(directory.path())
        .await
        .expect("backend");
    let first = coordinator_with_log(
        StreamsGroupConfig {
            topology_description_backend: Some(Arc::new(backend)),
            ..undelayed()
        },
        Arc::new(InMemoryOffsetsLog::default()),
    );
    let (expected, seed) = store_shutdown_and_snapshot(&first).await;
    drop(first);

    let reopened = FileTopologyDescriptionBackend::new(directory.path())
        .await
        .expect("reopen backend");
    let second = coordinator_with_log(
        StreamsGroupConfig {
            topology_description_backend: Some(Arc::new(reopened)),
            ..undelayed()
        },
        Arc::new(InMemoryOffsetsLog::default()),
    );
    let restarted = second.get_or_create_streams("app");
    restarted
        .tx
        .send(StreamsGroupActorMessage::Seed(seed))
        .await
        .expect("replay");
    assert!(describe(&restarted).await.topology_description == expected);
    second.shutdown_all().await;
}

async fn commit_retention_offset(coordinator: &Arc<GroupCoordinator>) {
    use crate::coordinator::unified::{actor::GroupActorMessage, classic_state::OffsetEntry};
    let home = coordinator.get_or_create_classic("app");
    ask(&home.tx, |reply| GroupActorMessage::UpdateCommitted {
        entries: vec![(("input".into(), 0), OffsetEntry {
            offset: 42.into(),
            leader_epoch: -1,
            metadata: String::new(),
            commit_timestamp_ms: 0,
            expire_timestamp_ms: None,
            topic_id: None,
        })],
        reply,
    })
    .await
    .expect("commit");
}

#[tokio::test]
async fn natural_cleanup_waits_for_offsets_and_retries_current_epoch_delete() {
    use crate::coordinator::retention::sweep;
    let (backend, coordinator) = native_setup();
    let handle = store_native_topology(&coordinator).await;
    commit_retention_offset(&coordinator).await;
    // An expired offset must not authorize cleanup underneath a live member.
    assert!(sweep(&coordinator, |_| true, 101, 100, 0).await.is_empty());
    leave_native_member(&handle).await;
    // The current stored epoch must survive until the committed offset expires.
    assert!(sweep(&coordinator, |_| true, 99, 100, 0).await.is_empty());
    assert!(backend.value.lock().expect("lock").is_some());
    backend.fail_delete.store(true, Ordering::Relaxed);
    let failed = sweep(&coordinator, |_| true, 101, 100, 0).await;
    assert!(failed.len() == 1);
    assert!(!failed[0].outcome.group_deleted);
    assert!(failed[0].outcome.reaped == vec![("input".to_owned(), 0)]);
    assert!(backend.value.lock().expect("lock").is_some());
    assert!(coordinator.find_streams("app").is_some());
    backend.fail_delete.store(false, Ordering::Relaxed);
    let retried = sweep(&coordinator, |_| true, 102, 100, 0).await;
    assert!(retried.len() == 1 && retried[0].outcome.group_deleted);
    assert!(backend.value.lock().expect("lock").is_none());
    assert!(coordinator.find_streams("app").is_none());
    assert!(coordinator.find("app").is_none());
}

#[tokio::test]
async fn unresolved_transaction_protects_topology_until_its_reservation_is_released() {
    use crate::coordinator::{
        retention::sweep,
        unified::actor::{GroupActorMessage, TxnOffsetReservation},
    };
    let (backend, coordinator) = native_setup();
    let handle = store_native_topology(&coordinator).await;
    commit_retention_offset(&coordinator).await;
    leave_native_member(&handle).await;
    let home = coordinator.find("app").expect("offset home");
    for reserve in [true, false] {
        ask(&home.tx, |reply| {
            GroupActorMessage::TxnOffsetReservation(TxnOffsetReservation {
                producer_id: 7,
                keys: vec![("input".into(), 0)],
                reserve,
                reply,
            })
        })
        .await
        .expect("reservation");
        let rows = sweep(&coordinator, |_| true, 101, 100, 0).await;
        if reserve {
            assert!(rows.is_empty());
            assert!(backend.value.lock().expect("lock").is_some());
            assert!(coordinator.find_streams("app").is_some());
        } else {
            assert!(rows.len() == 1 && rows[0].outcome.group_deleted);
            assert!(backend.value.lock().expect("lock").is_none());
        }
    }
}

mod model;

#[tokio::test]
async fn explicit_delete_tombstones_committed_and_pending_keys_and_retires_handle() {
    use crate::coordinator::unified::{
        actor::{GroupActorMessage, TxnOffsetReservation},
        persistence::OffsetCommitValue,
    };
    let backend = Arc::new(NativeStore::default());
    let (coordinator, log) = native_logging_setup(backend);
    let handle = store_native_topology(&coordinator).await;
    commit_retention_offset(&coordinator).await;
    leave_native_member(&handle).await;
    let home = coordinator.find("app").expect("offset home");
    ask(&home.tx, |reply| {
        GroupActorMessage::TxnOffsetReservation(TxnOffsetReservation {
            producer_id: 7,
            keys: vec![("pending".into(), 1)],
            reserve: true,
            reply,
        })
    })
    .await
    .expect("reserve");
    coordinator.delete_group("app").await.expect("delete");
    let batches = log.batches().await;
    let deletion = batches.last().expect("durable delete");
    for (topic, partition) in [("input", 0), ("pending", 1)] {
        let key = OffsetCommitValue::encode_key("app", topic, partition).expect("key");
        assert!(
            deletion
                .records
                .iter()
                .any(|r| r.key.as_ref() == Some(&key) && r.value.is_none())
        );
    }
    assert!(deletion.records.iter().any(|r| {
        r.key
            .as_deref()
            .is_some_and(|key| matches!(parse_key(key), Ok(Key::Streams(_))))
            && r.value.is_none()
    }));
    // A request already holding the retired mailbox cannot mutate storage.
    let count = batches.len();
    assert!(
        ask(&handle.tx, |reply| StreamsGroupActorMessage::Describe {
            reply
        })
        .await
        .is_err()
    );
    assert!(log.batches().await.len() == count);
    assert!(coordinator.group_type("app").is_none());
}

#[tokio::test]
async fn explicit_conversion_retires_old_actor_before_classic_join_can_reuse_it() {
    use crate::coordinator::{
        DeleteGroupError,
        unified::{GroupType, streams::migration::DowngradeOutcome},
    };
    let (_, coordinator) = native_setup();
    let handle = store_native_topology(&coordinator).await;
    leave_native_member(&handle).await;
    assert!(
        coordinator
            .try_convert_streams_to_classic("app", "", 1)
            .await
            .expect("convert")
            == DowngradeOutcome::Converted
    );
    assert!(coordinator.group_type("app") == Some(GroupType::Classic));
    assert!(!coordinator.streams_description_runtime.contains_key("app"));
    assert!(
        ask(&handle.tx, |reply| StreamsGroupActorMessage::Describe {
            reply
        })
        .await
        .is_err()
    );
    assert!(coordinator.find_streams("app").is_none());
    // Simulate a wire lookup that observed Streams just before conversion,
    // but acquires its handle after conversion. It must not steal ownership.
    let stale_lookup = coordinator.get_or_create_streams("app");
    assert!(
        ask(&stale_lookup.tx, StreamsGroupActorMessage::Delete)
            .await
            .expect("stale delete")
            == Err(DeleteGroupError::NotFound)
    );
    assert!(
        heartbeat_result_at(&stale_lookup, join("late"), 1)
            .await
            .response
            .error_code
            == codes::GROUP_ID_NOT_FOUND
    );
    assert!(coordinator.group_type("app") == Some(GroupType::Classic));
    assert!(!coordinator.streams_description_runtime.contains_key("app"));
}

#[tokio::test]
async fn join_queued_during_backend_cleanup_cannot_revive_retired_streams_actor() {
    for conversion in [false, true] {
        let backend = Arc::new(NativeStore {
            delete_barrier: Some((Notify::new(), Notify::new())),
            ..Default::default()
        });
        let (coordinator, _log) = native_logging_setup(Arc::clone(&backend));
        let handle = store_native_topology(&coordinator).await;
        leave_native_member(&handle).await;
        let owner = Arc::clone(&coordinator);
        let transition = tokio::spawn(async move {
            if conversion {
                owner
                    .try_convert_streams_to_classic("app", "", 1)
                    .await
                    .expect("convert");
            } else {
                owner.delete_group("app").await.expect("delete");
            }
        });
        let (entered, release) = backend.delete_barrier.as_ref().expect("barrier");
        entered.notified().await;
        let stale = Arc::clone(&handle);
        // The mailbox is definitely populated while deletion is inside its
        // external backend call; the actor must retire without consuming it.
        let (reply, answer) = oneshot::channel();
        stale
            .tx
            .send(StreamsGroupActorMessage::Heartbeat {
                request: Box::new(join("racer")),
                version: 1,
                client_id: String::new(),
                client_host: String::new(),
                reply,
            })
            .await
            .expect("queued heartbeat");
        release.notify_one();
        transition.await.expect("task");
        assert!(answer.await.is_err());
        assert!(coordinator.find_streams("app").is_none());
        assert!(backend.value.lock().expect("lock").is_none());
    }
}

#[tokio::test]
async fn reference_plugin_description_survives_same_broker_actor_reload() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let handle = store_native_topology(&coordinator).await;
    let before = describe(&handle).await.topology_description;
    assert!(before.is_some());
    ask(&handle.tx, StreamsGroupActorMessage::Shutdown)
        .await
        .expect("shutdown");
    handle.tx.closed().await;
    let replacement = coordinator.find_live_streams("app").expect("replacement");
    assert!(!Arc::ptr_eq(&handle, &replacement));
    assert!(describe(&replacement).await.topology_description == before);
}

#[tokio::test]
async fn solicitation_window_survives_same_broker_actor_reload() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let handle = coordinator.get_or_create_streams("app");
    let joined = heartbeat(&handle, join("a"), 1).await;
    assert!(joined.topology_description_required);
    ask(&handle.tx, StreamsGroupActorMessage::Shutdown)
        .await
        .expect("shutdown");
    handle.tx.closed().await;
    let replacement = coordinator.find_live_streams("app").expect("replacement");
    let response = heartbeat(&replacement, steady("a", joined.member_epoch), 1).await;
    assert!(response.error_code == codes::NONE);
    assert!(!response.topology_description_required);
}

#[tokio::test]
async fn deleting_unstored_group_clears_shared_solicitation_window() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    coordinator.mark_streams("app");
    let handle = coordinator.get_or_create_streams("app");
    assert!(
        heartbeat(&handle, join("a"), 1)
            .await
            .topology_description_required
    );
    leave_native_member(&handle).await;
    coordinator.delete_group("app").await.expect("delete");
    assert!(!coordinator.streams_description_runtime.contains_key("app"));
    let replacement = coordinator.get_or_create_streams("app");
    assert!(
        heartbeat(&replacement, join("b"), 1)
            .await
            .topology_description_required
    );
}

#[test]
fn old_actor_retirement_preserves_replacement_runtime() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let actor = ActorState::new("app".into());
    let replacement = Arc::default();
    coordinator
        .streams_description_runtime
        .insert("app".into(), Arc::clone(&replacement));
    retire_runtime(&actor, &coordinator);
    assert!(Arc::ptr_eq(
        coordinator
            .streams_description_runtime
            .get("app")
            .expect("replacement retained")
            .value(),
        &replacement
    ));
}

#[tokio::test]
async fn refused_first_heartbeat_retires_empty_shared_runtime() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let handle = coordinator.get_or_create_streams("app");
    let response = heartbeat_result_at(&handle, steady("unknown", -1), 1)
        .await
        .response;
    assert!(response.error_code == codes::UNKNOWN_MEMBER_ID);
    handle.tx.closed().await;
    assert!(coordinator.find_streams("app").is_none());
    assert!(!coordinator.streams_description_runtime.contains_key("app"));
}

#[tokio::test]
async fn abandoned_actor_cannot_retire_a_newer_closed_mailbox() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    coordinator.mark_streams("app");
    let old = coordinator.get_or_create_streams("app");
    heartbeat(&old, join("a"), 1).await;
    ask(&old.tx, StreamsGroupActorMessage::Shutdown)
        .await
        .expect("shutdown");
    old.tx.closed().await;
    let replacement = coordinator.find_live_streams("app").expect("replacement");
    ask(&replacement.tx, StreamsGroupActorMessage::Shutdown)
        .await
        .expect("shutdown");
    replacement.tx.closed().await;
    super::super::forget_abandoned_group(&coordinator, &ActorState::new("app".into()), &old.owner);
    assert!(Arc::ptr_eq(
        &coordinator
            .find_streams("app")
            .expect("registered replacement"),
        &replacement
    ));
    assert!(coordinator.streams_description_runtime.contains_key("app"));
    assert!(coordinator.group_type("app") == Some(GroupType::Streams));
}
