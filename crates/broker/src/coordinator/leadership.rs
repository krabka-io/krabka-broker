//! Group-coordinator actor ownership across offsets-partition leader changes.
//!
//! A partition this broker starts to lead is replayed in the background. Until
//! that replay has seeded every group of the partition, the partition is
//! loading: Kafka's `CoordinatorRuntime.withActiveContextOrThrow` answers
//! `COORDINATOR_LOAD_IN_PROGRESS` for a shard in the `LOADING` state, and
//! [`GroupCoordinator::is_loading`] is what the group RPC routing check reads
//! to do the same. Without it a request would find no actor, create an empty
//! one, and have its answer overwritten when the replay seeds the group.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use krabka_ids::PartitionIndex;
use krabka_metadata::{MetadataImage, NodeId};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    GroupCoordinator,
    bootstrap::{OFFSETS_TOPIC, replay_partition},
    partitioner::partition_for_group,
    unified::{
        actor::GroupActorMessage, share::actor::ShareGroupActorMessage,
        streams::actor::StreamsGroupActorMessage,
    },
};
use crate::{metadata_source::MetadataSource, partition_registry::PartitionRegistry};

pub(crate) fn spawn(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    coordinator: Arc<GroupCoordinator>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut images = metadata.watch_image();
        let mut previous_image = images.borrow_and_update().clone();
        let mut led = led_partitions(&previous_image, node_id);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                changed = images.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let image = images.borrow_and_update().clone();
                    let next = led_partitions(&image, node_id);
                    for partition in led.difference(&next).copied().collect::<Vec<_>>() {
                        coordinator.end_any_load(partition);
                        unload_partition(&coordinator, &previous_image, partition).await;
                    }
                    for partition in next.difference(&led).copied().collect::<Vec<_>>() {
                        // Marked before the task starts, so no request routed
                        // after this image lands can slip in ahead of it.
                        let load = LoadGuard::begin(Arc::clone(&coordinator), partition);
                        spawn_partition_load(
                            node_id,
                            Arc::clone(&metadata),
                            Arc::clone(&partitions),
                            load,
                            partition,
                            shutdown.child_token(),
                        );
                    }
                    led = next;
                    previous_image = image;
                }
            }
        }
    });
}

/// Source of the ids that tell one load of a partition from the next.
static NEXT_LOAD_ID: AtomicU64 = AtomicU64::new(0);

impl GroupCoordinator {
    /// `true` while `partition` of `__consumer_offsets` is led here but not
    /// yet replayed.
    pub(crate) fn is_loading(&self, partition: i32) -> bool {
        self.loading_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&partition)
    }

    fn begin_load(&self, partition: i32) -> u64 {
        let load_id = NEXT_LOAD_ID.fetch_add(1, Ordering::Relaxed);
        self.loading_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(partition, load_id);
        load_id
    }

    /// Clears the mark only if the load `load_id` still owns it, so a load
    /// that outlives a lost and regained leadership cannot clear the newer
    /// load's mark.
    fn end_load(&self, partition: i32, load_id: u64) {
        let mut loading = self
            .loading_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if loading.get(&partition) == Some(&load_id) {
            loading.remove(&partition);
        }
    }

    fn end_any_load(&self, partition: PartitionIndex) {
        self.loading_partitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&partition.get());
    }
}

/// The loading mark of one partition load. Dropping it clears the mark,
/// whether the load finished, failed, lost the leadership or was cancelled.
struct LoadGuard {
    coordinator: Arc<GroupCoordinator>,
    partition: i32,
    load_id: u64,
}

impl LoadGuard {
    fn begin(coordinator: Arc<GroupCoordinator>, partition: PartitionIndex) -> Self {
        let load_id = coordinator.begin_load(partition.get());
        Self {
            coordinator,
            partition: partition.get(),
            load_id,
        }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.coordinator.end_load(self.partition, self.load_id);
    }
}

fn spawn_partition_load(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    load: LoadGuard,
    partition: PartitionIndex,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let coordinator = Arc::clone(&load.coordinator);
        let _load = load;
        loop {
            let still_leader = metadata
                .current_image()
                .partition(OFFSETS_TOPIC, partition.get())
                .is_some_and(|record| record.leader == node_id);
            if !still_leader {
                return;
            }
            if partitions.contains(OFFSETS_TOPIC, partition) {
                if let Err(error) = replay_partition(&partitions, &coordinator, partition).await {
                    tracing::error!(
                        partition = partition.get(),
                        %error,
                        "could not load newly-led group coordinator partition"
                    );
                    return;
                }
                let image = metadata.current_image();
                super::topic_deletion::after_partition_load(&coordinator, &image, |group_id| {
                    partition_for_group(&image, group_id) == partition.get()
                })
                .await;
                return;
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
    });
}

fn led_partitions(image: &MetadataImage, node_id: NodeId) -> HashSet<PartitionIndex> {
    image
        .partitions_of(OFFSETS_TOPIC)
        .filter(|partition| partition.leader == node_id)
        .map(|partition| PartitionIndex(partition.partition))
        .collect()
}

async fn unload_partition(
    coordinator: &GroupCoordinator,
    image: &MetadataImage,
    partition: PartitionIndex,
) {
    let timeout = coordinator
        .config
        .shutdown_ack_timeout
        .max(Duration::from_millis(1));
    // Offset-only groups have a classic actor but no protocol-type record.
    // Include every live actor and seed map so losing an offsets partition
    // cannot leave any stale coordinator state reachable on the old leader.
    let mut known_group_ids = HashSet::new();
    known_group_ids.extend(
        coordinator
            .group_types
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(coordinator.groups.iter().map(|entry| entry.key().clone()));
    known_group_ids.extend(
        coordinator
            .share_groups
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_groups
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(coordinator.seeds.iter().map(|entry| entry.key().clone()));
    known_group_ids.extend(
        coordinator
            .seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .share_seeds
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .share_seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_seeds
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    let group_ids: Vec<String> = known_group_ids
        .into_iter()
        .filter(|group_id| partition_for_group(image, group_id) == partition.get())
        .collect();
    for group_id in group_ids {
        if let Some((_, handle)) = coordinator.groups.remove(&group_id) {
            let (reply, ack) = oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::Shutdown(reply))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(timeout, ack).await;
            }
        }
        if let Some((_, handle)) = coordinator.share_groups.remove(&group_id) {
            let (reply, ack) = oneshot::channel();
            if handle
                .tx
                .send(ShareGroupActorMessage::Shutdown(reply))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(timeout, ack).await;
            }
        }
        if let Some((_, handle)) = coordinator.streams_groups.remove(&group_id) {
            let (reply, ack) = oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Shutdown(reply))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(timeout, ack).await;
            }
        }
        coordinator.seeds.remove(&group_id);
        coordinator.seeds_cache.remove(&group_id);
        coordinator.share_seeds.remove(&group_id);
        coordinator.share_seeds_cache.remove(&group_id);
        coordinator.streams_seeds.remove(&group_id);
        coordinator.streams_seeds_cache.remove(&group_id);
        coordinator.group_types.remove(&group_id);
        // The group's offsets partition moved to another broker, so its lag is
        // that broker's to report from here on. Nothing else would release the
        // series: this broker's sampler will never name the group again.
        coordinator.forget_group_metrics(&group_id);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{MetadataRecord, PartitionRecord};

    use super::*;

    #[test]
    fn led_partition_set_tracks_all_local_offsets_leaders() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (partition, leader) in [(0, NodeId(1)), (1, NodeId(2)), (2, NodeId(1))] {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: OFFSETS_TOPIC.into(),
                partition,
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                ..PartitionRecord::default()
            }));
        }
        check!(
            led_partitions(&image, NodeId(1))
                == maplit::hashset! {PartitionIndex(0), PartitionIndex(2)}
        );
    }

    /// Losing the offsets partition that hosts a group ends this broker's
    /// claim to report that group's lag. Nothing else would release the
    /// series: no metadata image records a group's coordinator moving, and
    /// this broker's lag sampler will never name the group again.
    #[tokio::test]
    async fn unloading_a_partition_releases_its_groups_lag_series() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: OFFSETS_TOPIC.into(),
            topic_id: uuid::Uuid::from_u128(2),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: OFFSETS_TOPIC.into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            ..PartitionRecord::default()
        }));
        let metadata: Arc<dyn MetadataSource> = Arc::new(
            crate::test_support::FakeMetadataSource::builder()
                .image(image.clone())
                .leader(Some(NodeId(1)))
                .build(),
        );
        let coordinator = Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(crate::coordinator::unified::ImageMetadataProvider {
                controller: Arc::clone(&metadata),
            }),
            Arc::new(crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let metrics = crate::metrics::BrokerMetrics::new();
        coordinator.set_metrics(metrics.clone());
        // The group has to be live for the unload to find it.
        let _actor = coordinator.get_or_create_classic("billing");
        let label = crate::metrics::ConsumerGroupLabel {
            group_id: "billing".into(),
            topic: "orders".into(),
            partition: 0,
        };
        metrics.publish_consumer_group_lag(&std::collections::HashMap::from([(label.clone(), 33)]));

        unload_partition(&coordinator, &image, PartitionIndex(0)).await;

        check!(metrics.consumer_group_lag.get(&label).is_none());
    }

    /// The group RPCs this broker serves, each reduced to the error code a
    /// client reads first.
    #[derive(Debug, Clone, Copy)]
    enum GroupRpc {
        JoinGroup,
        Heartbeat,
        LeaveGroup,
        OffsetCommit,
        OffsetFetch,
        DeleteGroups,
    }

    async fn call(broker: &crate::broker::Broker, rpc: GroupRpc) -> i16 {
        use krabka_protocol::owned::{
            delete_groups_request::DeleteGroupsRequest,
            delete_groups_response::DeleteGroupsResponse,
            heartbeat_request::HeartbeatRequest,
            heartbeat_response::HeartbeatResponse,
            join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
            join_group_response::JoinGroupResponse,
            leave_group_request::{LeaveGroupRequest, MemberIdentity},
            leave_group_response::LeaveGroupResponse,
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_commit_response::OffsetCommitResponse,
            offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
            offset_fetch_response::OffsetFetchResponse,
        };

        use crate::test_support::{decode_response, encode_request};

        let principal = crate::test_support::principal("alice");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "load-client");
        match rpc {
            GroupRpc::JoinGroup => {
                let request = JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 30_000,
                    protocol_type: "consumer".into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: "range".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let bytes = crate::handlers::join_group::handle(
                    broker,
                    9,
                    1,
                    &encode_request(&request, 9),
                    &ctx,
                )
                .await
                .unwrap();
                decode_response::<JoinGroupResponse>(&bytes, 9).error_code
            }
            GroupRpc::Heartbeat => {
                let request = HeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    generation_id: 1,
                    ..Default::default()
                };
                let bytes = crate::handlers::heartbeat::handle(
                    broker,
                    4,
                    1,
                    &encode_request(&request, 4),
                    &ctx,
                )
                .await
                .unwrap();
                decode_response::<HeartbeatResponse>(&bytes, 4).error_code
            }
            GroupRpc::LeaveGroup => {
                let request = LeaveGroupRequest {
                    group_id: "g".into(),
                    members: vec![MemberIdentity {
                        member_id: "m".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let bytes = crate::handlers::leave_group::handle(
                    broker,
                    5,
                    1,
                    &encode_request(&request, 5),
                    &ctx,
                )
                .await
                .unwrap();
                decode_response::<LeaveGroupResponse>(&bytes, 5).error_code
            }
            GroupRpc::OffsetCommit => {
                let request = OffsetCommitRequest {
                    group_id: "g".into(),
                    generation_id_or_member_epoch: -1,
                    topics: vec![OffsetCommitRequestTopic {
                        // A topic the image knows, so the commit reaches the coordinator.
                        name: OFFSETS_TOPIC.into(),
                        partitions: vec![OffsetCommitRequestPartition {
                            partition_index: 0,
                            committed_offset: 5,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let bytes = crate::handlers::offset_commit::handle(
                    broker,
                    8,
                    1,
                    &encode_request(&request, 8),
                    &ctx,
                )
                .await
                .unwrap();
                decode_response::<OffsetCommitResponse>(&bytes, 8).topics[0].partitions[0]
                    .error_code
            }
            GroupRpc::OffsetFetch => {
                let request = OffsetFetchRequest {
                    groups: vec![OffsetFetchRequestGroup {
                        group_id: "g".into(),
                        topics: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let bytes = crate::handlers::offset_fetch::handle(
                    broker,
                    8,
                    1,
                    &encode_request(&request, 8),
                    &ctx,
                )
                .await
                .unwrap();
                decode_response::<OffsetFetchResponse>(&bytes, 8).groups[0].error_code
            }
            GroupRpc::DeleteGroups => {
                let request = DeleteGroupsRequest {
                    groups_names: vec!["g".into()],
                    ..Default::default()
                };
                let bytes = crate::handlers::delete_groups::handle(
                    broker,
                    2,
                    1,
                    &encode_request(&request, 2),
                    &ctx,
                )
                .await
                .unwrap();
                decode_response::<DeleteGroupsResponse>(&bytes, 2).results[0].error_code
            }
        }
    }

    /// While the group's offsets partition loads, every group RPC answers
    /// `COORDINATOR_LOAD_IN_PROGRESS` (the classic `Heartbeat` `NONE`, as
    /// `GroupCoordinatorService.heartbeat` maps it) and creates no actor. Once
    /// the load ends the same group is served again.
    #[tokio::test]
    async fn group_rpcs_answer_load_in_progress_while_the_partition_loads() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
            config.offsets_topic_replication_factor = 1;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let partition = partition_for_group(&broker.controller.current_image(), "g");
        let load = LoadGuard::begin(
            Arc::clone(&broker.group_coordinator),
            PartitionIndex(partition),
        );

        let rows = [
            (
                GroupRpc::JoinGroup,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (GroupRpc::Heartbeat, crate::codes::NONE),
            (
                GroupRpc::LeaveGroup,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::OffsetCommit,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::OffsetFetch,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::DeleteGroups,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
        ];
        for (rpc, want) in rows {
            check!(call(&broker, rpc).await == want, "{rpc:?}");
            check!(broker.group_coordinator.find("g").is_none(), "{rpc:?}");
        }

        drop(load);

        check!(!broker.group_coordinator.is_loading(partition));
        check!(call(&broker, GroupRpc::JoinGroup).await == crate::codes::MEMBER_ID_REQUIRED);
        broker_handle.shutdown().await;
    }

    /// A load that outlives a lost and regained leadership cannot clear the
    /// mark of the load that replaced it.
    #[test]
    fn a_stale_load_does_not_clear_the_newer_mark() {
        let coordinator = Arc::new(GroupCoordinator::new(
            crate::coordinator::unified::config::NextGenConfig::default(),
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            Arc::new(crate::coordinator::unified::ImageMetadataProvider {
                controller: Arc::new(crate::test_support::FakeMetadataSource::builder().build()),
            }),
            Arc::new(crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let stale = LoadGuard::begin(Arc::clone(&coordinator), PartitionIndex(3));
        coordinator.end_any_load(PartitionIndex(3));
        let current = LoadGuard::begin(Arc::clone(&coordinator), PartitionIndex(3));

        drop(stale);
        check!(coordinator.is_loading(3));
        drop(current);
        check!(!coordinator.is_loading(3));
    }
}
