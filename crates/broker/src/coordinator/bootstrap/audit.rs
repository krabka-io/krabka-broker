//! Creation and placement of the `__krabka_audit` internal topic.
//!
//! The audit topic is bootstrapped on its own schedule, separate from
//! `__consumer_offsets`, because it is optional and it places one partition on
//! every registered broker instead of spreading a fixed partition count. Each
//! partition has one replica, so the broker that holds it leads it and writes
//! its own audit records to it locally.
//!
//! Two paths keep that placement:
//!
//! - [`bootstrap_audit_topic`] runs once at startup. It creates the topic with
//!   one partition for every broker that is registered at that moment, and on
//!   a broker it waits for this broker's partition to reach the local image.
//! - [`spawn_audit_placement`] runs on every node with the controller role.
//!   While the node leads the metadata quorum, it adds one partition for each
//!   registered broker that holds no audit partition, as a `CreatePartitions`
//!   would: one `PartitionRecord` at the next index, and no `TopicRecord`. A
//!   broker that registers after the topic exists gets its partition this way.
//!
//! Every partition has one replica, so the topic declares
//! `min.insync.replicas=1`. With the cluster default of 2, every audit
//! partition would count as under min ISR, as Kafka counts any partition whose
//! ISR is smaller than its minimum. The creation batch carries the override,
//! and the placement path adds it to a topic that an earlier release created
//! without it. An override that an operator set is kept.

use {
    crate::{
        config::BrokerConfig, config_keys::MIN_INSYNC_REPLICAS, error::BrokerError,
        handlers::forward_to_controller::guarded_operation_on_source,
        metadata_source::MetadataSource,
    },
    krabka_ids::PartitionIndex,
    krabka_metadata::{
        MetadataError, MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord,
        TopicRecord,
    },
    krabka_raft::{NodeId, RaftError},
    krabka_units::convert::TimeExt as _,
    std::{
        collections::{BTreeMap, HashSet},
        sync::Arc,
    },
    tokio::time::Instant,
    tokio_util::sync::CancellationToken,
};

#[cfg(test)]
mod tests;

/// Create `__krabka_audit` if it is missing, and on a broker wait for the
/// audit partition that this broker holds.
///
/// The topic gets one partition per registered broker at RF=1: the i-th
/// broker in ascending node-id order leads partition `i`. A broker that
/// registers later gets its partition from [`spawn_audit_placement`] on the
/// quorum leader.
///
/// A node with the controller role creates the topic only when it is the
/// quorum leader. A broker-only node never leads the quorum, so it submits its
/// own batch, which its metadata source forwards to the leader. Without that
/// path, a cluster with isolated controllers has no node that can place the
/// topic on a broker. The leader counts a topic that a pending batch creates
/// as an existing topic, so two racing submits create the topic once, and the
/// function treats the `TopicExists` refusal as a success.
///
/// Only a registered broker gets a replica. Before any registration reaches
/// the image, the local node stands in for the broker list, but only when the
/// local node is a broker. A controller-only node hosts no replica. With no
/// registered broker, it submits nothing, and the first broker-only node to
/// start creates the topic.
///
/// On the quorum leader, the function then runs one placement pass, which
/// gives this node its partition and the topic its `min.insync.replicas`
/// override when the topic already existed without them. The pass runs here
/// and not only in [`spawn_audit_placement`] because that task starts after
/// the audit pipeline has read the image.
///
/// On a broker, the function last waits up to `audit_partition_wait_timeout`
/// for a partition with this broker as its replica to reach the local image.
/// The audit pipeline reads that image once, right after startup calls this
/// function, to find the partition that this broker leads. A broker that has
/// no audit partition when the wait ends drops its audit records until it
/// restarts.
///
/// The function returns `Ok(())` at once when `config.audit_enabled` is
/// `false`.
///
/// # Errors
///
/// Returns [`BrokerError::Startup`] when the controller refuses the creation
/// batch for a reason other than an existing topic.
pub async fn bootstrap_audit_topic(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
) -> Result<(), BrokerError> {
    if !config.audit_enabled {
        return Ok(());
    }

    // Copy out the leader id before any `.await` so we don't hold the `Ref`
    // across an await point.
    let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
    if am_leader || !config.is_controller() {
        create_audit_topic(config, &**controller).await?;
    }
    if am_leader {
        let deadline = Instant::now() + config.audit_partition_wait_timeout.to_std();
        place_audit_partitions(&config.audit_topic, &**controller, deadline).await;
    }
    if config.is_broker() {
        wait_for_local_audit_partition(config, &**controller).await;
    }
    Ok(())
}

/// Submit the batch that creates the audit topic, unless the local image
/// already has the topic or no broker can hold a partition.
async fn create_audit_topic(
    config: &BrokerConfig,
    controller: &dyn MetadataSource,
) -> Result<(), BrokerError> {
    let replicas = {
        let image = controller.current_image();
        // Already created (idempotent restart or another node beat us).
        if image.topic(&config.audit_topic).is_some() {
            return Ok(());
        }
        audit_replicas(&image, config.is_broker().then_some(config.node_id))
    };
    if replicas.is_empty() {
        tracing::info!(
            topic = %config.audit_topic,
            "no broker is registered, so this controller-only node does not create the audit topic"
        );
        return Ok(());
    }

    let records = audit_topic_records(&config.audit_topic, krabka_format::random_uuid(), &replicas);
    match controller.submit_change(records).await {
        // Idempotent: another node or a restart already created it.
        Ok(_) | Err(RaftError::Metadata(MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(BrokerError::Startup(e.to_string())),
    }
}

/// Keep the audit placement whole while this node leads the metadata quorum.
///
/// The task runs one placement pass whenever this node becomes the leader and
/// on every metadata change while it leads. A pass adds a partition for each
/// registered broker that holds none, and the `min.insync.replicas` override
/// when the topic lacks it; see [`audit_placement_records`]. A pass submits
/// with `submit_change_if_image`, which the leader refuses when its image has
/// moved on since the pass read it. Two passes therefore never place two
/// brokers at the same partition index, and a pass never undoes a change that
/// committed after its read, such as an operator's `CreatePartitions` or
/// `AlterConfigs`.
///
/// A node without the controller role never leads, and a node with audit
/// disabled does not place, so neither starts the task. The task stops when
/// `shutdown` is cancelled or the metadata source closes.
pub(crate) fn spawn_audit_placement(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    shutdown: CancellationToken,
) {
    if !config.audit_enabled || !config.is_controller() {
        return;
    }
    let node_id = config.node_id;
    let topic = config.audit_topic.clone();
    let pass_timeout = config.audit_partition_wait_timeout.to_std();
    let controller = Arc::clone(controller);
    tokio::spawn(async move {
        let mut images = controller.watch_image();
        let mut leader = controller.watch_leader();
        loop {
            images.mark_unchanged();
            let leads = *leader.borrow_and_update() == Some(node_id);
            if leads {
                place_audit_partitions(&topic, &*controller, Instant::now() + pass_timeout).await;
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                changed = images.changed() => if changed.is_err() { return },
                changed = leader.changed() => if changed.is_err() { return },
            }
        }
    });
}

/// One placement pass on the quorum leader, retried on a moved image until
/// `deadline`.
///
/// A failure is logged, not returned: the pass runs again on the next
/// metadata change, and a broker without its partition still starts.
async fn place_audit_partitions(topic: &str, controller: &dyn MetadataSource, deadline: Instant) {
    let outcome = guarded_operation_on_source(controller, deadline, |image| async move {
        let records = audit_placement_records(&image, topic);
        if records.is_empty() {
            return Ok(false);
        }
        controller
            .submit_change_if_image(records, image)
            .await
            .map(|_| true)
    })
    .await;
    match outcome {
        Ok(Ok(true)) => tracing::info!(%topic, "placed the missing audit partitions"),
        // Nothing was missing, or leadership moved during the pass and the
        // new leader runs its own.
        Ok(Ok(false) | Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => {}
        Ok(Err(error)) => {
            tracing::warn!(%topic, %error, "the audit placement pass failed");
        }
        Err(_elapsed) => tracing::warn!(
            %topic,
            "the audit placement pass did not commit before its deadline"
        ),
    }
}

/// The records that complete the placement of an existing audit topic, or
/// none when it is complete or missing.
///
/// Each registered broker that is the replica of no audit partition gets one
/// partition, in ascending node-id order, at the indices that follow the
/// topic's last partition. Membership counts, not leadership: a restarted
/// broker that is still fenced leads nothing, but it keeps its partition.
///
/// The topic config gets `min.insync.replicas=1` when it has no
/// `min.insync.replicas` override. A `TopicConfigRecord` replaces the whole
/// override map, so the record carries the topic's other overrides too.
fn audit_placement_records(image: &MetadataImage, topic: &str) -> Vec<MetadataRecord> {
    if image.topic(topic).is_none() {
        return Vec::new();
    }
    let hosted: HashSet<NodeId> = image
        .partitions_of(topic)
        .flat_map(|partition| partition.replicas.iter().copied())
        .collect();
    let mut unplaced: Vec<NodeId> = image
        .brokers()
        .map(|broker| broker.node_id)
        .filter(|node_id| !hosted.contains(node_id))
        .collect();
    unplaced.sort_unstable();
    let mut records: Vec<MetadataRecord> = (image.topic_partition_count(topic)..)
        .zip(unplaced)
        .map(|(partition, replica)| audit_partition_record(topic, partition, replica))
        .collect();
    let overrides = image.topic_config(topic);
    if !overrides.is_some_and(|overrides| overrides.contains_key(MIN_INSYNC_REPLICAS)) {
        let mut overrides = overrides.cloned().unwrap_or_default();
        overrides.extend(audit_topic_configs());
        records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.to_owned(),
            overrides,
        }));
    }
    records
}

/// The replica of each audit partition, in partition order: every registered
/// broker, in ascending node-id order.
///
/// `local_broker` is this node when this node is a broker, and `None` on a
/// node whose `process.roles` exclude `broker`. It stands in for the list
/// while the image holds no broker registration. Kafka never places a replica
/// on a node that is not a registered broker. A controller-only node that
/// named itself here would lead a partition that no broker serves, and every
/// `AssignReplicasToDirs` for it would fail with `BROKER_ID_NOT_REGISTERED`.
fn audit_replicas(image: &MetadataImage, local_broker: Option<NodeId>) -> Vec<NodeId> {
    let mut brokers: Vec<NodeId> = image.brokers().map(|broker| broker.node_id).collect();
    if brokers.is_empty() {
        brokers.extend(local_broker);
    }
    brokers.sort_unstable();
    brokers
}

/// The topic configs of the audit topic: `min.insync.replicas=1`, because
/// every partition has one replica.
fn audit_topic_configs() -> BTreeMap<String, String> {
    BTreeMap::from([(MIN_INSYNC_REPLICAS.to_owned(), "1".to_owned())])
}

/// The batch that creates `topic` at RF=1 with partition `i` on `replicas[i]`,
/// in the order a `CreateTopics` writes it: the topic, its partitions, and its
/// config.
fn audit_topic_records(
    topic: &str,
    topic_id: uuid::Uuid,
    replicas: &[NodeId],
) -> Vec<MetadataRecord> {
    let topic_record = MetadataRecord::V1Topic(TopicRecord {
        name: topic.to_owned(),
        topic_id,
        partitions: i32::try_from(replicas.len()).expect("audit partition count overflows i32"),
        replication_factor: 1,
    });
    let partition_records = (0_i32..)
        .zip(replicas)
        .map(|(partition, &replica)| audit_partition_record(topic, partition, replica));
    let config_record = MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.to_owned(),
        overrides: audit_topic_configs(),
    });
    std::iter::once(topic_record)
        .chain(partition_records)
        .chain(std::iter::once(config_record))
        .collect()
}

/// Audit partition `partition` with `replica` as its one replica, leader and
/// ISR member.
fn audit_partition_record(topic: &str, partition: i32, replica: NodeId) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_owned(),
        partition,
        leader: replica,
        replicas: vec![replica],
        isr: vec![replica],
        leader_epoch: krabka_metadata::LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

/// Wait until the local image carries an audit partition with this broker as
/// its replica, for at most `audit_partition_wait_timeout`.
///
/// A submit returns when the leader applies the batch. A broker-only node
/// sees the batch only after its next metadata fetch, so its image can trail
/// the reply. A broker that registered after the topic was created waits for
/// the quorum leader's placement pass. If the wait runs out, the broker still
/// starts, and the audit pipeline reports that this broker leads no audit
/// partition.
/// The index of the audit partition of `topic` placed on `node`, if any.
///
/// Chosen by replica, not by leader: a restarted broker reads the image before
/// it is unfenced, while its one-replica partition still has no leader.
pub(crate) fn hosted_audit_partition(
    image: &MetadataImage,
    topic: &str,
    node: NodeId,
) -> Option<PartitionIndex> {
    (0_i32..)
        .map_while(|index| image.partition(topic, index).map(|record| (index, record)))
        .find(|(_, record)| record.replicas.contains(&node))
        .map(|(index, _)| PartitionIndex(index))
}

async fn wait_for_local_audit_partition(config: &BrokerConfig, controller: &dyn MetadataSource) {
    let timeout = config.audit_partition_wait_timeout.to_std();
    let mut images = controller.watch_image();
    let published = tokio::time::timeout(
        timeout,
        images.wait_for(|image| {
            hosted_audit_partition(image, &config.audit_topic, config.node_id).is_some()
        }),
    )
    .await
    .is_ok_and(|seen| seen.is_ok());
    if !published {
        tracing::warn!(
            topic = %config.audit_topic,
            ?timeout,
            "no audit partition for this broker reached the local metadata image in time"
        );
    }
}
