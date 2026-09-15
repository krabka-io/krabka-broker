//! Per-partition gating and read planning: the authorization, witness,
//! leader-epoch and log-directory checks a requested partition passes
//! before anything reads it, and the [`PendingRead`] each requested tuple
//! resolves to.

use std::sync::Arc;

use krabka_log::{LeaderEpoch, Offset};
use krabka_protocol::{
    owned::fetch_response::{EpochEndOffset, LeaderIdAndEpoch, PartitionData},
    primitives::uuid::Uuid as WireUuid,
};

use super::request::{EffectivePartition, EffectiveTopic, FetchAuthorization};
use crate::{broker::Broker, codes, partition::Partition};

/// Resolved read for a single requested (topic, partition) tuple.
///
/// The handler keeps it so that it can read again after a long-poll wake.
pub(crate) struct PendingRead {
    pub(crate) topic_name: String,
    pub(crate) topic_id: WireUuid,
    pub(crate) partition_index: i32,
    /// Epoch the follower supplied with the original Fetch request.
    pub(crate) current_leader_epoch: i32,
    /// Epoch of the follower's last fetched record.
    pub(crate) last_fetched_epoch: i32,
    pub(crate) fetch_offset: i64,
    pub(crate) max_bytes: i32,
    /// `true` when `isolation_level == 1` on a consumer fetch, and not on a
    /// follower fetch. It causes batch-level LSO filtering and fills
    /// `aborted_transactions` in the response.
    pub(crate) read_committed: bool,
    /// `true` when `replica_id >= 0`, that is, when the request comes from a
    /// follower replicator and not from a consumer. Follower fetches see all
    /// records up to LEO and report LEO as HW and LSO. The handler clamps
    /// consumer fetches at HW.
    pub(crate) is_follower_fetch: bool,
    /// `true` when only the partition leader may serve this fetch. Kafka's
    /// `FetchParams.fetchOnlyLeader` is true for a follower fetch, and for a
    /// consumer fetch that carries no client metadata, which is every
    /// consumer fetch below v11. A long-poll wake checks leadership again.
    pub(crate) fetch_only_leader: bool,
    /// `None` for an unknown topic or partition, or for an out-of-range
    /// offset. The final response is already complete, and the handler does
    /// not read it again on a wake.
    pub(crate) partition: Option<Arc<Partition>>,
    /// Per-partition output. `do_read` mutates it in place.
    pub(crate) out: PartitionData,
    /// Accumulator for the microseconds spent in this partition's `do_read`
    /// calls. It covers the first pass and every long-poll re-read. The
    /// handler measures an `Instant` elapsed delta around each `do_read`. The
    /// heavy byte read runs in `spawn_blocking`, so this charges the read work
    /// and allocates no `tokio_metrics::TaskMonitor` per partition per fetch.
    /// The response-emit loop drains it into its
    /// `record_partition_cpu_micros` call.
    pub(crate) cpu_micros: u64,
}

impl PendingRead {
    pub(super) fn planned(
        topic_name: &str,
        topic_id: WireUuid,
        partition: &EffectivePartition,
        mode: (bool, bool),
        resolved: Option<Arc<Partition>>,
        out: PartitionData,
    ) -> Self {
        Self {
            topic_name: topic_name.to_owned(),
            topic_id,
            partition_index: partition.partition,
            current_leader_epoch: partition.current_leader_epoch,
            last_fetched_epoch: partition.last_fetched_epoch,
            fetch_offset: partition.fetch_offset,
            max_bytes: partition.partition_max_bytes,
            read_committed: mode.0,
            is_follower_fetch: mode.1,
            fetch_only_leader: mode.1,
            partition: resolved,
            out,
            cpu_micros: 0,
        }
    }
}

async fn update_follower_progress(partition: &Partition, follower_id: i32, fetch_offset: i64) {
    let leader_leo = partition.log_end_offset();
    let advanced = {
        let mut state = partition.replica_state.lock().await;
        let previous = state.hw;
        state.update_follower_leo(
            krabka_metadata::NodeId(u64::try_from(follower_id).unwrap_or(0)),
            Offset(fetch_offset),
            leader_leo,
            std::time::Instant::now(),
        ) > previous
    };
    if advanced {
        partition.hw_advance_notify.notify_waiters();
    }
}

fn preferred_read_replica(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition: i32,
    rack_id: &str,
) -> i32 {
    if rack_id.is_empty() {
        return -1;
    }
    let Some(record) = image.partition(topic, partition) else {
        return -1;
    };
    let isr: std::collections::HashSet<krabka_metadata::NodeId> =
        record.isr.iter().copied().collect();
    let replicas: Vec<crate::replica_selector::ReplicaView> = record
        .replicas
        .iter()
        .map(|&node_id| crate::replica_selector::ReplicaView {
            node_id: i32::try_from(node_id.0).unwrap_or(-1),
            rack: image.broker(node_id).and_then(|broker| broker.rack.clone()),
            in_isr: isr.contains(&node_id),
            is_witness: crate::config_keys::resolve_broker_witness(image, node_id),
        })
        .collect();
    broker.config.replica_selector.select(
        Some(rack_id),
        i32::try_from(record.leader.0).unwrap_or(-1),
        &replicas,
    )
}

/// The node that must lead a partition before a fetch may read it, or `None`
/// when any local replica may serve the fetch.
///
/// Kafka's `Partition.localLogWithEpochOrThrow` requires the leader when
/// `FetchParams.fetchOnlyLeader` is true. A diskless partition has no single
/// leader replica, so it keeps the path it has.
pub(super) fn required_leader(
    fetch_only_leader: bool,
    node_id: krabka_metadata::NodeId,
    partition: &Partition,
) -> Option<krabka_metadata::NodeId> {
    (fetch_only_leader && !partition.diskless).then_some(node_id)
}

/// The partition row that Kafka's `LogReadResult(Errors)` gives a read that
/// `ReplicaManager.readFromLog` refuses: every offset is -1, the records are
/// empty, and there are no aborted transactions.
pub(super) fn refused_read(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(krabka_protocol::records::RecordsPayload::Raw(
            bytes::Bytes::new(),
        )),
        ..Default::default()
    }
}

/// The leader the image names for a partition, as a KIP-951 `CurrentLeader`.
fn image_leader(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
) -> LeaderIdAndEpoch {
    image
        .partition(topic, partition_index)
        .map_or_else(LeaderIdAndEpoch::default, |record| LeaderIdAndEpoch {
            leader_id: i32::try_from(record.leader.0).unwrap_or(-1),
            leader_epoch: record.leader_epoch.0,
            ..Default::default()
        })
}

/// The refused row when `required_leader` names this node and this node does
/// not lead the partition, or `None` when the read may go on.
///
/// The partition's installed local role must name this node, and the metadata
/// image must not name another leader. The image can name a new leader before
/// the supervisor installs the new role, and the installed role can lag the
/// other way while a promotion prepares the log. Kafka checks the one
/// partition state that its metadata publisher updates; this broker has two,
/// so it checks both.
pub(super) fn leader_refusal(
    image: &krabka_metadata::MetadataImage,
    (topic, partition_index): (&str, i32),
    partition: &Partition,
    required_leader: Option<krabka_metadata::NodeId>,
) -> Option<PartitionData> {
    let node_id = required_leader?;
    let installed = partition
        .current_leader
        .load(std::sync::atomic::Ordering::Acquire)
        == node_id.0;
    let committed_elsewhere = image
        .partition(topic, partition_index)
        .is_some_and(|record| record.leader != node_id);
    (!installed || committed_elsewhere).then(|| PartitionData {
        current_leader: image_leader(image, topic, partition_index),
        ..refused_read(partition_index, codes::NOT_LEADER_OR_FOLLOWER)
    })
}

/// What a fetch may read from one local partition.
#[derive(Clone, Copy)]
pub(super) struct ReadRole<'a> {
    pub(super) partition: &'a Partition,
    /// The node that must lead the partition. See [`required_leader`].
    pub(super) required_leader: Option<krabka_metadata::NodeId>,
    /// `false` for a follower fetch whose replica id is not a follower in the
    /// partition's assignment. See [`is_assigned_follower`].
    pub(super) assigned_follower: bool,
}

/// Kafka's order in `Partition.fetchRecords`: the leader-epoch fence, the
/// leader check, the follower replica check, and then the diverging epoch of
/// the read. Returns `true` when `output` is final.
pub(super) fn apply_epoch_checks(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    request: &EffectivePartition,
    role: ReadRole<'_>,
    output: &mut PartitionData,
) -> bool {
    let ReadRole {
        partition,
        required_leader,
        assigned_follower,
    } = role;
    if let Some((error_code, current_epoch)) =
        partition.fetch_leader_epoch_fence(request.current_leader_epoch)
    {
        output.error_code = error_code;
        output.current_leader = LeaderIdAndEpoch {
            leader_id: image
                .partition(topic, partition_index)
                .map_or(-1, |record| i32::try_from(record.leader.0).unwrap_or(-1)),
            leader_epoch: current_epoch,
            ..Default::default()
        };
        return true;
    }
    if let Some(refused) =
        leader_refusal(image, (topic, partition_index), partition, required_leader)
    {
        *output = refused;
        return true;
    }
    if !assigned_follower {
        // Kafka's `Partition.followerReplicaOrThrow`: a fetch that carries a
        // leader epoch gets UNKNOWN_LEADER_EPOCH, and one without gets
        // NOT_LEADER_OR_FOLLOWER. Either way it moves no follower state.
        *output = if request.current_leader_epoch >= 0 {
            refused_read(partition_index, codes::UNKNOWN_LEADER_EPOCH)
        } else {
            PartitionData {
                current_leader: image_leader(image, topic, partition_index),
                ..refused_read(partition_index, codes::NOT_LEADER_OR_FOLLOWER)
            }
        };
        return true;
    }
    if request.last_fetched_epoch < 0 {
        return false;
    }
    let (found_epoch, end_offset) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint().epoch_and_offset_for(
            LeaderEpoch(request.last_fetched_epoch),
            log.log_end_offset(),
        )
    };
    if found_epoch >= request.last_fetched_epoch && end_offset.0 >= request.fetch_offset {
        return false;
    }
    output.error_code = codes::NONE;
    output.diverging_epoch = EpochEndOffset {
        epoch: found_epoch.0,
        end_offset: end_offset.0,
        ..Default::default()
    };
    true
}

/// The partition row that Kafka's `FetchResponse.partitionResponse` builds.
///
/// `KafkaApis.handleFetchRequest` uses it for a row that it refuses before the
/// read: `UNKNOWN_TOPIC_ID`, `TOPIC_AUTHORIZATION_FAILED` and
/// `UNKNOWN_TOPIC_OR_PARTITION`. Every offset is -1, and the aborted
/// transactions are an empty list, not a null one.
pub(super) fn refused_partition(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        ..Default::default()
    }
}

pub(super) struct PendingPlanContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a krabka_metadata::MetadataImage,
    pub(super) authorization: &'a FetchAuthorization,
    pub(super) rack_id: &'a str,
    /// The negotiated `Fetch` version. It decides whether a topic row names
    /// its topic by name or by id.
    pub(super) version: i16,
    pub(super) mode: (bool, bool),
    pub(super) follower_id: i32,
}

pub(super) async fn plan_partition_read(
    context: &PendingPlanContext<'_>,
    topic_name: &str,
    topic_id: WireUuid,
    topic_error: Option<i16>,
    request: &EffectivePartition,
) -> PendingRead {
    // Kafka's `KafkaApis.handleFetchRequest` refuses every row of a follower
    // fetch without `ClusterAction` before it resolves any topic. So that
    // refusal comes first, and the fetch never reaches
    // `update_follower_progress`.
    if context.authorization.refuses_every_row() {
        let output = refused_partition(request.partition, codes::TOPIC_AUTHORIZATION_FAILED);
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    // A topic that does not resolve is answered before the consumer `Read`
    // gate, on the consumer path and on the follower path.
    if let Some(error_code) = topic_error {
        let output = refused_partition(request.partition, error_code);
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if context.authorization.refuses_topic(topic_name) {
        let output = refused_partition(request.partition, codes::TOPIC_AUTHORIZATION_FAILED);
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    let mut output = PartitionData {
        partition_index: request.partition,
        ..Default::default()
    };
    // A witness replicates the partition and counts toward
    // `min.insync.replicas`, but it serves no client traffic. A consumer that
    // reaches one gets NOT_LEADER_OR_FOLLOWER, the partition-level code that
    // makes a Kafka client refresh its metadata and read somewhere else. A
    // FOLLOWER fetch passes: replication is the reason the witness holds the
    // data at all. The check sits below the two topic gates, so an
    // authorization failure still wins, and it sits above the partition
    // lookup, so a witness answers a consumer the same way whether or not it
    // hosts the partition.
    if !context.mode.1 && context.broker.config.is_witness() {
        output.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        // KIP-951: name the leader the consumer should go to instead. Kafka
        // fills `CurrentLeader` on every NOT_LEADER_OR_FOLLOWER row of a v16+
        // Fetch response, and the response's `NodeEndpoints` then carries that
        // node's address, so the consumer re-targets without a full Metadata
        // round-trip. A witness never leads, so the image's leader is always
        // some other node.
        if let Some(record) = context.image.partition(topic_name, request.partition) {
            output.current_leader = LeaderIdAndEpoch {
                leader_id: i32::try_from(record.leader.0).unwrap_or(-1),
                leader_epoch: record.leader_epoch.0,
                ..Default::default()
            };
        }
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    let partition = context
        .broker
        .partitions
        .get(topic_name, krabka_ids::PartitionIndex(request.partition));
    // Kafka's `FetchParams.fetchOnlyLeader`: a follower fetch, or a consumer
    // fetch below v11, which has no client metadata.
    let fetch_only_leader = context.mode.1 || context.version < FIRST_CLIENT_METADATA_VERSION;
    let node_id = context.broker.config.node_id;
    // A follower fetch holds the partition's replication-target read guard
    // from the leader check through the follower progress update, as Kafka's
    // `Partition.fetchRecords` holds `leaderIsrUpdateLock`. A leadership
    // change takes the write guard, so it cannot land in between.
    let _transition = match partition.as_ref() {
        Some(partition) if context.mode.1 => Some(partition.lock_produce_transition().await),
        _ => None,
    };
    if let Some(partition) = partition.as_ref()
        && apply_epoch_checks(
            context.image,
            topic_name,
            request.partition,
            request,
            ReadRole {
                partition,
                required_leader: required_leader(fetch_only_leader, node_id, partition),
                assigned_follower: !context.mode.1
                    || partition.diskless
                    || is_assigned_follower(
                        context.image,
                        topic_name,
                        request.partition,
                        context.follower_id,
                        node_id,
                    ),
            },
            &mut output,
        )
    {
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if let Some(partition) = partition.as_ref()
        && context
            .broker
            .log_dir_status
            .is_offline(&partition.log_dir.load())
    {
        output.error_code = codes::KAFKA_STORAGE_ERROR;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if context.mode.1
        && let Some(partition) = partition.as_ref()
    {
        update_follower_progress(partition, context.follower_id, request.fetch_offset).await;
    }
    if partition.is_none() || topic_name.is_empty() {
        let output = refused_partition(request.partition, codes::UNKNOWN_TOPIC_OR_PARTITION);
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    // Kafka's `ReplicaManager.findPreferredReadReplica` names a read replica
    // only on the leader.
    let leads = partition.as_ref().is_some_and(|partition| {
        partition
            .current_leader
            .load(std::sync::atomic::Ordering::Acquire)
            == node_id.0
    });
    if !context.mode.1 && leads {
        output.preferred_read_replica = preferred_read_replica(
            context.broker,
            context.image,
            topic_name,
            request.partition,
            context.rack_id,
        );
    }
    PendingRead {
        fetch_only_leader,
        ..PendingRead::planned(
            topic_name,
            topic_id,
            request,
            context.mode,
            partition,
            output,
        )
    }
}

/// The first `Fetch` version that carries client metadata, from which Kafka
/// lets a consumer read from a follower replica (KIP-392).
const FIRST_CLIENT_METADATA_VERSION: i16 = 11;

/// Whether `follower_id` is a replica of the partition other than this node,
/// as Kafka's `Partition.getReplica` finds it in the remote replicas of the
/// assignment.
fn is_assigned_follower(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    follower_id: i32,
    node_id: krabka_metadata::NodeId,
) -> bool {
    let Ok(follower) = u64::try_from(follower_id).map(krabka_metadata::NodeId) else {
        return false;
    };
    follower != node_id
        && image
            .partition(topic, partition_index)
            .is_some_and(|record| record.replicas.contains(&follower))
}

pub(super) async fn build_pending_reads(
    context: &PendingPlanContext<'_>,
    topics: &[EffectiveTopic],
) -> Vec<PendingRead> {
    let mut pending = Vec::new();
    for topic in topics {
        // Versions 12 and earlier name the topic. A name that does not resolve
        // goes on to the `Read` gate and then to the partition gate, which
        // answer TOPIC_AUTHORIZATION_FAILED or UNKNOWN_TOPIC_OR_PARTITION.
        // Version 13 and later name the topic by id only. Kafka's
        // `KafkaApis.handleFetchRequest` resolves the id through
        // `metadataCache.topicIdsToNames()` and answers UNKNOWN_TOPIC_ID on
        // every partition row when that gives no name. The zero id gives no
        // name, and the resolver sends it down the name path with an empty
        // name.
        let (name, id, error) =
            match crate::topic_resolve::resolve(context.image, &topic.topic, topic.topic_id) {
                Ok(record) => (
                    record.name.clone(),
                    WireUuid(record.topic_id.into_bytes()),
                    None,
                ),
                Err(codes::UNKNOWN_TOPIC_OR_PARTITION)
                    if context.version < super::FIRST_TOPIC_ID_VERSION =>
                {
                    (topic.topic.clone(), topic.topic_id, None)
                }
                Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => (
                    topic.topic.clone(),
                    topic.topic_id,
                    Some(codes::UNKNOWN_TOPIC_ID),
                ),
                Err(error_code) => (topic.topic.clone(), topic.topic_id, Some(error_code)),
            };
        for partition in &topic.partitions {
            pending.push(plan_partition_read(context, &name, id, error, partition).await);
        }
    }
    pending
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig};

    use crate::broker::Broker;

    /// A three-site image: node 1 leads `orders` from `dc-a`, node 2 replicates
    /// it from `dc-b`, and both are in the ISR. Every node in `witness_ids`
    /// carries `broker.witness=true`, the way a real witness registers.
    fn stretch_image(witness_ids: &[u64]) -> krabka_metadata::MetadataImage {
        use krabka_metadata::{
            BrokerConfigRecord, BrokerRegistrationRecord, MetadataImage, MetadataRecord,
            PartitionRecord, TopicRecord,
        };

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (node_id, rack) in [(1u64, "dc-a"), (2u64, "dc-b")] {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: krabka_audit::NodeId(node_id),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
                    host: "127.0.0.1".into(),
                    port: 9_092,
                    rack: Some(rack.into()),
                    endpoints: vec![],
                    log_dirs: vec![],
                    features: BTreeMap::new(),
                },
            ));
        }
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        for &node_id in witness_ids {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_audit::NodeId(node_id),
                config_name: crate::config_keys::BROKER_WITNESS.into(),
                config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
            }));
        }
        image
    }

    /// The witness gate refuses a client fetch with the witness row. It lets a
    /// follower fetch through, and the leader check then refuses it with the
    /// read row, because a witness never leads the partition.
    #[tokio::test]
    async fn witness_refuses_a_client_fetch_and_passes_a_follower_fetch_on() {
        const TOPIC: &str = "witness-fetch";

        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.roles.push(crate::config::NodeRole::Witness);
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join(format!("{TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        broker.partitions.insert(
            TOPIC.into(),
            PartitionIndex(0),
            crate::broker::spawn_partition(
                TOPIC.to_string(),
                PartitionIndex(0),
                dir.path().to_path_buf(),
                Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
                broker.log_dir_status.clone(),
                broker.producer_state.clone(),
                false,
            ),
        );
        let image = broker.controller.current_image();
        let consumer = super::FetchAuthorization::Consumer {
            denied_topics: std::collections::HashSet::new(),
        };
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            partition_max_bytes: 1024,
        };

        for (name, is_follower_fetch, follower_id, want) in [
            (
                "client fetch",
                false,
                -1,
                super::PartitionData {
                    partition_index: 0,
                    error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
                    ..Default::default()
                },
            ),
            (
                "follower fetch",
                true,
                2,
                super::refused_read(0, crate::codes::NOT_LEADER_OR_FOLLOWER),
            ),
        ] {
            let context = super::PendingPlanContext {
                broker: &broker,
                image: &image,
                authorization: if is_follower_fetch {
                    &super::FetchAuthorization::Follower
                } else {
                    &consumer
                },
                rack_id: "",
                version: super::super::FIRST_TOPIC_ID_VERSION,
                mode: (false, is_follower_fetch),
                follower_id,
            };
            let read =
                super::plan_partition_read(&context, TOPIC, super::WireUuid::ZERO, None, &request)
                    .await;
            assert!(read.out == want, "{name}: got {:?}", read.out);
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn rack_aware_preferred_read_replica_never_names_a_witness() {
        // The consumer sits in `dc-b`, the witness site. Node 2 is the only
        // same-rack in-ISR replica, so it is exactly the redirect a rack-aware
        // selector wants to make.
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.replica_selector = crate::replica_selector::ReplicaSelectorKind::RackAware;
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();

        for (name, witness_ids, want) in [
            ("node 2 is a plain broker in dc-b", &[][..], 2),
            ("node 2 is the witness in dc-b", &[2u64][..], -1),
        ] {
            let image = stretch_image(witness_ids);
            let got = super::preferred_read_replica(&broker, &image, "orders", 0, "dc-b");
            assert!(got == want, "{name}: got {got}, want {want}");
        }
        broker_handle.shutdown().await;
    }

    /// The leader check needs this node in the installed local role, and no
    /// other leader in the committed image. `stretch_image` names node 1 as the
    /// leader of `orders`-0.
    #[tokio::test]
    async fn a_read_needs_the_installed_role_and_the_image_to_name_this_node() {
        let image = stretch_image(&[]);
        let dir = tempfile::tempdir().expect("tempdir");
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(dir.path(), LogConfig::default()).expect("open partition log"),
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        let refused = |leader_id| super::PartitionData {
            current_leader: super::LeaderIdAndEpoch {
                leader_id,
                leader_epoch: 0,
                ..Default::default()
            },
            ..super::refused_read(0, crate::codes::NOT_LEADER_OR_FOLLOWER)
        };
        let cases = [
            ("installed and committed", 1, 1, None),
            (
                "installed, committed to another node",
                2,
                2,
                Some(refused(1)),
            ),
            ("committed, not installed yet", 1, 2, Some(refused(1))),
        ];
        for (name, node, installed, want) in cases {
            partition
                .install_replication_target(None, installed, 0)
                .await;
            let got = super::leader_refusal(
                &image,
                ("orders", 0),
                &partition,
                Some(krabka_metadata::NodeId(node)),
            );
            assert!(got == want, "{name}");
        }
        assert!(super::leader_refusal(&image, ("orders", 0), &partition, None) == None);
    }
}
