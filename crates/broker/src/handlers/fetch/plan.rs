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

use super::request::{EffectivePartition, EffectiveTopic};
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

pub(super) fn apply_epoch_checks(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    request: &EffectivePartition,
    partition: &Partition,
    output: &mut PartitionData,
) -> bool {
    let current_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    if request.current_leader_epoch >= 0 && request.current_leader_epoch != current_epoch {
        output.error_code = if request.current_leader_epoch < current_epoch {
            codes::FENCED_LEADER_EPOCH
        } else {
            codes::UNKNOWN_LEADER_EPOCH
        };
        output.current_leader = LeaderIdAndEpoch {
            leader_id: image
                .partition(topic, partition_index)
                .map_or(-1, |record| i32::try_from(record.leader.0).unwrap_or(-1)),
            leader_epoch: current_epoch,
            ..Default::default()
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

pub(super) struct PendingPlanContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a krabka_metadata::MetadataImage,
    pub(super) denied_topics: &'a std::collections::HashSet<String>,
    pub(super) rack_id: &'a str,
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
    let mut output = PartitionData {
        partition_index: request.partition,
        ..Default::default()
    };
    if context.denied_topics.contains(topic_name) {
        output.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if let Some(error_code) = topic_error {
        output.error_code = error_code;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
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
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    let partition = context
        .broker
        .partitions
        .get(topic_name, krabka_ids::PartitionIndex(request.partition));
    if let Some(partition) = partition.as_ref()
        && apply_epoch_checks(
            context.image,
            topic_name,
            request.partition,
            request,
            partition,
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
        output.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if !context.mode.1 {
        output.preferred_read_replica = preferred_read_replica(
            context.broker,
            context.image,
            topic_name,
            request.partition,
            context.rack_id,
        );
    }
    PendingRead::planned(
        topic_name,
        topic_id,
        request,
        context.mode,
        partition,
        output,
    )
}

pub(super) async fn build_pending_reads(
    context: &PendingPlanContext<'_>,
    topics: &[EffectiveTopic],
) -> Vec<PendingRead> {
    let mut pending = Vec::new();
    for topic in topics {
        let (name, id, error) =
            match crate::topic_resolve::resolve(context.image, &topic.topic, topic.topic_id) {
                Ok(record) => (
                    record.name.clone(),
                    WireUuid(record.topic_id.into_bytes()),
                    None,
                ),
                Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => {
                    (topic.topic.clone(), topic.topic_id, None)
                }
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

    #[tokio::test]
    async fn witness_refuses_a_client_fetch_and_still_serves_a_follower_fetch() {
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
        let denied_topics = std::collections::HashSet::new();
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            partition_max_bytes: 1024,
        };

        for (name, is_follower_fetch, follower_id, want_error) in [
            (
                "client fetch",
                false,
                -1,
                crate::codes::NOT_LEADER_OR_FOLLOWER,
            ),
            ("follower fetch", true, 2, crate::codes::NONE),
        ] {
            let context = super::PendingPlanContext {
                broker: &broker,
                image: &image,
                denied_topics: &denied_topics,
                rack_id: "",
                mode: (false, is_follower_fetch),
                follower_id,
            };
            let read =
                super::plan_partition_read(&context, TOPIC, super::WireUuid::ZERO, None, &request)
                    .await;
            let want = super::PartitionData {
                partition_index: 0,
                error_code: want_error,
                ..Default::default()
            };
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
}
