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

async fn update_follower_progress(
    partition: &Partition,
    follower_id: i32,
    request: &EffectivePartition,
) {
    let leader_leo = partition.log_end_offset();
    let follower = krabka_metadata::NodeId(u64::try_from(follower_id).unwrap_or(0));
    let advanced = {
        let mut state = partition.replica_state.lock().await;
        let previous = state.hw;
        // Kafka's `Replica.updateFetchStateOrThrow` records the follower's log
        // start offset with its fetch offset. A `DeleteRecords` waits for it.
        state.record_follower_log_start(follower, Offset(request.log_start_offset));
        state.update_follower_leo(
            follower,
            Offset(request.fetch_offset),
            leader_leo,
            std::time::Instant::now(),
        ) > previous
    };
    if advanced {
        partition.hw_advance_notify.notify_waiters();
    }
}

/// Chooses a preferred read replica, or `-1` for "read from the leader".
///
/// Kafka's `ReplicaManager.findPreferredReadReplica` only offers a candidate
/// whose reported range can serve `fetch_offset`: `logEndOffset >=
/// fetchOffset` and `logStartOffset <= fetchOffset`. Without that check a
/// consumer can be redirected to an in-ISR replica that has not replicated
/// far enough yet, or has already trimmed the offset away, and the follower
/// answers it `OFFSET_OUT_OF_RANGE` in the redirect's place.
async fn preferred_read_replica(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    partition: &Partition,
    topic: &str,
    partition_index: i32,
    rack_id: &str,
    fetch_offset: Offset,
) -> i32 {
    if rack_id.is_empty() {
        return -1;
    }
    let Some(record) = image.partition(topic, partition_index) else {
        return -1;
    };
    let isr: std::collections::HashSet<krabka_metadata::NodeId> =
        record.isr.iter().copied().collect();
    let replicas: Vec<crate::replica_selector::ReplicaView> = {
        let state = partition.replica_state.lock().await;
        record
            .replicas
            .iter()
            .filter(|&&node_id| {
                node_id == record.leader || state.follower_can_serve(node_id, fetch_offset)
            })
            .map(|&node_id| crate::replica_selector::ReplicaView {
                node_id: i32::try_from(node_id.0).unwrap_or(-1),
                rack: image.broker(node_id).and_then(|broker| broker.rack.clone()),
                in_isr: isr.contains(&node_id),
                is_witness: crate::config_keys::resolve_broker_witness(image, node_id),
            })
            .collect()
    };
    broker.config.replica_selector.select(
        Some(rack_id),
        i32::try_from(record.leader.0).unwrap_or(-1),
        &replicas,
    )
}

/// The response Kafka's `ReplicaManager.readFromLog` gives a fetch it never
/// reads: empty records at the offset snapshot's live bounds.
///
/// `ReplicaManager.fetchMessages` skips the log read entirely once it has
/// named a preferred read replica (`hasPreferredReadReplica` in
/// `maybeReadFromLocalLog` and again at `tryComplete`), and answers with this
/// snapshot rather than parking in the fetch purgatory -- a consumer told to
/// go elsewhere should not also wait out `fetch.max.wait.ms` on this broker
/// first.
async fn preferred_replica_snapshot(partition: &Partition) -> (i64, i64, i64) {
    let hw = partition.high_watermark().await;
    let mut log = partition.log.lock().expect("log mutex poisoned");
    let log_start = log.log_start_offset();
    let last_stable_offset = log.last_stable_offset(hw);
    (hw.0, last_stable_offset.0, log_start.0)
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
    /// Whether this partition's log directory is currently marked offline.
    /// The below-log-start check in [`apply_epoch_checks`] defers to the
    /// caller's own offline gate instead of racing to answer
    /// `OFFSET_OUT_OF_RANGE` first: an offline directory can make
    /// `Log::log_start_offset` unreadable or stale, and a client or follower
    /// told to reset its offset in response to what is really a storage
    /// failure would discard state it should have kept.
    pub(super) log_dir_offline: bool,
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
        log_dir_offline,
    } = role;
    if let Some((error_code, current_epoch)) =
        partition.fetch_leader_epoch_fence(request.current_leader_epoch)
    {
        // Kafka's `FetchResponse.partitionResponse` gives this row the -1
        // sentinels of a refused read, same as `leader_refusal` below it: the
        // fence trips before the log is ever touched. `CurrentLeader` is
        // filled for `FENCED_LEADER_EPOCH` (the client is ahead) and for
        // `NOT_LEADER_OR_FOLLOWER`, but not for `UNKNOWN_LEADER_EPOCH` (the
        // client is behind and has nothing new to learn from it).
        *output = PartitionData {
            current_leader: if error_code == codes::FENCED_LEADER_EPOCH {
                LeaderIdAndEpoch {
                    leader_id: image
                        .partition(topic, partition_index)
                        .map_or(-1, |record| i32::try_from(record.leader.0).unwrap_or(-1)),
                    leader_epoch: current_epoch,
                    ..Default::default()
                }
            } else {
                LeaderIdAndEpoch::default()
            },
            ..refused_read(partition_index, error_code)
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
    let (
        log_start_offset,
        established_log_start,
        found_epoch,
        end_offset,
        high_watermark,
        last_stable_offset,
    ) = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        let log_start_offset = log.log_start_offset();
        let established_log_start = log.established_log_start();
        let log_end_offset = log.log_end_offset();
        let (found_epoch, end_offset) = log
            .epoch_checkpoint()
            .epoch_and_offset_for(LeaderEpoch(request.last_fetched_epoch), log_end_offset);
        // Kafka reports the partition's live bounds on an `OFFSET_OUT_OF_RANGE`
        // row (`Partition.readRecords`'s `logReadInfo`), not the -1 sentinels
        // `refused_read` uses for a row this handler refuses without ever
        // touching the log. A follower fetch sees LEO as both HW and LSO (see
        // the module doc); using it here for every fetch type is an
        // approximation on a lagging acks=all topic, but still strictly more
        // useful than -1 and exactly right for the follower fetch this branch
        // exists to keep from hot-looping.
        let last_stable_offset = log.last_stable_offset(log_end_offset);
        (
            log_start_offset,
            established_log_start,
            found_epoch,
            end_offset,
            log_end_offset,
            last_stable_offset,
        )
    };
    // Kafka's `Partition.readRecords` (roughly lines 1385-1412): a fetch
    // offset below the log start is `OFFSET_OUT_OF_RANGE` whatever the epochs
    // say, checked before any divergence row is built. Only an *established*
    // floor counts: `Log::open` also infers a floor from whatever segments are
    // left on disk, and on a tiered partition reopened with its local segments
    // evicted that inferred floor sits above everything the remote tier still
    // holds. Refusing on the strength of it would hide readable records
    // instead of letting the request fall through to the remote-tier read
    // still ahead of it -- so this only fires once something has actually
    // moved the global floor. An offline log directory takes precedence: it
    // can make the floor unreadable or stale, and resetting a client or
    // follower in response to a storage failure would discard state it
    // should have kept, so this defers to the caller's own offline gate
    // instead of racing it.
    if let Some(established) = established_log_start
        && request.fetch_offset < established.0
    {
        if log_dir_offline {
            return false;
        }
        *output = PartitionData {
            log_start_offset: log_start_offset.0,
            high_watermark: high_watermark.0,
            last_stable_offset: last_stable_offset.0,
            ..refused_read(partition_index, codes::OFFSET_OUT_OF_RANGE)
        };
        return true;
    }
    if found_epoch >= request.last_fetched_epoch && end_offset.0 >= request.fetch_offset {
        return false;
    }
    // Kafka's `Partition.readRecords` fills a diverging-epoch row with the
    // partition's live bounds (`initialHighWatermark`, `initialLogStartOffset`
    // and `initialLastStableOffset`), the same values this check already read
    // under the log mutex above, not the zero defaults the row started from.
    output.error_code = codes::NONE;
    output.high_watermark = high_watermark.0;
    output.last_stable_offset = last_stable_offset.0;
    output.log_start_offset = log_start_offset.0;
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
/// `UNKNOWN_TOPIC_OR_PARTITION`. Every offset is -1. `partitionResponse` never
/// sets the aborted transactions, so they keep the generated default: the
/// schema gives `AbortedTransactions` no `"default": "null"`, which makes it
/// an empty list, not a null one. A row refused by the read itself
/// ([`refused_read`]) is the one that carries null.
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
        // This row never touches the log, so it carries the -1 sentinels of
        // a refused read, like `leader_refusal` below it.
        output = refused_read(request.partition, codes::NOT_LEADER_OR_FOLLOWER);
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
                log_dir_offline: context
                    .broker
                    .log_dir_status
                    .is_offline(&partition.log_dir.load()),
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
        // `KAFKA_STORAGE_ERROR` (56) postdates Fetch v5; a fetcher that old
        // has no code for it, so Kafka's `KafkaApis.handleFetchRequest`
        // down-converts it to `NOT_LEADER_OR_FOLLOWER` for those versions.
        // Either way this row never touches the log, so it carries the -1
        // sentinels of a refused read.
        let error_code = if context.version <= LAST_PRE_STORAGE_ERROR_FETCH_VERSION {
            codes::NOT_LEADER_OR_FOLLOWER
        } else {
            codes::KAFKA_STORAGE_ERROR
        };
        let output = refused_read(request.partition, error_code);
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if context.mode.1
        && let Some(partition) = partition.as_ref()
    {
        update_follower_progress(partition, context.follower_id, request).await;
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
        let leader_partition = partition
            .as_ref()
            .expect("`leads` implies a local partition");
        output.preferred_read_replica = preferred_read_replica(
            context.broker,
            context.image,
            leader_partition,
            topic_name,
            request.partition,
            context.rack_id,
            Offset(request.fetch_offset),
        )
        .await;
        if output.preferred_read_replica >= 0 {
            // Kafka never reads the log for a fetch it is redirecting: it
            // answers at once from the offset snapshot instead of also
            // parking this broker's copy of the request in the fetch
            // purgatory.
            let (high_watermark, last_stable_offset, log_start_offset) =
                preferred_replica_snapshot(leader_partition).await;
            output.error_code = codes::NONE;
            output.high_watermark = high_watermark;
            output.last_stable_offset = last_stable_offset;
            output.log_start_offset = log_start_offset;
            output.records = Some(krabka_protocol::records::RecordsPayload::Raw(
                bytes::Bytes::new(),
            ));
            return PendingRead {
                fetch_only_leader,
                ..PendingRead::planned(
                    topic_name,
                    topic_id,
                    request,
                    context.mode,
                    // No `partition`: the read loop skips both the read and
                    // the long-poll arm for an entry with none.
                    None,
                    output,
                )
            };
        }
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

/// The last `Fetch` version a fetcher could speak before `KAFKA_STORAGE_ERROR`
/// (56) existed. Kafka's `KafkaApis.handleFetchRequest` down-converts that
/// code to `NOT_LEADER_OR_FOLLOWER` at this version and below, because an
/// older fetcher has no case for the code it does not know.
const LAST_PRE_STORAGE_ERROR_FETCH_VERSION: i16 = 5;

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
    use std::{collections::BTreeMap, sync::Arc};

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
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };

        for (name, is_follower_fetch, follower_id, want) in [
            (
                "client fetch",
                false,
                -1,
                super::refused_read(0, crate::codes::NOT_LEADER_OR_FOLLOWER),
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
        let part_dir = dir.path().join("orders-0");
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        // A freshly spawned partition's replica state has no per-follower
        // entries yet, which default to a log end offset of 0 and an unknown
        // (permissive) log start -- exactly what a fetch at offset 0 needs to
        // pass the offset-range check.
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );

        for (name, witness_ids, want) in [
            ("node 2 is a plain broker in dc-b", &[][..], 2),
            ("node 2 is the witness in dc-b", &[2u64][..], -1),
        ] {
            let image = stretch_image(witness_ids);
            let got = super::preferred_read_replica(
                &broker,
                &image,
                &partition,
                "orders",
                0,
                "dc-b",
                super::Offset(0),
            )
            .await;
            assert!(got == want, "{name}: got {got}, want {want}");
        }
        broker_handle.shutdown().await;
    }

    /// Table over the follower's reported log end and log start offset
    /// against the fetch offset, to whether `preferred_read_replica` still
    /// names it (#873). A candidate that could not serve the offset, either
    /// because it has not replicated far enough or because it has already
    /// trimmed the offset away, must never be offered.
    #[tokio::test]
    async fn preferred_read_replica_excludes_a_follower_outside_its_reported_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.replica_selector = crate::replica_selector::ReplicaSelectorKind::RackAware;
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join("orders-0");
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );
        let image = stretch_image(&[]);

        for (name, follower_leo, follower_log_start, fetch_offset, want) in [
            ("caught up, fetch at the tail: eligible", 40, 0, 40, 2),
            ("not replicated that far yet: excluded", 39, 0, 40, -1),
            ("already trimmed the offset away: excluded", 40, 41, 40, -1),
            ("in range on both bounds: eligible", 40, 10, 20, 2),
        ] {
            {
                let mut state = partition.replica_state.lock().await;
                state.install_isr(
                    &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                    &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                    krabka_raft::NodeId(1),
                    std::time::Instant::now(),
                );
                state.update_follower_leo(
                    krabka_raft::NodeId(2),
                    super::Offset(follower_leo),
                    super::Offset(follower_leo.max(fetch_offset)),
                    std::time::Instant::now(),
                );
                state.record_follower_log_start(
                    krabka_raft::NodeId(2),
                    super::Offset(follower_log_start),
                );
            }
            let got = super::preferred_read_replica(
                &broker,
                &image,
                &partition,
                "orders",
                0,
                "dc-b",
                super::Offset(fetch_offset),
            )
            .await;
            assert!(got == want, "{name}: got {got}, want {want}");
        }
        broker_handle.shutdown().await;
    }

    /// A named preferred read replica skips the log read and the long poll
    /// entirely: the response is the offset snapshot taken at plan time, with
    /// empty records, and there is nothing left for the read loop to do with
    /// this partition (#873).
    #[tokio::test]
    async fn a_named_preferred_read_replica_skips_the_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.replica_selector = crate::replica_selector::ReplicaSelectorKind::RackAware;
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let part_dir = dir.path().join("orders-0");
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open partition log"),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );
        broker.partitions.insert(
            "orders".into(),
            PartitionIndex(0),
            std::sync::Arc::clone(&partition),
        );
        partition
            .install_replication_target(None, broker.config.node_id.0, 0)
            .await;
        let image = stretch_image(&[]);
        let consumer = super::FetchAuthorization::Consumer {
            denied_topics: std::collections::HashSet::new(),
        };
        let context = super::PendingPlanContext {
            broker: &broker,
            image: &image,
            authorization: &consumer,
            rack_id: "dc-b",
            version: super::super::FIRST_TOPIC_ID_VERSION,
            mode: (false, false),
            follower_id: -1,
        };
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };

        let read =
            super::plan_partition_read(&context, "orders", super::WireUuid::ZERO, None, &request)
                .await;

        assert!(read.out.preferred_read_replica == 2);
        assert!(read.out.error_code == crate::codes::NONE);
        assert!(
            matches!(read.out.records, Some(ref r) if r.payload_len() == 0),
            "empty records, not absent ones: {:?}",
            read.out.records
        );
        assert!(
            read.partition.is_none(),
            "no `Partition` left to read: the loop must not read or park on it"
        );
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

    /// Append one single-record batch at `epoch` and return the offset it
    /// landed at.
    fn append_at_epoch(log: &mut Log, epoch: i32) -> i64 {
        use krabka_protocol::records::{Record, RecordBatch};

        let mut batch = RecordBatch {
            partition_leader_epoch: epoch,
            records: vec![Record::default()],
            ..Default::default()
        };
        log.append(&mut batch).expect("append").0.0
    }

    fn epoch_checks_partition(dir: &std::path::Path) -> Arc<crate::partition::Partition> {
        crate::broker::spawn_partition(
            "diverge".to_string(),
            PartitionIndex(0),
            dir.to_path_buf(),
            Log::open(dir, LogConfig::default()).expect("open partition log"),
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        )
    }

    fn read_role(partition: &crate::partition::Partition) -> super::ReadRole<'_> {
        super::ReadRole {
            partition,
            required_leader: None,
            assigned_follower: true,
            log_dir_offline: false,
        }
    }

    fn effective_partition(
        last_fetched_epoch: i32,
        fetch_offset: i64,
    ) -> super::EffectivePartition {
        super::EffectivePartition {
            partition: 0,
            // -1 skips the KIP-101 fence so the KIP-320 check below it runs.
            current_leader_epoch: -1,
            last_fetched_epoch,
            fetch_offset,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        }
    }

    /// A fetch offset below an *established* log start answers
    /// `OFFSET_OUT_OF_RANGE` with the partition's live bounds, checked before
    /// the epoch lookup's divergence row is built, unless the log directory is
    /// offline, in which case the caller's own offline gate takes it instead.
    /// An unestablished (segment-inferred) floor never refuses here, since it
    /// may sit above data the remote tier still holds. An epoch lookup that
    /// cannot place `last_fetched_epoch` on this log -- an empty epoch
    /// history, or a `last_fetched_epoch` above every recorded epoch -- still
    /// gets an actionable `diverging_epoch` row (`Log::epoch_and_offset_for`
    /// resolves it to the log end offset, never -1, so a follower truncates to
    /// it instead of looping), same as a true divergence.
    #[tokio::test]
    async fn below_established_log_start_answers_offset_out_of_range() {
        // No records at all: the epoch cache is empty, and nothing has ever
        // moved the log start, so it is unestablished.
        let empty_dir = tempfile::tempdir().expect("tempdir");
        let empty = epoch_checks_partition(empty_dir.path());

        // Two epochs of two records each: checkpoint `0 -> 0`, `1 -> 2`, LEO 4.
        // Also never trimmed, so its log start is unestablished too.
        let history_dir = tempfile::tempdir().expect("tempdir");
        let with_history = epoch_checks_partition(history_dir.path());
        {
            let mut log = with_history.log.lock().expect("log mutex poisoned");
            append_at_epoch(&mut log, 0);
            append_at_epoch(&mut log, 0);
            append_at_epoch(&mut log, 1);
            append_at_epoch(&mut log, 1);
        }

        // Three epochs of two records each: checkpoint `0 -> 0`, `1 -> 2`,
        // `2 -> 4`, LEO 6. The log start then moves to 5 and becomes
        // established, above the epoch-0 boundary (2) that a
        // `last_fetched_epoch = 0` fetch would otherwise diverge to.
        let trimmed_dir = tempfile::tempdir().expect("tempdir");
        let trimmed = epoch_checks_partition(trimmed_dir.path());
        {
            let mut log = trimmed.log.lock().expect("log mutex poisoned");
            append_at_epoch(&mut log, 0);
            append_at_epoch(&mut log, 0);
            append_at_epoch(&mut log, 1);
            append_at_epoch(&mut log, 1);
            append_at_epoch(&mut log, 2);
            append_at_epoch(&mut log, 2);
            log.set_log_start_offset(super::Offset(5))
                .expect("move log start");
        }

        for (name, partition, last_fetched_epoch, fetch_offset, want_final, want_out) in [
            (
                "empty epoch history, unestablished floor: an actionable \
                 divergence, not a refusal",
                Arc::clone(&empty),
                0,
                0,
                true,
                super::PartitionData {
                    partition_index: 0,
                    error_code: crate::codes::NONE,
                    // A brand-new empty log's live bounds are all 0, and
                    // `apply_epoch_checks` now fills them on this row rather
                    // than leaving the wire defaults (-1) `PartitionData`
                    // starts from (#872/#873).
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: 0,
                    diverging_epoch: super::EpochEndOffset {
                        epoch: -1,
                        end_offset: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
            (
                "last_fetched_epoch above every recorded epoch, unestablished \
                 floor: an actionable divergence, not a refusal",
                Arc::clone(&with_history),
                5,
                4,
                true,
                super::PartitionData {
                    partition_index: 0,
                    error_code: crate::codes::NONE,
                    high_watermark: 4,
                    last_stable_offset: 4,
                    // `with_history` is never trimmed, so its live log start
                    // is still 0, not the wire default (-1) that
                    // `apply_epoch_checks` now fills over on this row.
                    log_start_offset: 0,
                    diverging_epoch: super::EpochEndOffset {
                        epoch: -1,
                        end_offset: 4,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
            (
                "fetch offset below an established log start, even though the \
                 epoch history alone would diverge",
                Arc::clone(&trimmed),
                0,
                4,
                true,
                super::PartitionData {
                    log_start_offset: 5,
                    high_watermark: 6,
                    last_stable_offset: 6,
                    ..super::refused_read(0, crate::codes::OFFSET_OUT_OF_RANGE)
                },
            ),
            (
                "a true divergence still answers a diverging_epoch with epoch >= 0",
                Arc::clone(&with_history),
                0,
                4,
                true,
                super::PartitionData {
                    partition_index: 0,
                    error_code: crate::codes::NONE,
                    high_watermark: 4,
                    last_stable_offset: 4,
                    // `with_history` is never trimmed, so its live log start
                    // is still 0, not the wire default (-1) that
                    // `apply_epoch_checks` now fills over on this row.
                    log_start_offset: 0,
                    diverging_epoch: super::EpochEndOffset {
                        epoch: 0,
                        end_offset: 2,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ),
        ] {
            let request = effective_partition(last_fetched_epoch, fetch_offset);
            let mut output = super::PartitionData {
                partition_index: 0,
                ..Default::default()
            };
            let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            let final_ = super::apply_epoch_checks(
                &image,
                "diverge",
                0,
                &request,
                read_role(&partition),
                &mut output,
            );
            assert!(final_ == want_final, "{name}: final");
            assert!(output == want_out, "{name}: got {output:?}");
        }
    }

    /// An offline log directory takes precedence over the below-established-
    /// log-start refusal: the caller's own offline gate answers
    /// `KAFKA_STORAGE_ERROR` instead, since a storage failure should never
    /// look like a client or follower reset.
    #[tokio::test]
    async fn below_established_log_start_defers_to_an_offline_log_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let partition = epoch_checks_partition(dir.path());
        {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            append_at_epoch(&mut log, 0);
            append_at_epoch(&mut log, 0);
            log.set_log_start_offset(super::Offset(1))
                .expect("move log start");
        }

        let request = effective_partition(0, 0);
        let mut output = super::PartitionData {
            partition_index: 0,
            ..Default::default()
        };
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let final_ = super::apply_epoch_checks(
            &image,
            "diverge",
            0,
            &request,
            super::ReadRole {
                partition: &partition,
                required_leader: None,
                assigned_follower: true,
                log_dir_offline: true,
            },
            &mut output,
        );
        assert!(!final_, "defers to the caller's own offline check");
    }
}
