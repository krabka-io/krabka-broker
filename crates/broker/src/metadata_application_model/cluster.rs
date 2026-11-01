//! The seams onto the production code. Each one builds a real
//! [`MetadataImage`] from the model's projection, runs the production function
//! on it, and projects the result back.
//!
//! - [`Cluster::commit`] encodes a batch the way the controller's
//!   `prepare_records` does: each record through the real `encode_record`
//!   against the controller's image with the batch's earlier records applied.
//! - [`apply_batch`] decodes committed bytes the way live apply
//!   (`Engine` in `kraft/controller/apply.rs`) and restart recovery
//!   (`replay_committed_with_timestamp`) both do: the real
//!   `decode_committed_value`, then `MetadataImage::validate` and `apply`.
//! - [`Cluster::replay`] is the restart recovery of `Engine::open` in
//!   `kraft/controller/startup.rs`: the image of the newest checkpoint, then
//!   the committed batches from the replay start [`ReplayFrom`] names.
//! - [`Cluster::restart_plan`] is the real `compute_reregistration_changes`,
//!   which `registration_admission::admit` runs for a new incarnation.
//! - [`Cluster::alter_partition`] is the real per-row
//!   `handle_partition_with_recovery` followed by `ElrPublisher::extend`, as
//!   `alter_partition_once` composes them.
//! - [`fetch_outcome`] is the leader's epoch fence and leader refusal
//!   (`apply_epoch_checks` and `leader_refusal` in `handlers/fetch/plan.rs`),
//!   restated over the installed target, then the follower's real
//!   `replica_fetch_mutation`.

use {
    super::state::{Batch, Checkpoint, Members, NODES, Node, NodeState, REPLICAS, Target, View},
    crate::{
        codes, elr::ElrPublisher, handlers::alter_partition::handle_partition_with_recovery,
        leader_election::compute_reregistration_changes, metrics::BrokerMetrics,
        test_support::broker_registration,
    },
    krabka_metadata::{
        LeaderEpoch, LeaderRecoveryState, MetadataImage, MetadataRecord, PartitionRecord,
        PartitionRecoveryRecord, TopicConfigRecord, TopicRecord,
    },
    krabka_protocol::owned::alter_partition_request::PartitionData as AlterPartitionRow,
    krabka_raft::{NodeId, decode_committed_value, encode_record},
    krabka_verified::{ReplicaFetchMutation, broker::ReplicaFetchFacts, replica_fetch_mutation},
    std::collections::{BTreeMap, HashSet},
    uuid::Uuid,
};

/// The partition's topic.
pub(super) const TOPIC: &str = "t";
const TOPIC_ID: Uuid = Uuid::from_u128(0x7);

/// The fixed part of every image: the topic, with
/// `unclean.leader.election.enable=true`, and the three registered brokers.
/// The partition is replicated on [`REPLICAS`].
pub(super) struct Cluster {
    base: MetadataImage,
    metrics: BrokerMetrics,
}

/// Where a restart replay starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReplayFrom {
    /// The checkpoint's end, past the durable log start: `Engine::open` since
    /// #1940 replays from `log.log_start_offset().max(last_snapshot_end_offset)`,
    /// as Kafka's `MetadataLoader` applies only the batches after the snapshot
    /// it loaded.
    CheckpointEnd,
    /// The durable log start, as `Engine::open` replayed before #1940. An
    /// interval checkpoint does not prune the log
    /// (`kraft/controller/snapshotting.rs`), so in the model's bounds the log
    /// start is offset 0 and the batches below the checkpoint apply twice.
    LogStart,
}

/// What a restart replay rebuilt.
pub(super) struct Replayed {
    pub(super) view: View,
    /// Whether the log held committed batches below the checkpoint.
    pub(super) below_checkpoint: bool,
}

/// What a restart plan committed.
pub(super) struct Plan {
    pub(super) records: Vec<MetadataRecord>,
    /// Whether the plan elected a leader uncleanly: it put the partition in
    /// `RECOVERING`.
    pub(super) unclean: bool,
}

/// The `AlterPartition` a leader sends: its ISR proposal at the epochs its
/// image holds.
#[derive(Clone, Copy, Debug)]
pub(super) struct IsrRequest {
    pub(super) requester: Node,
    pub(super) new_isr: Members,
    pub(super) leader_epoch: u8,
    pub(super) partition_epoch: u8,
}

impl Cluster {
    pub(super) fn new() -> Self {
        let mut base = MetadataImage::new(Uuid::nil());
        base.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.into(),
            topic_id: TOPIC_ID,
            partitions: 1,
            replication_factor: 2,
        }));
        base.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides: BTreeMap::from([(
                "unclean.leader.election.enable".to_owned(),
                "true".to_owned(),
            )]),
        }));
        for node in NODES {
            base.apply(&MetadataRecord::V1BrokerRegistration(broker_registration(
                NodeId(u64::from(node)),
            )));
        }
        Self {
            base,
            metrics: BrokerMetrics::new(),
        }
    }

    /// The real image whose partition `view` projects.
    pub(super) fn image(&self, view: View) -> MetadataImage {
        let mut image = self.base.clone();
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: TOPIC.into(),
            partition: 0,
            leader: NodeId(u64::from(view.leader)),
            replicas: REPLICAS.into_iter().map(node_id).collect(),
            isr: view.isr.iter().map(node_id).collect(),
            leader_epoch: LeaderEpoch(i32::from(view.leader_epoch)),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: i32::from(view.partition_epoch),
        }));
        if view.recovering {
            image.apply(&MetadataRecord::V1PartitionRecovery(
                PartitionRecoveryRecord {
                    topic: TOPIC.into(),
                    partition: 0,
                    state: LeaderRecoveryState::Recovering,
                },
            ));
        }
        image
    }

    /// The controller's `prepare_records`: each record through the real
    /// `encode_record`, against the image the batch's earlier records produce.
    pub(super) fn commit(&self, controller: View, records: &[MetadataRecord]) -> Batch {
        let mut scratch = self.image(controller);
        let mut blobs = Vec::new();
        for record in records {
            blobs.extend(
                encode_record(record, &scratch)
                    .unwrap_or_else(|error| panic!("{record:?} does not encode: {error}")),
            );
            scratch.apply(record);
        }
        Batch(blobs)
    }

    /// The restart recovery of `Engine::open`: the checkpoint's image, then
    /// the committed batches from `replay_from` up to `applied`.
    pub(super) fn replay(
        &self,
        replay_from: ReplayFrom,
        checkpoint: Checkpoint,
        log: &[Batch],
        applied: u8,
    ) -> Replayed {
        let start = match replay_from {
            ReplayFrom::CheckpointEnd => checkpoint.offset,
            ReplayFrom::LogStart => 0,
        };
        let mut image = self.image(checkpoint.view);
        for batch in &log[usize::from(start)..usize::from(applied)] {
            apply_batch(&mut image, batch);
        }
        Replayed {
            view: project(&image),
            below_checkpoint: checkpoint.offset > 0,
        }
    }

    /// Live apply of one committed batch on a node whose image is `view`.
    pub(super) fn apply(&self, view: View, batch: &Batch) -> View {
        let mut image = self.image(view);
        apply_batch(&mut image, batch);
        project(&image)
    }

    /// The real restart plan for `returning`, on the controller's image. Every
    /// node counts as alive: a crash-restart is atomic in the model.
    pub(super) fn restart_plan(&self, controller: View, returning: Node) -> Plan {
        let image = self.image(controller);
        let alive: HashSet<NodeId> = NODES.into_iter().map(node_id).collect();
        let plan =
            compute_reregistration_changes(&image, node_id(returning), &alive, &self.metrics);
        let unclean = plan.changes.iter().any(|record| {
            matches!(
                record,
                MetadataRecord::V1PartitionUpdate(update)
                    if update.recovery_state == Some(LeaderRecoveryState::Recovering)
            ) || matches!(
                record,
                MetadataRecord::V1PartitionRecovery(recovery)
                    if recovery.state == LeaderRecoveryState::Recovering
            )
        });
        Plan {
            records: plan.changes,
            unclean,
        }
    }

    /// The controller's answer to one `AlterPartition` row, and the records it
    /// submits for it.
    pub(super) fn alter_partition(
        &self,
        controller: View,
        request: IsrRequest,
    ) -> (i16, Vec<MetadataRecord>) {
        let image = self.image(controller);
        let active: HashSet<u64> = NODES.into_iter().map(u64::from).collect();
        let row = AlterPartitionRow {
            partition_index: 0,
            leader_epoch: i32::from(request.leader_epoch),
            new_isr: request.new_isr.iter().map(i32::from).collect(),
            // The leader always reports `RECOVERED`
            // (`isr_maintenance::request_builder`).
            leader_recovery_state: 0,
            partition_epoch: i32::from(request.partition_epoch),
            ..Default::default()
        };
        let mut changes = Vec::new();
        let answer = handle_partition_with_recovery(
            &image,
            &active,
            i32::from(request.requester),
            TOPIC,
            &row,
            &mut changes,
        );
        ElrPublisher::new(&image).extend(&mut changes);
        (answer.error_code, changes)
    }
}

/// Decode and apply one committed batch, as live apply and restart recovery
/// both do. A record that names state the image does not hold decodes to
/// `None` and is skipped.
fn apply_batch(image: &mut MetadataImage, batch: &Batch) {
    for value in &batch.0 {
        match decode_committed_value(value, image, 0) {
            Ok(Some(record)) => {
                if image.validate(&record).is_ok() {
                    image.apply(&record);
                }
            }
            Ok(None) => {}
            Err(error) => panic!("a committed record does not decode: {error}"),
        }
    }
}

fn project(image: &MetadataImage) -> View {
    let record = image
        .partition(TOPIC, 0)
        .expect("the model never deletes its partition");
    View {
        leader: small(record.leader.0),
        leader_epoch: small(record.leader_epoch.0),
        isr: Members::of(record.isr.iter().map(|node| small(node.0))),
        partition_epoch: small(record.partition_epoch),
        recovering: image.leader_recovery_state(TOPIC, 0) == LeaderRecoveryState::Recovering,
    }
}

fn small<T: TryInto<u8>>(value: T) -> u8 {
    value
        .try_into()
        .unwrap_or_else(|_| panic!("a model value outgrew u8"))
}

fn node_id(node: Node) -> NodeId { NodeId(u64::from(node)) }

/// How a fetch row ended, as the replica fetcher acts on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FetchOutcome {
    /// `ReplicaFetchMutation::Append`: the follower caught up.
    Appended,
    /// `RowAction::Drop`: "holding the partition back until the metadata
    /// image settles". A `Reject`, a `NOT_LEADER_OR_FOLLOWER`, or an epoch
    /// error from a target the follower's image has moved off.
    HeldBack,
    /// `RowAction::Backoff`: an epoch error from the target the image still
    /// names, retried after `epoch_fence_backoff`.
    BackedOff,
}

/// One fetch by `follower` from the node `leader` names, at the follower's
/// fetcher target.
///
/// The leader side mirrors `apply_epoch_checks`: the fence against the
/// installed epoch comes first (`Partition::fetch_leader_epoch_fence`), with
/// the leader hint filled for `FENCED_LEADER_EPOCH` only; then
/// `leader_refusal`, which answers `NOT_LEADER_OR_FOLLOWER` when the leader's
/// image names another leader or its installed target does. A partition the
/// leader has not materialized since a restart answers
/// `NOT_LEADER_OR_FOLLOWER` too. The follower side is the real
/// `replica_fetch_mutation` and `retry_after_error`'s mapping of the error to
/// a hold-back or a back-off.
pub(super) fn fetch_outcome(
    follower: &NodeState,
    target: Target,
    leader: &NodeState,
) -> FetchOutcome {
    let request_epoch = target.leader_epoch;
    let image_hint = Target {
        leader: leader.image.leader,
        leader_epoch: leader.image.leader_epoch,
    };
    let (error_code, reported) = match leader.installed {
        None => (codes::NOT_LEADER_OR_FOLLOWER, Some(image_hint)),
        Some(installed) if request_epoch < installed.leader_epoch => (
            codes::FENCED_LEADER_EPOCH,
            Some(Target {
                leader: leader.image.leader,
                leader_epoch: installed.leader_epoch,
            }),
        ),
        Some(installed) if request_epoch > installed.leader_epoch => {
            (codes::UNKNOWN_LEADER_EPOCH, None)
        }
        Some(installed)
            if leader.image.leader != target.leader || installed.leader != target.leader =>
        {
            (codes::NOT_LEADER_OR_FOLLOWER, Some(image_hint))
        }
        Some(_) => (codes::NONE, None),
    };
    let target_matches = follower.image.target() == target;
    let mutation = replica_fetch_mutation(ReplicaFetchFacts {
        request_leader_epoch: i32::from(request_epoch),
        current_leader_epoch: i32::from(target.leader_epoch),
        target_matches,
        reported_target_matches: reported.is_none_or(|reported| reported == target),
        error_code,
        diverging_epoch: -1,
    });
    match mutation {
        ReplicaFetchMutation::Append => FetchOutcome::Appended,
        ReplicaFetchMutation::Reject => FetchOutcome::HeldBack,
        ReplicaFetchMutation::Truncate => unreachable!("the model's leader names no divergence"),
        ReplicaFetchMutation::Retry => match error_code {
            codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH if target_matches => {
                FetchOutcome::BackedOff
            }
            _ => FetchOutcome::HeldBack,
        },
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::state::{Checkpoint, Members, View},
            Cluster, ReplayFrom,
        },
        assert2::assert,
    };

    const LED_ALONE: View = View {
        leader: 1,
        leader_epoch: 0,
        isr: Members::of_const(1 << 1),
        partition_epoch: 0,
        recovering: false,
    };

    const ELECTED: View = View {
        leader: 2,
        leader_epoch: 1,
        isr: Members::of_const(1 << 2),
        partition_epoch: 1,
        recovering: true,
    };

    /// The unclean election a restart of the lone ISR member commits reaches
    /// the log as a `PartitionChangeRecord` that sets the leader, so its
    /// decode bumps the epoch of whatever image it meets. A restart from a
    /// checkpoint that already holds it replays nothing below the checkpoint;
    /// the replay before #1940 applied it a second time.
    #[test]
    fn a_restart_from_a_checkpoint_holding_the_unclean_election() {
        let cluster = Cluster::new();
        let plan = cluster.restart_plan(LED_ALONE, 1);
        assert!(plan.unclean);
        let log = vec![cluster.commit(LED_ALONE, &plan.records)];
        assert!(cluster.apply(LED_ALONE, &log[0]) == ELECTED);
        let checkpoint = Checkpoint {
            view: ELECTED,
            offset: 1,
        };
        for (replay_from, expected) in [
            (ReplayFrom::CheckpointEnd, ELECTED),
            (ReplayFrom::LogStart, View {
                leader_epoch: 2,
                partition_epoch: 2,
                ..ELECTED
            }),
        ] {
            let replayed = cluster.replay(replay_from, checkpoint, &log, 1);
            assert!(replayed.view == expected, "replay from {replay_from:?}");
            assert!(replayed.below_checkpoint);
        }
    }
}
