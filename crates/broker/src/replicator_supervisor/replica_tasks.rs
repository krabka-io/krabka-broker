//! Configuration of the per-partition follower replicator tasks: the leader
//! endpoint and runtime policy that each spawned `replicator::run` task
//! receives.

use tokio_util::sync::CancellationToken;

use super::{ReplicatorSupervisor, TopicPartition, resolve_leader_endpoint};
use crate::replicator;

impl ReplicatorSupervisor {
    pub(super) fn replicator_config(
        &self,
        key: &TopicPartition,
        topic: &krabka_metadata::TopicRecord,
        partition: &krabka_metadata::PartitionRecord,
        broker: &krabka_metadata::BrokerRegistrationRecord,
        shutdown: CancellationToken,
    ) -> replicator::Config {
        let (leader_host, leader_port) =
            resolve_leader_endpoint(broker, &self.inter_broker_listener_name);
        replicator::Config {
            node_id: self.node_id,
            // The registry already owns one copy of the name, and the task
            // records a per-batch metric under it; take that copy rather than
            // the one this key carries.
            topic: self.partitions.shared_topic_name(&key.0),
            topic_id: krabka_protocol::primitives::uuid::Uuid(topic.topic_id.into_bytes()),
            partition: krabka_ids::PartitionIndex(key.1),
            leader_node_id: partition.leader,
            leader_epoch: partition.leader_epoch,
            leader_host,
            leader_port,
            partitions: self.partitions.clone(),
            log_dirs: self.log_dirs.clone(),
            log_settings: self.log_config.clone(),
            client_id: self.client_id.clone(),
            shutdown,
            inter_broker_client: self.inter_broker_client.clone(),
            inter_broker_listener_protocol: self.inter_broker_listener_protocol,
            inter_broker_server_name: self.inter_broker_server_name.clone(),
            replication: self.replication.clone(),
            throttle_state: self.throttle_state.clone(),
            controller: self.controller.clone(),
            log_dir_status: self.log_dir_status.clone(),
            producer_state: self.producer_state.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use krabka_raft::NodeId;
    use krabka_units::{bytes, millis};
    use uuid::Uuid;

    use super::*;
    use crate::replicator_supervisor::{
        ReplicatorTask, ReplicatorTaskTarget,
        test_support::{
            await_until, broker_record, image_with, partition_record, supervisor_fixture,
            topic_record,
        },
    };

    fn running_replicator_task(
        target: ReplicatorTaskTarget,
        shutdown: CancellationToken,
    ) -> ReplicatorTask {
        let child_shutdown = shutdown.clone();
        ReplicatorTask {
            shutdown,
            target,
            handle: tokio::spawn(async move { child_shutdown.cancelled().await }),
        }
    }

    #[test]
    fn replicator_task_config_receives_runtime_policy_and_tls_server_name() {
        let image = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 7),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ]);
        let (mut supervisor, _partitions, _reporter, _dir) = supervisor_fixture(image.clone());
        supervisor.replication.fetch_max = bytes(2_345_678);
        supervisor.replication.send_error_backoff = millis(37);
        supervisor.inter_broker_server_name = "broker.internal".into();
        let broker = image.broker(NodeId(1)).expect("leader broker");
        let topic = image.topic("t").expect("topic");

        let config = supervisor.replicator_config(
            &("t".into(), 0),
            topic,
            image.partition("t", 0).expect("partition"),
            broker,
            CancellationToken::new(),
        );

        assert!(config.replication.fetch_max == bytes(2_345_678));
        assert!(config.replication.send_error_backoff == millis(37));
        assert!(config.inter_broker_server_name == "broker.internal");
    }

    #[tokio::test]
    async fn reconcile_cancels_tasks_for_removed_partitions() {
        let img = MetadataImage::new(Uuid::nil());
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            ("stale".into(), 0),
            running_replicator_task(
                ReplicatorTaskTarget {
                    topic_id: Uuid::new_v4(),
                    leader: NodeId(1),
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                },
                token.clone(),
            ),
        );

        supervisor.reconcile(&img).await;

        check!(token.is_cancelled());
        check!(supervisor.tasks.len() == 0);
    }

    #[tokio::test]
    async fn reconcile_cancels_task_when_target_leader_or_epoch_changes() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 8),
        ]);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            ("t".into(), 0),
            running_replicator_task(
                ReplicatorTaskTarget {
                    topic_id: img.topic("t").expect("topic").topic_id,
                    leader: NodeId(9),
                    leader_epoch: krabka_metadata::LeaderEpoch(7),
                },
                token.clone(),
            ),
        );

        supervisor.reconcile(&img).await;

        check!(token.is_cancelled());
        check!(supervisor.tasks.len() == 0);
    }

    #[tokio::test]
    async fn reconcile_cancels_task_for_recreated_topic_with_same_generation() {
        let new_topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: new_topic_id,
                partitions: 1,
                replication_factor: 2,
            }),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 8),
        ]);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            ("t".into(), 0),
            running_replicator_task(
                ReplicatorTaskTarget {
                    topic_id: Uuid::new_v4(),
                    leader: NodeId(1),
                    leader_epoch: krabka_metadata::LeaderEpoch(8),
                },
                token.clone(),
            ),
        );

        supervisor.reconcile(&img).await;

        assert!(token.is_cancelled());
        assert!(supervisor.tasks.is_empty());
    }

    #[tokio::test]
    async fn reconcile_respawns_exited_replicator_task() {
        let img = image_with(&[
            topic_record("t", 1),
            partition_record("t", 0, NodeId(1), vec![NodeId(1), NodeId(2)], 8),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ]);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let stale_shutdown = CancellationToken::new();
        supervisor.tasks.insert(
            ("t".into(), 0),
            ReplicatorTask {
                shutdown: stale_shutdown.clone(),
                target: ReplicatorTaskTarget {
                    topic_id: img.topic("t").expect("topic").topic_id,
                    leader: NodeId(1),
                    leader_epoch: krabka_metadata::LeaderEpoch(8),
                },
                handle: tokio::spawn(async {}),
            },
        );
        await_until("old replicator exits", || {
            supervisor
                .tasks
                .get(&("t".into(), 0))
                .is_some_and(|task| task.handle.is_finished())
        })
        .await;

        supervisor.reconcile(&img).await;

        assert!(stale_shutdown.is_cancelled());
        let replacement = supervisor.tasks.get(&("t".into(), 0)).expect("respawned");
        assert!(!replacement.handle.is_finished());
        replacement.shutdown.cancel();
    }
}
