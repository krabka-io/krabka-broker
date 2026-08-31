//! Registration of the families keyed by topic, partition and replica: the
//! topic and partition byte, message and request counters, the replication
//! byte counters, and the partition-count, ISR and controller gauges.

use prometheus_client::registry::Registry;

use crate::metrics::BrokerMetrics;

impl BrokerMetrics {
    pub(super) fn register_group_1(&self, registry: &mut Registry) {
        registry.register(
            "topic_bytes_in",
            "Bytes received from producers, per topic (cumulative). \
             Operators compute throughput via rate(...).",
            self.topic_bytes_in.clone(),
        );

        registry.register(
            "topic_bytes_out",
            "Bytes delivered to fetchers, per topic (cumulative).",
            self.topic_bytes_out.clone(),
        );

        registry.register(
            "messages_in",
            "Cumulative count of records received from \
             producers, per topic. Mirrors Kafka's \
             BrokerTopicMetrics.MessagesInPerSec. Legacy v0/v1 \
             produce payloads are not counted (their per-record body \
             stays opaque on the Produce path); the paired \
             produce_message_conversions counter tracks the \
             legacy-arrival rate so operators can detect \
             under-counting.",
            self.topic_messages_in.clone(),
        );

        registry.register(
            "topic_produce_requests",
            "Produce requests handled, per topic (cumulative). One \
             increment per topic per Produce request.",
            self.topic_produce_requests.clone(),
        );

        registry.register(
            "topic_fetch_requests",
            "Fetch requests handled, per topic (cumulative). One \
             increment per topic per Fetch request.",
            self.topic_fetch_requests.clone(),
        );

        registry.register(
            "topic_failed_produce_requests",
            "Cumulative count of Produce partition \
             responses that returned a non-zero error code, per \
             topic. Mirrors Kafka's \
             BrokerTopicMetrics.FailedProduceRequestsPerSec. \
             Operators alert on rate(...) > 0 to catch quota / ACL \
             / NOT_ENOUGH_REPLICAS storms; the ratio against \
             topic_produce_requests yields the per-topic error rate.",
            self.topic_failed_produce_requests.clone(),
        );

        registry.register(
            "topic_failed_fetch_requests",
            "Cumulative count of Fetch partition \
             responses that returned a non-zero error code, per \
             topic. Mirrors Kafka's \
             BrokerTopicMetrics.FailedFetchRequestsPerSec. Pairs \
             with topic_fetch_requests for per-topic error rate.",
            self.topic_failed_fetch_requests.clone(),
        );

        registry.register(
            "partitions_led",
            "Number of partitions for which this broker is currently leader.",
            self.partitions_led.clone(),
        );

        registry.register(
            "partitions_total",
            "Total number of partitions (leader + follower \
             replicas) this broker hosts. Mirrors Kafka's \
             ReplicaManager.PartitionCount.",
            self.partitions_total.clone(),
        );

        registry.register(
            "under_replicated_partitions",
            "Count of partitions this broker leads whose ISR \
             is smaller than the assigned replica set. Mirrors Kafka's \
             ReplicaManager.UnderReplicatedPartitions; alert on > 0 \
             to spot stuck followers before they fail an unclean \
             election.",
            self.under_replicated_partitions.clone(),
        );
    }

    pub(super) fn register_group_2(&self, registry: &mut Registry) {
        registry.register(
            "under_min_isr_partition_count",
            "Count of partitions this broker leads whose ISR \
             is strictly less than the topic's min.insync.replicas. \
             Mirrors Kafka's ReplicaManager.UnderMinIsrPartitionCount; \
             alert on > 0 — these partitions reject acks=all produces \
             with NOT_ENOUGH_REPLICAS.",
            self.under_min_isr_partition_count.clone(),
        );

        registry.register(
            "offline_partitions_count",
            "Count of partitions this broker leads that have \
             no live leader (leader dead with no eligible ISR \
             replacement). Mirrors Kafka's \
             ReplicaManager.OfflinePartitionsCount; alert on > 0 — \
             these partitions are wholly unavailable until an ISR \
             member returns or an unclean election runs.",
            self.offline_partitions_count.clone(),
        );

        registry.register(
            "active_controller",
            "1 if this broker is the raft (controller) leader, 0 otherwise.",
            self.active_controller.clone(),
        );

        registry.register(
            "ignored_static_voters",
            "Configured static controller voters ignored at kraft.version 1.",
            self.ignored_static_voters.clone(),
        );

        registry.register(
            "witness_role",
            "1 if this node carries the data-bearing witness role, 0 \
             otherwise. The value comes from the broker.witness config in \
             the metadata image, so it confirms that the role reached the \
             controller.",
            self.witness_role.clone(),
        );

        registry.register(
            "leader_site_drift_partitions",
            "Count of partitions this broker leads from a site other than \
             the stretch cluster's preferred leader site. It stays at zero \
             on a cluster that pins leadership to no site; alert on > 0 to \
             catch leadership that drifted away from the pinned site.",
            self.leader_site_drift_partitions.clone(),
        );

        registry.register(
            "voted_directory",
            "1 for the controller directory identity voted for in this epoch.",
            self.voted_directory.clone(),
        );

        registry.register(
            "controller_leader_changes",
            "Cumulative count of distinct controller-leader \
             transitions this broker has observed (any change in the \
             raft leader, including this broker becoming or ceasing \
             to be leader). Mirrors Kafka's \
             KafkaController.LeaderElectionRateAndTimeMs; alert on a \
             sustained rate() > 0 to spot flapping raft leadership.",
            self.controller_leader_changes_total.clone(),
        );

        registry.register(
            "isr_shrinks",
            "Cumulative count of ISR shrinks proposed by this broker's \
             ISR-maintenance loop.",
            self.isr_shrinks_total.clone(),
        );

        registry.register(
            "isr_expands",
            "Cumulative count of ISR expands proposed by this broker's \
             ISR-maintenance loop.",
            self.isr_expands_total.clone(),
        );

        registry.register(
            "partition_bytes_in",
            "Bytes received from producers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            self.partition_bytes_in.clone(),
        );

        registry.register(
            "partition_bytes_out",
            "Bytes served to consumers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            self.partition_bytes_out.clone(),
        );

        registry.register(
            "replication_bytes_in",
            "Bytes received from the partition leader by this broker as a \
             follower (cumulative). Rate(...) for follower throughput; \
             plotted alongside partition_bytes_in surfaces ingest vs. \
             replication-driven traffic.",
            self.replication_bytes_in.clone(),
        );

        registry.register(
            "replication_bytes_out",
            "Bytes this broker served to followers as the partition leader \
             (cumulative). Rate(...) for leader-out-to-followers throughput; \
             together with partition_bytes_out (consumer reads) it attributes \
             outbound traffic to its source.",
            self.replication_bytes_out.clone(),
        );

        registry.register(
            "replica_lag_records",
            "Records a follower of a partition this broker leads has yet \
             to fetch: the leader's log end offset minus that follower's \
             last-fetched offset. Where under_replicated_partitions says \
             only that a follower left the ISR, this says how far behind \
             it is while it is still in.",
            self.replica_lag.clone(),
        );

        registry.register(
            "replica_lag_max_records",
            "The largest value replica_lag_records carries on this \
             broker, or zero when it leads no partition with a follower. \
             Mirrors Kafka's ReplicaFetcherManager.MaxLag; alert on it \
             rather than aggregating the per-follower family.",
            self.replica_lag_max.clone(),
        );

        registry.register(
            "consumer_group_lag_records",
            "Records a consumer group this broker coordinates has yet to \
             consume from one partition: the partition's high watermark \
             minus the group's committed offset. Classic and KIP-848 \
             groups both report here.",
            self.consumer_group_lag.clone(),
        );
    }
}
