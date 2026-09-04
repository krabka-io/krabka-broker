//! Registration of `KRaft` quorum, cluster-wide metadata state, tiered storage,
//! replication throttling, and per-entity client quota metrics.

use prometheus_client::registry::Registry;

use crate::metrics::BrokerMetrics;

impl BrokerMetrics {
    pub(super) fn register_group_9(&self, registry: &mut Registry) {
        registry.register(
            "raft_current_state",
            "Current state of the KRaft consensus state machine (one-hot across leader, follower, candidate, observer)",
            self.raft_current_state.clone(),
        );
        registry.register(
            "raft_current_epoch",
            "Current KRaft leader epoch",
            self.raft_current_epoch.clone(),
        );
        registry.register(
            "raft_high_watermark",
            "High watermark offset of the local metadata log",
            self.raft_high_watermark.clone(),
        );
        registry.register(
            "raft_log_end_offset",
            "Log end offset of the local metadata log",
            self.raft_log_end_offset.clone(),
        );
        registry.register(
            "raft_voters",
            "Number of active KRaft voters",
            self.raft_voters.clone(),
        );
        registry.register(
            "raft_observers",
            "Number of active KRaft observers",
            self.raft_observers.clone(),
        );
        registry.register(
            "metadata_last_applied_offset",
            "Highest metadata record offset applied to the active metadata image",
            self.metadata_last_applied_offset.clone(),
        );
        registry.register(
            "metadata_lag_records",
            "Lag in records between the quorum committed high watermark and this node's applied metadata offset",
            self.metadata_lag_records.clone(),
        );
        registry.register(
            "broker_state",
            "Kafka BrokerState lifecycle code (1=STARTING, 2=RECOVERY, 3=RUNNING, 6=PENDING_CONTROLLED_SHUTDOWN, 7=SHUTTING_DOWN)",
            self.broker_state.clone(),
        );
        registry.register(
            "active_brokers",
            "Number of unfenced brokers in the cluster (reported by active controller)",
            self.active_brokers.clone(),
        );
        registry.register(
            "fenced_brokers",
            "Number of fenced brokers in the cluster (reported by active controller)",
            self.fenced_brokers.clone(),
        );
        registry.register(
            "global_topics",
            "Total number of topics in the cluster metadata image (reported by active controller)",
            self.global_topics.clone(),
        );
        registry.register(
            "global_partitions",
            "Total number of topic partitions in the cluster metadata image (reported by active controller)",
            self.global_partitions.clone(),
        );
        registry.register(
            "at_min_isr_partition_count",
            "Number of partitions whose in-sync replica count equals min.insync.replicas",
            self.at_min_isr_partition_count.clone(),
        );
        registry.register(
            "reassigning_partitions",
            "Number of partitions currently undergoing replica reassignment",
            self.reassigning_partitions.clone(),
        );
        registry.register(
            "preferred_replica_imbalance",
            "Number of partitions whose current leader is not the preferred replica",
            self.preferred_replica_imbalance.clone(),
        );
        registry.register(
            "queued_requests",
            "Number of client requests currently queued awaiting execution",
            self.queued_requests.clone(),
        );
        registry.register(
            "queued_request_bytes",
            "Total bytes of client requests currently queued awaiting execution",
            self.queued_request_bytes.clone(),
        );
    }

    pub(super) fn register_group_10(&self, registry: &mut Registry) {
        registry.register(
            "remote_copy_bytes",
            "Cumulative bytes successfully copied to remote storage per topic",
            self.remote_copy_bytes_total.clone(),
        );
        registry.register(
            "remote_fetch_bytes",
            "Cumulative bytes served from remote storage per topic",
            self.remote_fetch_bytes_total.clone(),
        );
        registry.register(
            "remote_copy_requests",
            "Cumulative copy attempts to remote storage per topic",
            self.remote_copy_requests_total.clone(),
        );
        registry.register(
            "remote_fetch_requests",
            "Cumulative fetch attempts from remote storage per topic",
            self.remote_fetch_requests_total.clone(),
        );
        registry.register(
            "remote_delete_requests",
            "Cumulative delete requests submitted to remote storage per topic",
            self.remote_delete_requests_total.clone(),
        );
        registry.register(
            "remote_copy_errors",
            "Cumulative failed remote copy attempts per topic",
            self.remote_copy_errors_total.clone(),
        );
        registry.register(
            "remote_fetch_errors",
            "Cumulative failed remote fetch attempts per topic",
            self.remote_fetch_errors_total.clone(),
        );
        registry.register(
            "remote_delete_errors",
            "Cumulative failed remote delete attempts per topic",
            self.remote_delete_errors_total.clone(),
        );
        registry.register(
            "remote_copy_lag_bytes",
            "Bytes in eligible log segments pending remote copy per topic",
            self.remote_copy_lag_bytes.clone(),
        );
        registry.register(
            "remote_copy_lag_segments",
            "Number of eligible log segments pending remote copy per topic",
            self.remote_copy_lag_segments.clone(),
        );
        registry.register(
            "remote_delete_lag_bytes",
            "Bytes in expired remote segments pending remote deletion per topic",
            self.remote_delete_lag_bytes.clone(),
        );
        registry.register(
            "remote_delete_lag_segments",
            "Number of expired remote segments pending remote deletion per topic",
            self.remote_delete_lag_segments.clone(),
        );
        registry.register(
            "replication_throttled_bytes_out",
            "Outbound replication bytes throttled by leader replication quota",
            self.replication_throttled_bytes_out_total.clone(),
        );
        registry.register(
            "replication_throttled_bytes_in",
            "Inbound replication bytes throttled by follower replication quota",
            self.replication_throttled_bytes_in_total.clone(),
        );
        registry.register(
            "replication_throttle_sleeps",
            "Replication fetch requests delayed or rejected by replication quota",
            self.replication_throttle_sleeps_total.clone(),
        );
        registry.register(
            "quota_entity_throttle_seconds",
            "Cumulative throttle duration in seconds applied per quota entity",
            self.quota_entity_throttle_seconds_total.clone(),
        );
    }

    pub(super) fn register_group_11(&self, registry: &mut Registry) {
        registry.register(
            "remote_log_reader_task_queue_size",
            "Cold-tier reads waiting for a slot in the bounded remote reader pool",
            self.remote_log_reader_task_queue_size.clone(),
        );
        registry.register(
            "remote_log_reader_avg_idle_percent",
            "Percentage of the remote reader pool's slots that are currently free",
            self.remote_log_reader_avg_idle_percent.clone(),
        );
        registry.register(
            "remote_log_reader_fetch_duration_seconds",
            "Time each cold-tier read spent holding a remote reader slot",
            self.remote_log_reader_fetch_duration_seconds.clone(),
        );
        registry.register(
            "remote_log_reader_rejected",
            "Cold-tier reads refused because the remote reader pool's pending queue was full",
            self.remote_log_reader_rejected_total.clone(),
        );
        registry.register(
            "remote_index_cache_hits",
            "Remote segment index lookups served from the on-disk index cache",
            self.remote_index_cache_hits_total.clone(),
        );
        registry.register(
            "remote_index_cache_misses",
            "Remote segment index lookups that downloaded the index object",
            self.remote_index_cache_misses_total.clone(),
        );
        registry.register(
            "remote_index_cache_evictions",
            "Remote index cache entries dropped to stay inside the byte budget",
            self.remote_index_cache_evictions_total.clone(),
        );
        registry.register(
            "remote_index_cache_bytes",
            "Bytes currently held by the remote index cache",
            self.remote_index_cache_bytes.clone(),
        );
        registry.register(
            "remote_index_cache_entries",
            "Entries currently held by the remote index cache",
            self.remote_index_cache_entries.clone(),
        );
    }
}
