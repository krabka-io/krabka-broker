//! Registration of the request-path and per-broker resource families: the
//! api-request, latency, connection and error families, the client-identity
//! and authentication counters, the partition disk and CPU gauges, the
//! fetch-session cache gauges, tiered storage, message conversions and
//! unclean elections.

use prometheus_client::registry::Registry;

use crate::metrics::BrokerMetrics;

impl BrokerMetrics {
    pub(super) fn register_group_3(&self, registry: &mut Registry) {
        registry.register(
            "partition_disk_bytes",
            "On-disk size of a partition's log directory (gauge). Updated by \
             the broker's periodic disk scanner; suppress if scanner is disabled.",
            self.partition_disk_bytes.clone(),
        );

        registry.register(
            "share_group_backlog",
            "Share-group partition backlog in records, emitted by the group coordinator.",
            self.share_group_backlog.clone(),
        );

        registry.register(
            "partition_cpu_micros",
            "Cumulative handler-thread microseconds spent processing each \
             (topic, partition). Rebalancer-targeted; rate(...) divided by \
             1_000_000 yields core occupancy.",
            self.partition_cpu_micros.clone(),
        );

        registry.register(
            "fetch_response_drain",
            "Cumulative count of drained Fetch responses, labelled by the path \
             their records regions took to the socket: sendfile (kernel \
             zero-copy), pread (a file-backed region the drain had to copy \
             through a buffer), or vectored (no file-backed region). \
             rate(...{path=\"sendfile\"}) is how an operator sees that the \
             zero-copy fetch path is carrying traffic.",
            self.fetch_response_drain.clone(),
        );
        // Create all three series at zero. The drain only ever touches the
        // path it took, and on some targets it can never take two of them, so
        // without this a dashboard panel or an alert that names `sendfile` has
        // no series to read until the first zero-copy fetch — or, on Windows,
        // ever.
        for path in crate::metrics::FetchDrainPath::ALL {
            let _ = self
                .fetch_response_drain
                .get_or_create(&crate::metrics::FetchDrainPathLabel { path });
        }

        registry.register(
            "ktls_enabled",
            "1 when the startup probe found working Linux kTLS and TLS fetch \
             connections drain records through kernel-offloaded sendfile; 0 \
             when they encrypt in userspace, including on a broker with no TLS \
             listener and on every non-Linux target.",
            self.ktls_enabled.clone(),
        );

        registry.register(
            "incremental_fetch_sessions",
            "KIP-227: live incremental-fetch sessions cached by this broker (gauge).",
            self.incremental_fetch_sessions.clone(),
        );

        registry.register(
            "incremental_fetch_session_evictions",
            "KIP-227: cumulative count of incremental-fetch sessions evicted from \
             the cache to make room for a new allocation.",
            self.incremental_fetch_session_evictions_total.clone(),
        );

        registry.register(
            "incremental_fetch_partitions_cached",
            "KIP-227: total (topic, partition) tuples held across every live \
             incremental-fetch session (gauge).",
            self.incremental_fetch_partitions_cached.clone(),
        );

        registry.register(
            "client_software_versions",
            "KIP-511: cumulative count of accepted ApiVersions handshakes, \
             labelled by client software name and version. One increment \
             per successful v3+ ApiVersions call.",
            self.client_software_versions.clone(),
        );

        registry.register(
            "successful_authentication",
            "Cumulative count of SaslAuthenticate frames per \
             mechanism that ended in a successful auth state transition. \
             Mirrors Kafka's \
             kafka.network:type=Selector,name=successful-authentication-total. \
             Labelled by the canonical SASL mechanism wire name \
             (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER). \
             Paired with failed_authentication so rate(...) ratios \
             expose per-mechanism credential-failure rates.",
            self.successful_authentication.clone(),
        );

        registry.register(
            "failed_authentication",
            "Cumulative count of SaslAuthenticate frames per \
             mechanism that returned a non-zero error code. Mirrors \
             Kafka's failed-authentication-total. ILLEGAL_SASL_STATE \
             rejects (SaslAuthenticate without prior SaslHandshake) \
             land under the `Unknown` mechanism label.",
            self.failed_authentication.clone(),
        );

        registry.register(
            "api_requests",
            "Cumulative count of dispatched requests per \
             Kafka API key (variant name from the `ApiKey` enum, e.g. \
             Produce / Fetch / DescribeQuorum). Unknown api keys land \
             under the `Unknown` label. Mirrors Kafka's \
             RequestMetrics.RequestsPerSec; rate(...) yields per-API \
             throughput.",
            self.api_requests.clone(),
        );

        registry.register(
            "unsupported_api_requests",
            "Cumulative count of requests the dispatcher rejected \
             because the request version was outside the registered range. \
             Labelled with the ApiKey variant name. Alert on rate(...) > 0 \
             to catch upgrade-skew or \
             misconfigured clients.",
            self.unsupported_api_requests.clone(),
        );
    }

    pub(super) fn register_group_4(&self, registry: &mut Registry) {
        registry.register(
            "request_duration_seconds",
            "Per-Kafka-API request-handling latency in \
             seconds, observed in the dispatch path around the full \
             handler round-trip (decode → handle → encode). Labelled by \
             the ApiKey variant name. Operators graph \
             histogram_quantile(0.99, rate(..._bucket[5m])) per api to \
             spot tail-latency regressions.",
            self.request_duration_seconds.clone(),
        );

        registry.register(
            "request_local_duration_seconds",
            "Per-Kafka-API seconds one request spent on this \
             broker's own log: the Produce writer round-trip summed over the \
             request's partitions, or the Fetch read of every planned \
             partition. Mirrors Kafka's RequestMetrics.LocalTimeMs. Labelled \
             by the ApiKey variant name, like request_duration_seconds.",
            self.request_local_duration_seconds.clone(),
        );

        registry.register(
            "request_remote_duration_seconds",
            "Per-Kafka-API seconds one request spent waiting on \
             another broker: the acks=all high-watermark gate for Produce, \
             the long poll for Fetch. Mirrors Kafka's \
             RequestMetrics.RemoteTimeMs. High here with a low \
             request_local_duration_seconds is a lagging follower, not a slow \
             disk.",
            self.request_remote_duration_seconds.clone(),
        );

        registry.register(
            "request_throttle_duration_seconds",
            "Per-Kafka-API seconds one request slept in the \
             KIP-219 quota throttle, or in the KIP-599 controller-mutation \
             throttle the topic-mutating admin apis apply inline. Mirrors \
             Kafka's RequestMetrics.ThrottleTimeMs. Observed once per request \
             whose quota the broker accounts for, with an explicit zero when no \
             quota applied. The three phase families are disjoint and sum to \
             at most the total; the remainder is decode, authorization, \
             validation and encode.",
            self.request_throttle_duration_seconds.clone(),
        );

        registry.register(
            "quota_throttle_duration_seconds",
            "Seconds of throttle the broker actually applied, \
             labelled by the client quota that caused it (Produce = \
             producer_byte_rate, Fetch = consumer_byte_rate, Request = \
             request_percentage, ControllerMutation = \
             controller_mutation_rate). A request sleeps for the largest of the \
             delays it is charged, and the sample lands under the quota that \
             produced it — the one an operator would raise to stop the \
             throttle. Unthrottled requests are not observed, so _count is \
             the number of throttled requests.",
            self.quota_throttle_duration_seconds.clone(),
        );

        registry.register(
            "in_flight_requests",
            "Number of requests currently being handled by this broker \
             (gauge). Incremented on dispatch entry, decremented on exit; \
             a sustained climb signals handler stalls.",
            self.in_flight_requests.clone(),
        );

        registry.register(
            "active_connections",
            "Number of client connections currently open to this broker \
             (gauge). Incremented when the per-connection serve loop \
             starts, decremented when it exits (EOF / error / SASL expiry).",
            self.active_connections.clone(),
        );

        registry.register(
            "connection_closes",
            "Cumulative count of client connections the broker closed on its \
             own, labelled by reason: idle, sasl_session_expired, \
             decode_error, peer_closed. Alert on \
             rate(...{reason=\"idle\"}[5m]) to catch a peer that connects and \
             then sends nothing.",
            self.connection_closes.clone(),
        );

        registry.register(
            "request_errors",
            "Per-Kafka-API count of requests whose handler \
             returned an error (dispatcher closed the connection). \
             Labelled by the ApiKey variant name; disjoint from \
             unsupported_api_requests. Alert on rate(...) > 0 to catch \
             handler-level faults.",
            self.request_errors.clone(),
        );

        registry.register(
            "tiered_storage_rlmm_topic_backed",
            "KIP-405: 1 when this broker is answering remote-log \
             metadata queries from the durable __remote_log_metadata topic \
             (production RLMM); 0 while still on the fail-closed \
             NotReadyRlmm placeholder. Bumped to 1 by the bootstrap task \
             after a successful SwappableRlmm swap; stays at 0 for \
             clusters that never asked for `metadataManager: Topic`.",
            self.tiered_storage_rlmm_topic_backed.clone(),
        );

        registry.register(
            "tiered_storage_rlmm_bootstrap_attempts",
            "Number of topic-backed RLMM bootstrap attempts; climbs while \
             stuck retrying, flat once tiered_storage_rlmm_topic_backed \
             flips to 1.",
            self.tiered_storage_rlmm_bootstrap_attempts.clone(),
        );

        registry.register(
            "produce_message_conversions",
            "Cumulative count of v0/v1 → v2 record-batch \
             up-conversions on the Produce path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.ProduceMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             producers in the cluster.",
            self.produce_message_conversions.clone(),
        );

        registry.register(
            "fetch_message_conversions",
            "Cumulative count of v2 → v0/v1 record-batch \
             down-conversions on the Fetch path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.FetchMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             consumers in the cluster.",
            self.fetch_message_conversions.clone(),
        );

        registry.register(
            "unclean_leader_elections",
            "KIP-841: cumulative count of unclean leader \
             elections driven by this broker (as controller leader). An \
             unclean election is one where the new leader was picked \
             from outside the ISR because the partition's ISR was empty \
             at failover time and the topic had \
             unclean.leader.election.enable=true. Each such election \
             accepts possible data loss. Mirrors Kafka's \
             ControllerStats.UncleanLeaderElectionsPerSec; an operator \
             alert on rate(unclean_leader_elections_total[5m]) > 0 \
             flags the data-loss footgun.",
            self.unclean_leader_elections_total.clone(),
        );

        registry.register(
            "audit_events_total",
            "Cumulative audit records successfully written to the audit topic",
            self.audit_events_total.clone(),
        );
    }
}
