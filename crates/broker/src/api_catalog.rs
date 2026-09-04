//! Public catalog of the Kafka protocol APIs this broker advertises.
//!
//! This is the single source of truth for both the live `ApiVersions`
//! (`api_key` 18) response and the generated protocol-API reference page. The
//! handler in `handlers::api_versions` calls [`supported_apis`]. `crabka-docgen`
//! reads the same list and does not spawn the broker binary.
//!
//! It is also the source of truth for the per-KIP rows of the generated
//! `docs/KIP_MATRIX.md`. [`KIP_ANNOTATIONS`] holds one [`KipAnnotation`] per
//! KIP that any file under `crates/` names. `aspect generate-kip-matrix`
//! parses that table as text, between the `BEGIN KIP_ANNOTATIONS` and
//! `END KIP_ANNOTATIONS` marker comments, and fails when a KIP appears in
//! `crates/` without a row here, when a row names a KIP that no file mentions,
//! or when a row points at a module or test that does not exist. Keep each
//! entry in the literal shape the existing ones use: the generator does not
//! compile Rust.

use krabka_protocol::owned::api_versions_response::ApiVersion;

/// How far the broker takes one KIP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KipStatus {
    /// The KIP's behavior is in the tree and the listed tests establish it.
    Implemented,
    /// Part of the KIP is in the tree. `note` says which part is not.
    Partial,
    /// The KIP is deliberately not implemented. `note` cites the decision.
    OutOfScope,
}

/// Which non-JVM client family a suite drives against the KIP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientEvidence {
    /// No non-JVM client suite exercises the KIP.
    NotCovered,
    /// The stock kcat of `tests/librdkafka_conformance.rs` exercises it.
    Kcat,
}

/// One row of the generated KIP matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KipAnnotation {
    /// `KIP-<n>`, or a non-KIP key for a scope decision that has no KIP.
    pub key: &'static str,
    /// The Kafka behavior the row is about, in a few words.
    pub claim: &'static str,
    /// How far the tree takes it.
    pub status: KipStatus,
    /// The module that owns the behavior, as a path from the repository root.
    pub module: &'static str,
    /// The tests that establish the status, as `path` or `path::function`
    /// from the repository root. A container-driven suite carries its Kafka
    /// image into the matrix through its crate's `BUILD.bazel` `docker` map.
    pub tests: &'static [&'static str],
    /// The non-JVM client suite that also exercises the KIP.
    pub clients: ClientEvidence,
    /// What the status leaves out, or the citation for a scope decision.
    pub note: &'static str,
}

/// The key of the row that records mixed JVM and Krabka controller quorums
/// as out of scope. It is not a KIP, so the generator does not look for it in
/// `crates/`.
pub const MIXED_QUORUM_KEY: &str = "mixed-quorum";

/// The key of the KIP whose forwarding half the same decision rules out. The
/// controller listener serves `Envelope`; nothing in the tree builds one,
/// because a Krabka broker reaches its controller over the krabka-private
/// `SubmitChange` RPC, and a JVM controller is not a peer krabka speaks to.
pub const FORWARDING_KEY: &str = "KIP-590";

/// Where the out-of-scope decision for mixed quorums and JVM-side forwarding
/// is written down. The generator checks that this line still says so.
pub const OUT_OF_SCOPE_CITATION: &str = "crates/raft/src/lib.rs:52";

/// The per-KIP rows of `docs/KIP_MATRIX.md`, in ascending KIP order, with the
/// non-KIP scope rows last.
// BEGIN KIP_ANNOTATIONS
pub const KIP_ANNOTATIONS: &[KipAnnotation] = &[
    KipAnnotation {
        key: "KIP-13",
        claim: "Producer and consumer byte-rate quotas",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/mod.rs",
        tests: &[
            "crates/broker/tests/client_quotas.rs",
            "crates/broker/tests/client_quotas/throttling.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-32",
        claim: "Message timestamps in the v1 message set and the v2 batch",
        status: KipStatus::Implemented,
        module: "crates/records-legacy/src/lib.rs",
        tests: &[
            "crates/log/tests/integration.rs::read_jvm_produced_log_dir",
            "crates/log/tests/integration.rs::jvm_consumes_rust_written_log_dir",
            "crates/broker/tests/legacy_fetch.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-48",
        claim: "Delegation tokens: create, renew, expire, describe and SCRAM token auth",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/create_delegation_token.rs",
        tests: &[
            "crates/broker/tests/delegation_tokens.rs",
            "crates/broker/tests/jvm_acceptance_quotas/delegation_tokens.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-62",
        claim: "Classic group state machine with the AwaitingSync stage",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_state/group.rs",
        tests: &[
            "crates/broker/tests/group_protocol_negotiation.rs",
            "crates/broker/tests/unit/consumer_group.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-73",
        claim: "Replication throttling through the leader and follower throttle keys",
        status: KipStatus::Implemented,
        module: "crates/broker/src/throttle/mod.rs",
        tests: &["crates/broker/tests/throttle.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-98",
        claim: "Transactions and idempotent producers, with transactional-id expiry",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/coordinator.rs",
        tests: &[
            "crates/broker/tests/transactions.rs",
            "crates/broker/tests/transactions/txn_fencing.rs",
            "crates/broker/tests/jvm_streams_app.rs",
            "crates/broker/tests/jvm_connect_distributed.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-101",
        claim: "Leader-epoch based truncation for followers",
        status: KipStatus::Implemented,
        module: "crates/broker/src/replicator/truncation.rs",
        tests: &[
            "crates/broker/tests/leader_epoch.rs",
            "crates/broker/tests/leader_epoch/epoch_diverge_leader.rs",
            "crates/broker/tests/leader_epoch/epoch_fencing.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-107",
        claim: "DeleteRecords admin request",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/delete_records.rs",
        tests: &[
            "crates/broker/tests/client_admin_delete_records.rs",
            "crates/broker/tests/admin_handlers/admin_delete_records.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-108",
        claim: "A create-topic policy refusing a topic with POLICY_VIOLATION",
        status: KipStatus::Implemented,
        module: "crates/broker/src/topic_policy.rs",
        tests: &[
            "crates/broker/src/topic_policy.rs",
            "crates/broker/tests/admin_handlers/admin_topic_policy.rs",
            "crates/broker/tests/topic_freeze/wire.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "The policy is the declared `[topic_policy]` rule set rather than a Java class named by `create.topic.policy.class.name`: a replication-factor floor, a partition ceiling, a `min.insync.replicas` floor, and required / forbidden config values. It runs where Kafka calls `CreateTopicPolicy.validate` — after config validation, before the records are generated — on validate-only requests too. A frozen topic answers with the same error 44.",
    },
    KipAnnotation {
        key: "KIP-110",
        claim: "Zstandard compression in the v2 record batch",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/fetch_downconvert.rs",
        tests: &[
            "crates/broker/tests/recompression.rs",
            "crates/broker/tests/legacy_fetch.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "A zstd batch is re-compressed as snappy for a v0 or v1 fetch, because those formats never carried zstd.",
    },
    KipAnnotation {
        key: "KIP-112",
        claim: "JBOD: a broker with an offline log directory stays registered",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/broker_heartbeat/failover.rs",
        tests: &[
            "crates/broker/tests/jbod_disk_failure.rs",
            "crates/broker/tests/offline_replicas.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-113",
        claim: "AlterReplicaLogDirs and DescribeLogDirs: replica moves between log directories",
        status: KipStatus::Implemented,
        module: "crates/broker/src/future_log.rs",
        tests: &[
            "crates/broker/tests/alter_replica_log_dirs.rs",
            "crates/broker/tests/jbod.rs",
            "crates/broker/tests/jvm_acceptance_quotas/log_dirs.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-124",
        claim: "Request-rate quotas as a percentage of handler time",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/request.rs",
        tests: &[
            "crates/broker/tests/client_quotas/throttling.rs",
            "crates/broker/src/network/dispatch/tests/throttle_mute.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-133",
        claim: "An alter-config policy refusing a topic config change with POLICY_VIOLATION",
        status: KipStatus::Implemented,
        module: "crates/broker/src/topic_policy.rs",
        tests: &[
            "crates/broker/src/handlers/alter_configs/topic_configs.rs",
            "crates/broker/src/handlers/incremental_alter_configs/topic_scope.rs",
            "crates/broker/tests/admin_handlers/admin_topic_policy.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "The same `[topic_policy]` rule set stands in for the class named by `alter.config.policy.class.name`. Both alter paths check the resolved post-change config map, as `AlterConfigPolicy.validate` does; its `RequestMetadata` carries no partition count and no replication factor, so those two rules apply to `CreateTopics` alone.",
    },
    KipAnnotation {
        key: "KIP-207",
        claim: "The high watermark a new leader reports may regress after an election",
        status: KipStatus::Implemented,
        module: "crates/broker/src/data_path_model/model.rs",
        tests: &["crates/broker/src/data_path_model/model.rs"],
        clients: ClientEvidence::NotCovered,
        note: "The exhaustive data-path model checks durability without a watermark monotonicity assertion, which is what the KIP allows.",
    },
    KipAnnotation {
        key: "KIP-211",
        claim: "Committed-offset retention measured from the group's last activity",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/retention.rs",
        tests: &["crates/broker/tests/offsets_retention.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-219",
        claim: "Respond first, then mute the channel for the throttle time",
        status: KipStatus::Partial,
        module: "crates/broker/src/network/dispatch/response.rs",
        tests: &[
            "crates/broker/tests/client_quotas/throttling.rs",
            "crates/broker/src/network/dispatch/throttle_audit.rs::throttle_echo_divergences_are_the_recorded_ones",
        ],
        clients: ClientEvidence::NotCovered,
        note: "The dispatch loop echoes a request-quota delay by patching a leading `ThrottleTimeMs`. The throttle-echo section below lists the APIs whose schema puts the field elsewhere.",
    },
    KipAnnotation {
        key: "KIP-226",
        claim: "DescribeConfigs reports the source of every value",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_configs.rs",
        tests: &["crates/broker/tests/jvm_acceptance_cli/configs.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-227",
        claim: "Incremental fetch sessions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/fetch_session.rs",
        tests: &["crates/broker/tests/fetch_session.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-255",
        claim: "SASL/OAUTHBEARER",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/auth/oauthbearer.rs",
        tests: &["crates/broker/tests/auth_handlers/oauthbearer.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-257",
        claim: "Quota entities keyed by user, client id, or both",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/lookup.rs",
        tests: &[
            "crates/broker/tests/client_quotas.rs",
            "crates/broker/tests/tuple_quota_enforcement.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-290",
        claim: "Prefixed ACL patterns and the MATCH filter",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/acl_wire.rs",
        tests: &["crates/broker/tests/acl_handlers.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-320",
        claim: "Leader epochs in Fetch and ListOffsets, and OffsetForLeaderEpoch for consumers",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_for_leader_epoch.rs",
        tests: &[
            "crates/broker/tests/jvm_kip320_divergence.rs",
            "crates/broker/tests/jvm_kip320_divergence/wire_conformance.rs",
            "crates/broker/tests/consumer_proactive_validation.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-345",
        claim: "Static consumer-group membership",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_state/membership.rs",
        tests: &[
            "crates/broker/tests/static_membership.rs",
            "crates/broker/tests/jvm_acceptance_cli/console_groups.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-360",
        claim: "Epoch bump when a transactional producer re-initialises",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/init_producer_id/transactional.rs",
        tests: &[
            "crates/broker/tests/transactions/txn_fencing.rs::init_producer_id_fences_a_stale_producer_identity",
            "crates/broker/tests/jvm_acceptance_durability/transactional_eos.rs::transactional_console_producer_eos",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-368",
        claim: "SASL re-authentication and session expiry",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/dispatch/session.rs",
        tests: &[
            "crates/broker/tests/auth_handlers/oauthbearer_sessions.rs",
            "crates/broker/tests/auth_handlers/plain.rs::plain_session_capped_by_connections_max_reauth_then_closes",
            "crates/broker/tests/auth_handlers/scram.rs::scram_session_capped_by_connections_max_reauth_then_closes",
            "crates/broker/tests/jvm_acceptance_sasl/scram.rs::jvm_sasl_scram_sha512_in_band_reauth_under_max_reauth_window",
        ],
        clients: ClientEvidence::NotCovered,
        note: "`connections.max.reauth.ms` bounds every mechanism, and PLAIN, SCRAM and GSSAPI all re-authenticate in band under it. GSSAPI carries no automated re-auth case, because the suite has no KDC.",
    },
    KipAnnotation {
        key: "KIP-371",
        claim: "`ssl.principal.mapping.rules` maps an mTLS Subject DN to a principal",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/auth/ssl_principal_mapper.rs",
        tests: &[
            "crates/broker/src/network/auth/ssl_principal_mapper.rs::kafka_documented_rules_map_the_subject_dn",
            "crates/broker/src/file_config/listener.rs::apply_to_listener_parses_principal_mapping_rules",
            "crates/broker/tests/jvm_acceptance_tls/mtls_principal_mapping.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "The rules are per listener, under `[listeners.tls_config]`. Kafka's broker-wide `ssl.principal.mapping.rules` and its `listener.name.<name>.` prefixed form are not read from `server_properties`.",
    },
    KipAnnotation {
        key: "KIP-392",
        claim: "Fetch from the closest replica",
        status: KipStatus::Implemented,
        module: "crates/broker/src/replica_selector.rs",
        tests: &["crates/broker/tests/kip_392_fetch_from_follower.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-394",
        claim: "JoinGroup member-id bootstrap with MEMBER_ID_REQUIRED",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_ops/join.rs",
        tests: &["crates/broker/tests/group_protocol_negotiation.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-405",
        claim: "Tiered storage: remote segment copy, read, retention and metadata",
        status: KipStatus::Implemented,
        module: "crates/broker/src/remote_log_manager.rs",
        tests: &[
            "crates/broker/tests/jvm_acceptance_tiered.rs",
            "crates/broker/tests/tiered_storage_multi_broker.rs",
            "crates/remote-storage/tests/jvm_tiered_storage.rs",
            "crates/restore/tests/roundtrip.rs",
            "crates/restore/tests/roundtrip/consume.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-412",
        claim: "Dynamic broker log levels through the BROKER_LOGGER resource",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/incremental_alter_configs.rs",
        tests: &[
            "crates/broker/tests/jvm_broker_loggers.rs",
            "crates/broker/tests/broker_logger_config.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-429",
        claim: "Cooperative rebalance protocol negotiation in JoinGroup",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_ops/join.rs",
        tests: &[
            "crates/broker/tests/group_protocol_negotiation.rs",
            "crates/broker/tests/jvm_acceptance_cli/console_groups.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-430",
        claim: "Authorized-operations bitfields in Metadata, DescribeGroups and DescribeCluster",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/authorized_operations.rs",
        tests: &["crates/broker/tests/authorized_operations.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-447",
        claim: "OffsetFetch require_stable and the UNSTABLE_OFFSET_COMMIT answer",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_fetch/unstable.rs",
        tests: &[
            "crates/broker/tests/txn_offset_commit_materialize.rs",
            "crates/broker/src/handlers/offset_fetch/tests.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-455",
        claim: "AlterPartitionReassignments and ListPartitionReassignments",
        status: KipStatus::Implemented,
        module: "crates/broker/src/reassignment.rs",
        tests: &[
            "crates/broker/tests/partition_reassignment.rs",
            "crates/broker/tests/jvm_acceptance_reassign.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-460",
        claim: "ElectLeaders with unclean election, and automatic preferred-leader rebalance",
        status: KipStatus::Implemented,
        module: "crates/broker/src/leader_rebalance.rs",
        tests: &[
            "crates/broker/tests/elect_leaders.rs",
            "crates/broker/tests/elect_leaders/auto_rebalance.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-467",
        claim: "Per-record error indices and messages in the Produce response",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/produce/schema.rs",
        tests: &["crates/broker/tests/schema_validation/rejected.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-482",
        claim: "Flexible versions and tagged fields, with the v2 request header",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/dispatch.rs",
        tests: &[
            "crates/broker/src/network/dispatch/tests.rs",
            "crates/broker/tests/librdkafka_conformance.rs::round_trip_group_join_and_api_versions_with_kcat",
        ],
        clients: ClientEvidence::Kcat,
        note: "",
    },
    KipAnnotation {
        key: "KIP-496",
        claim: "OffsetDelete for consumer groups",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_delete.rs",
        tests: &[
            "crates/broker/tests/offset_delete.rs",
            "crates/broker/tests/jvm_acceptance_cli/consumer_groups.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-500",
        claim: "KRaft mode: broker heartbeats, fencing and controlled shutdown",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/broker_heartbeat.rs",
        tests: &[
            "crates/broker/tests/controlled_shutdown.rs",
            "crates/broker/tests/advertised_controller_liveness.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-511",
        claim: "Client software name and version in ApiVersions v3",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/api_versions/client_info.rs",
        tests: &[
            "crates/broker/tests/client_software_versions.rs",
            "crates/broker/tests/librdkafka_conformance.rs::round_trip_group_join_and_api_versions_with_kcat",
        ],
        clients: ClientEvidence::Kcat,
        note: "",
    },
    KipAnnotation {
        key: "KIP-516",
        claim: "Topic identifiers in Metadata, Produce, Fetch, offsets and DeleteTopics",
        status: KipStatus::Implemented,
        module: "crates/broker/src/topic_resolve.rs",
        tests: &[
            "crates/broker/tests/kip516_metadata.rs",
            "crates/broker/tests/kip516_produce.rs",
            "crates/broker/tests/kip516_fetch.rs",
            "crates/broker/tests/kip516_offsets.rs",
            "crates/broker/tests/kip516_delete_topics.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-525",
        claim: "CreateTopics v5 returns the created topic's configuration",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/create_topics.rs",
        tests: &["crates/broker/tests/admin_handlers.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-534",
        claim: "Compaction retains the last tombstone and transaction marker for a delay",
        status: KipStatus::Implemented,
        module: "crates/log/src/compact.rs",
        tests: &[
            "crates/log/src/compact/retention_fuzz.rs",
            "crates/log/src/compact_model/pass.rs",
            "crates/broker/tests/compaction.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-546",
        claim: "DescribeClientQuotas and AlterClientQuotas",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_client_quotas.rs",
        tests: &[
            "crates/broker/tests/client_quotas.rs",
            "crates/broker/tests/jvm_acceptance_quotas/client_quotas.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-554",
        claim: "SCRAM credentials through AlterUserScramCredentials and DescribeUserScramCredentials",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/alter_user_scram_credentials.rs",
        tests: &[
            "crates/broker/tests/describe_user_scram_credentials.rs",
            "crates/broker/tests/auth_handlers/alter_scram.rs",
            "crates/broker/tests/jvm_acceptance_sasl/scram.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-559",
        claim: "protocol_type and protocol_name in JoinGroup v7 and SyncGroup v5",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/sync_group.rs",
        tests: &["crates/broker/tests/kip559_l7_proxy_fields.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-584",
        claim: "Feature versioning: UpdateFeatures and the finalized features in ApiVersions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/update_features.rs",
        tests: &[
            "crates/broker/tests/feature_finalization.rs",
            "crates/broker/tests/api_versions_features.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "The finalizable features are `metadata.version`, `group.version`, `transaction.version`, `share.version`, `streams.version`, `eligible.leader.replicas.version` and `kraft.version`, the last finalized by a KRaft control record rather than by `UpdateFeatures`.",
    },
    KipAnnotation {
        key: "KIP-590",
        claim: "Envelope: the controller listener serves a forwarded admin write",
        status: KipStatus::Implemented,
        module: "crates/broker/src/envelope.rs",
        tests: &[
            "crates/broker/tests/kip590_envelope.rs",
            "crates/broker/tests/jvm_role_separated_admin.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "The broker side of KIP-590 is not needed: a Krabka broker reaches its controller over the krabka-private `SubmitChange` RPC (`crates/broker/src/metadata_source/observer_source.rs`), and a JVM controller is outside the compatibility target (crates/raft/src/lib.rs:52).",
    },
    KipAnnotation {
        key: "KIP-595",
        claim: "The KRaft controller quorum: Vote, BeginQuorumEpoch, EndQuorumEpoch, Fetch and DescribeQuorum",
        status: KipStatus::Implemented,
        module: "crates/kraft-core/src/core.rs",
        tests: &[
            "crates/raft/tests/kraft_sim.rs",
            "crates/raft/tests/kraft_engine_sim.rs",
            "crates/broker/tests/jvm_static_quorum_spike.rs",
            "crates/broker/tests/admin_handlers/admin_describe_quorum.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-599",
        claim: "Controller mutation quotas on CreateTopics, CreatePartitions and DeleteTopics",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/controller_mutation.rs",
        tests: &["crates/broker/tests/controller_mutation_quota.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-612",
        claim: "Per-IP connection creation rate quotas",
        status: KipStatus::Implemented,
        module: "crates/broker/src/broker/accept.rs",
        tests: &["crates/broker/tests/ip_quotas.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-630",
        claim: "Metadata snapshots: the checkpoint file and FetchSnapshot",
        status: KipStatus::Implemented,
        module: "crates/raft/src/snapshot.rs",
        tests: &[
            "crates/raft/tests/snapshot.rs",
            "crates/raft/tests/kraft_checkpoint_jvm.rs",
            "crates/broker/tests/fetch_snapshot.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-631",
        claim: "The KRaft metadata records and broker registration",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/broker_registration.rs",
        tests: &[
            "crates/raft/tests/kraft_checkpoint_jvm.rs",
            "crates/broker/tests/unregister_broker.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-642",
        claim: "Multi-node quorum reassignment in one operation",
        status: KipStatus::OutOfScope,
        module: "crates/raft/src/controller/membership.rs",
        tests: &[],
        clients: ClientEvidence::NotCovered,
        note: "Voter changes go one node at a time through KIP-853. `change_membership` rejects a batch that adds or removes more than one voter.",
    },
    KipAnnotation {
        key: "KIP-664",
        claim: "DescribeProducers, DescribeTransactions and ListTransactions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_producers.rs",
        tests: &[
            "crates/broker/tests/describe_producers.rs",
            "crates/broker/tests/list_describe_transactions.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-714",
        claim: "Client metrics push: GetTelemetrySubscriptions and PushTelemetry",
        status: KipStatus::Implemented,
        module: "crates/broker/src/client_metrics/mod.rs",
        tests: &[
            "crates/broker/tests/client_telemetry.rs",
            "crates/broker/tests/client_metrics_config.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-734",
        claim: "ListOffsets MAX_TIMESTAMP",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/timestamp.rs",
        tests: &["crates/broker/tests/list_offsets_isolation/timestamp_sentinels.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-778",
        claim: "metadata.version as a finalized feature that `krabka format` bootstraps",
        status: KipStatus::Implemented,
        module: "crates/format/src/format/features.rs",
        tests: &[
            "crates/format/tests/format_smoke.rs",
            "crates/broker/tests/format_features.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-827",
        claim: "DescribeLogDirs v4 reports total and usable bytes per directory",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_log_dirs/dirs.rs",
        tests: &["crates/broker/tests/jvm_acceptance_quotas/log_dirs.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-841",
        claim: "Unclean leader election when the ISR is empty and the topic allows it",
        status: KipStatus::Implemented,
        module: "crates/broker/src/leader_election.rs",
        tests: &[
            "crates/broker/tests/leader_election.rs",
            "crates/broker/src/leader_election/scan/dead_broker_tests.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-848",
        claim: "The next-generation consumer group protocol",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/consumer_state.rs",
        tests: &[
            "crates/broker/tests/consumer_group_next_gen.rs",
            "crates/broker/tests/jvm_consumer_group_next_gen.rs",
            "crates/broker/tests/group_version.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "A `ConsumerGroupHeartbeat` whose `SubscribedTopicRegex` does not compile is answered `INVALID_REGULAR_EXPRESSION` (128) before any member record is written, and the member is not admitted, as Kafka does. The pattern is compiled with Rust `regex` in Unicode mode, which accepts RE2J's Unicode character classes; topic names are ASCII, so RE2J's ASCII-only perl classes cannot diverge on a match. An inline flag group naming a flag RE2J has no equivalent for (`x`, `u`, `R`) is rejected ahead of the compile with RE2J's own message, since `regex` would take it. Two residues remain, both documented on `check_subscribed_topic_regex`: `regex` character-class set operations are accepted where RE2J would not, and RE2's literal-quoting escape pair, which `regex` has no equivalent for, is rejected where RE2J would accept. Neither can change which topics an accepted subscription matches. No JVM-lane case covers the refusal: `KafkaConsumer.subscribe(Pattern)` and `kafka-console-consumer --include` compile the pattern locally with `java.util.regex`, so a stock JVM client never sends an invalid one to the broker.",
    },
    KipAnnotation {
        key: "KIP-853",
        claim: "Dynamic controller quorum: AddRaftVoter, RemoveRaftVoter, UpdateRaftVoter and auto-join",
        status: KipStatus::Implemented,
        module: "crates/raft/src/kraft/controller/reconfiguration.rs",
        tests: &[
            "crates/broker/tests/dynamic_voters.rs",
            "crates/raft/tests/reconfig.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-858",
        claim: "Directory identifiers: AssignReplicasToDirs and directory ids in heartbeats",
        status: KipStatus::Implemented,
        module: "crates/broker/src/assign_dirs.rs",
        tests: &[
            "crates/broker/tests/jbod_disk_failure.rs",
            "crates/broker/tests/offline_replicas.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-890",
        claim: "Transactions v2: verify-only AddPartitionsToTxn and epoch bumps on completion",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/version.rs",
        tests: &[
            "crates/broker/tests/transaction_version.rs",
            "crates/broker/tests/transaction_version/txnver_verify_only.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-903",
        claim: "Broker epochs fence stale replicas in AlterPartition",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/alter_partition/isr_update.rs",
        tests: &[
            "crates/raft/src/kraft/controller/tests_broker_registration.rs",
            "crates/broker/src/elr/tests.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-919",
        claim: "Admin clients bootstrap from controllers: ControllerRegistration, DescribeCluster and UnregisterBroker",
        status: KipStatus::Implemented,
        module: "crates/broker/src/controller_admin.rs",
        tests: &[
            "crates/broker/tests/jvm_bootstrap_controller.rs",
            "crates/broker/tests/client_admin_controller_bootstrap.rs",
            "crates/broker/tests/unregister_broker.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-932",
        claim: "Share groups: membership, ShareFetch, ShareAcknowledge, the share coordinator and admin offsets",
        status: KipStatus::Implemented,
        module: "crates/broker/src/share_partition/mod.rs",
        tests: &[
            "crates/broker/tests/share_groups.rs",
            "crates/broker/tests/share_consume.rs",
            "crates/broker/tests/share_admin_offsets.rs",
            "crates/broker/tests/jvm_share_groups.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-939",
        claim: "Two-phase commit transactions with the KIP-939 timeout rules",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/two_pc.rs",
        tests: &[
            "crates/broker/tests/transactions_2pc.rs",
            "crates/broker/src/txn/two_pc_model.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-950",
        claim: "Tiered storage disablement: remote.log.copy.disable and remote.log.delete.on.disable",
        status: KipStatus::Implemented,
        module: "crates/broker/src/remote_log_manager.rs",
        tests: &[
            "crates/broker/src/config_keys/validation/tests.rs",
            "crates/broker/tests/jvm_acceptance_tiered.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "`remote.storage.enable` going true -> false is refused unless `remote.log.delete.on.disable=true` comes with it, and the flip then erases the partition's remote segments and raises its log start offset to the local log start. `remote.log.copy.disable=true` is the read-only tier: no new copies, reads and remote retention unchanged. Under a WORM archive the cascade clears the partition's remote metadata and removes nothing from the archive, as a `DeleteTopics` cascade does.",
    },
    KipAnnotation {
        key: "KIP-951",
        claim: "Leader hints in the Produce and Fetch responses",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/produce/pipeline.rs",
        tests: &[
            "crates/broker/tests/produce_leader_gate.rs",
            "crates/broker/tests/producer_leader_routing.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-966",
        claim: "Eligible leader replicas, unclean recovery and DescribeTopicPartitions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/elr.rs",
        tests: &[
            "crates/broker/tests/unclean_recovery.rs",
            "crates/broker/tests/describe_topic_partitions.rs",
            "crates/broker/tests/jvm_acceptance_cli/elr_columns.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "ELR maintenance is gated on the `eligible.leader.replicas.version` feature, as Kafka gates it on `FeatureControlManager.isElrFeatureEnabled()`: at level 0 the controller publishes no eligible or last-known-eligible set, and a downgrade to 0 clears what an earlier level 1 published. The release default is 0 at every `metadata.version` krabka advertises, because `ELRV_1` bootstraps at 4.1-IV0; level 1 declares Kafka's KIP-1022 dependency on `metadata.version` at 4.0-IV1. krabka carries the state as the controller-managed `krabka.elr` topic override rather than in `PartitionRecord`, so it does not consume ELR fields written by a JVM controller.",
    },
    KipAnnotation {
        key: "KIP-996",
        claim: "Pre-vote before a KRaft election",
        status: KipStatus::Implemented,
        module: "crates/kraft-core/src/core/election.rs",
        tests: &[
            "crates/kraft-core/src/core/election/tests.rs",
            "crates/raft/tests/kraft_engine_sim/failover.rs",
            "crates/broker/tests/jvm_static_quorum_spike/contested_election.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1005",
        claim: "ListOffsets LATEST_TIERED_TIMESTAMP",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/sentinels.rs",
        tests: &["crates/broker/tests/list_offsets_isolation/timestamp_sentinels.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1022",
        claim: "Feature flags with dependency checks at format and upgrade time",
        status: KipStatus::Implemented,
        module: "crates/format/src/format/features.rs",
        tests: &[
            "crates/broker/tests/format_features.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1023",
        claim: "ListOffsets EARLIEST_PENDING_UPLOAD_TIMESTAMP",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/resolve.rs",
        tests: &["crates/broker/tests/list_offsets_isolation/timestamp_sentinels.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1071",
        claim: "Streams groups: StreamsGroupHeartbeat and StreamsGroupDescribe",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/streams/mod.rs",
        tests: &[
            "crates/broker/tests/streams_groups.rs",
            "crates/broker/tests/streams_classic_upgrade.rs",
            "crates/broker/tests/jvm_streams_groups.rs",
            "crates/broker/tests/jvm_streams_app.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1073",
        claim: "DescribeCluster hides fenced brokers from clients",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_cluster.rs",
        tests: &["crates/broker/tests/role_separation_observer.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1075",
        claim: "A server-side timeout for remote ListOffsets work",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/remote.rs",
        tests: &["crates/broker/src/config_keys/registry/tests.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1142",
        claim: "ListConfigResources",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_config_resources.rs",
        tests: &["crates/broker/tests/admin_handlers/admin_listings.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1155",
        claim: "A checkpoint at every metadata.version downgrade",
        status: KipStatus::Implemented,
        module: "crates/raft/src/kraft/controller/snapshotting.rs",
        tests: &[
            "crates/raft/src/kraft/controller/tests_downgrade.rs",
            "crates/broker/tests/jvm_kip320_divergence/metadata_version_downgrade.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1186",
        claim: "kraft.version upgrade and the last-voter check that the Kafka 4.3 quorum tools drive",
        status: KipStatus::Implemented,
        module: "crates/raft/src/server/voter_admin.rs",
        tests: &["crates/broker/tests/jvm_features.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1242",
        claim: "ApiVersions v5 routing identity and REBOOTSTRAP_REQUIRED",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/api_versions.rs",
        tests: &["crates/broker/src/handlers/api_versions/tests.rs"],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "KIP-1319",
        claim: "Transactions v2 producer-id rotation and verification on Produce",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/coordinator/pid_index.rs",
        tests: &[
            "crates/broker/tests/transaction_version.rs",
            "crates/broker/tests/transactions.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "",
    },
    KipAnnotation {
        key: "SASL/GSSAPI",
        claim: "SASL/Kerberos authentication",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/auth/gssapi.rs",
        tests: &[
            "crates/broker/tests/gssapi_e2e.rs",
            "crates/broker/tests/auth_handlers/gssapi.rs",
        ],
        clients: ClientEvidence::NotCovered,
        note: "Kerberos predates the KIP process (KAFKA-1686, Kafka 0.9), so no KIP number. Both suites run in the scheduled `container gssapi` CI job: the KDC fixture writes keytabs through a bind mount, so the lane is schedule and workflow_dispatch only.",
    },
    KipAnnotation {
        key: "mixed-quorum",
        claim: "A controller quorum with both JVM and Krabka voters",
        status: KipStatus::OutOfScope,
        module: "crates/raft/src/lib.rs",
        tests: &[],
        clients: ClientEvidence::NotCovered,
        note: "Outside the raft crate's compatibility target: crates/raft/src/lib.rs:52.",
    },
];
// END KIP_ANNOTATIONS

macro_rules! v {
    ($mod:ident) => {
        ApiVersion {
            api_key: krabka_protocol::owned::$mod::API_KEY,
            min_version: krabka_protocol::owned::$mod::MIN_VERSION,
            max_version: krabka_protocol::owned::$mod::MAX_VERSION,
            ..Default::default()
        }
    };
}

/// The full advertised API set, mirrored from each API's generated
/// `MIN_VERSION` and `MAX_VERSION` constants. Update it when you add a handler.
#[must_use]
pub fn supported_apis() -> Vec<ApiVersion> {
    let mut apis = client_facing_apis();
    apis.extend(admin_apis());
    apis
}

fn client_facing_apis() -> Vec<ApiVersion> {
    use krabka_protocol::owned;
    vec![
        v!(api_versions_request),
        ApiVersion {
            api_key: owned::produce_request::API_KEY,
            min_version: krabka_protocol::kafka_3_6_2::owned::produce_request::MIN_VERSION,
            max_version: owned::produce_request::MAX_VERSION,
            ..Default::default()
        },
        ApiVersion {
            api_key: owned::fetch_request::API_KEY,
            min_version: krabka_protocol::kafka_3_6_2::owned::fetch_request::MIN_VERSION,
            max_version: owned::fetch_request::MAX_VERSION,
            ..Default::default()
        },
        ApiVersion {
            api_key: owned::list_offsets_request::API_KEY,
            min_version: 0,
            max_version: owned::list_offsets_request::MAX_VERSION,
            ..Default::default()
        },
        v!(metadata_request),
        v!(find_coordinator_request),
        v!(join_group_request),
        v!(sync_group_request),
        v!(heartbeat_request),
        v!(leave_group_request),
        v!(sasl_handshake_request),
        v!(sasl_authenticate_request),
        v!(offset_commit_request),
        v!(offset_fetch_request),
    ]
}

fn admin_apis() -> Vec<ApiVersion> {
    vec![
        v!(create_topics_request),
        v!(delete_topics_request),
        v!(delete_records_request),
        v!(init_producer_id_request),
        // AllocateProducerIds is the controller-backed broker RPC used to
        // reserve durable, cluster-wide producer-ID blocks.
        v!(allocate_producer_ids_request),
        v!(offset_for_leader_epoch_request),
        v!(add_partitions_to_txn_request),
        v!(add_offsets_to_txn_request),
        v!(end_txn_request),
        v!(write_txn_markers_request),
        v!(txn_offset_commit_request),
        v!(describe_configs_request),
        v!(alter_replica_log_dirs_request),
        v!(describe_log_dirs_request),
        v!(describe_groups_request),
        v!(list_groups_request),
        v!(alter_configs_request),
        v!(create_partitions_request),
        v!(delete_groups_request),
        v!(incremental_alter_configs_request),
        v!(alter_partition_request),
        v!(assign_replicas_to_dirs_request),
        v!(describe_cluster_request),
        v!(broker_heartbeat_request),
        v!(broker_registration_request),
        v!(controller_registration_request),
        // UnregisterBroker (KIP-919) — admin RPC to permanently drop a
        // broker registration from the cluster's metadata image.
        v!(unregister_broker_request),
        v!(alter_user_scram_credentials_request),
        // UpdateFeatures (api_key 57, KIP-584) — `kafka-features` admin tool
        // finalizes broker-supported features through a Raft-persisted path.
        v!(update_features_request),
        v!(describe_acls_request),
        v!(create_acls_request),
        v!(delete_acls_request),
        v!(elect_leaders_request),
        v!(alter_partition_reassignments_request),
        v!(list_partition_reassignments_request),
        // OffsetDelete (api_key 47, KIP-496): completes
        // `kafka-consumer-groups --delete-offsets` parity.
        v!(offset_delete_request),
        v!(describe_client_quotas_request),
        v!(alter_client_quotas_request),
        v!(describe_user_scram_credentials_request),
        // KIP-48: delegation-token RPCs. Conditional on the
        // broker having a master key configured is tempting, but Kafka
        // always advertises these — clients discover support at this
        // level then get DELEGATION_TOKEN_AUTH_DISABLED (61) on the
        // actual call when the broker isn't configured for tokens.
        v!(create_delegation_token_request),
        v!(renew_delegation_token_request),
        v!(expire_delegation_token_request),
        v!(describe_delegation_token_request),
        // DescribeProducers (KIP-664) — admin introspection of
        // per-(topic, partition) idempotent / transactional producer state.
        v!(describe_producers_request),
        // DescribeTransactions + ListTransactions (KIP-664) — admin
        // introspection of the TxnCoordinator's local state map.
        v!(describe_transactions_request),
        v!(list_transactions_request),
        // DescribeTopicPartitions (KIP-966) — paginated topic listing
        // used by JVM admin clients 3.7+ in place of fanned-out Metadata
        // calls for `kafka-topics --describe`.
        v!(describe_topic_partitions_request),
        // KIP-714 client-metrics push handshake. Krabka exposes its own
        // broker-side observability — these handlers return "no metrics
        // subscribed" so clients skip the push entirely. Advertising is
        // still important: clients query `ApiVersions` to learn the
        // broker supports the API at all, and absence flips them into
        // legacy-fallback paths we don't need.
        v!(get_telemetry_subscriptions_request),
        v!(push_telemetry_request),
        // ListConfigResources (KIP-1142) — typed enumeration of every
        // configurable resource (topics + brokers + client_metrics). v0
        // is the legacy ListClientMetricsResources surface (KIP-714); v1
        // adds the `resource_types` filter.
        v!(list_config_resources_request),
        // DescribeQuorum (KIP-595) — `kafka-metadata-quorum --describe`
        // admin introspection of the controller-raft quorum.
        v!(describe_quorum_request),
        // FetchSnapshot (KIP-630) — controller-snapshot byte-range fetch
        // used by replicas catching up via __cluster_metadata snapshots.
        v!(fetch_snapshot_request),
        // KIP-848 next-gen consumer group protocol.
        v!(consumer_group_heartbeat_request),
        v!(consumer_group_describe_request),
        // KIP-932 share-group membership protocol.
        v!(share_group_heartbeat_request),
        v!(share_group_describe_request),
        // KIP-1071 streams-group rebalance protocol.
        v!(streams_group_heartbeat_request),
        v!(streams_group_describe_request),
        // KIP-932 ShareFetch / ShareAcknowledge data-plane RPCs.
        v!(share_fetch_request),
        v!(share_acknowledge_request),
        // KIP-932 share-group admin offset RPCs.
        v!(describe_share_group_offsets_request),
        v!(alter_share_group_offsets_request),
        v!(delete_share_group_offsets_request),
        // KIP-932 share-coordinator (persister) RPCs.
        v!(initialize_share_group_state_request),
        v!(read_share_group_state_request),
        v!(write_share_group_state_request),
        v!(delete_share_group_state_request),
        v!(read_share_group_state_summary_request),
        // GetReplicaLogInfo (KIP-966) — inter-broker RPC the controller's
        // unclean recovery manager uses to read each replica's LEO + leader
        // epoch. Advertised so InterBrokerClient version negotiation succeeds.
        v!(get_replica_log_info_request),
        // KIP-853 dynamic-quorum reconfiguration — `kafka-metadata-quorum
        // --add-controller / --remove-controller` and the controller
        // auto-join path.
        v!(add_raft_voter_request),
        v!(remove_raft_voter_request),
        v!(update_raft_voter_request),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;

    #[test]
    fn share_group_apis_are_advertised() {
        let apis = supported_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        assert!(keys.contains(&76));
        assert!(keys.contains(&77));
        let hb = apis.iter().find(|a| a.api_key == 76).unwrap();
        assert!(hb.min_version == 1 && hb.max_version == 1);
    }

    #[test]
    fn streams_group_apis_are_advertised() {
        let apis = supported_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        // StreamsGroupHeartbeat(88), StreamsGroupDescribe(89).
        assert!(keys.contains(&88));
        assert!(keys.contains(&89));
        let hb = apis.iter().find(|a| a.api_key == 88).unwrap();
        assert!(hb.min_version == 0 && hb.max_version == 0);
    }

    #[test]
    fn share_coordinator_persister_apis_are_advertised() {
        let apis = supported_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        // InitializeShareGroupState(83), ReadShareGroupState(84),
        // WriteShareGroupState(85), DeleteShareGroupState(86),
        // ReadShareGroupStateSummary(87).
        for k in [83, 84, 85, 86, 87] {
            assert!(
                keys.contains(&k),
                "persister api_key {k} must be advertised"
            );
        }
    }

    #[test]
    fn supported_apis_is_nonempty_and_sane() {
        let apis = supported_apis();
        assert!(!apis.is_empty(), "advertised API table must not be empty");
        // ApiVersions itself (api_key 18) is always advertised.
        assert!(apis.iter().any(|a| a.api_key == 18));
        for a in &apis {
            assert!(a.min_version <= a.max_version, "api {} min>max", a.api_key);
        }
    }

    /// The KIP number of a `KIP-<n>` key, or `None` for a scope-only key.
    fn kip_number(key: &str) -> Option<u32> {
        key.strip_prefix("KIP-")?.parse().ok()
    }

    #[test]
    fn kip_annotation_keys_are_unique_and_ordered() {
        let keys: Vec<&str> = KIP_ANNOTATIONS.iter().map(|row| row.key).collect();
        let unique: BTreeSet<&str> = keys.iter().copied().collect();
        assert!(unique.len() == keys.len(), "duplicate keys in {keys:?}");

        // KIP rows first, ascending; the scope-only rows after them.
        let numbers: Vec<u32> = keys.iter().filter_map(|key| kip_number(key)).collect();
        let mut sorted = numbers.clone();
        sorted.sort_unstable();
        assert!(numbers == sorted, "KIP rows are not in ascending order");
        let first_scope_row = keys.iter().position(|key| kip_number(key).is_none());
        if let Some(index) = first_scope_row {
            assert!(
                keys[index..].iter().all(|key| kip_number(key).is_none()),
                "a KIP row follows a scope-only row in {keys:?}"
            );
        }
        assert!(keys.contains(&MIXED_QUORUM_KEY));
    }

    /// Whether `path` is a Rust source file named from the repository root.
    fn is_rust_source_path(path: &str) -> bool {
        path.starts_with("crates/")
            && std::path::Path::new(path).extension() == Some(std::ffi::OsStr::new("rs"))
    }

    #[test]
    fn kip_annotation_rows_are_complete() {
        for row in KIP_ANNOTATIONS {
            assert!(!row.claim.is_empty(), "{} has no claim", row.key);
            assert!(
                is_rust_source_path(row.module),
                "{} owner {} is not a Rust source path from the repository root",
                row.key,
                row.module
            );
            for test in row.tests {
                let path = test.split_once("::").map_or(*test, |(path, _)| path);
                assert!(
                    is_rust_source_path(path),
                    "{} test {test} is not a Rust source path from the repository root",
                    row.key
                );
            }
            match row.status {
                KipStatus::Implemented => {
                    assert!(
                        !row.tests.is_empty(),
                        "{} is Implemented without a test",
                        row.key
                    );
                }
                KipStatus::Partial => {
                    assert!(
                        !row.tests.is_empty(),
                        "{} is Partial without a test",
                        row.key
                    );
                    assert!(
                        !row.note.is_empty(),
                        "{} is Partial without a note",
                        row.key
                    );
                }
                KipStatus::OutOfScope => {
                    assert!(row.tests.is_empty(), "{} is OutOfScope with tests", row.key);
                    assert!(
                        !row.note.is_empty(),
                        "{} is OutOfScope without a note",
                        row.key
                    );
                }
            }
        }
    }

    /// Both rows that rest on the mixed-quorum decision cite it, and each
    /// carries the status that decision leaves it with: the mixed-quorum row
    /// is out of scope, and KIP-590 is served-only -- the controller listener
    /// answers `Envelope`, and a broker-only node needs no forwarding path of
    /// its own because it writes over the krabka-private `SubmitChange` RPC.
    #[test]
    fn mixed_quorum_and_forwarding_rows_cite_the_raft_decision() {
        for key in [FORWARDING_KEY, MIXED_QUORUM_KEY] {
            let row = KIP_ANNOTATIONS
                .iter()
                .find(|row| row.key == key)
                .unwrap_or_else(|| panic!("{key} has no annotation"));
            assert!(
                row.note.contains(OUT_OF_SCOPE_CITATION),
                "{key} does not cite {OUT_OF_SCOPE_CITATION}"
            );
        }
        let mixed = KIP_ANNOTATIONS
            .iter()
            .find(|row| row.key == MIXED_QUORUM_KEY)
            .expect("mixed-quorum row");
        assert!(mixed.status == KipStatus::OutOfScope);

        let forwarding = KIP_ANNOTATIONS
            .iter()
            .find(|row| row.key == FORWARDING_KEY)
            .expect("KIP-590 row");
        assert!(forwarding.status == KipStatus::Implemented);
    }

    /// Every module and test path an annotation names is a file in the tree.
    ///
    /// Cargo exports `CARGO_MANIFEST_DIR`, from which the repository root is
    /// two levels up. Bazel stages no source tree for a unit test, so there
    /// the check has nothing to look at and returns; `aspect generate-kip-matrix`
    /// makes the same check in CI's docs job, against the checked-out tree,
    /// and also checks that a `path::function` entry names a function the
    /// file defines.
    #[test]
    fn kip_annotation_paths_exist() {
        // A Bazel sandbox stages only this crate's sources, so the paths the
        // rows cite in other crates are absent there even though the
        // directory exists; `TEST_SRCDIR` is how Bazel announces itself. The
        // generator in the docs CI job is the hermetic gate for these paths.
        if std::env::var_os("TEST_SRCDIR").is_some() {
            return;
        }
        let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") else {
            return;
        };
        let root = std::path::Path::new(&manifest_dir).join("../..");
        if !root.join("crates").is_dir() {
            return;
        }
        for row in KIP_ANNOTATIONS {
            assert!(
                root.join(row.module).is_file(),
                "{} owner {} is missing",
                row.key,
                row.module
            );
            for test in row.tests {
                let path = test.split_once("::").map_or(*test, |(path, _)| path);
                assert!(
                    root.join(path).is_file(),
                    "{} test {path} is missing",
                    row.key
                );
            }
        }
        let (path, _) = OUT_OF_SCOPE_CITATION.split_once(':').expect("path:line");
        assert!(
            root.join(path).is_file(),
            "{OUT_OF_SCOPE_CITATION} is missing"
        );
    }
}
