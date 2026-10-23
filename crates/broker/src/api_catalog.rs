//! Public catalog of the Kafka protocol APIs this broker advertises.
//!
//! This is the single source of truth for both the live `ApiVersions`
//! (`api_key` 18) response and the generated protocol-API reference page. The
//! handler in `handlers::api_versions` calls [`supported_apis`] with the kind
//! of listener the request arrived on, the broker's KIP-714 setting and its
//! [`VersionGates`], so a listener a client reaches advertises what a Kafka
//! broker advertises. Under the default gates that is exactly Kafka 4.3.1's
//! table: [`CatalogApi::released`], from [`krabka_raft::KAFKA_4_3_1_APIS`], is
//! the one per-API record of what that release serves, and both the
//! advertised range and the receive-side refusal ([`is_disabled_version`])
//! are derived from it. The dispatch registry calls [`dispatched_apis`] instead, because what the broker
//! serves is wider than what any one listener names. `krabka-docgen` reads the
//! same list and does not spawn the broker binary.

pub use krabka_raft::UnstableApiVersions;
use {
    crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
    krabka_protocol::{
        ProtocolRequest,
        owned::{
            add_offsets_to_txn_request::AddOffsetsToTxnRequest,
            add_partitions_to_txn_request::AddPartitionsToTxnRequest,
            add_raft_voter_request::AddRaftVoterRequest,
            allocate_producer_ids_request::AllocateProducerIdsRequest,
            alter_client_quotas_request::AlterClientQuotasRequest,
            alter_configs_request::AlterConfigsRequest,
            alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
            alter_partition_request::AlterPartitionRequest,
            alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
            alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersion,
            assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
            broker_heartbeat_request::BrokerHeartbeatRequest,
            broker_registration_request::BrokerRegistrationRequest,
            consumer_group_describe_request::ConsumerGroupDescribeRequest,
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            controller_registration_request::ControllerRegistrationRequest,
            create_acls_request::CreateAclsRequest,
            create_delegation_token_request::CreateDelegationTokenRequest,
            create_partitions_request::CreatePartitionsRequest,
            create_topics_request::CreateTopicsRequest, delete_acls_request::DeleteAclsRequest,
            delete_groups_request::DeleteGroupsRequest,
            delete_records_request::DeleteRecordsRequest,
            delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
            delete_share_group_state_request::DeleteShareGroupStateRequest,
            delete_topics_request::DeleteTopicsRequest, describe_acls_request::DescribeAclsRequest,
            describe_client_quotas_request::DescribeClientQuotasRequest,
            describe_cluster_request::DescribeClusterRequest,
            describe_configs_request::DescribeConfigsRequest,
            describe_delegation_token_request::DescribeDelegationTokenRequest,
            describe_groups_request::DescribeGroupsRequest,
            describe_log_dirs_request::DescribeLogDirsRequest,
            describe_producers_request::DescribeProducersRequest,
            describe_quorum_request::DescribeQuorumRequest,
            describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest,
            describe_topic_partitions_request::DescribeTopicPartitionsRequest,
            describe_transactions_request::DescribeTransactionsRequest,
            describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest,
            elect_leaders_request::ElectLeadersRequest, end_txn_request::EndTxnRequest,
            expire_delegation_token_request::ExpireDelegationTokenRequest,
            fetch_snapshot_request::FetchSnapshotRequest,
            find_coordinator_request::FindCoordinatorRequest,
            get_replica_log_info_request::GetReplicaLogInfoRequest,
            get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
            heartbeat_request::HeartbeatRequest,
            incremental_alter_configs_request::IncrementalAlterConfigsRequest,
            init_producer_id_request::InitProducerIdRequest,
            initialize_share_group_state_request::InitializeShareGroupStateRequest,
            join_group_request::JoinGroupRequest, leave_group_request::LeaveGroupRequest,
            list_config_resources_request::ListConfigResourcesRequest,
            list_groups_request::ListGroupsRequest,
            list_partition_reassignments_request::ListPartitionReassignmentsRequest,
            list_transactions_request::ListTransactionsRequest, metadata_request::MetadataRequest,
            offset_commit_request::OffsetCommitRequest, offset_delete_request::OffsetDeleteRequest,
            offset_fetch_request::OffsetFetchRequest,
            offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
            push_telemetry_request::PushTelemetryRequest,
            read_share_group_state_request::ReadShareGroupStateRequest,
            read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest,
            remove_raft_voter_request::RemoveRaftVoterRequest,
            renew_delegation_token_request::RenewDelegationTokenRequest,
            sasl_authenticate_request::SaslAuthenticateRequest,
            sasl_handshake_request::SaslHandshakeRequest,
            share_acknowledge_request::ShareAcknowledgeRequest,
            share_fetch_request::ShareFetchRequest,
            share_group_describe_request::ShareGroupDescribeRequest,
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
            streams_group_describe_request::StreamsGroupDescribeRequest,
            streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
            streams_group_topology_description_update_request::StreamsGroupTopologyDescriptionUpdateRequest,
            sync_group_request::SyncGroupRequest,
            txn_offset_commit_request::TxnOffsetCommitRequest,
            unregister_broker_request::UnregisterBrokerRequest,
            unregister_controller_request::UnregisterControllerRequest,
            update_features_request::UpdateFeaturesRequest,
            update_raft_voter_request::UpdateRaftVoterRequest,
            write_share_group_state_request::WriteShareGroupStateRequest,
            write_txn_markers_request::WriteTxnMarkersRequest,
        },
    },
};

/// Read the complete decodable range from the generated request schema.
const fn catalog_request<R: ProtocolRequest>() -> CatalogApi {
    CatalogApi::new(R::API_KEY, R::MIN_VERSION, R::MAX_VERSION)
}

/// Whether the broker serves the request versions Kafka 4.x removed.
///
/// krabka-only; Kafka has no switch for it. The `[runtime]` key
/// `legacy_request_versions_enable` reads it. While it is
/// [`Disabled`][Self::Disabled], the default, every listener advertises and
/// accepts exactly Kafka 4.3.1's minimum versions: `Fetch` from v4 and
/// `ListOffsets` from v1, and `Produce` from v3, though `Produce` is still
/// advertised from v0 as Kafka advertises it (KAFKA-18659). A request below
/// the minimum closes the connection, as it does against a 4.3.1 broker.
/// [`Enabled`][Self::Enabled] serves `Fetch` v0-v3, `ListOffsets` v0 and
/// `Produce` v0-v2 as well.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LegacyRequestVersions {
    /// Serve Kafka 4.3.1's minimum versions only.
    #[default]
    Disabled,
    /// Also serve the pre-4.0 request versions krabka still decodes.
    Enabled,
}

impl From<bool> for LegacyRequestVersions {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// The two version switches a listener's table depends on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VersionGates {
    /// Kafka's internal `unstable.api.versions.enable`.
    pub unstable: UnstableApiVersions,
    /// krabka's `legacy_request_versions_enable`.
    pub legacy: LegacyRequestVersions,
}

/// One API the broker dispatches, with the version range it decodes and the
/// range Kafka 4.3.1 serves.
///
/// `min_version..=max_version` is what the handler decodes and answers.
/// `released` is the Kafka 4.3.1 row for the key, from
/// [`krabka_raft::KAFKA_4_3_1_APIS`], or `None` when that release has no such
/// api key; it is the single table the default gates read.
///
/// Under the defaults a listener advertises and accepts `released` exactly.
/// [`UnstableApiVersions::Enabled`] lifts the maximum to `max_version` and
/// admits the api keys 4.3.1 lacks, as Kafka's `unstable.api.versions.enable`
/// does for a `latestVersionUnstable` version; [`LegacyRequestVersions::Enabled`]
/// lowers the minimum to `min_version`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogApi {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub released: Option<krabka_raft::ReleasedApi>,
}

impl CatalogApi {
    /// The catalog row for `api_key` decoded over `min_version..=max_version`,
    /// with its Kafka 4.3.1 row looked up.
    ///
    /// A krabka-private api key -- `GetReplicaLogInfo` (1020), at or above
    /// `KRABKA_PRIVATE_API_KEY_FLOOR` -- has no Kafka counterpart for either
    /// switch to track, so its whole decodable range counts as released.
    #[must_use]
    pub const fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        let released = if api_key >= KRABKA_PRIVATE_API_KEY_FLOOR {
            Some(krabka_raft::ReleasedApi {
                api_key,
                min_version,
                max_version,
            })
        } else {
            krabka_raft::kafka_4_3_1_api(api_key)
        };
        Self {
            api_key,
            min_version,
            max_version,
            released,
        }
    }

    /// Kafka 4.3.1's highest version of this api, or `None` when that
    /// release does not have it.
    #[must_use]
    pub const fn released_max(self) -> Option<i16> {
        match self.released {
            Some(released) => Some(released.max_version),
            None => None,
        }
    }

    /// The versions this api is accepted at under `gates`, or `None` when the
    /// api key is not accepted at all.
    #[must_use]
    pub fn accepted(self, gates: VersionGates) -> Option<std::ops::RangeInclusive<i16>> {
        let max = match gates.unstable {
            UnstableApiVersions::Enabled => self.max_version,
            UnstableApiVersions::Disabled => self.released_max()?,
        };
        let min = match (gates.legacy, self.released) {
            (LegacyRequestVersions::Disabled, Some(released)) => {
                released.min_version.max(self.min_version)
            }
            _ => self.min_version,
        };
        Some(min..=max)
    }

    /// The `ApiVersions` row for this api under `gates`, Kafka's
    /// `ApiKeys.toApiVersionForApiResponse`, or `None` when the listener
    /// leaves the key out. `Produce` keeps advertising from v0 whatever it
    /// accepts, as Kafka's `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION` does.
    #[must_use]
    pub fn advertised(self, gates: VersionGates) -> Option<ApiVersion> {
        let accepted = self.accepted(gates)?;
        let min_version = if self.api_key == krabka_protocol::owned::produce_request::API_KEY {
            PRODUCE_ADVERTISED_MIN_VERSION.min(*accepted.start())
        } else {
            *accepted.start()
        };
        Some(ApiVersion {
            api_key: self.api_key,
            min_version,
            max_version: *accepted.end(),
            ..Default::default()
        })
    }
}

/// Kafka's `ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`: a broker
/// listener advertises `Produce` from v0 although it accepts only v3 and up,
/// because librdkafka reads a higher minimum as no `Produce` support at all
/// (KAFKA-18659).
const PRODUCE_ADVERTISED_MIN_VERSION: i16 = 0;

/// Which of the broker's listeners an `ApiVersions` response goes out on, and
/// where [`INTER_BROKER_ONLY_APIS`] may dispatch.
///
/// Apache Kafka tags every request schema with the listener types that accept
/// it, and `ApiVersionsResponse.filterApis` drops every row whose tag does not
/// hold for the listener the request arrived on. A Kafka broker answers with
/// `ListenerType.BROKER` on every listener it binds, client-facing or
/// inter-broker alike, so a key tagged `controller` only --
/// `AlterPartition`, `BrokerRegistration`, and the rest of
/// [`INTER_BROKER_ONLY_APIS`] -- reaches no broker listener at all, and the
/// socket layer closes a connection that sends one. krabka's controller
/// listener does the same through `krabka_raft`'s own
/// `CONTROLLER_LISTENER_APIS`; this enum is the broker side of it.
///
/// krabka still has to accept and answer [`INTER_BROKER_ONLY_APIS`]
/// somewhere, because it reaches a peer over the peer's *inter-broker*
/// endpoint for those RPCs rather than over a separate controller listener.
/// That is [`InterBroker`][Self::InterBroker] and
/// [`ClientAndInterBroker`][Self::ClientAndInterBroker] alike; only
/// [`Client`][Self::Client] closes the connection instead of dispatching.
/// Advertising is narrower: only the listener no client can reach --
/// [`InterBroker`][Self::InterBroker] -- keeps them in `ApiVersions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerKind {
    /// A listener a Kafka client reaches and that is not
    /// `inter.broker.listener.name`. It advertises what a Kafka broker
    /// advertises, every key except [`INTER_BROKER_ONLY_APIS`], and it closes
    /// the connection rather than dispatch one of them.
    Client,
    /// The listener `inter.broker.listener.name` names, when the broker also
    /// binds at least one other, purely client-facing listener -- a Kafka
    /// deployment's `BROKER` listener, reached only by peers. It dispatches
    /// and advertises [`INTER_BROKER_ONLY_APIS`], because a krabka broker
    /// negotiates those RPCs against the table this listener advertises
    /// before it can send one.
    InterBroker,
    /// The listener `inter.broker.listener.name` names, when it is the
    /// broker's only listener -- the default single-listener configuration --
    /// and so also serves ordinary clients.
    ///
    /// It still dispatches [`INTER_BROKER_ONLY_APIS`], the same as
    /// [`InterBroker`][Self::InterBroker]: krabka's peers reach it there
    /// because there is nowhere else. It withholds them from `ApiVersions`
    /// the same as [`Client`][Self::Client]: no Kafka broker listener ever
    /// advertises a controller-scoped key, and a client can reach this one.
    ClientAndInterBroker,
}

/// Whether the broker has somewhere to send KIP-714 client metrics.
///
/// Kafka advertises `GetTelemetrySubscriptions` and `PushTelemetry` only when
/// `metric.reporters` holds a `ClientTelemetry` implementation; a stock broker
/// has none and answers `ApiVersions` without those two rows, which is what
/// keeps a modern Java or librdkafka client from opening a telemetry handshake
/// it has nowhere to push to. krabka's equivalent is
/// `client_metrics_enable`, and
/// [`crate::config::BrokerConfig::client_metrics_receiver`] reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientMetricsReceiver {
    /// A receiver is configured, so the two KIP-714 keys are advertised.
    Configured,
    /// No receiver is configured. The handlers stay registered and answer a
    /// client that sends one anyway, exactly as they do today; nothing invites
    /// a client to start.
    Absent,
}

/// The api keys scoped to the listener `inter.broker.listener.name` names:
/// withheld from `ApiVersions` on every other listener (#843), and refused --
/// connection closed, no response -- when dispatched there (#683).
///
/// Eight of them are tagged `controller` only by their request schema in
/// `krabka-protocol`, so no Kafka broker listener has ever advertised or
/// dispatched them: `AlterPartition` (56), `FetchSnapshot` (59),
/// `BrokerRegistration` (62), `BrokerHeartbeat` (63), `AllocateProducerIds`
/// (67), `ControllerRegistration` (70), `AssignReplicasToDirs` (73) and
/// `UpdateRaftVoter` (82). krabka has no separate controller listener for
/// them -- its controller listener routes several of them back into these
/// same broker handlers, through `krabka_raft`'s own
/// `CONTROLLER_LISTENER_APIS` -- so it accepts them on
/// [`ListenerKind::InterBroker`] and
/// [`ListenerKind::ClientAndInterBroker`] instead, where every handler still
/// gates on `ClusterAction`. That is not a reason to advertise or dispatch
/// them on a listener a client reaches too: a tool or an audit that reads the
/// advertised set to infer a node's role would read a krabka broker as a
/// controller, and a client that skips version negotiation would otherwise
/// still reach the handlers.
///
/// `GetReplicaLogInfo` (1020) is the ninth. It is krabka-private: Kafka trunk
/// gives 93, the key it used to hold, to `StreamsGroupTopologyDescriptionUpdate`,
/// and no released Kafka advertises either -- `mirror.gcr.io/apache/kafka:4.3.1`
/// stops at api key 92. The only caller in this tree is the KIP-966 unclean
/// recovery manager, which dials a replica's inter-broker endpoint. It
/// therefore belongs on the same side of the split as the other eight.
///
/// [`dispatched_apis`] still carries every key here at its full version
/// range, because the version bounds a listener serves at do not change; only
/// whether the listener accepts the key at all does, and that is enforced in
/// `crate::network::dispatch` by reading
/// [`crate::config::BrokerConfig::listener_kind`], not by trimming this table.
pub const INTER_BROKER_ONLY_APIS: &[i16] = {
    use krabka_protocol::owned;
    &[
        owned::alter_partition_request::API_KEY,
        owned::fetch_snapshot_request::API_KEY,
        owned::broker_registration_request::API_KEY,
        owned::broker_heartbeat_request::API_KEY,
        owned::allocate_producer_ids_request::API_KEY,
        owned::controller_registration_request::API_KEY,
        owned::assign_replicas_to_dirs_request::API_KEY,
        owned::update_raft_voter_request::API_KEY,
        owned::get_replica_log_info_request::API_KEY,
    ]
};

/// The KIP-714 client-metrics keys, advertised only behind
/// [`ClientMetricsReceiver::Configured`].
pub const CLIENT_METRICS_APIS: &[i16] = {
    use krabka_protocol::owned;
    &[
        owned::get_telemetry_subscriptions_request::API_KEY,
        owned::push_telemetry_request::API_KEY,
    ]
};

/// Every API the broker dispatches, with the version range it decodes and
/// its latest stable version, in the order the handlers were added. Update it
/// when you add a handler.
///
/// This is the union across both listener kinds and both telemetry settings.
/// [`dispatched_apis`] and [`supported_apis`] are both views of it.
#[must_use]
pub fn catalog_apis() -> Vec<CatalogApi> {
    let mut apis = client_facing_apis();
    apis.extend(admin_apis());
    apis
}

/// Every API the broker dispatches, over the whole range its handler decodes,
/// mirrored from each API's generated `MIN_VERSION` and `MAX_VERSION`.
///
/// This is what the dispatch registry takes its per-key version bounds from.
/// It is not what any listener advertises; that is [`supported_apis`]. A
/// version outside Kafka 4.3.1's range, or any version of a key that release
/// lacks, is inside this range, and [`is_disabled_version`] is what refuses it
/// under the default [`VersionGates`].
#[must_use]
pub fn dispatched_apis() -> Vec<ApiVersion> {
    catalog_apis()
        .into_iter()
        .map(|api| ApiVersion {
            api_key: api.api_key,
            min_version: api.min_version,
            max_version: api.max_version,
            ..Default::default()
        })
        .collect()
}

/// The catalog, keyed for the per-request lookup [`is_disabled_version`]
/// makes. Computed once.
static CATALOG_BY_KEY: std::sync::LazyLock<std::collections::BTreeMap<i16, CatalogApi>> =
    std::sync::LazyLock::new(|| {
        catalog_apis()
            .into_iter()
            .map(|api| (api.api_key, api))
            .collect()
    });

/// Whether `version` of `api_key` is a decodable version that `gates`
/// disables: above Kafka 4.3.1's maximum, below its minimum, or any version of
/// an api key that release does not have.
///
/// Kafka's `Processor.parseRequestHeader` asks `ApiKeys.isVersionEnabled`,
/// which refuses a version the release knows but does not enable, and throws
/// `InvalidRequestException` for it; `RequestHeader.parse` throws the same for
/// an api key the release does not know (`ApiKeys.forId`). `SocketServer`
/// closes the connection without a response either way. `ApiVersions` is
/// exempt in Kafka -- every version of it reaches `KafkaApis`, which answers
/// `UNSUPPORTED_VERSION` -- so it is never disabled here. A version outside
/// the decodable range is not this function's concern: [`dispatched_apis`]
/// refuses it first. Catalogued krabka-private keys treat their whole
/// decodable range as released. Other private dispatch entries have no
/// catalog row and are not disabled by this lookup.
#[must_use]
pub fn is_disabled_version(api_key: i16, version: i16, gates: VersionGates) -> bool {
    if api_key == krabka_protocol::owned::api_versions_request::API_KEY {
        return false;
    }
    CATALOG_BY_KEY.get(&api_key).is_some_and(|api| {
        api.accepted(gates)
            .is_none_or(|accepted| !accepted.contains(&version))
    })
}

/// The API set `listener` advertises, sorted by API key.
///
/// Kafka fills the `ApiKeys` collection from `ApiKeys.apisForListener`, an
/// `EnumSet` that iterates in id order, so every Kafka broker listener answers
/// with a strictly ascending table, and so does this.
///
/// `client_metrics` gates the two KIP-714 keys on every listener kind, the way
/// Kafka gates them on a configured `ClientTelemetry` reporter. The
/// control-plane keys in [`INTER_BROKER_ONLY_APIS`] are withheld on every
/// listener a client can reach -- [`ListenerKind::Client`] and
/// [`ListenerKind::ClientAndInterBroker`] alike -- and kept only on
/// [`ListenerKind::InterBroker`], the dedicated listener no client dials
/// (#843). Dispatch is a separate, wider gate: see [`INTER_BROKER_ONLY_APIS`].
/// `gates` picks each row's range, and leaves out a key Kafka 4.3.1 lacks
/// unless unstable api versions are enabled.
#[must_use]
pub fn supported_apis(
    listener: ListenerKind,
    client_metrics: ClientMetricsReceiver,
    gates: VersionGates,
) -> Vec<ApiVersion> {
    let mut apis: Vec<ApiVersion> = catalog_apis()
        .into_iter()
        .filter(|api| {
            let withheld_control_plane = listener != ListenerKind::InterBroker
                && INTER_BROKER_ONLY_APIS.contains(&api.api_key);
            let withheld_telemetry = client_metrics == ClientMetricsReceiver::Absent
                && CLIENT_METRICS_APIS.contains(&api.api_key);
            !withheld_control_plane && !withheld_telemetry
        })
        .filter_map(|api| api.advertised(gates))
        .collect();
    apis.sort_unstable_by_key(|api| api.api_key);
    apis
}

fn client_facing_apis() -> Vec<CatalogApi> {
    use krabka_protocol::owned;
    vec![
        catalog_request::<ApiVersionsRequest>(),
        // Kafka 4.0 removed `Produce` v0-2, `Fetch` v0-3 and `ListOffsets`
        // v0. krabka still decodes them -- `Produce` up-converts the legacy
        // `MessageSet`, `Fetch` answers from the `kafka_3_6_2` flavor and
        // `ListOffsets` v0 from its own module -- and serves them only under
        // `LegacyRequestVersions::Enabled`. The row's Kafka 4.3.1 range is what
        // the default gate reads.
        CatalogApi::new(
            owned::produce_request::API_KEY,
            krabka_protocol::kafka_3_6_2::owned::produce_request::MIN_VERSION,
            owned::produce_request::MAX_VERSION,
        ),
        CatalogApi::new(
            owned::fetch_request::API_KEY,
            krabka_protocol::kafka_3_6_2::owned::fetch_request::MIN_VERSION,
            owned::fetch_request::MAX_VERSION,
        ),
        CatalogApi::new(
            owned::list_offsets_request::API_KEY,
            0,
            owned::list_offsets_request::MAX_VERSION,
        ),
        catalog_request::<MetadataRequest>(),
        catalog_request::<FindCoordinatorRequest>(),
        catalog_request::<JoinGroupRequest>(),
        catalog_request::<SyncGroupRequest>(),
        catalog_request::<HeartbeatRequest>(),
        catalog_request::<LeaveGroupRequest>(),
        catalog_request::<SaslHandshakeRequest>(),
        catalog_request::<SaslAuthenticateRequest>(),
        catalog_request::<OffsetCommitRequest>(),
        catalog_request::<OffsetFetchRequest>(),
    ]
}

fn admin_apis() -> Vec<CatalogApi> {
    vec![
        catalog_request::<CreateTopicsRequest>(),
        catalog_request::<DeleteTopicsRequest>(),
        catalog_request::<DeleteRecordsRequest>(),
        catalog_request::<InitProducerIdRequest>(),
        // AllocateProducerIds is the controller-backed broker RPC used to
        // reserve durable, cluster-wide producer-ID blocks.
        catalog_request::<AllocateProducerIdsRequest>(),
        catalog_request::<OffsetForLeaderEpochRequest>(),
        catalog_request::<AddPartitionsToTxnRequest>(),
        catalog_request::<AddOffsetsToTxnRequest>(),
        catalog_request::<EndTxnRequest>(),
        catalog_request::<WriteTxnMarkersRequest>(),
        // Version 6 (KIP-1319, topic ids) is Kafka trunk's; 4.3.1 stops at 5,
        // so v6 is served only under `unstable.api.versions.enable`.
        catalog_request::<TxnOffsetCommitRequest>(),
        catalog_request::<DescribeConfigsRequest>(),
        catalog_request::<AlterReplicaLogDirsRequest>(),
        catalog_request::<DescribeLogDirsRequest>(),
        catalog_request::<DescribeGroupsRequest>(),
        catalog_request::<ListGroupsRequest>(),
        catalog_request::<AlterConfigsRequest>(),
        catalog_request::<CreatePartitionsRequest>(),
        catalog_request::<DeleteGroupsRequest>(),
        catalog_request::<IncrementalAlterConfigsRequest>(),
        catalog_request::<AlterPartitionRequest>(),
        catalog_request::<AssignReplicasToDirsRequest>(),
        catalog_request::<DescribeClusterRequest>(),
        catalog_request::<BrokerHeartbeatRequest>(),
        catalog_request::<BrokerRegistrationRequest>(),
        catalog_request::<ControllerRegistrationRequest>(),
        // UnregisterBroker (KIP-919) — admin RPC to permanently drop a
        // broker registration from the cluster's metadata image.
        catalog_request::<UnregisterBrokerRequest>(),
        // UnregisterController (KIP-1312, Kafka trunk) — drops a controller
        // registration. Tagged `broker` and `controller`: a broker listener
        // forwards it to the active controller. Kafka 4.3.1 has no api key 94,
        // so it is advertised and accepted only under
        // `unstable.api.versions.enable`.
        catalog_request::<UnregisterControllerRequest>(),
        catalog_request::<AlterUserScramCredentialsRequest>(),
        // UpdateFeatures (api_key 57, KIP-584) — `kafka-features` admin tool
        // finalizes broker-supported features through a Raft-persisted path.
        catalog_request::<UpdateFeaturesRequest>(),
        catalog_request::<DescribeAclsRequest>(),
        catalog_request::<CreateAclsRequest>(),
        catalog_request::<DeleteAclsRequest>(),
        catalog_request::<ElectLeadersRequest>(),
        catalog_request::<AlterPartitionReassignmentsRequest>(),
        catalog_request::<ListPartitionReassignmentsRequest>(),
        // OffsetDelete (api_key 47, KIP-496): completes
        // `kafka-consumer-groups --delete-offsets` parity.
        catalog_request::<OffsetDeleteRequest>(),
        catalog_request::<DescribeClientQuotasRequest>(),
        catalog_request::<AlterClientQuotasRequest>(),
        catalog_request::<DescribeUserScramCredentialsRequest>(),
        // KIP-48: delegation-token RPCs. Conditional on the
        // broker having a master key configured is tempting, but Kafka
        // always advertises these — clients discover support at this
        // level then get DELEGATION_TOKEN_AUTH_DISABLED (61) on the
        // actual call when the broker isn't configured for tokens.
        catalog_request::<CreateDelegationTokenRequest>(),
        catalog_request::<RenewDelegationTokenRequest>(),
        catalog_request::<ExpireDelegationTokenRequest>(),
        catalog_request::<DescribeDelegationTokenRequest>(),
        // DescribeProducers (KIP-664) — admin introspection of
        // per-(topic, partition) idempotent / transactional producer state.
        catalog_request::<DescribeProducersRequest>(),
        // DescribeTransactions + ListTransactions (KIP-664) — admin
        // introspection of the TxnCoordinator's local state map.
        catalog_request::<DescribeTransactionsRequest>(),
        catalog_request::<ListTransactionsRequest>(),
        // DescribeTopicPartitions (KIP-966) — paginated topic listing
        // used by JVM admin clients 3.7+ in place of fanned-out Metadata
        // calls for `kafka-topics --describe`.
        catalog_request::<DescribeTopicPartitionsRequest>(),
        // KIP-714 client-metrics push handshake, advertised only behind
        // `ClientMetricsReceiver::Configured`. Kafka advertises the pair only
        // when `metric.reporters` holds a `ClientTelemetry` implementation, so
        // a client that sees the rows has somewhere to push to.
        catalog_request::<GetTelemetrySubscriptionsRequest>(),
        catalog_request::<PushTelemetryRequest>(),
        // ListConfigResources (KIP-1142) — typed enumeration of every
        // configurable resource (topics + brokers + client_metrics). v0
        // is the legacy ListClientMetricsResources surface (KIP-714); v1
        // adds the `resource_types` filter.
        catalog_request::<ListConfigResourcesRequest>(),
        // DescribeQuorum (KIP-595) — `kafka-metadata-quorum --describe`
        // admin introspection of the controller-raft quorum.
        catalog_request::<DescribeQuorumRequest>(),
        // FetchSnapshot (KIP-630) — controller-snapshot byte-range fetch
        // used by replicas catching up via __cluster_metadata snapshots.
        catalog_request::<FetchSnapshotRequest>(),
        // KIP-848 next-gen consumer group protocol.
        catalog_request::<ConsumerGroupHeartbeatRequest>(),
        catalog_request::<ConsumerGroupDescribeRequest>(),
        // KIP-932 share-group membership protocol.
        catalog_request::<ShareGroupHeartbeatRequest>(),
        catalog_request::<ShareGroupDescribeRequest>(),
        // KIP-1071 streams-group rebalance protocol. Version 1 of both
        // (KIP-1331, KIP-1357) is Kafka trunk's and is served only under
        // `unstable.api.versions.enable`.
        catalog_request::<StreamsGroupHeartbeatRequest>(),
        catalog_request::<StreamsGroupDescribeRequest>(),
        // KIP-1331 topology description push (Kafka trunk). krabka stores it
        // in Kafka's in-memory plugin when
        // `group.streams.topology.description.plugin.class` names it, and
        // answers as a trunk broker without a plugin otherwise. Kafka 4.3.1
        // has no api key 93, so it is advertised and accepted only under
        // `unstable.api.versions.enable`.
        catalog_request::<StreamsGroupTopologyDescriptionUpdateRequest>(),
        // KIP-932 ShareFetch / ShareAcknowledge data-plane RPCs.
        catalog_request::<ShareFetchRequest>(),
        catalog_request::<ShareAcknowledgeRequest>(),
        // KIP-932 share-group admin offset RPCs.
        catalog_request::<DescribeShareGroupOffsetsRequest>(),
        catalog_request::<AlterShareGroupOffsetsRequest>(),
        catalog_request::<DeleteShareGroupOffsetsRequest>(),
        // KIP-932 share-coordinator (persister) RPCs.
        catalog_request::<InitializeShareGroupStateRequest>(),
        catalog_request::<ReadShareGroupStateRequest>(),
        catalog_request::<WriteShareGroupStateRequest>(),
        catalog_request::<DeleteShareGroupStateRequest>(),
        catalog_request::<ReadShareGroupStateSummaryRequest>(),
        // GetReplicaLogInfo (KIP-966) — inter-broker RPC the controller's
        // unclean recovery manager uses to read each replica's LEO + leader
        // epoch. Advertised so InterBrokerClient version negotiation succeeds.
        catalog_request::<GetReplicaLogInfoRequest>(),
        // KIP-853 dynamic-quorum reconfiguration — `kafka-metadata-quorum
        // --add-controller / --remove-controller` and the controller
        // auto-join path.
        catalog_request::<AddRaftVoterRequest>(),
        catalog_request::<RemoveRaftVoterRequest>(),
        catalog_request::<UpdateRaftVoterRequest>(),
    ]
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::collections::BTreeSet};

    /// The default gates: Kafka 4.3.1's table exactly.
    const STRICT: VersionGates = VersionGates {
        unstable: UnstableApiVersions::Disabled,
        legacy: LegacyRequestVersions::Disabled,
    };
    /// Both opt-ins on: everything krabka decodes.
    const ALL: VersionGates = VersionGates {
        unstable: UnstableApiVersions::Enabled,
        legacy: LegacyRequestVersions::Enabled,
    };
    /// Every combination of the two switches.
    const EVERY_GATE: [VersionGates; 4] = [
        STRICT,
        VersionGates {
            unstable: UnstableApiVersions::Enabled,
            legacy: LegacyRequestVersions::Disabled,
        },
        VersionGates {
            unstable: UnstableApiVersions::Disabled,
            legacy: LegacyRequestVersions::Enabled,
        },
        ALL,
    ];

    /// The advertised version range of one API key.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct VersionRange {
        min: i16,
        max: i16,
    }

    const fn range(min: i16, max: i16) -> VersionRange { VersionRange { min, max } }

    /// An API key whose advertised range a switch moves.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct MovedRange {
        api_key: i16,
        before: Option<VersionRange>,
        after: Option<VersionRange>,
    }

    /// The client listener's table, which is what a Kafka client reads.
    fn client_apis() -> Vec<ApiVersion> {
        supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, STRICT)
    }

    #[test]
    fn share_group_apis_are_advertised() {
        let apis = client_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        assert!(keys.contains(&76));
        assert!(keys.contains(&77));
        let hb = apis.iter().find(|a| a.api_key == 76).unwrap();
        assert!(hb.min_version == 1 && hb.max_version == 1);
    }

    #[test]
    fn streams_group_apis_are_advertised() {
        let apis = client_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        // StreamsGroupHeartbeat(88), StreamsGroupDescribe(89), at Kafka
        // 4.3.1's v0; trunk's v1 is behind `unstable.api.versions.enable`.
        assert!(keys.contains(&88));
        assert!(keys.contains(&89));
        let hb = apis.iter().find(|a| a.api_key == 88).unwrap();
        assert!((hb.min_version, hb.max_version) == (0, 0));
    }

    #[test]
    fn share_coordinator_persister_apis_are_advertised() {
        let apis = client_apis();
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
        for apis in [client_apis(), dispatched_apis()] {
            assert!(!apis.is_empty(), "advertised API table must not be empty");
            // ApiVersions itself (api_key 18) is always advertised.
            assert!(apis.iter().any(|a| a.api_key == 18));
            for a in &apis {
                assert!(a.min_version <= a.max_version, "api {} min>max", a.api_key);
            }
        }
    }

    /// The keys one advertised table names, in ascending order.
    fn keys_of(apis: &[ApiVersion]) -> BTreeSet<i16> {
        apis.iter().map(|api| api.api_key).collect()
    }

    /// A client listener advertises what a Kafka broker advertises: every
    /// dispatched key except the control-plane set and, with no receiver
    /// configured, the two KIP-714 keys.
    #[test]
    fn the_client_listener_withholds_the_control_plane_and_telemetry_keys() {
        let withheld: BTreeSet<i16> = INTER_BROKER_ONLY_APIS
            .iter()
            .chain(CLIENT_METRICS_APIS)
            .copied()
            .collect();
        let dispatched = keys_of(&dispatched_apis());
        assert!(withheld.is_subset(&dispatched));
        let all = supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, ALL);
        assert!(keys_of(&all) == dispatched.difference(&withheld).copied().collect());
    }

    /// The dedicated inter-broker listener keeps the control-plane keys,
    /// because a krabka broker negotiates them against a peer's inter-broker
    /// endpoint. This is the split-listener configuration: a client cannot
    /// reach this listener.
    #[test]
    fn the_inter_broker_listener_keeps_the_control_plane_keys() {
        let apis = supported_apis(
            ListenerKind::InterBroker,
            ClientMetricsReceiver::Absent,
            STRICT,
        );
        let keys = keys_of(&apis);
        for api_key in INTER_BROKER_ONLY_APIS {
            assert!(keys.contains(api_key), "api_key {api_key}");
        }
        for api_key in CLIENT_METRICS_APIS {
            assert!(!keys.contains(api_key), "api_key {api_key}");
        }
    }

    /// #843: no listener a client can reach advertises a control-plane key,
    /// including the default single-listener broker where the sole listener
    /// carries client and inter-broker traffic together. Table-driven over
    /// every listener configuration from the issue: the default single
    /// listener, a split client/inter-broker configuration, and a listener
    /// that is neither (`Client`, e.g. a would-be controller-named listener
    /// that `inter.broker.listener.name` does not point at).
    #[test]
    fn no_client_reachable_listener_advertises_a_control_plane_key() {
        for listener in [ListenerKind::Client, ListenerKind::ClientAndInterBroker] {
            let keys = keys_of(&supported_apis(
                listener,
                ClientMetricsReceiver::Absent,
                STRICT,
            ));
            for api_key in INTER_BROKER_ONLY_APIS {
                assert!(
                    !keys.contains(api_key),
                    "listener {listener:?} must not advertise api_key {api_key}"
                );
            }
        }
    }

    /// A configured receiver adds the KIP-714 pair, and nothing else, to
    /// every listener kind.
    #[test]
    fn a_configured_client_metrics_receiver_adds_only_the_two_telemetry_keys() {
        for listener in [
            ListenerKind::Client,
            ListenerKind::InterBroker,
            ListenerKind::ClientAndInterBroker,
        ] {
            let absent = keys_of(&supported_apis(
                listener,
                ClientMetricsReceiver::Absent,
                STRICT,
            ));
            let configured = keys_of(&supported_apis(
                listener,
                ClientMetricsReceiver::Configured,
                STRICT,
            ));
            assert!(
                configured
                    .difference(&absent)
                    .copied()
                    .collect::<BTreeSet<i16>>()
                    == CLIENT_METRICS_APIS.iter().copied().collect()
            );
        }
    }

    /// Every advertised row keeps the version range the dispatch table serves
    /// it at, so filtering never narrows a range.
    #[test]
    fn filtering_by_listener_does_not_move_a_version_range() {
        let dispatched = dispatched_apis();
        for listener in [
            ListenerKind::Client,
            ListenerKind::InterBroker,
            ListenerKind::ClientAndInterBroker,
        ] {
            for metrics in [
                ClientMetricsReceiver::Absent,
                ClientMetricsReceiver::Configured,
            ] {
                for api in supported_apis(listener, metrics, ALL) {
                    assert!(dispatched.contains(&api), "api_key {}", api.api_key);
                }
            }
        }
    }

    /// #842: every listener's table is strictly ascending by api key, as
    /// Kafka's `apisForListener` `EnumSet` orders it.
    #[test]
    fn every_listener_table_is_strictly_ascending() {
        for listener in [
            ListenerKind::Client,
            ListenerKind::InterBroker,
            ListenerKind::ClientAndInterBroker,
        ] {
            for metrics in [
                ClientMetricsReceiver::Absent,
                ClientMetricsReceiver::Configured,
            ] {
                for gates in EVERY_GATE {
                    let apis = supported_apis(listener, metrics, gates);
                    assert!(
                        apis.windows(2)
                            .all(|pair| pair[0].api_key < pair[1].api_key),
                        "{listener:?} {metrics:?} {gates:?}"
                    );
                }
            }
        }
    }

    /// #784: with both switches at their defaults every listener advertises
    /// exactly Kafka 4.3.1's table -- each key's range is the release's
    /// `validVersions`, `Produce` advertised from v0 (KAFKA-18659) -- and no
    /// key that release lacks except krabka-private `GetReplicaLogInfo` on
    /// the inter-broker listener.
    #[test]
    fn the_default_table_is_kafka_4_3_1s() {
        let released: std::collections::BTreeMap<i16, VersionRange> = krabka_raft::KAFKA_4_3_1_APIS
            .iter()
            .map(|api| (api.api_key, range(api.min_version, api.max_version)))
            .collect();
        let apis = supported_apis(
            ListenerKind::InterBroker,
            ClientMetricsReceiver::Configured,
            STRICT,
        );
        for api in &apis {
            let advertised = range(api.min_version, api.max_version);
            match (api.api_key, released.get(&api.api_key)) {
                (0, Some(&VersionRange { min: 3, max })) => {
                    assert!(advertised == range(0, max), "Produce");
                }
                (key, Some(&want)) => assert!(advertised == want, "api_key {key}"),
                (key, None) => assert!(
                    key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
                    "api_key {key} is not in Kafka 4.3.1"
                ),
            }
        }
    }

    /// #784: what each switch moves, in the whole table. Table-driven over
    /// every combination: each row names the keys whose advertised range
    /// differs from the default table, and how.
    #[yare::parameterized(
        unstable_only = { EVERY_GATE[1] },
        legacy_only = { EVERY_GATE[2] },
        both_enabled = { ALL },
    )]
    fn each_switch_moves_only_its_own_rows(gates: VersionGates) {
        type Moved = Vec<MovedRange>;
        let table = |gates| {
            supported_apis(
                ListenerKind::InterBroker,
                ClientMetricsReceiver::Configured,
                gates,
            )
        };
        let strict = table(STRICT);
        let moved = |gates| -> Moved {
            let other = table(gates);
            let keys: BTreeSet<i16> = keys_of(&strict).union(&keys_of(&other)).copied().collect();
            let advertised = |apis: &[ApiVersion], key| {
                apis.iter()
                    .find(|api| api.api_key == key)
                    .map(|api| range(api.min_version, api.max_version))
            };
            keys.into_iter()
                .map(|api_key| MovedRange {
                    api_key,
                    before: advertised(&strict, api_key),
                    after: advertised(&other, api_key),
                })
                .filter(|row| row.before != row.after)
                .collect()
        };
        let unstable: Moved = vec![
            MovedRange {
                api_key: 18,
                before: Some(range(0, 4)),
                after: Some(range(0, 5)),
            },
            MovedRange {
                api_key: 22,
                before: Some(range(0, 5)),
                after: Some(range(0, 6)),
            },
            MovedRange {
                api_key: 28,
                before: Some(range(0, 5)),
                after: Some(range(0, 6)),
            },
            // KIP-1331's per-group `ErrorMessage`.
            MovedRange {
                api_key: 42,
                before: Some(range(0, 2)),
                after: Some(range(0, 3)),
            },
            MovedRange {
                api_key: 88,
                before: Some(range(0, 0)),
                after: Some(range(0, 1)),
            },
            MovedRange {
                api_key: 89,
                before: Some(range(0, 0)),
                after: Some(range(0, 1)),
            },
            MovedRange {
                api_key: 93,
                before: None,
                after: Some(range(0, 0)),
            },
            MovedRange {
                api_key: 94,
                before: None,
                after: Some(range(0, 0)),
            },
        ];
        let legacy: Moved = vec![
            MovedRange {
                api_key: 1,
                before: Some(range(4, 18)),
                after: Some(range(0, 18)),
            },
            MovedRange {
                api_key: 2,
                before: Some(range(1, 11)),
                after: Some(range(0, 11)),
            },
        ];
        let both: Moved = {
            let mut rows = legacy.clone();
            rows.extend(unstable.iter().copied());
            rows.sort_unstable_by_key(|row| row.api_key);
            rows
        };
        let expected = match (gates.unstable, gates.legacy) {
            (UnstableApiVersions::Enabled, LegacyRequestVersions::Disabled) => unstable,
            (UnstableApiVersions::Disabled, LegacyRequestVersions::Enabled) => legacy,
            (UnstableApiVersions::Enabled, LegacyRequestVersions::Enabled) => both,
            (UnstableApiVersions::Disabled, LegacyRequestVersions::Disabled) => unreachable!(),
        };
        assert2::assert!(moved(gates) == expected, "{gates:?}");
    }

    /// #784: a version is refused on receive exactly when it is not
    /// accepted, table-driven over the gated rows of every switch
    /// combination. `Produce` v0-v2 is refused by default although it is
    /// advertised, as Kafka 4.3.1 refuses it; `ApiVersions` is never refused
    /// here, since its handler answers `UNSUPPORTED_VERSION`.
    #[yare::parameterized(
        produce_v0 = { 0, 0, [true, true, false, false] },
        produce_v2 = { 0, 2, [true, true, false, false] },
        produce_v3 = { 0, 3, [false, false, false, false] },
        fetch_v3 = { 1, 3, [true, true, false, false] },
        fetch_v4 = { 1, 4, [false, false, false, false] },
        list_offsets_v0 = { 2, 0, [true, true, false, false] },
        list_offsets_v1 = { 2, 1, [false, false, false, false] },
        api_versions_v5 = { 18, 5, [false, false, false, false] },
        heartbeat_v6 = { 22, 6, [true, false, true, false] },
        offset_commit_v6 = { 28, 6, [true, false, true, false] },
        offset_commit_v5 = { 28, 5, [false, false, false, false] },
        describe_topic_partitions_v1 = { 88, 1, [true, false, true, false] },
        share_fetch_v1 = { 89, 1, [true, false, true, false] },
        share_group_heartbeat_v0 = { 93, 0, [true, false, true, false] },
        share_group_describe_v0 = { 94, 0, [true, false, true, false] },
        private_api_v0 = { 1020, 0, [false, false, false, false] },
    )]
    fn a_gated_version_is_refused_exactly_when_it_is_not_accepted(
        api_key: i16,
        version: i16,
        want: [bool; 4],
    ) {
        let [strict, unstable, legacy, all] = EVERY_GATE;
        // `want` is under [strict, unstable, legacy, all].
        for (gates, disabled) in [strict, unstable, legacy, all].into_iter().zip(want) {
            assert2::assert!(
                is_disabled_version(api_key, version, gates) == disabled,
                "api_key {api_key} v{version} {gates:?}"
            );
        }
    }

    /// The single row Kafka puts in an `UNSUPPORTED_VERSION` answer
    /// (`ApiVersionsResponse.toApiVersion(API_VERSIONS)`) is the same
    /// `ApiVersions` row every listener advertises.
    #[yare::parameterized(
        stable = { STRICT, 4 },
        unstable = { ALL, 5 },
    )]
    fn the_unsupported_version_row_is_the_advertised_api_versions_row(
        gates: VersionGates,
        max_version: i16,
    ) {
        let advertised: Vec<ApiVersion> =
            supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, gates)
                .into_iter()
                .filter(|api| api.api_key == 18)
                .collect();
        assert2::assert!(
            advertised
                == vec![ApiVersion {
                    api_key: 18,
                    min_version: 0,
                    max_version,
                    ..Default::default()
                }],
            "{gates:?}"
        );
        assert2::assert!(
            krabka_raft::unsupported_version_response(gates.unstable).api_keys == advertised,
            "{gates:?}"
        );
    }
}
