//! Broker API dispatch registry.
//!
//! The root holds the handler-signature aliases, the [`dispatch_table!`] that
//! generates an adapter and a registration for every Kafka api whose name
//! derives the rest, and [`build_registry`], which assembles the whole table.
//! The krabka-private table keeps a `macro_rules!` generator, which stays in
//! this file because `macro_rules!` scope is textual: a child module sees a
//! macro only when the definition comes before the `mod` declaration that
//! pulls the child in.
//!
//! [`dispatch_table!`]: krabka_macros::dispatch_table

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_protocol::api_key::ApiKey;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, RequestContext, TelemetryContext},
};

pub(crate) type ContextHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type ProduceHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    Bytes,
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type TelemetryHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a TelemetryContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type AuthHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a crate::network::auth::ConnectionAuth,
    &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

/// Registers krabka-private context dispatches by raw wire `api_key`.
///
/// A krabka-private api key sits at or above
/// [`KRABKA_PRIVATE_API_KEY_FLOOR`][crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR],
/// and `ApiKey::from_i16` returns `None` for every key in that range. So each
/// entry names the wire code and its `flexible_min` directly, where a Kafka
/// entry reads both from the generated schema constants. The registry entry is
/// then the only place the framing layer can learn that the body is flexible.
///
/// Every krabka-private api gets [`DispatchKind::Context`], so the handler
/// receives the [`RequestContext`] and can authorize on the principal.
macro_rules! krabka_private_context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api_key:path, $flexible_min:expr, $handler:path)),* $(,)?) => {
        $(
            fn $adapter<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin($handler(broker, version, correlation_id, body, ctx))
            }
        )*

        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
            let entries: &[(ApiKeyCode, ApiVersion, ContextHandler)] = &[
                $(($api_key, $flexible_min, $adapter as ContextHandler),)*
            ];
            for &(api_key, flexible_min, handler) in entries {
                assert2::assert!(
                    api_key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
                    "api_key {api_key} is below the krabka-private floor"
                );
                assert2::assert!(
                    registry.register(DispatchEntry::context(api_key, flexible_min, handler)),
                    "duplicate dispatch registration for api_key {api_key}"
                );
                // Kafka defines no krabka-private api, so no Kafka client
                // quota applies to one. `WriteBarrierMarkers` is inter-broker
                // traffic, and the operator apis are not what a
                // `request_percentage` quota is written for.
                registry.exempt_from_request_quota(api_key);
            }
        }
    };
}

mod auth;
mod entry;
mod krabka_private;
#[cfg(test)]
mod tests;

pub(crate) use self::entry::{DispatchEntry, DispatchKind, DispatchRegistry, RequestQuotaPolicy};
use self::{
    auth::{
        alter_replica_log_dirs_adapter, create_delegation_token_adapter,
        describe_delegation_token_adapter, expire_delegation_token_adapter,
        renew_delegation_token_adapter,
    },
    krabka_private::register_krabka_private_context_dispatches,
};

// One adapter and one `register_dispatch_table` registration per Kafka api.
// The handler is `crate::handlers::<snake_name>::handle` unless the entry names
// another after `=>`. `auth` entries register the
// hand-written `<snake_name>_adapter` imported above.
krabka_macros::dispatch_table! {
    // `handle(broker, version, correlation_id, body, ctx)`, awaited.
    context:
        AssignReplicasToDirs,
        Metadata,
        CreateTopics,
        ShareGroupDescribe,
        AlterShareGroupOffsets,
        DeleteShareGroupOffsets,
        DeleteGroups,
        UnregisterBroker,
        UnregisterController,
        AddRaftVoter,
        RemoveRaftVoter,
        UpdateRaftVoter,
        BrokerRegistration,
        ControllerRegistration,
        StreamsGroupHeartbeat,
        ListOffsets,
        DescribeQuorum,
        AllocateProducerIds,
        AddOffsetsToTxn => crate::txn::handlers::add_offset_commits_to_txn::handle,
        WriteTxnMarkers => crate::txn::handlers::write_txn_markers::handle,
        FetchSnapshot;
    // The same arguments; the result is wrapped in a ready future.
    sync_context:
        GetReplicaLogInfo,
        OffsetForLeaderEpoch;
    // `handle(broker, request, version, ctx)` on the decoded request, awaited;
    // the handler returns its response struct, which the adapter encodes.
    typed:
        DescribeCluster,
        DescribeGroups,
        ListGroups,
        OffsetDelete,
        DescribeProducers,
        DescribeTransactions,
        ListTransactions,
        ConsumerGroupDescribe,
        StreamsGroupDescribe,
        DescribeLogDirs,
        DescribeTopicPartitions,
        DeleteTopics,
        AlterConfigs,
        IncrementalAlterConfigs,
        DeleteRecords,
        DescribeShareGroupOffsets,
        AlterPartition,
        BrokerHeartbeat,
        StreamsGroupTopologyDescriptionUpdate,
        FindCoordinator,
        AlterUserScramCredentials,
        UpdateFeatures,
        ShareFetch,
        ShareAcknowledge,
        CreatePartitions,
        OffsetFetch,
        EndTxn => crate::txn::handlers::end_txn::handle,
        CreateAcls,
        DeleteAcls,
        ElectLeaders,
        AlterPartitionReassignments,
        AlterClientQuotas,
        AddPartitionsToTxn => crate::txn::handlers::add_partitions_to_txn::handle;
    // The same, with the request decoded by `decode_group_request`, which
    // refuses a string no coordinator record can carry.
    typed_group:
        Heartbeat,
        SyncGroup,
        LeaveGroup,
        JoinGroup,
        OffsetCommit,
        ConsumerGroupHeartbeat,
        ShareGroupHeartbeat,
        InitProducerId,
        TxnOffsetCommit => crate::txn::handlers::txn_offset_commit::handle,
        InitializeShareGroupState => crate::share_coordinator::handlers::initialize::handle,
        ReadShareGroupState => crate::share_coordinator::handlers::read::handle,
        WriteShareGroupState => crate::share_coordinator::handlers::write::handle,
        DeleteShareGroupState => crate::share_coordinator::handlers::delete::handle,
        ReadShareGroupStateSummary => crate::share_coordinator::handlers::read_summary::handle;
    // `typed`, called without awaiting: the result is wrapped in a ready future.
    typed_sync:
        ListConfigResources,
        DescribeConfigs,
        DescribeAcls,
        ListPartitionReassignments,
        DescribeClientQuotas,
        DescribeUserScramCredentials;
    // Hand-written in `auth`: the adapter receives the `ConnectionAuth` and
    // the peer address instead of a `RequestContext`.
    auth:
        AlterReplicaLogDirs,
        CreateDelegationToken,
        RenewDelegationToken,
        ExpireDelegationToken,
        DescribeDelegationToken;
    // KIP-714: the handler takes a `TelemetryContext`; the result is wrapped
    // in a ready future.
    telemetry:
        GetTelemetrySubscriptions,
        PushTelemetry;
}

fn produce_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    body_bytes: Bytes,
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(crate::handlers::produce::handle(
        broker,
        version,
        correlation_id,
        body,
        body_bytes,
        ctx,
    ))
}

pub(crate) fn build_registry() -> DispatchRegistry {
    let mut registry = DispatchRegistry::new();

    // KIP-219: `ApiVersionsResponse` carries `ThrottleTimeMs` behind the
    // `ApiKeys` array, so the dispatch loop's leading-int32 patch cannot report
    // a request-quota delay on it. The handler charges the quota and fills the
    // field in itself, the way Kafka's `handleApiVersionsRequest` answers
    // through `sendResponseMaybeThrottle`.
    registry.register(DispatchEntry::self_accounted_context(
        ApiKey::ApiVersions as i16,
        krabka_protocol::owned::api_versions_request::FLEXIBLE_MIN,
        crate::handlers::api_versions::handle,
    ));
    registry.register(DispatchEntry::produce(
        krabka_protocol::owned::produce_request::FLEXIBLE_MIN,
        produce_adapter,
    ));
    registry.register(DispatchEntry::fetch(
        krabka_protocol::owned::fetch_request::FLEXIBLE_MIN,
    ));
    registry.register(DispatchEntry::sasl_metadata(
        ApiKey::SaslHandshake as i16,
        i16::MAX,
    ));
    registry.register(DispatchEntry::sasl_metadata(
        ApiKey::SaslAuthenticate as i16,
        krabka_protocol::owned::sasl_authenticate_request::FLEXIBLE_MIN,
    ));
    register_dispatch_table(&mut registry);
    register_krabka_private_context_dispatches(&mut registry);
    // The apis Kafka answers through `sendResponseExemptThrottle`, so no
    // request quota holds them: `KafkaApis.handleWriteTxnMarkersRequest`,
    // `ControllerApis.handleAlterPartitionRequest`, and the raft rpcs of
    // `ControllerApis.handleRaftRequest` that no broker forwards
    // (`FetchSnapshot`, `UpdateRaftVoter`). `DescribeQuorum`, `AddRaftVoter`
    // and `RemoveRaftVoter` stay charged, because a broker forwards them and
    // `sendForwardedResponse` charges them there.
    for api in [
        ApiKey::WriteTxnMarkers,
        ApiKey::AlterPartition,
        ApiKey::FetchSnapshot,
        ApiKey::UpdateRaftVoter,
    ] {
        registry.exempt_from_request_quota(api as i16);
    }

    registry.apply_api_catalog();

    registry
}
