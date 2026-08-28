//! Broker API dispatch registry.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_protocol::api_key::ApiKey;
use krabka_units::convert::TimeExt as _;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId, RequestContext, TelemetryContext},
};

pub(crate) type PlainHandler =
    fn(&Broker, ApiVersion, CorrelationId, &[u8]) -> BoxFuture<'static, Result<Bytes, BrokerError>>;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestQuotaPolicy {
    ApplyFallbackAccounting,
    InlineExempt,
    SelfAccounted,
}

#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
    Auth(AuthHandler),
    Fetch,
    SaslMetadata,
}

#[derive(Clone, Copy)]
pub(crate) struct DispatchEntry {
    api_key: ApiKeyCode,
    flexible_min: ApiVersion,
    quota_policy: RequestQuotaPolicy,
    kind: DispatchKind,
}

#[derive(Default)]
pub(crate) struct DispatchRegistry {
    table: std::collections::HashMap<ApiKeyCode, DispatchEntry>,
}

impl DispatchEntry {
    pub(crate) fn plain(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: PlainHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Plain(handler),
        }
    }

    pub(crate) fn context(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Context(handler),
        }
    }

    pub(crate) fn produce(flexible_min: ApiVersion, handler: ProduceHandler) -> Self {
        Self {
            api_key: ApiKey::Produce as i16,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Produce(handler),
        }
    }

    pub(crate) fn telemetry(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: TelemetryHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Telemetry(handler),
        }
    }

    pub(crate) fn auth(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: AuthHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Auth(handler),
        }
    }

    pub(crate) fn fetch(flexible_min: ApiVersion) -> Self {
        Self {
            api_key: ApiKey::Fetch as i16,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Fetch,
        }
    }

    pub(crate) fn sasl_metadata(api_key: ApiKeyCode, flexible_min: ApiVersion) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::SaslMetadata,
        }
    }

    pub(crate) fn kind(self) -> DispatchKind {
        self.kind
    }

    pub(crate) fn quota_policy(self) -> RequestQuotaPolicy {
        self.quota_policy
    }

    pub(crate) fn body_flexible(self, version: ApiVersion) -> bool {
        self.flexible_min != i16::MAX && version >= self.flexible_min
    }

    #[cfg(test)]
    pub(crate) fn is_plain(self) -> bool {
        matches!(self.kind, DispatchKind::Plain(_))
    }
}

impl DispatchRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&mut self, entry: DispatchEntry) -> bool {
        self.table.insert(entry.api_key, entry).is_none()
    }

    pub(crate) fn get(&self, api_key: ApiKeyCode) -> Option<DispatchEntry> {
        self.table.get(&api_key).copied()
    }

    #[cfg(test)]
    pub(crate) fn get_plain(&self, api_key: ApiKeyCode) -> Option<PlainHandler> {
        match self.get(api_key)?.kind {
            DispatchKind::Plain(handler) => Some(handler),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn registered_api_keys(&self) -> impl Iterator<Item = ApiKeyCode> + '_ {
        self.table.keys().copied()
    }

    pub(crate) fn body_flexible(&self, api_key: ApiKeyCode, version: ApiVersion) -> bool {
        self.get(api_key)
            .is_some_and(|entry| entry.body_flexible(version))
    }
}

macro_rules! plain_dispatches {
    ($register_fn:ident; $(($api:ident, $request:ident, $handler:path)),+ $(,)?) => {
        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::plain(
                        ApiKey::$api as i16,
                        krabka_protocol::owned::$request::FLEXIBLE_MIN,
                        $handler,
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::$api
                );
            )+
        }
    };
}

plain_dispatches!(register_plain_dispatches;
    (ApiVersions, api_versions_request, crate::handlers::api_versions::handle),
    (AllocateProducerIds, allocate_producer_ids_request, crate::handlers::allocate_producer_ids::handle),
    (AddOffsetsToTxn, add_offsets_to_txn_request, crate::txn::handlers::add_offset_commits_to_txn::handle),
    (WriteTxnMarkers, write_txn_markers_request, crate::txn::handlers::write_txn_markers::handle),
    (FetchSnapshot, fetch_snapshot_request, crate::handlers::fetch_snapshot::handle),
    (AssignReplicasToDirs, assign_replicas_to_dirs_request, crate::handlers::assign_replicas_to_dirs::handle),
    (InitializeShareGroupState, initialize_share_group_state_request, crate::share_coordinator::handlers::initialize::handle),
    (ReadShareGroupState, read_share_group_state_request, crate::share_coordinator::handlers::read::handle),
    (WriteShareGroupState, write_share_group_state_request, crate::share_coordinator::handlers::write::handle),
    (DeleteShareGroupState, delete_share_group_state_request, crate::share_coordinator::handlers::delete::handle),
    (ReadShareGroupStateSummary, read_share_group_state_summary_request, crate::share_coordinator::handlers::read_summary::handle),
);

macro_rules! context_adapter {
    ($adapter:ident, $handler:expr) => {
        fn $adapter<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a RequestContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(($handler)(broker, version, correlation_id, body, ctx))
        }
    };
}

macro_rules! context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api:ident, $request:ident, $handler:path)),+ $(,)?) => {
        $(context_adapter!($adapter, $handler);)+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::context(
                        ApiKey::$api as i16,
                        krabka_protocol::owned::$request::FLEXIBLE_MIN,
                        $adapter,
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::$api
                );
            )+
        }
    };
}

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
        $(context_adapter!($adapter, $handler);)*

        fn $register_fn(registry: &mut DispatchRegistry) {
            let entries: &[(ApiKeyCode, ApiVersion, ContextHandler)] = &[
                $(($api_key, $flexible_min, $adapter as ContextHandler),)*
            ];
            for &(api_key, flexible_min, handler) in entries {
                assert!(
                    api_key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
                    "api_key {api_key} is below the krabka-private floor"
                );
                assert!(
                    registry.register(DispatchEntry::context(api_key, flexible_min, handler)),
                    "duplicate dispatch registration for api_key {api_key}"
                );
            }
        }
    };
}

macro_rules! sync_context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api:ident, $request:ident, $handler:path)),+ $(,)?) => {
        $(
            fn $adapter<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(std::future::ready($handler(
                    broker, version, correlation_id, body, ctx,
                )))
            }
        )+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::context(
                        ApiKey::$api as i16,
                        krabka_protocol::owned::$request::FLEXIBLE_MIN,
                        $adapter,
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::$api
                );
            )+
        }
    };
}

macro_rules! decoded_context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api:ident, $request_mod:ident, $request_ty:ident, $handler:path)),+ $(,)?) => {
        $(
            fn $adapter<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                _correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(async move {
                    use krabka_protocol::Decode;

                    let mut cur = body;
                    let req = krabka_protocol::owned::$request_mod::$request_ty::decode(
                        &mut cur, version,
                    )?;
                    $handler(broker, req, ctx, version).await
                })
            }
        )+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::context(
                        ApiKey::$api as i16,
                        krabka_protocol::owned::$request_mod::FLEXIBLE_MIN,
                        $adapter,
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::$api
                );
            )+
        }
    };
}

macro_rules! decoded_sync_context_dispatches {
    ($register_fn:ident; $(($adapter:ident, $api:ident, $request_mod:ident, $request_ty:ident, $handler:path)),+ $(,)?) => {
        $(
            fn $adapter<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                _correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(std::future::ready((|| {
                    use krabka_protocol::Decode;
                    let mut cur = body;
                    let req = krabka_protocol::owned::$request_mod::$request_ty::decode(
                        &mut cur, version,
                    )?;
                    $handler(broker, req, ctx, version)
                })()))
            }
        )+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::context(
                        ApiKey::$api as i16,
                        krabka_protocol::owned::$request_mod::FLEXIBLE_MIN,
                        $adapter,
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::$api
                );
            )+
        }
    };
}

macro_rules! telemetry_adapter {
    ($adapter:ident, $handler:expr) => {
        fn $adapter<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a TelemetryContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(std::future::ready(($handler)(
                broker,
                version,
                correlation_id,
                body,
                ctx,
            )))
        }
    };
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

decoded_context_dispatches!(register_decoded_context_dispatches;
    (create_acls_adapter, CreateAcls, create_acls_request, CreateAclsRequest, crate::handlers::create_acls::handle),
    (delete_acls_adapter, DeleteAcls, delete_acls_request, DeleteAclsRequest, crate::handlers::delete_acls::handle),
    (elect_leaders_adapter, ElectLeaders, elect_leaders_request, ElectLeadersRequest, crate::handlers::elect_leaders::handle),
    (alter_partition_reassignments_adapter, AlterPartitionReassignments, alter_partition_reassignments_request, AlterPartitionReassignmentsRequest, crate::handlers::alter_partition_reassignments::handle),
    (alter_client_quotas_adapter, AlterClientQuotas, alter_client_quotas_request, AlterClientQuotasRequest, crate::handlers::alter_client_quotas::handle),
);

decoded_sync_context_dispatches!(register_decoded_sync_context_dispatches;
    (describe_acls_adapter, DescribeAcls, describe_acls_request, DescribeAclsRequest, crate::handlers::describe_acls::handle),
    (list_partition_reassignments_adapter, ListPartitionReassignments, list_partition_reassignments_request, ListPartitionReassignmentsRequest, crate::handlers::list_partition_reassignments::handle),
    (describe_client_quotas_adapter, DescribeClientQuotas, describe_client_quotas_request, DescribeClientQuotasRequest, crate::handlers::describe_client_quotas::handle),
    (describe_user_scram_credentials_adapter, DescribeUserScramCredentials, describe_user_scram_credentials_request, DescribeUserScramCredentialsRequest, crate::handlers::describe_user_scram_credentials::handle),
);

fn alter_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, ctx).await;
        crate::handlers::encode_response(&resp, version)
    })
}

fn update_features_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
            &mut cur, version,
        )?;
        let resp = crate::handlers::update_features::handle(broker, req, version, ctx).await;
        crate::handlers::encode_response(&resp, version)
    })
}

fn alter_replica_log_dirs_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use std::collections::BTreeMap;

        use krabka_protocol::{
            Decode,
            owned::{
                alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
                alter_replica_log_dirs_response::{
                    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                    AlterReplicaLogDirsResponse,
                },
            },
        };

        let anonymous;
        let principal = if let Some(principal) = auth.principal() {
            principal
        } else {
            anonymous = krabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: krabka_security::AuthMethod::Anonymous,
                groups: vec![],
            };
            &anonymous
        };

        let image = broker.controller.current_image();
        let authorized = broker.config.authorizer.authorize(
            &*image,
            &crate::authorizer::AuthorizationRequest {
                principal,
                host: peer,
                resource_type: krabka_metadata::ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: krabka_metadata::AclOperation::Alter,
            },
        ) == crate::authorizer::AuthorizationResult::Allow;

        if !authorized {
            let mut cur = body;
            let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
            let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
                BTreeMap::new();
            for dir in req.dirs {
                for topic in dir.topics {
                    for partition_index in topic.partitions {
                        by_topic.entry(topic.name.clone()).or_default().push(
                            AlterReplicaLogDirPartitionResult {
                                partition_index,
                                error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
            let results = by_topic
                .into_iter()
                .map(|(topic_name, partitions)| AlterReplicaLogDirTopicResult {
                    topic_name,
                    partitions,
                    ..Default::default()
                })
                .collect();
            let resp = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            };
            return crate::handlers::encode_response(&resp, version);
        }

        crate::handlers::alter_replica_log_dirs::handle(broker, version, correlation_id, body).await
    })
}

fn create_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::create_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_max_lifetime.millis_i64(),
            broker
                .config
                .delegation_token_default_renew_period
                .millis_i64(),
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        crate::handlers::encode_response(&resp, version)
    })
}

fn renew_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::renew_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker
                .config
                .delegation_token_default_renew_period
                .millis_i64(),
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        crate::handlers::encode_response(&resp, version)
    })
}

fn expire_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::expire_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        crate::handlers::encode_response(&resp, version)
    })
}

fn describe_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::describe_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            peer,
            broker.config.authorizer.as_ref(),
        );
        crate::handlers::encode_response(&resp, version)
    })
}

context_dispatches!(register_context_dispatches;
    (metadata_adapter, Metadata, metadata_request, crate::handlers::metadata::handle),
    (describe_cluster_adapter, DescribeCluster, describe_cluster_request, crate::handlers::describe_cluster::handle),
    (create_topics_adapter, CreateTopics, create_topics_request, crate::handlers::create_topics::handle),
    (delete_topics_adapter, DeleteTopics, delete_topics_request, crate::handlers::delete_topics::handle),
    (alter_configs_adapter, AlterConfigs, alter_configs_request, crate::handlers::alter_configs::handle),
    (incremental_alter_configs_adapter, IncrementalAlterConfigs, incremental_alter_configs_request, crate::handlers::incremental_alter_configs::handle),
    (delete_records_adapter, DeleteRecords, delete_records_request, crate::handlers::delete_records::handle),
    (create_partitions_adapter, CreatePartitions, create_partitions_request, crate::handlers::create_partitions::handle),
    (describe_groups_adapter, DescribeGroups, describe_groups_request, crate::handlers::describe_groups::handle),
    (list_groups_adapter, ListGroups, list_groups_request, crate::handlers::list_groups::handle),
    (share_group_describe_adapter, ShareGroupDescribe, share_group_describe_request, crate::handlers::share_group_describe::handle),
    (share_fetch_adapter, ShareFetch, share_fetch_request, crate::handlers::share_fetch::handle),
    (share_acknowledge_adapter, ShareAcknowledge, share_acknowledge_request, crate::handlers::share_acknowledge::handle),
    (describe_share_group_offsets_adapter, DescribeShareGroupOffsets, describe_share_group_offsets_request, crate::handlers::describe_share_group_offsets::handle),
    (alter_share_group_offsets_adapter, AlterShareGroupOffsets, alter_share_group_offsets_request, crate::handlers::alter_share_group_offsets::handle),
    (delete_share_group_offsets_adapter, DeleteShareGroupOffsets, delete_share_group_offsets_request, crate::handlers::delete_share_group_offsets::handle),
    (delete_groups_adapter, DeleteGroups, delete_groups_request, crate::handlers::delete_groups::handle),
    (join_group_adapter, JoinGroup, join_group_request, crate::handlers::join_group::handle),
    (offset_commit_adapter, OffsetCommit, offset_commit_request, crate::handlers::offset_commit::handle),
    (offset_fetch_adapter, OffsetFetch, offset_fetch_request, crate::handlers::offset_fetch::handle),
    (offset_delete_adapter, OffsetDelete, offset_delete_request, crate::handlers::offset_delete::handle),
    (describe_producers_adapter, DescribeProducers, describe_producers_request, crate::handlers::describe_producers::handle),
    (describe_transactions_adapter, DescribeTransactions, describe_transactions_request, crate::handlers::describe_transactions::handle),
    (list_transactions_adapter, ListTransactions, list_transactions_request, crate::handlers::list_transactions::handle),
    (unregister_broker_adapter, UnregisterBroker, unregister_broker_request, crate::handlers::unregister_broker::handle),
    (add_raft_voter_adapter, AddRaftVoter, add_raft_voter_request, crate::handlers::add_raft_voter::handle),
    (remove_raft_voter_adapter, RemoveRaftVoter, remove_raft_voter_request, crate::handlers::remove_raft_voter::handle),
    (update_raft_voter_adapter, UpdateRaftVoter, update_raft_voter_request, crate::handlers::update_raft_voter::handle),
    (alter_partition_adapter, AlterPartition, alter_partition_request, crate::handlers::alter_partition::handle),
    (broker_heartbeat_adapter, BrokerHeartbeat, broker_heartbeat_request, crate::handlers::broker_heartbeat::handle),
    (broker_registration_adapter, BrokerRegistration, broker_registration_request, crate::handlers::broker_registration::handle),
    (controller_registration_adapter, ControllerRegistration, controller_registration_request, crate::handlers::controller_registration::handle),
    (heartbeat_adapter, Heartbeat, heartbeat_request, crate::handlers::heartbeat::handle),
    (sync_group_adapter, SyncGroup, sync_group_request, crate::handlers::sync_group::handle),
    (leave_group_adapter, LeaveGroup, leave_group_request, crate::handlers::leave_group::handle),
    (consumer_group_heartbeat_adapter, ConsumerGroupHeartbeat, consumer_group_heartbeat_request, crate::handlers::consumer_group_heartbeat::handle),
    (share_group_heartbeat_adapter, ShareGroupHeartbeat, share_group_heartbeat_request, crate::handlers::share_group_heartbeat::handle),
    (streams_group_heartbeat_adapter, StreamsGroupHeartbeat, streams_group_heartbeat_request, crate::handlers::streams_group_heartbeat::handle),
    (consumer_group_describe_adapter, ConsumerGroupDescribe, consumer_group_describe_request, crate::handlers::consumer_group_describe::handle),
    (streams_group_describe_adapter, StreamsGroupDescribe, streams_group_describe_request, crate::handlers::streams_group_describe::handle),
    (find_coordinator_adapter, FindCoordinator, find_coordinator_request, crate::handlers::find_coordinator::handle),
    (list_offsets_adapter, ListOffsets, list_offsets_request, crate::handlers::list_offsets::handle),
    (describe_log_dirs_adapter, DescribeLogDirs, describe_log_dirs_request, crate::handlers::describe_log_dirs::handle),
    (init_producer_id_adapter, InitProducerId, init_producer_id_request, crate::handlers::init_producer_id::handle),
    (add_partitions_to_txn_adapter, AddPartitionsToTxn, add_partitions_to_txn_request, crate::txn::handlers::add_partitions_to_txn::handle),
    (end_txn_adapter, EndTxn, end_txn_request, crate::txn::handlers::end_txn::handle),
    (txn_offset_commit_adapter, TxnOffsetCommit, txn_offset_commit_request, crate::txn::handlers::txn_offset_commit::handle),
);

sync_context_dispatches!(register_sync_context_dispatches;
    (describe_topic_partitions_adapter, DescribeTopicPartitions, describe_topic_partitions_request, crate::handlers::describe_topic_partitions::handle),
    (list_config_resources_adapter, ListConfigResources, list_config_resources_request, crate::handlers::list_config_resources::handle),
    (describe_quorum_adapter, DescribeQuorum, describe_quorum_request, crate::handlers::describe_quorum::handle),
    (get_replica_log_info_adapter, GetReplicaLogInfo, get_replica_log_info_request, crate::handlers::get_replica_log_info::handle),
    (offset_for_leader_epoch_adapter, OffsetForLeaderEpoch, offset_for_leader_epoch_request, crate::handlers::offset_for_leader_epoch::handle),
    (describe_configs_adapter, DescribeConfigs, describe_configs_request, crate::handlers::describe_configs::handle),
);

telemetry_adapter!(
    get_telemetry_subscriptions_adapter,
    crate::handlers::get_telemetry_subscriptions::handle
);
telemetry_adapter!(
    push_telemetry_adapter,
    crate::handlers::push_telemetry::handle
);

// `KRABKA_PRIVATE_API_KEY_FLOOR` for why.
// `flexible_min` comes from the codec of each message, so the framing the
// registry reports and the framing the codec writes cannot drift apart.
krabka_private_context_dispatches!(register_krabka_private_context_dispatches;
    (
        alter_barrier_groups_adapter,
        crate::handlers::ALTER_BARRIER_GROUPS_API_KEY,
        krabka_protocol::krabka::barrier::alter_barrier_groups::FLEXIBLE_MIN,
        crate::barrier::handlers::alter_groups::handle
    ),
    (
        describe_barrier_groups_adapter,
        crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
        krabka_protocol::krabka::barrier::describe_barrier_groups::FLEXIBLE_MIN,
        crate::barrier::handlers::describe_groups::handle
    ),
    (
        trigger_barrier_adapter,
        crate::handlers::TRIGGER_BARRIER_API_KEY,
        krabka_protocol::krabka::barrier::trigger_barrier::FLEXIBLE_MIN,
        crate::barrier::handlers::trigger::handle
    ),
    (
        list_barrier_cuts_adapter,
        crate::handlers::LIST_BARRIER_CUTS_API_KEY,
        krabka_protocol::krabka::barrier::list_barrier_cuts::FLEXIBLE_MIN,
        crate::barrier::handlers::list_cuts::handle
    ),
    (
        write_barrier_markers_adapter,
        crate::handlers::WRITE_BARRIER_MARKERS_API_KEY,
        krabka_protocol::krabka::barrier::write_barrier_markers::FLEXIBLE_MIN,
        crate::barrier::handlers::write_markers::handle
    ),
    (
        set_topic_freeze_adapter,
        crate::handlers::SET_TOPIC_FREEZE_API_KEY,
        crabka_protocol::krabka::freeze::set_topic_freeze::FLEXIBLE_MIN,
        crate::freeze::handlers::set_freeze::handle
    ),
    (
        describe_topic_freezes_adapter,
        crate::handlers::DESCRIBE_TOPIC_FREEZES_API_KEY,
        crabka_protocol::krabka::freeze::describe_topic_freezes::FLEXIBLE_MIN,
        crate::freeze::handlers::describe_freezes::handle
    ),
    (
        propose_break_glass_adapter,
        crate::handlers::PROPOSE_BREAK_GLASS_API_KEY,
        crabka_protocol::krabka::break_glass::propose::FLEXIBLE_MIN,
        crate::break_glass::handlers::propose::handle
    ),
    (
        approve_break_glass_adapter,
        crate::handlers::APPROVE_BREAK_GLASS_API_KEY,
        crabka_protocol::krabka::break_glass::approve::FLEXIBLE_MIN,
        crate::break_glass::handlers::approve::handle
    ),
    (
        describe_break_glass_adapter,
        crate::handlers::DESCRIBE_BREAK_GLASS_API_KEY,
        crabka_protocol::krabka::break_glass::describe::FLEXIBLE_MIN,
        crate::break_glass::handlers::describe::handle
    ),
);

pub(crate) fn build_registry() -> DispatchRegistry {
    let mut registry = DispatchRegistry::new();

    register_plain_dispatches(&mut registry);

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
    register_context_dispatches(&mut registry);
    register_sync_context_dispatches(&mut registry);
    register_krabka_private_context_dispatches(&mut registry);
    register_decoded_context_dispatches(&mut registry);
    register_decoded_sync_context_dispatches(&mut registry);
    registry.register(DispatchEntry::context(
        ApiKey::AlterUserScramCredentials as i16,
        krabka_protocol::owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        alter_user_scram_credentials_adapter,
    ));
    registry.register(DispatchEntry::context(
        ApiKey::UpdateFeatures as i16,
        krabka_protocol::owned::update_features_request::FLEXIBLE_MIN,
        update_features_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::AlterReplicaLogDirs as i16,
        krabka_protocol::owned::alter_replica_log_dirs_request::FLEXIBLE_MIN,
        alter_replica_log_dirs_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::CreateDelegationToken as i16,
        krabka_protocol::owned::create_delegation_token_request::FLEXIBLE_MIN,
        create_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::RenewDelegationToken as i16,
        krabka_protocol::owned::renew_delegation_token_request::FLEXIBLE_MIN,
        renew_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::ExpireDelegationToken as i16,
        krabka_protocol::owned::expire_delegation_token_request::FLEXIBLE_MIN,
        expire_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::DescribeDelegationToken as i16,
        krabka_protocol::owned::describe_delegation_token_request::FLEXIBLE_MIN,
        describe_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::telemetry(
        ApiKey::GetTelemetrySubscriptions as i16,
        krabka_protocol::owned::get_telemetry_subscriptions_request::FLEXIBLE_MIN,
        get_telemetry_subscriptions_adapter,
    ));
    registry.register(DispatchEntry::telemetry(
        ApiKey::PushTelemetry as i16,
        krabka_protocol::owned::push_telemetry_request::FLEXIBLE_MIN,
        push_telemetry_adapter,
    ));

    registry
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::{assert, check};

    use super::*;
    use crate::handlers;

    #[test]
    fn registry_registers_plain_handlers() {
        let registry = build_registry();

        let api_versions = registry
            .get(ApiKey::ApiVersions as i16)
            .expect("ApiVersions");
        assert!(api_versions.is_plain());
        assert!(api_versions.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting);
        assert!(api_versions.body_flexible(3));
        assert!(!api_versions.body_flexible(2));

        for key in [25, 27, 59, 73, 83, 84, 85, 86, 87] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(entry.is_plain(), "api_key {key}");
        }
    }

    #[test]
    fn registry_registers_raw_context_handlers() {
        let registry = build_registry();

        for api_key in [
            ApiKey::Produce as i16,
            ApiKey::Metadata as i16,
            ApiKey::OffsetCommit as i16,
            ApiKey::OffsetFetch as i16,
            ApiKey::FindCoordinator as i16,
            ApiKey::JoinGroup as i16,
            ApiKey::Heartbeat as i16,
            ApiKey::LeaveGroup as i16,
            ApiKey::SyncGroup as i16,
            ApiKey::DeleteGroups as i16,
            ApiKey::ListOffsets as i16,
            ApiKey::OffsetForLeaderEpoch as i16,
            ApiKey::CreateTopics as i16,
            ApiKey::DeleteTopics as i16,
            ApiKey::AlterConfigs as i16,
            ApiKey::IncrementalAlterConfigs as i16,
            ApiKey::DeleteRecords as i16,
            ApiKey::CreatePartitions as i16,
            ApiKey::DescribeGroups as i16,
            ApiKey::ListGroups as i16,
            ApiKey::OffsetDelete as i16,
            ApiKey::DescribeCluster as i16,
            ApiKey::DescribeProducers as i16,
            ApiKey::DescribeTransactions as i16,
            ApiKey::ListTransactions as i16,
            ApiKey::UnregisterBroker as i16,
            ApiKey::DescribeTopicPartitions as i16,
            ApiKey::ListConfigResources as i16,
            ApiKey::DescribeQuorum as i16,
            ApiKey::AddRaftVoter as i16,
            ApiKey::RemoveRaftVoter as i16,
            ApiKey::UpdateRaftVoter as i16,
            ApiKey::AlterPartition as i16,
            ApiKey::BrokerHeartbeat as i16,
            ApiKey::GetReplicaLogInfo as i16,
            ApiKey::ConsumerGroupHeartbeat as i16,
            ApiKey::ConsumerGroupDescribe as i16,
            ApiKey::ShareGroupDescribe as i16,
            ApiKey::ShareFetch as i16,
            ApiKey::ShareAcknowledge as i16,
            ApiKey::ShareGroupHeartbeat as i16,
            ApiKey::StreamsGroupHeartbeat as i16,
            ApiKey::StreamsGroupDescribe as i16,
            ApiKey::DescribeShareGroupOffsets as i16,
            ApiKey::AlterShareGroupOffsets as i16,
            ApiKey::DeleteShareGroupOffsets as i16,
            ApiKey::InitProducerId as i16,
            ApiKey::AddPartitionsToTxn as i16,
            ApiKey::EndTxn as i16,
            ApiKey::TxnOffsetCommit as i16,
        ] {
            let key = api_key;
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(
                    entry.kind(),
                    DispatchKind::Context(_) | DispatchKind::Produce(_)
                ),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_registers_telemetry_handlers() {
        let registry = build_registry();

        for key in [71, 72] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(entry.kind(), DispatchKind::Telemetry(_)),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_registers_decoded_context_handlers() {
        let registry = build_registry();

        for api_key in [
            ApiKey::DescribeAcls as i16,
            ApiKey::CreateAcls as i16,
            ApiKey::DeleteAcls as i16,
            ApiKey::ElectLeaders as i16,
            ApiKey::AlterPartitionReassignments as i16,
            ApiKey::ListPartitionReassignments as i16,
            ApiKey::DescribeClientQuotas as i16,
            ApiKey::AlterClientQuotas as i16,
            ApiKey::DescribeUserScramCredentials as i16,
            ApiKey::AlterUserScramCredentials as i16,
            ApiKey::UpdateFeatures as i16,
        ] {
            let key = api_key;
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(entry.kind(), DispatchKind::Context(_)),
                "api_key {key}"
            );
        }
    }

    #[test]
    fn registry_registers_auth_handlers() {
        let registry = build_registry();

        for key in [34, 38, 39, 40, 41] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(entry.kind(), DispatchKind::Auth(_)),
                "api_key {key}"
            );
        }
    }

    /// Every krabka-private api key, the adapter that dispatch must reach, and
    /// the label a failure reports.
    ///
    /// The five barrier keys and the five KFC-9 keys hold the same contract, so
    /// one table covers both. An entry here fails when a key loses its
    /// registration, when it reaches the wrong handler, or when it starts being
    /// advertised.
    fn krabka_private_dispatches() -> [(&'static str, ApiKeyCode, ContextHandler); 10] {
        [
            (
                "AlterBarrierGroups",
                handlers::ALTER_BARRIER_GROUPS_API_KEY,
                alter_barrier_groups_adapter,
            ),
            (
                "DescribeBarrierGroups",
                handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
                describe_barrier_groups_adapter,
            ),
            (
                "TriggerBarrier",
                handlers::TRIGGER_BARRIER_API_KEY,
                trigger_barrier_adapter,
            ),
            (
                "ListBarrierCuts",
                handlers::LIST_BARRIER_CUTS_API_KEY,
                list_barrier_cuts_adapter,
            ),
            (
                "WriteBarrierMarkers",
                handlers::WRITE_BARRIER_MARKERS_API_KEY,
                write_barrier_markers_adapter,
            ),
            (
                "SetTopicFreeze",
                handlers::SET_TOPIC_FREEZE_API_KEY,
                set_topic_freeze_adapter,
            ),
            (
                "DescribeTopicFreezes",
                handlers::DESCRIBE_TOPIC_FREEZES_API_KEY,
                describe_topic_freezes_adapter,
            ),
            (
                "ProposeBreakGlass",
                handlers::PROPOSE_BREAK_GLASS_API_KEY,
                propose_break_glass_adapter,
            ),
            (
                "ApproveBreakGlass",
                handlers::APPROVE_BREAK_GLASS_API_KEY,
                approve_break_glass_adapter,
            ),
            (
                "DescribeBreakGlass",
                handlers::DESCRIBE_BREAK_GLASS_API_KEY,
                describe_break_glass_adapter,
            ),
        ]
    }

    #[test]
    fn registry_dispatches_every_krabka_private_key_to_its_own_handler() {
        let registry = build_registry();

        for (label, api_key, adapter) in krabka_private_dispatches() {
            let entry = registry
                .get(api_key)
                .unwrap_or_else(|| panic!("{label} ({api_key}) is registered"));

            assert!(let DispatchKind::Context(handler) = entry.kind(), "{label}");
            check!(std::ptr::fn_addr_eq(handler, adapter), "{label}");
            // Version 0 only, flexible framing, and exempt from the
            // request-quota accounting a Kafka client drives.
            check!(entry.body_flexible(0), "{label}");
            check!(
                entry.quota_policy() == RequestQuotaPolicy::InlineExempt,
                "{label}"
            );
        }
    }

    #[test]
    fn no_krabka_private_key_reaches_api_versions() {
        let advertised: BTreeSet<ApiKeyCode> = crate::api_catalog::supported_apis()
            .into_iter()
            .map(|api| api.api_key)
            .collect();

        for (label, api_key, _) in krabka_private_dispatches() {
            check!(api_key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR, "{label}");
            check!(!advertised.contains(&api_key), "{label}");
        }
    }

    #[test]
    fn registry_reports_missing_keys() {
        let registry = build_registry();

        assert!(registry.get(9999).is_none());
    }

    #[test]
    fn registry_and_api_catalog_cover_the_same_kafka_api_keys() {
        let registry = build_registry();
        let registered: BTreeSet<ApiKeyCode> = registry.registered_api_keys().collect();
        let advertised: BTreeSet<ApiKeyCode> = crate::api_catalog::supported_apis()
            .into_iter()
            .map(|api| api.api_key)
            .collect();

        let floor = crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR;
        let registered_kafka: BTreeSet<ApiKeyCode> = registered
            .iter()
            .copied()
            .filter(|key| *key < floor)
            .collect();

        // Every advertised key is registered, and every registered Kafka key is
        // advertised. The krabka-private keys are deliberately absent from the
        // catalog: advertising them would put UNKNOWN(1010) rows into
        // kafka-broker-api-versions output, a visible divergence from a real
        // broker, and a client that does not find a key negotiates (0, 0),
        // which is right for a MIN = MAX = 0 request.
        assert!(advertised.is_subset(&registered));
        assert!(registered_kafka == advertised);
        assert!(advertised.iter().all(|key| *key < floor));
    }

    #[test]
    fn registry_body_flexible_matches_selected_schema_boundaries() {
        use krabka_protocol::owned;

        let registry = build_registry();
        let cases = [
            (0, owned::produce_request::FLEXIBLE_MIN - 1, false),
            (0, owned::produce_request::FLEXIBLE_MIN, true),
            (1, owned::fetch_request::FLEXIBLE_MIN - 1, false),
            (1, owned::fetch_request::FLEXIBLE_MIN, true),
            (
                36,
                owned::sasl_authenticate_request::FLEXIBLE_MIN - 1,
                false,
            ),
            (36, owned::sasl_authenticate_request::FLEXIBLE_MIN, true),
            (17, i16::MAX, false),
            (999, 0, false),
        ];

        for (api_key, version, want) in cases {
            assert!(
                registry.body_flexible(api_key, version) == want,
                "api_key {api_key} version {version}"
            );
        }
    }

    #[test]
    fn plain_handler_pointer_matches_existing_api_versions_handler() {
        let registry = build_registry();
        let handler = registry
            .get_plain(ApiKey::ApiVersions as i16)
            .expect("plain ApiVersions handler");

        assert!(std::ptr::fn_addr_eq(
            handler,
            handlers::api_versions::handle as PlainHandler
        ));
    }
}
