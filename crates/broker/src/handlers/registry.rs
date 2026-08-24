//! Broker API dispatch registry.

use bytes::Bytes;
use crabka_protocol::api_key::ApiKey;
use crabka_units::convert::TimeExt as _;
use futures_util::future::BoxFuture;

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
    DecodedContext(ContextHandler),
    EncodedContext(ContextHandler),
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

    pub(crate) fn decoded_context(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::DecodedContext(handler),
        }
    }

    pub(crate) fn encoded_context(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::EncodedContext(handler),
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
                        crabka_protocol::owned::$request::FLEXIBLE_MIN,
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
                        crabka_protocol::owned::$request::FLEXIBLE_MIN,
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
                        crabka_protocol::owned::$request::FLEXIBLE_MIN,
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
                    use crabka_protocol::Decode;

                    let mut cur = body;
                    let req = crabka_protocol::owned::$request_mod::$request_ty::decode(
                        &mut cur, version,
                    )?;
                    $handler(broker, req, ctx, version).await
                })
            }
        )+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::decoded_context(
                        ApiKey::$api as i16,
                        crabka_protocol::owned::$request_mod::FLEXIBLE_MIN,
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
                    use crabka_protocol::Decode;
                    let mut cur = body;
                    let req = crabka_protocol::owned::$request_mod::$request_ty::decode(
                        &mut cur, version,
                    )?;
                    $handler(broker, req, ctx, version)
                })()))
            }
        )+

        fn $register_fn(registry: &mut DispatchRegistry) {
            $(
                assert!(
                    registry.register(DispatchEntry::decoded_context(
                        ApiKey::$api as i16,
                        crabka_protocol::owned::$request_mod::FLEXIBLE_MIN,
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
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

        use crabka_protocol::{
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
            anonymous = crabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: crabka_security::AuthMethod::Anonymous,
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
                resource_type: crabka_metadata::ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: crabka_metadata::AclOperation::Alter,
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest::decode(
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest::decode(
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest::decode(
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
        use crabka_protocol::Decode;

        let mut cur = body;
        let req = crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::decode(
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

// The barrier control plane registers its five RPCs here, at api keys 1010 to
// 1014. They stay out of `api_catalog::supported_apis`; see
// `KRABKA_PRIVATE_API_KEY_FLOOR` for why.
krabka_private_context_dispatches!(register_krabka_private_context_dispatches;);

telemetry_adapter!(
    get_telemetry_subscriptions_adapter,
    crate::handlers::get_telemetry_subscriptions::handle
);
telemetry_adapter!(
    push_telemetry_adapter,
    crate::handlers::push_telemetry::handle
);

pub(crate) fn build_registry() -> DispatchRegistry {
    let mut registry = DispatchRegistry::new();

    register_plain_dispatches(&mut registry);

    registry.register(DispatchEntry::produce(
        crabka_protocol::owned::produce_request::FLEXIBLE_MIN,
        produce_adapter,
    ));
    registry.register(DispatchEntry::fetch(
        crabka_protocol::owned::fetch_request::FLEXIBLE_MIN,
    ));
    registry.register(DispatchEntry::sasl_metadata(
        ApiKey::SaslHandshake as i16,
        i16::MAX,
    ));
    registry.register(DispatchEntry::sasl_metadata(
        ApiKey::SaslAuthenticate as i16,
        crabka_protocol::owned::sasl_authenticate_request::FLEXIBLE_MIN,
    ));
    register_context_dispatches(&mut registry);
    register_sync_context_dispatches(&mut registry);
    register_krabka_private_context_dispatches(&mut registry);
    register_decoded_context_dispatches(&mut registry);
    register_decoded_sync_context_dispatches(&mut registry);
    registry.register(DispatchEntry::encoded_context(
        ApiKey::AlterUserScramCredentials as i16,
        crabka_protocol::owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        alter_user_scram_credentials_adapter,
    ));
    registry.register(DispatchEntry::encoded_context(
        ApiKey::UpdateFeatures as i16,
        crabka_protocol::owned::update_features_request::FLEXIBLE_MIN,
        update_features_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::AlterReplicaLogDirs as i16,
        crabka_protocol::owned::alter_replica_log_dirs_request::FLEXIBLE_MIN,
        alter_replica_log_dirs_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::CreateDelegationToken as i16,
        crabka_protocol::owned::create_delegation_token_request::FLEXIBLE_MIN,
        create_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::RenewDelegationToken as i16,
        crabka_protocol::owned::renew_delegation_token_request::FLEXIBLE_MIN,
        renew_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::ExpireDelegationToken as i16,
        crabka_protocol::owned::expire_delegation_token_request::FLEXIBLE_MIN,
        expire_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::auth(
        ApiKey::DescribeDelegationToken as i16,
        crabka_protocol::owned::describe_delegation_token_request::FLEXIBLE_MIN,
        describe_delegation_token_adapter,
    ));
    registry.register(DispatchEntry::telemetry(
        ApiKey::GetTelemetrySubscriptions as i16,
        crabka_protocol::owned::get_telemetry_subscriptions_request::FLEXIBLE_MIN,
        get_telemetry_subscriptions_adapter,
    ));
    registry.register(DispatchEntry::telemetry(
        ApiKey::PushTelemetry as i16,
        crabka_protocol::owned::push_telemetry_request::FLEXIBLE_MIN,
        push_telemetry_adapter,
    ));

    registry
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

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
            ApiKey::Produce,
            ApiKey::Metadata,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::SyncGroup,
            ApiKey::DeleteGroups,
            ApiKey::ListOffsets,
            ApiKey::OffsetForLeaderEpoch,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
            ApiKey::AlterConfigs,
            ApiKey::IncrementalAlterConfigs,
            ApiKey::DeleteRecords,
            ApiKey::CreatePartitions,
            ApiKey::DescribeGroups,
            ApiKey::ListGroups,
            ApiKey::OffsetDelete,
            ApiKey::DescribeCluster,
            ApiKey::DescribeProducers,
            ApiKey::DescribeTransactions,
            ApiKey::ListTransactions,
            ApiKey::UnregisterBroker,
            ApiKey::DescribeTopicPartitions,
            ApiKey::ListConfigResources,
            ApiKey::DescribeQuorum,
            ApiKey::AddRaftVoter,
            ApiKey::RemoveRaftVoter,
            ApiKey::UpdateRaftVoter,
            ApiKey::AlterPartition,
            ApiKey::BrokerHeartbeat,
            ApiKey::GetReplicaLogInfo,
            ApiKey::ConsumerGroupHeartbeat,
            ApiKey::ConsumerGroupDescribe,
            ApiKey::ShareGroupDescribe,
            ApiKey::ShareFetch,
            ApiKey::ShareAcknowledge,
            ApiKey::ShareGroupHeartbeat,
            ApiKey::StreamsGroupHeartbeat,
            ApiKey::StreamsGroupDescribe,
            ApiKey::DescribeShareGroupOffsets,
            ApiKey::AlterShareGroupOffsets,
            ApiKey::DeleteShareGroupOffsets,
            ApiKey::InitProducerId,
            ApiKey::AddPartitionsToTxn,
            ApiKey::EndTxn,
            ApiKey::TxnOffsetCommit,
        ] {
            let key = api_key as i16;
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
            ApiKey::DescribeAcls,
            ApiKey::CreateAcls,
            ApiKey::DeleteAcls,
            ApiKey::ElectLeaders,
            ApiKey::AlterPartitionReassignments,
            ApiKey::ListPartitionReassignments,
            ApiKey::DescribeClientQuotas,
            ApiKey::AlterClientQuotas,
            ApiKey::DescribeUserScramCredentials,
            ApiKey::AlterUserScramCredentials,
            ApiKey::UpdateFeatures,
        ] {
            let key = api_key as i16;
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(
                matches!(
                    entry.kind(),
                    DispatchKind::DecodedContext(_) | DispatchKind::EncodedContext(_)
                ),
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

    #[test]
    fn registry_reports_missing_keys() {
        let registry = build_registry();

        assert!(registry.get(9999).is_none());
    }

    fn krabka_private_test_adapter<'a>(
        _broker: &'a Broker,
        _version: ApiVersion,
        _correlation_id: CorrelationId,
        _body: &'a [u8],
        _ctx: &'a RequestContext<'a>,
    ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
        Box::pin(std::future::ready(Ok(Bytes::new())))
    }

    #[test]
    fn registry_and_api_catalog_cover_the_same_kafka_api_keys() {
        let registry = build_registry();
        let registered: BTreeSet<ApiKeyCode> = registry.registered_api_keys().collect();
        let advertised: BTreeSet<ApiKeyCode> = crate::api_catalog::supported_apis()
            .into_iter()
            .map(|api| api.api_key)
            .collect();
        let empty = BTreeSet::new();

        // Every advertised api key has a handler. A client that negotiates a
        // version and then gets UNSUPPORTED_VERSION is the failure this bars.
        let advertised_without_handler: BTreeSet<ApiKeyCode> =
            advertised.difference(&registered).copied().collect();
        assert!(advertised_without_handler == empty);

        // Every registered Kafka api key is advertised.
        let kafka_registered: BTreeSet<ApiKeyCode> = registered
            .iter()
            .copied()
            .filter(|key| *key < handlers::KRABKA_PRIVATE_API_KEY_FLOOR)
            .collect();
        let kafka_registered_without_advertisement: BTreeSet<ApiKeyCode> =
            kafka_registered.difference(&advertised).copied().collect();
        assert!(kafka_registered_without_advertisement == empty);

        // A krabka-private api key is registered but never advertised. An
        // advertised row would print as UNKNOWN(1010) in
        // kafka-broker-api-versions.sh output, and a real Kafka broker prints
        // no such row.
        let advertised_krabka_private: BTreeSet<ApiKeyCode> = advertised
            .iter()
            .copied()
            .filter(|key| *key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR)
            .collect();
        assert!(advertised_krabka_private == empty);
    }

    #[test]
    fn krabka_private_api_key_registers_and_frames_as_flexible() {
        // The generated enum does not know a krabka-private key, so the
        // registry entry is the only source of the flexibility that
        // `network::dispatch` reads through `body_flexible`.
        assert!(ApiKey::from_i16(handlers::TRIGGER_BARRIER_API_KEY).is_none());

        let mut registry = DispatchRegistry::new();
        assert!(registry.register(DispatchEntry::context(
            handlers::TRIGGER_BARRIER_API_KEY,
            0,
            krabka_private_test_adapter,
        )));

        let entry = registry
            .get(handlers::TRIGGER_BARRIER_API_KEY)
            .expect("registered krabka-private entry");
        assert!(matches!(entry.kind(), DispatchKind::Context(_)));
        assert!(entry.quota_policy() == RequestQuotaPolicy::InlineExempt);
        assert!(registry.body_flexible(handlers::TRIGGER_BARRIER_API_KEY, 0));
    }

    #[test]
    fn every_barrier_api_key_sits_in_the_krabka_private_range() {
        let barrier_keys = [
            handlers::ALTER_BARRIER_GROUPS_API_KEY,
            handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
            handlers::TRIGGER_BARRIER_API_KEY,
            handlers::LIST_BARRIER_CUTS_API_KEY,
            handlers::WRITE_BARRIER_MARKERS_API_KEY,
        ];

        assert!(barrier_keys == [1010, 1011, 1012, 1013, 1014]);
        for key in barrier_keys {
            assert!(
                key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
                "api_key {key}"
            );
            assert!(ApiKey::from_i16(key).is_none(), "api_key {key}");
        }
    }

    #[test]
    fn registry_body_flexible_matches_selected_schema_boundaries() {
        use crabka_protocol::owned;

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
