//! Broker API dispatch registry.
//!
//! The root holds the handler-signature aliases, the `macro_rules!` generators
//! that turn a table of api keys into a registration function, and
//! [`build_registry`], which assembles the whole table. The macros stay in this
//! file because `macro_rules!` scope is textual: a child module sees a macro
//! only when the definition comes before the `mod` declaration that pulls the
//! child in.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_protocol::api_key::ApiKey;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, RequestContext, TelemetryContext},
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

macro_rules! plain_dispatches {
    ($register_fn:ident; $(($api:ident, $request:ident, $handler:path)),+ $(,)?) => {
        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
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

        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
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

        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
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

        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
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

        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
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

        pub(super) fn $register_fn(registry: &mut DispatchRegistry) {
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
        pub(super) fn $adapter<'a>(
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

mod auth;
mod context;
mod decoded;
mod entry;
mod krabka_private;
mod plain;
mod telemetry;
#[cfg(test)]
mod tests;

pub(crate) use self::entry::{DispatchEntry, DispatchKind, DispatchRegistry, RequestQuotaPolicy};
use self::{
    auth::{
        alter_replica_log_dirs_adapter, create_delegation_token_adapter,
        describe_delegation_token_adapter, expire_delegation_token_adapter,
        renew_delegation_token_adapter,
    },
    context::{register_context_dispatches, register_sync_context_dispatches},
    decoded::{
        alter_user_scram_credentials_adapter, register_decoded_context_dispatches,
        register_decoded_sync_context_dispatches, update_features_adapter,
    },
    krabka_private::register_krabka_private_context_dispatches,
    plain::register_plain_dispatches,
    telemetry::{get_telemetry_subscriptions_adapter, push_telemetry_adapter},
};

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
