//! What `dispatch_table!` generates, driven through stand-ins for the broker
//! types it names: every section registers its entry under the right kind,
//! api key and `FLEXIBLE_MIN`, and each adapter hands its handler what that
//! section promises.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use assert2::assert;

type ApiVersion = i16;
type CorrelationId = i32;
type Bytes = Vec<u8>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

struct Broker {
    name: &'static str,
}

struct RequestContext<'a> {
    client_id: &'a str,
}

struct TelemetryContext<'a> {
    client_instance: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BrokerError {
    Decode,
    EmptyBody,
    /// `decode_group_request` refused a record string over its bound.
    RecordString,
    /// `encode_response` refused the response.
    Encode,
}

impl From<krabka_protocol::DecodeError> for BrokerError {
    fn from(_: krabka_protocol::DecodeError) -> Self {
        Self::Decode
    }
}

#[derive(Debug, Clone, Copy)]
enum ApiKey {
    Metadata = 3,
    DescribeConfigs = 32,
    CreateAcls = 30,
    DescribeAcls = 29,
    AddPartitionsToTxn = 24,
    CreateDelegationToken = 38,
    PushTelemetry = 72,
    ListGroups = 16,
    Heartbeat = 12,
    ListConfigResources = 74,
}

type ContextHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

type TelemetryHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a TelemetryContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

type AuthHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a str,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

#[derive(Clone, Copy)]
enum Handler {
    Context(ContextHandler),
    Telemetry(TelemetryHandler),
    Auth(AuthHandler),
}

#[derive(Clone, Copy)]
struct DispatchEntry {
    api_key: i16,
    flexible_min: ApiVersion,
    handler: Handler,
}

impl DispatchEntry {
    fn context(api_key: i16, flexible_min: ApiVersion, handler: ContextHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            handler: Handler::Context(handler),
        }
    }

    fn telemetry(api_key: i16, flexible_min: ApiVersion, handler: TelemetryHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            handler: Handler::Telemetry(handler),
        }
    }

    fn auth(api_key: i16, flexible_min: ApiVersion, handler: AuthHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            handler: Handler::Auth(handler),
        }
    }
}

#[derive(Default)]
struct DispatchRegistry(BTreeMap<i16, DispatchEntry>);

impl DispatchRegistry {
    fn register(&mut self, entry: DispatchEntry) -> bool {
        self.0.insert(entry.api_key, entry).is_none()
    }
}

/// The request schemas the table names: a `FLEXIBLE_MIN` per api, and a
/// decodable request type for the `decoded` sections.
mod krabka_protocol {
    pub struct DecodeError;

    pub trait Decode: Sized {
        fn decode(buf: &mut &[u8], version: i16) -> Result<Self, DecodeError>;
    }

    /// A request whose body is its UTF-8 text, prefixed with the version it
    /// was decoded at.
    pub struct Text(pub String);

    impl Decode for Text {
        fn decode(buf: &mut &[u8], version: i16) -> Result<Self, DecodeError> {
            let text = std::str::from_utf8(buf).map_err(|_| DecodeError)?;
            *buf = &[];
            Ok(Self(format!("v{version}:{text}")))
        }
    }

    pub mod owned {
        pub mod metadata_request {
            pub const FLEXIBLE_MIN: i16 = 9;
        }
        pub mod describe_configs_request {
            pub const FLEXIBLE_MIN: i16 = 4;
        }
        pub mod create_acls_request {
            pub use crate::krabka_protocol::Text as CreateAclsRequest;
            pub const FLEXIBLE_MIN: i16 = 2;
        }
        pub mod describe_acls_request {
            pub use crate::krabka_protocol::Text as DescribeAclsRequest;
            pub const FLEXIBLE_MIN: i16 = 2;
        }
        pub mod list_groups_request {
            pub use crate::krabka_protocol::Text as ListGroupsRequest;
            pub const FLEXIBLE_MIN: i16 = 3;
        }
        pub mod heartbeat_request {
            pub use crate::krabka_protocol::Text as HeartbeatRequest;
            pub const FLEXIBLE_MIN: i16 = 4;
        }
        pub mod list_config_resources_request {
            pub use crate::krabka_protocol::Text as ListConfigResourcesRequest;
            pub const FLEXIBLE_MIN: i16 = 0;
        }
        pub mod add_partitions_to_txn_request {
            pub const FLEXIBLE_MIN: i16 = 3;
        }
        pub mod create_delegation_token_request {
            pub const FLEXIBLE_MIN: i16 = 2;
        }
        pub mod push_telemetry_request {
            pub const FLEXIBLE_MIN: i16 = 0;
        }
    }
}

/// Renders what a handler received, or fails on an empty body so that the
/// tests can see an error pass through the adapter unchanged.
fn reply(parts: &[&dyn std::fmt::Display]) -> Result<Bytes, BrokerError> {
    let text = parts
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    if text.ends_with(' ') || text.ends_with(':') {
        Err(BrokerError::EmptyBody)
    } else {
        Ok(text.into_bytes())
    }
}

fn text(body: &[u8]) -> String {
    String::from_utf8_lossy(body).into_owned()
}

mod handlers {
    use crate::{ApiVersion, BrokerError, Bytes, krabka_protocol::Decode};

    /// The `typed` adapters' encoder: the response, then the version it was
    /// encoded at. It refuses a response that says `nope`.
    pub fn encode_response<R: std::fmt::Display>(
        resp: &R,
        version: ApiVersion,
    ) -> Result<Bytes, BrokerError> {
        let text = resp.to_string();
        if text.contains("nope") {
            return Err(BrokerError::Encode);
        }
        Ok(format!("{text} @v{version}").into_bytes())
    }

    /// The `typed_group` adapters' decoder, which refuses a body longer than
    /// eight bytes as the broker's refuses an over-long record string.
    pub fn decode_group_request<R: Decode>(
        buf: &mut &[u8],
        version: ApiVersion,
    ) -> Result<R, BrokerError> {
        if buf.len() > 8 {
            return Err(BrokerError::RecordString);
        }
        Ok(R::decode(buf, version)?)
    }

    /// Renders what a typed handler received, failing on an empty body like
    /// [`crate::reply`].
    fn typed_reply(parts: &[&dyn std::fmt::Display]) -> Result<String, BrokerError> {
        crate::reply(parts).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    }

    pub mod list_groups {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::list_groups_request::ListGroupsRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: ListGroupsRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> std::future::Ready<Result<String, BrokerError>> {
            let ListGroupsRequest(text) = request;
            std::future::ready(super::typed_reply(&[
                &"list_groups",
                &broker.name,
                &version,
                &ctx.client_id,
                &text,
            ]))
        }
    }

    pub mod heartbeat {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::heartbeat_request::HeartbeatRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: HeartbeatRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> std::future::Ready<Result<String, BrokerError>> {
            let HeartbeatRequest(text) = request;
            std::future::ready(super::typed_reply(&[
                &"heartbeat",
                &broker.name,
                &version,
                &ctx.client_id,
                &text,
            ]))
        }
    }

    pub mod list_config_resources {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::list_config_resources_request::ListConfigResourcesRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: ListConfigResourcesRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> Result<String, BrokerError> {
            let ListConfigResourcesRequest(text) = request;
            super::typed_reply(&[
                &"list_config_resources",
                &broker.name,
                &version,
                &ctx.client_id,
                &text,
            ])
        }
    }

    pub mod metadata {
        use crate::{
            ApiVersion, BoxFuture, Broker, BrokerError, Bytes, CorrelationId, RequestContext,
        };

        pub fn handle<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a RequestContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(std::future::ready(crate::reply(&[
                &"metadata",
                &broker.name,
                &version,
                &correlation_id,
                &ctx.client_id,
                &crate::text(body),
            ])))
        }
    }

    pub mod describe_configs {
        use crate::{ApiVersion, Broker, BrokerError, Bytes, CorrelationId, RequestContext};

        pub fn handle(
            broker: &Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &[u8],
            ctx: &RequestContext<'_>,
        ) -> Result<Bytes, BrokerError> {
            crate::reply(&[
                &"describe_configs",
                &broker.name,
                &version,
                &correlation_id,
                &ctx.client_id,
                &crate::text(body),
            ])
        }
    }

    pub mod create_acls {
        use crate::{
            ApiVersion, BoxFuture, Broker, BrokerError, Bytes, RequestContext,
            krabka_protocol::owned::create_acls_request::CreateAclsRequest,
        };

        pub fn handle<'a>(
            broker: &'a Broker,
            request: CreateAclsRequest,
            ctx: &'a RequestContext<'a>,
            version: ApiVersion,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            let CreateAclsRequest(text) = request;
            Box::pin(std::future::ready(crate::reply(&[
                &"create_acls",
                &broker.name,
                &version,
                &ctx.client_id,
                &text,
            ])))
        }
    }

    pub mod describe_acls {
        use crate::{
            ApiVersion, Broker, BrokerError, Bytes, RequestContext,
            krabka_protocol::owned::describe_acls_request::DescribeAclsRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: DescribeAclsRequest,
            ctx: &RequestContext<'_>,
            version: ApiVersion,
        ) -> Result<Bytes, BrokerError> {
            let DescribeAclsRequest(text) = request;
            crate::reply(&[
                &"describe_acls",
                &broker.name,
                &version,
                &ctx.client_id,
                &text,
            ])
        }
    }

    pub mod push_telemetry {
        use crate::{ApiVersion, Broker, BrokerError, Bytes, CorrelationId, TelemetryContext};

        pub fn handle(
            broker: &Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &[u8],
            ctx: &TelemetryContext<'_>,
        ) -> Result<Bytes, BrokerError> {
            crate::reply(&[
                &"push_telemetry",
                &broker.name,
                &version,
                &correlation_id,
                &ctx.client_instance,
                &crate::text(body),
            ])
        }
    }
}

/// The `=> path` override: a handler outside `crate::handlers`.
mod txn {
    pub mod add_partitions_to_txn {
        use crate::{
            ApiVersion, BoxFuture, Broker, BrokerError, Bytes, CorrelationId, RequestContext,
        };

        pub fn handle<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a RequestContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(std::future::ready(crate::reply(&[
                &"txn::add_partitions_to_txn",
                &broker.name,
                &version,
                &correlation_id,
                &ctx.client_id,
                &crate::text(body),
            ])))
        }
    }
}

/// The hand-written adapter an `auth` entry registers.
fn create_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    principal: &'a str,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(std::future::ready(reply(&[
        &"create_delegation_token_adapter",
        &broker.name,
        &version,
        &correlation_id,
        &principal,
        &text(body),
    ])))
}

krabka_macros::dispatch_table! {
    context: Metadata, AddPartitionsToTxn => crate::txn::add_partitions_to_txn::handle;
    sync_context: DescribeConfigs;
    decoded: CreateAcls;
    decoded_sync: DescribeAcls;
    typed: ListGroups;
    typed_group: Heartbeat;
    typed_sync: ListConfigResources;
    auth: CreateDelegationToken;
    telemetry: PushTelemetry;
}

/// Polls a future that is ready on its first poll, which every handler here
/// is.
fn ready<T>(mut future: BoxFuture<'_, T>) -> T {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("the adapter's future was not ready"),
    }
}

/// Calls `entry`'s handler at version 5 with correlation id 7.
fn call(entry: DispatchEntry, body: &[u8]) -> Result<String, BrokerError> {
    let broker = Broker { name: "b1" };
    let bytes = match entry.handler {
        Handler::Context(handler) => {
            let ctx = RequestContext { client_id: "c1" };
            ready(handler(&broker, 5, 7, body, &ctx))
        }
        Handler::Telemetry(handler) => {
            let ctx = TelemetryContext {
                client_instance: "i1",
            };
            ready(handler(&broker, 5, 7, body, &ctx))
        }
        Handler::Auth(handler) => ready(handler(&broker, 5, 7, body, "User:alice")),
    }?;
    Ok(String::from_utf8(bytes).expect("handlers reply in UTF-8"))
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    kind: &'static str,
    flexible_min: ApiVersion,
    reply: Result<String, BrokerError>,
    empty_body: Result<String, BrokerError>,
}

fn observe(registry: &DispatchRegistry, api: ApiKey) -> Option<Observed> {
    let entry = *registry.0.get(&(api as i16))?;
    Some(Observed {
        kind: match entry.handler {
            Handler::Context(_) => "context",
            Handler::Telemetry(_) => "telemetry",
            Handler::Auth(_) => "auth",
        },
        flexible_min: entry.flexible_min,
        reply: call(entry, b"body"),
        empty_body: call(entry, b""),
    })
}

#[test]
fn every_section_registers_an_adapter_that_reaches_its_handler() {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);

    let ok = |reply: &str| Ok(reply.to_owned());
    let cases = [
        (
            ApiKey::Metadata,
            "context",
            9,
            ok("metadata b1 5 7 c1 body"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::AddPartitionsToTxn,
            "context",
            3,
            ok("txn::add_partitions_to_txn b1 5 7 c1 body"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::DescribeConfigs,
            "context",
            4,
            ok("describe_configs b1 5 7 c1 body"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::CreateAcls,
            "context",
            2,
            ok("create_acls b1 5 c1 v5:body"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::DescribeAcls,
            "context",
            2,
            ok("describe_acls b1 5 c1 v5:body"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::ListGroups,
            "context",
            3,
            ok("list_groups b1 5 c1 v5:body @v5"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::Heartbeat,
            "context",
            4,
            ok("heartbeat b1 5 c1 v5:body @v5"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::ListConfigResources,
            "context",
            0,
            ok("list_config_resources b1 5 c1 v5:body @v5"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::CreateDelegationToken,
            "auth",
            2,
            ok("create_delegation_token_adapter b1 5 7 User:alice body"),
            Err(BrokerError::EmptyBody),
        ),
        (
            ApiKey::PushTelemetry,
            "telemetry",
            0,
            ok("push_telemetry b1 5 7 i1 body"),
            Err(BrokerError::EmptyBody),
        ),
    ];

    assert!(registry.0.len() == cases.len());
    for (api, kind, flexible_min, reply, empty_body) in cases {
        let expected = Observed {
            kind,
            flexible_min,
            reply,
            empty_body,
        };
        assert!(observe(&registry, api) == Some(expected), "{api:?}");
    }
}

#[test]
fn a_decoded_adapter_maps_a_decode_failure_to_a_broker_error() {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);

    for api in [
        ApiKey::CreateAcls,
        ApiKey::DescribeAcls,
        ApiKey::ListGroups,
        ApiKey::Heartbeat,
        ApiKey::ListConfigResources,
    ] {
        let entry = registry.0[&(api as i16)];
        assert!(call(entry, &[0xff]) == Err(BrokerError::Decode), "{api:?}");
    }
}

#[test]
fn only_a_typed_group_adapter_decodes_through_decode_group_request() {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);

    let long = b"longer-than-eight";
    let replies =
        [ApiKey::ListGroups, ApiKey::Heartbeat].map(|api| call(registry.0[&(api as i16)], long));
    assert!(
        replies
            == [
                Ok("list_groups b1 5 c1 v5:longer-than-eight @v5".to_owned()),
                Err(BrokerError::RecordString),
            ]
    );
}

#[test]
fn a_typed_adapter_returns_the_encoders_error() {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);

    for api in [
        ApiKey::ListGroups,
        ApiKey::Heartbeat,
        ApiKey::ListConfigResources,
    ] {
        let entry = registry.0[&(api as i16)];
        assert!(call(entry, b"nope") == Err(BrokerError::Encode), "{api:?}");
    }
}

#[test]
#[should_panic(expected = "assertion failed")]
fn registering_the_table_twice_panics() {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);
    register_dispatch_table(&mut registry);
}
