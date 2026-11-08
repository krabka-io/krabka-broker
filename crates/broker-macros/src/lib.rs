//! Derive and function macros for the krabka broker alone.
//!
//! `krabka-broker-macros` holds the macros whose expansions name modules of
//! `krabka-broker`, such as `crate::handlers` or `crate::coordinator`, so they
//! compile only inside the broker. It is built on [`moxy`], which supplies the
//! token, syntax-tree, attribute-parsing and template layers, and shares its
//! syntax helpers with `krabka-macros`. It is not published.
//!
//! # `RegisterMetrics`
//!
//! `#[derive(RegisterMetrics)]` goes on a struct of `prometheus-client` metric
//! handles. It adds two private methods to the struct:
//!
//! - `fn unregistered() -> Self` constructs every field: with
//!   `Default::default()`, with a histogram over `buckets`, or with the `new`
//!   expression.
//! - `fn register(&self, registry: &mut Registry)` registers a clone of every
//!   field that is not `skip`, in field order. The order of the fields is the
//!   order of the families in the text exposition.
//!
//! Each field takes one optional `#[metric(...)]` attribute:
//!
//! - `help = "..."` â€” the help text. Every registered field needs one.
//! - `name = "..."` â€” the registered name. The default is the field name with
//!   a trailing `_total` removed, because `prometheus-client` appends `_total`
//!   to the name of a counter itself.
//! - `buckets = EXPR` â€” construct a `Histogram` over `EXPR`. On a
//!   `Family<_, Histogram>` field, construct the family so that each of its
//!   histograms uses `EXPR`.
//! - `new = EXPR` â€” construct the field with `EXPR`.
//! - `skip` â€” construct the field, but do not register it.
//!
//! ```ignore
//! #[derive(krabka_broker_macros::RegisterMetrics)]
//! struct Metrics {
//!     #[metric(help = "Records received")]
//!     records_total: Counter,
//!     #[metric(help = "Request latency in seconds", buckets = [0.001, 0.01, 0.1])]
//!     latency_seconds: Family<ApiLabel, Histogram>,
//! }
//! ```
//!
//! # `human_units`
//!
//! `#[human_units]` goes on a file-config struct, above its
//! `#[derive(Deserialize, Serialize, JsonSchema)]`. Each field typed
//! `Option<Time>`, `Option<ByteSize>` or `Option<Ratio>` (under any path
//! prefix) gets two attributes:
//!
//! - `#[serde(default, with = "krabka_units::serde_units::human::option_X")]`,
//!   so that the TOML file writes the value as a string with a unit, such as
//!   `"5s"` or `"1MiB"`. `default` is left out when the field already has a
//!   serde `default`.
//! - `#[schemars(with = "Option<crate::file_config::schema_units::X>")]`, so
//!   that the JSON Schema names the unit in its `format`.
//!
//! A field with its own serde `with`, `deserialize_with` or `serialize_with`
//! keeps its codec and gets neither attribute. Every other field passes through
//! unchanged.
//!
//! ```ignore
//! #[krabka_broker_macros::human_units]
//! #[derive(Deserialize, Serialize, JsonSchema)]
//! struct FileTimeouts {
//!     request_timeout: Option<Time>,
//!     segment_bytes: Option<ByteSize>,
//! }
//! ```
//!
//! # `dispatch_table!`
//!
//! `dispatch_table! { ... }` goes in the broker's dispatch registry module. It
//! reads sections of `CamelCase` Kafka api names, each a label, a colon, and
//! comma-separated entries closed by `;`:
//!
//! ```ignore
//! krabka_broker_macros::dispatch_table! {
//!     context: Metadata, AddPartitionsToTxn => crate::txn::handlers::add_partitions_to_txn::handle;
//!     sync_context: DescribeConfigs;
//!     typed: ListGroups;
//!     typed_group: Heartbeat;
//!     typed_sync: DescribeAcls;
//!     typed_infallible: ListConfigResources;
//!     auth: CreateDelegationToken;
//!     telemetry: PushTelemetry;
//! }
//! ```
//!
//! From each name, such as `CreateAcls`, it derives the adapter
//! `create_acls_adapter`, the `ApiKey::CreateAcls` variant, the request module
//! `krabka_protocol::owned::create_acls_request` and its `CreateAclsRequest`
//! type. The handler is `crate::handlers::create_acls::handle` unless the entry
//! names another with `=> path`.
//!
//! - `context` and `sync_context` adapters call
//!   `handler(broker, version, body, ctx)` on the raw body; a `sync_context`
//!   handler returns its result instead of a future. A handler that needs the
//!   request's correlation id reads it from the context.
//! - `typed` adapters decode the request, await
//!   `handler(broker, request, version, ctx)` for a
//!   `Result<Response, BrokerError>`, and encode the response with
//!   `crate::handlers::encode_response`. `typed_group` adapters do the same
//!   but decode through `crate::handlers::decode_group_request`.
//! - `typed_sync` adapters decode the request, call
//!   `handler(broker, &request, version, ctx)` for a
//!   `Result<Response, BrokerError>` without awaiting it, encode the response,
//!   and wrap the result in a ready future. `typed_infallible` adapters do the
//!   same for a handler that returns the `Response` itself.
//! - `telemetry` adapters pass a `TelemetryContext` and wrap a synchronous
//!   result.
//! - `auth` entries generate no adapter: the hand-written `<name>_adapter`
//!   must be in scope.
//! - `typed_own_span` generates the `typed` adapter without the span below,
//!   for a handler that keeps a `#[tracing::instrument]` of its own.
//!
//! Every other generated adapter runs the decode, the handler and the encode
//! inside an `info` span named `handle_<snake_name>`, with `api = "<Name>"`,
//! `version` and, for the `context`, `sync_context` and `telemetry` sections,
//! `req_bytes = body.len()`. On `Err` it emits, inside that span, the event
//! `#[tracing::instrument(err)]` would: `ERROR` with `error` set to the
//! error's `Display`. The expansion calls `::tracing`, so the calling crate
//! depends on it, and `BrokerError` implements `Display`.
//!
//! It then emits `fn register_dispatch_table(registry: &mut DispatchRegistry)`,
//! which registers every entry at the request schema's `FLEXIBLE_MIN` and
//! panics on a duplicate registration. The expansion names `Broker`,
//! `ApiVersion`, `CorrelationId`, `RequestContext`, `TelemetryContext`,
//! `BoxFuture`, `Bytes`, `BrokerError`, `ApiKey`, `DispatchEntry` and
//! `DispatchRegistry` unqualified, so the calling module imports them.
//!
//! # `throttle_probes!`
//!
//! `throttle_probes! { ... }` is an expression for the KIP-219 throttle-echo
//! audit: an array of `(API_KEY, probe as Probe)`, one per entry, keyed by the
//! response module's generated `API_KEY`. Its sections are `throttled` (the
//! response has `throttle_time_ms`, which the probe sets to `SENTINEL`),
//! `unthrottled` (it has none), and `legacy_split: Name = version`, whose
//! probe encodes the `kafka_3_6_2` response below `version`. The probes call
//! `position`, `ApiVersion`, `ThrottlePosition`, `SENTINEL` and `Probe` from
//! the calling module.
//!
//! ```ignore
//! let probes: BTreeMap<_, _> = krabka_broker_macros::throttle_probes! {
//!     throttled: Metadata, ListOffsets;
//!     unthrottled: SaslHandshake;
//!     legacy_split: Produce = 3, Fetch = 4;
//! }
//! .into_iter()
//! .collect();
//! ```
//!
//! # `RuntimeOverlay`
//!
//! `#[derive(RuntimeOverlay)]` goes on a clap argument group whose fields
//! overlay the same-named fields of a config struct. The struct names that
//! config with `#[overlay(target = Type)]`. The derive adds
//! `pub(crate) fn copy_into(&self, target: &mut Type)`, which assigns every
//! field in field order:
//!
//! - by default, `target.f = self.f`, so the field must be `Copy`;
//! - with `#[overlay(refined)]`, `target.f = self.f.map(|value| value.into_value())`,
//!   for an `Option` of a refined newtype;
//! - with `#[overlay(clone)]`, `target.f.clone_from(&self.f)`;
//! - with `#[overlay(skip)]`, not at all.
//!
//! A field that the target lacks, or whose type differs, fails to compile.
//!
//! ```ignore
//! #[derive(clap::Args, krabka_broker_macros::RuntimeOverlay)]
//! #[overlay(target = RuntimeFileConfig)]
//! struct RuntimeArgs {
//!     cleaner_interval: Option<Time>,
//!     #[overlay(refined)]
//!     num_partitions: Option<PositiveI32>,
//! }
//! ```
//!
//! # Group timings and epoch values
//!
//! `#[derive(GroupTimingConfig)]` adds `for_group` and the test-only
//! `assigning_at_once` constructor to a membership config. Its named timing
//! fields and bounds remain visible on the struct. Select the stored keys with
//! `#[group_timing(session = KEY, heartbeat = KEY, assignment = KEY)]`.
//! The implementation uses the broker's existing group timing parser and bounds.
//!
//! `#[derive(GroupLifecycle)]` adds epoch changes, session expiry queries, and
//! member eviction to a group state. Select its existing policy with
//! `#[group_lifecycle(protocol = "consumer" | "share" | "streams")]`.
//! Consumer and share groups get assignment delay checks; consumer and streams
//! groups get rebalance fencing. A successful streams epoch change marks the
//! group dirty. All removals call the state's own `remove_member` method.
//!
//! `#[derive(FlexibleEpochValue)]` adds the version-zero epoch codec to a
//! named struct with an `i32` epoch followed by an `i64` tagged value.
//! `#[epoch_value(record = "RecordName")]` supplies the decoding error name.
//! A zero tagged value is omitted on encode and restored on decode.
//!
//! `#[flexible_value(record = "KafkaRecord")]` goes on an inherent impl with
//! private `encode_fields(self or &self, buf: &mut BytesMut)` and
//! `decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError>` methods. An
//! `encode_fields` that can fail returns `Result<(), BrokerError>`; the
//! generated `encode` returns `Result<Bytes, BrokerError>` either way.
//! It retains those methods and adds the public framed codecs. The field
//! methods keep wire order explicit; the encoder retains its receiver ownership.
//!
//! # Handler admission
//!
//! `#[voter_admission(request = TYPE, api = KEY, gate = FUNCTION, refusals = EXPR)]`
//! goes on an ordinary async `handle(broker, version, req_bytes, ctx)` function.
//! It runs the shared voter prelude before the operation body and returns early
//! for an encoded refusal. The body retains its original tokens and scopes.
//!
//! `share_state_handler!(Request, Response, TopicRow, PartitionRow, serve)`
//! declares a share-state handler. A denied request returns an error row for
//! every requested partition in request order. An allowed request calls `serve`
//! with the share coordinator and a fresh metadata snapshot.
//!
//! # Request decoding
//!
//! `#[decode_dispatch(request = TYPE, binding(req))]` decodes a request before
//! the handler's native body. `binding(mut req)` retains an owned mutable
//! request. `decoded_dispatch!` accepts the same explicit function, with
//! `[request = TYPE, binding(req)]` after its parameters and before its return type.
//!
//! `#[extension_wire(name = "SPAN", api = "API", level = "info")]` adapts an
//! ordinary operation with broker, owned request, version and context parameters.
//! It emits the raw five-argument entry point and traces both decoding and the
//! native operation. `"debug"` retains the marker API's tracing level.
//!
//! # Direct runtime and response tables
//!
//! `set_runtime!` applies optional fields in table order. Each group names a
//! validator. `plain` assigns directly; `duration` validates positive time
//! and converts it to `Duration`. A mapping selects a different target field,
//! and `in` supplies the validator's range argument. An absent field retains
//! the target value. A validation error stops later assignments.
//!
//! ```ignore
//! krabka_broker_macros::set_runtime! {
//!     runtime => config;
//!     plain: enabled;
//!     duration: heartbeat_interval;
//!     in_range: max_size => group.max_size in 1..=1000;
//! }
//! ```
//!
//! `response_errors!` implements the caller's error traits for external
//! protocol types. Its four sections are `response`, `code_response`, `row`
//! and `resource_row`. Each section contains type paths separated by commas
//! inside braces. A resource row also implements `ErrorRow`, so
//! each resource type is listed once. The traits remain in the caller's scope.
//!
//! `metric_registry_fixture!(metrics, output, guard; expect("encode"))`
//! renders directly into the named bindings. The caller retains the registry
//! lock until it drops `guard`. Use `unwrap()` to select that result policy.
//!
//! `broker_config! { pub struct BrokerConfig { ... } }` constructs the broker
//! config in one expansion. The fields stay in declaration order with their
//! original tokens, documentation and attributes. It adds `Debug` and `Clone`.
//!
//! `#[membership_defaults]` goes before `FieldDefaults` on a membership config.
//! It assigns the shared defaults to the six named session and heartbeat
//! fields. An existing default on one of those fields is an input error.
//!
//! `#[derive(ShareStateValue)]` writes scalar fields in declaration order,
//! followed by state batches and the delivery-count tag. The existing batch,
//! version and tag helpers retain their byte layouts and validation. Supply
//! method documentation with `#[share_state(encode_doc = "...", decode_doc = "...")]`.
//!
//! `#[derive(FlexibleArrayValue)]` wraps one `Vec` field in version-zero
//! framing. `#[array_value(record = "RecordName", encode = writer, decode = reader)]`
//! selects the existing array callbacks and decoding error name. The writer
//! returns `Result<(), BrokerError>`, and so does the generated `encode`.
//!
//! `tagged_wire!` and `unthrottled_wire!` construct exhaustive struct values.
//! Both supply an empty tagged-field set; the second supplies zero throttle.
//! Every remaining field stays required. Struct updates with `..` are refused,
//! and field expressions retain their original tokens and evaluation order.

use {
    moxy::{
        ast::{ItemEnum, ItemStruct, ParseError},
        token::TokenStream,
    },
    proc_macro::TokenStream as NativeTokenStream,
};

mod api_names;

mod async_write_delegate;

mod auth_fixtures;

mod background_controllers;

mod bound_start_fixture;

mod broker_config;

mod config_table;

mod decode_dispatch;

mod dispatch;

mod extension_wire;

mod fixtures;

mod flexible_array_value;

mod flexible_epoch_value;

mod flexible_value;

mod group_lifecycle;

mod group_timing_config;

mod human_units;

mod membership_defaults;

mod meta;

mod metrics;

mod named_acl_gates;

mod network_fixtures;

mod node_endpoints;

mod prelude;

mod produce_fixtures;

mod projection_identity;

mod replay_member_maps;

mod response_encoding;

mod runtime_assignments;

mod runtime_overlay;

mod runtime_policy_fields;

#[path = "../../macros/src/shared/sendfile_gate.rs"]
mod sendfile_gate;
mod sendfile_match;

mod share_state_handler;

mod share_state_value;

mod throttle_probes;

mod topic_filter;

mod value_framing;

mod voter_admission;

mod wire_codec_helpers;

mod wire_codes;

mod wire_fixtures;

mod wire_state_batch;

mod wire_values;

/// Append the shared topic, topic-id and partition setters of Produce fixtures.
#[moxy::attribute]
pub fn share_produce_parameters(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    wire_fixtures::share_produce_parameters(meta, item)
}

/// Derives `unregistered` and `register` for a struct of metric handles. The
/// crate documentation lists the field attributes.
#[moxy::derive(RegisterMetrics, attributes(metric))]
pub fn register_metrics(item: ItemStruct) -> Result<TokenStream, ParseError> {
    metrics::expand(item)
}

/// Gives every `Option<Time>`, `Option<ByteSize>` and `Option<Ratio>` field of
/// a file-config struct its human-unit serde codec and schema marker. The
/// crate documentation describes the expansion.
#[moxy::attribute(name = "human_units")]
pub fn human_units(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    human_units::expand(meta, item)
}

/// Generates the broker's dispatch adapters and `register_dispatch_table`
/// from one table of api names. The crate documentation describes the table.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn dispatch_table(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    dispatch::expand(tokens)
}

/// Expands to the throttle-echo audit's `[(API_KEY, Probe); N]` array from one
/// table of api names. The crate documentation describes the table.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn throttle_probes(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    throttle_probes::expand(tokens)
}

/// Derives `copy_into`, which copies each field of a clap argument group onto
/// the same-named field of its `#[overlay(target = Type)]`. The crate
/// documentation lists the field attributes.
#[moxy::derive(RuntimeOverlay, attributes(overlay))]
pub fn runtime_overlay(item: ItemStruct) -> Result<TokenStream, ParseError> {
    runtime_overlay::expand(item)
}

/// Derive clamped per-group timings and immediate-assignment test defaults.
#[moxy::derive(GroupTimingConfig, attributes(group_timing))]
pub fn group_timing_config(item: ItemStruct) -> Result<TokenStream, ParseError> {
    group_timing_config::expand(item)
}

/// Derive group transitions with the selected protocol's existing lifecycle policy.
#[moxy::derive(GroupLifecycle, attributes(group_lifecycle))]
pub fn group_lifecycle(item: ItemStruct) -> Result<TokenStream, ParseError> {
    group_lifecycle::expand(item)
}

/// Decode the selected request before an ordinary raw dispatch function's body.
#[moxy::attribute]
pub fn decode_dispatch(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    decode_dispatch::expand(&meta.into(), item)
}

/// Decode a request using bracketed metadata after the function parameters.
#[moxy::function]
pub fn decoded_dispatch(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    decode_dispatch::expand_function(tokens)
}

/// Adapt an explicitly typed extension operation to its decoded and traced raw entry point.
#[moxy::attribute]
pub fn extension_wire(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    extension_wire::expand(&meta.into(), item)
}

/// Derive the controller adapter's three background-task trait implementations.
/// The struct has `handle` and `node_id` fields, plus `holds_leadership` and
/// `submit` methods. The derive delegates to the existing metadata source.
#[moxy::derive(BackgroundControllers)]
pub fn background_controllers(item: ItemStruct) -> Result<TokenStream, ParseError> {
    background_controllers::expand(item)
}

/// Declare wire constants and their ordered release, trunk, and private guard tables.
/// Accepts `release TABLE { NAME = -1; } trunk TABLE { ... } private TABLE { ... }`.
/// Each section emits public i16 constants and a private test-only `NamedCode` table.
#[moxy::function]
pub fn wire_codes(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    wire_codes::expand(&tokens.into())
}

/// Emit fixed ACL gates from `name: Group(operation, parameter)` or
/// `name: Cluster(operation)` declarations, using the caller's audited ACL helper.
#[moxy::function]
pub fn named_acl_gates(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    named_acl_gates::expand(&tokens.into())
}

/// Supply `Allow` or `Deny` to the shared topic-selection helper while keeping
/// the function's five parameter types and names explicit.
#[moxy::attribute]
pub fn topic_filter(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    topic_filter::expand(meta.into(), item)
}

/// Emit explicit request/response codec and context helper declarations directly.
/// For example: `encode_request(Request); decode_response(Response); test_context ["test"];`.
/// Add `version [EXPR];` to pin the codecs, or omit it to accept a version per call.
#[moxy::function]
pub fn wire_codec_helpers(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_codec_helpers::expand(tokens)
}

/// Derive endpoint projections into the wire types listed by `node_endpoints(targets(...))`.
#[moxy::derive(NodeEndpoints, attributes(node_endpoints))]
pub fn node_endpoints(item: ItemStruct) -> Result<TokenStream, ParseError> {
    node_endpoints::expand(item)
}

/// Derive read/write share-state batch conversions with their existing ownership directions.
/// `#[wire_state_batch(read = ReadBatch, write = WriteBatch)]` selects both target types.
#[moxy::derive(WireStateBatch, attributes(wire_state_batch))]
pub fn wire_state_batch(item: ItemStruct) -> Result<TokenStream, ParseError> {
    wire_state_batch::expand(item)
}

/// Derive member-map replay methods for the selected consumer, share, or streams protocol.
/// Select the protocol with `#[replay_member_maps(protocol = "consumer")]`, `"share"`, or `"streams"`.
#[moxy::derive(ReplayMemberMaps, attributes(replay_member_maps))]
pub fn replay_member_maps(item: ItemStruct) -> Result<TokenStream, ParseError> {
    replay_member_maps::expand(item)
}

/// Add flexible value framing around explicitly ordered private field codecs.
#[moxy::attribute]
pub fn flexible_value(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    flexible_value::expand(&meta.into(), item)
}

/// Add the shared timing defaults before `FieldDefaults` derives the config constructor.
#[moxy::attribute]
pub fn membership_defaults(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    membership_defaults::expand(meta, item)
}

/// Derive the version-zero epoch codec with an optional i64 value in tag zero.
#[moxy::derive(FlexibleEpochValue, attributes(epoch_value))]
pub fn flexible_epoch_value(item: ItemStruct) -> Result<TokenStream, ParseError> {
    flexible_epoch_value::expand(item)
}

/// Derive the existing share-state value codecs from their ordered fields.
#[moxy::derive(ShareStateValue, attributes(share_state))]
pub fn share_state_value(item: ItemStruct) -> Result<TokenStream, ParseError> {
    share_state_value::expand(item)
}

/// Derive version-zero framing around one array field's existing leaf codecs.
#[moxy::derive(FlexibleArrayValue, attributes(array_value))]
pub fn flexible_array_value(item: ItemStruct) -> Result<TokenStream, ParseError> {
    flexible_array_value::expand(item)
}

/// Implement the shared error traits for external protocol response and row types.
#[moxy::function]
pub fn response_errors(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    response_encoding::expand(&tokens.into())
}

/// Check share-state admission and construct denial rows in request order.
#[moxy::function]
pub fn share_state_handler(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    share_state_handler::expand(&tokens.into())
}

/// Run voter admission before the explicit handler's operation body.
#[moxy::attribute]
pub fn voter_admission(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    voter_admission::expand(meta, item)
}

/// Generate the flat broker configuration declaration with its shared derives.
#[moxy::function]
pub fn broker_config(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    broker_config::expand(&tokens.into())
}

/// Construct an exhaustive wire value with an empty tagged-field set.
#[moxy::function]
pub fn tagged_wire(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_values::expand_native(tokens, false)
}

/// Construct an exhaustive response with zero throttle and empty tagged fields.
#[moxy::function]
pub fn unthrottled_wire(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_values::expand_native(tokens, true)
}

/// Apply optional runtime fields in table order through their named validators.
#[moxy::function]
pub fn set_runtime(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    runtime_assignments::expand(&tokens.into())
}

/// Generate the single-topic delete request fixture with a five-second timeout.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn delete_topic_request(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::delete_topic_request(tokens)
}

/// Add the shared runtime policy fields before TOML or CLI field derives.
#[moxy::attribute(name = "runtime_policy_fields")]
pub fn runtime_policy_fields(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    runtime_policy_fields::expand(meta, item)
}

/// Generate the expected fetch row of a refused partition.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn refused_fetch_partition_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::refused_fetch_partition(tokens)
}

/// Generate a one-topic `CreateTopics` request builder with explicit sizes and timeout.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn create_topic_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::create_topic(tokens)
}

/// Generate the consumer Fetch request fixture for partition zero with a 1 MiB limit.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn consumer_fetch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::consumer_fetch(tokens)
}

/// Generate projection-based Eq and Hash, and optionally Debug, for model state.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn projection_identity(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    projection_identity::expand(tokens)
}

/// Start a SCRAM client exchange while preserving its private handshake phase type.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn scram_client_first(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    auth_fixtures::scram_client_first(tokens)
}

/// Generate the direct flush and shutdown delegates for a tuple write wrapper.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn async_write_delegate(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    async_write_delegate::expand(tokens)
}

/// Match inline writes and conditionally available sendfile regions.
#[moxy::function(name = "sendfile_match")]
pub fn sendfile_match(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    sendfile_match::expand_native(tokens)
}

/// Generate the SCRAM proof XOR fixture without imposing extra length checks.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn scram_client_proof_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    auth_fixtures::scram_client_proof(tokens)
}

/// Generate a single-partition Produce fixture with explicit request settings.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn single_partition_produce_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    produce_fixtures::single_partition_produce(tokens)
}

/// Generate a manual Kafka request-frame fixture with explicit header inputs.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn request_frame_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::request_frame(tokens)
}

/// Generate deterministic patterned bytes for framing tests and benchmarks.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn patterned_bytes_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::patterned_bytes(tokens)
}

/// Literal Kafka 4.3.1 principal bytes: alice or bob, token flag.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn jvm_principal_golden(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::jvm_principal_golden(tokens)
}

/// Generate a response frame prefix independently of production framing.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn frame_prefix_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::frame_prefix(tokens)
}

/// Derive a TOML table's schema and value traits with an explicit unknown-key policy.
#[moxy::attribute(name = "config_table")]
pub fn config_table(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    config_table::expand(meta, item)
}

/// Match record payloads with file-region arms only on supported platforms.
#[moxy::function(name = "records_payload_match")]
pub fn records_payload_match(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    sendfile_match::records_payload_native(tokens)
}

/// Generate keyed fixture records with explicit count and value size.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn keyed_record_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::keyed_record_batch(tokens)
}

/// Generate bound-listener configuration or startup with the caller's diagnostics.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bound_start_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::expand(tokens)
}

/// Generate directory-assignment requests with explicit broker and partition identities.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn assignment_dirs_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::assignment_dirs(tokens)
}

/// Render a metric registry into caller-named bindings while retaining its lock guard.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn metric_registry_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::metric_registry(tokens)
}

/// Generate a SCRAM user request preserving optional and ordered user lists.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn scram_users_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::scram_users(tokens)
}

/// Generate the first matching record header decoder with an explicit UTF-8 policy.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn record_header_text(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::record_header_text(tokens)
}

/// Generate the shared dead-letter metric observations with caller-selected types.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn share_dlq_meters(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::share_dlq_meters(tokens)
}

/// Keep partition-spawn wrappers on one shared parameter list.
#[moxy::attribute(name = "partition_spawn_parameters")]
pub fn partition_spawn_parameters(
    meta: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    network_fixtures::partition_spawn_parameters(meta, item)
}

/// Generate a checkpoint writer with a caller-selected failure kind.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_checkpoint_failure(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::epoch_checkpoint_failure(tokens)
}

/// Compose a compile-time Java string-producer probe with shared imports and setup.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn java_string_producer_source(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::java_string_producer(tokens)
}

/// Gate original items or expressions on the supported sendfile platforms.
#[moxy::function(name = "sendfile_platform")]
#[expect(
    clippy::unnecessary_wraps,
    reason = "Moxy's proc_macro adapter requires a Result-returning entry point"
)]
pub fn sendfile_platform(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    Ok(sendfile_gate::sendfile_platform(tokens))
}

/// Generate a test authorizer while retaining the original policy body tokens.
#[moxy::function(name = "test_authorizer")]
pub fn test_authorizer(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    network_fixtures::test_authorizer(tokens)
}

mod reaper_task;

mod record_strings;

mod seed_member_maps;

mod sweep_loop;

/// Snapshot member metadata and assignments using the protocol's explicit target body.
#[moxy::function]
pub fn snapshot_member_maps(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    seed_member_maps::snapshot(tokens)
}

/// Restore member epochs before the protocol's explicit assignment body.
#[moxy::function]
pub fn hydrate_member_epochs(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    seed_member_maps::hydrate(tokens)
}

/// Declare request string traversal in field order, with optional native traversal bodies.
#[moxy::function]
pub fn record_strings(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    record_strings::expand(tokens)
}

/// Run an explicit sweep body with the selected cadence and shutdown body.
#[moxy::function]
pub fn sweep_loop(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    sweep_loop::expand(tokens)
}

/// Schedule an ordinary transaction sweep and retain its caller's shutdown event.
#[moxy::attribute]
pub fn reaper_task(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reaper_task::expand(meta, item)
}

mod broker_test_setup;

mod described_group_rows;

mod forwardable_requests;

mod group_record_key;

mod handler_wire_fixtures;

mod model_properties;

mod model_runner;

mod reconciliation_fields;

mod reconciliation_operations;

mod reconciliation_projection;

mod request_test_identity;

mod scheduled_loads;

mod share_fixture_schemas;

mod share_refusal_matrix;

mod share_state_constructors;

mod share_state_operation;

mod share_state_results;

/// Declare forwardable API keys and decode each embedded body with its selected request type.
#[moxy::function]
pub fn forwardable_requests(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    forwardable_requests::expand(&input.into()).map(Into::into)
}

/// Add the selected common projection fields around the visible protocol-specific fields.
#[moxy::attribute]
pub fn reconciliation_fields(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reconciliation_fields::expand(&meta.into(), item)
}

/// Run share-offset refusal cases with direct startup, identity, assertions and shutdown.
#[moxy::function]
pub fn share_refusal_matrix(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_refusal_matrix::expand(input)
}

/// Derive projection construction from the member declaration's visible fields.
#[moxy::derive(ReconciliationMember, attributes(reconciliation))]
pub fn reconciliation_member(item: ItemStruct) -> Result<TokenStream, ParseError> {
    reconciliation_projection::member(item)
}

/// Encode and parse a visible group-record key enum with explicit version constants.
#[moxy::derive(GroupRecordKey, attributes(group_key))]
pub fn group_record_key(item: ItemEnum) -> Result<TokenStream, ParseError> {
    group_record_key::expand(item)
}

/// Construct the fixed empty ACL matching filter for the selected external request type.
#[moxy::function]
pub fn empty_match_acl_filter(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_fixtures::empty_match_acl_filter(&input.into()).map(Into::into)
}

/// Compare a delegation-token refusal with the full selected wire response.
#[moxy::function]
pub fn token_refusal_result(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_fixtures::token_refusal_result(&input.into()).map(Into::into)
}

/// Construct an external share-state request from visible field expressions.
#[moxy::function]
pub fn share_state_request(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_state_constructors::expand(input)
}

/// Associate share-state response schemas with the listed request types.
#[moxy::function]
pub fn share_fixture_schemas(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_fixture_schemas::expand(&input.into()).map(Into::into)
}

/// Adapt the listed external response rows to the partition-result trait.
#[moxy::function]
pub fn share_partition_results(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    described_group_rows::partition_results(&input.into()).map(Into::into)
}

/// Run native share-state partition operations concurrently while preserving request order.
#[moxy::function]
pub fn share_state_results(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_state_results::expand(input)
}

/// Add shared active-state admission and reply handling to a visible async method.
#[moxy::attribute]
pub fn share_state_operation(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    share_state_operation::expand(&meta.into(), item)
}

/// Derive group projection construction and epoch checks from its visible fields.
#[moxy::derive(ReconciliationGroup, attributes(reconciliation))]
pub fn reconciliation_group(item: ItemStruct) -> Result<TokenStream, ParseError> {
    reconciliation_projection::group(item)
}

/// Derive heartbeat conversion from a visible reconciliation action enum.
#[moxy::derive(ReconciliationAction)]
pub fn reconciliation_action(item: ItemEnum) -> Result<TokenStream, ParseError> {
    reconciliation_projection::action(item)
}

/// Observe a projected model epoch using its visible epoch field and violation ledger.
#[moxy::derive(ModelEpochLedger)]
pub fn model_epoch_ledger(item: ItemStruct) -> Result<TokenStream, ParseError> {
    reconciliation_projection::epoch(item)
}

/// Build ordered model properties around their original native oracle blocks.
#[moxy::function]
pub fn model_properties(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    model_properties::expand(input)
}

/// Run a model with explicit search bounds and pinned state counts.
#[moxy::function]
pub fn model_runner(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    model_runner::expand(input)
}

/// Rebuild a reconciliation group around explicit native setup and projection operations.
#[moxy::function]
pub fn rebuild_reconciliation_group(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reconciliation_operations::rebuild(input)
}

/// Apply an explicit member action and project the resulting reconciliation state.
#[moxy::function]
pub fn reconciliation_next_state(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reconciliation_operations::next_state(input)
}

/// Emit reconciliation action scheduling with explicit caller predicates and extensions.
#[moxy::function]
pub fn reconciliation_actions(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    reconciliation_operations::actions(input)
}

/// Retain the broker handle and directory before native readiness operations.
#[moxy::function]
pub fn broker_test_setup(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    broker_test_setup::expand(input)
}

/// Retain principal and peer values for an explicitly selected borrowed request context.
#[moxy::function]
pub fn request_test_identity(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    request_test_identity::expand(input)
}

/// Join the visible task collection in order and log its selected failure message.
#[moxy::derive(ScheduledLoads, attributes(scheduled_loads))]
pub fn scheduled_loads(item: ItemStruct) -> Result<TokenStream, ParseError> {
    scheduled_loads::expand(item)
}

/// Implement group-describe row access for an explicit table of external response types.
#[moxy::function]
pub fn described_group_rows(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    described_group_rows::expand(&input.into()).map(Into::into)
}

mod fixture_methods;

mod group_record_flush;

mod unit_schema;

/// Describe a visible human-unit schema marker.
#[moxy::derive(UnitSchema, attributes(unit_schema))]
pub fn unit_schema(item: ItemStruct) -> Result<TokenStream, ParseError> {
    unit_schema::expand(item)
}

/// Append the shared metadata forwarding methods to an authorizer implementation.
#[moxy::attribute]
pub fn delegate_authorizer(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    fixture_methods::authorizer(meta, item)
}

/// Append missing-read behavior to an explicit remote-log implementation.
#[moxy::attribute]
pub fn missing_remote_reads(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    fixture_methods::remote(meta, item)
}

/// Preserve the visible flush body inside the shared group-log append boundary.
#[moxy::attribute]
pub fn flush_group_records(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    group_record_flush::expand(meta, item)
}

mod handler_wire_rows;

/// Build offset-fetch rows for an explicit table of wire schemas.
#[moxy::function]
pub fn offset_fetch_rows(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::offset_fetch_rows(&input.into()).map(Into::into)
}

/// Build a share-group refusal with the caller's owned fields.
#[moxy::function]
pub fn share_group_denial(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::share_group_denial(&input.into()).map(Into::into)
}

/// Project acknowledgement batches while preserving the native expression.
#[moxy::function]
pub fn acknowledgement_batches(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::acknowledgement_batches(input)
}

mod coordinator_fixtures;

/// Declare heartbeat fixtures with named status observations.
#[moxy::function]
pub fn heartbeat_status(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    coordinator_fixtures::heartbeat(&input.into()).map(Into::into)
}

/// Preserve native key-boundary observations in generated coordinator tests.
#[moxy::function]
pub fn member_key_boundaries(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    coordinator_fixtures::key_boundaries(input)
}

/// Schedule explicit commit changes and preserve caller-owned async actions.
#[moxy::function]
pub fn schedule_commit_changes(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    coordinator_fixtures::commit_changes(input)
}

mod fixture_constructors;

/// Declare fixture constructors from the existing election, supervisor and quota domains.
#[moxy::function]
pub fn fixture_constructors(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_constructors::expand(&input.into()).map(Into::into)
}

mod wire_test_driver;

/// Declare wire drivers that call the existing socket and framing helpers.
#[moxy::function]
pub fn wire_test_driver(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_test_driver::expand(&input.into()).map(Into::into)
}

mod replication_target_guard;

/// Acquire the local replication target with the original caller-level drop behavior.
#[moxy::function]
pub fn replication_target_guard(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    replication_target_guard::expand(&input.into()).map(Into::into)
}

mod share_test_drivers;

/// Declare a first share fetch through the existing client helper.
#[moxy::function]
pub fn share_first_fetch(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_test_drivers::acquisition(&input.into()).map(Into::into)
}

/// Declare a share-consumption driver with native caller operations.
#[moxy::function]
pub fn share_consumption_driver(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_test_drivers::consumption(input)
}

/// Route `(broker, context, request, version; operation, refusal)` through the
/// controller listener, continuing with the request or returning its completed dispatch.
#[moxy::function]
pub fn controller_request(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::controller_request(input)
}

mod config_alter_handler;

/// Declare the shared configuration handler with explicit wire and audit policies.
#[moxy::function]
pub fn config_alter_handler(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    config_alter_handler::expand(&input.into()).map(Into::into)
}

mod fixture_expressions;

/// Read the evicted local segment state with the declared retention policy.
#[moxy::function]
pub fn evicted_local_segments(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_expressions::evicted_segments(&input.into()).map(Into::into)
}

/// Apply reassignment fields in the caller's explicit expression order.
#[moxy::function]
pub fn reassignment_fields(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_expressions::reassignment(input)
}

mod share_request_identity;

/// Resolve a share request identity before entering the native handler body.
#[moxy::attribute]
pub fn share_request_identity(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    share_request_identity::expand(meta.into(), item)
}

mod group_fixture_tables;

mod partition_gate;

mod private_dispatch_table;

mod string_mutations;

/// Declare partition authorization gates with explicit actions.
#[moxy::function]
pub fn partition_gate(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    partition_gate::expand(&input.into()).map(Into::into)
}

/// Declare private request adapters and their registration table.
#[moxy::function]
pub fn private_dispatch_table(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    private_dispatch_table::expand(&input.into()).map(Into::into)
}

/// Declare required and optional string mutation cases.
#[moxy::function]
pub fn string_mutations(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    string_mutations::expand(&input.into()).map(Into::into)
}

/// Declare member identity fixtures with explicit schema fields.
#[moxy::function]
pub fn member_identity_fixtures(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    group_fixture_tables::member_identity(input)
}

/// Declare required-topic traversal for a topology schema.
#[moxy::function]
pub fn required_topics(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    group_fixture_tables::required_topics(&input.into()).map(Into::into)
}

mod token_dispatch;

/// Adapt an owned delegation-token handler to negotiated wire dispatch.
#[moxy::attribute]
pub fn token_dispatch(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    token_dispatch::expand(meta.into(), item)
}

#[cfg(test)]
mod export_contract_tests {
    use {
        super::*,
        assert2::assert,
        meta::{FunctionMacro, empty_input_accepted},
    };

    const FUNCTION_MACROS: [FunctionMacro; 24] = [
        FunctionMacro {
            name: "dispatch_table",
            expand: dispatch_table,
        },
        FunctionMacro {
            name: "throttle_probes",
            expand: throttle_probes,
        },
        FunctionMacro {
            name: "delete_topic_request",
            expand: delete_topic_request,
        },
        FunctionMacro {
            name: "create_topic_fixture",
            expand: create_topic_fixture,
        },
        FunctionMacro {
            name: "consumer_fetch_fixture",
            expand: consumer_fetch_fixture,
        },
        FunctionMacro {
            name: "projection_identity",
            expand: projection_identity,
        },
        FunctionMacro {
            name: "scram_client_first",
            expand: scram_client_first,
        },
        FunctionMacro {
            name: "async_write_delegate",
            expand: async_write_delegate,
        },
        FunctionMacro {
            name: "scram_client_proof_fixture",
            expand: scram_client_proof_fixture,
        },
        FunctionMacro {
            name: "single_partition_produce_fixture",
            expand: single_partition_produce_fixture,
        },
        FunctionMacro {
            name: "request_frame_fixture",
            expand: request_frame_fixture,
        },
        FunctionMacro {
            name: "patterned_bytes_fixture",
            expand: patterned_bytes_fixture,
        },
        FunctionMacro {
            name: "jvm_principal_golden",
            expand: jvm_principal_golden,
        },
        FunctionMacro {
            name: "frame_prefix_fixture",
            expand: frame_prefix_fixture,
        },
        FunctionMacro {
            name: "keyed_record_batch_fixture",
            expand: keyed_record_batch_fixture,
        },
        FunctionMacro {
            name: "bound_start_fixture",
            expand: bound_start_fixture,
        },
        FunctionMacro {
            name: "assignment_dirs_fixture",
            expand: assignment_dirs_fixture,
        },
        FunctionMacro {
            name: "metric_registry_fixture",
            expand: metric_registry_fixture,
        },
        FunctionMacro {
            name: "scram_users_fixture",
            expand: scram_users_fixture,
        },
        FunctionMacro {
            name: "record_header_text",
            expand: record_header_text,
        },
        FunctionMacro {
            name: "share_dlq_meters",
            expand: share_dlq_meters,
        },
        FunctionMacro {
            name: "epoch_checkpoint_failure",
            expand: epoch_checkpoint_failure,
        },
        FunctionMacro {
            name: "java_string_producer_source",
            expand: java_string_producer_source,
        },
        FunctionMacro {
            name: "refused_fetch_partition_fixture",
            expand: refused_fetch_partition_fixture,
        },
    ];

    #[test]
    fn function_macros_keep_their_empty_input_contract() {
        // Dispatch and throttle tables permit zero entries; fixtures need arguments.
        assert!(empty_input_accepted(FUNCTION_MACROS) == ["dispatch_table", "throttle_probes"]);
    }
}
