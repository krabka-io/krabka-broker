//! Derive macros for the krabka broker.
//!
//! `krabka-macros` turns one annotated declaration into code that the broker
//! would otherwise write out by hand two or three times. It is built on
//! [`moxy`], which supplies the token, syntax-tree, attribute-parsing and
//! template layers.
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
//! - `help = "..."` — the help text. Every registered field needs one.
//! - `name = "..."` — the registered name. The default is the field name with
//!   a trailing `_total` removed, because `prometheus-client` appends `_total`
//!   to the name of a counter itself.
//! - `buckets = EXPR` — construct a `Histogram` over `EXPR`. On a
//!   `Family<_, Histogram>` field, construct the family so that each of its
//!   histograms uses `EXPR`.
//! - `new = EXPR` — construct the field with `EXPR`.
//! - `skip` — construct the field, but do not register it.
//!
//! ```ignore
//! #[derive(krabka_macros::RegisterMetrics)]
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
//! #[krabka_macros::human_units]
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
//! krabka_macros::dispatch_table! {
//!     context: Metadata, AddPartitionsToTxn => crate::txn::handlers::add_partitions_to_txn::handle;
//!     sync_context: DescribeConfigs;
//!     typed: ListGroups;
//!     typed_group: Heartbeat;
//!     typed_sync: ListConfigResources;
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
//!   `handler(broker, request, version, ctx)` for a
//!   `Result<Response, BrokerError>` without awaiting it, encode the response,
//!   and wrap the result in a ready future.
//! - `telemetry` adapters pass a `TelemetryContext` and wrap a synchronous
//!   result.
//! - `auth` entries generate no adapter: the hand-written `<name>_adapter`
//!   must be in scope.
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
//! let probes: BTreeMap<_, _> = krabka_macros::throttle_probes! {
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
//! #[derive(clap::Args, krabka_macros::RuntimeOverlay)]
//! #[overlay(target = RuntimeFileConfig)]
//! struct RuntimeArgs {
//!     cleaner_interval: Option<Time>,
//!     #[overlay(refined)]
//!     num_partitions: Option<PositiveI32>,
//! }
//! ```
//!
//! # `krabka_env`
//!
//! `#[krabka_env]` goes on a clap argument struct, above its
//! `#[derive(clap::Args)]`. Each field without an `#[arg(...)]` of its own gets
//! `#[arg(long, env = "KRABKA_<FIELD>", ...)]`, where `<FIELD>` is the field
//! name in upper case and the rest follows from the field type:
//!
//! | Field type | Added argument |
//! |---|---|
//! | `Option<Time>` | `value_parser = krabka_units::parse::positive_time` |
//! | `Option<ByteSize>` | `value_parser = krabka_units::parse::positive_byte_size` |
//! | `Option<Ratio>` | `value_parser = krabka_units::parse::positive_ratio` |
//! | `Option<PositiveCount>` | `value_parser = krabka_broker::config_value::parse_positive_count` |
//! | `Option<PositiveI16>`, `I32`, `I64` | the matching `parse_positive_*` |
//! | `Option<u32>` | `value_parser = clap::value_parser!(u32).range(1..)` |
//! | `Option<i64>` | `value_parser = clap::value_parser!(i64).range(0..)` |
//! | `Option<bool>` | `action = clap::ArgAction::Set` |
//! | `Option<i16>`, `Option<i32>`, `Option<String>` | none |
//!
//! The type is matched as written, so a field of any other type, or of one
//! of these that needs another parser, keeps its own `#[arg(...)]`. Doc
//! comments stay where they are and remain the help text.
//!
//! # `RefinedNewtype`
//!
//! `#[derive(RefinedNewtype)]` goes on a tuple struct with one field, the value
//! a `refined_type` rule checks. It adds `new(value) -> Result<Self, E>`, which
//! validates through the rule, and a `#[must_use] const` getter that returns
//! the value. Both take the struct's visibility. One `#[refined(...)]`
//! attribute configures it:
//!
//! - `rule(<type>)` — the `refined_type` rule, such as `GreaterU32<0>`.
//!   Required. It goes in parentheses because a generic type does not parse
//!   after `=`.
//! - `getter = name` — the getter's name. The default is `into_value`.
//! - `string_error` — `new` returns `String`, the rule's error text, rather
//!   than `refined_type::result::Error<T>`.
//! - `label = "..."` — with `string_error`, `new` returns
//!   `"<label>: <rule error>"`.
//! - `default = EXPR` — implement `Default` as `Self::new(EXPR)`, which panics
//!   when `EXPR` breaks the rule.
//! - `from_str` — implement `FromStr` with `Err = String`: the field type's
//!   own `parse`, then `new`, each error as its text.
//! - `display` — implement `Display` as the field's.
//! - `parse_fn = name` — add a free `fn name(&str) -> Result<Self, String>`
//!   that parses the way `from_str` does.
//! - `quantity = ByteSize`, `Time` or `Frequency` — the field is a whole
//!   number of bytes, milliseconds or Hz, and `new` takes the `krabka_units`
//!   quantity. It refuses a fraction, an infinity, or a count the field type
//!   cannot hold with `"<label>: must be a whole number of <unit> that fits in
//!   <field type>"`, then hands the count to the rule. It implies
//!   `string_error`, adds `TryFrom<quantity>` through `new`, makes `from_str`
//!   parse with `krabka_units::parse` and `display` print the quantity's
//!   `human()` text, and makes `default` take a quantity.
//! - `quantity_getter = name` — with `quantity`, add a getter that returns the
//!   value as its quantity.
//!
//! ```ignore
//! #[derive(Clone, Copy, krabka_macros::RefinedNewtype)]
//! #[refined(rule(GreaterU32<0>), string_error, label = "fetch miss limit", getter = get,
//!           default = 3, from_str, display)]
//! pub struct FetchMissLimit(u32);
//!
//! #[derive(Clone, Copy, krabka_macros::RefinedNewtype)]
//! #[refined(rule(GreaterI32<0>), quantity = ByteSize, label = "fetch max", getter = bytes,
//!           quantity_getter = size, default = mebibytes(8), from_str, display)]
//! pub struct FetchMax(i32);
//! ```
//!
//! # `PrimitiveCmp`
//!
//! `#[derive(PrimitiveCmp)]` goes on a tuple struct with one field. It
//! implements `PartialEq` and `PartialOrd` between the struct and the field's
//! type in both directions, so that `Seq(3) == 3_u64` and `2_u64 < Seq(3)`
//! compile.
//!
//! # `EnumStr`
//!
//! `#[derive(EnumStr)]` goes on an enum whose variants each stand for one
//! fixed text, such as a metric label value or a config value. It adds a
//! `#[must_use] const fn as_str(self) -> &'static str` with the enum's
//! visibility. The method takes `&self` when any variant has fields, which
//! its arm matches with `{ .. }` or `(..)`. One optional `#[enum_str(...)]`
//! attribute on the enum configures it:
//!
//! - `case = "..."` — how a variant name becomes its text: `"snake_case"`,
//!   `"kebab-case"`, `"lowercase"` or `"UPPERCASE"`. Without it the text is
//!   the variant name unchanged.
//! - `as_str = name` — the method's name.
//! - `parse` or `parse = name` — add `fn parse(&str) -> Option<Self>`, under
//!   that name, over the unit variants.
//! - `all` — add `const ALL: [Self; N]`, every variant in declaration order.
//!   Every variant must be a unit variant.
//! - `label_value` — implement `prometheus_client`'s `EncodeLabelValue` as the
//!   text. The crate must depend on `prometheus-client`.
//!
//! A variant's own `#[enum_str(...)]` takes `name = "..."`, its text in place
//! of the cased name, and `alias = "..."` or `alias("...", "...")`, further
//! text that `parse` accepts.
//!
//! ```ignore
//! #[derive(Clone, Copy, krabka_macros::EnumStr)]
//! #[enum_str(case = "snake_case", as_str = config_name, parse = from_config_name)]
//! pub enum AssignorKind {
//!     Auto,
//!     #[enum_str(alias = "highly-available")]
//!     HighlyAvailable,
//! }
//! ```
//!
//! # `FieldDefaults`
//!
//! `#[derive(FieldDefaults)]` goes on a struct with named fields in place of a
//! hand-written `impl Default` that only fills each field with a fixed
//! expression. Each field takes one optional `#[default(EXPR)]` attribute and
//! starts as `EXPR`; a field without one starts as `Default::default()`. The
//! expression is evaluated each time `default()` runs, so it may call
//! functions, and it is written out as given, so it must name what it uses in
//! the scope of the struct. The derive adds no bounds: a generic struct
//! writes the bounds its fields need on the struct itself.
//!
//! The name differs from `Default` because the standard derive owns the bare
//! name and its own `#[default]` attribute on enum variants. Do not derive
//! both on one struct.
//!
//! ```ignore
//! #[derive(krabka_macros::FieldDefaults)]
//! pub struct FreezeConfig {
//!     #[default(Duration::from_secs(30))]
//!     pub poll_interval: Duration,
//!     #[default(true)]
//!     pub enabled: bool,
//!     pub owners: Vec<String>,
//! }
//! ```

use moxy::{
    ast::{ItemEnum, ItemStruct, ParseError},
    token::TokenStream,
};

mod api_names;
mod dispatch;
mod enum_str;
mod field_defaults;
mod human_units;
mod krabka_env;
mod meta;
mod metrics;
mod primitive_cmp;
mod refined_newtype;
mod runtime_overlay;
mod throttle_probes;

/// Derives `unregistered` and `register` for a struct of metric handles. The
/// crate documentation lists the field attributes.
#[moxy::derive(RegisterMetrics, attributes(metric))]
pub fn register_metrics(item: ItemStruct) -> Result<TokenStream, ParseError> {
    metrics::expand(item)
}

// New macros: put the implementation in its own module under `src/` and add
// its entry point below this line, one block per macro.

/// Gives every `Option<Time>`, `Option<ByteSize>` and `Option<Ratio>` field of
/// a file-config struct its human-unit serde codec and schema marker. The
/// crate documentation describes the expansion.
#[moxy::attribute(name = "human_units")]
pub fn human_units(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    human_units::expand(meta, item)
}

/// Derives `new`, a getter, and optionally `Default`, `FromStr`, `Display`, a
/// free parse function and whole-unit quantity conversions for a tuple newtype
/// over a `refined_type` rule. The crate documentation lists the
/// `#[refined(...)]` arguments.
#[moxy::derive(RefinedNewtype, attributes(refined))]
pub fn refined_newtype(item: ItemStruct) -> Result<TokenStream, ParseError> {
    refined_newtype::expand(item)
}

/// Derives `PartialEq` and `PartialOrd` in both directions between a tuple
/// newtype and the type of its one field.
#[moxy::derive(PrimitiveCmp)]
pub fn primitive_cmp(item: ItemStruct) -> Result<TokenStream, ParseError> {
    primitive_cmp::expand(item)
}

/// Generates the broker's dispatch adapters and `register_dispatch_table`
/// from one table of api names. The crate documentation describes the table.
#[moxy::function(name = "dispatch_table")]
pub fn dispatch_table(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    dispatch::expand(tokens)
}

/// Expands to the throttle-echo audit's `[(API_KEY, Probe); N]` array from one
/// table of api names. The crate documentation describes the table.
#[moxy::function(name = "throttle_probes")]
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

/// Gives every field of a clap argument struct without an `#[arg(...)]` a
/// `--long` flag, a `KRABKA_<FIELD>` environment variable, and the value
/// parser its type implies. The crate documentation lists the types.
#[moxy::attribute(name = "krabka_env")]
pub fn krabka_env(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    krabka_env::expand(meta, item)
}

/// Derives `as_str`, and optionally a parse function, `ALL` and
/// `EncodeLabelValue`, for an enum whose variants each stand for one text.
/// The crate documentation lists the `#[enum_str(...)]` arguments.
#[moxy::derive(EnumStr, attributes(enum_str))]
pub fn enum_str(item: ItemEnum) -> Result<TokenStream, ParseError> {
    enum_str::expand(item)
}

/// Derives `Default` from each field's `#[default(EXPR)]`, or
/// `Default::default()` for a field without one. The crate documentation
/// describes the expansion.
#[moxy::derive(FieldDefaults, attributes(default))]
pub fn field_defaults(item: ItemStruct) -> Result<TokenStream, ParseError> {
    field_defaults::expand(item)
}
