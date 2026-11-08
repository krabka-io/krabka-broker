//! Derive and function macros that krabka crates other than the broker use.
//!
//! `krabka-macros` turns one annotated declaration into code that a crate
//! would otherwise write out by hand two or three times. It is built on
//! [`moxy`], which supplies the token, syntax-tree, attribute-parsing and
//! template layers. The macros whose expansions name `krabka-broker` modules
//! live in the unpublished `krabka-broker-macros` crate.
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
//! | `Option<i16>`, `Option<i32>`, `Option<String>`, `Option<PathBuf>` | none |
//!
//! The type is matched as written, so a field of any other type, or of one
//! of these that needs another parser, keeps its own `#[arg(...)]`. Doc
//! comments stay where they are and remain the help text.
//!
//! `#[krabka_env(prefix = "BENCH_")]` puts every variable under that prefix in
//! place of `KRABKA_`, so the field `tls_ca_path` reads `BENCH_TLS_CA_PATH`.
//! A field whose variable is not its name under the prefix keeps its own
//! `#[arg(...)]`.
//!
//! # `RefinedNewtype`
//!
//! `#[derive(RefinedNewtype)]` goes on a tuple struct with one field, the value
//! a `refined_type` rule checks. It adds `new(value) -> Result<Self, E>`, which
//! validates through the rule, and a `#[must_use] const` getter that returns
//! the value. Both take the struct's visibility. One `#[refined(...)]`
//! attribute configures it:
//!
//! - `rule(<type>)` â€” the `refined_type` rule, such as `GreaterU32<0>`.
//!   Required. It goes in parentheses because a generic type does not parse
//!   after `=`.
//! - `getter = name` â€” the getter's name. The default is `into_value`.
//! - `string_error` â€” `new` returns `String`, the rule's error text, rather
//!   than `refined_type::result::Error<T>`.
//! - `label = "..."` â€” with `string_error`, `new` returns
//!   `"<label>: <rule error>"`.
//! - `default = EXPR` â€” implement `Default` as `Self::new(EXPR)`, which panics
//!   when `EXPR` breaks the rule.
//! - `from_str` â€” implement `FromStr` with `Err = String`: the field type's
//!   own `parse`, then `new`, each error as its text.
//! - `display` â€” implement `Display` as the field's.
//! - `parse_fn = name` â€” add a free `fn name(&str) -> Result<Self, String>`
//!   that parses the way `from_str` does.
//! - `quantity = ByteSize`, `Time` or `Frequency` â€” the field is a whole
//!   number of bytes, milliseconds or Hz, and `new` takes the `krabka_units`
//!   quantity. It refuses a fraction, an infinity, or a count the field type
//!   cannot hold with `"<label>: must be a whole number of <unit> that fits in
//!   <field type>"`, then hands the count to the rule. It implies
//!   `string_error`, adds `TryFrom<quantity>` through `new`, makes `from_str`
//!   parse with `krabka_units::parse` and `display` print the quantity's
//!   `human()` text, and makes `default` take a quantity.
//! - `quantity_getter = name` â€” with `quantity`, add a getter that returns the
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
//! - `case = "..."` â€” how a variant name becomes its text: `"snake_case"`,
//!   `"kebab-case"`, `"lowercase"` or `"UPPERCASE"`. Without it the text is
//!   the variant name unchanged.
//! - `as_str = name` â€” the method's name.
//! - `parse` or `parse = name` â€” add `fn parse(&str) -> Option<Self>`, under
//!   that name, over the unit variants.
//! - `all` â€” add `const ALL: [Self; N]`, every variant in declaration order.
//!   Every variant must be a unit variant.
//! - `label_value` â€” implement `prometheus_client`'s `EncodeLabelValue` as the
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
//!
//! # `cli_main!`
//!
//! `cli_main!(path)` goes at the top level of an operator binary's
//! `main.rs`, in place of its `main`. It expands to a `#[tokio::main]`
//! `async fn main()` that installs a `tracing_subscriber::fmt()` subscriber
//! filtered by `RUST_LOG`, or at `info` when `RUST_LOG` is unset or does not
//! parse, and writes to stderr. stdout stays the binary's own, so a report that
//! a script parses, such as `krabka-restore --report json`, holds no log lines.
//! It then awaits `path::run_from_args(std::env::args_os())` and exits the
//! process with the `i32` it returns. Any arguments after the path, such as
//! `flavor = "multi_thread"`, go to `#[tokio::main(...)]` unchanged. The
//! binary depends on `tokio` with its `macros` feature and on
//! `tracing-subscriber`.
//!
//! ```ignore
//! krabka_macros::cli_main!(krabka_restore, flavor = "multi_thread");
//! ```
//!
//! # `KafkaUuid`
//!
//! `#[derive(KafkaUuid)]` adds display, parsing, and string serde to a UUID
//! wrapper. It calls the declaration's existing `encode`, `decode`, and
//! `KafkaUuidError` helpers. Display retains the formatter's padding rules.
//!
//! # Storage methods
//!
//! `#[derive(SparseIndex)]` adds the existing loader and file operations to a
//! sparse index. `#[sparse_index(kind = "offset" | "time")]` selects its raw
//! entry layout and I/O target. The loader retains its monotonic-entry checks.
//! `#[index_open]` adds the same tracing span to other ordinary index loaders.
//!
//! `#[remote_operation]` traces an ordinary remote-storage SPI method. Its
//! existing method name selects the span level and fields. It retains the
//! native method body, parameter names, and synchronous signature.
//!
//! `#[model_type]` attaches the existing proof and runtime derives to an
//! ordinary struct or enum. `proof(...)` and `runtime(...)` replace only the
//! selected derive list, preserving its path order and the declaration tokens.
//!
//! `#[sendfile_cfg]` gates one ordinary item with the fetch path's shared
//! platform predicate. Apply it separately to imports, exports and methods.
//!
//! `worm_object_set!` emits one ordered admission rule for its `Model` and
//! `Runtime` declarations. Function headers and count observations stay native;
//! proof projections remain inside the caller's Pearlite scope.

use {
    moxy::{
        ast::{ItemEnum, ItemStruct, ParseError},
        token::{Span, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

mod bound_start_fixture;
mod cli_main;
mod cli_support;
mod connection_policy;
mod container_fixtures;
mod cross_storage_fixtures;
mod deterministic_random;
mod enum_str;
mod field_defaults;
mod fixtures;
mod gcs_fields;
mod kafka_uuid;
mod krabka_env;
mod legacy_fixtures;
mod meta;
mod metadata_log_delegate;
mod model_type;
mod network_fixtures;
mod newtype_styles;
mod object_ops;
mod object_store_config;
mod object_store_delegate;
mod otlp_exporters;
mod prelude;
mod primitive_cmp;
mod raft_fixtures;
mod refined_newtype;
mod remote_metadata_fixtures;
mod remote_operation;
mod s3_fields;
mod sendfile_cfg;
#[path = "shared/sendfile_gate.rs"]
mod sendfile_gate;
mod sparse_index;
mod timer_hooks;
mod timestamped_batch;
mod transport_errors;
mod wire_fixtures;
mod worm_object_set;

/// Share sparse-window and nonnegative base/floor preconditions between timestamp kernels.
#[moxy::attribute]
pub fn retained_timestamp_bounds(
    floor: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let expanded = wire_fixtures::retained_timestamp_bounds(floor, item.into())?;
    // Creusot captures and MIR bindings must share one identifier context.
    expanded
        .to_string()
        .parse()
        .map_err(|_| ParseError::new(Span::call_site(), "invalid timestamp bounds expansion"))
}

// Keep implementations in their own modules and document each adapter below.

/// Give a scalar id transparent JSON, primitive comparisons and integer conversions.
#[moxy::attribute]
pub fn scalar_id(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    newtype_styles::scalar_id(meta, item)
}

/// Gives a test scalar the shared display, conversion, value, and default traits.
#[moxy::attribute]
pub fn test_scalar(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    newtype_styles::test_scalar(meta, item)
}

/// Share the lowercase hex display and serde identity of byte newtypes.
#[moxy::attribute]
pub fn hex_bytes_identity(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    newtype_styles::hex_bytes_identity(meta, item)
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

/// Gives every field of a clap argument struct without an `#[arg(...)]` a
/// `--long` flag, a `KRABKA_<FIELD>` environment variable (under another
/// prefix with `prefix = "..."`), and the value parser its type implies. The
/// crate documentation lists the types.
#[moxy::attribute(name = "krabka_env")]
pub fn krabka_env(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    krabka_env::expand(meta, item)
}

/// Derives `as_str`, and optionally a parse function, `ALL` and
/// `EncodeLabelValue`, for an enum whose variants each stand for one text.
/// The crate documentation lists the `#[enum_str(...)]` arguments.
#[moxy::derive(EnumStr, attributes(enum_str))]
pub fn enum_str(item: ItemEnum) -> Result<TokenStream, ParseError> { enum_str::expand(item) }

/// Derives `Default` from each field's `#[default(EXPR)]`, or
/// `Default::default()` for a field without one. The crate documentation
/// describes the expansion.
#[moxy::derive(FieldDefaults, attributes(default))]
pub fn field_defaults(item: ItemStruct) -> Result<TokenStream, ParseError> {
    field_defaults::expand(item)
}

/// Derive display, parsing, and string serde using the caller's Kafka UUID codec.
#[moxy::derive(KafkaUuid)]
pub fn kafka_uuid(item: ItemStruct) -> Result<TokenStream, ParseError> { kafka_uuid::expand(item) }

/// Derive the sparse index's existing loader and file operations.
#[moxy::derive(SparseIndex, attributes(sparse_index))]
pub fn sparse_index(item: ItemStruct) -> Result<TokenStream, ParseError> {
    sparse_index::expand(item)
}

/// Apply the shared index-open tracing span to an ordinary loader method.
#[moxy::attribute]
pub fn index_open(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    sparse_index::index_open(meta, item)
}

/// Trace an ordinary remote-storage operation with its segment identity.
#[moxy::attribute]
pub fn remote_operation(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    remote_operation::expand(meta, item)
}

/// Attach the selected proof and runtime derives to an ordinary model declaration.
#[moxy::attribute]
pub fn model_type(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    model_type::expand(meta, item)
}

/// Keep an ordinary item on the platforms supported by the sendfile fetch path.
#[moxy::attribute]
pub fn sendfile_cfg(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    sendfile_cfg::expand(meta, item)
}

/// Emit the ordered WORM object-set rule around an explicit runtime or proof header.
#[moxy::function]
pub fn worm_object_set(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    worm_object_set::expand(tokens)
}

/// Apply the existing retry and client options to an object-store builder expression.
/// Accepts `connection_policy!(builder_expression, config_identifier)` and uses
/// the caller's `retry_config`, `client_options`, and `ClientOptions` helpers.
#[moxy::function]
pub fn connection_policy(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    connection_policy::expand(tokens)
}

/// Implement transport selection for a typed OTLP exporter method declaration.
/// Accepts an ordinary `fn method(&self) -> Result<SpanExporter, TelemetryError>;`
/// declaration, or the same signature with `LogExporter`, inside `OtlpConfig`.
#[moxy::function]
pub fn otlp_exporter(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    otlp_exporters::expand(tokens)
}

/// Expands to an operator binary's `main`: tracing setup, then
/// `std::process::exit` with what the named crate's `run_from_args` returns.
/// The crate documentation describes the expansion.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn cli_main(tokens: TokenStream) -> Result<TokenStream, ParseError> { cli_main::expand(tokens) }

/// Generate the bounded exhaustive Stateright runner under the supplied function name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bounded_bfs(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::bounded_bfs(tokens)
}

/// Generate the compacted-batch test projection under the supplied struct name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn compacted_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::compacted_batch(tokens)
}

/// Generate the benchmark's Kafka record-batch builder under the supplied function name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn record_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::record_batch(tokens)
}

/// Prepend the common bucket, prefix, redacted credentials and endpoint fields to a GCS config.
/// Select `file` or `runtime` to preserve that configuration's field documentation.
#[moxy::attribute(name = "gcs_fields")]
pub fn gcs_fields(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    gcs_fields::expand(meta, item)
}

/// Generate action-to-message/log/timer adaptation inside a simulation impl.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_actions(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_actions(tokens)
}
/// Generate the shared simulation bus routing and replication preconditions.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_transport(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_transport(tokens)
}
/// Generate simulation leader heartbeats with the caller's interval.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_heartbeat(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_heartbeat(tokens)
}
/// Generate a saturating CI benchmark scenario with a caller-supplied name and size.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn benchmark_scenario_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::benchmark_scenario(tokens)
}

/// Generate the benchmark report's representative latency percentiles.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn benchmark_latency_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::benchmark_latency(tokens)
}

/// Generate the synthetic S3 archive config used by argument-mapping assertions.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn s3_archive_config_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::s3_archive_config(tokens)
}

/// Delegate unchanged metadata-log operations through `self.inner`, before `async_trait`.
#[moxy::attribute(name = "metadata_log_delegate")]
pub fn metadata_log_delegate(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    metadata_log_delegate::expand(meta, item)
}

/// Generate the finished remote-segment lookup assertions shared by manager fixtures.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_segment_check(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::remote_segment_check(tokens)
}

/// Find the earliest armed timer of a simulation's nodes.
///
/// # Errors
///
/// Rejects an empty crate path.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_earliest_timer(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_earliest_timer(tokens)
}

/// Generate the shared simulation event adapter inside a harness impl.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_step(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_step(tokens)
}

/// Generate the shared two-partition metadata topic fixture.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn snapshot_topic_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::snapshot_topic(tokens)
}

/// Generate the timestamped record-batch fixture used by local and remote read tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn timestamped_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    timestamped_batch::expand(tokens)
}

/// Generate a producer batch builder with the caller's exact record value expression.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn producer_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::producer_batch(tokens)
}

/// Generate the initial single-replica metadata partition record builder.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn single_replica_partition_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::single_replica_partition(tokens)
}

/// Generate the version-zero control-marker key and value fixture encoders.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn control_marker_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::control_marker(tokens)
}

/// Generate a topic record builder for one partition and one replica.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn topic_record_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::topic_record(tokens)
}

/// Generate the loopback ephemeral-port helper used by container tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn ephemeral_port_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::ephemeral_port(tokens)
}

/// Generate the shared `MinIO` process/readiness helpers under the supplied module name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn minio_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    container_fixtures::expand(tokens)
}

/// Generate the independent four-state remote-segment transition oracle.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_segment_transition_matrix(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::remote_segment_transition_matrix(tokens)
}

/// Generate a tracing capture layer with an explicit mutex poison policy.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn capture_layer_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::capture_layer(tokens)
}

/// Add the selected direct `ObjectStore` delegates to an implementation.
#[moxy::attribute(name = "object_store_delegate")]
pub fn object_store_delegate(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    object_store_delegate::expand(meta, item)
}

/// Generate remote metadata lifecycle fixtures with an explicit timestamp policy.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_segment_fixtures(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::segment(tokens)
}

/// Generate the independent missing-partition metadata assertions.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_metadata_missing(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::missing(tokens)
}

/// Generate a model transition's signature and cloned state before its body.
#[moxy::function(name = "model_transition")]
pub fn model_transition(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixtures::model_transition(tokens)
}

/// Append the shared object-store retry and HTTP timeout fields.
#[moxy::attribute(name = "object_store_config")]
pub fn object_store_config(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    object_store_config::expand(meta, item)
}

/// Expand the object-store file-upload method before async trait processing.
#[moxy::attribute(name = "object_ops")]
pub fn object_ops(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    object_ops::expand(meta, item)
}

/// Generate timer registration and completion helpers with caller-specific messages.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn timer_hooks(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    timer_hooks::expand(tokens)
}

/// Generate the distinctive proposal UUID shared by signing golden fixtures.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn break_glass_golden_id(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::break_glass_golden_id(tokens)
}

/// Generate the independent file-region byte reader used by wire comparisons.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn file_region_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::file_region(tokens)
}

/// Expand the common I/O, TLS and SASL error variants at their original position.
#[moxy::attribute(name = "transport_errors")]
pub fn transport_errors(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    transport_errors::expand(meta, item)
}

/// Generate a voter-set fixture whose endpoints and directory IDs remain empty.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn empty_endpoint_voters(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    raft_fixtures::empty_endpoint_voters(tokens)
}

/// Generate the one-record leader-epoch batch fixture.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_record_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    raft_fixtures::epoch_record_batch(tokens)
}

/// Generate the epoch-cache `LogView` delegation used by Raft simulations.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_log_view(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    raft_fixtures::epoch_log_view(tokens)
}

/// Generate the canonical compressed legacy record used by policy tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn legacy_policy_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    legacy_fixtures::policy(tokens)
}

/// Add the shared generic argv signature to a command-line entry point.
#[moxy::attribute(name = "argv_entrypoint")]
pub fn argv_entrypoint(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::argv_entrypoint(meta, item)
}

/// Generate a duration-parser check over each caller's literal cases.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn duration_cases_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::duration_cases(tokens)
}

/// Generate a sorted directory-tree fixture with the caller's path projection.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn directory_tree_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::directory_tree(tokens)
}

/// Generate a CLI entry point with the caller's documentation.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn parsed_cli_entrypoint(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::parsed_cli_entrypoint(tokens)
}

/// Generate a duration-parser test from explicit literal cases.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn duration_parser_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::duration_parser_fixture(tokens)
}

/// Generate bootstrap parser tests from accepted and refused arguments.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bootstrap_parser_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::bootstrap_parser_fixture(tokens)
}

/// Generate the shared snapshot fixture over an on-disk metadata log.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn snapshot_node_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::snapshot_node_fixture(tokens)
}

/// Generate the temporary metadata and data directories used by format tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn format_directories_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::format_directories_fixture(tokens)
}

/// Generate the explicit topic map used by WAL capture fixtures.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn wal_capture_topic_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::wal_capture_topic_fixture(tokens)
}

/// Generate a CLI flag's name, environment and default-value projection.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn flag_metadata_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::flag_metadata_fixture(tokens)
}

/// Generate a listener bind retry with an explicit caller timeout.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bind_retry_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::bind_retry_fixture(tokens)
}

/// Generate indexed WORM segment metadata with an explicit segment-id namespace.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn worm_segment_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::worm_segment(tokens)
}

/// Generate the started remote-segment fixture with an explicit metadata crate.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_started_segment(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::remote_started_segment(tokens)
}

/// Generate exported segment paths with an explicit snapshot and lazy epoch projection.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn export_segment_data_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::export_segment_data(tokens)
}

/// Generate a JVM checkpoint decoder fixture with explicit bytes, name and image.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn jvm_checkpoint_dump_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::jvm_checkpoint_dump(tokens)
}

/// Generate checked Unix-millisecond fixtures with explicit panic contexts.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn unix_millis_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::unix_millis(tokens)
}

/// Generate a Vec request frame with the caller's checked length-prefix type.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn vector_request_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::vector_request(tokens)
}
/// Generate the independent eight-row supported-feature oracle.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn supported_features_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::supported_features_fixture(tokens)
}

/// Generate supported-feature rows from explicit wire bounds.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn supported_feature_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::supported_feature_fixture(tokens)
}
/// Generate finalized-feature rows from explicit wire levels.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn finalized_feature_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::finalized_feature_fixture(tokens)
}
/// Generate the record input used by byte-limit tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn record_limit_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::record_limit_fixture(tokens)
}
/// Generate keyed compaction batches with explicit producer state and timestamp.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn compaction_record_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::compaction_record_fixture(tokens)
}
/// Project registration feature bounds into an ordered map.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn registration_feature_projection(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::registration_feature_projection(tokens)
}
/// Generate the validated client frame-size parser.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn frame_max_parser(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::frame_max_parser(tokens)
}

/// Generate the validated connection dispatch-capacity parser.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn dispatch_capacity_parser(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::dispatch_capacity_parser(tokens)
}

/// Share S3 endpoint and credential fields with the original configuration documentation.
#[moxy::attribute]
pub fn s3_fields(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    s3_fields::expand(meta, item)
}

/// Define a named `SplitMix64` step that advances the caller's stream state.
/// Placement shuffles and benchmark byte streams share the exact same arithmetic.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn splitmix64_step(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    deterministic_random::splitmix64_step(tokens)
}

/// Generate an independent compression-byte oracle with a named codec level.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn compression_bytes_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::compression_bytes_fixture(tokens)
}

mod archive_rows;
mod authorization_refusal;
mod bench_harness;
mod freeze_refusal_rules;
mod quorum_epoch_wire;
mod simulation_message;

/// Implement partition routing from a visible simulation message declaration.
#[moxy::derive(PartitionMessage)]
pub fn partition_message(item: ItemStruct) -> Result<TokenStream, ParseError> {
    simulation_message::expand(item)
}

/// Adapt the external quorum-epoch request types listed at the caller.
#[moxy::function]
pub fn quorum_epoch_requests(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    quorum_epoch_wire::requests(&input.into()).map(Into::into)
}

/// Map native voter listeners to the selected quorum wire endpoint type.
#[moxy::function]
pub fn voter_endpoints(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    quorum_epoch_wire::voter_endpoints(input)
}

/// Supply the common quorum-epoch response layout under an explicit function signature.
#[moxy::function]
pub fn quorum_epoch_encoder(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    quorum_epoch_wire::encoder(input)
}

/// Encode a typed authorization refusal with its optional native message expression.
#[moxy::function]
pub fn authorization_refusal(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    authorization_refusal::expand(input)
}

/// Share an exhaustive freeze mutation inventory under explicit runtime and logical headers.
#[moxy::function]
pub fn freeze_refusal_rules(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    freeze_refusal_rules::expand(input)
}

/// `benches()` and `criterion_main!` for a benchmark suite's target paths.
///
/// `bench_main!(a, m::b)` runs each target in order. A target returns `()` or
/// a `Result`; the first error is printed under the target's path and exits
/// with status 1.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[moxy::function]
pub fn bench_main(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    bench_harness::main(&input.into()).map(Into::into)
}

/// The recording harness of a benchmark suite that measures on after a
/// failure: `BenchResult`, `FirstError`, `run` and `report_failures`.
///
/// # Errors
///
/// Rejects any argument.
#[moxy::function]
pub fn bench_failures(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    bench_harness::failures(&input.into()).map(Into::into)
}

/// Generate one annotated archive-validation walk with coherent proof bindings.
#[moxy::function(name = "validate_archive_rows")]
pub fn validate_archive_rows(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let expanded = archive_rows::expand(tokens.into())?;
    // Creusot matches logical captures to MIR locals by identifier context.
    // Reparse the complete function together so generated and supplied annotations
    // share the same context as the local bindings they describe.
    expanded
        .to_string()
        .parse()
        .map_err(|_| ParseError::new(Span::call_site(), "invalid archive walk expansion"))
}

#[cfg(test)]
mod export_contract_tests {
    use {
        super::*,
        assert2::assert,
        meta::{FunctionMacro, empty_input_accepted},
    };

    const FUNCTION_MACROS: [FunctionMacro; 58] = [
        FunctionMacro {
            name: "cli_main",
            expand: cli_main,
        },
        FunctionMacro {
            name: "bounded_bfs",
            expand: bounded_bfs,
        },
        FunctionMacro {
            name: "compacted_batch",
            expand: compacted_batch,
        },
        FunctionMacro {
            name: "record_batch_fixture",
            expand: record_batch_fixture,
        },
        FunctionMacro {
            name: "simulation_actions",
            expand: simulation_actions,
        },
        FunctionMacro {
            name: "simulation_transport",
            expand: simulation_transport,
        },
        FunctionMacro {
            name: "simulation_heartbeat",
            expand: simulation_heartbeat,
        },
        FunctionMacro {
            name: "benchmark_scenario_fixture",
            expand: benchmark_scenario_fixture,
        },
        FunctionMacro {
            name: "benchmark_latency_fixture",
            expand: benchmark_latency_fixture,
        },
        FunctionMacro {
            name: "s3_archive_config_fixture",
            expand: s3_archive_config_fixture,
        },
        FunctionMacro {
            name: "remote_segment_check",
            expand: remote_segment_check,
        },
        FunctionMacro {
            name: "simulation_step",
            expand: simulation_step,
        },
        FunctionMacro {
            name: "snapshot_topic_fixture",
            expand: snapshot_topic_fixture,
        },
        FunctionMacro {
            name: "timestamped_batch",
            expand: timestamped_batch,
        },
        FunctionMacro {
            name: "producer_batch_fixture",
            expand: producer_batch_fixture,
        },
        FunctionMacro {
            name: "single_replica_partition_fixture",
            expand: single_replica_partition_fixture,
        },
        FunctionMacro {
            name: "control_marker_fixture",
            expand: control_marker_fixture,
        },
        FunctionMacro {
            name: "topic_record_fixture",
            expand: topic_record_fixture,
        },
        FunctionMacro {
            name: "ephemeral_port_fixture",
            expand: ephemeral_port_fixture,
        },
        FunctionMacro {
            name: "minio_fixture",
            expand: minio_fixture,
        },
        FunctionMacro {
            name: "remote_segment_transition_matrix",
            expand: remote_segment_transition_matrix,
        },
        FunctionMacro {
            name: "capture_layer_fixture",
            expand: capture_layer_fixture,
        },
        FunctionMacro {
            name: "remote_segment_fixtures",
            expand: remote_segment_fixtures,
        },
        FunctionMacro {
            name: "remote_metadata_missing",
            expand: remote_metadata_missing,
        },
        FunctionMacro {
            name: "timer_hooks",
            expand: timer_hooks,
        },
        FunctionMacro {
            name: "break_glass_golden_id",
            expand: break_glass_golden_id,
        },
        FunctionMacro {
            name: "file_region_fixture",
            expand: file_region_fixture,
        },
        FunctionMacro {
            name: "empty_endpoint_voters",
            expand: empty_endpoint_voters,
        },
        FunctionMacro {
            name: "epoch_record_batch_fixture",
            expand: epoch_record_batch_fixture,
        },
        FunctionMacro {
            name: "epoch_log_view",
            expand: epoch_log_view,
        },
        FunctionMacro {
            name: "legacy_policy_fixture",
            expand: legacy_policy_fixture,
        },
        FunctionMacro {
            name: "duration_cases_fixture",
            expand: duration_cases_fixture,
        },
        FunctionMacro {
            name: "directory_tree_fixture",
            expand: directory_tree_fixture,
        },
        FunctionMacro {
            name: "parsed_cli_entrypoint",
            expand: parsed_cli_entrypoint,
        },
        FunctionMacro {
            name: "duration_parser_fixture",
            expand: duration_parser_fixture,
        },
        FunctionMacro {
            name: "bootstrap_parser_fixture",
            expand: bootstrap_parser_fixture,
        },
        FunctionMacro {
            name: "snapshot_node_fixture",
            expand: snapshot_node_fixture,
        },
        FunctionMacro {
            name: "format_directories_fixture",
            expand: format_directories_fixture,
        },
        FunctionMacro {
            name: "wal_capture_topic_fixture",
            expand: wal_capture_topic_fixture,
        },
        FunctionMacro {
            name: "flag_metadata_fixture",
            expand: flag_metadata_fixture,
        },
        FunctionMacro {
            name: "bind_retry_fixture",
            expand: bind_retry_fixture,
        },
        FunctionMacro {
            name: "worm_segment_fixture",
            expand: worm_segment_fixture,
        },
        FunctionMacro {
            name: "remote_started_segment",
            expand: remote_started_segment,
        },
        FunctionMacro {
            name: "export_segment_data_fixture",
            expand: export_segment_data_fixture,
        },
        FunctionMacro {
            name: "jvm_checkpoint_dump_fixture",
            expand: jvm_checkpoint_dump_fixture,
        },
        FunctionMacro {
            name: "unix_millis_fixture",
            expand: unix_millis_fixture,
        },
        FunctionMacro {
            name: "vector_request_fixture",
            expand: vector_request_fixture,
        },
        FunctionMacro {
            name: "supported_features_fixture",
            expand: supported_features_fixture,
        },
        FunctionMacro {
            name: "supported_feature_fixture",
            expand: supported_feature_fixture,
        },
        FunctionMacro {
            name: "finalized_feature_fixture",
            expand: finalized_feature_fixture,
        },
        FunctionMacro {
            name: "record_limit_fixture",
            expand: record_limit_fixture,
        },
        FunctionMacro {
            name: "compaction_record_fixture",
            expand: compaction_record_fixture,
        },
        FunctionMacro {
            name: "registration_feature_projection",
            expand: registration_feature_projection,
        },
        FunctionMacro {
            name: "dispatch_capacity_parser",
            expand: dispatch_capacity_parser,
        },
        FunctionMacro {
            name: "frame_max_parser",
            expand: frame_max_parser,
        },
        FunctionMacro {
            name: "simulation_earliest_timer",
            expand: simulation_earliest_timer,
        },
        FunctionMacro {
            name: "splitmix64_step",
            expand: splitmix64_step,
        },
        FunctionMacro {
            name: "compression_bytes_fixture",
            expand: compression_bytes_fixture,
        },
    ];

    #[test]
    fn function_macros_keep_their_empty_input_contract() {
        // Fixtures need arguments.
        assert!(empty_input_accepted(FUNCTION_MACROS).is_empty());
    }
}

mod verified_index;

/// Declare a verified binary-search cursor over named index entries.
#[moxy::function]
pub fn sorted_index_cursor(input: TokenStream) -> Result<TokenStream, ParseError> {
    verified_index::cursor(input)
}

/// Declare a verified last-predecessor lookup over named index entries.
#[moxy::function]
pub fn index_floor_lookup(input: TokenStream) -> Result<TokenStream, ParseError> {
    verified_index::floor(input)
}

/// Add the contract of a remote finished-segment selection.
#[moxy::attribute]
pub fn finished_selection(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    verified_index::finished(meta, item)
}

/// Add the shared domain and proof annotations of a round-robin site lemma.
#[moxy::attribute]
pub fn round_robin_site_lemma(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    verified_index::site(meta, item)
}

mod bounded_unit_modules;

/// Declare concrete bounded human-unit serde modules from one flat table.
#[moxy::function]
pub fn bounded_unit_modules(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    bounded_unit_modules::expand(&input.into()).map(Into::into)
}

mod index_seed_helpers;

/// Declare typed sparse-index fixture helpers with explicit index and key types.
#[moxy::function]
pub fn index_seed_helpers(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    index_seed_helpers::expand(&input.into()).map(Into::into)
}

mod fixture_expressions;

/// Verify a guard response, or report its refusal to the caller's stderr.
#[moxy::function]
pub fn guard_response(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_expressions::guard_response(&input.into()).map(Into::into)
}
