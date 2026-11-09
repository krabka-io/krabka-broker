//! Structured-JSON `tracing` log formatter shared across Krabka services.
//!
//! Every Krabka service installs this formatter on its stdout `fmt` layer, so
//! container log collectors ingest each line as fields and not as
//! ANSI-coloured human text. Those services are the broker, the gateway, the
//! operator, and the schema-registry. The output is shaped for Google Cloud
//! Logging on GKE. That agent parses stdout JSON and recognises a few special
//! fields:
//!
//! - `severity`: mapped from the `tracing` level, where `WARN` becomes
//!   `WARNING` and `TRACE` becomes `DEBUG`. It sets the log entry's
//!   `LogSeverity`.
//! - `message`: the event message, flattened to the top level, so it becomes
//!   the entry's summary line.
//! - `timestamp`: RFC3339 UTC, recognised as the entry timestamp.
//! - `logging.googleapis.com/trace`, `logging.googleapis.com/spanId` and
//!   `logging.googleapis.com/trace_sampled`: the trace the entry links to in
//!   Cloud Trace. See [Trace correlation](#trace-correlation).
//!
//! The formatter emits everything else at the top level too, that is the event
//! `target`, the fields of the span the event runs in, and the event fields.
//! An event field wins over a span field of the same name. A line looks like
//! this:
//!
//! ```json
//! {"timestamp":"2026-06-13T05:55:09.951788Z","severity":"DEBUG","target":"krabka_broker::network::dispatch","message":"connection opened","listener":"PLAIN","sasl":false}
//! ```
//!
//! Only the current span's fields are written, not those of the spans around
//! it.
//!
//! ## Fields the formatter keeps under `fields`
//!
//! A field whose name the formatter writes itself (`timestamp`, `severity`,
//! `target` and `fields`) is kept under the `fields` object rather than
//! dropped, so an operator still finds it as `jsonPayload.fields.severity`. So
//! is a field named like a Cloud Logging special key, such as `httpRequest`,
//! `time` or anything under `logging.googleapis.com/`. Cloud Logging lifts those
//! keys out of the payload into the log entry itself, and an event must not set
//! them.
//!
//! ## Integers past 2^53
//!
//! Cloud Logging stores a JSON number as a double, which holds every integer up
//! to 2^53 exactly. An integer field whose magnitude exceeds 2^53 is therefore
//! written as a JSON string of its digits, and a smaller one stays a number.
//!
//! ## Trace correlation
//!
//! With a Google Cloud project configured, an entry written inside an
//! OpenTelemetry span carries the three trace keys, the trace as
//! `projects/<project id>/traces/<trace id>`. The span context is the one the
//! `tracing-opentelemetry` layer attaches when it enters a span, so the keys
//! appear only when the service exports its spans over OTLP. Without a
//! project, entries carry no trace keys.
//!
//! The JSON formatter never writes ANSI escape codes, so logs stay clean in
//! non-TTY environments. The default `tracing_subscriber` `fmt` layer emits
//! ANSI colours even when stdout is not a terminal. This crate avoids that
//! bug.

#![forbid(unsafe_code)]

use {
    opentelemetry::{Context, trace::TraceContextExt as _},
    serde_json::{Map, Value},
    std::fmt,
    tracing::{
        Event, Level, Subscriber,
        field::{Field, Visit},
        span,
    },
    tracing_subscriber::{
        Layer, Registry,
        field::RecordFields,
        fmt::{
            FmtContext, FormatEvent, FormatFields, FormattedFields, MakeWriter,
            format::Writer,
            layer as fmt_layer,
            time::{FormatTime, SystemTime},
        },
        layer::Filter,
        registry::LookupSpan,
    },
};

/// The largest magnitude up to which a double holds every integer exactly.
const MAX_EXACT_INTEGER: u64 = 1 << 53;

/// The object that holds the fields the formatter cannot write at the top
/// level.
const FIELDS_KEY: &str = "fields";

/// The keys the formatter writes itself.
const ENTRY_KEYS: [&str; 4] = ["timestamp", "severity", "target", FIELDS_KEY];

/// The Cloud Logging special keys outside [`RESERVED_PREFIX`]. See
/// <https://cloud.google.com/logging/docs/structured-logging#structured_logging_special_fields>.
const SPECIAL_KEYS: [&str; 4] = ["httpRequest", "time", "timestampSeconds", "timestampNanos"];

/// The prefix of the special keys Cloud Logging reserves for itself.
const RESERVED_PREFIX: &str = "logging.googleapis.com/";

const TRACE_KEY: &str = "logging.googleapis.com/trace";
const SPAN_ID_KEY: &str = "logging.googleapis.com/spanId";
const TRACE_SAMPLED_KEY: &str = "logging.googleapis.com/trace_sampled";

/// Map a `tracing` [`Level`] to a Google Cloud Logging `LogSeverity` string.
///
/// `WARN` becomes `WARNING`, which is Cloud Logging's spelling. `TRACE` floors
/// to `DEBUG`, because Cloud Logging has no finer level. See
/// <https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity>.
fn severity(level: Level) -> &'static str {
    match level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARNING",
        Level::INFO => "INFO",
        Level::DEBUG | Level::TRACE => "DEBUG",
    }
}

/// Whether a field named `name` goes under [`FIELDS_KEY`] instead of the top
/// level: the formatter writes that key itself, or Cloud Logging would lift it
/// out of the payload.
fn kept_under_fields(name: &str) -> bool {
    ENTRY_KEYS.contains(&name) || SPECIAL_KEYS.contains(&name) || name.starts_with(RESERVED_PREFIX)
}

/// `value` as a JSON number when a double holds it exactly, and as a string of
/// its digits otherwise.
fn unsigned_integer(value: u128) -> Value {
    match u64::try_from(value) {
        Ok(value) if value <= MAX_EXACT_INTEGER => Value::from(value),
        _ => Value::from(value.to_string()),
    }
}

/// `value` as a JSON number when a double holds it exactly, and as a string of
/// its digits otherwise.
fn signed_integer(value: i128) -> Value {
    match i64::try_from(value) {
        Ok(value) if value.unsigned_abs() <= MAX_EXACT_INTEGER => Value::from(value),
        _ => Value::from(value.to_string()),
    }
}

/// Collects `tracing` fields into a JSON object. It keeps the primitive types
/// and turns everything else into a string with `Debug`.
#[derive(Default)]
struct JsonVisitor {
    fields: Map<String, Value>,
}

impl JsonVisitor {
    fn insert(&mut self, field: &Field, value: Value) {
        self.fields.insert(field.name().to_owned(), value);
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) { self.insert(field, Value::from(value)); }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::from(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, signed_integer(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, unsigned_integer(value.into()));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.insert(field, signed_integer(value));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.insert(field, unsigned_integer(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) { self.insert(field, Value::from(value)); }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // The event message is recorded here as a `fmt::Arguments`, whose
        // `Debug` impl is its `Display` output — so `message` is a clean string,
        // not a quoted one.
        self.insert(field, Value::from(format!("{value:?}")));
    }
}

/// A JSON object as the text [`CloudLoggingFields`] stores, or an empty object
/// for text it did not write.
fn parse_object(text: &str) -> Map<String, Value> { serde_json::from_str(text).unwrap_or_default() }

/// `tracing_subscriber` span field formatter that stores a span's fields as a
/// JSON object, so [`CloudLogging`] can write them on each event in the span.
#[derive(Debug, Clone, Copy, Default)]
pub struct CloudLoggingFields;

impl<'writer> FormatFields<'writer> for CloudLoggingFields {
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        fields.record(&mut visitor);
        write!(writer, "{}", Value::Object(visitor.fields))
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &span::Record<'_>,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor {
            fields: parse_object(&current.fields),
        };
        fields.record(&mut visitor);
        current.fields = Value::Object(visitor.fields).to_string();
        Ok(())
    }
}

/// The fields of the span `ctx`'s event runs in, or none outside a span.
fn current_span_fields<S>(ctx: &FmtContext<'_, S, CloudLoggingFields>) -> Map<String, Value>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let Some(span) = ctx.event_scope().and_then(|mut scope| scope.next()) else {
        return Map::new();
    };
    span.extensions()
        .get::<FormattedFields<CloudLoggingFields>>()
        .map(|fields| parse_object(&fields.fields))
        .unwrap_or_default()
}

/// Add the Cloud Logging trace keys of the active OpenTelemetry span to
/// `entry`, the trace named in `project`. Outside a span, `entry` stays as it
/// is.
fn insert_trace_keys(entry: &mut Map<String, Value>, project: &str) {
    let context = Context::current();
    let span = context.span();
    let span_context = span.span_context();
    if !span_context.is_valid() {
        return;
    }
    let trace = format!("projects/{project}/traces/{}", span_context.trace_id());
    entry.insert(TRACE_KEY.to_owned(), Value::from(trace));
    entry.insert(
        SPAN_ID_KEY.to_owned(),
        Value::from(span_context.span_id().to_string()),
    );
    entry.insert(
        TRACE_SAMPLED_KEY.to_owned(),
        Value::from(span_context.is_sampled()),
    );
}

/// `tracing_subscriber` event formatter that emits one Cloud Logging-friendly
/// JSON object per line. See the crate-level docs for the output shape.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct CloudLogging {
    /// The Google Cloud project whose Cloud Trace holds the traces. Without it,
    /// entries carry no trace keys.
    #[builder(into)]
    gcp_project_id: Option<String>,
}

impl<S> FormatEvent<S, CloudLoggingFields> for CloudLogging
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, CloudLoggingFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut entry = Map::new();

        // Reuse `tracing_subscriber`'s default timer so the timestamp format
        // (RFC3339 UTC, microsecond precision) matches the rest of the
        // ecosystem exactly.
        let mut timestamp = String::new();
        {
            let mut tw = Writer::new(&mut timestamp);
            SystemTime.format_time(&mut tw)?;
        }
        entry.insert("timestamp".to_owned(), Value::from(timestamp));
        entry.insert("severity".to_owned(), Value::from(severity(*meta.level())));
        entry.insert("target".to_owned(), Value::from(meta.target()));

        // The current span's fields, with the event's own fields (the message
        // among them) recorded over them.
        let mut visitor = JsonVisitor {
            fields: current_span_fields(ctx),
        };
        event.record(&mut visitor);
        let mut kept = Map::new();
        for (name, value) in visitor.fields {
            if kept_under_fields(&name) {
                kept.insert(name, value);
            } else {
                entry.insert(name, value);
            }
        }
        if !kept.is_empty() {
            entry.insert(FIELDS_KEY.to_owned(), Value::Object(kept));
        }

        if let Some(project) = &self.gcp_project_id {
            insert_trace_keys(&mut entry, project);
        }

        writeln!(writer, "{}", Value::Object(entry))
    }
}

/// Build a stdout `fmt` layer that emits [`CloudLogging`] JSON, filtered by
/// `filter`.
///
/// `writer` is the sink. Production code passes `std::io::stdout`, and tests
/// pass a capturing buffer.
///
/// `filter` is any per-layer [`Filter`], not only an `EnvFilter`: a service
/// that retargets its log level at runtime passes a filter it can swap out
/// from under this layer.
///
/// `gcp_project_id` is the Google Cloud project whose Cloud Trace holds the
/// service's traces. With it, an entry written inside an OpenTelemetry span
/// links to its trace. Without it, entries carry no trace keys.
///
/// The function returns a boxed layer over a [`Registry`], so call sites
/// compose it with `tracing_subscriber::registry().with(...)`.
#[bon::builder]
pub fn layer<W, F>(
    filter: F,
    writer: W,
    #[builder(into)] gcp_project_id: Option<String>,
) -> Box<dyn Layer<Registry> + Send + Sync>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
    F: Filter<Registry> + Send + Sync + 'static,
{
    fmt_layer()
        .fmt_fields(CloudLoggingFields)
        .event_format(
            CloudLogging::builder()
                .maybe_gcp_project_id(gcp_project_id)
                .build(),
        )
        .with_writer(writer)
        .with_filter(filter)
        .boxed()
}
