//! Scenarios for the Cloud Logging shape of the structured-JSON formatter.
//!
//! Every scenario installs the public `krabka_logfmt::layer` over a captured
//! sink in place of stdout, has the broker log through it, and reads back the
//! line it wrote. A scenario that traces its request also installs the
//! `tracing-opentelemetry` layer the services install for OTLP export, over a
//! tracer that hands out the scenario's own ids.

use {
    assert2::assert,
    krabka_bdd::Given,
    opentelemetry::trace::{SpanId, TraceId, TracerProvider as _},
    opentelemetry_sdk::trace::{IdGenerator, Sampler, SdkTracerProvider},
    rstest_bdd_macros::{given, then, when},
    serde_json::{Map, Value},
    std::{
        io::{self, Write},
        sync::{Arc, Mutex, PoisonError},
    },
    time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339},
    tracing::{Level, level_filters::LevelFilter, subscriber},
    tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt as _},
};

const LOGS: &str = "When the broker logs ...";
const AWKWARD: &str = "When the broker logs a message containing <text>";

// The prefix of the keys Cloud Logging reserves for itself, the trace keys
// among them.
const RESERVED_PREFIX: &str = "logging.googleapis.com/";

// A tracing level, as a step names it.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum LogLevel {
    #[step_param("ERROR")]
    Error,
    #[step_param("WARN")]
    Warn,
    #[step_param("INFO")]
    Info,
    #[step_param("DEBUG")]
    Debug,
    #[step_param("TRACE")]
    Trace,
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => Self::ERROR,
            LogLevel::Warn => Self::WARN,
            LogLevel::Info => Self::INFO,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Trace => Self::TRACE,
        }
    }
}

// An event field the broker logs, each with a value of its own kind.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum EventField {
    Listener,
    Sasl,
    Delta,
    Ratio,
    Replicas,
    Peer,
    Error,
}

impl EventField {
    fn name(self) -> &'static str {
        match self {
            Self::Listener => "listener",
            Self::Sasl => "sasl",
            Self::Delta => "delta",
            Self::Ratio => "ratio",
            Self::Replicas => "replicas",
            Self::Peer => "peer",
            Self::Error => "error",
        }
    }
}

// The JSON type a step expects a field value to have.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum JsonType {
    Text,
    Boolean,
    Number,
}

impl JsonType {
    // `value` as a JSON value of this type.
    fn value(self, value: String) -> Value {
        match self {
            Self::Text => Value::String(value),
            Self::Boolean => {
                let flag = value.parse::<bool>();
                assert!(let Ok(flag) = flag, "{value} is true or false");
                Value::Bool(flag)
            }
            Self::Number => {
                let number = serde_json::from_str::<Value>(&value);
                assert!(let Ok(number @ Value::Number(_)) = number, "{value} is a JSON number");
                number
            }
        }
    }
}

// The integer types a field value can have, by width and sign.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum IntegerWidth {
    #[step_param("unsigned 64-bit")]
    Unsigned64,
    #[step_param("signed 64-bit")]
    Signed64,
    #[step_param("unsigned 128-bit")]
    Unsigned128,
    #[step_param("signed 128-bit")]
    Signed128,
}

// A key the formatter writes on every entry.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum EntryKey {
    Severity,
    Target,
    Timestamp,
    Fields,
}

// A key Cloud Logging lifts out of the payload into the log entry itself.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum SpecialKey {
    #[step_param("httpRequest")]
    HttpRequest,
    #[step_param("time")]
    Time,
    #[step_param("timestampSeconds")]
    TimestampSeconds,
    #[step_param("logging.googleapis.com/trace")]
    Trace,
    #[step_param("logging.googleapis.com/labels")]
    Labels,
}

// Message text that a careless line writer would break.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum AwkwardText {
    #[step_param("a line break")]
    LineBreak,
    #[step_param("a double quote")]
    DoubleQuote,
    #[step_param("a backslash")]
    Backslash,
    #[step_param("a terminal escape code")]
    TerminalEscape,
}

impl AwkwardText {
    fn message(self) -> &'static str {
        match self {
            Self::LineBreak => "replica fetcher stopped\ncaused by: connection reset",
            Self::DoubleQuote => "topic \"orders\" created",
            Self::Backslash => r"log dir C:\krabka\data offline",
            Self::TerminalEscape => "\u{1b}[31mpartition offline\u{1b}[0m",
        }
    }
}

// The produce request an event is logged within.
#[derive(Clone, Default)]
struct ProduceRequest {
    topic: Option<String>,
    // The error code the request span records once it has opened.
    error_code: Option<String>,
}

// The trace OTLP export puts a request span in: the ids the tracer hands out
// and whether the sampler keeps the span.
#[derive(Clone, Copy, Debug)]
struct RequestTrace {
    trace_id: TraceId,
    span_id: SpanId,
    sampled: bool,
}

impl IdGenerator for RequestTrace {
    fn new_trace_id(&self) -> TraceId { self.trace_id }

    fn new_span_id(&self) -> SpanId { self.span_id }
}

impl RequestTrace {
    // A tracer provider that exports nothing and hands out these ids.
    fn provider(self) -> SdkTracerProvider {
        let sampler = if self.sampled {
            Sampler::AlwaysOn
        } else {
            Sampler::AlwaysOff
        };
        SdkTracerProvider::builder()
            .with_id_generator(self)
            .with_sampler(sampler)
            .build()
    }
}

// The code that logs, standing in for a broker module.
mod broker {
    use {
        super::{AwkwardText, EntryKey, EventField, IntegerWidth, ProduceRequest, SpecialKey},
        std::{error::Error, io, net::SocketAddr},
        tracing::{Level, field::Empty},
    };

    // The module path tracing records for events logged here.
    pub const MODULE: &str = module_path!();

    pub fn connection_opened() {
        tracing::info!(
            target: "krabka_broker::network::dispatch",
            listener = "PLAIN",
            sasl = false,
            port = 9092,
            "connection opened"
        );
    }

    pub fn connections_opened(count: u32) {
        tracing::info!("opened {count} connections");
    }

    pub fn offset_only() {
        tracing::info!(offset = 42_u64);
    }

    pub fn at_level(level: Level) {
        match level {
            Level::ERROR => tracing::error!("replica fetcher stopped"),
            Level::WARN => tracing::warn!("disk almost full"),
            Level::INFO => tracing::info!("partition leader elected"),
            Level::DEBUG => tracing::debug!("fetch session evicted"),
            Level::TRACE => tracing::trace!("fetch response sized"),
        }
    }

    pub fn authorizer_decision() {
        tracing::info!(target: "kafka.authorizer.logger", operation = "Write", "principal allowed");
    }

    pub fn with_field(field: EventField) {
        match field {
            EventField::Listener => tracing::info!(listener = "PLAIN", "listener bound"),
            EventField::Sasl => tracing::info!(sasl = false, "listener bound"),
            EventField::Delta => tracing::info!(delta = -3_i64, "lag changed"),
            EventField::Ratio => tracing::info!(ratio = 0.5_f64, "cleaner ran"),
            EventField::Replicas => tracing::info!(replicas = ?vec![1, 2, 3], "partition created"),
            EventField::Peer => {
                let peer: SocketAddr = ([10, 0, 0, 7], 9092).into();
                tracing::info!(peer = %peer, "connection opened");
            }
            EventField::Error => {
                let error = io::Error::other("disk full");
                tracing::error!(error = &error as &dyn Error, "log dir offline");
            }
        }
    }

    pub fn integer(width: IntegerWidth, value: &str) {
        match width {
            IntegerWidth::Unsigned64 => {
                let integer = value.parse::<u64>();
                assert2::assert!(let Ok(integer) = integer, "{value} is an unsigned 64-bit integer");
                tracing::info!(integer, "counter sampled");
            }
            IntegerWidth::Signed64 => {
                let integer = value.parse::<i64>();
                assert2::assert!(let Ok(integer) = integer, "{value} is a signed 64-bit integer");
                tracing::info!(integer, "counter sampled");
            }
            IntegerWidth::Unsigned128 => {
                let integer = value.parse::<u128>();
                assert2::assert!(let Ok(integer) = integer, "{value} is an unsigned 128-bit integer");
                tracing::info!(integer, "counter sampled");
            }
            IntegerWidth::Signed128 => {
                let integer = value.parse::<i128>();
                assert2::assert!(let Ok(integer) = integer, "{value} is a signed 128-bit integer");
                tracing::info!(integer, "counter sampled");
            }
        }
    }

    pub fn warning_with(key: EntryKey, value: &str) {
        match key {
            EntryKey::Severity => tracing::warn!(severity = value, "disk almost full"),
            EntryKey::Target => tracing::warn!(target = value, "disk almost full"),
            EntryKey::Timestamp => tracing::warn!(timestamp = value, "disk almost full"),
            EntryKey::Fields => tracing::warn!(fields = value, "disk almost full"),
        }
    }

    pub fn with_special_key(key: SpecialKey, value: &str) {
        match key {
            SpecialKey::HttpRequest => tracing::info!("httpRequest" = value, "request served"),
            SpecialKey::Time => tracing::info!("time" = value, "request served"),
            SpecialKey::TimestampSeconds => {
                tracing::info!("timestampSeconds" = value, "request served");
            }
            SpecialKey::Trace => {
                tracing::info!("logging.googleapis.com/trace" = value, "request served");
            }
            SpecialKey::Labels => {
                tracing::info!("logging.googleapis.com/labels" = value, "request served");
            }
        }
    }

    pub fn message(text: AwkwardText) {
        tracing::info!("{}", text.message());
    }

    // Run `logs` within the span of `request`, or outside any span without one.
    pub fn within(request: Option<&ProduceRequest>, logs: impl FnOnce()) {
        let Some(request) = request else {
            logs();
            return;
        };
        let span = tracing::info_span!("produce", topic = request.topic, error_code = Empty);
        if let Some(error_code) = &request.error_code {
            span.record("error_code", error_code.as_str());
        }
        span.in_scope(logs);
    }

    pub fn batch_appended() {
        tracing::info!(base_offset = 40_u64, "batch appended");
    }

    pub fn batch_appended_to(topic: &str) {
        tracing::info!(base_offset = 40_u64, topic, "batch appended");
    }

    pub fn segment_rolled(partition: u32) {
        tracing::info_span!("roll", partition).in_scope(|| {
            tracing::info!(base_offset = 40_u64, "segment rolled");
        });
    }
}

// The stand-in for stdout: it keeps every byte the layer writes.
#[derive(Clone, Default)]
struct CapturedStdout(Arc<Mutex<Vec<u8>>>);

impl CapturedStdout {
    fn text(&self) -> String {
        let bytes = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let text = String::from_utf8(bytes);
        assert!(let Ok(text) = text, "the formatter writes UTF-8");
        text
    }
}

impl Write for CapturedStdout {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl<'a> MakeWriter<'a> for CapturedStdout {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer { self.clone() }
}

// What the broker wrote to stdout, and the wall-clock window it wrote in.
struct Written {
    text: String,
    before: OffsetDateTime,
    after: OffsetDateTime,
}

impl Written {
    // The one entry written, as a JSON object.
    fn entry(&self) -> Map<String, Value> {
        let lines: Vec<&str> = self.text.lines().collect();
        assert!(let [line] = lines.as_slice(), "exactly one entry is written");
        let entry = serde_json::from_str::<Value>(line);
        assert!(let Ok(Value::Object(entry)) = entry, "an entry is a JSON object");
        entry
    }

    fn timestamp(&self) -> String {
        let entry = self.entry();
        let timestamp = entry.get("timestamp").cloned();
        assert!(let Some(Value::String(timestamp)) = timestamp, "the entry has a text timestamp");
        timestamp
    }
}

// Install the formatter at `log_level` over a captured stdout, run `logs`,
// and return what it wrote. With a `trace`, the OTLP tracing layer traces the
// spans `logs` opens.
#[bon::builder]
fn capture<F: FnOnce()>(
    #[builder(default = LevelFilter::TRACE)] log_level: LevelFilter,
    gcp_project_id: Option<String>,
    trace: Option<RequestTrace>,
    logs: F,
) -> Written {
    let stdout = CapturedStdout::default();
    let provider = trace.map(RequestTrace::provider);
    let otlp = provider
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("broker")));
    let service = tracing_subscriber::registry()
        .with(
            krabka_logfmt::layer()
                .filter(log_level)
                .writer(stdout.clone())
                .maybe_gcp_project_id(gcp_project_id)
                .call(),
        )
        .with(otlp);
    let now = OffsetDateTime::now_utc();
    // The formatter writes microseconds, so the window opens on one.
    let before = now.replace_microsecond(now.microsecond());
    assert!(let Ok(before) = before);
    subscriber::with_default(service, logs);
    let after = OffsetDateTime::now_utc();
    Written {
        text: stdout.text(),
        before,
        after,
    }
}

struct LogScenario {
    // The broker's configured log level. A scenario that does not set it
    // writes every event, so it is a default and not a `Given`.
    log_level: LevelFilter,
    // Without a configured project, entries carry no trace keys.
    gcp_project_id: Option<String>,
    // Without a request, the broker logs outside any span.
    request: Option<ProduceRequest>,
    // Without a trace, OTLP export is off and no span is traced.
    trace: Option<RequestTrace>,
    sent_message: Given<&'static str>,
    written: Given<Written>,
}

#[rstest::fixture]
fn log_scenario() -> LogScenario {
    LogScenario {
        log_level: LevelFilter::TRACE,
        gcp_project_id: None,
        request: None,
        trace: None,
        sent_message: Given::new("sent_message", AWKWARD),
        written: Given::new("written", LOGS),
    }
}

impl LogScenario {
    // Run `logs` within the scenario's request, if it has one, and keep what
    // the broker wrote.
    fn logs(&mut self, logs: impl FnOnce()) {
        let request = self.request.clone();
        self.written.set(
            capture()
                .log_level(self.log_level)
                .maybe_gcp_project_id(self.gcp_project_id.clone())
                .maybe_trace(self.trace)
                .logs(|| broker::within(request.as_ref(), logs))
                .call(),
        );
    }

    fn entry(&self) -> Map<String, Value> { self.written.get().entry() }

    // The value at `path`: a top-level key, or `fields.<name>` for a key in the
    // fields object.
    fn entry_value(&self, path: &str) -> Option<Value> {
        let entry = self.entry();
        match path.split_once('.') {
            Some(("fields", name)) => entry.get("fields")?.get(name).cloned(),
            _ => entry.get(path).cloned(),
        }
    }

    // The keys under Cloud Logging's reserved prefix, the trace keys among
    // them.
    fn trace_keys(&self) -> Map<String, Value> {
        self.entry()
            .into_iter()
            .filter(|(key, _)| key.starts_with(RESERVED_PREFIX))
            .collect()
    }
}

fn trace_id(hex: &str) -> TraceId {
    let id = TraceId::from_hex(hex);
    assert!(let Ok(id) = id, "{hex} is a trace id");
    id
}

fn span_id(hex: &str) -> SpanId {
    let id = SpanId::from_hex(hex);
    assert!(let Ok(id) = id, "{hex} is a span id");
    id
}

#[given("the log level has been set to {level}")]
fn log_level_set(log_scenario: &mut LogScenario, level: LogLevel) {
    log_scenario.log_level = LevelFilter::from_level(level.into());
}

#[given("the broker has been configured for Google Cloud project {project}")]
fn project_configured(log_scenario: &mut LogScenario, project: String) {
    log_scenario.gcp_project_id = Some(project);
}

#[given("the broker is handling a produce request for topic {topic}")]
fn handling_produce_request(log_scenario: &mut LogScenario, topic: String) {
    log_scenario.request = Some(ProduceRequest {
        topic: Some(topic),
        error_code: None,
    });
}

#[given("the broker is handling a produce request that has recorded error code {code}")]
fn handling_failed_produce_request(log_scenario: &mut LogScenario, code: String) {
    log_scenario.request = Some(ProduceRequest {
        topic: None,
        error_code: Some(code),
    });
}

#[given("the broker is tracing a produce request as span {span} of trace {trace}")]
fn tracing_produce_request(log_scenario: &mut LogScenario, span: String, trace: String) {
    log_scenario.request = Some(ProduceRequest::default());
    log_scenario.trace = Some(RequestTrace {
        trace_id: trace_id(&trace),
        span_id: span_id(&span),
        sampled: true,
    });
}

#[given("the broker is tracing a produce request that the trace sampler dropped")]
fn tracing_dropped_produce_request(log_scenario: &mut LogScenario) {
    log_scenario.request = Some(ProduceRequest::default());
    log_scenario.trace = Some(RequestTrace {
        trace_id: trace_id("4bf92f3577b34da6a3ce929d0e0e4736"),
        span_id: span_id("00f067aa0ba902b7"),
        sampled: false,
    });
}

#[when("the broker logs an event at level {level}")]
fn logs_at_level(log_scenario: &mut LogScenario, level: LogLevel) {
    log_scenario.logs(|| broker::at_level(level.into()));
}

#[when("the broker logs that a connection opened")]
fn logs_connection_opened(log_scenario: &mut LogScenario) {
    log_scenario.logs(broker::connection_opened);
}

#[when("the broker logs that {count} connections opened")]
fn logs_connections_opened(log_scenario: &mut LogScenario, count: u32) {
    log_scenario.logs(|| broker::connections_opened(count));
}

#[when("the broker logs an event with only an offset field")]
fn logs_offset_only(log_scenario: &mut LogScenario) { log_scenario.logs(broker::offset_only); }

#[when("the broker logs an event with a {field} field")]
fn logs_with_field(log_scenario: &mut LogScenario, field: EventField) {
    log_scenario.logs(|| broker::with_field(field));
}

#[when("the broker logs a {width} integer field of {value}")]
fn logs_integer(log_scenario: &mut LogScenario, width: IntegerWidth, value: String) {
    log_scenario.logs(|| broker::integer(width, &value));
}

#[when("the broker logs an authorizer decision")]
fn logs_authorizer_decision(log_scenario: &mut LogScenario) {
    log_scenario.logs(broker::authorizer_decision);
}

#[when("the broker logs a warning with a {key} field of {value}")]
fn logs_warning_with(log_scenario: &mut LogScenario, key: EntryKey, value: String) {
    log_scenario.logs(|| broker::warning_with(key, &value));
}

#[when("the broker logs an event with a field named {key} of {value}")]
fn logs_with_special_key(log_scenario: &mut LogScenario, key: SpecialKey, value: String) {
    log_scenario.logs(|| broker::with_special_key(key, &value));
}

#[when("the broker logs a message containing {text}")]
fn logs_awkward_message(log_scenario: &mut LogScenario, text: AwkwardText) {
    log_scenario.sent_message.set(text.message());
    log_scenario.logs(|| broker::message(text));
}

#[when("the broker logs that a batch was appended")]
fn logs_batch_appended(log_scenario: &mut LogScenario) {
    log_scenario.logs(broker::batch_appended);
}

#[when("the broker logs that a batch was appended to topic {topic}")]
fn logs_batch_appended_to(log_scenario: &mut LogScenario, topic: String) {
    log_scenario.logs(|| broker::batch_appended_to(&topic));
}

#[when("the broker logs that a segment rolled for partition {partition} within the request")]
fn logs_segment_rolled(log_scenario: &mut LogScenario, partition: u32) {
    log_scenario.logs(|| broker::segment_rolled(partition));
}

#[then("the entry severity is {severity}")]
fn entry_severity(log_scenario: &LogScenario, severity: String) {
    assert!(log_scenario.entry().get("severity") == Some(&Value::from(severity)));
}

#[then("the entry message is {message}")]
fn entry_message(log_scenario: &LogScenario, message: String) {
    assert!(log_scenario.entry().get("message") == Some(&Value::from(message)));
}

#[then("the entry has no message")]
fn entry_has_no_message(log_scenario: &LogScenario) {
    assert!(log_scenario.entry().get("message") == None);
}

#[then("the entry has no {key} field")]
fn entry_has_no_key(log_scenario: &LogScenario, key: String) {
    assert!(log_scenario.entry().get(&key) == None);
}

#[then("the entry apart from its timestamp reads")]
fn entry_apart_from_timestamp(log_scenario: &LogScenario, docstring: String) {
    let mut entry = log_scenario.entry();
    entry.remove("timestamp");
    let expected = serde_json::from_str::<Value>(&docstring);
    assert!(let Ok(expected) = expected, "the expected entry is JSON");
    assert!(Value::Object(entry) == expected);
}

#[then("the entry field {field} is the {kind} {value}")]
fn entry_field_typed(log_scenario: &LogScenario, field: EventField, kind: JsonType, value: String) {
    assert!(log_scenario.entry().get(field.name()) == Some(&kind.value(value)));
}

#[then("the entry integer field is the {kind} {value}")]
fn entry_integer(log_scenario: &LogScenario, kind: JsonType, value: String) {
    assert!(log_scenario.entry().get("integer") == Some(&kind.value(value)));
}

#[then("the entry field {path} reads {text}")]
fn entry_field_reads(log_scenario: &LogScenario, path: String, text: String) {
    assert!(log_scenario.entry_value(&path) == Some(Value::String(text)));
}

#[then("the entry trace keys read")]
fn entry_trace_keys(log_scenario: &LogScenario, docstring: String) {
    let expected = serde_json::from_str::<Value>(&docstring);
    assert!(let Ok(expected) = expected, "the expected trace keys are JSON");
    assert!(Value::Object(log_scenario.trace_keys()) == expected);
}

#[then("the entry carries no trace keys")]
fn entry_carries_no_trace_keys(log_scenario: &LogScenario) {
    assert!(log_scenario.trace_keys() == Map::new());
}

#[then("the entry is marked as not sampled")]
fn entry_not_sampled(log_scenario: &LogScenario) {
    let sampled = log_scenario.entry_value("logging.googleapis.com/trace_sampled");
    assert!(sampled == Some(Value::Bool(false)));
}

#[then("the entry target is the module path of the logging code")]
fn entry_target_is_module_path(log_scenario: &LogScenario) {
    assert!(log_scenario.entry().get("target") == Some(&Value::from(broker::MODULE)));
}

#[then("the entry target reads {target}")]
fn entry_target(log_scenario: &LogScenario, target: String) {
    assert!(log_scenario.entry().get("target") == Some(&Value::from(target)));
}

#[then("the entry timestamp is the time of writing in RFC 3339 UTC")]
fn entry_timestamp_is_time_of_writing(log_scenario: &LogScenario) {
    let written = log_scenario.written.get();
    let timestamp = written.timestamp();
    let instant = OffsetDateTime::parse(&timestamp, &Rfc3339);
    assert!(let Ok(instant) = instant, "{timestamp} is RFC 3339");
    assert!(timestamp.ends_with('Z'));
    assert!(instant.offset() == UtcOffset::UTC);
    assert!(
        (written.before..=written.after).contains(&instant),
        "{timestamp} is the time of writing"
    );
}

#[then("the output is one JSON line")]
fn output_is_one_json_line(log_scenario: &LogScenario) {
    let text = &log_scenario.written.get().text;
    assert!(text.ends_with('\n'));
    assert!(text.lines().count() == 1);
    assert!(serde_json::from_str::<Value>(text).is_ok_and(|entry| entry.is_object()));
}

#[then("the entry message reads back unchanged")]
fn message_reads_back_unchanged(log_scenario: &LogScenario) {
    let entry = log_scenario.entry();
    let sent = *log_scenario.sent_message.get();
    assert!(entry.get("message") == Some(&Value::from(sent)));
}

#[then("the output holds no terminal escape code")]
fn output_holds_no_escape(log_scenario: &LogScenario) {
    assert!(!log_scenario.written.get().text.contains('\u{1b}'));
}

#[then("nothing is written")]
fn nothing_written(log_scenario: &LogScenario) {
    assert!(log_scenario.written.get().text.is_empty());
}

#[then("one entry is written")]
fn one_entry_written(log_scenario: &LogScenario) {
    assert!(log_scenario.written.get().text.lines().count() == 1);
}

krabka_bdd::scenarios!("tests/features", fixtures = [log_scenario: LogScenario]);
