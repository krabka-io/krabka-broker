//! Scenarios for the Cloud Logging shape of the structured-JSON formatter.
//!
//! Every scenario installs the public `krabka_logfmt::layer` over a captured
//! sink in place of stdout, has the broker log through it, and reads back the
//! line it wrote.

use {
    assert2::assert,
    krabka_bdd::Given,
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
const REQUEST: &str = "Given the broker is handling a produce request for topic <topic>";
const AWKWARD: &str = "When the broker logs a message containing <text>";

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
    Offset,
    Ratio,
    Load,
    Rate,
    Headroom,
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
            Self::Offset => "offset",
            Self::Ratio => "ratio",
            Self::Load => "load",
            Self::Rate => "rate",
            Self::Headroom => "headroom",
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
    Null,
}

// A key the formatter writes on every entry.
#[derive(Clone, Copy, krabka_bdd::StepParam)]
enum EntryKey {
    Severity,
    Target,
    Timestamp,
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

// The code that logs, standing in for a broker module.
mod broker {
    use {
        super::{AwkwardText, EntryKey, EventField},
        std::{error::Error, io, net::SocketAddr},
        tracing::Level,
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
            EventField::Offset => tracing::info!(offset = u64::MAX, "offset committed"),
            EventField::Ratio => tracing::info!(ratio = 0.5_f64, "cleaner ran"),
            EventField::Load => tracing::info!(load = f64::NAN, "request handler sampled"),
            EventField::Rate => tracing::info!(rate = f64::INFINITY, "fetch window closed"),
            EventField::Headroom => {
                tracing::warn!(headroom = f64::NEG_INFINITY, "quota exhausted");
            }
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

    pub fn warning_with(key: EntryKey, value: &str) {
        match key {
            EntryKey::Severity => tracing::warn!(severity = value, "disk almost full"),
            EntryKey::Target => tracing::warn!(target = value, "disk almost full"),
            EntryKey::Timestamp => tracing::warn!(timestamp = value, "disk almost full"),
        }
    }

    pub fn message(text: AwkwardText) {
        tracing::info!("{}", text.message());
    }

    pub fn batch_appended_in_request(topic: &str) {
        let request = tracing::info_span!("produce", topic);
        let _entered = request.enter();
        tracing::info!(base_offset = 40_u64, "batch appended");
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
// and return what it wrote.
#[bon::builder]
fn capture<F: FnOnce()>(
    #[builder(default = LevelFilter::TRACE)] log_level: LevelFilter,
    logs: F,
) -> Written {
    let stdout = CapturedStdout::default();
    let service =
        tracing_subscriber::registry().with(krabka_logfmt::layer(log_level, stdout.clone()));
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
    request_topic: Given<String>,
    sent_message: Given<&'static str>,
    written: Given<Written>,
}

#[rstest::fixture]
fn log_scenario() -> LogScenario {
    LogScenario {
        log_level: LevelFilter::TRACE,
        request_topic: Given::new("request_topic", REQUEST),
        sent_message: Given::new("sent_message", AWKWARD),
        written: Given::new("written", LOGS),
    }
}

impl LogScenario {
    fn logs(&mut self, logs: impl FnOnce()) {
        self.written
            .set(capture().log_level(self.log_level).logs(logs).call());
    }

    fn entry(&self) -> Map<String, Value> { self.written.get().entry() }
}

#[given("the log level has been set to {level}")]
fn log_level_set(log_scenario: &mut LogScenario, level: LogLevel) {
    log_scenario.log_level = LevelFilter::from_level(level.into());
}

#[given("the broker is handling a produce request for topic {topic}")]
fn handling_produce_request(log_scenario: &mut LogScenario, topic: String) {
    log_scenario.request_topic.set(topic);
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

#[when("the broker logs an authorizer decision")]
fn logs_authorizer_decision(log_scenario: &mut LogScenario) {
    log_scenario.logs(broker::authorizer_decision);
}

#[when("the broker logs a warning with a {key} field of {value}")]
fn logs_warning_with(log_scenario: &mut LogScenario, key: EntryKey, value: String) {
    log_scenario.logs(|| broker::warning_with(key, &value));
}

#[when("the broker logs a message containing {text}")]
fn logs_awkward_message(log_scenario: &mut LogScenario, text: AwkwardText) {
    log_scenario.sent_message.set(text.message());
    log_scenario.logs(|| broker::message(text));
}

#[when("the broker logs that a batch was appended")]
fn logs_batch_appended(log_scenario: &mut LogScenario) {
    let topic = log_scenario.request_topic.get().clone();
    log_scenario.logs(|| broker::batch_appended_in_request(&topic));
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

#[then("the entry has no topic field")]
fn entry_has_no_topic(log_scenario: &LogScenario) {
    assert!(log_scenario.entry().get("topic") == None);
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
    let expected = match kind {
        JsonType::Text => Value::String(value),
        JsonType::Boolean => {
            let flag = value.parse::<bool>();
            assert!(let Ok(flag) = flag, "{value} is true or false");
            Value::Bool(flag)
        }
        JsonType::Number => {
            let number = serde_json::from_str::<Value>(&value);
            assert!(let Ok(number @ Value::Number(_)) = number, "{value} is a JSON number");
            number
        }
        JsonType::Null => {
            let null = serde_json::from_str::<Value>(&value);
            assert!(let Ok(null @ Value::Null) = null, "{value} is JSON null");
            null
        }
    };
    assert!(log_scenario.entry().get(field.name()) == Some(&expected));
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
