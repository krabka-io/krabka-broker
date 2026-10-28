//! The topic-name rules of Apache Kafka's `org.apache.kafka.common.internals.Topic`.
//!
//! A topic name becomes part of a filesystem path: the partition directory is `<log_dir>/<topic>-<partition>`. A name that holds a `/`, or that is `.` or `..`, gives a path outside the log directory. [`validate_topic_name`] is the one check. Every path that creates a topic, and every path that creates or removes a partition directory from a name, calls it. A name that passes gives a partition directory that is a direct child of the log directory.
//!
//! The error text is Kafka's text, because `CreateTopics` sends it to the client as the error message.

use thiserror::Error;

/// Longest topic name Kafka accepts, in UTF-16 code units (`Topic.MAX_NAME_LENGTH`).
pub const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Why a topic name is invalid. The display text is the message of Kafka's `InvalidTopicException` from `Topic.validate`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidTopicName {
    /// The name is the empty string.
    #[error("Topic name is invalid: the empty string is not allowed")]
    Empty,
    /// The name is `.`.
    #[error("Topic name is invalid: '.' is not allowed")]
    Dot,
    /// The name is `..`.
    #[error("Topic name is invalid: '..' is not allowed")]
    DotDot,
    /// The name is longer than [`MAX_TOPIC_NAME_LENGTH`].
    #[error(
        "Topic name is invalid: the length of '{0}' is longer than the max allowed length {MAX_TOPIC_NAME_LENGTH}"
    )]
    TooLong(String),
    /// The name holds a character other than ASCII alphanumerics, `.`, `_` and `-`.
    #[error(
        "Topic name is invalid: '{0}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
    )]
    IllegalCharacter(String),
}

/// Checks `name` against Kafka's `Topic.validate`, in Kafka's order.
///
/// # Errors
///
/// Returns the first [`InvalidTopicName`] rule that `name` breaks.
pub fn validate_topic_name(name: &str) -> Result<(), InvalidTopicName> {
    if name.is_empty() {
        return Err(InvalidTopicName::Empty);
    }
    if name == "." {
        return Err(InvalidTopicName::Dot);
    }
    if name == ".." {
        return Err(InvalidTopicName::DotDot);
    }
    // Java's `String.length()` counts UTF-16 code units. A string holds no
    // more UTF-16 code units than UTF-8 bytes, so a name within the limit in
    // bytes is within it in code units, and only a longer one is counted.
    if name.len() > MAX_TOPIC_NAME_LENGTH && name.encode_utf16().count() > MAX_TOPIC_NAME_LENGTH {
        return Err(InvalidTopicName::TooLong(name.to_owned()));
    }
    // A fold with no early exit and a table lookup per byte lets the
    // compiler check many bytes at once.
    if name.bytes().fold(1, |legal, byte| {
        // Every `u8` indexes the 256-entry table.
        legal & LEGAL_BYTES.get(usize::from(byte)).copied().unwrap_or(0)
    }) == 0
    {
        return Err(InvalidTopicName::IllegalCharacter(name.to_owned()));
    }
    Ok(())
}

/// `1` for each byte Kafka's `Topic.containsValidPattern` accepts (an ASCII
/// alphanumeric, `.`, `_` or `-`) and `0` for every other byte.
const LEGAL_BYTES: [u8; 256] = {
    let mut table = [0; 256];
    let mut rest: &mut [u8] = &mut table;
    let mut byte = 0;
    while let [slot, tail @ ..] = rest {
        // The byte values of `0-9`, `A-Z`, `a-z`, `.`, `_` and `-`: the
        // index is a `usize`, which a byte literal is not.
        if matches!(byte, 0x30..=0x39 | 0x41..=0x5A | 0x61..=0x7A | 0x2E | 0x5F | 0x2D) {
            *slot = 1;
        }
        rest = tail;
        byte += 1;
    }
    table
};

/// Whether `a` and `b` are the same name after `.` becomes `_` (Kafka's `Topic.hasCollision`).
///
/// Kafka metric names cannot tell `.` from `_`, so Kafka refuses to create a topic that collides with an existing topic.
#[must_use]
pub fn topic_names_collide(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| unify_collision_byte(x) == unify_collision_byte(y))
}

fn unify_collision_byte(byte: u8) -> u8 { if byte == b'.' { b'_' } else { byte } }

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, std::collections::BTreeSet};

    #[derive(Default)]
    struct TopicNameScenario {
        name: String,
        checked: Option<Result<(), InvalidTopicName>>,
        error: Option<InvalidTopicName>,
        rendered: String,
        bytes: Vec<u8>,
        legal: Option<BTreeSet<u8>>,
    }
    #[rstest::fixture]
    fn topic_name() -> TopicNameScenario {
        let name = String::new();
        TopicNameScenario {
            name,
            ..TopicNameScenario::default()
        }
    }
    #[rstest_bdd_macros::given("log topic name {name}")]
    fn log_topic_text(topic_name: &mut TopicNameScenario, name: String) { topic_name.name = name; }
    #[rstest_bdd_macros::given("an empty log topic name")]
    fn empty_log_topic(topic_name: &mut TopicNameScenario) { topic_name.name.clear(); }
    #[rstest_bdd_macros::given(
        "a log topic name repeating {fragment} {count} times with prefix {prefix}"
    )]
    fn repeated_log_topic(
        topic_name: &mut TopicNameScenario,
        fragment: String,
        count: usize,
        prefix: String,
    ) {
        topic_name.name = format!(
            "{}{}",
            if prefix == "none" { "" } else { &prefix },
            fragment.repeat(count)
        );
    }
    #[rstest_bdd_macros::when("the log topic name is validated")]
    fn validate_log_topic(topic_name: &mut TopicNameScenario) {
        topic_name.checked = Some(validate_topic_name(&topic_name.name));
    }
    #[rstest_bdd_macros::then("the log topic name is accepted")]
    fn accepted_log_topic(topic_name: &TopicNameScenario) {
        check!(topic_name.checked == Some(Ok(())), "{:?}", topic_name.name);
    }
    #[rstest_bdd_macros::then("the log topic name is refused as {reason}")]
    fn rejected_log_topic(topic_name: &TopicNameScenario, reason: String) {
        let expected = match reason.as_str() {
            "empty" => InvalidTopicName::Empty,
            "dot" => InvalidTopicName::Dot,
            "dot-dot" => InvalidTopicName::DotDot,
            "too-long" => InvalidTopicName::TooLong(topic_name.name.clone()),
            "illegal-character" => InvalidTopicName::IllegalCharacter(topic_name.name.clone()),
            _ => panic!("unknown topic name refusal {reason}"),
        };
        check!(
            topic_name.checked == Some(Err(expected)),
            "{:?}",
            topic_name.name
        );
    }
    #[rstest_bdd_macros::given("topic-name error {kind} for text {name}")]
    fn log_topic_error(topic_name: &mut TopicNameScenario, kind: String, name: String) {
        topic_name.error = Some(match kind.as_str() {
            "empty" => InvalidTopicName::Empty,
            "dot" => InvalidTopicName::Dot,
            "dot-dot" => InvalidTopicName::DotDot,
            "too-long" => InvalidTopicName::TooLong(name),
            "illegal-character" => InvalidTopicName::IllegalCharacter(name),
            _ => panic!("unknown topic name error {kind}"),
        });
    }
    #[rstest_bdd_macros::when("the topic-name error is displayed")]
    fn render_log_topic_error(topic_name: &mut TopicNameScenario) {
        topic_name.rendered = topic_name
            .error
            .as_ref()
            .expect("topic name error given")
            .to_string();
    }
    #[rstest_bdd_macros::then("the Kafka topic diagnostic is {message}")]
    fn kafka_topic_diagnostic(topic_name: &TopicNameScenario, message: String) {
        check!(topic_name.rendered == message);
    }
    #[rstest_bdd_macros::given("all 256 possible topic-name byte values")]
    fn log_topic_bytes(topic_name: &mut TopicNameScenario) {
        topic_name.bytes = (0..=u8::MAX).collect();
    }
    #[rstest_bdd_macros::when("the topic-name byte lookup table classifies them")]
    fn classify_topic_bytes(topic_name: &mut TopicNameScenario) {
        topic_name.legal = Some(
            topic_name
                .bytes
                .iter()
                .copied()
                .filter(|byte| LEGAL_BYTES[usize::from(*byte)] == 1)
                .collect(),
        );
    }
    #[rstest_bdd_macros::then(
        "only ASCII letters digits dot underscore and hyphen are legal topic bytes"
    )]
    fn legal_kafka_topic_bytes(topic_name: &TopicNameScenario) {
        let expected: BTreeSet<u8> = (b'a'..=b'z')
            .chain(b'A'..=b'Z')
            .chain(b'0'..=b'9')
            .chain(*b"._-")
            .collect();
        check!(topic_name.bytes.len() == 256);
        check!(topic_name.legal == Some(expected));
    }
    rstest_bdd_macros::scenarios!("tests/features/log_topic_names", fixtures = [topic_name: TopicNameScenario]);

    #[yare::parameterized(
        dot_to_underscore = { "a.b", "a_b", true },
        underscore_to_dot = { "a_b", "a.b", true },
        equal_dot = { "a.b", "a.b", true },
        hyphen_differs = { "a.b", "a-b", false },
        longer_name_differs = { "a.b", "a.bc", false },
        distinct_names = { "orders", "orders2", false },
    )]
    fn topic_names_collide_unifies_dot_and_underscore(a: &str, b: &str, expected: bool) {
        check!(topic_names_collide(a, b) == expected, "{a} {b}");
    }
}
