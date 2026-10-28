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

    #[cfg(test)]
    #[rstest_bdd_macros::then("topic name tests: validate topic name matches kafka for {example}")]
    fn bdd_verify_validate_topic_name_matches_kafka(example: String) {
        type Example = (String, Result<(), InvalidTopicName>);
        let (name, expected): Example = match example.as_str() {
            "ordinary" => ("orders".to_owned(), Ok(())),
            "allowed punctuation" => ("a.b_c-D9".to_owned(), Ok(())),
            "max utf16 length" => ("a".repeat(MAX_TOPIC_NAME_LENGTH), Ok(())),
            "empty" => (String::new(), Err(InvalidTopicName::Empty)),
            "dot" => (".".to_owned(), Err(InvalidTopicName::Dot)),
            "dot dot" => ("..".to_owned(), Err(InvalidTopicName::DotDot)),
            "multiple dots" => ("...".to_owned(), Ok(())),
            "too long" => (
                "a".repeat(MAX_TOPIC_NAME_LENGTH + 1),
                Err(InvalidTopicName::TooLong(
                    "a".repeat(MAX_TOPIC_NAME_LENGTH + 1),
                )),
            ),
            "slash" => (
                "a/b".to_owned(),
                Err(InvalidTopicName::IllegalCharacter("a/b".to_owned())),
            ),
            "parent path" => (
                "../x".to_owned(),
                Err(InvalidTopicName::IllegalCharacter("../x".to_owned())),
            ),
            "absolute path" => (
                "/tmp/x".to_owned(),
                Err(InvalidTopicName::IllegalCharacter("/tmp/x".to_owned())),
            ),
            "space" => (
                "a b".to_owned(),
                Err(InvalidTopicName::IllegalCharacter("a b".to_owned())),
            ),
            "non ascii" => (
                "é".repeat(200),
                Err(InvalidTopicName::IllegalCharacter("é".repeat(200))),
            ),
            "too long before illegal" => (
                "a/".repeat(125),
                Err(InvalidTopicName::TooLong("a/".repeat(125))),
            ),
            "astral utf16 too long" => (
                "\u{1F600}".repeat(125),
                Err(InvalidTopicName::TooLong("\u{1F600}".repeat(125))),
            ),
            "astral utf16 at limit" => (
                format!("a{}", "\u{1F600}".repeat(124)),
                Err(InvalidTopicName::IllegalCharacter(format!(
                    "a{}",
                    "\u{1F600}".repeat(124)
                ))),
            ),
            _ => panic!("unknown BDD example: {example}"),
        };

        check!(validate_topic_name(&name) == expected, "{name:?}");
    }

    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/topic_name.feature",
        name = "tests: validate topic name matches kafka"
    )]
    fn validate_topic_name_matches_kafka(example: String) { let _ = example; }

    #[cfg(test)]
    #[rstest_bdd_macros::then(
        "topic name tests: invalid topic name messages are kafka text for {example}"
    )]
    fn bdd_verify_invalid_topic_name_messages_are_kafka_text(example: String) {
        type Example<'a> = (InvalidTopicName, &'a str);
        let (error, message): Example<'_> = match example.as_str() {
            "empty" => (
                InvalidTopicName::Empty,
                "Topic name is invalid: the empty string is not allowed",
            ),
            "dot" => (
                InvalidTopicName::Dot,
                "Topic name is invalid: '.' is not allowed",
            ),
            "dot dot" => (
                InvalidTopicName::DotDot,
                "Topic name is invalid: '..' is not allowed",
            ),
            "too long" => (
                InvalidTopicName::TooLong("x".to_owned()),
                "Topic name is invalid: the length of 'x' is longer than the max allowed length 249",
            ),
            "illegal character" => (
                InvalidTopicName::IllegalCharacter("a/b".to_owned()),
                "Topic name is invalid: 'a/b' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
            ),
            _ => panic!("unknown BDD example: {example}"),
        };

        check!(error.to_string() == message);
    }

    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/topic_name.feature",
        name = "tests: invalid topic name messages are kafka text"
    )]
    fn invalid_topic_name_messages_are_kafka_text(example: String) { let _ = example; }

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

    #[cfg(test)]
    #[rstest_bdd_macros::then("topic name tests: every byte is legal exactly when kafka allows it")]
    fn bdd_verify_every_byte_is_legal_exactly_when_kafka_allows_it() {
        let expected: BTreeSet<u8> = (b'a'..=b'z')
            .chain(b'A'..=b'Z')
            .chain(b'0'..=b'9')
            .chain(*b"._-")
            .collect();
        let actual: BTreeSet<u8> = (0..=u8::MAX)
            .filter(|&byte| LEGAL_BYTES[usize::from(byte)] == 1)
            .collect();
        check!(actual == expected);
    }

    /// The lookup table accepts exactly the bytes Kafka's legal pattern
    /// `[a-zA-Z0-9._-]` accepts.
    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/topic_name.feature",
        name = "tests: every byte is legal exactly when kafka allows it"
    )]
    fn every_byte_is_legal_exactly_when_kafka_allows_it() {}
}
