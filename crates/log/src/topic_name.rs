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

    #[yare::parameterized(
        ordinary = { "orders".to_owned(), Ok(()) },
        allowed_punctuation = { "a.b_c-D9".to_owned(), Ok(()) },
        max_utf16_length = { "a".repeat(MAX_TOPIC_NAME_LENGTH), Ok(()) },
        empty = { String::new(), Err(InvalidTopicName::Empty) },
        dot = { ".".to_owned(), Err(InvalidTopicName::Dot) },
        dot_dot = { "..".to_owned(), Err(InvalidTopicName::DotDot) },
        multiple_dots = { "...".to_owned(), Ok(()) },
        too_long = {
            "a".repeat(MAX_TOPIC_NAME_LENGTH + 1),
            Err(InvalidTopicName::TooLong("a".repeat(MAX_TOPIC_NAME_LENGTH + 1))),
        },
        slash = { "a/b".to_owned(), Err(InvalidTopicName::IllegalCharacter("a/b".to_owned())) },
        parent_path = {
            "../x".to_owned(),
            Err(InvalidTopicName::IllegalCharacter("../x".to_owned())),
        },
        absolute_path = {
            "/tmp/x".to_owned(),
            Err(InvalidTopicName::IllegalCharacter("/tmp/x".to_owned())),
        },
        space = { "a b".to_owned(), Err(InvalidTopicName::IllegalCharacter("a b".to_owned())) },
        non_ascii = { "é".repeat(200), Err(InvalidTopicName::IllegalCharacter("é".repeat(200))) },
        too_long_before_illegal = {
            "a/".repeat(125),
            Err(InvalidTopicName::TooLong("a/".repeat(125))),
        },
        astral_utf16_too_long = {
            "\u{1F600}".repeat(125),
            Err(InvalidTopicName::TooLong("\u{1F600}".repeat(125))),
        },
        astral_utf16_at_limit = {
            format!("a{}", "\u{1F600}".repeat(124)),
            Err(InvalidTopicName::IllegalCharacter(format!("a{}", "\u{1F600}".repeat(124)))),
        },
    )]
    fn validate_topic_name_matches_kafka(name: String, expected: Result<(), InvalidTopicName>) {
        check!(validate_topic_name(&name) == expected, "{name:?}");
    }

    #[yare::parameterized(
        empty = {
            InvalidTopicName::Empty,
            "Topic name is invalid: the empty string is not allowed",
        },
        dot = { InvalidTopicName::Dot, "Topic name is invalid: '.' is not allowed" },
        dot_dot = { InvalidTopicName::DotDot, "Topic name is invalid: '..' is not allowed" },
        too_long = {
            InvalidTopicName::TooLong("x".to_owned()),
            "Topic name is invalid: the length of 'x' is longer than the max allowed length 249",
        },
        illegal_character = {
            InvalidTopicName::IllegalCharacter("a/b".to_owned()),
            "Topic name is invalid: 'a/b' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
        },
    )]
    fn invalid_topic_name_messages_are_kafka_text(error: InvalidTopicName, message: &str) {
        check!(error.to_string() == message);
    }

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

    /// The lookup table accepts exactly the bytes Kafka's legal pattern
    /// `[a-zA-Z0-9._-]` accepts.
    #[test]
    fn every_byte_is_legal_exactly_when_kafka_allows_it() {
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
}
