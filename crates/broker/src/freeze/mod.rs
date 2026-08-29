//! The topic write-freeze registry (KFC-9).
//!
//! A write freeze is a cluster state that no ACL can express. The cluster is
//! up, reads work, and replication runs, but the broker refuses every append
//! that puts new client-authored data into a frozen topic's log. Incident
//! response, migrations, and disaster-recovery promotion each need that state.
//!
//! The registry itself lives in the metadata log as
//! [`krabka_metadata::TopicFreezeRecord`], and
//! [`MetadataImage::topic_freeze`][krabka_metadata::MetadataImage::topic_freeze]
//! holds the index that resolves a topic name to the entry that covers it.
//! This module is the broker half: the produce-path resolver, the signature
//! layer, and the two control-plane RPCs.
//!
//! # Key Modules
//!
//! - [`resolve`] answers "is this topic frozen" for the produce path.
//! - [`signing`] builds the canonical bytes of a freeze record and verifies
//!   the detached operator signature over them.
//! - [`handlers`] is the wire surface: `SetTopicFreeze` (1015) and
//!   `DescribeTopicFreezes` (1016).
//!
//! # A freeze takes one command, and a thaw takes two people
//!
//! A freeze needs no break-glass approval, because an operator must reach it
//! in one command during an incident and freezing is the safe direction. A
//! thaw needs an approved proposal and a signature, because a freeze that one
//! credential can lift is only as strong as that credential.

pub(crate) mod handlers;
pub(crate) mod resolve;
pub(crate) mod signing;

use krabka_metadata::PatternType;

/// The name a scope's pattern type takes in an operator-facing string.
///
/// It is the lowercase spelling of Kafka's ACL pattern type, so an operator
/// who knows `kafka-acls` reads the same vocabulary in an error message, in an
/// audit event, and in a break-glass proposal target.
pub(crate) fn pattern_type_name(pattern_type: PatternType) -> &'static str {
    match pattern_type {
        PatternType::Literal => "literal",
        PatternType::Prefixed => "prefixed",
    }
}

/// The break-glass proposal target that names one registry scope.
///
/// A thaw is a gated transition, and the proposal that authorizes it names its
/// target as `"<pattern>:<scope>"`. Both sides of the check build the string
/// here, so a proposal for `prefixed:tenant-a.` can never authorize a thaw of
/// the literal topic `tenant-a.`.
pub(crate) fn freeze_target(pattern_type: PatternType, scope: &str) -> String {
    format!("{}:{scope}", pattern_type_name(pattern_type))
}

/// Whether a scope covers a topic name that starts with `__`.
///
/// An internal topic is never freezable. A prefix scope of `""`, `"_"`, or
/// `"__"` would otherwise cover `__consumer_offsets` and take the cluster
/// down, and a literal scope can name one directly. The `__` convention is the
/// test rather than the three-name internal-topic list that
/// [`crate::handlers::is_internal_topic`] carries, because that list is stale
/// and a new internal topic would be freezable the day it lands.
pub(crate) fn scope_covers_internal_topic(pattern_type: PatternType, scope: &str) -> bool {
    if scope.starts_with(INTERNAL_TOPIC_PREFIX) {
        return true;
    }
    // A prefix shorter than `__` still covers every `__` name that extends it.
    pattern_type == PatternType::Prefixed && INTERNAL_TOPIC_PREFIX.starts_with(scope)
}

/// The name prefix that marks a broker-owned topic.
const INTERNAL_TOPIC_PREFIX: &str = "__";

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn a_pattern_type_names_itself_as_kafka_acls_spell_it() {
        check!(pattern_type_name(PatternType::Literal) == "literal");
        check!(pattern_type_name(PatternType::Prefixed) == "prefixed");
    }

    #[test]
    fn a_target_joins_the_pattern_type_to_the_scope() {
        for (label, pattern_type, scope, expected) in [
            (
                "a literal scope",
                PatternType::Literal,
                "orders",
                "literal:orders",
            ),
            (
                "a prefixed scope",
                PatternType::Prefixed,
                "tenant-a.",
                "prefixed:tenant-a.",
            ),
            (
                "two scopes that differ only by pattern type",
                PatternType::Literal,
                "tenant-a.",
                "literal:tenant-a.",
            ),
        ] {
            check!(freeze_target(pattern_type, scope) == expected, "{label}");
        }
    }

    #[test]
    fn a_scope_that_reaches_an_internal_topic_is_named_as_one() {
        for (label, pattern_type, scope, expected) in [
            ("a literal topic", PatternType::Literal, "orders", false),
            (
                "a literal internal topic",
                PatternType::Literal,
                "__consumer_offsets",
                true,
            ),
            (
                "a literal name that is only the prefix",
                PatternType::Literal,
                "__",
                true,
            ),
            (
                "a literal name with one underscore",
                PatternType::Literal,
                "_metrics",
                false,
            ),
            (
                "a prefixed namespace",
                PatternType::Prefixed,
                "tenant-a.",
                false,
            ),
            ("an empty prefix", PatternType::Prefixed, "", true),
            ("a one-underscore prefix", PatternType::Prefixed, "_", true),
            ("a two-underscore prefix", PatternType::Prefixed, "__", true),
            (
                "a prefix inside the internal namespace",
                PatternType::Prefixed,
                "__consumer",
                true,
            ),
            (
                "a prefix that starts with one underscore only",
                PatternType::Prefixed,
                "_metrics",
                false,
            ),
        ] {
            check!(
                scope_covers_internal_topic(pattern_type, scope) == expected,
                "{label}"
            );
        }
    }
}
