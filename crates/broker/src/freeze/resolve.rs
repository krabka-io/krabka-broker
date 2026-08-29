//! The produce path's question: is this topic frozen (KFC-9)?
//!
//! [`resolve_topic_freeze`] runs once per topic per produce request, beside
//! the schema-validation resolve and for the same reason. A freeze is a
//! property of the topic and not of a partition or of a batch, so every
//! partition of the topic then pays one test of an [`Option`] and nothing
//! else.
//!
//! The precedence rules live on
//! [`MetadataImage::topic_freeze`][crabka_metadata::MetadataImage::topic_freeze],
//! which holds the two indexes that answer them: a cluster with no freeze
//! costs two emptiness tests, a literal entry beats every prefix entry, the
//! longest matching prefix wins, and a name that starts with `__` never
//! matches. This module adds no rule of its own. It gives the produce path a
//! borrow of the entry, and [`FreezeVerdict`] turns that entry into the
//! `error_message` the producer reads.

use crabka_metadata::{MetadataImage, PatternType, TopicFreezeRecord};

use crate::freeze::pattern_type_name;

/// The write-freeze entry that covers `topic`, or `None` when the topic
/// accepts writes.
///
/// This is the whole of the produce-path gate's read. An unfrozen cluster pays
/// the image's own fast path, and the call allocates nothing.
pub(crate) fn resolve_topic_freeze<'a>(
    image: &'a MetadataImage,
    topic: &str,
) -> Option<&'a TopicFreezeRecord> {
    image.topic_freeze(topic)
}

/// The refusal that one frozen topic gives every produce partition of a
/// request.
///
/// The produce path resolves it once per topic and carries it down to each
/// partition, so a partition of a frozen topic builds its message and nothing
/// more. A partition of an unfrozen topic never sees one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FreezeVerdict {
    /// The registry scope that matched the topic.
    pub scope: String,
    /// The pattern type of that scope.
    pub pattern_type: PatternType,
    /// The text the operator gave when they set the freeze. It can be empty.
    pub reason: String,
}

impl FreezeVerdict {
    /// The `error_message` that rides beside `POLICY_VIOLATION` (44).
    ///
    /// It names the freeze and the scope that matched, so the producer's
    /// on-call reads the state of the cluster rather than guessing at a
    /// misconfigured principal. The operator's reason follows when there is
    /// one.
    pub(crate) fn error_message(&self) -> String {
        self.refusal("this write")
    }

    /// The `error_message` for an operation that would take data *out* of the
    /// frozen topic, which `DeleteRecords` and `DeleteTopics` both do.
    ///
    /// The wording differs from [`Self::error_message`] because the operator is
    /// not producing. Telling someone who ran `kafka-topics --delete` that
    /// their write was refused sends them looking for a producer that does not
    /// exist.
    pub(crate) fn removal_message(&self) -> String {
        self.refusal("this deletion")
    }

    /// The refusal text, with `refused` naming what the freeze turned away.
    ///
    /// It names the freeze and the scope that matched, so the caller's on-call
    /// reads the state of the cluster rather than guessing at a misconfigured
    /// principal. The operator's reason follows when there is one.
    fn refusal(&self, refused: &str) -> String {
        let kind = pattern_type_name(self.pattern_type);
        let scope = &self.scope;
        let mut message = format!("a write freeze on the {kind} scope {scope:?} refuses {refused}");
        if !self.reason.is_empty() {
            message.push_str(": ");
            message.push_str(&self.reason);
        }
        message
    }
}

impl From<&TopicFreezeRecord> for FreezeVerdict {
    fn from(record: &TopicFreezeRecord) -> Self {
        Self {
            scope: record.scope.clone(),
            pattern_type: record.pattern_type,
            reason: record.reason.clone(),
        }
    }
}

/// The refusal that covers `topic`, or `None` when the topic accepts writes.
///
/// This is [`resolve_topic_freeze`] plus the message build, for a caller that
/// wants the finished verdict rather than the record.
pub(crate) fn resolve_freeze_verdict(image: &MetadataImage, topic: &str) -> Option<FreezeVerdict> {
    resolve_topic_freeze(image, topic).map(FreezeVerdict::from)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_metadata::MetadataRecord;
    use uuid::Uuid;

    use super::*;

    fn image() -> MetadataImage {
        MetadataImage::new(Uuid::from_u128(0x5150))
    }

    fn freeze(scope: &str, pattern_type: PatternType, reason: &str) -> TopicFreezeRecord {
        TopicFreezeRecord {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: reason.to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        }
    }

    fn frozen(image: &mut MetadataImage, scope: &str, pattern_type: PatternType, reason: &str) {
        image.apply(&MetadataRecord::V1TopicFreeze(freeze(
            scope,
            pattern_type,
            reason,
        )));
    }

    #[test]
    fn a_cluster_with_no_freeze_resolves_nothing() {
        let image = image();

        for (label, topic) in [
            ("an ordinary topic", "orders"),
            ("an internal topic", "__consumer_offsets"),
            ("an empty name", ""),
        ] {
            check!(resolve_topic_freeze(&image, topic).is_none(), "{label}");
            check!(resolve_freeze_verdict(&image, topic).is_none(), "{label}");
        }
    }

    #[test]
    fn a_literal_scope_beats_a_prefix_and_the_longest_prefix_wins() {
        let mut image = image();
        frozen(&mut image, "tenant-a.", PatternType::Prefixed, "wide");
        frozen(
            &mut image,
            "tenant-a.orders.",
            PatternType::Prefixed,
            "narrow",
        );
        frozen(
            &mut image,
            "tenant-a.orders.eu",
            PatternType::Literal,
            "one topic",
        );

        for (label, topic, expected) in [
            (
                "a literal entry beats every prefix entry",
                "tenant-a.orders.eu",
                Some("tenant-a.orders.eu"),
            ),
            (
                "the longest matching prefix wins",
                "tenant-a.orders.us",
                Some("tenant-a.orders."),
            ),
            (
                "a shorter prefix still covers its namespace",
                "tenant-a.billing",
                Some("tenant-a."),
            ),
            (
                "a topic that no scope covers",
                "tenant-b.orders",
                None::<&str>,
            ),
        ] {
            check!(
                resolve_topic_freeze(&image, topic).map(|f| f.scope.as_str()) == expected,
                "{label}"
            );
            check!(
                resolve_freeze_verdict(&image, topic).map(|v| v.scope)
                    == expected.map(str::to_owned),
                "{label}"
            );
        }
    }

    #[test]
    fn an_internal_topic_resolves_no_freeze_even_under_a_live_registry() {
        for (label, scope, pattern_type, covered) in [
            (
                "an empty prefix scope",
                "",
                PatternType::Prefixed,
                Some("orders"),
            ),
            (
                "a one-underscore prefix scope",
                "_",
                PatternType::Prefixed,
                Some("_metrics"),
            ),
            (
                "a literal internal name",
                "__consumer_offsets",
                PatternType::Literal,
                None,
            ),
        ] {
            let mut image = image();
            frozen(&mut image, scope, pattern_type, "incident");

            check!(
                resolve_topic_freeze(&image, "__consumer_offsets").is_none(),
                "{label}"
            );
            check!(
                resolve_topic_freeze(&image, "__transaction_state").is_none(),
                "{label}"
            );
            if let Some(topic) = covered {
                // The registry is live, so the exemption comes from the name
                // and not from an empty registry.
                check!(resolve_topic_freeze(&image, topic).is_some(), "{label}");
            }
        }
    }

    #[test]
    fn a_verdict_carries_the_scope_the_pattern_type_and_the_reason() {
        let mut image = image();
        frozen(&mut image, "tenant-a.", PatternType::Prefixed, "DR cutover");

        let expected = FreezeVerdict {
            scope: "tenant-a.".to_owned(),
            pattern_type: PatternType::Prefixed,
            reason: "DR cutover".to_owned(),
        };
        check!(resolve_freeze_verdict(&image, "tenant-a.orders") == Some(expected));
    }

    #[test]
    fn a_verdict_message_names_the_freeze_and_the_scope_that_matched() {
        for (label, scope, pattern_type, reason, expected) in [
            (
                "a literal scope with a reason",
                "orders",
                PatternType::Literal,
                "DR cutover",
                "a write freeze on the literal scope \"orders\" refuses this write: DR cutover",
            ),
            (
                "a prefixed scope with a reason",
                "tenant-a.",
                PatternType::Prefixed,
                "tenant offboarding",
                "a write freeze on the prefixed scope \"tenant-a.\" refuses this write: tenant offboarding",
            ),
            (
                "a scope with no reason",
                "orders",
                PatternType::Literal,
                "",
                "a write freeze on the literal scope \"orders\" refuses this write",
            ),
        ] {
            let verdict = FreezeVerdict {
                scope: scope.to_owned(),
                pattern_type,
                reason: reason.to_owned(),
            };
            check!(verdict.error_message() == expected, "{label}");
        }
    }

    #[test]
    fn a_removal_message_says_deletion_rather_than_write() {
        for (label, reason, expected) in [
            (
                "with a reason",
                "DR cutover",
                "a write freeze on the literal scope \"orders\" refuses this deletion: DR cutover",
            ),
            (
                "with no reason",
                "",
                "a write freeze on the literal scope \"orders\" refuses this deletion",
            ),
        ] {
            let verdict = FreezeVerdict {
                scope: "orders".to_owned(),
                pattern_type: PatternType::Literal,
                reason: reason.to_owned(),
            };
            check!(verdict.removal_message() == expected, "{label}");
            // The two messages differ only in what they name as refused. A
            // `DeleteTopics` caller told their *write* was refused goes looking
            // for a producer that is not there.
            check!(
                verdict.removal_message() != verdict.error_message(),
                "{label}"
            );
        }
    }
}
