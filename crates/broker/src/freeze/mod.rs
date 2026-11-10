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

pub mod handlers;
pub mod resolve;
pub mod signing;

use {crate::internal_topics::INTERNAL_TOPIC_PREFIX, krabka_metadata::PatternType};

/// The name a scope's pattern type takes in an operator-facing string.
///
/// It is the lowercase spelling of Kafka's ACL pattern type, so an operator
/// who knows `kafka-acls` reads the same vocabulary in an error message, in an
/// audit event, and in a break-glass proposal target.
pub fn pattern_type_name(pattern_type: PatternType) -> &'static str {
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
pub fn freeze_target(pattern_type: PatternType, scope: &str) -> String {
    format!("{}:{scope}", pattern_type_name(pattern_type))
}

/// Whether a scope covers a topic name that starts with `__`.
///
/// An internal topic is never freezable. A prefix scope of `""`, `"_"`, or
/// `"__"` would otherwise cover `__consumer_offsets` and take the cluster
/// down, and a literal scope can name one directly. The test is the
/// [`INTERNAL_TOPIC_PREFIX`] convention rather than membership of
/// [`crate::internal_topics::INTERNAL_TOPICS`], because the convention is the
/// wider of the two: every name in that set carries the prefix, and a name an
/// operator invented for the prefix is one they should not freeze either.
/// `a_freeze_scope_covers_every_internal_topic` holds the two together, and
/// [`crate::internal_topics::validate_audit_topic_name`] does the same for the
/// audit log, whose name is configured rather than fixed.
pub fn scope_covers_internal_topic(pattern_type: PatternType, scope: &str) -> bool {
    if scope.starts_with(INTERNAL_TOPIC_PREFIX) {
        return true;
    }
    // A prefix shorter than `__` still covers every `__` name that extends it.
    pattern_type == PatternType::Prefixed && INTERNAL_TOPIC_PREFIX.starts_with(scope)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            freeze::handlers::{FreezeFilter, pattern_type_concrete, pattern_type_filter},
            internal_topics::INTERNAL_TOPICS,
        },
        assert2::check,
        krabka_bdd::Given,
    };

    /// A Kafka ACL pattern type, spelled as `kafka-acls` prints it.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
    enum AclPatternType {
        #[step_param("Literal")]
        Literal,
        #[step_param("Prefixed")]
        Prefixed,
    }

    impl From<AclPatternType> for PatternType {
        fn from(pattern: AclPatternType) -> Self {
            match pattern {
                AclPatternType::Literal => Self::Literal,
                AclPatternType::Prefixed => Self::Prefixed,
            }
        }
    }

    /// The kind of freeze scope a request byte names, if it names one.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
    enum ScopeKind {
        Literal,
        Prefixed,
        None,
    }

    impl From<ScopeKind> for Option<PatternType> {
        fn from(kind: ScopeKind) -> Self {
            match kind {
                ScopeKind::Literal => Some(PatternType::Literal),
                ScopeKind::Prefixed => Some(PatternType::Prefixed),
                ScopeKind::None => None,
            }
        }
    }

    /// The registry entries a describe filter selects.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
    enum Selection {
        #[step_param("every entry")]
        Every,
        #[step_param("literal entries")]
        Literal,
        #[step_param("prefixed entries")]
        Prefixed,
        #[step_param("no entry")]
        Nothing,
    }

    impl From<Selection> for FreezeFilter {
        fn from(selection: Selection) -> Self {
            match selection {
                Selection::Every => Self::EveryPatternType,
                Selection::Literal => Self::One(PatternType::Literal),
                Selection::Prefixed => Self::One(PatternType::Prefixed),
                Selection::Nothing => Self::Nothing,
            }
        }
    }

    /// One scenario's pattern type or request byte, and what the broker read.
    struct FreezeScopeVocabulary {
        pattern: Given<PatternType>,
        name: Given<&'static str>,
        byte: Given<i8>,
        kind: Given<Option<PatternType>>,
        filter: Given<FreezeFilter>,
    }

    #[rstest::fixture]
    fn freeze_scope() -> FreezeScopeVocabulary {
        FreezeScopeVocabulary {
            pattern: Given::new(
                "pattern",
                "Given a freeze scope uses Kafka ACL pattern type <pattern>",
            ),
            name: Given::new(
                "name",
                "When the scope's pattern type is rendered for an operator",
            ),
            byte: Given::new(
                "byte",
                "Given a freeze request carries pattern type byte <byte>",
            ),
            kind: Given::new(
                "kind",
                "When the broker reads the byte as the kind of a freeze scope",
            ),
            filter: Given::new(
                "filter",
                "When the broker reads the byte as a describe filter",
            ),
        }
    }

    #[krabka_bdd::given("a freeze scope uses Kafka ACL pattern type {pattern}")]
    fn freeze_scope_pattern(freeze_scope: &mut FreezeScopeVocabulary, pattern: AclPatternType) {
        freeze_scope.pattern.set(pattern.into());
    }

    #[krabka_bdd::when("the scope's pattern type is rendered for an operator")]
    fn render_freeze_scope_pattern(freeze_scope: &mut FreezeScopeVocabulary) {
        freeze_scope
            .name
            .set(pattern_type_name(*freeze_scope.pattern.get()));
    }

    #[krabka_bdd::then("the operator-facing pattern type is {name}")]
    fn operator_freeze_scope_pattern(freeze_scope: &FreezeScopeVocabulary, name: String) {
        check!(*freeze_scope.name.get() == name.as_str());
    }

    #[krabka_bdd::given("a freeze request carries pattern type byte {byte}")]
    fn freeze_request_byte(freeze_scope: &mut FreezeScopeVocabulary, byte: i8) {
        freeze_scope.byte.set(byte);
    }

    #[krabka_bdd::given("a describe request carries pattern type filter byte {byte}")]
    fn describe_filter_byte(freeze_scope: &mut FreezeScopeVocabulary, byte: i8) {
        freeze_scope.byte.set(byte);
    }

    #[krabka_bdd::when("the broker reads the byte as the kind of a freeze scope")]
    fn read_scope_kind(freeze_scope: &mut FreezeScopeVocabulary) {
        freeze_scope
            .kind
            .set(pattern_type_concrete(*freeze_scope.byte.get()));
    }

    #[krabka_bdd::when("the broker reads the byte as a describe filter")]
    fn read_describe_filter(freeze_scope: &mut FreezeScopeVocabulary) {
        freeze_scope
            .filter
            .set(pattern_type_filter(*freeze_scope.byte.get()));
    }

    #[krabka_bdd::then("the freeze scope kind is {kind}")]
    fn freeze_scope_kind(freeze_scope: &FreezeScopeVocabulary, kind: ScopeKind) {
        check!(*freeze_scope.kind.get() == Option::<PatternType>::from(kind));
    }

    #[krabka_bdd::then("the filter selects {selection}")]
    fn filter_selection(freeze_scope: &FreezeScopeVocabulary, selection: Selection) {
        check!(*freeze_scope.filter.get() == FreezeFilter::from(selection));
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/freeze/mod.feature",
        fixtures = [freeze_scope: FreezeScopeVocabulary]
    );

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

    /// The two internal-topic rules have to stay in step: a topic the broker
    /// owns must be unfreezable under both a literal scope that names it and a
    /// prefix scope that reaches it.
    #[test]
    fn a_freeze_scope_covers_every_internal_topic() {
        for name in INTERNAL_TOPICS {
            check!(
                scope_covers_internal_topic(PatternType::Literal, name),
                "literal {name}"
            );
            check!(
                scope_covers_internal_topic(PatternType::Prefixed, name),
                "prefixed {name}"
            );
        }
    }
}
