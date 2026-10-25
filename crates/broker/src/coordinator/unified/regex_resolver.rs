//! Resolving the regular expressions of a consumer group against the metadata
//! image: Kafka's `TopicRegexResolver`.
//!
//! A `ConsumerGroupHeartbeat` reaches the group actor with a resolver bound to
//! the request: the metadata image the handler read, and the principal and
//! address of the caller. The actor decides when the group's regular
//! expressions need a resolution (see `actor::regex_resolution`) and calls the
//! resolver only then. The resolution is done with the requesting member's
//! principal, as Kafka does: `AuthorizableRequestContext` of the heartbeat that
//! triggered the refresh decides which of the matching topics the group keeps.

pub use super::consumer_state::ResolvedRegularExpression;
use {
    crate::{
        authorizer::{AuthorizationResult, Authorizer, authorize_topics_logged},
        re2j::compile_full_match,
        time_util::now_ms,
    },
    krabka_metadata::{AclOperation, MetadataImage},
    krabka_security::Principal,
    std::{
        collections::{BTreeSet, HashMap, HashSet},
        fmt,
        net::SocketAddr,
        sync::Arc,
    },
};

/// Resolves regular expressions to topics. See the module documentation.
pub trait TopicRegexResolver: Send + Sync + fmt::Debug {
    /// Kafka's `TopicRegexResolver.resolveRegularExpressions`: each of
    /// `regexes` mapped to the topics that it matches by the whole name and
    /// that the requesting principal may `Describe`, stamped with the version
    /// of the image the topics come from and the time. Every pattern of
    /// `regexes` has an entry. A pattern that does not compile resolves to no
    /// topic, as in Kafka, where the members' patterns were validated when
    /// they arrived.
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression>;
}

/// The resolver of the running broker: it matches against the topics of one
/// metadata image and asks the authorizer whether `principal` may `Describe`
/// each match.
#[derive(derive_more::Debug)]
pub struct ImageTopicRegexResolver {
    #[debug(skip)]
    image: Arc<MetadataImage>,
    /// The version of `image`: the metadata offset that the caller read before
    /// it read the image. The offset can then only be older than the image,
    /// which makes a later refresh happen once too often at worst, never once
    /// too rarely.
    version: i64,
    #[debug(skip)]
    authorizer: Arc<dyn Authorizer>,
    #[debug("{:?}", principal.name)]
    principal: Principal,
    peer: SocketAddr,
}

impl ImageTopicRegexResolver {
    #[must_use]
    pub fn new(
        image: Arc<MetadataImage>,
        version: i64,
        authorizer: Arc<dyn Authorizer>,
        principal: Principal,
        peer: SocketAddr,
    ) -> Self {
        Self {
            image,
            version,
            authorizer,
            principal,
            peer,
        }
    }
}

/// A subscribed pattern and its compiled whole-name matcher.
struct CompiledPattern<'a> {
    pattern: &'a String,
    regex: regex::Regex,
}

/// The fewest patterns for which one [`regex::RegexSet`] pass beats testing
/// each pattern on its own. Below it, a single pattern's literal-prefix
/// optimizations make the per-pattern test faster even at 10 000 topics.
const MIN_SET_PATTERNS: usize = 5;

/// The fewest topic-pattern pairs for which building the set pays for itself:
/// the build costs about as much as testing 50 000 pairs one by one.
const MIN_SET_PAIRS: usize = 50_000;

/// How the topics are tested against the compiled patterns. Both report, for
/// a topic, exactly the patterns whose own regex matches it: a
/// [`regex::RegexSet`] of the same anchored sources is defined to.
enum PatternMatcher {
    /// One `is_match` per pattern and topic.
    EachPattern,
    /// One pass of a set built from the patterns' sources, in `compiled` order.
    Set(regex::RegexSet),
}

impl PatternMatcher {
    fn new(compiled: &[CompiledPattern<'_>], topic_count: usize) -> Self {
        if compiled.len() < MIN_SET_PATTERNS
            || topic_count.saturating_mul(compiled.len()) < MIN_SET_PAIRS
        {
            return Self::EachPattern;
        }
        // A set can exceed the compiled-size limit that each pattern alone
        // stays within; the per-pattern test then still applies.
        regex::RegexSet::new(compiled.iter().map(|compiled| compiled.regex.as_str()))
            .map_or(Self::EachPattern, Self::Set)
    }

    /// Calls `on_match` with the position in `compiled` of every pattern that
    /// matches the whole of `topic`.
    fn for_each_match(
        &self,
        compiled: &[CompiledPattern<'_>],
        topic: &str,
        mut on_match: impl FnMut(usize),
    ) {
        match self {
            Self::EachPattern => compiled
                .iter()
                .enumerate()
                .filter(|(_, compiled)| compiled.regex.is_match(topic))
                .for_each(|(position, _)| on_match(position)),
            Self::Set(set) => set.matches(topic).into_iter().for_each(on_match),
        }
    }
}

impl TopicRegexResolver for ImageTopicRegexResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        let timestamp_ms = now_ms();
        let compiled: Vec<CompiledPattern<'_>> = regexes
            .iter()
            .filter_map(|pattern| {
                // Kafka's `TopicRegexResolver` selects a topic with
                // `Matcher.matches()`, so the pattern must match the whole
                // name.
                match compile_full_match(pattern) {
                    Ok(regex) => Some(CompiledPattern { pattern, regex }),
                    Err(error) => {
                        tracing::error!(%pattern, %error,
                            "ignoring a subscribed topic regex that does not compile");
                        None
                    }
                }
            })
            .collect();
        let mut matched: HashMap<&String, BTreeSet<String>> = regexes
            .iter()
            .map(|pattern| (pattern, BTreeSet::new()))
            .collect();
        let pattern_matcher = PatternMatcher::new(&compiled, self.image.topics().count());
        for topic in self.image.topics() {
            pattern_matcher.for_each_match(&compiled, &topic.name, |position| {
                if let Some(compiled) = compiled.get(position) {
                    matched
                        .entry(compiled.pattern)
                        .or_default()
                        .insert(topic.name.clone());
                }
            });
        }

        // Kafka's `filterTopicDescribeAuthorizedTopics`: every distinct match
        // is authorized once, and a topic that is denied leaves every pattern.
        let candidates: BTreeSet<&str> = matched
            .values()
            .flat_map(|topics| topics.iter().map(String::as_str))
            .collect();
        let denied: HashSet<String> = authorize_topics_logged(
            &*self.authorizer,
            &*self.image,
            &self.principal,
            &self.peer,
            AclOperation::Describe,
            candidates,
            false,
        )
        .into_iter()
        .filter(|(_, decision)| *decision != AuthorizationResult::Allow)
        .map(|(topic, _)| topic.to_owned())
        .collect();

        matched
            .into_iter()
            .map(|(pattern, mut topics)| {
                topics.retain(|topic| !denied.contains(topic));
                (pattern.clone(), ResolvedRegularExpression {
                    topics,
                    version: self.version,
                    timestamp_ms,
                })
            })
            .collect()
    }
}

/// A resolver that finds no topic: the tests of the actor that resolve no
/// pattern use it.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct NoTopicRegexResolver;

#[cfg(test)]
impl TopicRegexResolver for NoTopicRegexResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        regexes
            .iter()
            .map(|pattern| {
                (pattern.clone(), ResolvedRegularExpression {
                    topics: BTreeSet::new(),
                    version: 0,
                    timestamp_ms: 0,
                })
            })
            .collect()
    }
}

/// The resolver that the tests of the actor use when a heartbeat resolves no
/// pattern.
#[cfg(test)]
#[must_use]
pub(crate) fn no_topic_regex_resolver() -> Arc<dyn TopicRegexResolver> {
    Arc::new(NoTopicRegexResolver)
}

/// The time that the tests of the actor put a heartbeat at, in milliseconds
/// since the epoch, and that a [`FixedRegexResolver`] stamps its resolutions
/// with.
#[cfg(test)]
pub(crate) const TEST_NOW_MS: i64 = 1_000_000_000_000;

/// A resolver that answers from a fixed table of what each pattern selects,
/// at version 100 and at [`TEST_NOW_MS`], and counts its calls. A pattern that
/// the table does not list selects no topic.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct FixedRegexResolver {
    topics: HashMap<String, BTreeSet<String>>,
    timestamp_ms: i64,
    calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FixedRegexResolver {
    /// A resolver for `entries`, each a pattern and the topics it selects.
    pub(crate) fn new(entries: &[(&str, &[&str])]) -> Self {
        Self {
            topics: entries
                .iter()
                .map(|(regex, topics)| {
                    (
                        (*regex).to_owned(),
                        topics.iter().map(|topic| (*topic).to_owned()).collect(),
                    )
                })
                .collect(),
            timestamp_ms: TEST_NOW_MS,
            calls: std::sync::atomic::AtomicUsize::default(),
        }
    }

    /// How many times the resolver was asked.
    pub(crate) fn calls(&self) -> usize { self.calls.load(std::sync::atomic::Ordering::SeqCst) }
}

#[cfg(test)]
impl TopicRegexResolver for FixedRegexResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        regexes
            .iter()
            .map(|regex| {
                (regex.clone(), ResolvedRegularExpression {
                    topics: self.topics.get(regex).cloned().unwrap_or_default(),
                    version: 100,
                    timestamp_ms: self.timestamp_ms,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
            test_support::{allow_acl, peer, sasl_principal},
            time_util::now_ms,
        },
        assert2::assert,
        krabka_metadata::{MetadataRecord, TopicRecord},
        std::sync::atomic::{AtomicUsize, Ordering},
    };

    fn image_with_topics(names: &[&str]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (index, name) in names.iter().enumerate() {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: (*name).into(),
                topic_id: uuid::Uuid::from_bytes([u8::try_from(index + 1).unwrap(); 16]),
                partitions: 1,
                replication_factor: 1,
            }));
        }
        image
    }

    fn alice() -> Principal { sasl_principal("alice") }

    /// The positions of the patterns `matcher` reports for `topic`.
    fn matches_of(
        matcher: &PatternMatcher,
        compiled: &[CompiledPattern<'_>],
        topic: &str,
    ) -> Vec<usize> {
        let mut positions = Vec::new();
        matcher.for_each_match(compiled, topic, |position| positions.push(position));
        positions
    }

    /// A set of the anchored sources reports, for every topic, exactly the
    /// patterns that each match it alone: full-name matching, a flag scoped to
    /// its own pattern, alternation that must not leak past the anchors, and
    /// overlapping patterns.
    #[test]
    fn a_set_matches_exactly_the_patterns_that_match_alone() {
        let patterns: Vec<String> = [
            "orders-.*",
            "(?i)PAYMENTS\\..*",
            "a|ab",
            "prod\\.(orders|payments)\\..*",
            ".*-dlq",
            "orders-[0-9]+",
            "",
        ]
        .map(str::to_owned)
        .into();
        let compiled: Vec<CompiledPattern<'_>> = patterns
            .iter()
            .map(|pattern| CompiledPattern {
                pattern,
                regex: compile_full_match(pattern).unwrap(),
            })
            .collect();
        let set = PatternMatcher::Set(
            regex::RegexSet::new(compiled.iter().map(|c| c.regex.as_str())).unwrap(),
        );
        let topics = [
            "orders-1",
            "orders-x-dlq",
            "payments.events",
            "PAYMENTS.events",
            "xpayments.events",
            "ab",
            "abc",
            "a",
            "prod.orders.v1",
            "prod.ads.v1",
            "",
            "Orders-1",
        ];
        for topic in topics {
            assert!(
                matches_of(&set, &compiled, topic)
                    == matches_of(&PatternMatcher::EachPattern, &compiled, topic),
                "topic {topic:?}"
            );
        }
    }

    /// Few patterns, or few topic-pattern pairs, are tested one by one; many
    /// of both build a set.
    #[yare::parameterized(
        one_pattern_many_topics = { 1, 100_000, false },
        below_pattern_floor = { MIN_SET_PATTERNS - 1, 100_000, false },
        below_pair_floor = { MIN_SET_PATTERNS, MIN_SET_PAIRS / MIN_SET_PATTERNS - 1, false },
        at_both_floors = { MIN_SET_PATTERNS, MIN_SET_PAIRS / MIN_SET_PATTERNS, true },
    )]
    fn a_set_is_built_only_where_it_pays(patterns: usize, topics: usize, expect_set: bool) {
        let sources: Vec<String> = (0..patterns).map(|i| format!("t{i}-.*")).collect();
        let compiled: Vec<CompiledPattern<'_>> = sources
            .iter()
            .map(|pattern| CompiledPattern {
                pattern,
                regex: compile_full_match(pattern).unwrap(),
            })
            .collect();
        assert2::assert!(
            matches!(
                PatternMatcher::new(&compiled, topics),
                PatternMatcher::Set(_)
            ) == expect_set
        );
    }

    fn describe_acl(topic: &str) -> MetadataRecord {
        MetadataRecord::V1AccessControlEntry(
            allow_acl()
                .resource_name(topic)
                .operation(AclOperation::Describe)
                .call(),
        )
    }

    fn resolver(image: MetadataImage, authorizer: Arc<dyn Authorizer>) -> ImageTopicRegexResolver {
        ImageTopicRegexResolver::new(Arc::new(image), 42, authorizer, alice(), peer())
    }

    fn resolved(
        resolver: &ImageTopicRegexResolver,
        patterns: &[&str],
    ) -> HashMap<String, Vec<String>> {
        let patterns: BTreeSet<String> = patterns.iter().map(|p| (*p).to_owned()).collect();
        resolver
            .resolve(&patterns)
            .into_iter()
            .map(|(pattern, resolution)| (pattern, resolution.topics.into_iter().collect()))
            .collect()
    }

    /// Kafka's `TopicRegexResolver` selects a topic with `Matcher.matches()`,
    /// so the pattern `orders` selects the topic `orders` and neither
    /// `orders-eu` nor `my-orders`. Every pattern has an entry, and a pattern
    /// that does not compile resolves to no topic.
    #[test]
    fn a_pattern_selects_only_topics_whose_whole_name_matches() {
        struct Row {
            pattern: &'static str,
            /// The topics the pattern selects.
            topics: &'static [&'static str],
        }

        let image = image_with_topics(&["orders", "orders-eu", "my-orders", "audit"]);
        let resolver = resolver(image, Arc::new(AllowAllAuthorizer));
        let rows: [Row; 6] = [
            Row {
                pattern: "orders",
                topics: &["orders"],
            },
            Row {
                pattern: "orders.*",
                topics: &["orders", "orders-eu"],
            },
            Row {
                pattern: ".*orders",
                topics: &["my-orders", "orders"],
            },
            Row {
                pattern: "rders",
                topics: &[],
            },
            Row {
                pattern: "a|orders",
                topics: &["orders"],
            },
            Row {
                pattern: "*invalid",
                topics: &[],
            },
        ];
        let patterns: Vec<&str> = rows.iter().map(|row| row.pattern).collect();
        let expected: HashMap<String, Vec<String>> = rows
            .iter()
            .map(|row| {
                (
                    row.pattern.to_owned(),
                    row.topics.iter().map(|topic| (*topic).to_owned()).collect(),
                )
            })
            .collect();
        assert!(resolved(&resolver, &patterns) == expected);
    }

    /// The resolution carries the version of the image, and the time of the
    /// call.
    #[test]
    fn a_resolution_carries_the_version_and_the_time() {
        let resolver = resolver(image_with_topics(&["a"]), Arc::new(AllowAllAuthorizer));
        let before = now_ms();
        let resolutions = resolver.resolve(&BTreeSet::from(["a".to_owned()]));
        let after = now_ms();
        let resolution = &resolutions["a"];
        assert!(resolution.version == 42);
        assert!((before..=after).contains(&resolution.timestamp_ms));
    }

    /// Kafka's `filterTopicDescribeAuthorizedTopics`: a match that the
    /// requesting principal may not `Describe` leaves every pattern that
    /// selects it, and an allowed match stays.
    #[test]
    fn a_match_the_principal_may_not_describe_is_left_out() {
        let mut image = image_with_topics(&["orders-eu", "orders-us", "shipments"]);
        image.apply(&describe_acl("orders-eu"));
        image.apply(&describe_acl("shipments"));
        let resolver = resolver(image, Arc::new(SimpleAclAuthorizer::new(HashSet::new())));
        let got = resolved(&resolver, &["orders-.*", "shipments", ".*"]);
        assert!(
            got == HashMap::from([
                ("orders-.*".to_owned(), vec!["orders-eu".to_owned()]),
                ("shipments".to_owned(), vec!["shipments".to_owned()]),
                (".*".to_owned(), vec![
                    "orders-eu".to_owned(),
                    "shipments".to_owned()
                ]),
            ])
        );
    }

    /// Each distinct match is authorized once, however many patterns select
    /// it.
    #[test]
    fn each_distinct_match_is_authorized_once() {
        #[derive(Debug, Default)]
        struct Counting(AtomicUsize);
        krabka_macros::test_authorizer! { Counting, _req; {
            self.0.fetch_add(1, Ordering::SeqCst);
            AuthorizationResult::Allow
        }}
        let counting = Arc::new(Counting::default());
        let resolver = resolver(image_with_topics(&["a", "b"]), counting.clone());
        let got = resolved(&resolver, &["a", ".*", "[ab]"]);
        assert!(got["a"] == ["a"]);
        assert!(got[".*"] == ["a", "b"]);
        assert!(counting.0.load(Ordering::SeqCst) == 2);
    }
}
