//! A custom resolver implemented through the broker's public API.
#![cfg(test)]

use {
    assert2::assert,
    krabka_broker::coordinator::unified::regex_resolver::{
        ResolvedRegularExpression, TopicRegexResolver,
    },
    std::collections::{BTreeSet, HashMap},
};

#[derive(Debug)]
struct OrdersResolver;

impl TopicRegexResolver for OrdersResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        regexes.first().map_or_default(|pattern| {
            HashMap::from([(pattern.clone(), ResolvedRegularExpression {
                topics: BTreeSet::from(["orders".to_owned()]),
                version: 42,
                timestamp_ms: 1_000,
            })])
        })
    }
}

#[derive(Default)]
struct CustomResolution {
    patterns: BTreeSet<String>,
    subscriptions: Option<HashMap<String, ResolvedRegularExpression>>,
}

#[rstest::fixture]
fn custom_resolution() -> CustomResolution {
    // Store the public resolver's inputs separately from its observations.
    CustomResolution::default()
}

#[rstest_bdd_macros::given("a custom orders resolver subscription matching {pattern}")]
fn orders_subscription(custom_resolution: &mut CustomResolution, pattern: String) {
    custom_resolution.patterns.insert(pattern);
}

#[rstest_bdd_macros::when(
    "a consumer resolves the subscriptions through the public resolver trait"
)]
fn resolve_orders(custom_resolution: &mut CustomResolution) {
    let resolver: &dyn TopicRegexResolver = &OrdersResolver;
    custom_resolution.subscriptions = Some(resolver.resolve(&custom_resolution.patterns));
}

#[rstest_bdd_macros::then(
    "orders resolves to topic orders at version 42 and timestamp 1000 milliseconds"
)]
fn resolved_orders(custom_resolution: &CustomResolution) {
    assert!(
        custom_resolution
            .subscriptions
            .as_ref()
            .expect("subscriptions resolved")
            == &HashMap::from([("orders.*".to_owned(), ResolvedRegularExpression {
                topics: BTreeSet::from(["orders".to_owned()]),
                version: 42,
                timestamp_ms: 1_000,
            })])
    );
}

rstest_bdd_macros::scenarios!("tests/features/migrated/tests/regex_resolver.feature", fixtures = [custom_resolution: CustomResolution]);
