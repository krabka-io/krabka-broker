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

#[cfg(test)]
#[rstest_bdd_macros::then("regex resolver: a consumer can implement and call a custom resolver")]
fn bdd_verify_a_consumer_can_implement_and_call_a_custom_resolver() {
    let resolver: &dyn TopicRegexResolver = &OrdersResolver;
    let subscriptions = resolver.resolve(&BTreeSet::from(["orders.*".to_owned()]));
    assert!(
        subscriptions
            == HashMap::from([("orders.*".to_owned(), ResolvedRegularExpression {
                topics: BTreeSet::from(["orders".to_owned()]),
                version: 42,
                timestamp_ms: 1_000,
            },)])
    );
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/regex_resolver.feature",
    name = "a consumer can implement and call a custom resolver"
)]
fn a_consumer_can_implement_and_call_a_custom_resolver() {}
