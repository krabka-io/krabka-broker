//! Subscription matching for KIP-714 client metrics: which subscriptions a
//! connecting client matches, the union of the metric prefixes and the
//! smallest push interval across them, and the stable subscription id derived
//! from that result.

use {
    super::{ClientAttributes, ComputedSubscription},
    crate::client_metrics::config::{self, ALL_METRICS, MatchRule, MatchSelector},
    krabka_java_regex::MatchError,
    krabka_metadata::MetadataImage,
    krabka_protocol::records::crc32c,
    std::{
        borrow::Cow,
        collections::{BTreeMap, HashMap},
        mem,
    },
    uuid::Uuid,
};

/// A `match` selector of a subscription that the matcher could not test
/// against a client: the search ran past the backtrack limit, or the input
/// was one the matcher refuses.
///
/// Java's `Matcher.matches()` answers every such test, however slowly, so
/// reading the failure as no match would hide the subscription from the
/// client without a word. The request fails instead, as an SSL principal
/// mapping rule past the limit fails the connection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("subscription {subscription}: cannot test its {selector:?} selector: {source}")]
pub struct SelectorError {
    pub subscription: String,
    pub selector: MatchSelector,
    #[source]
    pub source: MatchError,
}

/// One subscription's configs and the parts of them that matching reads,
/// parsed when the subscription changes rather than once per client.
#[derive(Debug)]
struct CompiledSubscription {
    name: String,
    configs: BTreeMap<String, String>,
    /// The compiled `match` selectors, or `None` when they do not parse, which
    /// leaves the subscription matching no client.
    rules: Option<Vec<MatchRule>>,
    metrics: Vec<String>,
}

impl CompiledSubscription {
    fn new(name: &str, configs: &BTreeMap<String, String>) -> Self {
        let rules = configs
            .get(config::KEY_MATCH)
            .map_or_else(|| Ok(Vec::new()), |v| config::parse_match_rules(v))
            .ok();
        let metrics = configs
            .get(config::KEY_METRICS)
            .map_or_else(Vec::new, |v| config::parse_metrics(v));
        Self {
            name: name.to_owned(),
            configs: configs.clone(),
            rules,
            metrics,
        }
    }
}

/// The client-metrics subscriptions of a metadata image with their `match`
/// patterns compiled, kept in the image's iteration order.
///
/// [`Self::observe`] recompiles only a subscription whose configs changed, so
/// a `java.util.regex.Pattern` is compiled once per subscription change, not
/// once per new client instance.
#[derive(Debug, Default)]
pub struct Subscriptions {
    entries: Vec<CompiledSubscription>,
    by_name: HashMap<String, usize>,
    /// How many subscriptions have been compiled, for the tests that pin the
    /// reuse.
    #[cfg(test)]
    compilations: usize,
}

impl Subscriptions {
    /// The subscriptions of `image`, all compiled.
    #[cfg(test)]
    pub(crate) fn from_image(image: &MetadataImage) -> Self {
        let mut subscriptions = Self::default();
        subscriptions.observe(image);
        subscriptions
    }

    /// Brings the set in line with `image`, and says whether anything changed:
    /// Kafka's `subscriptionUpdateVersion` moves exactly then.
    pub(crate) fn observe(&mut self, image: &MetadataImage) -> bool {
        let unchanged = image.client_metrics_subscriptions().count() == self.entries.len()
            && image.client_metrics_subscriptions().all(|(name, configs)| {
                self.get(name)
                    .is_some_and(|compiled| compiled.configs == *configs)
            });
        if unchanged {
            return false;
        }
        let mut previous: HashMap<String, CompiledSubscription> = mem::take(&mut self.entries)
            .into_iter()
            .map(|compiled| (compiled.name.clone(), compiled))
            .collect();
        for (name, configs) in image.client_metrics_subscriptions() {
            let compiled = match previous.remove(name.as_str()) {
                Some(compiled) if compiled.configs == *configs => compiled,
                _ => {
                    #[cfg(test)]
                    {
                        self.compilations += 1;
                    }
                    CompiledSubscription::new(name, configs)
                }
            };
            self.entries.push(compiled);
        }
        self.by_name = self
            .entries
            .iter()
            .enumerate()
            .map(|(position, compiled)| (compiled.name.clone(), position))
            .collect();
        true
    }

    fn get(&self, name: &str) -> Option<&CompiledSubscription> {
        let &position = self.by_name.get(name)?;
        self.entries.get(position)
    }

    /// Kafka's `ClientMetricsManager.createClientInstance`: the push interval
    /// starts at `default_interval_ms` (Kafka's
    /// [`config::INTERVAL_MS_DEFAULT`] unless the operator moved it) and every
    /// matched subscription lowers it, whether or not it names any metric.
    ///
    /// # Errors
    ///
    /// A [`SelectorError`] for the first `match` selector that the matcher
    /// cannot test against `attrs`.
    pub(crate) fn compute(
        &self,
        attrs: &ClientAttributes,
        default_interval_ms: i32,
    ) -> Result<ComputedSubscription, SelectorError> {
        let mut matched_metrics: Vec<String> = Vec::new();
        let mut push_interval_ms = default_interval_ms;
        let mut any_star = false;

        for compiled in &self.entries {
            let Some(rules) = &compiled.rules else {
                continue;
            };
            if !selectors_match(&compiled.name, rules, attrs)? {
                continue;
            }
            for m in &compiled.metrics {
                if m == ALL_METRICS {
                    any_star = true;
                }
                if !matched_metrics.contains(m) {
                    matched_metrics.push(m.clone());
                }
            }
            push_interval_ms = push_interval_ms.min(config::effective_interval_ms(
                &compiled.configs,
                default_interval_ms,
            ));
        }

        let metrics = if any_star {
            vec![ALL_METRICS.to_owned()]
        } else {
            matched_metrics
        };
        Ok(ComputedSubscription {
            metrics,
            push_interval_ms,
        })
    }

    #[cfg(test)]
    pub(crate) fn compilations(&self) -> usize { self.compilations }
}

/// [`Subscriptions::compute`] over the subscriptions of `image`, compiled for
/// this one call.
#[cfg(test)]
pub fn compute_subscription(
    image: &MetadataImage,
    attrs: &ClientAttributes,
    default_interval_ms: i32,
) -> ComputedSubscription {
    Subscriptions::from_image(image)
        .compute(attrs, default_interval_ms)
        .expect("no selector of the test subscriptions fails to test")
}

/// Whether every selector of subscription `name` matches `attrs`. Like
/// Kafka's `ClientMetricsInstanceMetadata.isMatch`, it stops at the first
/// selector that does not match.
fn selectors_match(
    name: &str,
    rules: &[MatchRule],
    attrs: &ClientAttributes,
) -> Result<bool, SelectorError> {
    for rule in rules {
        let matched = selector_matches(rule, attrs).map_err(|source| SelectorError {
            subscription: name.to_owned(),
            selector: rule.selector,
            source,
        })?;
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn selector_matches(rule: &MatchRule, attrs: &ClientAttributes) -> Result<bool, MatchError> {
    use config::MatchSelector::{
        Id, InstanceId, SoftwareName, SoftwareVersion, SourceAddress, SourcePort,
    };
    let target: Cow<'_, str> = match rule.selector {
        InstanceId => attrs.client_instance_id.to_string().into(),
        Id => (&attrs.client_id).into(),
        SoftwareName => (&attrs.software_name).into(),
        SoftwareVersion => (&attrs.software_version).into(),
        SourceAddress => (&attrs.source_address).into(),
        SourcePort => attrs.source_port.to_string().into(),
    };
    // `JavaPattern::matches` is Kafka's full match.
    rule.pattern.matches(target.as_ref())
}

/// Stable, change-sensitive subscription id.
///
/// It is the CRC32C over a canonical, sorted rendering of the metric set and
/// the push interval, XOR-ed with the instance-id hash. It stays consistent
/// across a re-fetch. It is not byte-identical to the JVM broker's id, and it
/// does not need to be.
pub fn subscription_id(sub: &ComputedSubscription, client_instance_id: Uuid) -> i32 {
    let mut sorted = sub.metrics.clone();
    sorted.sort();
    let rendered = format!("[{}]{}", sorted.join(", "), sub.push_interval_ms);
    // CRC32C output is u32; reinterpreting as i32 is intentional — the value
    // is used only for equality checks, not arithmetic.
    let crc = crc32c(rendered.as_bytes()).cast_signed();
    crc ^ uuid_hashcode(client_instance_id)
}

/// Reproduces `java.util.UUID.hashCode()`, so the shape matches.
fn uuid_hashcode(id: Uuid) -> i32 {
    let (msb, lsb) = id.as_u64_pair();
    let [h0, h1, h2, h3, l0, l1, l2, l3] = (msb ^ lsb).to_be_bytes();
    let high = i32::from_be_bytes([h0, h1, h2, h3]);
    let low = i32::from_be_bytes([l0, l1, l2, l3]);
    high ^ low
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::client_metrics::manager::test_support::{
            ConfigEntry, apply_config, attrs, img_with,
        },
        assert2::{assert, check},
        krabka_bdd::Given,
        krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord},
        std::{convert::Infallible, str::FromStr},
        uuid::Uuid,
    };

    /// `subscription_id` of `[a., b.]` at 60000 ms for instance id 7, as the
    /// `crc32c` crate computed it before the switch to `crc-fast`.
    const PINNED_SUBSCRIPTION_ID: i32 = 1_450_358_637;

    /// The subscription of a broker that keeps Kafka's default interval.
    fn compute_subscription(
        image: &MetadataImage,
        attrs: &ClientAttributes,
    ) -> ComputedSubscription {
        super::compute_subscription(image, attrs, config::INTERVAL_MS_DEFAULT)
    }

    /// The `client_metrics_default_interval` runtime key moves the interval
    /// that a client with no subscription, or a subscription with no
    /// `interval.ms`, is given; a subscription still lowers it (#1246).
    #[test]
    fn the_default_interval_is_the_one_the_broker_is_given() {
        let rows = [
            ("no subscription", MetadataImage::new(Uuid::nil()), 600_000),
            (
                "a subscription without interval.ms",
                img_with("all", &[ConfigEntry {
                    key: "metrics",
                    value: "*",
                }]),
                600_000,
            ),
            (
                "a subscription below the default",
                img_with("all", &[
                    ConfigEntry {
                        key: "metrics",
                        value: "*",
                    },
                    ConfigEntry {
                        key: "interval.ms",
                        value: "60000",
                    },
                ]),
                60_000,
            ),
            (
                "a subscription above the default",
                img_with("all", &[
                    ConfigEntry {
                        key: "metrics",
                        value: "*",
                    },
                    ConfigEntry {
                        key: "interval.ms",
                        value: "3600000",
                    },
                ]),
                600_000,
            ),
        ];
        for (name, img, expected) in rows {
            assert!(
                super::compute_subscription(&img, &attrs(), 600_000).push_interval_ms == expected,
                "row {name}"
            );
        }
    }

    /// The id is pinned: brokers of two versions in one cluster must hand a
    /// client the same id for the same subscription, so the CRC-32C
    /// implementation behind it must not change the value.
    #[test]
    fn subscription_id_is_pinned() {
        let sub = ComputedSubscription {
            metrics: vec!["b.".into(), "a.".into()],
            push_interval_ms: 60_000,
        };
        let rendered = b"[a., b.]60000";
        let instance = Uuid::from_u128(7);
        check!(crc32c(rendered) == crc32c::crc32c(rendered));
        check!(subscription_id(&sub, instance) == PINNED_SUBSCRIPTION_ID);
    }

    /// A subscription is compiled when it first appears and again only when
    /// its configs change; an unchanged image reports no change.
    #[test]
    fn match_rules_compile_once_per_subscription_change() {
        let java_only = [
            ConfigEntry {
                key: "metrics",
                value: "a.",
            },
            ConfigEntry {
                key: "match",
                value: "client_software_name=apache-kafka-.*",
            },
        ];
        let mut img = img_with("s1", &java_only);
        apply_config(&mut img, "s2", &[ConfigEntry {
            key: "metrics",
            value: "b.",
        }]);
        let mut subscriptions = Subscriptions::default();
        let first = subscriptions.observe(&img);
        let unchanged = subscriptions.observe(&img);
        let computed = subscriptions
            .compute(&attrs(), config::INTERVAL_MS_DEFAULT)
            .unwrap();
        check!((first, unchanged, subscriptions.compilations()) == (true, false, 2));
        let mut metrics = computed.metrics;
        metrics.sort();
        check!(metrics == ["a.", "b."]);

        apply_config(&mut img, "s2", &[ConfigEntry {
            key: "metrics",
            value: "c.",
        }]);
        let changed = subscriptions.observe(&img);
        let computed = subscriptions
            .compute(&attrs(), config::INTERVAL_MS_DEFAULT)
            .unwrap();
        check!((changed, subscriptions.compilations()) == (true, 3));
        let mut metrics = computed.metrics;
        metrics.sort();
        check!(metrics == ["a.", "c."]);
    }

    /// A selector that runs the matcher past its backtrack limit fails the
    /// computation and names its subscription and selector, where it used to
    /// read as no match.
    #[test]
    fn a_selector_past_the_backtrack_limit_names_its_subscription() {
        let img = img_with("slow", &[
            ConfigEntry {
                key: "metrics",
                value: "*",
            },
            ConfigEntry {
                key: "match",
                value: r"client_id=((x+x+)+)\2y",
            },
        ]);
        let client = ClientAttributes {
            client_id: "x".repeat(29),
            ..attrs()
        };
        check!(
            Subscriptions::from_image(&img)
                .compute(&client, config::INTERVAL_MS_DEFAULT)
                .map_err(|error| (error.subscription, error.selector))
                == Err(("slow".to_owned(), MatchSelector::Id))
        );
    }

    /// A selector that does not match ends the test, so the slow selector
    /// after it never runs and the client is simply not subscribed.
    #[test]
    fn a_selector_that_does_not_match_stops_before_a_slow_one() {
        let img = img_with("slow", &[
            ConfigEntry {
                key: "metrics",
                value: "*",
            },
            ConfigEntry {
                key: "match",
                value: r"client_software_name=other,client_id=((x+x+)+)\2y",
            },
        ]);
        let client = ClientAttributes {
            client_id: "x".repeat(29),
            ..attrs()
        };
        check!(
            Subscriptions::from_image(&img).compute(&client, config::INTERVAL_MS_DEFAULT)
                == Ok(ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: config::INTERVAL_MS_DEFAULT,
                })
        );
    }

    #[test]
    fn subscription_id_stable_and_change_sensitive() {
        let a = attrs();
        let s1 = ComputedSubscription {
            metrics: vec!["a.".into(), "b.".into()],
            push_interval_ms: 60_000,
        };
        let id1 = subscription_id(&s1, a.client_instance_id);
        let s1b = ComputedSubscription {
            metrics: vec!["b.".into(), "a.".into()],
            push_interval_ms: 60_000,
        };
        check!(id1 == subscription_id(&s1b, a.client_instance_id));
        let s2 = ComputedSubscription {
            metrics: vec!["a.".into(), "b.".into()],
            push_interval_ms: 30_000,
        };
        check!(id1 != subscription_id(&s2, a.client_instance_id));
        let s3 = ComputedSubscription {
            metrics: vec!["a.".into()],
            push_interval_ms: 60_000,
        };
        check!(id1 != subscription_id(&s3, a.client_instance_id));
    }

    /// A comma-separated list of metric prefixes, as a scenario writes the
    /// metrics a client receives.
    struct MetricPrefixes(Vec<String>);

    impl FromStr for MetricPrefixes {
        type Err = Infallible;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            Ok(Self(text.split(", ").map(String::from).collect()))
        }
    }

    /// The client-metrics subscriptions a scenario configures, by name, and
    /// what the broker computes for the client.
    struct MetricsSubscription {
        subscriptions: BTreeMap<String, BTreeMap<String, String>>,
        computed: Given<ComputedSubscription>,
    }

    impl MetricsSubscription {
        fn configure(&mut self, name: String, key: &str, value: impl Into<String>) {
            self.subscriptions
                .entry(name)
                .or_default()
                .insert(key.to_owned(), value.into());
        }

        /// Computes the subscription the configured subscriptions give
        /// `client`.
        fn compute_for(&mut self, client: &ClientAttributes) {
            let mut image = MetadataImage::new(Uuid::nil());
            for (name, configs) in &self.subscriptions {
                image.apply(&MetadataRecord::V1ClientMetricsConfig(
                    ClientMetricsConfigRecord {
                        name: name.clone(),
                        configs: configs.clone(),
                    },
                ));
            }
            self.computed.set(compute_subscription(&image, client));
        }

        /// What the client receives, its metrics sorted: the image keeps the
        /// subscriptions in no defined order, as Kafka's map does, so the
        /// union has none either.
        fn received(&self) -> ComputedSubscription {
            let mut received = self.computed.get().clone();
            received.metrics.sort();
            received
        }
    }

    #[rstest::fixture]
    fn metrics_subscription() -> MetricsSubscription {
        MetricsSubscription {
            subscriptions: BTreeMap::new(),
            computed: Given::new(
                "computed",
                "When client <client> running <software> asks for its metrics subscription",
            ),
        }
    }

    #[krabka_bdd::given("a subscription {name} selects metrics {metrics}")]
    fn subscription_selects(
        metrics_subscription: &mut MetricsSubscription,
        name: String,
        metrics: String,
    ) {
        metrics_subscription.configure(name, config::KEY_METRICS, metrics);
    }

    #[krabka_bdd::given(
        "a subscription {name} pushes metrics {metrics} every {interval} milliseconds"
    )]
    fn subscription_pushes(
        metrics_subscription: &mut MetricsSubscription,
        name: String,
        metrics: String,
        interval: i32,
    ) {
        metrics_subscription.configure(name.clone(), config::KEY_METRICS, metrics);
        metrics_subscription.configure(name, config::KEY_INTERVAL_MS, interval.to_string());
    }

    #[krabka_bdd::given(
        "a subscription {name} pushes an empty metrics list every {interval} milliseconds"
    )]
    fn subscription_pushes_nothing(
        metrics_subscription: &mut MetricsSubscription,
        name: String,
        interval: i32,
    ) {
        subscription_pushes(metrics_subscription, name, String::new(), interval);
    }

    #[krabka_bdd::given(
        "a subscription {name} sets only a push interval of {interval} milliseconds"
    )]
    fn subscription_sets_interval(
        metrics_subscription: &mut MetricsSubscription,
        name: String,
        interval: i32,
    ) {
        metrics_subscription.configure(name, config::KEY_INTERVAL_MS, interval.to_string());
    }

    #[krabka_bdd::given("the subscription {name} matches clients by {selector}")]
    fn subscription_matches(
        metrics_subscription: &mut MetricsSubscription,
        name: String,
        selector: String,
    ) {
        metrics_subscription.configure(name, config::KEY_MATCH, selector);
    }

    #[krabka_bdd::when("client {client} running {software} asks for its metrics subscription")]
    fn client_asks(
        metrics_subscription: &mut MetricsSubscription,
        client: String,
        software: String,
    ) {
        metrics_subscription.compute_for(&ClientAttributes {
            client_id: client,
            software_name: software,
            ..attrs()
        });
    }

    /// A client attribute that a `match` selector names, by Kafka's key.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
    enum ClientAttribute {
        #[step_param("client_software_version")]
        SoftwareVersion,
        #[step_param("client_source_address")]
        SourceAddress,
        #[step_param("client_source_port")]
        SourcePort,
    }

    #[krabka_bdd::when("a client with {attribute} {value} asks for its metrics subscription")]
    fn client_with_attribute_asks(
        metrics_subscription: &mut MetricsSubscription,
        attribute: ClientAttribute,
        value: String,
    ) {
        let mut client = attrs();
        match attribute {
            ClientAttribute::SoftwareVersion => client.software_version = value,
            ClientAttribute::SourceAddress => client.source_address = value,
            ClientAttribute::SourcePort => {
                client.source_port = value.parse().expect("a source port is a number");
            }
        }
        metrics_subscription.compute_for(&client);
    }

    #[krabka_bdd::then("the client receives metrics {metrics} every {interval} milliseconds")]
    fn client_receives(
        metrics_subscription: &MetricsSubscription,
        metrics: MetricPrefixes,
        interval: i32,
    ) {
        assert!(
            metrics_subscription.received()
                == ComputedSubscription {
                    metrics: metrics.0,
                    push_interval_ms: interval,
                }
        );
    }

    #[krabka_bdd::then("the client receives no metrics every {interval} milliseconds")]
    fn client_receives_nothing(metrics_subscription: &MetricsSubscription, interval: i32) {
        assert!(
            metrics_subscription.received()
                == ComputedSubscription {
                    metrics: Vec::new(),
                    push_interval_ms: interval,
                }
        );
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/client_metrics/manager",
        fixtures = [metrics_subscription: MetricsSubscription],
    );
}
