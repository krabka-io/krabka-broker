//! Subscription matching for KIP-714 client metrics: which subscriptions a
//! connecting client matches, the union of the metric prefixes and the
//! smallest push interval across them, and the stable subscription id derived
//! from that result.

use {
    super::{ClientAttributes, ComputedSubscription},
    crate::client_metrics::config::{self, ALL_METRICS, MatchRule},
    krabka_metadata::MetadataImage,
    krabka_protocol::records::crc32c,
    std::{
        borrow::Cow,
        collections::{BTreeMap, HashMap},
        mem,
    },
    uuid::Uuid,
};

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
    pub(crate) fn compute(
        &self,
        attrs: &ClientAttributes,
        default_interval_ms: i32,
    ) -> ComputedSubscription {
        let mut matched_metrics: Vec<String> = Vec::new();
        let mut push_interval_ms = default_interval_ms;
        let mut any_star = false;

        for compiled in &self.entries {
            let Some(rules) = &compiled.rules else {
                continue;
            };
            if !rules.iter().all(|r| selector_matches(r, attrs)) {
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
        ComputedSubscription {
            metrics,
            push_interval_ms,
        }
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
    Subscriptions::from_image(image).compute(attrs, default_interval_ms)
}

fn selector_matches(rule: &config::MatchRule, attrs: &ClientAttributes) -> bool {
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
    // `JavaPattern::matches` is Kafka's full match. A search that runs past the
    // backtrack limit counts as no match.
    rule.pattern.matches(target.as_ref()).unwrap_or(false)
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
        krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord},
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

    #[test]
    fn no_subscription_means_no_metrics() {
        let img = MetadataImage::new(Uuid::nil());
        let m = compute_subscription(&img, &attrs());
        assert!(m.metrics.is_empty());
        check!(m.push_interval_ms == 300_000);
    }

    #[test]
    fn match_all_empty_match_applies() {
        let img = img_with("all", &[
            ConfigEntry {
                key: "metrics",
                value: "*",
            },
            ConfigEntry {
                key: "interval.ms",
                value: "60000",
            },
        ]);
        let m = compute_subscription(&img, &attrs());
        check!(m.metrics == vec!["*".to_owned()]);
        check!(m.push_interval_ms == 60_000);
    }

    #[test]
    fn selector_filters_clients() {
        let img = img_with("java-only", &[
            ConfigEntry {
                key: "metrics",
                value: "a.",
            },
            ConfigEntry {
                key: "match",
                value: "client_software_name=apache-kafka-java",
            },
        ]);
        let m = compute_subscription(&img, &attrs());
        check!(m.metrics == vec!["a.".to_owned()]);

        let img2 = img_with("py-only", &[
            ConfigEntry {
                key: "metrics",
                value: "a.",
            },
            ConfigEntry {
                key: "match",
                value: "client_software_name=kafka-python",
            },
        ]);
        let m2 = compute_subscription(&img2, &attrs());
        assert!(
            m2.metrics.is_empty(),
            "java client must not match python selector"
        );
    }

    #[test]
    fn min_interval_and_metric_union_across_subs() {
        let mut img = img_with("s1", &[
            ConfigEntry {
                key: "metrics",
                value: "a.",
            },
            ConfigEntry {
                key: "interval.ms",
                value: "60000",
            },
        ]);
        apply_config(&mut img, "s2", &[
            ConfigEntry {
                key: "metrics",
                value: "b.",
            },
            ConfigEntry {
                key: "interval.ms",
                value: "30000",
            },
        ]);
        let m = compute_subscription(&img, &attrs());
        let mut got = m.metrics.clone();
        got.sort();
        check!(got == vec!["a.".to_owned(), "b.".to_owned()]);
        check!(m.push_interval_ms == 30_000);
    }

    #[test]
    fn an_empty_metrics_list_collects_nothing() {
        // KIP-714: `*` is the value that means every metric. An empty list
        // names no prefix, so it contributes nothing to the client's set --
        // `apache/kafka:4.3.1` builds the same set by adding each matching
        // subscription's metrics, and an empty one adds none. The registry
        // row documents the key that way.
        let img = img_with("empty", &[
            ConfigEntry {
                key: "metrics",
                value: "",
            },
            ConfigEntry {
                key: "interval.ms",
                value: "60000",
            },
        ]);
        let m = compute_subscription(&img, &attrs());
        assert!(m.metrics.is_empty());

        let unset = img_with("unset", &[ConfigEntry {
            key: "interval.ms",
            value: "60000",
        }]);
        let m2 = compute_subscription(&unset, &attrs());
        assert!(m2.metrics.is_empty());
    }

    #[test]
    fn star_collapses_union() {
        let mut img = img_with("s1", &[ConfigEntry {
            key: "metrics",
            value: "a.",
        }]);
        apply_config(&mut img, "s2", &[ConfigEntry {
            key: "metrics",
            value: "*",
        }]);
        let m = compute_subscription(&img, &attrs());
        check!(m.metrics == vec!["*".to_owned()]);
    }

    /// Kafka's `createClientInstance` caps the interval at 300000, lets every
    /// matched subscription lower it, and full-matches the last pattern of
    /// each selector (#712).
    #[test]
    fn subscription_matches_kafkas_client_metrics_manager() {
        struct Subscription {
            name: &'static str,
            configs: Vec<ConfigEntry<'static>>,
        }
        struct Row {
            name: &'static str,
            subscriptions: Vec<Subscription>,
            client: ClientAttributes,
            expected: ComputedSubscription,
        }
        let with = |client_id: &str, software: &str| ClientAttributes {
            client_id: client_id.into(),
            software_name: software.into(),
            ..attrs()
        };
        let subscription =
            |name: &'static str, configs: &[(&'static str, &'static str)]| Subscription {
                name,
                configs: configs
                    .iter()
                    .map(|&(key, value)| ConfigEntry { key, value })
                    .collect(),
            };
        let rows: Vec<Row> = vec![
            Row {
                name: "an interval above the default is capped",
                subscriptions: vec![subscription("s1", &[
                    ("metrics", "a."),
                    ("interval.ms", "600000"),
                ])],
                client: attrs(),
                expected: ComputedSubscription {
                    metrics: vec!["a.".into()],
                    push_interval_ms: 300_000,
                },
            },
            Row {
                name: "a matched subscription with no metrics lowers the interval",
                subscriptions: vec![
                    subscription("s1", &[("metrics", "a."), ("interval.ms", "60000")]),
                    subscription("s2", &[("metrics", ""), ("interval.ms", "1000")]),
                ],
                client: attrs(),
                expected: ComputedSubscription {
                    metrics: vec!["a.".into()],
                    push_interval_ms: 1_000,
                },
            },
            Row {
                name: "no subscription",
                subscriptions: vec![],
                client: attrs(),
                expected: ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: 300_000,
                },
            },
            Row {
                name: "an alternation full-matches its longer branch",
                subscriptions: vec![subscription("s1", &[
                    ("metrics", "*"),
                    ("match", "client_id=app|app-1"),
                ])],
                client: with("app-1", "apache-kafka-java"),
                expected: ComputedSubscription {
                    metrics: vec!["*".into()],
                    push_interval_ms: 300_000,
                },
            },
            Row {
                name: "a partial match does not match",
                subscriptions: vec![subscription("s1", &[
                    ("metrics", "*"),
                    ("match", "client_id=app"),
                ])],
                client: with("app-1", "apache-kafka-java"),
                expected: ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: 300_000,
                },
            },
            Row {
                name: "a missing KIP-511 name is unknown",
                subscriptions: vec![subscription("s1", &[
                    ("metrics", "*"),
                    ("match", "client_software_name=unknown"),
                ])],
                client: with("svc-1", "unknown"),
                expected: ComputedSubscription {
                    metrics: vec!["*".into()],
                    push_interval_ms: 300_000,
                },
            },
            Row {
                name: "a repeated selector keeps its last pattern",
                subscriptions: vec![subscription("s1", &[
                    ("metrics", "*"),
                    ("match", "client_id=a,client_id=b"),
                ])],
                client: with("b", "apache-kafka-java"),
                expected: ComputedSubscription {
                    metrics: vec!["*".into()],
                    push_interval_ms: 300_000,
                },
            },
        ];
        for Row {
            name,
            subscriptions,
            client,
            expected,
        } in rows
        {
            let mut img = MetadataImage::new(Uuid::nil());
            for Subscription {
                name: sub_name,
                configs,
            } in subscriptions
            {
                img.apply(&MetadataRecord::V1ClientMetricsConfig(
                    ClientMetricsConfigRecord {
                        name: sub_name.into(),
                        configs: configs
                            .into_iter()
                            .map(|ConfigEntry { key, value }| (key.to_owned(), value.to_owned()))
                            .collect(),
                    },
                ));
            }
            check!(
                compute_subscription(&img, &client) == expected,
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
        let computed = subscriptions.compute(&attrs(), config::INTERVAL_MS_DEFAULT);
        check!((first, unchanged, subscriptions.compilations()) == (true, false, 2));
        let mut metrics = computed.metrics;
        metrics.sort();
        check!(metrics == ["a.", "b."]);

        apply_config(&mut img, "s2", &[ConfigEntry {
            key: "metrics",
            value: "c.",
        }]);
        let changed = subscriptions.observe(&img);
        let computed = subscriptions.compute(&attrs(), config::INTERVAL_MS_DEFAULT);
        check!((changed, subscriptions.compilations()) == (true, 3));
        let mut metrics = computed.metrics;
        metrics.sort();
        check!(metrics == ["a.", "c."]);
    }

    /// A subscription whose `match` does not compile matches no client and
    /// does not lower the interval, before and after it is cached.
    #[test]
    fn an_unparsable_match_matches_no_client() {
        let img = img_with("bad", &[
            ConfigEntry {
                key: "metrics",
                value: "*",
            },
            ConfigEntry {
                key: "interval.ms",
                value: "1000",
            },
            ConfigEntry {
                key: "match",
                value: "client_id=(",
            },
        ]);
        let subscriptions = Subscriptions::from_image(&img);
        check!(
            subscriptions.compute(&attrs(), config::INTERVAL_MS_DEFAULT)
                == ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: config::INTERVAL_MS_DEFAULT,
                }
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
}
