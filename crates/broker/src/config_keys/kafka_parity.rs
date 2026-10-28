//! Parity between krabka's topic-config surface and Apache Kafka 4.3.1's.
//!
//! `MirrorMaker` 2 reads a source topic's config set with `DescribeConfigs` and
//! replays it onto the target in **one** `IncrementalAlterConfigs` call. Kafka
//! refuses an unknown topic config, and so does krabka, so a single key krabka
//! does not carry fails the whole set: `retention.ms` and everything beside it
//! never propagate, and the operator sees one WARN in a JVM log. That is how
//! `compression.lz4.level` was found -- one CI run at a time -- and this module
//! exists so the next gap fails here instead.
//!
//! # Where the roster comes from
//!
//! [`KAFKA_TOPIC_CONFIGS`] is `LogConfig.configKeys()` of the released Apache
//! Kafka 4.3.1 binary distribution, which is the artifact set the
//! `mirror.gcr.io/apache/kafka:4.3.1` image ships. It was captured by running
//! that jar, not by reading a wiki page and not from memory:
//!
//! ```text
//! curl -O https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
//! # sha512 matches the published .sha512:
//! # c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e
//! # 8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c
//! tar xzf kafka_2.13-4.3.1.tgz
//! # then, against kafka_2.13-4.3.1/libs/*:
//! #   LogConfig.configKeys()  -> name, ConfigDef.Type, default, validator
//! #   LogConfig.validate(map) -> what an alter accepts or refuses
//! ```
//!
//! `LogConfig`'s `ConfigDef`, not `TopicConfig`'s constants, is the authority:
//! `TopicConfig` still declares `message.downconversion.enable`, and
//! `LogConfig` no longer defines it, so `LogConfig.validate` answers `Unknown
//! topic config name: message.downconversion.enable`. A roster built from
//! `TopicConfig` would have carried a key Kafka 4.3.1 rejects.
//!
//! The same run pinned the refusals in [`kafka_refuses_what_krabka_refuses`]
//! and confirmed that Kafka rejects an unrecognised key rather than ignoring
//! it, which is why krabka stays strict here and closes the gap by carrying
//! Kafka's keys rather than by becoming permissive.
//!
//! # The other direction
//!
//! [`KRABKA_TOPIC_CONFIGS`] is every topic key krabka has that Kafka 4.3.1 does
//! not. A key added to the registry without a row here fails
//! [`the_topic_key_sets_differ_only_by_the_rosters_below`], which is the point:
//! a krabka-private key is a deliberate divergence and has to be named as one.
//!
//! # Kafka trunk
//!
//! [`KAFKA_TRUNK_TOPIC_CONFIGS`] is the four topic keys Kafka trunk's
//! `LogConfig` defines beyond 4.3.1. A broker without
//! `unstable.api.versions.enable` serves Kafka 4.3.1's topic resource, so it
//! refuses each of them as 4.3.1 does (`Unknown topic config name`) and does
//! not describe them. A `MirrorMaker` 2 replay from a trunk cluster that sets
//! one of them therefore fails against krabka exactly as it fails against a
//! stock 4.3.1 broker, until the target opts in to trunk behaviour.

use {
    super::{
        delivery::{DELIVERY_MAX_DELAY_MS, DELIVERY_MODE, DELIVERY_SCHEDULE_MONOTONIC},
        diskless::DISKLESS,
        docs::topic_config_docs,
        recovery::UNCLEAN_RECOVERY_STRATEGY,
        registry::{self, ConfigScope, ConfigType},
        schema::{SCHEMA_VALIDATION_KEY, SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_VALUE},
        topic_scope::WRITE_FREEZE,
        validation::{
            TopicDefaults, canonical_topic_config, canonical_topic_config_map,
            validate_topic_config, validate_topic_config_map,
        },
    },
    crate::api_catalog::UnstableApiVersions,
    assert2::{assert, check},
    rstest_bdd::datatable::Rows,
    std::collections::BTreeMap,
};

/// One row of Kafka 4.3.1's `LogConfig.configKeys()`.
struct KafkaTopicConfig {
    name: &'static str,
    /// `ConfigDef.Type`, as the JVM `AdminClient` parses the value with.
    config_type: ConfigType,
    /// The `ConfigKey.defaultValue`, spelled the way `DescribeConfigs` renders
    /// it: an empty LIST is `""`, and `None` is Kafka's `null`.
    default: Option<&'static str>,
    /// A value Kafka's own validator for the key accepts, used to drive the
    /// alter path. Chosen so the whole set is also a legal combination.
    sample: &'static str,
}

const fn kafka(
    name: &'static str,
    config_type: ConfigType,
    default: Option<&'static str>,
    sample: &'static str,
) -> KafkaTopicConfig {
    KafkaTopicConfig {
        name,
        config_type,
        default,
        sample,
    }
}

/// Every topic config Apache Kafka 4.3.1 defines, in the order
/// `LogConfig.configKeys()` sorts them.
///
/// 34 rows: the 33 `LogConfig.nonInternalConfigNames()` plus
/// `internal.segment.bytes`, which `ConfigDef` marks internal. Internal only
/// hides a key from `kafka-configs --help`; `LogConfig.validate` accepts it,
/// so an alter carrying it succeeds against a real broker and must here too.
const KAFKA_TOPIC_CONFIGS: &[KafkaTopicConfig] = &[
    kafka("cleanup.policy", ConfigType::List, Some("delete"), "delete"),
    kafka("compression.gzip.level", ConfigType::Int, Some("-1"), "6"),
    kafka("compression.lz4.level", ConfigType::Int, Some("9"), "9"),
    kafka(
        "compression.type",
        ConfigType::String,
        Some("producer"),
        "producer",
    ),
    kafka("compression.zstd.level", ConfigType::Int, Some("3"), "3"),
    kafka(
        "delete.retention.ms",
        ConfigType::Long,
        Some("86400000"),
        "86400000",
    ),
    kafka(
        "file.delete.delay.ms",
        ConfigType::Long,
        Some("60000"),
        "60000",
    ),
    kafka(
        "flush.messages",
        ConfigType::Long,
        Some("9223372036854775807"),
        "10000",
    ),
    kafka(
        "flush.ms",
        ConfigType::Long,
        Some("9223372036854775807"),
        "1000",
    ),
    kafka(
        "follower.replication.throttled.replicas",
        ConfigType::List,
        Some(""),
        "0:1",
    ),
    kafka(
        "index.interval.bytes",
        ConfigType::Int,
        Some("4096"),
        "4096",
    ),
    kafka("internal.segment.bytes", ConfigType::Int, None, "1048576"),
    kafka(
        "leader.replication.throttled.replicas",
        ConfigType::List,
        Some(""),
        "0:1",
    ),
    kafka("local.retention.bytes", ConfigType::Long, Some("-2"), "-2"),
    kafka("local.retention.ms", ConfigType::Long, Some("-2"), "-2"),
    kafka(
        "max.compaction.lag.ms",
        ConfigType::Long,
        Some("9223372036854775807"),
        "86400000",
    ),
    kafka(
        "max.message.bytes",
        ConfigType::Int,
        Some("1048588"),
        "1048588",
    ),
    kafka(
        "message.timestamp.after.max.ms",
        ConfigType::Long,
        Some("3600000"),
        "3600000",
    ),
    kafka(
        "message.timestamp.before.max.ms",
        ConfigType::Long,
        Some("9223372036854775807"),
        "3600000",
    ),
    kafka(
        "message.timestamp.type",
        ConfigType::String,
        Some("CreateTime"),
        "CreateTime",
    ),
    kafka(
        "min.cleanable.dirty.ratio",
        ConfigType::Double,
        Some("0.5"),
        "0.5",
    ),
    kafka("min.compaction.lag.ms", ConfigType::Long, Some("0"), "0"),
    kafka("min.insync.replicas", ConfigType::Int, Some("1"), "1"),
    kafka("preallocate", ConfigType::Boolean, Some("false"), "false"),
    kafka(
        "remote.log.copy.disable",
        ConfigType::Boolean,
        Some("false"),
        "false",
    ),
    kafka(
        "remote.log.delete.on.disable",
        ConfigType::Boolean,
        Some("false"),
        "false",
    ),
    kafka(
        "remote.storage.enable",
        ConfigType::Boolean,
        Some("false"),
        "false",
    ),
    kafka("retention.bytes", ConfigType::Long, Some("-1"), "-1"),
    kafka(
        "retention.ms",
        ConfigType::Long,
        Some("604800000"),
        "604800000",
    ),
    kafka(
        "segment.bytes",
        ConfigType::Int,
        Some("1073741824"),
        "1073741824",
    ),
    kafka(
        "segment.index.bytes",
        ConfigType::Int,
        Some("10485760"),
        "10485760",
    ),
    kafka("segment.jitter.ms", ConfigType::Long, Some("0"), "0"),
    kafka(
        "segment.ms",
        ConfigType::Long,
        Some("604800000"),
        "604800000",
    ),
    kafka(
        "unclean.leader.election.enable",
        ConfigType::Boolean,
        Some("false"),
        "false",
    ),
];

/// The topic configs Apache Kafka trunk defines beyond 4.3.1, from
/// `storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java`
/// at commit `4338e13ea3864a51d9ead14db23a3e0b76ca9fb7`. They arrive with the
/// next Kafka release. krabka serves them only under
/// `unstable.api.versions.enable`; without it, as on Kafka 4.3.1, each is an
/// unknown topic config.
const KAFKA_TRUNK_TOPIC_CONFIGS: &[KafkaTopicConfig] = &[
    kafka(
        "errors.deadletterqueue.group.enable",
        ConfigType::Boolean,
        Some("false"),
        "false",
    ),
    kafka(
        "max.decompressed.message.bytes",
        ConfigType::Int,
        Some("2147483639"),
        "2147483639",
    ),
    kafka("remote.copy.lag.bytes", ConfigType::Long, Some("-1"), "-1"),
    kafka("remote.copy.lag.ms", ConfigType::Long, Some("0"), "0"),
];

/// Every Kafka topic config, released and trunk.
fn kafka_rosters() -> impl Iterator<Item = &'static KafkaTopicConfig> {
    KAFKA_TOPIC_CONFIGS
        .iter()
        .chain(KAFKA_TRUNK_TOPIC_CONFIGS.iter())
}

/// The topic keys krabka has and Apache Kafka 4.3.1 does not.
///
/// Every one is a deliberate krabka extension. `unclean.recovery.strategy` is
/// KIP-966, which Kafka has accepted but not shipped as a topic config in
/// 4.3.1; the rest are krabka's own.
const KRABKA_TOPIC_CONFIGS: &[&str] = &[
    UNCLEAN_RECOVERY_STRATEGY,
    DISKLESS,
    DELIVERY_MODE,
    DELIVERY_MAX_DELAY_MS,
    DELIVERY_SCHEDULE_MONOTONIC,
    SCHEMA_VALIDATION_KEY,
    SCHEMA_VALIDATION_VALUE,
    SCHEMA_VALIDATION_MODE,
    WRITE_FREEZE,
];

/// Kafka keys whose default krabka reports differently, each with the reason.
///
/// This is not a place to park a fresh divergence. A row here is a behaviour
/// krabka has not implemented, stated so that
/// [`kafka_topic_key_types_and_defaults_match`] still pins every other key.
/// The table is empty, and
/// [`every_recorded_divergence_is_still_a_divergence`] is what keeps a row
/// from outliving the gap it names.
const DEFAULT_DIVERGENCES: &[Divergence] = &[];

/// A Kafka key and the default krabka reports for it.
struct Divergence {
    name: &'static str,
    krabka_default: &'static str,
}

fn krabka_topic_keys() -> Vec<&'static str> {
    registry::keys_in(ConfigScope::Topic)
        .map(|row| row.name)
        .collect()
}

struct TopicMetadataObservation {
    oracle: &'static KafkaTopicConfig,
    observed: Option<&'static registry::ConfigKey>,
}

struct TopicDefaultDivergence {
    name: &'static str,
    kafka_default: Option<&'static str>,
    krabka_default: &'static str,
}

struct TopicValidationOutcome {
    key: &'static str,
    result: Result<(), String>,
}

struct TopicCanonicalOutcome {
    key: &'static str,
    result: Result<String, String>,
}

#[derive(Default)]
struct TopicParityScenario {
    samples: Vec<&'static KafkaTopicConfig>,
    properties: BTreeMap<String, String>,
    unstable: Option<UnstableApiVersions>,
    per_key: Option<Vec<TopicValidationOutcome>>,
    canonical_keys: Option<Vec<TopicCanonicalOutcome>>,
    released_map: Option<Result<(), String>>,
    opt_in_map: Option<Result<(), String>>,
    registry_keys: Option<Vec<&'static str>>,
    documented: Option<Vec<&'static str>>,
    metadata: Option<Vec<TopicMetadataObservation>>,
    divergences: Option<Vec<TopicDefaultDivergence>>,
    key: String,
    value: String,
    single_validation: Option<Result<(), String>>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct TopicExtensionKey {
    key: String,
}

#[rstest::fixture]
fn topic_parity() -> TopicParityScenario {
    // Each catalog corpus and alteration result belongs to one scenario.
    TopicParityScenario::default()
}

#[rstest_bdd_macros::given("the topic properties captured from released Apache Kafka 4.3.1")]
fn released_topic_properties(topic_parity: &mut TopicParityScenario) {
    topic_parity.samples = KAFKA_TOPIC_CONFIGS.iter().collect();
    topic_parity.properties = topic_parity
        .samples
        .iter()
        .map(|row| (row.name.to_owned(), row.sample.to_owned()))
        .collect();
}

#[rstest_bdd_macros::given(
    "the Kafka trunk topic properties together with the released property corpus"
)]
fn trunk_topic_properties(topic_parity: &mut TopicParityScenario) {
    topic_parity.samples = KAFKA_TRUNK_TOPIC_CONFIGS.iter().collect();
    topic_parity.properties = kafka_rosters()
        .map(|row| (row.name.to_owned(), row.sample.to_owned()))
        .collect();
}

#[rstest_bdd_macros::given("topic alteration unstable API versions are {enabled}")]
fn topic_parity_unstable_mode(topic_parity: &mut TopicParityScenario, enabled: bool) {
    topic_parity.unstable = Some(if enabled {
        UnstableApiVersions::Enabled
    } else {
        UnstableApiVersions::Disabled
    });
}

#[rstest_bdd_macros::given("the captured released and trunk topic-key reference catalogs")]
fn all_topic_reference_metadata(topic_parity: &mut TopicParityScenario) {
    topic_parity.samples = kafka_rosters().collect();
}

#[rstest_bdd_macros::given("an operator alters topic key {key} to quoted value {value}")]
fn individual_topic_property(topic_parity: &mut TopicParityScenario, key: String, value: String) {
    topic_parity.key = key;
    topic_parity.value = serde_json::from_str(&value).expect("the topic value is quoted");
}

#[rstest_bdd_macros::when(
    "the broker validates each released property and the complete released alteration"
)]
fn validate_released_topic_corpus(topic_parity: &mut TopicParityScenario) {
    topic_parity.per_key = Some(
        topic_parity
            .samples
            .iter()
            .map(|row| TopicValidationOutcome {
                key: row.name,
                result: validate_topic_config(row.name, row.sample),
            })
            .collect(),
    );
    topic_parity.released_map = Some(validate_topic_config_map(&topic_parity.properties));
}

#[rstest_bdd_macros::when(
    "the broker validates trunk properties and their complete MirrorMaker alteration"
)]
fn validate_trunk_topic_corpus(topic_parity: &mut TopicParityScenario) {
    let unstable = topic_parity
        .unstable
        .expect("the unstable API policy was given");
    topic_parity.canonical_keys = Some(
        topic_parity
            .samples
            .iter()
            .map(|row| TopicCanonicalOutcome {
                key: row.name,
                result: canonical_topic_config(row.name, row.sample, unstable),
            })
            .collect(),
    );
    topic_parity.opt_in_map = Some(
        canonical_topic_config_map(
            &topic_parity.properties,
            &TopicDefaults::default(),
            true,
            unstable,
        )
        .map(drop),
    );
    topic_parity.released_map = Some(validate_topic_config_map(&topic_parity.properties));
}

#[rstest_bdd_macros::when(
    "the topic registry and generated configuration descriptions are queried"
)]
fn query_topic_reference_metadata(topic_parity: &mut TopicParityScenario) {
    topic_parity.registry_keys = Some(krabka_topic_keys());
    topic_parity.documented = Some(topic_config_docs().iter().map(|doc| doc.key).collect());
    topic_parity.metadata = Some(
        topic_parity
            .samples
            .iter()
            .map(|row| TopicMetadataObservation {
                oracle: row,
                observed: registry::lookup(ConfigScope::Topic, row.name),
            })
            .collect(),
    );
    topic_parity.divergences = Some(
        DEFAULT_DIVERGENCES
            .iter()
            .map(|divergence| {
                let kafka_default = kafka_rosters()
                    .find(|row| row.name == divergence.name)
                    .unwrap_or_else(|| panic!("{} is not a Kafka topic config", divergence.name))
                    .default;
                TopicDefaultDivergence {
                    name: divergence.name,
                    kafka_default,
                    krabka_default: divergence.krabka_default,
                }
            })
            .collect(),
    );
}

#[rstest_bdd_macros::when("the released topic alteration validator checks the operator value")]
fn validate_individual_topic_property(topic_parity: &mut TopicParityScenario) {
    topic_parity.single_validation = Some(validate_topic_config(
        &topic_parity.key,
        &topic_parity.value,
    ));
}

#[rstest_bdd_macros::then(
    "every captured released property and the complete alteration are accepted"
)]
fn accepted_released_topic_corpus(topic_parity: &TopicParityScenario) {
    let results = topic_parity
        .per_key
        .as_ref()
        .expect("released properties were validated");
    assert!(!results.is_empty());
    for observation in results {
        check!(observation.result == Ok(()), "{}", observation.key);
    }
    assert!(topic_parity.released_map == Some(Ok(())));
}

#[rstest_bdd_macros::then(
    "every trunk key is refused with Kafka's exact unknown-topic-config message"
)]
fn refused_trunk_topic_keys(topic_parity: &TopicParityScenario) {
    let results = topic_parity
        .canonical_keys
        .as_ref()
        .expect("trunk properties were validated");
    assert!(!results.is_empty());
    for observation in results {
        check!(
            observation.result == Err(format!("Unknown topic config name: {}", observation.key)),
            "{}",
            observation.key
        );
    }
    assert!(
        topic_parity
            .opt_in_map
            .as_ref()
            .expect("the whole map was validated")
            .is_err()
    );
}

#[rstest_bdd_macros::then(
    "every trunk property and the complete opted-in alteration preserve their values"
)]
fn accepted_trunk_topic_keys(topic_parity: &TopicParityScenario) {
    let results = topic_parity
        .canonical_keys
        .as_ref()
        .expect("trunk properties were validated");
    assert!(results.len() == topic_parity.samples.len());
    for (observation, sample) in results.iter().zip(&topic_parity.samples) {
        check!(
            observation.result == Ok(sample.sample.to_owned()),
            "{}",
            observation.key
        );
    }
    assert!(topic_parity.opt_in_map == Some(Ok(())));
}

#[rstest_bdd_macros::then(
    "the release-only validator refuses an alteration containing trunk properties"
)]
fn release_only_refuses_trunk_properties(topic_parity: &TopicParityScenario) {
    assert!(
        topic_parity
            .released_map
            .as_ref()
            .expect("the released validator ran")
            .is_err()
    );
}

#[rstest_bdd_macros::then("no captured Kafka topic key is missing and the only extra keys are:")]
fn complete_topic_key_surface(
    topic_parity: &TopicParityScenario,
    #[datatable] rows: Rows<TopicExtensionKey>,
) {
    let krabka = topic_parity
        .registry_keys
        .as_ref()
        .expect("the registry was queried");
    let kafka: Vec<_> = topic_parity.samples.iter().map(|row| row.name).collect();
    let missing: Vec<_> = kafka
        .iter()
        .copied()
        .filter(|name| !krabka.contains(name))
        .collect();
    let extra: Vec<_> = krabka
        .iter()
        .copied()
        .filter(|name| !kafka.contains(name))
        .collect();
    let expected: Vec<_> = rows.into_iter().map(|row| row.key).collect();
    check!(missing == Vec::<&str>::new(), "Kafka keys krabka rejects");
    check!(extra == KRABKA_TOPIC_CONFIGS.to_vec());
    let extra: Vec<_> = extra.into_iter().map(str::to_owned).collect();
    check!(extra == expected);
}

#[rstest_bdd_macros::then("every captured Kafka topic key is documented")]
fn documented_topic_key_surface(topic_parity: &TopicParityScenario) {
    let documented = topic_parity
        .documented
        .as_ref()
        .expect("the configuration descriptions were queried");
    for row in &topic_parity.samples {
        check!(documented.contains(&row.name), "{}", row.name);
    }
}

#[rstest_bdd_macros::then(
    "every captured topic key retains Kafka's type and its default or a documented divergence"
)]
fn kafka_topic_metadata(topic_parity: &TopicParityScenario) {
    for observation in topic_parity
        .metadata
        .as_ref()
        .expect("the metadata was queried")
    {
        let row = observation.oracle;
        let actual = observation
            .observed
            .expect("every captured key is registered");
        let expected_default = DEFAULT_DIVERGENCES
            .iter()
            .find(|d| d.name == row.name)
            .map_or(row.default, |divergence| Some(divergence.krabka_default));
        check!(
            (actual.config_type, actual.default) == (row.config_type, expected_default),
            "{}",
            row.name
        );
    }
}

#[rstest_bdd_macros::then("each documented default divergence is still an actual divergence")]
fn real_topic_default_divergences(topic_parity: &TopicParityScenario) {
    for divergence in topic_parity
        .divergences
        .as_ref()
        .expect("divergences were inspected")
    {
        check!(
            divergence.kafka_default != Some(divergence.krabka_default),
            "{}",
            divergence.name
        );
    }
}

#[rstest_bdd_macros::then("the individual topic alteration is accepted")]
fn accepted_topic_property(topic_parity: &TopicParityScenario) {
    check!(
        topic_parity.single_validation == Some(Ok(())),
        "{}={}",
        topic_parity.key,
        topic_parity.value
    );
}

#[rstest_bdd_macros::then("the individual topic alteration is refused")]
fn refused_topic_property(topic_parity: &TopicParityScenario) {
    check!(
        topic_parity
            .single_validation
            .as_ref()
            .expect("the topic alteration was validated")
            .is_err(),
        "{}={} must be refused",
        topic_parity.key,
        topic_parity.value
    );
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/config_keys/kafka_parity",
    fixtures = [topic_parity: TopicParityScenario]
);
