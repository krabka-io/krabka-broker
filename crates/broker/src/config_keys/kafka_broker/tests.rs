use {
    super::*,
    crate::config_keys::registry::{ConfigKey, ConfigType},
    assert2::check,
    rstest_bdd::{Slot, datatable::Rows},
};

#[derive(Default)]
struct BrokerConfigCatalogScenario {
    catalog: Option<&'static [KafkaBrokerConfig]>,
    names: Option<Vec<&'static str>>,
    shape: Option<(usize, usize, usize)>,
    per_broker: Option<Vec<&'static str>>,
    key: String,
    value: String,
    dynamic: Slot<Option<Dynamic>>,
    canonical: Option<Result<String, String>>,
    resolved: Slot<Option<(&'static str, ConfigType)>>,
    descriptor: Option<ConfigKey>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct KafkaBrokerKeyName {
    key: String,
}

#[rstest::fixture]
fn broker_catalog() -> BrokerConfigCatalogScenario {
    // Each lookup and configuration value has an independent observed result.
    BrokerConfigCatalogScenario::default()
}

#[rstest_bdd_macros::given("the released Kafka 4.3.1 broker configuration catalog")]
fn kafka_broker_config_catalog(broker_catalog: &mut BrokerConfigCatalogScenario) {
    broker_catalog.catalog = Some(KAFKA_BROKER_CONFIGS);
}

#[rstest_bdd_macros::given("the broker configuration key is {key}")]
fn kafka_broker_config_key(broker_catalog: &mut BrokerConfigCatalogScenario, key: String) {
    broker_catalog.key = key;
}

#[rstest_bdd_macros::given("the quoted broker configuration value is {value}")]
fn kafka_broker_config_value(broker_catalog: &mut BrokerConfigCatalogScenario, value: String) {
    broker_catalog.value = serde_json::from_str(&value).expect("the config value is quoted");
}

#[rstest_bdd_macros::when("the released broker configuration roster is inspected")]
fn inspect_broker_config_catalog(broker_catalog: &mut BrokerConfigCatalogScenario) {
    let catalog = broker_catalog
        .catalog
        .expect("a broker config catalog was given");
    broker_catalog.names = Some(catalog.iter().map(|row| row.name).collect());
    broker_catalog.shape = Some((
        catalog.len(),
        catalog.iter().filter(|row| row.internal).count(),
        catalog
            .iter()
            .filter(|row| matches!(row.dynamic, Dynamic::Cluster | Dynamic::PerBroker))
            .count(),
    ));
    broker_catalog.per_broker = Some(
        catalog
            .iter()
            .filter(|row| row.dynamic == Dynamic::PerBroker)
            .map(|row| row.name)
            .collect(),
    );
}

#[rstest_bdd_macros::when("the broker catalog looks up that key's dynamic mode")]
fn lookup_broker_dynamic_mode(broker_catalog: &mut BrokerConfigCatalogScenario) {
    broker_catalog
        .dynamic
        .set(lookup(&broker_catalog.key).map(|row| row.dynamic));
}

#[rstest_bdd_macros::when("Kafka ConfigDef canonicalization validates the broker value")]
fn canonicalize_broker_config(broker_catalog: &mut BrokerConfigCatalogScenario) {
    let key = lookup(&broker_catalog.key).expect("a KafkaConfig key");
    broker_catalog.canonical = Some(key.canonical(&broker_catalog.value));
}

#[rstest_bdd_macros::when("the broker catalog resolves that configured key")]
fn resolve_broker_config_key(broker_catalog: &mut BrokerConfigCatalogScenario) {
    broker_catalog
        .resolved
        .set(resolve(&broker_catalog.key).map(|row| (row.name, row.config_type)));
}

#[rstest_bdd_macros::when("DescribeConfigs derives that broker key's public descriptor")]
fn describe_broker_config_key(broker_catalog: &mut BrokerConfigCatalogScenario) {
    broker_catalog.descriptor = Some(
        lookup(&broker_catalog.key)
            .expect("a broker key exists")
            .row(),
    );
}

#[rstest_bdd_macros::then("the broker key roster is sorted and contains no duplicate names")]
fn sorted_unique_broker_keys(broker_catalog: &BrokerConfigCatalogScenario) {
    let names = broker_catalog
        .names
        .as_ref()
        .expect("the roster was inspected");
    let mut sorted = names.clone();
    sorted.sort_unstable();
    sorted.dedup();
    check!(*names == sorted);
}

#[rstest_bdd_macros::then("the broker catalog has 360 keys, 20 internal keys, and 99 dynamic keys")]
fn released_broker_catalog_shape(broker_catalog: &BrokerConfigCatalogScenario) {
    check!(broker_catalog.shape == Some((360, 20, 99)));
}

#[rstest_bdd_macros::then("the ordered per-broker-only keys are exactly:")]
fn per_broker_config_keys(
    broker_catalog: &BrokerConfigCatalogScenario,
    #[datatable] rows: Rows<KafkaBrokerKeyName>,
) {
    let mut expected: Vec<_> = rows.into_iter().map(|row| row.key).collect();
    expected.sort_unstable();
    let actual = broker_catalog
        .per_broker
        .as_ref()
        .expect("the roster was inspected");
    let actual: Vec<_> = actual.iter().map(|key| (*key).to_owned()).collect();
    check!(actual == expected);
}

#[rstest_bdd_macros::then("that broker key is startup-only and read-only to dynamic alterations")]
fn read_only_broker_config(broker_catalog: &BrokerConfigCatalogScenario) {
    check!(broker_catalog.dynamic.get() == Some(Some(Dynamic::ReadOnly)));
}

#[rstest_bdd_macros::then("the quoted canonical broker value is {expected}")]
fn canonical_broker_config_value(broker_catalog: &BrokerConfigCatalogScenario, expected: String) {
    let expected: String = serde_json::from_str(&expected).expect("the canonical value is quoted");
    check!(broker_catalog.canonical == Some(Ok(expected)));
}

#[rstest_bdd_macros::then("broker value validation fails with Kafka message {message}")]
fn invalid_broker_config_value(broker_catalog: &BrokerConfigCatalogScenario, message: String) {
    check!(broker_catalog.canonical == Some(Err(message)));
}

#[rstest_bdd_macros::then("the configured name resolves to base key {key} with Kafka type {kind}")]
fn resolved_broker_config_key(
    broker_catalog: &BrokerConfigCatalogScenario,
    key: String,
    kind: String,
) {
    let expected_type = match kind.as_str() {
        "INT" => ConfigType::Int,
        "STRING" => ConfigType::String,
        "PASSWORD" => ConfigType::Password,
        other => panic!("unknown expected Kafka type {other}"),
    };
    check!(broker_catalog.resolved.get() == Some(Some((key.as_str(), expected_type))));
}

#[rstest_bdd_macros::then("the configured name does not resolve to a broker key")]
fn unresolved_broker_config_key(broker_catalog: &BrokerConfigCatalogScenario) {
    check!(broker_catalog.resolved.get() == Some(None));
}

#[rstest_bdd_macros::then(
    "the broker descriptor is sensitive {sensitive}, read-only {read_only}, with wire type {wire_type}"
)]
fn broker_config_descriptor(
    broker_catalog: &BrokerConfigCatalogScenario,
    sensitive: bool,
    read_only: bool,
    wire_type: i8,
) {
    let descriptor = broker_catalog
        .descriptor
        .as_ref()
        .expect("the descriptor was derived");
    check!(
        (
            descriptor.sensitive,
            descriptor.read_only,
            descriptor.config_type.wire()
        ) == (sensitive, read_only, wire_type)
    );
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/config_keys/kafka_broker/tests",
    fixtures = [broker_catalog: BrokerConfigCatalogScenario]
);
