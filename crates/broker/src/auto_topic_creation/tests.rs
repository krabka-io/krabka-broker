use {
    super::*,
    crate::{
        coordinator::{bootstrap, bootstrap::OFFSETS_TOPIC},
        handlers::test_support::StartedBroker,
        test_support::{PartitionCount, ReplicationFactor, start_broker_no_audit},
        time_util::now_ms,
    },
    assert2::{assert, check},
    krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            create_topics_request::{CreatableTopic, CreatableTopicConfig},
            create_topics_response::CreatableTopicResult,
        },
        primitives::uuid::Uuid as ProtoUuid,
    },
    rstest_bdd::datatable::Rows,
    std::{net::IpAddr, path::PathBuf},
};

/// A topic config entry, by name and value.
struct ConfigPair<'a> {
    name: &'a str,
    value: &'a str,
}

fn configs(pairs: &[ConfigPair<'_>]) -> Vec<CreatableTopicConfig> {
    pairs
        .iter()
        .map(|ConfigPair { name, value }| CreatableTopicConfig {
            name: (*name).to_owned(),
            value: Some((*value).to_owned()),
            ..Default::default()
        })
        .collect()
}

struct AutoTopicSetup<'a> {
    name: &'a str,
    num_partitions: PartitionCount,
    replication_factor: ReplicationFactor,
    configs: Vec<CreatableTopicConfig>,
}

impl Default for AutoTopicSetup<'_> {
    fn default() -> Self {
        Self {
            name: "orders",
            num_partitions: PartitionCount(-1),
            replication_factor: ReplicationFactor(-1),
            configs: vec![],
        }
    }
}

#[bon::builder]
fn topic(
    #[builder(default = AutoTopicSetup::default().name)] name: &'_ str,
    #[builder(into, default = AutoTopicSetup::default().num_partitions)]
    num_partitions: PartitionCount,
    #[builder(into, default = AutoTopicSetup::default().replication_factor)]
    replication_factor: ReplicationFactor,
    #[builder(default = AutoTopicSetup::default().configs)] configs: Vec<CreatableTopicConfig>,
) -> CreatableTopic {
    let setup = AutoTopicSetup {
        name,
        num_partitions,
        replication_factor,
        configs,
    };
    let AutoTopicSetup {
        name,
        num_partitions,
        replication_factor,
        configs,
    } = setup;
    CreatableTopic {
        name: name.to_owned(),
        num_partitions: num_partitions.0,
        replication_factor: replication_factor.0,
        configs,
        ..Default::default()
    }
}

/// Before the broker binds itself, a request creates nothing, and its caller
/// retries as it does for any other `COORDINATOR_NOT_AVAILABLE`.
#[tokio::test]
async fn an_unbound_request_creates_nothing() {
    let creation = Arc::new(AutoTopicCreation::default());
    creation.request(OFFSETS_TOPIC);
    check!(creation.started() == 0);
    check!(!creation.is_in_flight(bootstrap::OFFSETS_TOPIC));
}

/// An unbound component cannot create the dead-letter topic: the write
/// fails, and the leader archives the record regardless.
#[tokio::test]
async fn an_unbound_component_cannot_create_the_dead_letter_topic() {
    let creation = AutoTopicCreation::default();

    check!(
        creation
            .create_dead_letter_topic("dlq.orders")
            .await
            .is_err()
    );
}

/// Kafka's `ExpiringErrorCache`: an entry counts until its expiry time, a new
/// `put` replaces the entry of the same topic, and a full cache drops the
/// entry that expires first, not the one written first.
#[test]
fn the_error_cache_expires_and_evicts_by_expiry_time() {
    let cache = ExpiringErrorCache::new(2);
    cache.put("a", "first".into(), 1_000, 0);
    cache.put("b", "second".into(), 500, 0);

    check!(cache.has_error("a", 999));
    check!(!cache.has_error("a", 1_000));
    check!(!cache.has_error("unknown", 0));
    check!(
        cache.errors_for_topics(["a", "b", "unknown"], 499)
            == BTreeMap::from([
                ("a".to_owned(), "first".to_owned()),
                ("b".to_owned(), "second".to_owned()),
            ])
    );
    check!(
        cache.errors_for_topics(["a", "b"], 500)
            == BTreeMap::from([("a".to_owned(), "first".to_owned())])
    );

    // A third entry goes over the capacity. `b` expires first, so it goes,
    // though `a` was written before it.
    cache.put("c", "third".into(), 2_000, 100);
    check!(cache.len() == 2);
    check!(
        cache.errors_for_topics(["a", "b", "c"], 100)
            == BTreeMap::from([
                ("a".to_owned(), "first".to_owned()),
                ("c".to_owned(), "third".to_owned()),
            ])
    );

    // A new entry for `a` replaces the old one. The stale heap entry of `a`
    // expires at 1000 and must not remove the new entry.
    cache.put("a", "again".into(), 5_000, 200);
    cache.put("d", "fourth".into(), 10, 1_500);
    check!(
        cache.errors_for_topics(["a", "c", "d"], 1_500)
            == BTreeMap::from([
                ("a".to_owned(), "again".to_owned()),
                ("c".to_owned(), "third".to_owned()),
            ])
    );
}

/// A put drops every entry that has expired, and the cache keeps at most its
/// capacity: `ERROR_CACHE_CAPACITY` by default, as in Kafka.
#[test]
fn the_error_cache_is_bounded() {
    let cache = ExpiringErrorCache::default();
    for index in 0..=ERROR_CACHE_CAPACITY {
        cache.put(&format!("t{index}"), "no brokers".into(), 1_000, 100);
    }
    check!(cache.len() == ERROR_CACHE_CAPACITY);

    cache.put("late", "no brokers".into(), 1_000, 2_000);
    check!(cache.len() == 1);
}

fn result_row(name: &str, error_code: i16, error_message: Option<&str>) -> CreatableTopicResult {
    CreatableTopicResult {
        name: name.to_owned(),
        topic_id: ProtoUuid([0; 16]),
        error_code,
        error_message: error_message.map(str::to_owned),
        num_partitions: -1,
        replication_factor: -1,
        configs: None,
        topic_config_error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// Kafka's `createStreamsInternalTopics` skips a topic whose last failure has
/// not expired and a topic whose creation is in flight. It sends a request
/// only when some topic is left.
#[tokio::test]
async fn a_streams_creation_skips_a_backed_off_or_in_flight_topic() {
    let StartedBroker {
        broker: handle,
        dir: _dir,
    } = start_broker_no_audit().await;
    let broker = handle.broker_arc_for_test();
    let creation = &broker.auto_topic_creation;
    let identity = ForwardedIdentity {
        principal_name: "alice".to_owned(),
        client_address: IpAddr::from([127, 0, 0, 1]),
        client_id: "streams-client".to_owned(),
        correlation_id: 1,
    };
    creation
        .errors
        .put("backed-off", "no brokers".into(), 60_000, now_ms());
    assert!(creation.hold_for_test("in-flight"));
    let internal_topic = |name| topic().name(name).num_partitions(1).call();

    creation.create_streams_internal_topics(
        &broker,
        vec![internal_topic("backed-off"), internal_topic("in-flight")],
        identity.clone(),
        60_000,
    );
    check!(creation.started() == 0);

    creation.create_streams_internal_topics(
        &broker,
        vec![internal_topic("backed-off"), internal_topic("fresh")],
        identity,
        60_000,
    );
    check!(creation.started() == 1);
    handle.wait_until_partition_present("fresh", 0).await;
    check!(
        broker
            .controller
            .current_image()
            .topic("backed-off")
            .is_none()
    );
    handle.shutdown().await;
}

#[derive(Default)]
struct AutoTopicDefinition {
    broker: Option<BrokerConfig>,
    name: String,
    created: Option<CreatableTopic>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct ExpectedTopicDefinition {
    field: String,
    value: String,
}

#[rstest::fixture]
fn auto_topic_definition() -> AutoTopicDefinition {
    // Each topic request reads independently supplied broker defaults.
    AutoTopicDefinition::default()
}

#[rstest_bdd_macros::given(
    "ordinary topic defaults are {partitions} partitions and replication {replication}"
)]
fn ordinary_auto_topic_defaults(
    auto_topic_definition: &mut AutoTopicDefinition,
    partitions: i32,
    replication: i16,
) {
    let mut config = BrokerConfig::for_tests(PathBuf::from("/tmp"));
    config.num_partitions = partitions;
    config.default_replication_factor = replication;
    auto_topic_definition.broker = Some(config);
}

#[rstest_bdd_macros::given(
    "the {kind} coordinator topic uses {partitions} partitions and replication {replication}"
)]
fn coordinator_auto_topic_defaults(
    auto_topic_definition: &mut AutoTopicDefinition,
    kind: String,
    partitions: i32,
    replication: i16,
) {
    let config = auto_topic_definition
        .broker
        .as_mut()
        .expect("broker defaults were given");
    match kind.as_str() {
        "offsets" => {
            config.offsets_topic_num_partitions = partitions;
            config.offsets_topic_replication_factor = replication;
        }
        "transaction" => {
            config.transaction_state_num_partitions = partitions;
            config.transaction_state_replication_factor = replication;
        }
        "share" => {
            config.share_coordinator.state_topic_num_partitions = partitions;
            config.share_coordinator.state_topic_replication_factor = replication;
        }
        "barrier" => {
            config.barrier_state_num_partitions = partitions;
            config.barrier_state_replication_factor = replication;
        }
        _ => panic!("unknown coordinator topic domain {kind}"),
    }
}

#[rstest_bdd_macros::given("the {kind} coordinator topic minimum ISR is {minimum}")]
fn coordinator_auto_topic_minimum_isr(
    auto_topic_definition: &mut AutoTopicDefinition,
    kind: String,
    minimum: i32,
) {
    let config = auto_topic_definition
        .broker
        .as_mut()
        .expect("broker defaults were given");
    match kind.as_str() {
        "transaction" => config.transaction_state_min_isr = minimum,
        "share" => config.share_coordinator.state_topic_min_isr = minimum,
        _ => panic!("unknown minimum ISR domain {kind}"),
    }
}

#[rstest_bdd_macros::given(
    "the operator explicitly supplied topic partitions {partitions} and replication {replication}"
)]
fn explicitly_supplied_auto_topic_defaults(
    auto_topic_definition: &mut AutoTopicDefinition,
    partitions: bool,
    replication: bool,
) {
    let origins = &mut auto_topic_definition
        .broker
        .as_mut()
        .expect("broker defaults were given")
        .static_config_origins
        .topic_creation;
    origins.num_partitions = partitions;
    origins.default_replication_factor = replication;
}

#[rstest_bdd_macros::given("the requested automatic topic name is {name}")]
fn supplied_auto_topic_name(auto_topic_definition: &mut AutoTopicDefinition, name: String) {
    auto_topic_definition.name = name;
}

#[rstest_bdd_macros::when("the manager builds the automatic topic definition")]
fn build_auto_topic_definition(auto_topic_definition: &mut AutoTopicDefinition) {
    auto_topic_definition.created = Some(creatable_topic(
        auto_topic_definition
            .broker
            .as_ref()
            .expect("broker defaults were given"),
        &auto_topic_definition.name,
    ));
}

#[rstest_bdd_macros::when("the manager builds the dead-letter topic definition")]
fn build_dead_letter_topic_definition(auto_topic_definition: &mut AutoTopicDefinition) {
    auto_topic_definition.created = Some(dead_letter_topic(&auto_topic_definition.name));
}

#[rstest_bdd_macros::then("the complete automatic topic definition is:")]
fn expected_auto_topic_definition(
    auto_topic_definition: &AutoTopicDefinition,
    #[datatable] rows: Rows<ExpectedTopicDefinition>,
) {
    let fields: BTreeMap<String, String> =
        rows.into_iter().map(|row| (row.field, row.value)).collect();
    let field = |name: &str| {
        fields
            .get(name)
            .expect("complete expected topic definition")
    };
    let values: Vec<(String, String)> =
        serde_json::from_str(field("configs")).expect("ordered independent topic config pairs");
    let pairs: Vec<_> = values
        .iter()
        .map(|(name, value)| ConfigPair { name, value })
        .collect();
    let expected = topic()
        .name(field("name"))
        .num_partitions(
            field("partitions")
                .parse::<i32>()
                .expect("expected partitions"),
        )
        .replication_factor(
            field("replication")
                .parse::<i16>()
                .expect("expected replication"),
        )
        .configs(configs(&pairs))
        .call();
    check!(auto_topic_definition.created.as_ref() == Some(&expected));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/auto_topic_creation/definitions",
    fixtures = [auto_topic_definition: AutoTopicDefinition]
);

#[derive(Default)]
struct StreamsTopicCompletion {
    creation: Option<AutoTopicCreation>,
    names: Vec<String>,
    response: Option<Result<CreateTopicsResponse, TopicCreatorError>>,
    cached: Option<BTreeMap<String, String>>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct ControllerTopicResult {
    name: String,
    code: i16,
    message: String,
}

#[rstest::fixture]
fn streams_topic_completion() -> StreamsTopicCompletion {
    // Every controller completion receives its own in-flight set and error cache.
    StreamsTopicCompletion::default()
}

#[rstest_bdd_macros::given("streams topics a, b, c and d are being created")]
fn in_flight_streams_topics(streams_topic_completion: &mut StreamsTopicCompletion) {
    let creation = AutoTopicCreation::default();
    streams_topic_completion.names = ["a", "b", "c", "d"].map(str::to_owned).to_vec();
    for name in &streams_topic_completion.names {
        assert!(creation.begin(name));
    }
    streams_topic_completion.creation = Some(creation);
}

#[rstest_bdd_macros::given("the controller answered topic creation with:")]
fn supplied_streams_topic_response(
    streams_topic_completion: &mut StreamsTopicCompletion,
    #[datatable] rows: Rows<ControllerTopicResult>,
) {
    let topics = rows
        .into_iter()
        .map(|row| {
            let message: Option<String> =
                serde_json::from_str(&row.message).expect("exact nullable controller message");
            result_row(&row.name, row.code, message.as_deref())
        })
        .collect();
    streams_topic_completion.response = Some(Ok(CreateTopicsResponse {
        throttle_time_ms: 0,
        topics,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }));
}

#[rstest_bdd_macros::given("the controller topic creation request timed out")]
fn timed_out_streams_topic_response(streams_topic_completion: &mut StreamsTopicCompletion) {
    streams_topic_completion.response = Some(Err(TopicCreatorError::Timeout));
}

#[rstest_bdd_macros::given("the controller refused the envelope with CLUSTER_AUTHORIZATION_FAILED")]
fn refused_streams_topic_envelope(streams_topic_completion: &mut StreamsTopicCompletion) {
    streams_topic_completion.response = Some(Err(TopicCreatorError::Envelope(
        codes::CLUSTER_AUTHORIZATION_FAILED,
    )));
}

#[rstest_bdd_macros::when(
    "streams creation completes at millisecond 100 with error cache lifetime 1000 milliseconds"
)]
fn finish_streams_topic_response(streams_topic_completion: &StreamsTopicCompletion) {
    streams_topic_completion
        .creation
        .as_ref()
        .expect("in-flight creations were given")
        .finish_streams_creation(
            &streams_topic_completion.names,
            streams_topic_completion
                .response
                .as_ref()
                .expect("controller response was given"),
            1000,
            100,
        );
}

#[rstest_bdd_macros::then("all four streams topics leave the in-flight set")]
fn finished_streams_topics(streams_topic_completion: &StreamsTopicCompletion) {
    let creation = streams_topic_completion
        .creation
        .as_ref()
        .expect("in-flight creations were given");
    for name in &streams_topic_completion.names {
        check!(!creation.is_in_flight(name));
    }
}

#[rstest_bdd_macros::when("streams creation errors are queried at millisecond {at}")]
fn query_streams_topic_errors(streams_topic_completion: &mut StreamsTopicCompletion, at: i64) {
    streams_topic_completion.cached = Some(
        streams_topic_completion
            .creation
            .as_ref()
            .expect("in-flight creations were given")
            .streams_internal_topic_creation_errors(
                streams_topic_completion.names.iter().map(String::as_str),
                at,
            ),
    );
}

#[rstest_bdd_macros::then("the cached streams topic errors are JSON {errors}")]
fn expected_streams_topic_errors(
    streams_topic_completion: &StreamsTopicCompletion,
    errors: String,
) {
    let expected: BTreeMap<String, String> =
        serde_json::from_str(&errors).expect("independent per-topic controller failures");
    check!(streams_topic_completion.cached.as_ref() == Some(&expected));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/auto_topic_creation/completion.feature",
    fixtures = [streams_topic_completion: StreamsTopicCompletion]
);
