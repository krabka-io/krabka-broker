//! Handler tests for Kafka's version rules for topic ids in `Metadata`.
//!
//! `KafkaApis.handleTopicMetadataRequest` refuses a null name or a non-zero id
//! at versions 10 and 11. From version 12 on, a request with any non-zero id
//! describes only its ids, and ignores every name and every zero-id row. A
//! request with only zero ids describes its names, and a null name among them
//! fails the whole request. Each table row is one request, and the tests
//! compare the whole response as a client decodes it.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::{AllowAllAuthorizer, AuthorizationResult, Authorizer},
        broker::BrokerHandle,
        codes,
        test_support::{dispatch_wire, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            metadata_response::{MetadataResponse, MetadataResponseTopic},
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The two topics that every test creates.
const TOPIC_A: &str = "resolution-a";
const TOPIC_B: &str = "resolution-b";

/// Denies `Describe` on every topic and allows everything else, so that topic
/// creation still works and only the per-topic gate refuses.
#[derive(Debug)]
struct DenyTopicDescribe;

krabka_macros::test_authorizer!(DenyTopicDescribe, request; {
    if request.resource_type == ResourceType::Topic && request.operation == AclOperation::Describe {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

/// A request row: the name, if any, and the kind of id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Name {
    A,
    Unknown,
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Id {
    A,
    B,
    Unknown,
    Zero,
}

/// What Kafka answers for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expect {
    /// The ordinary response with these topic rows.
    Rows(Vec<Row>),
    /// `MetadataRequest.getErrorResponse` with this error code.
    Failed(i16),
}

/// One expected topic row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Row {
    /// The full metadata of topic A or topic B.
    Described(Id),
    /// `UNKNOWN_TOPIC_ID` with a null name and the unknown id.
    UnknownId,
    /// `UNKNOWN_TOPIC_OR_PARTITION` with the unknown name and the zero id.
    UnknownName,
    /// `TOPIC_AUTHORIZATION_FAILED` with a null name and the id of topic A.
    DeniedId,
    /// `TOPIC_AUTHORIZATION_FAILED` with the name of topic A and the zero id.
    DeniedName,
}

/// A topic reference of a request: the name and the id it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestedTopic {
    name: Name,
    id: Id,
}

/// One table row.
#[derive(Debug, Clone)]
struct Case {
    version: i16,
    rows: Vec<RequestedTopic>,
    expect: Expect,
}

struct Fixture {
    broker: BrokerHandle,
    _dir: tempfile::TempDir,
    id_a: WireUuid,
    id_b: WireUuid,
}

async fn start(authorizer: Arc<dyn Authorizer>) -> Fixture {
    let HandlerStartedBroker { broker, dir } =
        start_broker_no_audit_with(|cfg| cfg.authorizer = authorizer).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("metadata-resolution-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: [TOPIC_A, TOPIC_B]
                .into_iter()
                .map(|name| CreatableTopic {
                    name: name.to_owned(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                })
                .collect(),
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == codes::NONE),
        "{response:?}"
    );
    for name in [TOPIC_A, TOPIC_B] {
        broker.wait_until_partition_present(name, 0).await;
    }
    let image = broker.controller_image_for_test();
    let id = |name| WireUuid(image.topic(name).expect("topic").topic_id.into_bytes());
    let (id_a, id_b) = (id(TOPIC_A), id(TOPIC_B));
    Fixture {
        broker,
        _dir: dir,
        id_a,
        id_b,
    }
}

async fn metadata(
    broker: &BrokerHandle,
    version: i16,
    request: &MetadataRequest,
) -> MetadataResponse {
    let shared = broker.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(user, address, ctx);
        user ["describer"];
        client ["metadata-client"];
    };
    dispatch_wire(&shared, ApiKey::Metadata as i16, version, request, &ctx).await
}

impl Fixture {
    fn id(&self, id: Id) -> WireUuid {
        match id {
            Id::A => self.id_a,
            Id::B => self.id_b,
            Id::Unknown => UNKNOWN_ID,
            Id::Zero => WireUuid::ZERO,
        }
    }

    fn request(&self, rows: &[RequestedTopic]) -> MetadataRequest {
        MetadataRequest {
            topics: Some(
                rows.iter()
                    .map(|RequestedTopic { name, id }| MetadataRequestTopic {
                        name: match name {
                            Name::A => Some(TOPIC_A.to_owned()),
                            Name::Unknown => Some(UNKNOWN_NAME.to_owned()),
                            Name::Null => None,
                        },
                        topic_id: self.id(*id),
                        ..Default::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: false,
            ..Default::default()
        }
    }

    /// The expected response for `case`.
    ///
    /// The broker list, the cluster id, the controller and the rows of the
    /// described topics come from `baseline`, a v12 name-only request for both
    /// topics that an allow-all broker answered. The rules under test do not
    /// touch those parts.
    fn expected(&self, case: &Case, baseline: &MetadataResponse) -> MetadataResponse {
        match &case.expect {
            Expect::Failed(error_code) => {
                let request = self.request(&case.rows);
                MetadataResponse {
                    topics: request
                        .topics
                        .unwrap_or_default()
                        .into_iter()
                        .map(|topic| MetadataResponseTopic {
                            error_code: *error_code,
                            name: Some(topic.name.unwrap_or_default()),
                            topic_id: topic.topic_id,
                            ..Default::default()
                        })
                        .collect(),
                    // The wire carries the top-level error from v13 on.
                    error_code: if case.version >= 13 { *error_code } else { 0 },
                    ..Default::default()
                }
            }
            Expect::Rows(rows) => MetadataResponse {
                topics: rows
                    .iter()
                    .map(|row| match row {
                        Row::Described(id) => {
                            let name = if *id == Id::A { TOPIC_A } else { TOPIC_B };
                            baseline
                                .topics
                                .iter()
                                .find(|topic| topic.name.as_deref() == Some(name))
                                .expect("baseline row")
                                .clone()
                        }
                        Row::UnknownId => MetadataResponseTopic {
                            error_code: codes::UNKNOWN_TOPIC_ID,
                            name: None,
                            topic_id: UNKNOWN_ID,
                            ..Default::default()
                        },
                        Row::UnknownName => MetadataResponseTopic {
                            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                            name: Some(UNKNOWN_NAME.to_owned()),
                            topic_id: WireUuid::ZERO,
                            ..Default::default()
                        },
                        Row::DeniedId => MetadataResponseTopic {
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            name: None,
                            topic_id: self.id_a,
                            ..Default::default()
                        },
                        Row::DeniedName => MetadataResponseTopic {
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            name: Some(TOPIC_A.to_owned()),
                            topic_id: WireUuid::ZERO,
                            ..Default::default()
                        },
                    })
                    .collect(),
                ..baseline.clone()
            },
        }
    }

    async fn run(&self, cases: &[Case], baseline: &MetadataResponse) {
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for case in cases {
            let response = metadata(&self.broker, case.version, &self.request(&case.rows)).await;
            actual.push((case.version, case.rows.clone(), response));
            expected.push((
                case.version,
                case.rows.clone(),
                self.expected(case, baseline),
            ));
        }
        assert!(actual == expected);
    }
}

async fn baseline(fixture: &Fixture) -> MetadataResponse {
    let request = MetadataRequest {
        topics: Some(
            [TOPIC_A, TOPIC_B]
                .into_iter()
                .map(|name| MetadataRequestTopic {
                    name: Some(name.to_owned()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    let response = metadata(&fixture.broker, 12, &request).await;
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == codes::NONE),
        "{response:?}"
    );
    response
}

/// The response envelope for `fixture`'s own broker: `brokers`, `cluster_id`,
/// `controller_id` and `cluster_authorized_operations`, with no topic row.
///
/// An empty topic list asks for nothing, so no per-topic authorization runs,
/// and this works under any authorizer. `Fixture::expected` borrows these
/// fields for every case whose rows do not name `Row::Described`. Reading
/// them from `fixture` itself, rather than from a differently-authorized
/// sibling broker, matters because each `for_tests` broker binds its own
/// listener: two instances answer with two different ports, so a shared
/// baseline across instances no longer round-trips through `==`.
async fn envelope(fixture: &Fixture) -> MetadataResponse {
    let request = MetadataRequest {
        topics: Some(Vec::new()),
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    metadata(&fixture.broker, 12, &request).await
}

#[derive(Default)]
struct TopicResolutionScenario {
    fixture: Option<Fixture>,
    baseline: Option<MetadataResponse>,
    version: i16,
    rows: Vec<RequestedTopic>,
    response: Option<MetadataResponse>,
}
#[rstest::fixture]
fn topic_resolution() -> TopicResolutionScenario {
    let fixture = None;
    TopicResolutionScenario {
        fixture,
        ..Default::default()
    }
}
fn resolution_topic_id(text: &str) -> Id {
    match text {
        TOPIC_A => Id::A,
        TOPIC_B => Id::B,
        "unknown" => Id::Unknown,
        "zero" => Id::Zero,
        _ => panic!("unknown topic UUID reference: {text}"),
    }
}
#[rstest_bdd_macros::given(
    "topics resolution-a and resolution-b exist with their own UUIDs and a complete metadata baseline"
)]
async fn existing_resolution_topics(topic_resolution: &mut TopicResolutionScenario) {
    let fixture = start(Arc::new(AllowAllAuthorizer)).await;
    topic_resolution.baseline = Some(baseline(&fixture).await);
    topic_resolution.fixture = Some(fixture);
}
#[rstest_bdd_macros::given("Metadata API version {version} carries topic references {rows}")]
fn requested_topic_resolution(
    topic_resolution: &mut TopicResolutionScenario,
    version: i16,
    rows: String,
) {
    topic_resolution.version = version;
    topic_resolution.rows = rows
        .split(';')
        .map(|row| {
            let (name, id) = row.split_once('/').unwrap();
            RequestedTopic {
                name: match name {
                    TOPIC_A => Name::A,
                    UNKNOWN_NAME => Name::Unknown,
                    "null" => Name::Null,
                    _ => panic!("unknown topic name input: {name}"),
                },
                id: resolution_topic_id(id),
            }
        })
        .collect();
}
#[rstest_bdd_macros::when("the broker answers the versioned topic metadata request")]
async fn resolve_versioned_topics(topic_resolution: &mut TopicResolutionScenario) {
    let fixture = topic_resolution.fixture.as_ref().unwrap();
    topic_resolution.response = Some(
        metadata(
            &fixture.broker,
            topic_resolution.version,
            &fixture.request(&topic_resolution.rows),
        )
        .await,
    );
}
impl TopicResolutionScenario {
    async fn check_resolution_response(&mut self, expect: Expect) {
        let case = Case {
            version: self.version,
            rows: self.rows.clone(),
            expect,
        };
        let fixture = self.fixture.as_ref().expect("metadata fixture");
        assert!(
            self.response.as_ref().expect("metadata response")
                == &fixture.expected(&case, self.baseline.as_ref().expect("metadata baseline"))
        );
        self.fixture
            .take()
            .expect("metadata fixture")
            .broker
            .shutdown()
            .await;
    }
}
#[rstest_bdd_macros::then("the complete Metadata response contains ordered topic rows {rows}")]
async fn check_resolved_topic_rows(topic_resolution: &mut TopicResolutionScenario, rows: String) {
    let rows = rows
        .split(';')
        .map(|row| match row {
            "described resolution-a" => Row::Described(Id::A),
            "described resolution-b" => Row::Described(Id::B),
            "unknown UUID" => Row::UnknownId,
            "unknown name" => Row::UnknownName,
            _ => panic!("unknown expected topic row: {row}"),
        })
        .collect();
    topic_resolution
        .check_resolution_response(Expect::Rows(rows))
        .await;
}
#[rstest_bdd_macros::then("the complete Metadata request error response has code {code}")]
async fn check_topic_resolution_failure(topic_resolution: &mut TopicResolutionScenario, code: i16) {
    topic_resolution
        .check_resolution_response(Expect::Failed(code))
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_topic_resolution/resolution.feature",
    name = "Names and UUIDs resolve according to the requested Kafka version"
)]
#[tokio::test]
async fn topic_ids_follow_kafka_version_rules(
    topic_resolution: TopicResolutionScenario,
    version: i16,
    rows: String,
    expected: String,
) {
    let _ = (topic_resolution, version, rows, expected);
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_topic_resolution/resolution.feature",
    name = "Invalid UUID or null-name combinations fail the complete versioned request"
)]
#[tokio::test]
async fn invalid_topic_id_requests(
    topic_resolution: TopicResolutionScenario,
    version: i16,
    rows: String,
    code: i16,
) {
    let _ = (topic_resolution, version, rows, code);
}

/// A denied id row carries a null name and the real id. A denied name row
/// carries the name and the zero id. An unknown id needs no authorization, and
/// Kafka lists it ahead of every other row.
#[tokio::test]
async fn a_denied_topic_row_follows_how_the_request_names_it() {
    use Expect::Rows;
    let cases = vec![
        Case {
            version: 13,
            rows: vec![
                RequestedTopic {
                    name: Name::Null,
                    id: Id::A,
                },
                RequestedTopic {
                    name: Name::Null,
                    id: Id::Unknown,
                },
            ],
            expect: Rows(vec![Row::UnknownId, Row::DeniedId]),
        },
        Case {
            version: 12,
            rows: vec![RequestedTopic {
                name: Name::A,
                id: Id::Zero,
            }],
            expect: Rows(vec![Row::DeniedName]),
        },
    ];
    let denied = start(Arc::new(DenyTopicDescribe)).await;
    let baseline = envelope(&denied).await;

    denied.run(&cases, &baseline).await;
    denied.broker.shutdown().await;
}
