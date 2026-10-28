//! Handler tests for how `Metadata` authorizes, auto-creates and orders its
//! topic rows, after Kafka's `KafkaApis.handleTopicMetadataRequest`.
//!
//! One broker serves every case that shares an `auto.create.topics.enable`
//! value. The principal under test holds exactly the grants a case lists, and
//! each case asks about topics of its own, so a topic one case auto-creates is
//! never missing in another.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::{AuthorizationResult, Authorizer},
        broker::BrokerHandle,
        codes,
        handlers::{acl_wire::CLUSTER_RESOURCE_NAME, test_support::configured_topic_request},
        test_support::{dispatch_wire, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            metadata_response::{MetadataResponse, MetadataResponseTopic},
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        collections::HashSet,
        sync::{Arc, Mutex},
    },
};

/// The principal whose grants each case sets.
const TESTER: &str = "tester";

/// The topic that exists before any case runs.
const EXISTING: &str = "authz-existing";

/// The wire bit of an operation in an authorized-operations field (KIP-430):
/// `1 << AclOperation.code`.
const DESCRIBE_BIT: i32 = 1 << 8;
const CREATE_BIT: i32 = 1 << 5;

/// One grant: a resource type, a resource name and an operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Grant {
    resource_type: ResourceType,
    resource_name: String,
    operation: AclOperation,
}

/// Allows [`TESTER`] exactly the grants it holds, and every other principal
/// everything, so the broker's own setup is never refused.
#[derive(Debug, Default)]
struct Grants(Mutex<HashSet<Grant>>);

impl Grants {
    fn set(&self, grants: &[Grant]) {
        *self.0.lock().expect("grants") = grants.iter().cloned().collect();
    }
}

krabka_macros::test_authorizer!(Grants, request; {
    if request.principal.name != TESTER
        || self.0.lock().expect("grants").contains(&Grant {
            resource_type: request.resource_type,
            resource_name: request.resource_name.to_owned(),
            operation: request.operation,
        })
    {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
});

fn topic(name: &str, operation: AclOperation) -> Grant {
    Grant {
        resource_type: ResourceType::Topic,
        resource_name: name.to_owned(),
        operation,
    }
}

fn cluster(operation: AclOperation) -> Grant {
    Grant {
        resource_type: ResourceType::Cluster,
        resource_name: CLUSTER_RESOURCE_NAME.to_owned(),
        operation,
    }
}

struct Fixture {
    broker: BrokerHandle,
    grants: Arc<Grants>,
    _dir: tempfile::TempDir,
}

/// A broker whose `auto.create.topics.enable` is `auto_create_topics_enable`,
/// holding [`EXISTING`].
async fn start(auto_create_topics_enable: bool) -> Fixture {
    let grants = Arc::new(Grants::default());
    let authorizer: Arc<dyn Authorizer> = Arc::clone(&grants) as Arc<dyn Authorizer>;
    let HandlerStartedBroker { broker, dir } = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = authorizer;
        cfg.auto_create_topics_enable = auto_create_topics_enable;
    })
    .await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("metadata-authorization-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(configured_topic_request().topic(EXISTING).call())
        .await
        .expect("CreateTopics");
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == codes::NONE),
        "{response:?}"
    );
    broker.wait_until_partition_present(EXISTING, 0).await;
    Fixture {
        broker,
        grants,
        _dir: dir,
    }
}

impl Fixture {
    async fn metadata(&self, version: i16, request: &MetadataRequest) -> MetadataResponse {
        let shared = self.broker.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(user, address, ctx);
            user [TESTER];
            client ["metadata-client"];
        };
        dispatch_wire(&shared, ApiKey::Metadata as i16, version, request, &ctx).await
    }

    /// The row an allow-all principal gets for [`EXISTING`] at `version`.
    async fn existing_row(&self, version: i16) -> MetadataResponseTopic {
        self.grants.set(&[topic(EXISTING, AclOperation::Describe)]);
        let response = self
            .metadata(version, &named(&[EXISTING], false, false))
            .await;
        let [row] = <[MetadataResponseTopic; 1]>::try_from(response.topics).expect("one row");
        assert!(row.error_code == codes::NONE);
        row
    }
}

fn named(names: &[&str], allow_auto_topic_creation: bool, include_ops: bool) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            names
                .iter()
                .map(|name| MetadataRequestTopic {
                    name: Some((*name).to_owned()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation,
        include_topic_authorized_operations: include_ops,
        ..Default::default()
    }
}

/// A partitionless row for a topic that is not described in full.
fn row(error_code: i16, name: &str) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code,
        name: Some(name.to_owned()),
        topic_id: WireUuid::ZERO,
        ..Default::default()
    }
}

#[derive(Default)]
struct MetadataAuthorizationScenario {
    fixture: Option<Fixture>,
    version: i16,
    existing: Option<MetadataResponseTopic>,
    response: Option<MetadataResponse>,
    requested: Vec<String>,
}
#[rstest::fixture]
fn metadata_authorization() -> MetadataAuthorizationScenario {
    let fixture = None;
    MetadataAuthorizationScenario {
        fixture,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a metadata broker with automatic topic creation {enabled} and protocol version {version}"
)]
async fn metadata_creation_policy(
    metadata_authorization: &mut MetadataAuthorizationScenario,
    enabled: bool,
    version: i16,
) {
    let fixture = start(enabled).await;
    metadata_authorization.existing = Some(fixture.existing_row(version).await);
    metadata_authorization.fixture = Some(fixture);
    metadata_authorization.version = version;
}
#[rstest_bdd_macros::given("the metadata principal holds these exact grants")]
fn metadata_topic_grants(
    metadata_authorization: &mut MetadataAuthorizationScenario,
    datatable: Vec<Vec<String>>,
) {
    let grants = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let operation = match row[1].as_str() {
                "Describe" => AclOperation::Describe,
                "Create" => AclOperation::Create,
                value => panic!("unknown operation {value}"),
            };
            if row[0] == "cluster" {
                cluster(operation)
            } else {
                topic(&row[0], operation)
            }
        })
        .collect::<Vec<_>>();
    metadata_authorization
        .fixture
        .as_ref()
        .expect("started broker")
        .grants
        .set(&grants);
}
#[rstest_bdd_macros::when(
    "metadata requests automatic creation {auto} and authorized operations {ops} for these topics"
)]
async fn request_authorized_metadata(
    metadata_authorization: &mut MetadataAuthorizationScenario,
    auto: bool,
    ops: bool,
    datatable: Vec<Vec<String>>,
) {
    metadata_authorization.requested = datatable
        .into_iter()
        .skip(1)
        .map(|row| row[0].clone())
        .collect();
    let names = metadata_authorization
        .requested
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    metadata_authorization.response = Some(
        metadata_authorization
            .fixture
            .as_ref()
            .expect("started broker")
            .metadata(metadata_authorization.version, &named(&names, auto, ops))
            .await,
    );
}
#[rstest_bdd_macros::then("metadata returns these complete ordered topic rows")]
fn authorized_metadata_rows(
    metadata_authorization: &MetadataAuthorizationScenario,
    datatable: Vec<Vec<String>>,
) {
    let expected = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            let base = if cells[1] == "full" {
                metadata_authorization
                    .existing
                    .as_ref()
                    .expect("existing topic baseline")
                    .clone()
            } else {
                row(cells[1].parse().expect("error code"), &cells[0])
            };
            MetadataResponseTopic {
                topic_authorized_operations: cells[2].parse().expect("authorized operations"),
                ..base
            }
        })
        .collect::<Vec<_>>();
    assert!(
        metadata_authorization
            .response
            .as_ref()
            .expect("metadata response")
            .topics
            == expected
    );
}
#[rstest_bdd_macros::then("only these requested missing topics are created: \"{created}\"")]
async fn metadata_creation_effects(
    metadata_authorization: &mut MetadataAuthorizationScenario,
    created: String,
) {
    let fixture = metadata_authorization
        .fixture
        .take()
        .expect("started broker");
    let created = created
        .split(',')
        .filter(|name| !name.is_empty() && *name != "<empty>")
        .collect::<Vec<_>>();
    for name in &created {
        fixture.broker.wait_until_partition_present(name, 0).await;
    }
    {
        let image = fixture.broker.controller_image_for_test();
        for name in &metadata_authorization.requested {
            if name != EXISTING && !created.contains(&name.as_str()) {
                assert!(image.topic(name).is_none(), "unexpected topic {name}");
            }
        }
    }
    fixture.broker.shutdown().await;
}

/// KIP-430 `cluster_authorized_operations`: Kafka answers 0 when `Describe`
/// on the cluster is denied, whatever else the principal holds, and the bit
/// field when it is allowed.
#[tokio::test]
async fn cluster_authorized_operations_need_cluster_describe() {
    let cases = [
        ("Create only", vec![cluster(AclOperation::Create)], 0),
        (
            "Describe and Create",
            vec![
                cluster(AclOperation::Describe),
                cluster(AclOperation::Create),
            ],
            DESCRIBE_BIT | CREATE_BIT,
        ),
    ];
    let fixture = start(true).await;
    for (name, grants, expected) in cases {
        fixture.grants.set(&grants);
        let request = MetadataRequest {
            topics: Some(Vec::new()),
            include_cluster_authorized_operations: true,
            ..Default::default()
        };
        let response = fixture.metadata(8, &request).await;
        assert!(response.cluster_authorized_operations == expected, "{name}");
    }
    fixture.broker.shutdown().await;
}

/// `MetadataRequest.isAllTopics`: at version 0 an empty topic list asks for
/// every topic, and like any all-topics request it leaves out the topics the
/// principal may not describe.
#[tokio::test]
async fn an_empty_version_0_request_asks_for_every_topic() {
    let fixture = start(true).await;
    let existing = fixture.existing_row(0).await;
    fixture
        .grants
        .set(&[topic(EXISTING, AclOperation::Describe)]);
    let response = fixture
        .metadata(0, &MetadataRequest {
            topics: Some(Vec::new()),
            ..Default::default()
        })
        .await;
    assert!(response.topics == vec![existing]);

    fixture.grants.set(&[]);
    let empty_req = MetadataRequest {
        topics: Some(Vec::new()),
        ..Default::default()
    };
    let response = fixture.metadata(0, &empty_req).await;
    assert!(response.topics.is_empty());
    fixture.broker.shutdown().await;
}

/// The checks that Kafka's `KafkaApis.handleTopicMetadataRequest` makes with
/// `logIfDenied = false` leave no Deny in `authorization_denied_total` (or in
/// the audit trail behind it): the `Describe` filter of an all-topics request,
/// the cluster `Create` probe for auto-creation, and every operation of an
/// authorized-operations field. A `Describe` denied on a topic the request
/// names is a refusal and is counted.
#[tokio::test]
async fn quiet_probes_leave_no_denial_behind_and_a_named_refusal_does() {
    use {
        crate::metrics::AuthorizationDeniedLabel,
        AclOperation::{Alter, Create, Describe},
    };

    let fixture = start(true).await;
    let shared = fixture.broker.broker_arc_for_test();
    let denied = |operation: &str, resource_type: &str| {
        shared
            .metrics
            .authorization_denied
            .get_or_create(&AuthorizationDeniedLabel {
                operation: operation.into(),
                resource_type: resource_type.into(),
            })
            .get()
    };

    // An all-topics request includes an allowed topic and quietly hides one
    // the principal may not describe.
    let existing = fixture.existing_row(12).await;
    let all_topics = MetadataRequest {
        topics: None,
        ..Default::default()
    };
    for (grants, expected) in [
        (vec![topic(EXISTING, Describe)], vec![existing]),
        (vec![], vec![]),
    ] {
        fixture.grants.set(&grants);
        let response = fixture.metadata(12, &all_topics).await;
        assert!(response.topics == expected);
        assert!(denied("Describe", "Topic") == 0);
    }

    // Auto-creation with topic `Create` alone: the cluster `Create` probe is
    // denied, and the per-topic check allows the topic.
    fixture.grants.set(&[
        topic("authz-quiet-probe", Describe),
        topic("authz-quiet-probe", Create),
    ]);
    let response = fixture
        .metadata(12, &named(&["authz-quiet-probe"], true, false))
        .await;
    assert!(response.topics[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    fixture
        .broker
        .wait_until_partition_present("authz-quiet-probe", 0)
        .await;
    assert!(denied("Create", "Cluster") == 0);
    assert!(denied("Create", "Topic") == 0);

    // The operations of a topic the principal may describe, all but `Describe`
    // denied: a probe, not a refusal.
    fixture.grants.set(&[topic(EXISTING, Describe)]);
    let response = fixture.metadata(12, &named(&[EXISTING], false, true)).await;
    assert!(response.topics[0].topic_authorized_operations == DESCRIBE_BIT);
    assert!(denied("Alter", "Topic") == 0);

    // The same topic named without the `Describe` grant is a refusal.
    fixture.grants.set(&[topic(EXISTING, Alter)]);
    let response = fixture
        .metadata(12, &named(&[EXISTING], false, false))
        .await;
    assert!(response.topics[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    assert!(denied("Describe", "Topic") == 1);
    fixture.broker.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "cluster Create auto-creates a missing topic and answers 3"
)]
#[tokio::test]
async fn metadata_authorization_case_0(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "topic Create alone auto-creates a missing topic"
)]
#[tokio::test]
async fn metadata_authorization_case_1(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "no Create answers 29 with the zero id"
)]
#[tokio::test]
async fn metadata_authorization_case_2(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "no auto-creation asked needs no Create"
)]
#[tokio::test]
async fn metadata_authorization_case_3(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "an invalid name answers 17 and is not created"
)]
#[tokio::test]
async fn metadata_authorization_case_4(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "rows are described, then denied Create, then denied Describe"
)]
#[tokio::test]
async fn metadata_authorization_case_5(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "a repeated name answers one row"
)]
#[tokio::test]
async fn metadata_authorization_case_6(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "the missing topic row carries its authorized operations"
)]
#[tokio::test]
async fn metadata_authorization_case_7(metadata_authorization: MetadataAuthorizationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_metadata_authorization.feature",
    name = "auto.create.topics.enable = false answers 3 and creates nothing"
)]
#[tokio::test]
async fn metadata_authorization_case_8(metadata_authorization: MetadataAuthorizationScenario) {}
