//! KIP-108 / KIP-133: the `[topic_policy]` rule set refusing a `CreateTopics`
//! and a topic `IncrementalAlterConfigs` over the wire, with
//! `POLICY_VIOLATION` (44) and the reason the operator reads.
//!
//! `kafka-topics --create --replication-factor 1` and `kafka-configs --alter
//! --add-config unclean.leader.election.enable=true` send exactly these two
//! requests; the JVM tools turn error 44 plus the message into the
//! `PolicyViolationException` they print.

use {
    crate::{
        RESOURCE_TYPE_TOPIC,
        admin_harness::{build_client, create_topic_helper},
        support::{
            configs::{
                ConfigOperation, incremental_config, incremental_request, incremental_resource,
            },
            shutdown_cluster, start_n_node_with,
            topics::{creatable_topic, create_topic_request},
        },
    },
    assert2::{assert, check},
    krabka_broker::{BrokerConfig, BrokerHandle, topic_policy::TopicPolicy},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        create_topics_request::CreateTopicsRequest, create_topics_response::CreateTopicsResponse,
        incremental_alter_configs_request::IncrementalAlterConfigsRequest,
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{collections::BTreeMap, iter},
    tempfile::TempDir,
};

/// `POLICY_VIOLATION` in the Kafka error table.
const POLICY_VIOLATION: i16 = 44;

fn create_request(name: &str, replication_factor: i16) -> CreateTopicsRequest {
    create_topic_request(
        creatable_topic()
            .name((name).into())
            .replicas(replication_factor)
            .call(),
    )
}

fn alter_request(topic: &str, value: &str) -> IncrementalAlterConfigsRequest {
    incremental_request()
        .resources(vec![incremental_resource(
            RESOURCE_TYPE_TOPIC,
            topic,
            vec![incremental_config(
                "unclean.leader.election.enable",
                Some(value.into()),
                ConfigOperation::Set,
            )],
        )])
        .call()
}
struct PolicyScenario {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    client: Option<Client>,
    created: Option<CreateTopicsResponse>,
    altered: Option<IncrementalAlterConfigsResponse>,
}

#[bon::bon]
impl PolicyScenario {
    #[builder]
    async fn start(
        &mut self,
        min_replication_factor: Option<usize>,
        max_partitions: Option<usize>,
        #[builder(default)] forbidden: BTreeMap<String, String>,
    ) {
        let policy = TopicPolicy {
            min_replication_factor,
            max_partitions,
            forbidden,
            ..Default::default()
        };
        self.cluster = start_n_node_with(1, |_, cfg| cfg.topic_policy = policy.clone())
            .await
            .expect("start_n_node_with");
        self.client = Some(build_client(self.cluster[0].1.listen_addr).await);
    }

    fn client(&self) -> &Client { self.client.as_ref().expect("a topic-policy broker") }

    fn broker(&self) -> &BrokerHandle { &self.cluster[0].0 }
}

#[fixture]
fn policy() -> PolicyScenario {
    // Start this scenario with fresh inputs and observations.

    PolicyScenario {
        cluster: Vec::new(),
        client: None,
        created: None,
        altered: None,
    }
}

#[given("topic policy requires at least 3 replicas")]
async fn replication_floor(policy: &mut PolicyScenario) {
    policy.start().min_replication_factor(3).call().await;
}

#[given("topic policy permits 1 replica and at most 4 partitions")]
async fn allowed_topic_policy(policy: &mut PolicyScenario) {
    policy
        .start()
        .min_replication_factor(1)
        .max_partitions(4)
        .call()
        .await;
}

#[given("topic policy forbids unclean.leader.election.enable=true")]
async fn forbidden_topic_config(policy: &mut PolicyScenario) {
    policy
        .start()
        .forbidden(
            iter::once((
                "unclean.leader.election.enable".to_owned(),
                "true".to_owned(),
            ))
            .collect(),
        )
        .call()
        .await;
}

#[given("topic policy-alter already exists with one partition")]
async fn existing_policy_topic(policy: &PolicyScenario) {
    create_topic_helper(policy.client(), "policy-alter", 1).await;
}

#[when("the operator creates topic {name} with 1 replica and validate-only {validate_only}")]
async fn policy_create(policy: &mut PolicyScenario, name: String, validate_only: bool) {
    policy.created = Some(
        policy
            .client()
            .send(CreateTopicsRequest {
                validate_only,
                ..create_request(&name, 1)
            })
            .await
            .expect("create_topics"),
    );
}

#[then("topic creation is refused with POLICY_VIOLATION naming replication factor 1 and floor 3")]
fn below_floor(policy: &PolicyScenario) {
    let result = &policy
        .created
        .as_ref()
        .expect("a topic creation request")
        .topics[0];
    check!(
        result.error_code == POLICY_VIOLATION,
        "{:?}",
        result.error_message
    );
    let message = result.error_message.clone().unwrap_or_default();
    check!(message.contains("replication factor 1"), "{message}");
    check!(message.contains("at least 3"), "{message}");
}

#[then("topic creation succeeds")]
fn successful_policy_create(policy: &PolicyScenario) {
    let result = &policy
        .created
        .as_ref()
        .expect("a topic creation request")
        .topics[0];
    check!(result.error_code == 0, "{:?}", result.error_message);
}

#[then("no topic named {name} is committed")]
fn absent_policy_topic(policy: &PolicyScenario, name: String) {
    assert!(
        policy
            .broker()
            .controller_image_for_test()
            .topic(&name)
            .is_none(),
        "validate-only or refused requests must commit no topic"
    );
}

#[then("topic policy-ok has one partition and is committed")]
fn committed_policy_topic(policy: &PolicyScenario) {
    let result = &policy
        .created
        .as_ref()
        .expect("a successful topic creation")
        .topics[0];
    check!(result.num_partitions == 1);
    assert!(
        policy
            .broker()
            .controller_image_for_test()
            .topic("policy-ok")
            .is_some()
    );
}

#[when("the operator sets policy-alter unclean.leader.election.enable to {value}")]
async fn policy_alter(policy: &mut PolicyScenario, value: String) {
    policy.altered = Some(
        policy
            .client()
            .send(alter_request("policy-alter", &value))
            .await
            .expect("incremental_alter_configs"),
    );
}

#[then("the configuration change is refused with POLICY_VIOLATION naming the forbidden key")]
fn forbidden_config_refused(policy: &PolicyScenario) {
    let result = &policy
        .altered
        .as_ref()
        .expect("a configuration change")
        .responses[0];
    check!(
        result.error_code == POLICY_VIOLATION,
        "{:?}",
        result.error_message
    );
    let message = result.error_message.clone().unwrap_or_default();
    check!(
        message.contains("unclean.leader.election.enable"),
        "{message}"
    );
    check!(message.contains("forbids"), "{message}");
}

#[then("the configuration change succeeds")]
fn allowed_config_commits(policy: &PolicyScenario) {
    let result = &policy
        .altered
        .as_ref()
        .expect("a configuration change")
        .responses[0];
    check!(result.error_code == 0, "{:?}", result.error_message);
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_topic_policy.feature",
    name = "create topics below the replication floor is a policy violation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_below_the_replication_floor_is_a_policy_violation(
    policy: PolicyScenario,
    validate_only: bool,
) {
    let _ = validate_only;
    shutdown_cluster(policy.cluster).await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_topic_policy.feature",
    name = "a topic the policy allows is created and validate only commits nothing"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_the_policy_allows_is_created_and_validate_only_commits_nothing(
    policy: PolicyScenario,
) {
    shutdown_cluster(policy.cluster).await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_topic_policy.feature",
    name = "altering a topic to a forbidden config is a policy violation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn altering_a_topic_to_a_forbidden_config_is_a_policy_violation(policy: PolicyScenario) {
    shutdown_cluster(policy.cluster).await;
}
