#![cfg(test)]

mod support;

use {
    crate::support::admin::topic_spec,
    assert2::check,
    krabka_client_admin::{
        AdminClient, AdminClientLike as _, AdminError, ConfigResource, DescribeConfigsOptions,
    },
    std::slice,
};

krabka_macros::bound_start_fixture!(config, bound_config, ::krabka_broker);
krabka_macros::bound_start_fixture!(start, start_bound, ::krabka_broker, unwrap, bound_config);

/// A running broker and the directory that backs it.
struct StartedBroker {
    broker: krabka_broker::BrokerHandle,
    dir: tempfile::TempDir,
}

async fn start_broker() -> StartedBroker {
    let StartedBoundBroker { broker, dir, .. } = start_bound(|_| {}).await;
    StartedBroker { broker, dir }
}

async fn describe_topic(
    controller_admin: &mut AdminClient,
    topic: &ConfigResource,
) -> krabka_client_admin::DescribeConfigsResults {
    controller_admin
        .describe_configs(slice::from_ref(topic), DescribeConfigsOptions::default())
        .await
        .unwrap()
}

use krabka_client_admin::DescribeConfigsResults;
struct ControllerAdminRouting {
    broker: Option<StartedBroker>,
    admin: Option<AdminClient>,
    configs: Option<DescribeConfigsResults>,
    reconciliation: Option<Result<(), AdminError>>,
    quotas: Option<Result<(), AdminError>>,
    connection: Option<Result<AdminClient, AdminError>>,
    creations: Option<Vec<Option<String>>>,
}
#[rstest::fixture]
fn routing() -> ControllerAdminRouting {
    // Start this scenario with fresh inputs and observations.

    ControllerAdminRouting {
        broker: None,
        admin: None,
        configs: None,
        reconciliation: None,
        quotas: None,
        connection: None,
        creations: None,
    }
}
#[rstest_bdd_macros::given("a broker serves separate data and controller endpoints")]
async fn controller_admin_endpoints(routing: &mut ControllerAdminRouting) {
    routing.broker = Some(start_broker().await);
}
#[rstest_bdd_macros::given("topic controller-admin already exists on the broker")]
async fn controller_admin_existing_topic(routing: &mut ControllerAdminRouting) {
    let bootstrap = routing
        .broker
        .as_ref()
        .expect("broker started")
        .broker
        .listen_addr()
        .to_string();
    let mut admin = AdminClient::connect(slice::from_ref(&bootstrap))
        .await
        .unwrap();
    let created = admin
        .create_topics(
            &[topic_spec("controller-admin", 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();
    check!(created[0].error.is_none());
}
#[rstest_bdd_macros::when("AdminClient connects using the controller bootstrap endpoint")]
async fn bootstrap_controller_admin(routing: &mut ControllerAdminRouting) {
    let bootstrap = routing
        .broker
        .as_ref()
        .expect("broker started")
        .broker
        .controller_addr()
        .to_string();
    routing.admin = Some(
        AdminClient::connect_controller(slice::from_ref(&bootstrap))
            .await
            .unwrap(),
    );
}
#[rstest_bdd_macros::when("the controller admin describes controller-admin topic configuration")]
async fn controller_admin_describe(routing: &mut ControllerAdminRouting) {
    routing.configs = Some(
        describe_topic(
            routing.admin.as_mut().expect("controller admin connected"),
            &ConfigResource::topic("controller-admin"),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the controller returns only controller-admin configuration successfully"
)]
fn controller_admin_described(routing: &ControllerAdminRouting) {
    let topic = ConfigResource::topic("controller-admin");
    let configs = routing.configs.as_ref().expect("configs requested");
    check!(configs.keys().collect::<Vec<_>>() == vec![&topic]);
    check!(configs[&topic].is_ok());
}
#[rstest_bdd_macros::when("the controller admin attempts replication factor reconciliation")]
async fn controller_admin_reconcile(routing: &mut ControllerAdminRouting) {
    routing.reconciliation = Some(
        routing
            .admin
            .as_mut()
            .expect("controller admin connected")
            .reconcile_topic_replication_factor("controller-admin", 1, krabka_units::secs(5))
            .await
            .map(|_| ()),
    );
}
#[rstest_bdd_macros::then(
    "reconciliation is locally refused with ControllerEndpoint error 115 UNSUPPORTED_ENDPOINT_TYPE"
)]
fn controller_admin_reconcile_refused(routing: &ControllerAdminRouting) {
    check!(matches!(
        routing
            .reconciliation
            .as_ref()
            .expect("reconciliation attempted"),
        Err(AdminError::Broker {
            api: "ControllerEndpoint",
            code: 115,
            name: "UNSUPPORTED_ENDPOINT_TYPE",
            ..
        })
    ));
}
#[rstest_bdd_macros::when("the controller admin creates created-through-controller")]
async fn controller_admin_create(routing: &mut ControllerAdminRouting) {
    let created = routing
        .admin
        .as_mut()
        .expect("controller admin connected")
        .create_topics(
            &[topic_spec("created-through-controller", 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();
    routing.creations = Some(
        created
            .into_iter()
            .map(|outcome| outcome.error.map(|error| format!("{error:?}")))
            .collect(),
    );
}
#[rstest_bdd_macros::then("the topic creation through the controller succeeds")]
fn controller_admin_topic_created(routing: &ControllerAdminRouting) {
    check!(
        routing
            .creations
            .as_ref()
            .expect("topic creation attempted")[0]
            .is_none()
    );
}
#[rstest_bdd_macros::when("the controller admin requests alice user quotas")]
async fn controller_admin_quotas(routing: &mut ControllerAdminRouting) {
    routing.quotas = Some(
        routing
            .admin
            .as_mut()
            .expect("controller admin connected")
            .describe_user_quotas("alice")
            .await
            .map(|_| ()),
    );
}
#[rstest_bdd_macros::then(
    "quota discovery is locally refused with ControllerEndpoint error 115 UNSUPPORTED_ENDPOINT_TYPE"
)]
fn controller_admin_quotas_refused(routing: &ControllerAdminRouting) {
    check!(matches!(
        routing.quotas.as_ref().expect("quota discovery attempted"),
        Err(AdminError::Broker {
            api: "ControllerEndpoint",
            code: 115,
            name: "UNSUPPORTED_ENDPOINT_TYPE",
            ..
        })
    ));
}
#[rstest_bdd_macros::when(
    "AdminClient treats the data endpoint as a controller bootstrap endpoint"
)]
async fn wrong_controller_bootstrap(routing: &mut ControllerAdminRouting) {
    routing.connection = Some(
        AdminClient::connect_controller(&[routing
            .broker
            .as_ref()
            .expect("broker started")
            .broker
            .listen_addr()
            .to_string()])
        .await,
    );
}
#[rstest_bdd_macros::then(
    "bootstrap is refused with DescribeCluster error 114 MISMATCHED_ENDPOINT_TYPE"
)]
fn mismatched_controller_bootstrap(routing: &ControllerAdminRouting) {
    check!(matches!(
        routing.connection.as_ref().expect("bootstrap attempted"),
        Err(AdminError::Broker {
            api: "DescribeCluster",
            code: 114,
            name: "MISMATCHED_ENDPOINT_TYPE",
            ..
        })
    ));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_admin_controller_bootstrap.feature",
    name = "controller bootstrap routes supported and rejects unsupported admin rpc"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_bootstrap_routes_supported_and_rejects_unsupported_admin_rpc(
    routing: ControllerAdminRouting,
) {
    let StartedBroker {
        broker,
        dir: _directory,
    } = routing.broker.expect("broker started");
    broker.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_admin_controller_bootstrap.feature",
    name = "controller bootstrap rejects broker endpoint"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_bootstrap_rejects_broker_endpoint(routing: ControllerAdminRouting) {
    let StartedBroker {
        broker,
        dir: _directory,
    } = routing.broker.expect("broker started");
    broker.shutdown().await;
}
