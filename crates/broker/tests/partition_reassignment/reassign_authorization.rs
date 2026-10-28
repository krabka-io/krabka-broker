//! Authentication does not imply permission to alter the cluster's replica assignments.
use {
    crate::{
        plaintext_wire::CLIENT_ID,
        sasl_wire::{
            create_topic_as_admin, drive_alter_reassignments_sasl_plain,
            start_single_broker_sasl_plaintext_with_users,
        },
        support::{acl::topic_acl_record, retry::retry_until, topics::configured_topic_setup},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::net::SocketAddr,
    tempfile::TempDir,
};
type ReassignmentRows = Vec<(String, Vec<(i32, i16)>)>;
struct ReassignmentPermission {
    broker: Option<(BrokerHandle, TempDir, SocketAddr)>,
    response: Option<ReassignmentRows>,
}
#[fixture]
fn permission() -> ReassignmentPermission {
    // Start this scenario with fresh inputs and observations.

    ReassignmentPermission {
        broker: None,
        response: None,
    }
}
#[given("a SASL PLAIN broker grants admin superuser rights and authenticates these users")]
async fn identities(permission: &mut ReassignmentPermission, datatable: Vec<Vec<String>>) {
    let users: Vec<[String; 2]> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.try_into().expect("username password"))
        .collect();
    let users: Vec<(&str, &str)> = users
        .iter()
        .map(|[name, password]| (name.as_str(), password.as_str()))
        .collect();
    permission.broker = Some(start_single_broker_sasl_plaintext_with_users("admin", &users).await);
}
#[given("User admin has Read on topic {topic} while alice has no ACL grants")]
async fn nonempty_policy(permission: &mut ReassignmentPermission, topic: String) {
    let (broker, _, _) = permission.broker.as_ref().expect("SASL broker started");
    broker
        .submit_metadata_record_for_test(
            topic_acl_record()
                .topic(&topic)
                .principal("User:admin")
                .call(),
        )
        .await
        .expect("seed nonempty ACL policy");
    broker
        .wait_for_image(|image| image.all_acls().next().is_some())
        .await;
}
#[given("the superuser has created topic {topic}")]
async fn topic(permission: &mut ReassignmentPermission, topic: String) {
    let (broker, _, address) = permission.broker.as_ref().expect("SASL broker started");
    create_topic_as_admin()
        .addr(*address)
        .client_id(CLIENT_ID)
        .topic(configured_topic_setup().name(&topic).call())
        .call()
        .await;
    broker.wait_until_partition_present(&topic, 0).await;
}
#[when(
    "user {user} password {password} requests these replica assignments while authorization settles"
)]
async fn reassign(
    permission: &mut ReassignmentPermission,
    user: String,
    password: String,
    datatable: Vec<Vec<String>>,
) {
    let address = permission.broker.as_ref().expect("SASL broker started").2;
    let rows: Vec<(String, i32, Vec<i32>)> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [topic, partition, replicas]: [String; 3] =
                row.try_into().expect("topic partition replicas");
            (
                topic,
                partition.parse().expect("partition index"),
                replicas
                    .split(',')
                    .map(|id| id.parse().expect("replica ID"))
                    .collect(),
            )
        })
        .collect();
    permission.response = Some(
        retry_until()
            .attempt(|| async {
                drive_alter_reassignments_sasl_plain(
                    address,
                    &user,
                    &password,
                    rows.iter()
                        .map(|(topic, partition, replicas)| {
                            (topic.as_str(), *partition, Some(replicas.clone()))
                        })
                        .collect(),
                )
                .await
            })
            .is_complete(|rows| {
                rows.iter()
                    .all(|(_, partitions)| partitions.iter().all(|(_, code)| *code == 31))
            })
            .call()
            .await,
    );
}
#[then("the reassignment response contains exactly these cluster authorization failures")]
fn denied(permission: &ReassignmentPermission, datatable: Vec<Vec<String>>) {
    let expected: ReassignmentRows = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [topic, partition, code]: [String; 3] =
                row.try_into().expect("topic partition error code");
            (topic, vec![(
                partition.parse().expect("partition index"),
                code.parse().expect("error code"),
            )])
        })
        .collect();
    assert!(
        permission
            .response
            .as_ref()
            .expect("reassignment response received")
            == &expected
    );
}
#[scenario(
    path = "tests/features/migrated/tests/partition_reassignment/reassign_authorization.feature",
    name = "A valid PLAIN identity without cluster Alter cannot change replica assignments"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied(permission: ReassignmentPermission) {
    let (broker, _directory, _address) = permission.broker.expect("SASL broker started");
    broker.shutdown().await;
}
