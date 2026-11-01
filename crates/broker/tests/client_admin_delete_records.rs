#![cfg(test)]

mod support;

use {
    crate::support::client_admin::*,
    assert2::assert,
    krabka_client_admin::{AdminClientLike as _, DeleteRecordsOp},
    krabka_client_core::{ClientError, Connection, ConnectionOptions, fetch_partition},
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    tokio::net,
};

struct ClientRecordDeletion {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    bootstrap: String,
    admin: Option<krabka_client_admin::AdminClient>,
    outcomes: Option<Vec<krabka_client_admin::DeleteRecordsOutcome>>,
    fetch: Option<Result<(), ClientError>>,
}
#[rstest::fixture]
fn deletion() -> ClientRecordDeletion {
    // Start this scenario with fresh inputs and observations.

    ClientRecordDeletion {
        directory: None,
        broker: None,
        bootstrap: String::new(),
        admin: None,
        outcomes: None,
        fetch: None,
    }
}
#[rstest_bdd_macros::given("wal partition 0 contains 100 flushed records on a standalone broker")]
async fn populated_wal(deletion: &mut ClientRecordDeletion) {
    let (directory, broker, bootstrap, mut admin) = standalone_admin().await;
    admin
        .create_topics(
            &[topic_spec("wal".to_owned(), 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();

    let producer = default_producer(&bootstrap).await;
    for offset in 0..100 {
        producer
            .send(
                producer_record()
                    .topic("wal".to_owned())
                    .maybe_partition(Some(0))
                    .maybe_value(Some(Some(format!("frame-{offset}").into_bytes().into())))
                    .call(),
            )
            .await
            .unwrap();
    }
    producer.flush().await.unwrap();
    drop(producer);

    deletion.directory = Some(directory);
    deletion.broker = Some(broker);
    deletion.bootstrap = bootstrap;
    deletion.admin = Some(admin);
}
#[rstest_bdd_macros::when("the admin deletes wal partition 0 records before offset 50")]
async fn delete_wal_prefix(deletion: &mut ClientRecordDeletion) {
    let admin = deletion.admin.as_mut().expect("admin connected");
    let outcomes = admin
        .delete_records(
            &[DeleteRecordsOp {
                topic: "wal".to_owned(),
                partition: 0,
                offset: 50,
            }],
            krabka_units::secs(5),
        )
        .await
        .unwrap();

    deletion.outcomes = Some(outcomes);
}
#[rstest_bdd_macros::then(
    "DeleteRecords returns exactly one successful wal outcome with low watermark 50"
)]
fn deleted_wal_outcome(deletion: &ClientRecordDeletion) {
    let outcomes = deletion.outcomes.as_ref().expect("records deleted");
    assert!(
        outcomes.as_slice()
            == [krabka_client_admin::DeleteRecordsOutcome {
                topic: "wal".to_owned(),
                partition: 0,
                error_code: 0,
                low_watermark: 50,
            }]
    );
}
#[rstest_bdd_macros::when("a reader fetches wal partition 0 at offset 0")]
async fn read_deleted_wal(deletion: &mut ClientRecordDeletion) {
    let admin = deletion.admin.as_mut().expect("admin connected");
    let bootstrap = &deletion.bootstrap;
    let topic_id = admin
        .metadata(&["wal"])
        .await
        .unwrap()
        .topics
        .into_iter()
        .find(|topic| topic.name == "wal")
        .and_then(|topic| topic.topic_id)
        .map_or(WireUuid::ZERO, |id| WireUuid(*id.as_bytes()));

    let reader = connect_reader(bootstrap).await;
    let fetch_error = fetch_partition(
        &reader,
        "wal",
        topic_id,
        0,
        0,
        krabka_units::millis(500),
        krabka_units::mebibytes(1),
    )
    .await
    .map(|_| ());
    deletion.fetch = Some(fetch_error);
}
#[rstest_bdd_macros::then("the fetch fails with OFFSET_OUT_OF_RANGE error 1")]
fn deleted_wal_out_of_range(deletion: &ClientRecordDeletion) {
    let fetch = deletion.fetch.as_ref().expect("fetch attempted");
    assert!(
        matches!(fetch, Err(ClientError::Server { error_code: 1, .. })),
        "fetch below log start should return OFFSET_OUT_OF_RANGE, got {fetch:?}"
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_admin_delete_records.feature",
    name = "delete records truncates wal and maps outcome"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_truncates_wal_and_maps_outcome(deletion: ClientRecordDeletion) {
    deletion.broker.expect("broker started").shutdown().await;
}

async fn connect_reader(bootstrap: &str) -> Connection {
    let addr = net::lookup_host(bootstrap)
        .await
        .expect("resolve bootstrap")
        .next()
        .expect("bootstrap address");
    Connection::connect_with_options(addr, ConnectionOptions {
        client_id: "delete-records-test-reader".to_owned(),
        ..Default::default()
    })
    .await
    .expect("connect reader")
}
