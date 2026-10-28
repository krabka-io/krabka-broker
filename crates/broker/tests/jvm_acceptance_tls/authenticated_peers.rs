//! Authenticated peer topologies and the stock client's observable writes.
use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, ClientPropsFile, KAFKA_IMAGE, KAFKA_IMAGE_TXN,
        assert_console_produced, create_console_topic, nc_check_connectivity, numbered_payload,
        prepare_jks_truststore, produce_console, provision_ssl_topic, start_two_sasl_brokers,
        start_two_sasl_ssl_brokers_with_controller_protocol, write_plain_props,
    },
    krabka_broker::BrokerHandle,
    krabka_security::ListenerProtocol,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::process::Output,
    tempfile::TempDir,
};
pub struct AuthenticatedPeers {
    brokers: Option<(BrokerHandle, BrokerHandle)>,
    directories: Option<(TempDir, TempDir)>,
    client: Option<ClientPropsFile>,
    admin: Option<ClientPropsFile>,
    truststore_mount: Option<String>,
    image: &'static str,
    topic: String,
    produced: Option<Output>,
}
#[fixture]
pub fn peers() -> AuthenticatedPeers {
    // Start this scenario with fresh inputs and observations.

    AuthenticatedPeers {
        brokers: None,
        directories: None,
        client: None,
        admin: None,
        truststore_mount: None,
        image: KAFKA_IMAGE,
        topic: String::new(),
        produced: None,
    }
}
#[given(
    "two registered PLAIN authenticated peers host a single replica topic for the administrator"
)]
async fn plaintext(peers: &mut AuthenticatedPeers) {
    let (broker0, broker1, dir0, dir1) = start_two_sasl_brokers(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();
    broker0.wait_until_brokers_registered(2).await;
    broker1.wait_until_brokers_registered(2).await;
    let props = write_plain_props(ADMIN, ADMIN_PASS);
    let topic = "krabka-jvm-inter-broker-itest";
    create_console_topic(KAFKA_IMAGE, &[&props.mount_str()], topic, 1, 1);
    tokio::select! {
        () = broker0.wait_until_partition_present(topic,0) => {}
        () = broker1.wait_until_partition_present(topic,0) => {}
    }
    peers.brokers = Some((broker0, broker1));
    peers.directories = Some((dir0, dir1));
    peers.client = Some(props);
    peers.topic = topic.into();
}
#[given(
    "two registered SASL SSL peers with encrypted controller traffic host an alice SCRAM topic replicated on both brokers"
)]
async fn encrypted(peers: &mut AuthenticatedPeers) {
    let (broker0, broker1, dir0, dir1) = start_two_sasl_ssl_brokers_with_controller_protocol(
        ListenerProtocol::SaslSsl,
        ADMIN,
        ADMIN_PASS,
    )
    .await;
    nc_check_connectivity();
    broker0.wait_until_brokers_registered(2).await;
    broker1.wait_until_brokers_registered(2).await;
    let store = prepare_jks_truststore();
    let mount = format!("{}:/truststore.jks:ro", store.display());
    let topic = "krabka-sasl-ssl-raft-rf2";
    let (admin, client) =
        provision_ssl_topic((ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS), &mount, topic, 2);
    broker0.wait_until_partition_present(topic, 0).await;
    broker1.wait_until_partition_present(topic, 0).await;
    peers.brokers = Some((broker0, broker1));
    peers.directories = Some((dir0, dir1));
    peers.client = Some(client);
    peers.admin = Some(admin);
    peers.truststore_mount = Some(mount);
    peers.topic = topic.into();
    peers.image = KAFKA_IMAGE_TXN;
}
#[when("the authenticated stock producer writes fifty numbered records through the peer cluster")]
fn write(peers: &mut AuthenticatedPeers) {
    let client = peers
        .client
        .as_ref()
        .expect("authenticated client configured")
        .mount_str();
    let mut mounts = vec![client.as_str()];
    if let Some(store) = &peers.truststore_mount {
        mounts.push(store);
    }
    peers.produced = Some(produce_console(
        peers.image,
        &mounts,
        &peers.topic,
        false,
        numbered_payload("rec", 50).as_bytes(),
    ));
}
#[then("the peer cluster producer succeeds")]
fn acknowledged(peers: &AuthenticatedPeers) {
    assert_console_produced(peers.produced.as_ref().expect("producer completed"));
}
#[then("the partition leader stores all fifty records")]
async fn leader_storage(peers: &AuthenticatedPeers) {
    let (broker0, broker1) = peers.brokers.as_ref().expect("peer cluster registered");
    tokio::select! {
        () = broker0.wait_until_local_log_end_offset(&peers.topic,0,50) => {}
        () = broker1.wait_until_local_log_end_offset(&peers.topic,0,50) => {}
    }
}
#[then("both encrypted replicas store all fifty records")]
async fn replica_storage(peers: &AuthenticatedPeers) {
    let (broker0, broker1) = peers.brokers.as_ref().expect("peer cluster registered");
    broker0
        .wait_until_local_log_end_offset(&peers.topic, 0, 50)
        .await;
    broker1
        .wait_until_local_log_end_offset(&peers.topic, 0, 50)
        .await;
}
pub async fn shutdown(peers: AuthenticatedPeers) {
    let (broker0, broker1) = peers.brokers.expect("peer cluster started");
    broker0.shutdown().await;
    broker1.shutdown().await;
}
