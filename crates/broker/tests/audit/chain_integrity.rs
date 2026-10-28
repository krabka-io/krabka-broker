//! The tamper-evidence apparatus over the audit records: the signed
//! checkpoints, and the hash chain those checkpoints sign.
//!
//! A chain is evidence only while it is unbroken. One case drives the
//! checkpoint cadence down to one record and looks for the signed checkpoint
//! on the topic. The other restarts the broker on the same data directory and
//! asserts that the second boot carried the sequence numbers on rather than
//! resetting them to zero.

use {
    crate::support::{
        self,
        topics::{configured_topic_request, creatable_topic, create_topic_request},
    },
    krabka_protocol::owned::create_topics_request::CreateTopicsRequest,
    ring::rand::SystemRandom,
    std::{fs, path::Path},
};

/// A broker booted on a data directory, with a client connected to it.
struct AuditedBoot {
    broker: krabka_broker::BrokerHandle,
    client: krabka_client_core::Client,
}

/// Await the exact topic-creation audit event on each boot before shutdown or consumption.
async fn boot_with_audited_topic(directory: &Path, request: CreateTopicsRequest) -> AuditedBoot {
    let (broker, client) = support::start_with_dir(directory).await;
    let audit_before = broker.metrics().audit_events.get();
    let _ = support::client::send_topic_creation(&client, request).await;
    broker
        .wait_for_metrics("audit event written", |metrics| {
            metrics.audit_events.get() > audit_before
        })
        .await;
    AuditedBoot { broker, client }
}
struct AuditChain {
    key_directory: Option<tempfile::TempDir>,
    directory: Option<tempfile::TempDir>,
    signed: Option<support::InProcess>,
    restarted: Option<AuditedBoot>,
    baseline: Option<u64>,
    sequences: Option<Vec<u64>>,
}
#[rstest::fixture]
fn audit_chain() -> AuditChain {
    // Start this scenario with fresh inputs and observations.

    AuditChain {
        key_directory: None,
        directory: None,
        signed: None,
        restarted: None,
        baseline: None,
        sequences: None,
    }
}
#[rstest_bdd_macros::given(
    "an Ed25519 audit signing key k-test with a checkpoint after every event"
)]
async fn signed_audit_input(audit_chain: &mut AuditChain) {
    use ring::signature::Ed25519KeyPair;

    // Generate a key, write it to a temp file, start a broker configured to use it.
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let keydir = tempfile::tempdir().unwrap();
    let keypath = keydir.path().join("audit.pk8");
    fs::write(&keypath, pkcs8.as_ref()).unwrap();

    // Start a broker with audit signing + a tiny checkpoint cadence (every 1 record).
    let p = support::start_with_audit_key(&keypath, "k-test", 1).await;

    audit_chain.key_directory = Some(keydir);
    audit_chain.signed = Some(p);
}
#[rstest_bdd_macros::when("the admin creates cp-topic")]
async fn create_checkpoint_event(audit_chain: &mut AuditChain) {
    let p = audit_chain.signed.as_ref().expect("signed broker started");
    let audit_before = p.broker.metrics().audit_events.get();
    let _ = p
        .client
        .send(create_topic_request(
            creatable_topic().name(("cp-topic").into()).call(),
        ))
        .await
        .unwrap();

    audit_chain.baseline = Some(audit_before);
}
#[rstest_bdd_macros::then(
    "a chained creation and signed checkpoint k-test become durable on the audit topic"
)]
async fn checkpoint_event_durable(audit_chain: &AuditChain) {
    let p = audit_chain.signed.as_ref().expect("signed broker started");
    let audit_before = audit_chain.baseline.expect("topic creation attempted");
    // Wait for the create's chained record AND its signed checkpoint to be
    // durable: with `every_n = 1`, each audit event triggers a checkpoint, so
    // the counter advances by 2 (chained record + checkpoint) per create.
    p.broker
        .wait_for_metrics("audit checkpoint written", |m| {
            m.audit_events.get() >= audit_before + 2
        })
        .await;

    let recs = support::wait_for_audit_record(&p.client, "signed checkpoint", |j| {
        j["type"] == "checkpoint" && j["key_id"] == "k-test"
    })
    .await;
    let saw_checkpoint = recs
        .iter()
        .any(|j| j["type"] == "checkpoint" && j["key_id"] == "k-test");
    assert2::check!(saw_checkpoint);
}
#[rstest_bdd_macros::given("a broker created r1 and shut down with its audit log retained")]
async fn prior_audit_boot(audit_chain: &mut AuditChain) {
    let dir = tempfile::tempdir().unwrap();

    // First boot: generate some audit events, then shut down cleanly.
    {
        let request = configured_topic_request().topic("r1").call();
        let AuditedBoot {
            broker,
            client: _client,
        } = boot_with_audited_topic(dir.path(), request).await;
        broker.shutdown().await;
    }

    audit_chain.directory = Some(dir);
}
#[rstest_bdd_macros::when("the broker restarts from the same log and creates r2")]
async fn restart_audit_log(audit_chain: &mut AuditChain) {
    let request = configured_topic_request().topic("r2").call();
    let AuditedBoot { broker, client } = boot_with_audited_topic(
        audit_chain
            .directory
            .as_ref()
            .expect("original log retained")
            .path(),
        request,
    )
    .await;

    audit_chain.restarted = Some(AuditedBoot { broker, client });
}
#[rstest_bdd_macros::when("a client reads at least four chained audit sequence numbers")]
async fn read_restarted_audit_chain(audit_chain: &mut AuditChain) {
    let client = &audit_chain
        .restarted
        .as_ref()
        .expect("broker restarted")
        .client;
    audit_chain.sequences = Some(support::wait_for_audit_seq_count(client, 4).await);
}
#[rstest_bdd_macros::then("the audit sequences are contiguous from zero and contain no duplicates")]
fn restarted_audit_chain_contiguous(audit_chain: &AuditChain) {
    let seqs = audit_chain
        .sequences
        .as_ref()
        .expect("audit sequences consumed");
    assert2::check!(seqs.len() >= 4); // 2 BrokerStarted + 2 CreateTopics (at least)
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert2::check!(sorted.len() == seqs.len()); // no duplicate seqs
    assert2::check!(sorted == (0..seqs.len() as u64).collect::<Vec<_>>()); // contiguous from 0
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/chain_integrity.feature",
    name = "signed checkpoints appear on audit topic"
)]
#[tokio::test]
async fn signed_checkpoints_appear_on_audit_topic(audit_chain: AuditChain) {
    audit_chain
        .signed
        .expect("broker started")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/chain_integrity.feature",
    name = "audit chain continues across restart"
)]
#[tokio::test]
async fn audit_chain_continues_across_restart(audit_chain: AuditChain) {
    audit_chain
        .restarted
        .expect("broker started")
        .broker
        .shutdown()
        .await;
}
