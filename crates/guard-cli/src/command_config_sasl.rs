//! `--command-config` against a SASL/PLAINTEXT listener: the freeze a tool
//! sets carries the principal the file's credentials log in as.

use {
    crate::{EXIT_UNREACHABLE, run_from_args},
    assert2::{assert, check},
    krabka_broker::{Broker, testing::sasl_plain_broker_config},
    krabka_cli_connection::{ConnectionArgs, SecurityArgs},
    krabka_protocol::krabka::freeze::{DescribeTopicFreezesRequest, PATTERN_TYPE_ANY},
    std::{
        fs,
        path::{Path, PathBuf},
    },
};

/// A Kafka client properties file that logs in to a SASL/PLAINTEXT listener
/// with PLAIN.
fn plain_login(dir: &Path, username: &str, password: &str) -> PathBuf {
    let path = dir.join(format!("{username}-{password}.properties"));
    let properties = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{username}\" password=\"{password}\";\n"
    );
    fs::write(&path, properties).expect("write client properties");
    path
}

async fn freeze_as(bootstrap: &str, command_config: &Path, topic: &str) -> i32 {
    run_from_args([
        "krabka-guard",
        "--bootstrap-server",
        bootstrap,
        "--command-config",
        command_config.to_str().expect("utf-8 path"),
        "freeze",
        "set",
        "--topic",
        topic,
        "--reason",
        "authentication drill",
    ])
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_freeze_set_over_sasl_carries_the_authenticated_principal() {
    let dir = tempfile::tempdir().expect("temporary broker directory");
    let config = sasl_plain_broker_config()
        .log_dir(dir.path().join("data"))
        .users(&[("alice", "alice-secret"), ("bob", "bob-secret")])
        .call();
    let broker = Broker::start(config).await.expect("start SASL broker");
    let bootstrap = broker.listen_addr().to_string();
    let alice = plain_login(dir.path(), "alice", "alice-secret");
    let bob = plain_login(dir.path(), "bob", "bob-secret");

    check!(freeze_as(&bootstrap, &alice, "orders").await == 0);
    check!(freeze_as(&bootstrap, &bob, "payments").await == 0);
    let wrong_password = plain_login(dir.path(), "alice", "guessed");
    check!(freeze_as(&bootstrap, &wrong_password, "refunds").await == EXIT_UNREACHABLE);

    let reader = ConnectionArgs {
        bootstrap_server: bootstrap,
        security: SecurityArgs {
            command_config: Some(alice),
        },
    }
    .connect("guard-sasl-test")
    .await;
    assert!(let Ok(reader) = reader);
    let described = reader
        .send(DescribeTopicFreezesRequest {
            pattern_type_filter: PATTERN_TYPE_ANY,
            ..DescribeTopicFreezesRequest::default()
        })
        .await
        .expect("describe freezes");
    let mut freezes = described
        .freezes
        .iter()
        .map(|freeze| (freeze.scope.as_str(), freeze.set_by.as_str()))
        .collect::<Vec<_>>();
    freezes.sort_unstable();
    check!(freezes == [("orders", "User:alice"), ("payments", "User:bob")]);
}
