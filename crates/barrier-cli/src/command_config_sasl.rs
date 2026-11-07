//! `--command-config` against a SASL/PLAINTEXT listener that enforces ACLs:
//! the broker allows or refuses each command by the principal the file's
//! credentials log in as.

use {
    crate::{EXIT_REFUSED, EXIT_UNREACHABLE, dispatch::run_from_args},
    assert2::check,
    krabka_broker::{Broker, BrokerHandle, testing::sasl_plain_broker_config},
    std::{fs, path::PathBuf},
    tempfile::TempDir,
};

/// The broker's only super-user. Every other principal has no ACL, so the
/// broker refuses it any barrier write.
const OPERATOR: (&str, &str) = ("alice", "alice-secret");
const BYSTANDER: (&str, &str) = ("bob", "bob-secret");

struct SaslCluster {
    dir: TempDir,
    bootstrap: String,
    _broker: BrokerHandle,
}

impl SaslCluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("temporary broker directory");
        let config = sasl_plain_broker_config()
            .log_dir(dir.path().join("data"))
            .users(&[OPERATOR, BYSTANDER])
            .super_users(&[OPERATOR.0])
            .call();
        let broker = Broker::start(config).await.expect("start SASL broker");
        Self {
            bootstrap: broker.listen_addr().to_string(),
            dir,
            _broker: broker,
        }
    }

    /// A Kafka client properties file that logs in with PLAIN.
    fn login(&self, (username, password): (&str, &str)) -> PathBuf {
        let path = self.dir.path().join(format!("{username}-{password}.conf"));
        fs::write(
            &path,
            format!(
                "security.protocol = SASL_PLAINTEXT\n\
                 sasl.mechanism = PLAIN\n\
                 sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule \
                 required username='{username}' password='{password}';\n"
            ),
        )
        .expect("write client properties");
        path
    }

    async fn run(&self, login: (&str, &str), command: &[&str]) -> i32 {
        let command_config = self.login(login);
        let mut argv = vec![
            "krabka-barrier",
            "-b",
            &self.bootstrap,
            "--command-config",
            command_config.to_str().expect("utf-8 path"),
        ];
        argv.extend_from_slice(command);
        run_from_args(argv).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_broker_authorizes_the_principal_the_command_config_logs_in_as() {
    let cluster = SaslCluster::start().await;
    let define = ["define", "--group", "orders-cut", "--topic", "orders"];

    check!(cluster.run(BYSTANDER, &define).await == EXIT_REFUSED);
    check!(cluster.run(OPERATOR, &define).await == 0);
    check!(cluster.run((OPERATOR.0, "guessed"), &define).await == EXIT_UNREACHABLE);
    check!(
        cluster
            .run(OPERATOR, &["describe", "--group", "orders-cut"])
            .await
            == 0
    );
}
