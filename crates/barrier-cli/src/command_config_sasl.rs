//! `--command-config` against a SASL/PLAINTEXT listener that enforces ACLs:
//! the broker allows or refuses each command by the principal the file's
//! credentials log in as.

use {
    crate::{Cli, Console, EXIT_REFUSED, EXIT_UNREACHABLE, dispatch::run_with},
    assert2::{assert, check},
    clap::Parser as _,
    krabka_broker::{Broker, BrokerHandle, testing::sasl_plain_broker_config},
    std::{fs, path::PathBuf, time::Duration},
    tempfile::TempDir,
    tokio::time::{self, Instant},
};

/// What one run of the tool ended with.
struct Outcome {
    exit: i32,
    stderr: String,
}

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

    async fn run(&self, login: (&str, &str), command: &[&str]) -> Outcome {
        let command_config = self.login(login);
        let mut argv = vec![
            "krabka-barrier",
            "-b",
            &self.bootstrap,
            "--command-config",
            command_config.to_str().expect("utf-8 path"),
        ];
        argv.extend_from_slice(command);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let exit = run_with(Cli::parse_from(argv), &mut Console::new(&mut out, &mut err)).await;
        Outcome {
            exit,
            stderr: String::from_utf8(err).expect("utf-8 stderr"),
        }
    }

    /// Wait until the operator can read barrier groups, so a refusal below
    /// cannot be a coordinator that has not loaded `__barrier_state` yet.
    async fn wait_for_coordinator(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.run(OPERATOR, &["describe"]).await.exit != 0 {
            assert!(
                Instant::now() < deadline,
                "the barrier coordinator never became available"
            );
            time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_broker_authorizes_the_principal_the_command_config_logs_in_as() {
    let cluster = SaslCluster::start().await;
    cluster.wait_for_coordinator().await;
    let define = ["define", "--group", "orders-cut", "--topic", "orders"];

    // The broker refuses bob for who he is, not for any other reason.
    let refused = cluster.run(BYSTANDER, &define).await;
    check!(refused.exit == EXIT_REFUSED);
    check!(
        refused.stderr.contains("alter-barrier-groups denied"),
        "got: {}",
        refused.stderr
    );

    check!(cluster.run(OPERATOR, &define).await.exit == 0);
    check!(
        cluster
            .run(OPERATOR, &["describe", "--group", "orders-cut"])
            .await
            .exit
            == 0
    );
    let wrong_password = cluster.run((OPERATOR.0, "guessed"), &define).await;
    check!(wrong_password.exit == EXIT_UNREACHABLE);
}
