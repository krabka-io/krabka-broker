//! SASL authentication through the Kafka 4.3.1 JVM client, checked against a
//! stock Kafka 4.3.1 broker.
//!
//! The [`plain`], [`scram`] and [`oauthbearer`] round-trips run the cp-kafka
//! 6.1.1 and 7.5.0 clients. This module runs the `apache/kafka:4.3.1` client
//! instead, and it covers the refusals as well as the successes: each
//! mechanism logs in with good credentials and with bad ones, and the case
//! compares what `kafka-topics.sh --list` reports.
//!
//! Each case is a differential. The same 4.3.1 tool runs first against a stock
//! 4.3.1 broker (the [`oracle`]) and then against krabka, and both outcomes
//! must equal the expectation in the case table. When Kafka disagrees with a
//! row, the oracle half fails first, so the table cannot drift from Kafka.
//!
//! Both brokers enable PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER on
//! one `SASL_PLAINTEXT` listener. Each knows `alice` as a PLAIN user and holds
//! a SCRAM credential for her under both digests. Each validates OAUTHBEARER
//! tokens with its unsecured-JWS validator under the default principal claim
//! `sub`. The negative OAUTHBEARER case relies on that claim: the client's
//! unsecured login handler puts the principal in `upn` instead, which leaves
//! the token with no `sub`.
//!
//! [`plain`]: super::plain
//! [`scram`]: super::scram
//! [`oauthbearer`]: super::oauthbearer
//! [`oracle`]: super::oracle

use {
    crate::{
        jvm_acceptance::{
            ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, broker0_advertised, configure_sasl,
            plain_client_properties, plain_jaas, scram_jaas, start_host_broker_with,
        },
        oracle::{CliRun, Oracle, Side, ToolFile},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_security::SaslMechanism,
    tempfile::TempDir,
    tokio::task,
};

/// The SASL listener the oracle's own tools reach from inside its container.
const ORACLE_SASL_BOOTSTRAP: &str = "localhost:9094";

/// The plaintext listener the oracle's tools provision credentials over.
const ORACLE_PLAINTEXT_BOOTSTRAP: &str = "localhost:9092";

/// Where each invocation's client configuration is placed on either side.
const CLIENT_PROPS: &str = "/tmp/client.properties";

/// The prefix `TopicCommand` prints before its authentication refusal.
const TOPIC_COMMAND_ERROR: &str = "Error while executing topic command : ";

/// The `KAFKA_*` overrides that add the SASL listener to the oracle. They
/// follow the oracle's own defaults, and Docker keeps the last value given for
/// a name.
const ORACLE_SASL_ENV: &[&str] = &[
    "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SASL://0.0.0.0:9094",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,SASL://localhost:9094",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:\
     SASL_PLAINTEXT",
    "KAFKA_LISTENER_NAME_SASL_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512,\
     OAUTHBEARER",
    "KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.\
     PlainLoginModule required user_alice=\"alice-secret\";",
    "KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___256_SASL_JAAS_CONFIG=org.apache.kafka.common.\
     security.scram.ScramLoginModule required;",
    "KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG=org.apache.kafka.common.\
     security.scram.ScramLoginModule required;",
    "KAFKA_LISTENER_NAME_SASL_OAUTHBEARER_SASL_JAAS_CONFIG=org.apache.kafka.common.security.\
     oauthbearer.OAuthBearerLoginModule required;",
];

fn credential_properties(mechanism: &str, user: &str, password: &str, claim: &str) -> String {
    let jaas = match mechanism {
        "PLAIN" => plain_jaas(user, password),
        "SCRAM-SHA-256" | "SCRAM-SHA-512" => scram_jaas(user, password),
        "OAUTHBEARER" => format!(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginPrincipalClaimName=\"{claim}\" unsecuredLoginStringClaim_{claim}=\"{user}\";"
        ),
        _ => panic!("unsupported SASL mechanism {mechanism}"),
    };
    format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism={mechanism}\nsasl.jaas.config={jaas}\n"
    )
}

/// What `kafka-topics.sh --list` came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Listed,
    /// The text `TopicCommand` printed after its error prefix. A failed run
    /// that printed no such line keeps its whole output here, so the mismatch
    /// shows it.
    Refused(String),
}

impl Outcome {
    fn of(run: &CliRun) -> Self {
        if run.succeeded() {
            return Self::Listed;
        }
        let text = run.text();
        let refusal = text
            .lines()
            .find_map(|line| line.strip_prefix(TOPIC_COMMAND_ERROR))
            .map_or_else(|| text.clone(), str::to_owned);
        Self::Refused(refusal)
    }
}

/// A stock 4.3.1 broker and krabka, both with `alice` provisioned for every
/// mechanism.
struct SaslPair {
    oracle: Oracle,
    broker: BrokerHandle,
    _dir: TempDir,
    krabka_bootstrap: String,
}

impl SaslPair {
    async fn start(label: String) -> Self {
        let oracle = task::spawn_blocking(move || Oracle::start_with_env(&label, ORACLE_SASL_ENV))
            .await
            .expect("oracle boot");
        let (broker, dir) = start_host_broker_with(|config| {
            configure_sasl(
                config,
                &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS)],
                Some(ADMIN),
            );
            config.enabled_sasl_mechanisms.extend([
                SaslMechanism::ScramSha256,
                SaslMechanism::ScramSha512,
                SaslMechanism::OAuthBearer,
            ]);
        })
        .await;
        let pair = Self {
            oracle,
            broker,
            _dir: dir,
            krabka_bootstrap: broker0_advertised().to_owned(),
        };
        pair.provision_scram();
        pair
    }

    fn sides(&self) -> [Side<'_>; 2] {
        [Side::Oracle(&self.oracle), Side::Krabka {
            bootstrap: &self.krabka_bootstrap,
        }]
    }

    fn sasl_bootstrap<'a>(side: &'a Side<'_>) -> &'a str {
        match side {
            Side::Oracle(_) => ORACLE_SASL_BOOTSTRAP,
            Side::Krabka { bootstrap } => bootstrap,
        }
    }

    // Kafka refuses two credential changes for one user in one request, so
    // each digest is its own `kafka-configs` run. The oracle provisions over
    // its plaintext listener, which has no authorizer; krabka's provisioning
    // authenticates as its PLAIN super user.
    fn provision_scram(&self) {
        let admin_props = plain_client_properties(ADMIN, ADMIN_PASS);
        for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
            let config = format!("{mechanism}=[password={ALICE_PASS}]");
            for side in self.sides() {
                let mut args = match side {
                    Side::Oracle(_) => vec!["--bootstrap-server", ORACLE_PLAINTEXT_BOOTSTRAP],
                    Side::Krabka { bootstrap } => vec![
                        "--bootstrap-server",
                        bootstrap,
                        "--command-config",
                        CLIENT_PROPS,
                    ],
                };
                args.extend([
                    "--alter",
                    "--add-config",
                    &config,
                    "--entity-type",
                    "users",
                    "--entity-name",
                    ALICE,
                ]);
                side.run_with_files(
                    "kafka-configs",
                    &args,
                    &[ToolFile::new(CLIENT_PROPS, &admin_props)],
                    None,
                )
                .expect_success();
            }
        }
    }

    fn list_topics(side: &Side<'_>, properties: &str) -> Outcome {
        let run = side.run_with_files(
            "kafka-topics",
            &[
                "--bootstrap-server",
                Self::sasl_bootstrap(side),
                "--list",
                "--command-config",
                CLIENT_PROPS,
            ],
            &[ToolFile::new(CLIENT_PROPS, properties)],
            None,
        );
        Outcome::of(&run)
    }

    async fn shutdown(self) { self.broker.shutdown().await; }
}

struct SaslLogins {
    pair: Option<SaslPair>,
    observed: Vec<Outcome>,
}
#[rstest::fixture]
fn logins() -> SaslLogins {
    // Start this scenario with fresh inputs and observations.

    SaslLogins {
        pair: None,
        observed: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "Kafka and Krabka pair {name} enable PLAIN, both SCRAM digests and OAUTHBEARER with alice password alice-secret and principal claim sub"
)]
async fn sasl_pair(logins: &mut SaslLogins, name: String) {
    logins.pair = Some(SaslPair::start(name).await);
}
#[rstest_bdd_macros::when(
    "the Kafka 4.3.1 topics client lists topics on both brokers with these credentials"
)]
fn authenticate(logins: &mut SaslLogins, datatable: Vec<Vec<String>>) {
    let pair = logins.pair.as_ref().expect("SASL pair");
    for row in datatable.into_iter().skip(1) {
        let [mechanism, user, password, claim]: [String; 4] =
            row.try_into().expect("mechanism user password claim");
        let properties = credential_properties(&mechanism, &user, &password, &claim);
        for side in pair.sides() {
            logins
                .observed
                .push(SaslPair::list_topics(&side, &properties));
        }
    }
}
#[rstest_bdd_macros::then("both brokers return these authentication outcomes in credential order")]
fn authentication_outcomes(logins: &SaslLogins, datatable: Vec<Vec<String>>) {
    let expected: Vec<Outcome> = datatable
        .into_iter()
        .skip(1)
        .flat_map(|row| {
            let [outcome, message]: [String; 2] = row.try_into().expect("outcome message");
            let expected = match outcome.as_str() {
                "listed" => Outcome::Listed,
                "refused" => Outcome::Refused(message),
                _ => panic!("listed or refused"),
            };
            [expected.clone(), expected]
        })
        .collect();
    assert!(logins.observed == expected);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/kafka_4_3_1_client.feature",
    name = "kafka 4 3 1 client authenticates with every mechanism"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_4_3_1_client_authenticates_with_every_mechanism(logins: SaslLogins) {
    logins.pair.take().expect("pair").shutdown().await;
    drop(logins);
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_sasl/kafka_4_3_1_client.feature",
    name = "kafka 4 3 1 client reports kafkas refusal for bad credentials"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_4_3_1_client_reports_kafkas_refusal_for_bad_credentials(logins: SaslLogins) {
    logins.pair.take().expect("pair").shutdown().await;
    drop(logins);
}
