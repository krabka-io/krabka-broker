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
};

/// The SASL listener the oracle's own tools reach from inside its container.
const ORACLE_SASL_BOOTSTRAP: &str = "localhost:9094";

/// The plaintext listener the oracle's tools provision credentials over.
const ORACLE_PLAINTEXT_BOOTSTRAP: &str = "localhost:9092";

/// Where each invocation's client configuration is placed on either side.
const CLIENT_PROPS: &str = "/tmp/client.properties";

/// A user neither broker knows.
const MALLORY: &str = "mallory";

/// A password that is not `alice`'s.
const WRONG_PASS: &str = "not-alice-secret";

/// The prefix `TopicCommand` puts in front of the error that stopped it.
const TOPIC_COMMAND_ERROR: &str = "Error while executing topic command : ";

/// Kafka's `PlainServerCallbackHandler` refusal.
const PLAIN_REFUSAL: &str = "Authentication failed: Invalid username or password";

/// Kafka's `ScramSaslServer` refusal for SCRAM-SHA-256.
const SCRAM_SHA_256_REFUSAL: &str = "Authentication failed during authentication due to invalid credentials with SASL mechanism \
     SCRAM-SHA-256";

/// Kafka's `ScramSaslServer` refusal for SCRAM-SHA-512.
const SCRAM_SHA_512_REFUSAL: &str = "Authentication failed during authentication due to invalid credentials with SASL mechanism \
     SCRAM-SHA-512";

/// The RFC 7628 error status `OAuthBearerSaslServer` fails a rejected token
/// with.
const OAUTHBEARER_REFUSAL: &str = r#"{"status":"invalid_token"}"#;

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

#[derive(Clone, Copy, Debug)]
enum PasswordMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl PasswordMechanism {
    fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// What the client logs in with.
#[derive(Clone, Copy, Debug)]
enum Credential {
    Password {
        mechanism: PasswordMechanism,
        user: &'static str,
        password: &'static str,
    },
    /// An unsecured (`alg:none`) JWS minted by the client's default
    /// `OAuthBearerUnsecuredLoginCallbackHandler`, carrying `principal` in
    /// `claim`.
    UnsecuredJws {
        claim: &'static str,
        principal: &'static str,
    },
}

impl Credential {
    fn client_properties(self) -> String {
        match self {
            Self::Password {
                mechanism,
                user,
                password,
            } => {
                let jaas = match mechanism {
                    PasswordMechanism::Plain => plain_jaas(user, password),
                    PasswordMechanism::ScramSha256 | PasswordMechanism::ScramSha512 => {
                        scram_jaas(user, password)
                    }
                };
                format!(
                    "security.protocol=SASL_PLAINTEXT\nsasl.mechanism={}\nsasl.jaas.config={jaas}\n",
                    mechanism.name(),
                )
            }
            Self::UnsecuredJws { claim, principal } => format!(
                "security.protocol=SASL_PLAINTEXT\n\
                 sasl.mechanism=OAUTHBEARER\n\
                 sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.\
                 OAuthBearerLoginModule required unsecuredLoginPrincipalClaimName=\"{claim}\" \
                 unsecuredLoginStringClaim_{claim}=\"{principal}\";\n",
            ),
        }
    }
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

    fn refused(text: &str) -> Self { Self::Refused(text.to_owned()) }
}

struct Case {
    label: &'static str,
    credential: Credential,
    expected: Outcome,
}

fn password(mechanism: PasswordMechanism, user: &'static str, pass: &'static str) -> Credential {
    Credential::Password {
        mechanism,
        user,
        password: pass,
    }
}

fn positive_cases() -> Vec<Case> {
    vec![
        Case {
            label: "PLAIN, alice",
            credential: password(PasswordMechanism::Plain, ALICE, ALICE_PASS),
            expected: Outcome::Listed,
        },
        Case {
            label: "SCRAM-SHA-256, alice",
            credential: password(PasswordMechanism::ScramSha256, ALICE, ALICE_PASS),
            expected: Outcome::Listed,
        },
        Case {
            label: "SCRAM-SHA-512, alice",
            credential: password(PasswordMechanism::ScramSha512, ALICE, ALICE_PASS),
            expected: Outcome::Listed,
        },
        Case {
            label: "OAUTHBEARER, sub=alice",
            credential: Credential::UnsecuredJws {
                claim: "sub",
                principal: ALICE,
            },
            expected: Outcome::Listed,
        },
    ]
}

fn negative_cases() -> Vec<Case> {
    vec![
        Case {
            label: "PLAIN, wrong password",
            credential: password(PasswordMechanism::Plain, ALICE, WRONG_PASS),
            expected: Outcome::refused(PLAIN_REFUSAL),
        },
        Case {
            label: "PLAIN, unknown user",
            credential: password(PasswordMechanism::Plain, MALLORY, ALICE_PASS),
            expected: Outcome::refused(PLAIN_REFUSAL),
        },
        Case {
            label: "SCRAM-SHA-256, wrong password",
            credential: password(PasswordMechanism::ScramSha256, ALICE, WRONG_PASS),
            expected: Outcome::refused(SCRAM_SHA_256_REFUSAL),
        },
        Case {
            label: "SCRAM-SHA-256, unknown user",
            credential: password(PasswordMechanism::ScramSha256, MALLORY, ALICE_PASS),
            expected: Outcome::refused(SCRAM_SHA_256_REFUSAL),
        },
        Case {
            label: "SCRAM-SHA-512, wrong password",
            credential: password(PasswordMechanism::ScramSha512, ALICE, WRONG_PASS),
            expected: Outcome::refused(SCRAM_SHA_512_REFUSAL),
        },
        Case {
            label: "SCRAM-SHA-512, unknown user",
            credential: password(PasswordMechanism::ScramSha512, MALLORY, ALICE_PASS),
            expected: Outcome::refused(SCRAM_SHA_512_REFUSAL),
        },
        Case {
            label: "OAUTHBEARER, principal in upn and no sub",
            credential: Credential::UnsecuredJws {
                claim: "upn",
                principal: ALICE,
            },
            expected: Outcome::refused(OAUTHBEARER_REFUSAL),
        },
    ]
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
    async fn start(label: &'static str) -> Self {
        let oracle =
            tokio::task::spawn_blocking(move || Oracle::start_with_env(label, ORACLE_SASL_ENV))
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

    fn list_topics(side: &Side<'_>, credential: Credential) -> Outcome {
        let run = side.run_with_files(
            "kafka-topics",
            &[
                "--bootstrap-server",
                Self::sasl_bootstrap(side),
                "--list",
                "--command-config",
                CLIENT_PROPS,
            ],
            &[ToolFile::new(CLIENT_PROPS, &credential.client_properties())],
            None,
        );
        Outcome::of(&run)
    }

    // The oracle runs first in every case, so a row Kafka disagrees with is
    // reported against the oracle before krabka is judged by it. Every case
    // runs before the comparison, so one failure does not hide the rest.
    fn check_cases(&self, cases: &[Case]) {
        let mut observed = Vec::new();
        let mut expected = Vec::new();
        for case in cases {
            for side in self.sides() {
                let outcome = Self::list_topics(&side, case.credential);
                eprintln!(
                    "KRABKA[test] case={:?} side={} outcome={outcome:?}",
                    case.label,
                    side.label(),
                );
                observed.push((case.label, side.label(), outcome));
                expected.push((case.label, side.label(), case.expected.clone()));
            }
        }
        assert!(observed == expected);
    }

    async fn shutdown(self) { self.broker.shutdown().await; }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_4_3_1_client_authenticates_with_every_mechanism() {
    let pair = SaslPair::start("sasl-accept").await;
    pair.check_cases(&positive_cases());
    pair.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_4_3_1_client_reports_kafkas_refusal_for_bad_credentials() {
    let pair = SaslPair::start("sasl-refuse").await;
    pair.check_cases(&negative_cases());
    pair.shutdown().await;
}
