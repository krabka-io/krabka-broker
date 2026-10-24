//! The CLI command path and the broker independently encode a signed
//! break-glass approval. Exercise that boundary with two authenticated users.

use {
    crate::{cli::Cli, command::dispatch},
    assert2::assert,
    clap::Parser as _,
    krabka_broker::{
        Broker, BrokerConfig,
        config::{InterBrokerCredentials, ListenerSpec},
        operator_keys::{OperatorKeyEntry, OperatorKeys},
    },
    krabka_client_core::{Client, SaslCredentials, security::ClientSecurity},
    krabka_protocol::krabka::break_glass::DescribeBreakGlassRequest,
    krabka_security::{ListenerProtocol, SaslMechanism},
    ring::signature::{Ed25519KeyPair, KeyPair as _},
    std::{
        net::SocketAddr,
        path::{Path, PathBuf},
    },
};

struct KeyFiles {
    private: PathBuf,
    public: PathBuf,
}

fn mint_key(dir: &Path, id: &str) -> KeyFiles {
    let der = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
        .expect("generate Ed25519 key");
    let pair = Ed25519KeyPair::from_pkcs8(der.as_ref()).expect("parse generated key");
    let private = dir.join(format!("{id}.pk8"));
    std::fs::write(&private, der.as_ref()).expect("write private key");
    let public = dir.join(format!("{id}.pub"));
    std::fs::write(&public, pair.public_key().as_ref()).expect("write public key");
    KeyFiles { private, public }
}

async fn client(bootstrap: SocketAddr, username: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("guard-signed-approval-test")
        .security(ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: username.to_owned(),
                password: format!("{username}-secret"),
            }),
            sasl_host: None,
        })
        .build()
        .await
        .expect("authenticated client connects")
}

async fn cli(client: &Client, command_args: &[&str]) -> i32 {
    let mut argv = vec!["krabka-guard", "--bootstrap-server", "test-client"];
    argv.extend_from_slice(command_args);
    let parsed = Cli::try_parse_from(argv).expect("test CLI arguments parse");
    dispatch(client, parsed.command)
        .await
        .expect("command returns an exit code")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_approval_interoperates_and_rejects_self_approval_and_tampering() {
    let dir = tempfile::TempDir::new().expect("temporary broker directory");
    let alice_key = mint_key(dir.path(), "alice");
    let bob_key = mint_key(dir.path(), "bob");
    let mut config = BrokerConfig::for_tests(dir.path().join("data"));
    config.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_owned(),
        bind_addr: "127.0.0.1:0".parse().expect("socket address"),
        advertised: "127.0.0.1:0".to_owned(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: Some(vec![SaslMechanism::Plain]),
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    config.inter_broker_listener_name = "SASL_PLAINTEXT".to_owned();
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    config.plain_credentials = [
        ("alice".to_owned(), "alice-secret".to_owned()),
        ("bob".to_owned(), "bob-secret".to_owned()),
    ]
    .into_iter()
    .collect();
    config.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "alice".to_owned(),
        password: "alice-secret".to_owned(),
    });
    config.operator_keys = OperatorKeys::load(&[OperatorKeyEntry {
        key_id: "bob-yubi".to_owned(),
        principal: "User:bob".to_owned(),
        public_key_path: bob_key.public.clone(),
    }])
    .expect("load broker trust set");
    config.break_glass.approvers = vec!["User:alice".to_owned(), "User:bob".to_owned()];

    let broker = Broker::start(config).await.expect("start SASL broker");
    let bootstrap = broker.listen_addr();
    let alice = client(bootstrap, "alice").await;
    let bob = client(bootstrap, "bob").await;

    assert!(
        cli(&alice, &[
            "break-glass",
            "propose",
            "--action",
            "delete-topic",
            "--target",
            "signed-approval-test",
            "--reason",
            "verify independent signing encoders",
            "--ttl",
            "30m",
        ],)
        .await
            == 0
    );
    let described = alice
        .send(DescribeBreakGlassRequest::default())
        .await
        .expect("describe proposal");
    let proposal = uuid::Uuid::from_bytes(
        described
            .proposals
            .first()
            .expect("proposal exists")
            .proposal_id
            .0,
    )
    .to_string();

    let self_approval = [
        "break-glass",
        "approve",
        "--proposal",
        &proposal,
        "--sign-with",
        alice_key.private.to_str().expect("utf-8 key path"),
        "--key-id",
        "bob-yubi",
    ];
    assert!(cli(&alice, &self_approval).await == crate::EXIT_REFUSED);

    let tampered = [
        "break-glass",
        "approve",
        "--proposal",
        &proposal,
        "--sign-with",
        alice_key.private.to_str().expect("utf-8 key path"),
        "--key-id",
        "bob-yubi",
    ];
    assert!(cli(&bob, &tampered).await == crate::EXIT_BAD_SIGNATURE);

    let successful = [
        "break-glass",
        "approve",
        "--proposal",
        &proposal,
        "--sign-with",
        bob_key.private.to_str().expect("utf-8 key path"),
        "--key-id",
        "bob-yubi",
    ];
    assert!(cli(&bob, &successful).await == 0);
}
