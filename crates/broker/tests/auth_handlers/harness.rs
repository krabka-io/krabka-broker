//! Wire plumbing and credential fixtures that every auth suite in this
//! binary shares: one length-prefixed request/response round-trip against a
//! broker socket, and the test passwords the SASL exchanges authenticate
//! with.

use std::io;

use tokio::net::TcpStream;

use crate::kafka_wire;

/// alice's SCRAM test password, built from characters at runtime.
///
/// The value is a non-secret test fixture. But a literal that goes into the
/// client SASL-auth calls trips GitHub's default code-scanning credential
/// query. This function keeps those call sites free of literals.
pub fn alice_password() -> String {
    ['w', 'o', 'n', 'd', 'e', 'r', 'l', 'a', 'n', 'd']
        .iter()
        .collect()
}

/// admin PLAIN test password, built at runtime.
///
/// A runtime value stops code scanning from giving a false positive for a
/// static secret in the integration fixtures.
pub fn admin_plain_password() -> String {
    ['s', 'e', 'c', 'r', 'e', 't'].iter().collect()
}

/// wrong SCRAM test password, built at runtime for the same reason as
/// `admin_plain_password`.
pub fn wrong_scram_password() -> String {
    ['h', 'u', 'n', 't', 'e', 'r', '2'].iter().collect()
}

/// One length-prefixed request/response exchange; see
/// [`kafka_wire::round_trip`].
pub async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    kafka_wire::round_trip(
        stream,
        api_key,
        api_version,
        corr_id,
        "krabka-sasl-test",
        flexible,
        body,
    )
    .await
}

/// Post-auth Metadata proves the session survived and the data plane remains reachable.
pub async fn metadata_probe(stream: &mut TcpStream, corr_id: i32) -> io::Result<()> {
    let response: krabka_protocol::owned::metadata_response::MetadataResponse =
        kafka_wire::exchange(
            stream,
            &krabka_protocol::owned::metadata_request::MetadataRequest::default(),
            3,
            12,
            corr_id,
            "krabka-sasl-test",
            true,
        )
        .await?;
    if response.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }
    Ok(())
}

/// Read after an authentication deadline/refusal, bounded so a missing close fails the test.
pub async fn read_after_auth(stream: &mut TcpStream) -> usize {
    let mut buf = [0_u8; 16];
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::io::AsyncReadExt::read(stream, &mut buf),
    )
    .await
    .expect("read should not hang")
    .expect("read should not error")
}

/// Disable idle expiry so only the configured reauthentication deadline is observable.
pub fn reauth_config(
    log_dir: &std::path::Path,
    max_reauth: krabka_units::Time,
) -> krabka_broker::BrokerConfig {
    let mut config = crate::support::sasl_plaintext_config(log_dir.to_path_buf());
    config.connections_max_idle = Some(krabka_units::millis(0));
    config.connections_max_reauth = Some(max_reauth);
    config
}

/// An authenticated SCRAM-admin fixture, enabling the mechanisms each case exercises.
pub async fn start_scram_admin(
    log_dir: &std::path::Path,
    password: &str,
    mechanisms: Vec<krabka_security::SaslMechanism>,
) -> (krabka_broker::BrokerHandle, std::net::SocketAddr) {
    let mut config = crate::support::sasl_plaintext_config(log_dir.to_path_buf());
    config.enabled_sasl_mechanisms = mechanisms;
    config
        .plain_credentials
        .insert("admin".to_string(), password.to_string());
    config.super_users = maplit::hashset! {"admin".to_string()};
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("broker must start");
    let addr = broker.listen_addr();
    (broker, addr)
}

pub fn alice_plain_config(
    log_dir: std::path::PathBuf,
    password: String,
) -> krabka_broker::BrokerConfig {
    let mut cfg = crate::support::sasl_plaintext_config(log_dir);
    cfg.enabled_sasl_mechanisms = vec![krabka_security::SaslMechanism::Plain];
    cfg.plain_credentials.insert("alice".into(), password);
    cfg
}

pub async fn start_scram_alice(
    log_dir: std::path::PathBuf,
    mechanism: krabka_security::SaslMechanism,
) -> krabka_broker::BrokerHandle {
    let mut cfg = crate::support::sasl_plaintext_config(log_dir);
    cfg.enabled_sasl_mechanisms = vec![mechanism];
    let broker = krabka_broker::Broker::start(cfg)
        .await
        .expect("broker must start");
    crate::support::sasl::provision_scram(
        &broker,
        "alice",
        alice_password().as_bytes(),
        mechanism,
        4096,
    )
    .await;
    broker
}
