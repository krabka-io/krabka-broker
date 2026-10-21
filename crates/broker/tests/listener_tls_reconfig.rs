//! Dynamic listener SSL reconfiguration on a running broker.
//!
//! Each case starts one broker with a PLAINTEXT listener, which carries the
//! alters, and an SSL listener that serves a static certificate. A per-broker
//! `IncrementalAlterConfigs` of `listener.name.ssl.ssl.keystore.*` then
//! changes what the SSL listener serves, or is refused, as Kafka 4.3.1's
//! `SslFactory` reconfiguration does.

mod kafka_wire;
mod support;

use {
    crate::support::{
        configs::{BROKER_RESOURCE, ConfigMutation, ConfigOperation, broker_alter_request},
        listeners::listener,
        tls::{
            CertificateDer, ClientConfig, PemObject as _, PinnedCertVerifier, ServerName,
            TcpStream, TlsConnector, fixture_signature_schemes,
        },
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_protocol::owned::incremental_alter_configs_response::AlterConfigsResourceResponse,
    krabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig},
    rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair},
    std::{
        net::SocketAddr,
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-listener-tls-test";

/// The keystore location key of the SSL listener.
const KEYSTORE_LOCATION: &str = "listener.name.ssl.ssl.keystore.location";

/// How long a committed change may take to reach the listener.
const PROPAGATION: Duration = Duration::from_secs(10);

/// A certificate and its PKCS#8 key, both PEM.
struct Identity {
    cert: String,
    key: String,
}

impl Identity {
    /// A self-signed certificate for `common_name` with `sans`.
    fn new(common_name: &str, sans: &[&str]) -> Self {
        let key = KeyPair::generate().expect("key pair");
        let mut params =
            CertificateParams::new(sans.iter().map(|san| (*san).to_owned()).collect::<Vec<_>>())
                .expect("params");
        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, common_name);
        params.distinguished_name = name;
        let cert = params.self_signed(&key).expect("self-signed certificate");
        Self {
            cert: cert.pem(),
            key: key.serialize_pem(),
        }
    }

    fn der(&self) -> CertificateDer<'static> {
        CertificateDer::from_pem_slice(self.cert.as_bytes()).expect("certificate")
    }

    /// Write this identity as one PEM keystore file, key first.
    fn write_keystore(&self, path: &Path) {
        std::fs::write(path, format!("{}{}", self.key, self.cert)).expect("write keystore");
    }
}

/// A broker with a PLAINTEXT admin listener and an SSL listener serving
/// `served`.
struct TlsBroker {
    handle: BrokerHandle,
    dir: TempDir,
    admin: SocketAddr,
    ssl: SocketAddr,
    served: Identity,
}

impl TlsBroker {
    async fn start() -> Self {
        drop(rustls::crypto::ring::default_provider().install_default());
        let dir = tempfile::tempdir().expect("tempdir");
        let served = Identity::new("localhost", &["localhost"]);
        let cert_chain_path = dir.path().join("static-cert.pem");
        let private_key_path = dir.path().join("static-key.pem");
        std::fs::write(&cert_chain_path, &served.cert).unwrap();
        std::fs::write(&private_key_path, &served.key).unwrap();

        let plaintext = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ssl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (admin, ssl_addr) = (plaintext.local_addr().unwrap(), ssl.local_addr().unwrap());
        let mut config = BrokerConfig::for_tests(dir.path().join("log"));
        config.listeners = vec![
            listener("PLAINTEXT", admin, ListenerProtocol::Plaintext),
            listener("SSL", ssl_addr, ListenerProtocol::Ssl),
        ];
        "PLAINTEXT".clone_into(&mut config.inter_broker_listener_name);
        config.tls_config = Some(TlsConfig {
            cert_chain_path,
            private_key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        });
        // The static file watcher plays no part in these cases.
        config.tls_reload_interval = <krabka_units::Time as krabka_units::convert::TimeExt>::ZERO;
        let handle = Broker::start_with_listeners(config, None, [plaintext, ssl])
            .await
            .expect("broker start");
        Self {
            handle,
            dir,
            admin,
            ssl: ssl_addr,
            served,
        }
    }

    fn resource(&self) -> String { self.handle.node_id().to_string() }

    /// A keystore file in the broker's directory.
    fn keystore(&self, name: &str, identity: &Identity) -> String {
        let path = self.dir.path().join(name);
        identity.write_keystore(&path);
        path.to_string_lossy().into_owned()
    }

    /// Send one `IncrementalAlterConfigs` of this broker's resource.
    async fn alter(
        &self,
        configs: Vec<(&str, ConfigOperation, Option<&str>)>,
        mutation: ConfigMutation,
    ) -> AlterConfigsResourceResponse {
        let request = broker_alter_request(self.resource(), configs, mutation);
        kafka_wire::alter_one_resource(self.admin, &request, CLIENT_ID).await
    }

    /// Whether the SSL listener completes a handshake serving `identity`.
    async fn serves(&self, identity: &Identity) -> bool {
        let config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
                pinned: identity.der(),
                schemes: fixture_signature_schemes(),
                mismatch: "the listener serves another certificate",
            }))
            .with_no_client_auth();
        let Ok(tcp) = TcpStream::connect(self.ssl).await else {
            return false;
        };
        TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .is_ok()
    }

    /// Poll until the SSL listener serves `identity`.
    async fn eventually_serves(&self, identity: &Identity) -> bool {
        let deadline = Instant::now() + PROPAGATION;
        loop {
            if self.serves(identity).await {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            // The alter is committed before its response, but the listener
            // reads the image the broker has applied, which has no signal a
            // test can wait on.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The response row of an alter of `resource` that succeeded.
fn accepted(resource: String) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: krabka_broker::codes::NONE,
        error_message: None,
        resource_type: BROKER_RESOURCE,
        resource_name: resource,
        ..Default::default()
    }
}

/// A renewed keystore with the same DN and SANs is served to the next
/// handshake; the same path with new contents is read again on the next
/// alter of it; deleting the key returns the listener to its static
/// certificate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renewed_keystore_is_served_without_restart() {
    let broker = TlsBroker::start().await;
    assert!(broker.serves(&broker.served).await);

    let renewed = Identity::new("localhost", &["localhost"]);
    let location = broker.keystore("dynamic.pem", &renewed);
    let set = broker
        .alter(
            vec![(KEYSTORE_LOCATION, ConfigOperation::Set, Some(&location))],
            ConfigMutation::Apply,
        )
        .await;
    assert!(set == accepted(broker.resource()));
    assert!(broker.eventually_serves(&renewed).await);
    assert!(!broker.serves(&broker.served).await);

    // Kafka reloads a keystore whose path did not change when the key is
    // altered again; the new contents are served.
    let rotated = Identity::new("localhost", &["localhost"]);
    let later = std::time::SystemTime::now() + Duration::from_secs(5);
    rotated.write_keystore(Path::new(&location));
    std::fs::File::options()
        .write(true)
        .open(&location)
        .unwrap()
        .set_modified(later)
        .unwrap();
    let again = broker
        .alter(
            vec![(KEYSTORE_LOCATION, ConfigOperation::Set, Some(&location))],
            ConfigMutation::Apply,
        )
        .await;
    assert!(again == accepted(broker.resource()));
    assert!(broker.eventually_serves(&rotated).await);

    let deleted = broker
        .alter(
            vec![(KEYSTORE_LOCATION, ConfigOperation::Delete, None)],
            ConfigMutation::Apply,
        )
        .await;
    assert!(deleted == accepted(broker.resource()));
    assert!(broker.eventually_serves(&broker.served).await);
}

/// The refusal of a listener SSL change whose `SslFactory` failure is
/// `cause`, as Kafka's `ConfigAdminManager` answers it.
fn refusal(resource: String, cause: &str) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: krabka_broker::codes::INVALID_REQUEST,
        error_message: Some(format!(
            "Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic \
             config update of SSLFactory failed: {cause} for configuration Invalid dynamic \
             configuration"
        )),
        ..accepted(resource)
    }
}

/// A keystore that changes the DN, drops a SAN, is not PEM or does not load
/// is refused with `INVALID_REQUEST`, `validate_only` or not, and the
/// listener keeps serving its certificate. A valid one under `validate_only`
/// is accepted and not applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incompatible_keystore_is_refused_and_validate_only_applies_nothing() {
    let broker = TlsBroker::start().await;
    let elsewhere = broker.keystore("elsewhere.pem", &Identity::new("elsewhere", &["localhost"]));
    let narrower = broker.keystore(
        "narrower.pem",
        &Identity::new("localhost", &["other.example"]),
    );
    let missing = broker
        .dir
        .path()
        .join("missing.pem")
        .to_string_lossy()
        .into_owned();
    let config_exception = "org.apache.kafka.common.config.ConfigException";
    let invalid_configuration = "org.apache.kafka.common.errors.InvalidConfigurationException";
    let cases = [
        (
            vec![(KEYSTORE_LOCATION, elsewhere.as_str())],
            format!(
                "{config_exception}: Keystore DistinguishedName does not match:  \
                 existing={{alias=kafka, DN=CN=localhost}}, new={{alias=kafka, DN=CN=elsewhere}}"
            ),
        ),
        (
            vec![(KEYSTORE_LOCATION, narrower.as_str())],
            format!(
                "{config_exception}: Keystore SubjectAltNames do not match:  \
                 existing={{alias=kafka, SAN=[[2, localhost]]}}, new={{alias=kafka, SAN=[[2, \
                 other.example]]}}"
            ),
        ),
        (
            vec![
                ("listener.name.ssl.ssl.keystore.type", "JKS"),
                (KEYSTORE_LOCATION, elsewhere.as_str()),
            ],
            format!(
                "{invalid_configuration}: krabka loads PEM key stores only, but the key store \
                 type is JKS."
            ),
        ),
        (
            vec![(KEYSTORE_LOCATION, missing.as_str())],
            format!(
                "{invalid_configuration}: Failed to load PEM SSL keystore {missing}: No such file \
                 or directory (os error 2)"
            ),
        ),
    ];
    for mutation in [ConfigMutation::Validate, ConfigMutation::Apply] {
        for (keys, cause) in &cases {
            let row = broker
                .alter(
                    keys.iter()
                        .map(|(key, value)| (*key, ConfigOperation::Set, Some(*value)))
                        .collect(),
                    mutation,
                )
                .await;
            assert!(row == refusal(broker.resource(), cause), "{keys:?}");
        }
    }

    let renewed = Identity::new("localhost", &["localhost"]);
    let location = broker.keystore("renewed.pem", &renewed);
    let validated = broker
        .alter(
            vec![(KEYSTORE_LOCATION, ConfigOperation::Set, Some(&location))],
            ConfigMutation::Validate,
        )
        .await;
    assert!(validated == accepted(broker.resource()));
    assert!(broker.serves(&broker.served).await);
    assert!(!broker.serves(&renewed).await);
}
