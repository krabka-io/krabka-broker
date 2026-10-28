//! Dynamic listener SSL reconfiguration on a running broker.
//!
//! Each case starts one broker with a PLAINTEXT listener, which carries the
//! alters, and an SSL listener that serves a static certificate. A per-broker
//! `IncrementalAlterConfigs` of `listener.name.ssl.ssl.keystore.*` then
//! changes what the SSL listener serves, or is refused, as Kafka 4.3.1's
//! `SslFactory` reconfiguration does.
#![cfg(test)]

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
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, codes},
    krabka_protocol::owned::incremental_alter_configs_response::AlterConfigsResourceResponse,
    krabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig},
    krabka_units::{Time, convert::TimeExt},
    rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair},
    rustls::crypto::aws_lc_rs,
    std::{
        fs::{self, File},
        net::SocketAddr,
        path::Path,
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    },
    tempfile::TempDir,
    tokio::{net::TcpListener, time},
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
        fs::write(path, format!("{}{}", self.key, self.cert)).expect("write keystore");
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
        drop(aws_lc_rs::default_provider().install_default());
        let dir = tempfile::tempdir().expect("tempdir");
        let served = Identity::new("localhost", &["localhost"]);
        let cert_chain_path = dir.path().join("static-cert.pem");
        let private_key_path = dir.path().join("static-key.pem");
        fs::write(&cert_chain_path, &served.cert).unwrap();
        fs::write(&private_key_path, &served.key).unwrap();

        let plaintext = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ssl = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        config.tls_reload_interval = <Time as TimeExt>::ZERO;
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
            time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The response row of an alter of `resource` that succeeded.
fn accepted(resource: String) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: codes::NONE,
        error_message: None,
        resource_type: BROKER_RESOURCE,
        resource_name: resource,
        ..Default::default()
    }
}

/// The refusal of a listener SSL change whose `SslFactory` failure is
/// `cause`, as Kafka's `ConfigAdminManager` answers it.
fn refusal(resource: String, cause: &str) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: codes::INVALID_REQUEST,
        error_message: Some(format!(
            "Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic \
             config update of SSLFactory failed: {cause} for configuration Invalid dynamic \
             configuration"
        )),
        ..accepted(resource)
    }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::collections::BTreeMap,
};
struct KeystoreUpdate {
    broker: Option<TlsBroker>,
    identities: BTreeMap<String, Identity>,
    paths: BTreeMap<String, String>,
    response: Option<AlterConfigsResourceResponse>,
    refusals: Vec<AlterConfigsResourceResponse>,
}
#[fixture]
fn keystores() -> KeystoreUpdate {
    // Start this scenario with fresh inputs and observations.

    KeystoreUpdate {
        broker: None,
        identities: BTreeMap::new(),
        paths: BTreeMap::new(),
        response: None,
        refusals: Vec::new(),
    }
}
#[given(
    "a running SSL listener serves CN localhost with SAN localhost from its static certificate"
)]
async fn static_identity(keystores: &mut KeystoreUpdate) {
    let broker = TlsBroker::start().await;
    assert!(broker.serves(&broker.served).await);
    keystores.broker = Some(broker);
}
#[given("candidate PEM keystores have these certificate identities")]
fn candidates(keystores: &mut KeystoreUpdate, datatable: Vec<Vec<String>>) {
    let broker = keystores
        .broker
        .as_ref()
        .expect("static TLS broker running");
    for row in datatable.into_iter().skip(1) {
        let [name, common_name, san]: [String; 3] = row.try_into().expect("file CN and SAN");
        let identity = Identity::new(&common_name, &[&san]);
        let path = broker.keystore(&name, &identity);
        keystores.paths.insert(name.clone(), path);
        keystores.identities.insert(name, identity);
    }
}
#[given("candidate keystore file {name} does not exist")]
fn absent(keystores: &mut KeystoreUpdate, name: String) {
    let path = keystores
        .broker
        .as_ref()
        .expect("static TLS broker running")
        .dir
        .path()
        .join(&name)
        .to_string_lossy()
        .into_owned();
    keystores.paths.insert(name, path);
}
impl KeystoreUpdate {
    async fn change_listener_keystore(&mut self, name: &str, mutation: ConfigMutation) {
        let broker = self.broker.as_ref().expect("static TLS broker running");
        let path = self.paths.get(name).expect("candidate path configured");
        self.response = Some(
            broker
                .alter(
                    vec![(KEYSTORE_LOCATION, ConfigOperation::Set, Some(path))],
                    mutation,
                )
                .await,
        );
    }
}
#[when("the operator applies PEM keystore {name} to the live SSL listener")]
async fn apply(keystores: &mut KeystoreUpdate, name: String) {
    keystores
        .change_listener_keystore(&name, ConfigMutation::Apply)
        .await;
}
#[when("the operator validates PEM keystore {name} without applying it")]
async fn validate(keystores: &mut KeystoreUpdate, name: String) {
    keystores
        .change_listener_keystore(&name, ConfigMutation::Validate)
        .await;
}
#[then("the complete dynamic configuration response accepts the broker resource")]
fn accepted_update(keystores: &KeystoreUpdate) {
    let broker = keystores
        .broker
        .as_ref()
        .expect("static TLS broker running");
    assert!(
        *keystores
            .response
            .as_ref()
            .expect("configuration update completed")
            == accepted(broker.resource())
    );
}
#[then("new handshakes serve keystore {name}")]
async fn served(keystores: &KeystoreUpdate, name: String) {
    assert!(
        keystores
            .broker
            .as_ref()
            .expect("TLS broker running")
            .eventually_serves(
                keystores
                    .identities
                    .get(&name)
                    .expect("candidate identity configured")
            )
            .await
    );
}
#[then("new handshakes no longer accept the original static certificate")]
async fn static_replaced(keystores: &KeystoreUpdate) {
    let broker = keystores.broker.as_ref().expect("TLS broker running");
    assert!(!broker.serves(&broker.served).await);
}
#[when(
    "PEM keystore {name} is rewritten with a fresh CN localhost SAN localhost identity and a modification time five seconds later"
)]
fn rewrite(keystores: &mut KeystoreUpdate, name: String) {
    let identity = Identity::new("localhost", &["localhost"]);
    let path = keystores.paths.get(&name).expect("existing keystore path");
    identity.write_keystore(Path::new(path));
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(SystemTime::now() + Duration::from_secs(5))
        .unwrap();
    keystores.identities.insert(name, identity);
}
#[when("the operator deletes the dynamic SSL keystore location")]
async fn delete(keystores: &mut KeystoreUpdate) {
    let broker = keystores.broker.as_ref().expect("TLS broker running");
    keystores.response = Some(
        broker
            .alter(
                vec![(KEYSTORE_LOCATION, ConfigOperation::Delete, None)],
                ConfigMutation::Apply,
            )
            .await,
    );
}
#[then("new handshakes again serve the original static certificate")]
async fn restored(keystores: &KeystoreUpdate) {
    let broker = keystores.broker.as_ref().expect("TLS broker running");
    assert!(broker.eventually_serves(&broker.served).await);
}
#[when("the operator validates every update then applies every update in this order")]
async fn invalid_updates(keystores: &mut KeystoreUpdate, datatable: Vec<Vec<String>>) {
    let updates: Vec<[String; 2]> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.try_into().expect("keystore file and optional type"))
        .collect();
    let broker = keystores.broker.as_ref().expect("TLS broker running");
    for mutation in [ConfigMutation::Validate, ConfigMutation::Apply] {
        for [name, store_type] in &updates {
            let path = keystores
                .paths
                .get(name)
                .expect("candidate path configured");
            let mut settings = Vec::new();
            if store_type != "inherit" {
                settings.push((
                    "listener.name.ssl.ssl.keystore.type",
                    ConfigOperation::Set,
                    Some(store_type.as_str()),
                ));
            }
            settings.push((KEYSTORE_LOCATION, ConfigOperation::Set, Some(path.as_str())));
            keystores
                .refusals
                .push(broker.alter(settings, mutation).await);
        }
    }
}
#[then("both validation and application return these complete invalid request causes in order")]
fn refused(keystores: &KeystoreUpdate, datatable: Vec<Vec<String>>) {
    let causes: Vec<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let mut cause = row.into_iter().next().expect("expected refusal cause");
            for (name, path) in &keystores.paths {
                cause = cause.replace(&format!("{{{name}}}"), path);
            }
            cause
        })
        .collect();
    assert!(keystores.refusals.len() == causes.len() * 2);
    let broker = keystores.broker.as_ref().expect("TLS broker running");
    for (response, cause) in keystores.refusals.iter().zip(causes.iter().cycle()) {
        assert!(*response == refusal(broker.resource(), cause));
    }
}
#[then("validation keeps serving the static identity and does not serve candidate {name}")]
async fn validation_unchanged(keystores: &KeystoreUpdate, name: String) {
    let broker = keystores.broker.as_ref().expect("TLS broker running");
    assert!(broker.serves(&broker.served).await);
    assert!(
        !broker
            .serves(
                keystores
                    .identities
                    .get(&name)
                    .expect("candidate identity configured")
            )
            .await
    );
}
async fn shutdown(keystores: KeystoreUpdate) {
    keystores
        .broker
        .expect("TLS broker started")
        .handle
        .shutdown()
        .await;
}
#[scenario(
    path = "tests/features/migrated/tests/listener_tls_reconfig.feature",
    name = "A live listener reloads renewed rewritten and deleted dynamic PEM keystores"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renewed_keystore_is_served_without_restart(keystores: KeystoreUpdate) {
    shutdown(keystores).await;
}
#[scenario(
    path = "tests/features/migrated/tests/listener_tls_reconfig.feature",
    name = "Incompatible keystores are refused and valid validation leaves the listener unchanged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incompatible_keystore_is_refused_and_validate_only_applies_nothing(
    keystores: KeystoreUpdate,
) {
    shutdown(keystores).await;
}
