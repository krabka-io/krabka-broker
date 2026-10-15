use {
    super::*,
    assert2::{assert, check},
    krabka_security::ListenerProtocol,
    rcgen::{CertificateParams, DnType, KeyPair},
};

fn install_provider() { drop(rustls::crypto::ring::default_provider().install_default()); }

/// A certificate and its PKCS#8 key, both PEM.
struct Identity {
    cert: String,
    key: String,
}

/// A self-signed certificate for `common_name` with `sans`.
fn identity(common_name: &str, sans: &[&str]) -> Identity {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    for san in sans {
        params.subject_alt_names.push(rcgen::SanType::DnsName(
            (*san).try_into().expect("a DNS name"),
        ));
    }
    let key = KeyPair::generate().expect("key pair");
    Identity {
        cert: params
            .self_signed(&key)
            .expect("self-signed certificate")
            .pem(),
        key: key.serialize_pem(),
    }
}

/// A directory holding the static certificate of an SSL listener, and the
/// listener itself.
struct Fixture {
    dir: tempfile::TempDir,
    tls: TlsConfig,
    listeners: Vec<ListenerSpec>,
}

impl Fixture {
    fn new() -> Self {
        install_provider();
        let dir = tempfile::tempdir().expect("tempdir");
        let static_identity = identity("localhost", &["localhost"]);
        let cert_chain_path = dir.path().join("static-cert.pem");
        let private_key_path = dir.path().join("static-key.pem");
        std::fs::write(&cert_chain_path, &static_identity.cert).unwrap();
        std::fs::write(&private_key_path, &static_identity.key).unwrap();
        let tls = TlsConfig {
            cert_chain_path,
            private_key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        let listener = |name: &str, protocol| ListenerSpec {
            name: name.to_owned(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            advertised: "127.0.0.1:0".to_owned(),
            protocol,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: auth::SslPrincipalMapper::default(),
        };
        Self {
            dir,
            tls,
            listeners: vec![
                listener("SSL", ListenerProtocol::Ssl),
                listener("PLAINTEXT", ListenerProtocol::Plaintext),
            ],
        }
    }

    fn inputs(&self) -> ListenerTlsInputs<'_> {
        ListenerTlsInputs {
            listeners: &self.listeners,
            tls_config: Some(&self.tls),
        }
    }

    /// Write `identity` as one PEM keystore file, key first as Kafka writes
    /// it, and return its path.
    fn keystore_file(&self, name: &str, identity: &Identity) -> String {
        let path = self.dir.path().join(name);
        std::fs::write(&path, format!("{}{}", identity.key, identity.cert)).unwrap();
        path.to_string_lossy().into_owned()
    }
}

fn props(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn the_keys_are_read_under_the_lowercased_listener_prefix() {
    let ssl = ListenerSsl::from_props(
        &props(&[
            ("listener.name.ssl.ssl.keystore.location", "/k"),
            ("listener.name.ssl.ssl.truststore.type", "PEM"),
            ("listener.name.other.ssl.keystore.password", "x"),
            ("ssl.keystore.key", "unprefixed"),
        ]),
        "SSL",
    );
    assert!(
        ssl == ListenerSsl {
            keystore_location: Some("/k".to_owned()),
            truststore_type: Some("PEM".to_owned()),
            ..ListenerSsl::default()
        }
    );
}

/// What a listener SSL check answers.
type Checked = Result<(), (i16, String)>;

/// The refusal of a change whose `SslFactory` failure is `cause`.
fn refused(cause: &str) -> Checked {
    Err((
        codes::INVALID_REQUEST,
        format!(
            "Invalid value org.apache.kafka.common.config.ConfigException: Validation of dynamic \
             config update of SSLFactory failed: {cause} for configuration Invalid dynamic \
             configuration"
        ),
    ))
}

fn invalid_configuration(message: &str) -> String {
    format!("org.apache.kafka.common.errors.InvalidConfigurationException: {message}")
}

/// Kafka's `createKeystore` and `createTruststore` refusals, and krabka's
/// two of its own: a store that is not PEM, and an encrypted key.
#[test]
fn a_store_is_checked_as_kafkas_default_ssl_engine_factory_checks_it() {
    let fixture = Fixture::new();
    let renewed = identity("localhost", &["localhost"]);
    let keystore = fixture.keystore_file("renewed.pem", &renewed);
    let cases: Vec<(Vec<(&str, &str)>, Checked)> = vec![
        (vec![("ssl.keystore.location", keystore.as_str())], Ok(())),
        (
            vec![
                ("ssl.keystore.type", "PEM"),
                ("ssl.keystore.location", keystore.as_str()),
            ],
            Ok(()),
        ),
        (
            vec![
                ("ssl.keystore.key", renewed.key.as_str()),
                ("ssl.keystore.certificate.chain", renewed.cert.as_str()),
            ],
            Ok(()),
        ),
        (
            vec![
                ("ssl.keystore.type", "JKS"),
                ("ssl.keystore.location", keystore.as_str()),
            ],
            refused(&invalid_configuration(
                "krabka loads PEM key stores only, but the key store type is JKS.",
            )),
        ),
        (
            vec![
                ("ssl.keystore.location", keystore.as_str()),
                ("ssl.keystore.password", "secret"),
            ],
            refused(&invalid_configuration(
                "SSL key store password cannot be specified with PEM format, only key password \
                 may be specified",
            )),
        ),
        (
            vec![("ssl.keystore.key", renewed.key.as_str())],
            refused(&invalid_configuration(
                "SSL private key is specified, but certificate chain is not specified.",
            )),
        ),
        (
            vec![("ssl.keystore.certificate.chain", renewed.cert.as_str())],
            refused(&invalid_configuration(
                "SSL certificate chain is specified, but private key is not specified",
            )),
        ),
        (
            vec![
                ("ssl.keystore.location", keystore.as_str()),
                ("ssl.key.password", "secret"),
            ],
            refused(&invalid_configuration(
                "krabka cannot decrypt an encrypted PEM private key; give the key unencrypted and \
                 leave ssl.key.password unset.",
            )),
        ),
        (
            vec![("ssl.truststore.certificates", renewed.cert.as_str())],
            Ok(()),
        ),
        (
            vec![
                ("ssl.truststore.certificates", renewed.cert.as_str()),
                ("ssl.truststore.password", "secret"),
            ],
            refused(&invalid_configuration(
                "SSL trust store password cannot be specified for PEM format.",
            )),
        ),
        (
            vec![
                ("ssl.truststore.type", "PKCS12"),
                ("ssl.truststore.location", keystore.as_str()),
            ],
            refused(&invalid_configuration(
                "krabka loads PEM trust stores only, but the trust store type is PKCS12.",
            )),
        ),
    ];
    for (keys, want) in cases {
        let proposed: BTreeMap<String, String> = keys
            .iter()
            .map(|(key, value)| (format!("listener.name.ssl.{key}"), (*value).to_owned()))
            .collect();
        check!(
            fixture
                .inputs()
                .check_reconfiguration(&BTreeMap::new(), &proposed)
                == want,
            "{keys:?}"
        );
    }
}

/// `CertificateEntries.ensureCompatible`: a new keystore keeps the DN and
/// every SAN, and adding a SAN is allowed.
#[test]
fn a_new_certificate_keeps_the_dn_and_the_sans() {
    let fixture = Fixture::new();
    let cases = [
        (
            "same identity",
            identity("localhost", &["localhost"]),
            Ok(()),
        ),
        (
            "an added SAN",
            identity("localhost", &["localhost", "broker-0.example"]),
            Ok(()),
        ),
        (
            "another DN",
            identity("elsewhere", &["localhost"]),
            refused(
                "org.apache.kafka.common.config.ConfigException: Keystore DistinguishedName does \
                 not match:  existing={alias=kafka, DN=CN=localhost}, new={alias=kafka, \
                 DN=CN=elsewhere}",
            ),
        ),
        (
            "a dropped SAN",
            identity("localhost", &["broker-0.example"]),
            refused(
                "org.apache.kafka.common.config.ConfigException: Keystore SubjectAltNames do not \
                 match:  existing={alias=kafka, SAN=[[2, localhost]]}, new={alias=kafka, SAN=[[2, \
                 broker-0.example]]}",
            ),
        ),
    ];
    for (case, new_identity, want) in cases {
        let location = fixture.keystore_file("new.pem", &new_identity);
        let proposed = props(&[("listener.name.ssl.ssl.keystore.location", &location)]);
        check!(
            fixture
                .inputs()
                .check_reconfiguration(&BTreeMap::new(), &proposed)
                == want,
            "{case}"
        );
    }
}

/// A listener that is not TLS has no `SslFactory`, and an unchanged listener
/// needs no check: both pass whatever the keys hold.
#[test]
fn only_a_changed_tls_listener_is_checked() {
    let fixture = Fixture::new();
    let broken = props(&[
        (
            "listener.name.plaintext.ssl.keystore.location",
            "/no/such/file",
        ),
        ("listener.name.ssl.ssl.keystore.location", "/no/such/file"),
    ]);
    let current = props(&[("listener.name.ssl.ssl.keystore.location", "/no/such/file")]);
    assert!(fixture.inputs().check_reconfiguration(&current, &broken) == Ok(()));
    assert!(
        fixture
            .inputs()
            .check_reconfiguration(&BTreeMap::new(), &broken)
            == refused(&invalid_configuration(
                "Failed to load PEM SSL keystore /no/such/file: No such file or directory (os \
                 error 2)"
            ))
    );
}

/// Apply this node's per-broker `keys` for the SSL listener to a fresh
/// image.
fn image_with(keys: &[(&str, &str)]) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for (key, value) in keys {
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: format!("listener.name.ssl.{key}"),
                config_value: Some((*value).to_owned()),
            },
        ));
    }
    image
}

#[test]
fn the_cache_serves_the_dynamic_keystore_and_reloads_a_changed_file() {
    let fixture = Fixture::new();
    let cache = ListenerTlsCache::default();
    assert!(
        cache
            .server_config(&image_with(&[]), NodeId(1), "SSL", &fixture.tls)
            .is_none()
    );

    let first = identity("localhost", &["localhost"]);
    let location = fixture.keystore_file("dynamic.pem", &first);
    let image = image_with(&[("ssl.keystore.location", &location)]);
    let built = cache
        .server_config(&image, NodeId(1), "SSL", &fixture.tls)
        .expect("the dynamic keystore applies");
    let again = cache
        .server_config(&image, NodeId(1), "SSL", &fixture.tls)
        .expect("still applies");
    assert!(
        Arc::ptr_eq(&built, &again),
        "an unchanged store is not rebuilt"
    );

    // The same path with new contents is read again once its mtime moves.
    let second = identity("localhost", &["localhost"]);
    let later = SystemTime::now() + std::time::Duration::from_secs(5);
    std::fs::write(&location, format!("{}{}", second.key, second.cert)).unwrap();
    std::fs::File::options()
        .write(true)
        .open(&location)
        .unwrap()
        .set_modified(later)
        .unwrap();
    let reloaded = cache
        .server_config(&image, NodeId(1), "SSL", &fixture.tls)
        .expect("the reloaded keystore applies");
    assert!(!Arc::ptr_eq(&built, &reloaded));
    assert!(cache.leaf("SSL") == Some(pem_certificates(&second.cert).unwrap().remove(0)));

    // A file that no longer loads leaves the last good config in force.
    std::fs::write(&location, "not a keystore").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&location)
        .unwrap()
        .set_modified(later + std::time::Duration::from_secs(5))
        .unwrap();
    let kept = cache
        .server_config(&image, NodeId(1), "SSL", &fixture.tls)
        .expect("the last good keystore stays");
    assert!(Arc::ptr_eq(&reloaded, &kept));

    // Deleting the keys returns the listener to its static config.
    assert!(
        cache
            .server_config(&image_with(&[]), NodeId(1), "SSL", &fixture.tls)
            .is_none()
    );
}
