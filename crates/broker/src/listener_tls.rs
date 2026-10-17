//! Kafka's dynamic listener SSL reconfiguration: the keystore and truststore
//! of a TLS listener, set per broker with
//! `listener.name.<listener>.ssl.keystore.*` and
//! `listener.name.<listener>.ssl.truststore.*`.
//!
//! Kafka 4.3.1 checks such a change on the broker the client addressed,
//! before it forwards the alter (`ConfigAdminManager.validateBrokerConfigChange`
//! → `SslFactory.validateReconfiguration`): the new stores must load, and a
//! new keystore must keep the subject DN and the subject alternative names of
//! the certificate the listener serves. A refusal is `INVALID_REQUEST`. Once
//! the change is committed, the listener serves the new stores to every new
//! handshake (`SslFactory.reconfigure`), and an established connection keeps
//! the session it negotiated.
//!
//! krabka reads PEM stores only, which is the format of the static
//! `tls_config` a listener starts with, so a store whose type is unset is a
//! PEM store and any other type is refused. A store file is read again when
//! it changes on disk, which covers Kafka's reload of a file whose path did
//! not change (`DynamicBrokerConfig.reloadUpdatedFilesWithoutConfigChange`).
//! A store that fails to load at accept time leaves the last good one in
//! force, as Kafka keeps its old `SslEngineFactory`.

use {
    crate::{codes, config::ListenerSpec, network::auth},
    krabka_metadata::{MetadataImage, NodeId},
    krabka_security::{ClientAuthMode, TlsConfig},
    rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject},
    std::{
        borrow::Cow,
        collections::{BTreeMap, BTreeSet, HashMap},
        fmt::Write as _,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, PoisonError},
        time::SystemTime,
    },
};

/// Kafka's `DefaultSslEngineFactory.PEM_TYPE`.
const PEM: &str = "PEM";

/// The alias Kafka's `PemStore` gives the key entry of a PEM keystore.
const PEM_ALIAS: &str = "kafka";

/// The class names Kafka's error text quotes.
const INVALID_CONFIGURATION: &str = "org.apache.kafka.common.errors.InvalidConfigurationException";
const CONFIG_EXCEPTION: &str = "org.apache.kafka.common.config.ConfigException";

/// The listener SSL keys of `SslConfigs.RECONFIGURABLE_CONFIGS` one listener
/// has set, by their unprefixed name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ListenerSsl {
    keystore_type: Option<String>,
    keystore_location: Option<String>,
    keystore_password: Option<String>,
    key_password: Option<String>,
    keystore_key: Option<String>,
    keystore_certificate_chain: Option<String>,
    truststore_type: Option<String>,
    truststore_location: Option<String>,
    truststore_password: Option<String>,
    truststore_certificates: Option<String>,
}

impl ListenerSsl {
    /// The keys `props` sets for `listener`, which Kafka's
    /// `ListenerName.configPrefix` spells with the name lowercased.
    pub(crate) fn from_props(props: &BTreeMap<String, String>, listener: &str) -> Self {
        let prefix = format!("listener.name.{}.", listener.to_ascii_lowercase());
        let get = |key: &str| props.get(&format!("{prefix}{key}")).cloned();
        Self {
            keystore_type: get("ssl.keystore.type"),
            keystore_location: get("ssl.keystore.location"),
            keystore_password: get("ssl.keystore.password"),
            key_password: get("ssl.key.password"),
            keystore_key: get("ssl.keystore.key"),
            keystore_certificate_chain: get("ssl.keystore.certificate.chain"),
            truststore_type: get("ssl.truststore.type"),
            truststore_location: get("ssl.truststore.location"),
            truststore_password: get("ssl.truststore.password"),
            truststore_certificates: get("ssl.truststore.certificates"),
        }
    }

    /// The keys this node's own dynamic config sets for `listener`. The keys
    /// are per broker only, so the cluster default holds none.
    fn from_image(image: &MetadataImage, node_id: NodeId, listener: &str) -> Self {
        image
            .broker_config(node_id)
            .map(|props| Self::from_props(props, listener))
            .unwrap_or_default()
    }

    fn is_empty(&self) -> bool { *self == Self::default() }
}

/// A Kafka `InvalidConfigurationException`, as its `toString` reads.
fn invalid(message: impl std::fmt::Display) -> String {
    format!("{INVALID_CONFIGURATION}: {message}")
}

/// The PEM type check Kafka makes of a store that is given as text.
fn require_pem(kind: &str, store_type: Option<&str>) -> Result<(), String> {
    match store_type {
        Some(store_type) if !store_type.eq_ignore_ascii_case(PEM) => Err(invalid(format!(
            "krabka loads PEM {kind} stores only, but the {kind} store type is {store_type}."
        ))),
        _ => Ok(()),
    }
}

/// The certificates of a PEM text: Kafka's `PemStore.certs`.
fn pem_certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(format!("Invalid PEM certificate: {error}")))?;
    if certs.is_empty() {
        return Err(invalid("At least one certificate expected, but none found"));
    }
    Ok(certs)
}

/// The private key of a PEM text: Kafka's `PemStore.privateKey`, which reads
/// one PKCS#8 key. krabka cannot decrypt an encrypted key.
fn pem_private_key(
    pem: &str,
    key_password: Option<&str>,
) -> Result<PrivateKeyDer<'static>, String> {
    if key_password.is_some() || pem.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----") {
        return Err(invalid(
            "krabka cannot decrypt an encrypted PEM private key; give the key unencrypted and \
             leave ssl.key.password unset.",
        ));
    }
    let keys = PrivatePkcs8KeyDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(format!("Private key could not be loaded: {error}")))?;
    match <[_; 1]>::try_from(keys) {
        Ok([key]) => Ok(PrivateKeyDer::Pkcs8(key)),
        Err(keys) if keys.is_empty() => Err(invalid("Private key not provided")),
        Err(keys) => Err(invalid(format!(
            "Expected one private key, but found {}",
            keys.len()
        ))),
    }
}

/// Read a store file, with Kafka's `FileBasedPemStore` failure text.
fn read_store(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path)
        .map_err(|error| invalid(format!("Failed to load PEM SSL keystore {path}: {error}")))
}

/// What a TLS listener serves: its certificate chain and key, and the roots
/// it verifies client certificates against.
#[derive(Debug)]
struct Material {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    client_roots: Option<Vec<CertificateDer<'static>>>,
}

/// The files a listener's stores are read from, whose changes reload them.
fn store_files(static_tls: &TlsConfig, ssl: &ListenerSsl) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if ssl.keystore_key.is_none() {
        match &ssl.keystore_location {
            Some(location) => files.push(PathBuf::from(location)),
            None => files.extend([
                static_tls.cert_chain_path.clone(),
                static_tls.private_key_path.clone(),
            ]),
        }
    }
    if ssl.truststore_certificates.is_none() {
        match &ssl.truststore_location {
            Some(location) => files.push(PathBuf::from(location)),
            None => files.extend(static_tls.client_ca_path.clone()),
        }
    }
    files
}

fn modified(path: &Path) -> Option<SystemTime> { std::fs::metadata(path).ok()?.modified().ok() }

impl Material {
    /// The keystore: the dynamic one, checked as Kafka's
    /// `DefaultSslEngineFactory.createKeystore` checks it, else the static
    /// one.
    fn keystore(
        static_tls: &TlsConfig,
        ssl: &ListenerSsl,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
        let store_type = ssl.keystore_type.as_deref();
        let key_password = ssl.key_password.as_deref();
        if let Some(key) = &ssl.keystore_key {
            if store_type.is_some_and(|t| !t.eq_ignore_ascii_case(PEM)) {
                return Err(invalid(format!(
                    "SSL private key can be specified only for PEM, but key store type is {}.",
                    store_type.unwrap_or_default()
                )));
            }
            let Some(chain) = &ssl.keystore_certificate_chain else {
                return Err(invalid(
                    "SSL private key is specified, but certificate chain is not specified.",
                ));
            };
            if ssl.keystore_location.is_some() {
                return Err(invalid(
                    "Both SSL key store location and separate private key are specified.",
                ));
            }
            if ssl.keystore_password.is_some() {
                return Err(invalid(
                    "SSL key store password cannot be specified with PEM format, only key \
                     password may be specified.",
                ));
            }
            return Ok((
                pem_certificates(chain)?,
                pem_private_key(key, key_password)?,
            ));
        }
        if ssl.keystore_certificate_chain.is_some() {
            return Err(invalid(
                "SSL certificate chain is specified, but private key is not specified",
            ));
        }
        if let Some(location) = &ssl.keystore_location {
            require_pem("key", store_type)?;
            if ssl.keystore_password.is_some() {
                return Err(invalid(
                    "SSL key store password cannot be specified with PEM format, only key \
                     password may be specified",
                ));
            }
            let pem = read_store(location)?;
            return Ok((
                pem_certificates(&pem)?,
                pem_private_key(&pem, key_password)?,
            ));
        }
        if ssl.keystore_password.is_some() {
            return Err(invalid(
                "SSL key store is not specified, but key store password is specified.",
            ));
        }
        let certs = pem_certificates(&read_store(&static_tls.cert_chain_path.to_string_lossy())?)?;
        let key = std::fs::read_to_string(&static_tls.private_key_path)
            .map_err(invalid)
            .and_then(|pem| PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(invalid))?;
        Ok((certs, key))
    }

    /// The truststore: the dynamic one, checked as Kafka's
    /// `DefaultSslEngineFactory.createTruststore` checks it, else the static
    /// client CA.
    fn truststore(
        static_tls: &TlsConfig,
        ssl: &ListenerSsl,
    ) -> Result<Option<Vec<CertificateDer<'static>>>, String> {
        let store_type = ssl.truststore_type.as_deref();
        if let Some(certificates) = &ssl.truststore_certificates {
            if store_type.is_some_and(|t| !t.eq_ignore_ascii_case(PEM)) {
                return Err(invalid(format!(
                    "SSL trust store certs can be specified only for PEM, but trust store type is \
                     {}.",
                    store_type.unwrap_or_default()
                )));
            }
            if ssl.truststore_location.is_some() {
                return Err(invalid(
                    "Both SSL trust store location and separate trust certificates are specified.",
                ));
            }
            if ssl.truststore_password.is_some() {
                return Err(invalid(
                    "SSL trust store password cannot be specified for PEM format.",
                ));
            }
            return pem_certificates(certificates).map(Some);
        }
        if let Some(location) = &ssl.truststore_location {
            require_pem("trust", store_type)?;
            if ssl.truststore_password.is_some() {
                return Err(invalid(
                    "SSL trust store password cannot be specified for PEM format.",
                ));
            }
            return pem_certificates(&read_store(location)?).map(Some);
        }
        if ssl.truststore_password.is_some() {
            return Err(invalid(
                "SSL trust store is not specified, but trust store password is specified.",
            ));
        }
        static_tls
            .client_ca_path
            .as_ref()
            .map(|path| pem_certificates(&read_store(&path.to_string_lossy())?))
            .transpose()
    }

    /// The material a listener started from `static_tls` serves with the
    /// dynamic keys `ssl`.
    fn load(static_tls: &TlsConfig, ssl: &ListenerSsl) -> Result<Self, String> {
        let (certs, key) = Self::keystore(static_tls, ssl)?;
        let client_roots = Self::truststore(static_tls, ssl)?;
        Ok(Self {
            certs,
            key,
            client_roots,
        })
    }

    /// The rustls server config, built as
    /// [`TlsConfig::build_server_config`] builds the static one.
    fn server_config(
        &self,
        client_auth: ClientAuthMode,
    ) -> Result<Arc<rustls::ServerConfig>, String> {
        let builder = rustls::ServerConfig::builder();
        let builder = match client_auth {
            ClientAuthMode::Disabled => builder.with_no_client_auth(),
            ClientAuthMode::Optional | ClientAuthMode::Required => {
                let mut roots = rustls::RootCertStore::empty();
                for root in self.client_roots.iter().flatten() {
                    roots.add(root.clone()).map_err(invalid)?;
                }
                let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
                let verifier = if client_auth == ClientAuthMode::Optional {
                    verifier.allow_unauthenticated().build()
                } else {
                    verifier.build()
                }
                .map_err(invalid)?;
                builder.with_client_cert_verifier(verifier)
            }
        };
        let mut config = builder
            .with_single_cert(self.certs.clone(), self.key.clone_key())
            .map_err(invalid)?;
        export_secrets(&mut config);
        Ok(Arc::new(config))
    }

    /// The subject DN and SANs of the leaf certificate.
    fn identity(&self) -> (Option<String>, BTreeSet<Vec<u8>>) {
        let leaf = self.certs.first().map(AsRef::as_ref).unwrap_or_default();
        (
            auth::subject_dn_rfc2253(leaf),
            auth::subject_alt_names(leaf).unwrap_or_default(),
        )
    }
}

/// As in `TlsConfig::build_server_config`: on Linux, kTLS installs the
/// traffic secrets rustls exports.
fn export_secrets(config: &mut rustls::ServerConfig) {
    #[cfg(target_os = "linux")]
    {
        config.enable_secret_extraction = true;
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = config;
    }
}

/// A `GeneralName` as Java's `X509Certificate.getSubjectAlternativeNames`
/// prints it: `[type, value]`.
fn general_name(der: &[u8]) -> String {
    let tag = der.first().map_or(0, |tag| tag & 0x1F);
    let value = der.get(2..).unwrap_or_default();
    let text = match (tag, value.len()) {
        (1 | 2 | 6, _) => String::from_utf8_lossy(value).into_owned(),
        (7, 4) => std::net::Ipv4Addr::new(value[0], value[1], value[2], value[3]).to_string(),
        _ => value.iter().fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02X}");
            hex
        }),
    };
    format!("[{tag}, {text}]")
}

fn general_names(names: &BTreeSet<Vec<u8>>) -> String {
    let names: Vec<String> = names.iter().map(|name| general_name(name)).collect();
    format!("[{}]", names.join(", "))
}

/// Kafka's `SslFactory.CertificateEntries.ensureCompatible` for the one key
/// entry of a PEM keystore: the DN must not change, and the new certificate
/// must keep every SAN of the old.
fn ensure_compatible(old: &Material, new: &Material) -> Result<(), String> {
    let (old_dn, old_sans) = old.identity();
    let (new_dn, new_sans) = new.identity();
    let same_dn = match (&old_dn, &new_dn) {
        (Some(old), Some(new)) => old.eq_ignore_ascii_case(new),
        _ => old_dn == new_dn,
    };
    if !same_dn {
        return Err(format!(
            "{CONFIG_EXCEPTION}: Keystore DistinguishedName does not match:  \
             existing={{alias={PEM_ALIAS}, DN={}}}, new={{alias={PEM_ALIAS}, DN={}}}",
            old_dn.unwrap_or_default(),
            new_dn.unwrap_or_default()
        ));
    }
    if !new_sans.is_superset(&old_sans) {
        return Err(format!(
            "{CONFIG_EXCEPTION}: Keystore SubjectAltNames do not match:  \
             existing={{alias={PEM_ALIAS}, SAN={}}}, new={{alias={PEM_ALIAS}, SAN={}}}",
            general_names(&old_sans),
            general_names(&new_sans)
        ));
    }
    Ok(())
}

/// Kafka's refusal of a listener SSL change, as `ConfigAdminManager` answers
/// it: the `SslFactory` failure, wrapped by `DynamicBrokerConfig` in
/// `ConfigException("Invalid dynamic configuration", e)`.
fn refusal(cause: &str) -> (i16, String) {
    (
        codes::INVALID_REQUEST,
        format!(
            "Invalid value {CONFIG_EXCEPTION}: Validation of dynamic config update of \
             SSLFactory failed: {cause} for configuration Invalid dynamic configuration"
        ),
    )
}

/// What a listener SSL check needs of the node the client addressed: the
/// listeners it runs, a listener a dynamic `listeners` added among them, and
/// the broker-wide `tls_config` a listener without its own uses.
#[derive(Clone)]
pub(crate) struct ListenerTlsInputs<'a> {
    pub(crate) listeners: Cow<'a, [ListenerSpec]>,
    pub(crate) tls_config: Option<&'a TlsConfig>,
}

impl ListenerTlsInputs<'_> {
    /// The TLS listeners, with the static TLS material each starts from.
    fn tls_listeners(&self) -> impl Iterator<Item = (&ListenerSpec, &TlsConfig)> {
        self.listeners.iter().filter_map(|spec| {
            spec.protocol
                .requires_tls()
                .then(|| spec.tls_config.as_ref().or(self.tls_config))
                .flatten()
                .map(|tls| (spec, tls))
        })
    }

    /// Kafka's `SslFactory.validateReconfiguration` of every TLS listener
    /// whose SSL keys differ between this node's `current` per-broker config
    /// and the `proposed` one. A listener that is not TLS has no
    /// `SslFactory`, so its keys are stored unchecked, as in Kafka.
    ///
    /// The inputs are taken by value: they are a snapshot of the running
    /// listeners, built for the one check.
    ///
    /// # Errors
    /// `INVALID_REQUEST` with Kafka's message when a store does not load or a
    /// new certificate changes the DN or drops a SAN.
    pub(crate) fn check_reconfiguration(
        self,
        current: &BTreeMap<String, String>,
        proposed: &BTreeMap<String, String>,
    ) -> Result<(), (i16, String)> {
        for (spec, static_tls) in self.tls_listeners() {
            let before = ListenerSsl::from_props(current, &spec.name);
            let after = ListenerSsl::from_props(proposed, &spec.name);
            if before == after {
                continue;
            }
            let new = Material::load(static_tls, &after).map_err(|cause| refusal(&cause))?;
            new.server_config(static_tls.client_auth)
                .map_err(|cause| refusal(&cause))?;
            let old = Material::load(static_tls, &before)
                .or_else(|_| Material::load(static_tls, &ListenerSsl::default()));
            if let Ok(old) = old {
                ensure_compatible(&old, &new).map_err(|cause| refusal(&cause))?;
            }
        }
        Ok(())
    }
}

/// The server config a listener last built from its dynamic keys, and what
/// it was built from.
struct Built {
    ssl: ListenerSsl,
    files: Vec<(PathBuf, Option<SystemTime>)>,
    config: Arc<rustls::ServerConfig>,
    /// The leaf certificate `config` serves.
    leaf: Option<CertificateDer<'static>>,
}

/// Each TLS listener's server config built from its dynamic SSL keys, rebuilt
/// when the keys or a store file change.
#[derive(Default)]
pub(crate) struct ListenerTlsCache {
    built: Mutex<HashMap<String, Built>>,
}

impl ListenerTlsCache {
    /// The server config `listener`, started from `static_tls`, serves as of
    /// `image`, or `None` when this node sets none of its SSL keys and the
    /// static config applies.
    pub(crate) fn server_config(
        &self,
        image: &MetadataImage,
        node_id: NodeId,
        listener: &str,
        static_tls: &TlsConfig,
    ) -> Option<Arc<rustls::ServerConfig>> {
        let ssl = ListenerSsl::from_image(image, node_id, listener);
        let mut built = self.built.lock().unwrap_or_else(PoisonError::into_inner);
        if ssl.is_empty() {
            built.remove(listener);
            return None;
        }
        let files: Vec<_> = store_files(static_tls, &ssl)
            .into_iter()
            .map(|path| {
                let at = modified(&path);
                (path, at)
            })
            .collect();
        if let Some(entry) = built.get(listener)
            && entry.ssl == ssl
            && entry.files == files
        {
            return Some(Arc::clone(&entry.config));
        }
        match Material::load(static_tls, &ssl).and_then(|material| {
            let config = material.server_config(static_tls.client_auth)?;
            Ok((config, material.certs.into_iter().next()))
        }) {
            Ok((config, leaf)) => {
                let dn = |leaf: Option<&CertificateDer<'_>>| {
                    leaf.and_then(|cert| auth::subject_dn_rfc2253(cert))
                        .unwrap_or_default()
                };
                tracing::info!(
                    listener,
                    previous = %dn(built.get(listener).and_then(|entry| entry.leaf.as_ref())),
                    serving = %dn(leaf.as_ref()),
                    "listener SSL stores rebuilt from the dynamic config"
                );
                built.insert(listener.to_owned(), Built {
                    ssl,
                    files,
                    config: Arc::clone(&config),
                    leaf,
                });
                Some(config)
            }
            Err(error) => {
                tracing::warn!(
                    listener,
                    %error,
                    "dynamic listener SSL stores failed to load; keeping the stores in force"
                );
                let entry = built.get_mut(listener)?;
                // Remember what failed, so the next accept does not retry it
                // until the keys or the files change again.
                entry.ssl = ssl;
                entry.files = files;
                Some(Arc::clone(&entry.config))
            }
        }
    }
}

#[cfg(test)]
impl ListenerTlsCache {
    /// The leaf certificate the config last built for `listener` serves.
    fn leaf(&self, listener: &str) -> Option<CertificateDer<'static>> {
        self.built
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(listener)?
            .leaf
            .clone()
    }
}

#[cfg(test)]
mod tests;
