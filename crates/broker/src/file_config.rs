//! TOML file-config surface for the `krabka-broker` binary.
//!
//! Deserialized by `--config-file PATH` in `bin/broker.rs` and applied to
//! [`crate::BrokerConfig`] by [`FileConfig::apply_to`]. Every field here is
//! `Option` or defaulted: a present value replaces the current broker value,
//! an absent one retains it.
//!
//! [`FileConfig`] is the whole document; each `[section]` it names has its own
//! module below, holding that section's TOML shape and the code that applies
//! it. `runtime_macros` is declared first and with `#[macro_use]`, because
//! `macro_rules!` is textually scoped and every `runtime_*` module expands the
//! `set_runtime_*` macros it defines.

use krabka_security::ListenerProtocol;
use krabka_units::Time;
use schemars::JsonSchema;
use serde::Deserialize;

#[macro_use]
mod runtime_macros;

mod apply;
mod audit;
mod authorization;
mod delegation_token;
mod gssapi;
mod listener;
mod listener_settings;
mod oauthbearer;
mod oauthbearer_apply;
mod object_store;
mod process;
mod quorum_voters;
mod remote_storage;
mod runtime_config;
mod runtime_coordinators;
mod runtime_core;
mod runtime_policy;
mod runtime_storage;
mod runtime_transactions;
mod schema_registry;
mod tail;
mod validate;

pub use self::{
    audit::{
        FileAuditCheckpointConfig, FileAuditConfig, FileAuditSigningConfig, FileAuditSpoolConfig,
    },
    authorization::{AuthzType, FileAuthorizationConfig, FileOpaConfig},
    delegation_token::FileDelegationTokenConfig,
    gssapi::{FileGssapiConfig, FileInterBrokerCredentials},
    listener::{FileClientAuthMode, FileListener, FileListenerSaslConfig, FileTlsConfig},
    oauthbearer::FileOAuthBearerConfig,
    object_store::{FileRemoteStorageGcsConfig, FileRemoteStorageS3Config, FileWormConfig},
    process::{FileProcessConfig, FileStretchConfig},
    remote_storage::{FileKafkaRlmmConfig, FileRemoteStorageConfig},
    runtime_config::RuntimeFileConfig,
    schema_registry::FileSchemaRegistryConfig,
};

/// Failures surfaced by [`FileConfig::apply_to`]. Each variant
/// corresponds to a specific misconfiguration the broker can diagnose
/// at startup; the variants exist (rather than a single `String`
/// fallthrough) so the binary entry point can log structured context.
#[derive(Debug, thiserror::Error)]
pub enum FileConfigError {
    /// A `[section]` referenced by another field is missing — e.g.
    /// `[authorization] type = "opa"` without an `[authorization.opa]`
    /// table. The payload names the missing section.
    #[error("missing required TOML section: {0}")]
    MissingSection(String),
    /// `OpaAuthorizer::new` failed (see [`crate::authorizer::opa::OpaConfigError`]).
    /// The payload is the underlying error's `Debug` form — formatted
    /// here rather than at the call site so the binary entry point can
    /// log a single string.
    #[error("OPA authorizer configuration error: {0}")]
    OpaConfig(String),
    /// [`crate::schema_validation::SchemaValidator::new`] failed. The payload
    /// is the underlying
    /// [`SchemaValidatorError`][crate::schema_validation::SchemaValidatorError]'s
    /// `Debug` form — formatted here rather than at the call site so the
    /// binary entry point can log a single string.
    #[error("schema registry configuration error: {0}")]
    SchemaRegistryConfig(String),
    /// A TOML section's contents conflict in a way only the apply step
    /// can diagnose — e.g. `[remote_storage]` carrying both `storage_dir`
    /// (local backend) and `[remote_storage.s3]` (object-store backend).
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// A `controller_quorum_voters` entry is malformed (no `@`, non-numeric
    /// node id) or its `<host>:<port>` could not be DNS-resolved within the
    /// startup retry budget. The payload is the offending entry plus the
    /// underlying reason.
    #[error("invalid controller_quorum_voters entry: {0}")]
    InvalidQuorumVoter(String),
}

/// Top-level shape of `broker.toml`. `serde(deny_unknown_fields)` is
/// off — new fields may be added and old binaries should warn rather
/// than refuse to start.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileConfig {
    /// Operational runtime policy. Present values replace the current broker
    /// value; absent values retain it.
    pub runtime: Option<RuntimeFileConfig>,
    pub broker_id: Option<i32>,
    pub log_dir: Option<String>,
    /// Additional JBOD data directories (KIP-113). Maps to
    /// [`crate::BrokerConfig::extra_log_dirs`].
    #[serde(default)]
    pub extra_log_dirs: Vec<String>,
    /// KIP-392: this broker's rack id. Maps to `BrokerConfig::rack`.
    pub rack: Option<String>,

    /// KIP-392: replica selector name (`"leader"` | `"rack-aware"`).
    /// Maps to `BrokerConfig::replica_selector`.
    pub replica_selector: Option<String>,

    /// `[stretch]` section — the three-site stretch deployment this node
    /// belongs to. Maps to `BrokerConfig::stretch`. Absent leaves the
    /// `BrokerConfig` default `None`, an ordinary non-stretched cluster.
    pub stretch: Option<FileStretchConfig>,
    /// How often this broker sends `BrokerHeartbeat` to the controller leader.
    /// Absent leaves the `BrokerConfig` default intact.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub heartbeat_interval: Option<Time>,
    /// Controller-side session timeout for broker heartbeats. Absent leaves the
    /// `BrokerConfig` default intact.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub heartbeat_timeout: Option<Time>,
    /// Maximum follower lag before the leader proposes ISR shrink. Absent
    /// leaves the `BrokerConfig` default intact.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub replica_lag_time_max: Option<Time>,
    /// Controller election timeout. Absent leaves the `BrokerConfig` default intact.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub controller_election_timeout: Option<Time>,
    /// Controller heartbeat interval. Absent leaves the `BrokerConfig` default intact.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub controller_heartbeat_interval: Option<Time>,
    pub inter_broker_listener_name: Option<String>,

    /// Maximum number of live broker connections across all listeners
    /// (Kafka `max.connections`). Connections accepted past this ceiling
    /// are closed immediately. Absent leaves the `BrokerConfig` default
    /// `usize::MAX` (unlimited), matching Kafka's `Integer.MAX_VALUE`.
    pub max_connections: Option<usize>,

    /// Maximum number of live connections from any single client IP
    /// (Kafka `max.connections.per.ip`). Absent leaves the `BrokerConfig`
    /// default `usize::MAX` (unlimited).
    pub max_connections_per_ip: Option<usize>,

    /// KIP-595 static controller quorum voter set. Each entry is
    /// `<node_id>@<host>:<port>` pointing at a broker's controller listener
    /// (port 9093). At apply time each entry is parsed (NOT DNS-resolved) and
    /// its `<host>:<port>` is carried verbatim into
    /// `BrokerConfig::controller_quorum_voters`. The inter-broker dialer
    /// re-resolves the host on every (re)connect (`TcpStream::connect`), so a
    /// peer that restarts on a new pod IP (a `StatefulSet` pod keeps its stable
    /// DNS name but gets a fresh A record) is reached again without restarting
    /// this broker — pre-resolving here would freeze the peer's boot-time IP
    /// and strand a rejoining voter. Empty leaves the single self-voter the
    /// binary seeds (standalone).
    #[serde(default)]
    pub controller_quorum_voters: Vec<String>,

    /// KIP-853 controller discovery endpoints. Hosts remain unresolved so DNS
    /// names can be refreshed on each retry.
    #[serde(default)]
    pub bootstrap_servers: Vec<String>,

    /// Enable automatic dynamic controller enrollment.
    pub auto_join: Option<bool>,

    /// TLS server name (SNI) presented when dialing a PEER's controller
    /// listener for the KIP-595 quorum. The operator renders the shared
    /// headless-Service FQDN here — a SAN on every broker's serving cert —
    /// so mTLS validation succeeds no matter which peer (resolved to a pod
    /// IP) is dialed. Absent falls back to `"localhost"`. Maps to
    /// [`crate::BrokerConfig::controller_server_name`].
    pub controller_server_name: Option<String>,

    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,

    /// Controller listener security protocol. When `Some(Ssl)`
    /// the controller listener terminates TLS using `tls_config`.
    #[schemars(with = "Option<String>")]
    pub controller_listener_protocol: Option<ListenerProtocol>,

    /// TLS material for the controller listener (and any
    /// listener whose `protocol` is TLS-bearing).
    pub tls_config: Option<FileTlsConfig>,

    /// SASL/OAUTHBEARER validator tuning. Only relevant when a
    /// listener enables the `OAUTHBEARER` mechanism.
    pub oauthbearer: Option<FileOAuthBearerConfig>,

    /// KIP-48: delegation-token master key + lifetime knobs.
    /// Env var `KRABKA_DELEGATION_TOKEN_SECRET_KEY` wins over `secret_key`
    /// here. When neither source provides a key, the broker disables
    /// delegation-token auth.
    pub delegation_token: Option<FileDelegationTokenConfig>,

    /// Principals that are unconditionally authorized for
    /// all operations, including KIP-48 delegation-token `act-as`. The
    /// operator emits `super_users = ["ANONYMOUS"]` when
    /// `Kafka.spec.delegationToken` is set so its PLAINTEXT
    /// inter-broker reconcile loop can mint per-`KafkaUser` tokens.
    /// `None` and `Some(empty)` are equivalent — both leave
    /// `BrokerConfig.super_users` empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub super_users: Option<Vec<String>>,

    /// KIP-405: tiered-storage enablement. Setting
    /// `storage_dir` turns tiered storage on broker-wide and roots the
    /// local reference `RemoteStorageManager` there.
    pub remote_storage: Option<FileRemoteStorageConfig>,

    /// Pluggable cluster authorizer + super-user list.
    /// `None` ⇒ [`crate::authorizer::AllowAllAuthorizer`] with empty
    /// super-users (default-on-no-config behavior). When `Some`, the
    /// `type` field selects the authorizer implementation; for
    /// `type = "opa"`, the `[authorization.opa]` subtable is required.
    pub authorization: Option<FileAuthorizationConfig>,

    /// `[process]` section — `KRaft` `process.roles`. Absent / empty leaves
    /// the `BrokerConfig` default `[Controller, Broker]`.
    pub process: Option<FileProcessConfig>,

    /// SASL/GSSAPI (Kerberos) accept-path config. Broker-global —
    /// there is one `[gssapi]` block per broker. Relevant when a listener
    /// enables the `GSSAPI` mechanism.
    pub gssapi: Option<FileGssapiConfig>,

    /// Credentials this broker uses to authenticate *to* peer brokers and
    /// controller listeners (inter-broker initiate path).
    pub inter_broker_credentials: Option<FileInterBrokerCredentials>,

    /// `FedRAMP` 20x MLA audit subsystem configuration.
    /// Absent → secure default (enabled, standard internal topic name).
    pub audit: Option<FileAuditConfig>,

    /// `[schema_registry]` section — the Confluent-compatible registry that
    /// supplies the schemas for KFC-7 broker-side validation. `None` means no
    /// topic can turn schema validation on.
    pub schema_registry: Option<FileSchemaRegistryConfig>,
}
