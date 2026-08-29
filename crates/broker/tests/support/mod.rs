//! Shared helpers for broker integration tests.
//!
//! # Single-broker helper
//!
//! [`start`] and [`InProcess`] boot one broker and one client for simple
//! unit-style integration tests.
//!
//! # Multi-broker helpers
//!
//! [`start_n_node_with_retry`] boots an `n`-broker cluster with
//! ephemeral ports and short raft timings. Each `tests/*.rs` integration-test
//! crate that needs a 3-broker cluster declares `mod support;` and calls
//! `start_n_node_with_retry`.
//!
//! # Fault injection
//!
//! [`relay`] is a test-only TCP forwarder. Point a broker at a relay instead of
//! at its peer and the test can cut the link — including the connections that
//! are already open — without stopping either node, which is the only way to
//! produce a live minority.
//!
//! Cargo treats `tests/support/mod.rs` (rather than `tests/support.rs`) as
//! a non-binary submodule, so it does not compile the file as its own test
//! crate.

#![allow(dead_code)]

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use krabka_client_core::Client;
use tempfile::TempDir;

mod audit;
mod cluster;
// A cut-and-heal TCP relay for partition tests. Declared here so every suite
// that pulls in `support` can reach it as `support::relay`.
pub mod relay;

pub use self::{
    audit::{audit_record_seqs, consume_audit_records},
    cluster::start_n_node_with,
};

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("krabka-broker-test")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

/// Start a broker rooted at `dir` (caller owns the directory).
///
/// Restart tests use this helper. Pass the same path across two boots to
/// verify that the broker recovers persistent state (audit chain, spool)
/// correctly. The helper detects an existing raft log and then uses `Rejoin`.
pub async fn start_with_dir(dir: &std::path::Path) -> (BrokerHandle, krabka_client_core::Client) {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    // Mirror the production heuristic from `detect_bootstrap_mode` in
    // broker.rs: key Rejoin on `metadata_log_nonempty` (committed
    // quorum-state), NOT bare directory presence.  The segment dir is created
    // before the first raft commit, so dir-existence would re-bootstrap a node
    // killed mid-election instead of letting it rejoin correctly.
    let metadata_dir = dir.join("__cluster_metadata");
    if krabka_raft::metadata_log_nonempty(&metadata_dir) {
        config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    }
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = krabka_client_core::Client::builder()
        .bootstrap(&bootstrap)
        .client_id("krabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client)
}

/// Start a broker configured with an audit signing key and a given checkpoint cadence.
///
/// Uses `every_secs = 3600` so only the count-based trigger fires in tests.
pub fn start_with_audit_key(
    key_path: &std::path::Path,
    key_id: &str,
    every_n: u64,
) -> impl std::future::Future<Output = InProcess> {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.audit_signing_key_path = Some(key_path.to_path_buf());
    config.audit_signing_key_id = Some(key_id.to_string());
    config.audit_checkpoint_every_n = every_n;
    config.audit_checkpoint_every = krabka_units::hours(1); // only count trigger fires
    Box::pin(async move {
        let broker = Broker::start(config).await.expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("krabka-broker-test-audit-key")
            .build()
            .await
            .expect("client build");
        InProcess {
            broker,
            client,
            _tempdir: tempdir,
        }
    })
}

/// Start a broker whose authorizer is `SimpleAclAuthorizer` with no ACLs and no
/// super-users (deny-all for the anonymous test client). The `for_tests`
/// defaults enable audit. The broker denies the anonymous client every admin
/// operation, which produces `AuthorizationDenied` audit events.
pub async fn start_with_deny_all_authz() -> InProcess {
    use std::collections::HashSet;

    use krabka_broker::authorizer::SimpleAclAuthorizer;

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    // Replace the default AllowAllAuthorizer with a deny-all SimpleAclAuthorizer
    // (empty ACL store, no super-users). The anonymous test client connects
    // with no credentials so it has no super-user bypass — every operation is
    // denied and the auditing decorator emits AuthorizationDenied events.
    config.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(HashSet::new()));
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("krabka-broker-test-deny")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

/// Fetch all records from `AUDIT_TOPIC` partition 0, JSON-decode each
/// record value, and return the decoded objects. Mirrors the
/// `broker_started_event_is_written_to_audit_topic` fetch pattern.
pub async fn wait_for_audit_record<F>(
    client: &krabka_client_core::Client,
    what: &str,
    mut predicate: F,
) -> Vec<serde_json::Value>
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = consume_audit_records(client).await;
        if records.iter().any(&mut predicate) {
            return records;
        }
        assert!(
            Instant::now() <= deadline,
            "audit record '{what}' did not appear within 30s; last={records:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_audit_seq_count(
    client: &krabka_client_core::Client,
    min_count: usize,
) -> Vec<u64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seqs = audit_record_seqs(client).await;
        if seqs.len() >= min_count {
            return seqs;
        }
        assert!(
            Instant::now() <= deadline,
            "audit seq count did not reach {min_count} within 30s; last={seqs:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Round-trip a Metadata request to learn the topic's assigned UUID.
/// Produce / Fetch at v ≥ 13 carry only `topic_id` on the wire, so the
/// caller must plumb the real UUID through.
pub async fn topic_id_for(
    client: &krabka_client_core::Client,
    name: &str,
) -> krabka_protocol::primitives::uuid::Uuid {
    use krabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};

    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

// ── Multi-broker helpers ──────────────────────────────────────────────────────
//
// The functions below are only meaningful on non-Windows targets because
// openraft's debug_assert! races on the hosted Windows task scheduler.
// Individual test files gate their use with ``.

/// Lazily-initialized tracing subscriber so `RUST_LOG=...` works in
/// integration tests. It is safe to call this many times, because `try_init`
/// is a no-op after the first success.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Reserve `n` pairs of ephemeral loopback ports, one client port and one
/// controller port per broker, with the bind-and-drop trick. Bind a
/// `TcpListener` on `127.0.0.1:0`, read its assigned port, then drop the
/// listener. The OS does not immediately reuse the port for another bind, so
/// the caller can pass it to `Broker::start` and the broker re-binds it on the
/// same address.
///
/// This avoids the Linux `TIME_WAIT` problem that fixed ports hit when many
/// tests in the same binary boot 3-broker clusters back-to-back.
pub async fn bind_and_drop_ports(n: usize) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let mut client_addrs = Vec::with_capacity(n);
    let mut controller_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_addrs.push(cl.local_addr().unwrap());
        let ct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        controller_addrs.push(ct.local_addr().unwrap());
        drop((cl, ct));
    }
    (client_addrs, controller_addrs)
}

/// Race-free replacement for [`bind_and_drop_ports`]. It binds `n` pairs of
/// ephemeral loopback listeners, one client and one controller per broker, and
/// returns their concrete addrs **alongside the still-open listeners**,
/// index-aligned.
///
/// Hand `client_listeners[i]` and `controller_listeners[i]` to
/// [`krabka_broker::Broker::start_with_listeners`] or
/// `start_with_controller_listener`, so the OS port is never released before
/// the broker adopts it. That closes the [`bind_and_drop_ports`] TOCTOU window
/// in which a concurrently-running test binary steals the freed port
/// (`AddrInUse`) under parallel `cargo nextest`.
///
/// The returned `SocketAddr`s are the listeners' real `local_addr()`s, so the
/// caller builds its static voter set and advertised addresses from them
/// exactly as with [`bind_and_drop_ports`]. The only call-site change is to
/// pass the matching listener into `start_with_listeners` instead of letting
/// `Broker::start` re-bind the address.
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub async fn bind_and_hold_ports(
    n: usize,
) -> (
    Vec<SocketAddr>,
    Vec<SocketAddr>,
    Vec<tokio::net::TcpListener>,
    Vec<tokio::net::TcpListener>,
) {
    let mut client_listeners = Vec::with_capacity(n);
    let mut controller_listeners = Vec::with_capacity(n);
    for _ in 0..n {
        client_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        controller_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let client_addrs = client_listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let controller_addrs = controller_listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    (
        client_addrs,
        controller_addrs,
        client_listeners,
        controller_listeners,
    )
}

/// Build a `BrokerConfig` for broker `i` (0-indexed) in an `n`-broker
/// cluster from the supplied ephemeral port lists and static voter map.
/// This is the *static-voter* bootstrap-then-join helper. It exists for tests
/// such as `elect_leaders` that drive `add_learner` and `change_membership`
/// manually and need extra config overrides per broker. `start_n_node`'s
/// auto-join path cannot support that flow.
pub fn broker_config(
    i: usize,
    client_addrs: &[SocketAddr],
    controller_addrs: &[SocketAddr],
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
    mode: BootstrapMode,
) -> BrokerConfig {
    let listen = client_addrs[i];
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.node_id = NodeId(u64::try_from(i + 1).unwrap());
    cfg.controller_listen_addr = controller_addrs[i];
    // `controller_quorum_voters` carries `<host>:<port>` strings (the dialer
    // re-resolves per connect); test voter sets are built from `SocketAddr`s,
    // so stringify here.
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (NodeId(*id), a.to_string()))
        .collect();
    cfg.bootstrap_mode = mode;
    cfg
}

/// Boot an `n`-broker cluster with ephemeral ports and short raft timings
/// through **static multi-voter bootstrap** (KIP-595 Slice 3c):
///
/// * All `n` brokers boot in `Bootstrap` mode (`auto_join = false`), each
///   configured with the *same* `controller_quorum_voters` = the full
///   `[(1, ctrl_addr_1), …, (n, ctrl_addr_n)]` set.
/// * Each node seeds the full static voter set, and the nodes elect a leader
///   among themselves over the real KIP-595 wire. There is no `AddRaftVoter`
///   and no auto-join.
///
/// Blocks until a leader emerges and reports the full `n`-voter committed set.
/// Returns `(handle, config, tempdir)` triples in spawn order.
/// `cluster[0]` is `broker_id` 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    start_n_node_with(n, |_, _| {}).await
}

/// Retry `start_n_node` up to 3 times. Short raft timings sometimes
/// split-vote on slow runners. A fresh tempdir and port set on retry
/// clears the openraft state and usually succeeds within 2 attempts.
pub async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match start_n_node(n).await {
            Ok(cluster) => return cluster,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "cluster start failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts; last error: {last_err:?}");
}

/// Start a broker on listen addresses another broker has just vacated.
///
/// [`BrokerHandle::shutdown`] awaits its listener tasks, so the sockets are
/// closed by the time it returns, but the port can still be unbindable for a
/// moment afterwards, and a concurrently-running test binary can win the race
/// for the freed ephemeral port. Both surface as `AddrInUse` on the re-bind.
/// Retry briefly instead of failing the test on a port-reuse race, in the
/// spirit of [`start_n_node_with_retry`].
pub async fn start_reusing_addrs(cfg: &BrokerConfig, what: &str) -> BrokerHandle {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Broker::start(cfg.clone()).await {
            Ok(handle) => return handle,
            Err(BrokerError::Io(e))
                if e.kind() == std::io::ErrorKind::AddrInUse && Instant::now() < deadline =>
            {
                tracing::warn!(%what, error = %e, "vacated port not yet bindable; retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("{what}: {e:?}"),
        }
    }
}

/// Await every broker's controller image until each one sees `n` brokers
/// registered. Call this before any test that needs the partition's replica
/// set to include all `n` nodes. `CreateTopics` reads `image.brokers()` to pick
/// replicas, and a race here silently degrades to a smaller replica set.
///
/// This helper uses the panicking `wait_until_brokers_registered` awaiter on
/// purpose. Tests call this helper directly, not through the
/// `start_n_node_with_retry` path, so a timeout must fail the test.
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(n).await;
    }
}

// ---------------------------------------------------------------------------
// Container-suite addressing
// ---------------------------------------------------------------------------

/// A free TCP port on the loopback interface.
///
/// Bound and immediately dropped, so the port is free when the caller binds it.
/// That leaves a window in which something else could take it; the alternative
/// is a fixed port, which is not a window but a certainty whenever two tests run
/// at once.
#[must_use]
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Listen and advertised addresses for a broker the JVM containers talk to.
///
/// The suites that drive real Kafka containers used to hard-code `9092`/`9093`,
/// which is why they had to run one at a time: two of them, or two tests inside
/// one of them, would race for the same port and the loser reported `Address
/// already in use` as a test failure. Each caller gets its own pair now.
///
/// `advertised` keeps the `host.docker.internal` name. Containers resolve it
/// through `--add-host=host.docker.internal:host-gateway`; the host resolves it
/// through an `/etc/hosts` entry pointing at loopback, which CI adds before
/// running these suites.
pub struct JvmListeners {
    /// What the broker binds, e.g. `0.0.0.0:41551`.
    pub listen: String,
    /// What it advertises and what the containers bootstrap against.
    pub advertised: String,
    /// The controller listener, on its own port.
    pub controller: String,
}

impl JvmListeners {
    /// Allocate a fresh set.
    #[must_use]
    pub fn allocate() -> Self {
        let client = free_port();
        let controller = free_port();
        Self {
            listen: format!("0.0.0.0:{client}"),
            advertised: format!("host.docker.internal:{client}"),
            controller: format!("0.0.0.0:{controller}"),
        }
    }

    /// The controller as containers address it.
    #[must_use]
    pub fn controller_advertised(&self) -> String {
        let port = self
            .controller
            .rsplit(':')
            .next()
            .expect("controller addr has a port");
        format!("host.docker.internal:{port}")
    }
}

/// A container name unlikely to collide with a concurrent run.
///
/// `docker run --name` fails outright when the name is taken, so a fixed name is
/// a second reason these suites could not overlap -- and a stale container from
/// a killed run blocks every later run until someone removes it by hand.
#[must_use]
pub fn unique_container_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// This crate's directory, wherever the test is running from.
///
/// Cargo exports `CARGO_MANIFEST_DIR` to a test process, so under Cargo this is
/// the path `env!` would have produced. It is read rather than expanded because
/// `env!` bakes an absolute build path into the binary, which ties the test to
/// the directory it was compiled in -- `rules_rust` rejects such a binary
/// outright, and under Cargo it only works when launched from that same path.
///
/// Bazel sets no such variable; it stages a target's `data` under
/// `$TEST_SRCDIR/$TEST_WORKSPACE/<package>`. Falling back to that is what lets
/// the TLS suites find their fixtures under both.
///
/// # Panics
///
/// Panics when neither Cargo's variable nor Bazel's pair is set, which means the
/// test was launched by something that stages fixtures differently again.
#[must_use]
pub fn manifest_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let srcdir = std::env::var("TEST_SRCDIR")
        .expect("CARGO_MANIFEST_DIR (cargo) or TEST_SRCDIR (bazel) must be set");
    let workspace =
        std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
    std::path::PathBuf::from(srcdir)
        .join(workspace)
        .join("crates/broker")
}

// ── KFC-9 operator keys and freeze/break-glass brokers ────────────────────────

/// One operator key pair on disk, in the forms a test and a broker need.
///
/// The broker is handed only `public`; the private half stays here and signs
/// in process. That mirrors the operator's real position, where `--sign-with`
/// reads a PKCS#8 file that never leaves their machine.
///
/// `crates/guard-cli/tests/guard_cli.rs` carries its own copy of this fixture.
/// Cargo cannot share a `tests/` helper across crates, and the alternative --
/// a `#[cfg(feature)]` seam in the broker library purely for a test key -- puts
/// key-minting code in the shipped artifact to save a duplicated fixture.
pub struct OperatorKey {
    /// The PKCS#8 private key. Signs in process; the broker never sees it.
    pub pkcs8: Vec<u8>,
    /// The raw 32-byte public key, on disk, as `[[operator_keys]]` names it.
    pub public_path: std::path::PathBuf,
    /// The key id the signature carries.
    pub key_id: String,
    /// The principal this key is bound to, in the Kafka form both the freeze
    /// path and the break-glass path use.
    pub principal: String,
}

impl OperatorKey {
    /// The signing key, rebuilt per call because `Ed25519KeyPair` is not `Sync`.
    pub fn pair(&self) -> ring::signature::Ed25519KeyPair {
        ring::signature::Ed25519KeyPair::from_pkcs8(&self.pkcs8).expect("parse pkcs8")
    }

    /// The trust-set entry that names this key.
    pub fn entry(&self) -> krabka_broker::operator_keys::OperatorKeyEntry {
        krabka_broker::operator_keys::OperatorKeyEntry {
            key_id: self.key_id.clone(),
            principal: self.principal.clone(),
            public_key_path: self.public_path.clone(),
        }
    }
}

/// Mint one operator key pair under `dir`, bound to `principal`.
pub fn mint_operator_key(dir: &std::path::Path, key_id: &str, principal: &str) -> OperatorKey {
    use ring::signature::KeyPair as _;

    let rng = ring::rand::SystemRandom::new();
    let der = ring::signature::Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
    let pair = ring::signature::Ed25519KeyPair::from_pkcs8(der.as_ref()).expect("parse pkcs8");

    let public_path = dir.join(format!("{key_id}.pub"));
    std::fs::write(&public_path, pair.public_key().as_ref()).expect("write public key");

    OperatorKey {
        pkcs8: der.as_ref().to_vec(),
        public_path,
        key_id: key_id.to_owned(),
        principal: principal.to_owned(),
    }
}

/// The principal a plaintext listener authenticates, in the Kafka form.
///
/// [`start_with_operator_key`] binds its key to this name and lists it as the
/// one break-glass approver, so a single entry serves both paths. A suite that
/// needs two distinct principals -- a proposer and an approver -- wants
/// [`start_with_operator_keys_sasl`] instead.
pub const ANONYMOUS: &str = "User:ANONYMOUS";

/// Boot one broker on `dir` that trusts `keys` and takes `approvers` as its
/// break-glass approver set.
///
/// The returned [`BrokerConfig`] is what makes a restart possible: hand it back
/// to [`start_reusing_addrs`] after a `shutdown` and the node comes up on the
/// same addresses and the same trust set. Set `bootstrap_mode` to
/// [`BootstrapMode::Rejoin`] before doing so -- unlike [`start_with_dir`], this
/// helper cannot infer it for you on the second boot, and a node that
/// re-bootstraps comes back with an empty freeze registry.
pub async fn start_with_operator_keys(
    dir: &std::path::Path,
    keys: &[&OperatorKey],
    approvers: &[&str],
) -> (BrokerHandle, Client, BrokerConfig) {
    let entries: Vec<_> = keys.iter().map(|k| k.entry()).collect();
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = krabka_broker::operator_keys::OperatorKeys::load(&entries)
        .expect("load the operator trust set");
    config.break_glass.approvers = approvers.iter().map(|a| (*a).to_owned()).collect();

    let broker = Broker::start(config.clone()).await.expect("broker start");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("krabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client, config)
}

/// [`start_with_operator_keys`] with the single-key, single-approver shape the
/// freeze cases want.
pub async fn start_with_operator_key(
    dir: &std::path::Path,
    key: &OperatorKey,
) -> (BrokerHandle, Client, BrokerConfig) {
    start_with_operator_keys(dir, &[key], &[ANONYMOUS]).await
}

/// Boot one broker on `dir` behind a `SASL_PLAINTEXT` listener, so a suite can
/// speak to it as more than one principal.
///
/// The break-glass workflow needs that: a proposal is consumed by two distinct
/// approving principals, and the proposer may not approve their own. Over a
/// plaintext listener every connection authenticates as the same
/// [`ANONYMOUS`], which can prove a refusal but never a completion.
///
/// `users` are `(name, password)` pairs for the PLAIN credential store. Each
/// authenticates as `User:<name>`, which is the spelling `approvers` and
/// `[[operator_keys]]` must use.
pub async fn start_with_operator_keys_sasl(
    dir: &std::path::Path,
    keys: &[&OperatorKey],
    approvers: &[&str],
    users: &[(&str, &str)],
) -> (BrokerHandle, String, BrokerConfig) {
    let entries: Vec<_> = keys.iter().map(|k| k.entry()).collect();
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = krabka_broker::operator_keys::OperatorKeys::load(&entries)
        .expect("load the operator trust set");
    config.break_glass.approvers = approvers.iter().map(|a| (*a).to_owned()).collect();
    config.listeners = vec![krabka_broker::config::ListenerSpec {
        name: "SASL_PLAINTEXT".to_owned(),
        bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
        advertised: "127.0.0.1:0".to_owned(),
        protocol: krabka_security::ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    "SASL_PLAINTEXT".clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![krabka_security::SaslMechanism::Plain];
    for (name, pass) in users {
        config
            .plain_credentials
            .insert((*name).to_owned(), (*pass).to_owned());
    }

    let broker = Broker::start(config.clone()).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, config)
}

/// Client-side `SASL_PLAINTEXT`/`PLAIN` security for one `(user, pass)` pair.
///
/// Pair it with [`start_with_operator_keys_sasl`] to get a client that the
/// broker authenticates as `User:<user>`.
pub fn sasl_plain_security(user: &str, pass: &str) -> krabka_client_core::security::ClientSecurity {
    krabka_client_core::security::ClientSecurity {
        protocol: krabka_security::ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(krabka_client_core::security::SaslCredentials::Plain {
            username: user.to_owned(),
            password: pass.to_owned(),
        }),
        sasl_host: None,
    }
}

/// A client authenticated as `user` against `bootstrap`.
pub async fn sasl_client(bootstrap: &str, user: &str, pass: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("krabka-broker-test")
        .security(sasl_plain_security(user, pass))
        .build()
        .await
        .expect("client build")
}
