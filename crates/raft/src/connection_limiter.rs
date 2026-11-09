//! Live-connection accounting for Kafka's `ConnectionQuotas`: the
//! `max.connections` caps, broker-wide and per listener, the
//! `max.connections.per.ip` cap and its per-host overrides, and the KIP-612
//! `max.connection.creation.rate` gates, together with the RAII guard that
//! releases a slot when a connection ends. It sits apart from the accept
//! loops that consult it: one limiter serves every broker listener of a node,
//! and the controller listener has one of its own, as Kafka's
//! `ControllerServer` builds its own `SocketServer`.
//!
//! Every quota is resolved per accept from the metadata image
//! ([`ConnectionQuotaSettings::resolve`]), so a dynamic change applies to the
//! next connection without a restart.

mod overrides;
mod rate;
mod settings;

use {
    crate::types::NodeId,
    krabka_metadata::MetadataImage,
    overrides::OverrideAddresses,
    rate::CreationRates,
    std::{
        collections::HashMap,
        hash,
        net::IpAddr,
        sync::{Arc, Mutex, MutexGuard, PoisonError},
        time::{Duration, Instant},
    },
};
pub use {
    overrides::IpOverrides,
    settings::{
        ConnectionLimits, ConnectionQuotaSettings, MAX_CONNECTION_CREATION_RATE, MAX_CONNECTIONS,
        MAX_CONNECTIONS_PER_IP, MAX_CONNECTIONS_PER_IP_OVERRIDES,
    },
};

/// The quotas one accept is checked against: the settings in force and the
/// resolved override addresses.
#[derive(Debug, Clone)]
pub struct ConnectionQuotas {
    pub settings: ConnectionQuotaSettings,
    pub per_ip_overrides: IpOverrides,
}

impl ConnectionQuotas {
    /// The cap for `ip`: its override, else `max.connections.per.ip`.
    #[must_use]
    pub fn max_connections_for(&self, ip: IpAddr) -> usize {
        self.per_ip_overrides
            .get(&ip.to_canonical())
            .copied()
            .unwrap_or(self.settings.max_connections_per_ip)
    }
}

/// The listener an accept arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptingListener<'a> {
    /// The listener's name, matched without regard to case as Kafka's
    /// `ListenerName.normalised` matches it.
    pub name: &'a str,
    /// Whether the listener is exempt from the broker-wide cap and rate:
    /// Kafka's `protectedListener`, the inter-broker listener of a node with
    /// more than one listener.
    pub protected: bool,
}

/// Live connection counts, under one lock as Kafka keeps them under `counts`.
#[derive(Debug, Default)]
struct Counts {
    total: usize,
    per_ip: HashMap<IpAddr, usize>,
    per_listener: HashMap<String, usize>,
}

/// Live-connection accounting for one node's listeners. A clone shares the
/// same counters, so every accept loop and every [`ConnectionGuard`] account
/// against one set of totals.
#[derive(Clone)]
pub struct ConnectionLimiter {
    configured: ConnectionLimits,
    counts: Arc<Mutex<Counts>>,
    rates: Arc<Mutex<CreationRates>>,
    overrides: Arc<OverrideAddresses>,
    /// The settings last resolved for each listener, which a refused value
    /// leaves in force.
    applied: Arc<Mutex<HashMap<String, ConnectionQuotaSettings>>>,
}

/// The connection ceiling that refused an inbound connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionLimit {
    /// `max.connections`, broker-wide.
    Global,
    /// `listener.name.<listener>.max.connections`.
    Listener,
    /// `max.connections.per.ip` or the address's override.
    PerIp,
}

/// Lock `mutex`, recovering the data of a poisoned lock: a count is still
/// right after a panic elsewhere, because every update is one statement.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ConnectionLimiter {
    /// A limiter for a node started with `max_connections` and
    /// `max_connections_per_ip`.
    #[must_use]
    pub fn new(max_connections: usize, max_connections_per_ip: usize) -> Self {
        Self {
            configured: ConnectionLimits {
                max_connections,
                max_connections_per_ip,
            },
            counts: Arc::default(),
            rates: Arc::default(),
            overrides: Arc::default(),
            applied: Arc::default(),
        }
    }

    /// The caps this limiter was started with.
    #[must_use]
    pub const fn limits(&self) -> ConnectionLimits { self.configured }

    /// The quotas in force on `listener` of `node_id` as of `image`. A value
    /// Kafka would refuse leaves the listener's previous value in force. A new
    /// `max.connections.per.ip.overrides` set that names a host is resolved in
    /// the background, so this never waits on DNS. It must be called within a
    /// Tokio runtime.
    #[must_use]
    pub fn quotas(
        &self,
        image: &MetadataImage,
        node_id: NodeId,
        listener: &str,
    ) -> ConnectionQuotas {
        let name = listener.to_ascii_uppercase();
        let mut applied = lock(&self.applied);
        let settings = ConnectionQuotaSettings::resolve(
            image,
            node_id,
            listener,
            self.configured,
            applied.get(&name),
        );
        applied.insert(name, settings.clone());
        drop(applied);
        let per_ip_overrides = self.overrides.addresses(&settings.per_ip_overrides);
        ConnectionQuotas {
            settings,
            per_ip_overrides,
        }
    }

    /// Record a new connection on `listener` at `now` against the creation
    /// rates of `quotas`, and return how long the acceptor waits before it
    /// takes the connection, at most `max_delay`
    /// (`quota.window.size.seconds`). Kafka's acceptor waits the throttle out
    /// and then accepts.
    #[must_use]
    pub fn creation_delay(
        &self,
        listener: AcceptingListener<'_>,
        quotas: &ConnectionQuotas,
        now: Instant,
        max_delay: Duration,
    ) -> Duration {
        lock(&self.rates).admit(
            listener.name,
            listener.protected,
            (
                quotas.settings.max_connection_creation_rate,
                quotas.settings.listener_max_connection_creation_rate,
            ),
            now,
            max_delay,
        )
    }

    /// Take back a connection [`Self::creation_delay`] recorded, for one the
    /// peer's `ip` quota throttles, as Kafka's `updateListenerMetrics`
    /// un-records it from the broker-wide and listener rates.
    pub fn refund_creation(&self, listener: AcceptingListener<'_>, quotas: &ConnectionQuotas) {
        lock(&self.rates).refund(
            listener.name,
            listener.protected,
            (
                quotas.settings.max_connection_creation_rate,
                quotas.settings.listener_max_connection_creation_rate,
            ),
        );
    }

    /// Try to reserve a slot for a connection from `ip` on `listener` under
    /// `quotas`, which the caller resolves per accept. On success returns a
    /// [`ConnectionGuard`] that releases the slot on drop. Lowering a cap
    /// closes no connection that is already counted, as in Kafka's
    /// `ConnectionQuotas`.
    ///
    /// The checks run in Kafka's order: the listener's cap, then, unless the
    /// listener is protected, the broker-wide cap, then the cap of the
    /// address.
    ///
    /// # Errors
    /// Returns the [`ConnectionLimit`] that refused the connection, and
    /// reserves nothing.
    pub fn try_acquire_within(
        &self,
        ip: IpAddr,
        listener: AcceptingListener<'_>,
        quotas: &ConnectionQuotas,
    ) -> Result<ConnectionGuard, ConnectionLimit> {
        let name = listener.name.to_ascii_uppercase();
        let mut counts = lock(&self.counts);
        let on_listener = counts.per_listener.get(&name).copied().unwrap_or(0);
        if on_listener >= quotas.settings.listener_max_connections {
            return Err(ConnectionLimit::Listener);
        }
        if !listener.protected && counts.total >= quotas.settings.max_connections {
            return Err(ConnectionLimit::Global);
        }
        let from_ip = counts.per_ip.get(&ip).copied().unwrap_or(0);
        if from_ip >= quotas.max_connections_for(ip) {
            return Err(ConnectionLimit::PerIp);
        }
        counts.total += 1;
        *counts.per_ip.entry(ip).or_default() += 1;
        *counts.per_listener.entry(name.clone()).or_default() += 1;
        drop(counts);
        Ok(ConnectionGuard {
            limiter: self.clone(),
            ip,
            listener: name,
        })
    }

    /// Diagnostic accessor: current live-connection count across listeners.
    #[must_use]
    pub fn total(&self) -> usize { lock(&self.counts).total }
}

/// RAII release for one accepted connection.
///
/// Moved into the spawned per-connection task, so it fires however the
/// connection ends: clean close, error, panic, or task abort. On drop it
/// decrements the total, the address and the listener counts, and removes a
/// count that reaches 0 so the maps do not grow without bound.
pub struct ConnectionGuard {
    limiter: ConnectionLimiter,
    ip: IpAddr,
    listener: String,
}

/// Decrement `key`'s count in `map`, removing it at 0.
fn release<K: Eq + hash::Hash>(map: &mut HashMap<K, usize>, key: &K) {
    if let Some(count) = map.get_mut(key) {
        *count -= 1;
        if *count == 0 {
            map.remove(key);
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let mut counts = lock(&self.limiter.counts);
        counts.total -= 1;
        release(&mut counts.per_ip, &self.ip);
        release(&mut counts.per_listener, &self.listener);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, std::collections::BTreeMap};

    const PLAINTEXT: AcceptingListener<'static> = AcceptingListener {
        name: "PLAINTEXT",
        protected: false,
    };

    const A: &str = "10.0.0.1";

    fn ip(address: &str) -> IpAddr { address.parse().unwrap() }

    /// Quotas with every cap at `usize::MAX` and no rate, before `customise`.
    fn quotas(customise: impl FnOnce(&mut ConnectionQuotas)) -> ConnectionQuotas {
        let mut quotas = ConnectionQuotas {
            settings: ConnectionQuotaSettings {
                max_connections: usize::MAX,
                max_connections_per_ip: usize::MAX,
                per_ip_overrides: BTreeMap::new(),
                listener_max_connections: usize::MAX,
                max_connection_creation_rate: None,
                listener_max_connection_creation_rate: None,
            },
            per_ip_overrides: Arc::default(),
        };
        customise(&mut quotas);
        quotas
    }

    impl ConnectionLimiter {
        fn per_ip_count(&self, ip: IpAddr) -> usize {
            lock(&self.counts).per_ip.get(&ip).copied().unwrap_or(0)
        }

        fn listener_count(&self, name: &str) -> usize {
            lock(&self.counts)
                .per_listener
                .get(name)
                .copied()
                .unwrap_or(0)
        }
    }

    #[test]
    fn a_guard_counts_and_releases_the_total_the_address_and_the_listener() {
        let limiter = ConnectionLimiter::new(usize::MAX, usize::MAX);
        let unlimited = quotas(|_| {});
        let first = limiter
            .try_acquire_within(ip(A), PLAINTEXT, &unlimited)
            .unwrap();
        let second = limiter
            .try_acquire_within(ip(A), PLAINTEXT, &unlimited)
            .unwrap();
        check!(
            (
                limiter.total(),
                limiter.per_ip_count(ip(A)),
                limiter.listener_count("PLAINTEXT")
            ) == (2, 2, 2)
        );
        drop((first, second));
        check!(limiter.total() == 0);
        check!(lock(&limiter.counts).per_ip.is_empty());
        check!(lock(&limiter.counts).per_listener.is_empty());
    }

    #[test]
    fn a_v4_mapped_peer_meets_its_v4_override() {
        let quotas = quotas(|q| q.per_ip_overrides = Arc::new(HashMap::from([(ip(A), 0)])));
        assert2::assert!(quotas.max_connections_for(ip("::ffff:10.0.0.1")) == 0);
    }

    use rstest_bdd_macros::{given, then, when};

    /// A listener's connection counts under the quotas a scenario sets, and
    /// the connections its clients hold.
    struct ConnectionQuotaScenario {
        limiter: ConnectionLimiter,
        quotas: ConnectionQuotas,
        held: Vec<ConnectionGuard>,
        refusal: krabka_bdd::Given<Option<ConnectionLimit>>,
    }

    impl ConnectionQuotaScenario {
        /// One connection from `address` on `listener`, under the scenario's
        /// quotas. An accepted connection is held until the scenario ends.
        fn connect(&mut self, address: &str, listener: AcceptingListener<'_>) {
            let accepted = self
                .limiter
                .try_acquire_within(ip(address), listener, &self.quotas);
            self.refusal.set(accepted.as_ref().err().copied());
            self.held.extend(accepted.ok());
        }

        /// The refusal the last connection met, and the connections the
        /// broker counts after it: a refused connection reserves nothing.
        fn assert_outcome(&self, refusal: Option<ConnectionLimit>) {
            assert2::assert!(
                (*self.refusal.get(), self.limiter.total()) == (refusal, self.held.len())
            );
        }
    }

    #[rstest::fixture]
    fn connection_quotas() -> ConnectionQuotaScenario {
        ConnectionQuotaScenario {
            limiter: ConnectionLimiter::new(usize::MAX, usize::MAX),
            quotas: quotas(|_| {}),
            held: Vec::new(),
            refusal: krabka_bdd::Given::new(
                "refusal",
                "When a client at <address> connects on listener <listener>",
            ),
        }
    }

    #[given("max.connections is {cap}")]
    fn max_connections(connection_quotas: &mut ConnectionQuotaScenario, cap: usize) {
        connection_quotas.quotas.settings.max_connections = cap;
    }

    #[given("the listener max.connections is {cap}")]
    fn listener_max_connections(connection_quotas: &mut ConnectionQuotaScenario, cap: usize) {
        connection_quotas.quotas.settings.listener_max_connections = cap;
    }

    #[given("max.connections.per.ip is {cap}")]
    fn max_connections_per_ip(connection_quotas: &mut ConnectionQuotaScenario, cap: usize) {
        connection_quotas.quotas.settings.max_connections_per_ip = cap;
    }

    #[given("max.connections.per.ip.overrides gives {address} a cap of {cap}")]
    fn per_ip_override(
        connection_quotas: &mut ConnectionQuotaScenario,
        address: String,
        cap: usize,
    ) {
        let mut overrides = HashMap::clone(&connection_quotas.quotas.per_ip_overrides);
        overrides.insert(ip(&address), cap);
        connection_quotas.quotas.per_ip_overrides = Arc::new(overrides);
    }

    #[given("a client at {address} holds a connection on listener {listener}")]
    fn held_connection(
        connection_quotas: &mut ConnectionQuotaScenario,
        address: String,
        listener: String,
    ) {
        // The held connection is accepted whatever the scenario's caps say.
        let held = connection_quotas
            .limiter
            .try_acquire_within(
                ip(&address),
                AcceptingListener {
                    name: &listener,
                    protected: false,
                },
                &quotas(|_| {}),
            )
            .expect("an unlimited listener accepts the connection");
        connection_quotas.held.push(held);
    }

    #[when("a client at {address} connects on listener {listener}")]
    fn client_connects(
        connection_quotas: &mut ConnectionQuotaScenario,
        address: String,
        listener: String,
    ) {
        connection_quotas.connect(&address, AcceptingListener {
            name: &listener,
            protected: false,
        });
    }

    #[when("a client at {address} connects on the inter-broker listener {listener}")]
    fn client_connects_inter_broker(
        connection_quotas: &mut ConnectionQuotaScenario,
        address: String,
        listener: String,
    ) {
        connection_quotas.connect(&address, AcceptingListener {
            name: &listener,
            protected: true,
        });
    }

    #[then("the connection is accepted")]
    fn connection_accepted(connection_quotas: &ConnectionQuotaScenario) {
        connection_quotas.assert_outcome(None);
    }

    #[then("the connection is refused by max.connections")]
    fn refused_by_max_connections(connection_quotas: &ConnectionQuotaScenario) {
        connection_quotas.assert_outcome(Some(ConnectionLimit::Global));
    }

    #[then("the connection is refused by the listener max.connections")]
    fn refused_by_listener_max_connections(connection_quotas: &ConnectionQuotaScenario) {
        connection_quotas.assert_outcome(Some(ConnectionLimit::Listener));
    }

    #[then("the connection is refused by max.connections.per.ip")]
    fn refused_by_max_connections_per_ip(connection_quotas: &ConnectionQuotaScenario) {
        connection_quotas.assert_outcome(Some(ConnectionLimit::PerIp));
    }

    krabka_bdd::scenarios!(
        "tests/features/connection_quotas",
        fixtures = [connection_quotas: ConnectionQuotaScenario],
    );
}
