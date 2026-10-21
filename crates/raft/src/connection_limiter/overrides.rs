//! The addresses of `max.connections.per.ip.overrides`.
//!
//! Kafka resolves each host with `InetAddress.getByName` when the config
//! changes (`ConnectionQuotas.updateMaxConnectionsPerIpOverride`), not per
//! connection, and it does so in the reconfiguration, off its acceptor
//! threads. Here a set of address literals applies at once, and a set that
//! names a host is resolved by a task of its own, so an accept loop never
//! waits on DNS: the addresses in force before the change stay in force until
//! the lookup ends. A set with a host that does not resolve leaves the
//! previous addresses in force, as Kafka's update throws before it replaces
//! them.

use {
    std::{
        collections::{BTreeMap, HashMap},
        net::IpAddr,
        sync::{Arc, Mutex, PoisonError},
    },
    tracing::warn,
};

/// The cap of each override address.
pub type IpOverrides = Arc<HashMap<IpAddr, usize>>;

/// The host set last applied, the one being resolved, and the addresses in
/// force.
#[derive(Debug, Default)]
struct State {
    applied: BTreeMap<String, usize>,
    pending: Option<BTreeMap<String, usize>>,
    addresses: IpOverrides,
}

/// The resolved override addresses, keyed by the host set they came from.
#[derive(Debug, Default)]
pub(super) struct OverrideAddresses {
    state: Mutex<State>,
}

/// The addresses of `hosts` when every host is an address literal.
fn literals(hosts: &BTreeMap<String, usize>) -> Option<HashMap<IpAddr, usize>> {
    hosts
        .iter()
        .map(|(host, cap)| Some((host.parse::<IpAddr>().ok()?.to_canonical(), *cap)))
        .collect()
}

/// `InetAddress.getByName`: an address literal as it is, else the first
/// address the name resolves to.
async fn resolve_host(host: &str) -> Option<IpAddr> {
    if let Ok(address) = host.parse::<IpAddr>() {
        return Some(address.to_canonical());
    }
    tokio::net::lookup_host((host, 0))
        .await
        .ok()?
        .next()
        .map(|address| address.ip().to_canonical())
}

impl OverrideAddresses {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The override addresses in force for `hosts`. A new set of literals
    /// applies at once; a new set that names a host starts its lookup and
    /// leaves the previous addresses in force meanwhile. It must be called
    /// within a Tokio runtime.
    pub(super) fn addresses(self: &Arc<Self>, hosts: &BTreeMap<String, usize>) -> IpOverrides {
        let mut state = self.state();
        if state.applied == *hosts || state.pending.as_ref() == Some(hosts) {
            return Arc::clone(&state.addresses);
        }
        if let Some(addresses) = literals(hosts) {
            state.applied.clone_from(hosts);
            state.pending = None;
            state.addresses = Arc::new(addresses);
            return Arc::clone(&state.addresses);
        }
        state.pending = Some(hosts.clone());
        let this = Arc::clone(self);
        let hosts = hosts.clone();
        tokio::spawn(async move { this.resolve(hosts).await });
        Arc::clone(&state.addresses)
    }

    /// Resolve `hosts` and apply them, unless a newer set replaced them
    /// meanwhile.
    async fn resolve(&self, hosts: BTreeMap<String, usize>) {
        let mut addresses = HashMap::with_capacity(hosts.len());
        let mut unresolved = Vec::new();
        for (host, cap) in &hosts {
            match resolve_host(host).await {
                Some(address) => {
                    addresses.insert(address, *cap);
                }
                None => unresolved.push(host.clone()),
            }
        }
        let mut state = self.state();
        if state.pending.as_ref() != Some(&hosts) {
            return;
        }
        state.pending = None;
        state.applied = hosts;
        if unresolved.is_empty() {
            state.addresses = Arc::new(addresses);
        } else {
            warn!(
                ?unresolved,
                "max.connections.per.ip.overrides names hosts that do not resolve; keeping the \
                 previous overrides"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        std::time::{Duration, Instant},
    };

    fn hosts(entries: &[(&str, usize)]) -> BTreeMap<String, usize> {
        entries
            .iter()
            .map(|(host, cap)| ((*host).to_owned(), *cap))
            .collect()
    }

    fn addresses(entries: &[(&str, usize)]) -> HashMap<IpAddr, usize> {
        entries
            .iter()
            .map(|(address, cap)| (address.parse().unwrap(), *cap))
            .collect()
    }

    /// Poll until the lookup `hosts` started has been applied.
    async fn applied(cache: &Arc<OverrideAddresses>, hosts: &BTreeMap<String, usize>) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cache.state().applied != *hosts {
            assert!(Instant::now() < deadline, "the lookup never finished");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn literals_apply_at_once_and_a_v4_mapped_address_canonicalises() {
        let cache = Arc::new(OverrideAddresses::default());
        let got = cache.addresses(&hosts(&[
            ("127.0.0.2", 3),
            ("::ffff:10.0.0.1", 1),
            ("::1", 2),
        ]));
        assert!(*got == addresses(&[("127.0.0.2", 3), ("10.0.0.1", 1), ("::1", 2)]));
    }

    #[tokio::test]
    async fn a_host_name_resolves_in_the_background_and_the_previous_set_holds_meanwhile() {
        let cache = Arc::new(OverrideAddresses::default());
        let previous = cache.addresses(&hosts(&[("127.0.0.2", 3)]));
        let named = hosts(&[("localhost", 4)]);
        assert!(cache.addresses(&named) == previous);
        applied(&cache, &named).await;
        let got = cache.addresses(&named);
        assert!(got.values().copied().collect::<Vec<_>>() == vec![4]);
        assert!(got.keys().all(IpAddr::is_loopback));
    }

    #[tokio::test]
    async fn an_unresolvable_set_keeps_the_previous_addresses() {
        let cache = Arc::new(OverrideAddresses::default());
        let previous = cache.addresses(&hosts(&[("127.0.0.2", 3)]));
        let unresolvable = hosts(&[("no-such-host.invalid", 1)]);
        drop(cache.addresses(&unresolvable));
        applied(&cache, &unresolvable).await;
        assert!(cache.addresses(&unresolvable) == previous);
    }
}
