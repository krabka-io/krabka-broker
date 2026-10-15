//! The addresses of `max.connections.per.ip.overrides`.
//!
//! Kafka resolves each host with `InetAddress.getByName` when the config
//! changes (`ConnectionQuotas.updateMaxConnectionsPerIpOverride`), not per
//! connection. The cache here does the same: the accept loop resolves a new
//! override set once, and reuses the addresses until the set changes. A set
//! with a host that does not resolve leaves the previous addresses in force,
//! as Kafka's update throws before it replaces them.

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

/// The override set last resolved, and the addresses in force.
#[derive(Debug, Default)]
struct Resolved {
    hosts: BTreeMap<String, usize>,
    addresses: IpOverrides,
}

/// The resolved override addresses, keyed by the host set they came from.
#[derive(Debug, Default)]
pub(super) struct OverrideAddresses {
    resolved: Mutex<Resolved>,
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
    /// The override addresses for `hosts`, resolving them when the set
    /// differs from the last one.
    pub(super) async fn addresses(&self, hosts: &BTreeMap<String, usize>) -> IpOverrides {
        {
            let resolved = self.resolved.lock().unwrap_or_else(PoisonError::into_inner);
            if resolved.hosts == *hosts {
                return Arc::clone(&resolved.addresses);
            }
        }
        let mut addresses = HashMap::with_capacity(hosts.len());
        let mut unresolved = Vec::new();
        for (host, cap) in hosts {
            match resolve_host(host).await {
                Some(address) => {
                    addresses.insert(address, *cap);
                }
                None => unresolved.push(host.as_str()),
            }
        }
        let mut resolved = self.resolved.lock().unwrap_or_else(PoisonError::into_inner);
        resolved.hosts.clone_from(hosts);
        if unresolved.is_empty() {
            resolved.addresses = Arc::new(addresses);
        } else {
            warn!(
                ?unresolved,
                "max.connections.per.ip.overrides names hosts that do not resolve; keeping the \
                 previous overrides"
            );
        }
        Arc::clone(&resolved.addresses)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

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

    #[tokio::test]
    async fn literals_resolve_as_they_are_and_a_v4_mapped_address_canonicalises() {
        let cache = OverrideAddresses::default();
        let got = cache
            .addresses(&hosts(&[
                ("127.0.0.2", 3),
                ("::ffff:10.0.0.1", 1),
                ("::1", 2),
            ]))
            .await;
        assert!(*got == addresses(&[("127.0.0.2", 3), ("10.0.0.1", 1), ("::1", 2)]));
    }

    #[tokio::test]
    async fn localhost_resolves_by_name() {
        let cache = OverrideAddresses::default();
        let got = cache.addresses(&hosts(&[("localhost", 4)])).await;
        assert!(got.values().copied().collect::<Vec<_>>() == vec![4]);
        assert!(got.keys().all(IpAddr::is_loopback));
    }

    #[tokio::test]
    async fn an_unresolvable_set_keeps_the_previous_addresses() {
        let cache = OverrideAddresses::default();
        let first = cache.addresses(&hosts(&[("127.0.0.2", 3)])).await;
        let second = cache
            .addresses(&hosts(&[("no-such-host.invalid", 1)]))
            .await;
        assert!(second == first);
    }
}
