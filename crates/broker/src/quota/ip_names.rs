//! Which `ip` quota entity names stand for which peer address.
//!
//! Kafka resolves the name of an `ip` entity to an `InetAddress` when it
//! applies the quota record (`ClientQuotaMetadataManager.handleIpQuota` calls
//! `InetAddress.getByName`), and enforces the quota on every connection whose
//! peer address equals the result: `SocketServer` keeps `connectionRatePerIp`
//! keyed by `InetAddress` and reads it with `socket.getInetAddress`. So
//! `localhost`, `127.1`, `0:0:0:0:0:0:0:1` and an upper-case IPv6 spelling all
//! enforce, and an IPv4-mapped peer is an `Inet4Address` (#1214).
//!
//! `AlterClientQuotas` stores the name as the operator wrote it, and
//! `DescribeClientQuotas` reports it back that way, so the image keeps
//! `localhost` and not the address it resolved to. This module is the
//! resolution Kafka does at apply time: an IP literal in any spelling
//! resolves on the spot, and a host name resolves once, off the request path,
//! when the quota refresh task first sees it.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    net::IpAddr,
    sync::{Mutex, MutexGuard, PoisonError},
};

use krabka_metadata::MetadataImage;

/// The address an IP literal names, with an IPv4-mapped IPv6 address as the
/// IPv4 address it maps to, as Java's `InetAddress.getByName` returns it.
/// `None` for anything that is not a literal, such as a host name.
///
/// A bracketed IPv6 literal such as `[::1]` is a literal, as it is to Java,
/// and so is an IPv4 address of one to three parts, such as `127.1`, which
/// Java reads the way `inet_aton` does. Reading them here and not through the
/// resolver keeps the answer the same on every platform: Windows' resolver
/// refuses `127.1`.
#[must_use]
pub fn parse_ip_literal(name: &str) -> Option<IpAddr> {
    let bare = name
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(name);
    bare.parse::<IpAddr>()
        .ok()
        .or_else(|| short_ipv4(bare).map(IpAddr::V4))
        .map(|addr| addr.to_canonical())
}

/// An IPv4 address of one to four decimal parts, as Java's
/// `IPAddressUtil.textToNumericFormatV4` reads it: the last part fills the
/// bytes that the parts before it leave, so `127.1` is `127.0.0.1` and a lone
/// `2130706433` is too.
fn short_ipv4(text: &str) -> Option<std::net::Ipv4Addr> {
    let parts = text
        .split('.')
        .map(|part| {
            (!part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
                .then(|| part.parse::<u64>().ok())
                .flatten()
        })
        .collect::<Option<Vec<u64>>>()?;
    let (last, leading) = parts.split_last()?;
    if leading.len() > 3 || leading.iter().any(|part| *part > 0xff) {
        return None;
    }
    // Each leading part is one byte; the last part fills what is left.
    let last_bytes = 4 - leading.len();
    if *last >> (8 * last_bytes) != 0 {
        return None;
    }
    let address = leading
        .iter()
        .fold(0_u64, |address, part| (address << 8) | part)
        << (8 * last_bytes)
        | last;
    u32::try_from(address).ok().map(std::net::Ipv4Addr::from)
}

/// The resolved host names of the image's `ip` entities, and the entity names
/// that stand for each address.
#[derive(Debug, Default)]
pub struct IpNames {
    inner: Mutex<Resolved>,
}

#[derive(Debug, Default)]
struct Resolved {
    /// Host names resolved so far, each with the address it resolved to.
    hosts: HashMap<String, IpAddr>,
    /// The `ip` entity names of the last image that stand for each address.
    names: HashMap<IpAddr, BTreeSet<String>>,
}

impl IpNames {
    fn lock(&self) -> MutexGuard<'_, Resolved> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The entity names of the last [`Self::update`] that stand for `peer`, in
    /// name order. `peer` is canonical.
    #[must_use]
    pub(super) fn entity_names(&self, peer: IpAddr) -> Vec<String> {
        self.lock()
            .names
            .get(&peer)
            .map(|names| names.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Rebuilds the address index from `image`'s `ip` entities, and forgets
    /// the hosts whose entity is gone, so that a record applied again resolves
    /// again, as Kafka resolves it on every apply. Returns the host names of
    /// the image that have no address yet, for [`Self::resolve`].
    pub(super) fn update(&self, image: &MetadataImage) -> Vec<String> {
        let mut resolved = self.lock();
        let mut by_address: HashMap<IpAddr, BTreeSet<String>> = HashMap::new();
        let mut in_image = HashSet::new();
        let mut unresolved = Vec::new();
        for key in image.client_quotas().keys() {
            let [(entity_type, Some(name))] = key.as_slice() else {
                continue;
            };
            if entity_type != "ip" {
                continue;
            }
            in_image.insert(name.as_str());
            match parse_ip_literal(name).or_else(|| resolved.hosts.get(name).copied()) {
                Some(addr) => {
                    by_address.entry(addr).or_default().insert(name.clone());
                }
                None => unresolved.push(name.clone()),
            }
        }
        resolved
            .hosts
            .retain(|host, _| in_image.contains(host.as_str()));
        resolved.names = by_address;
        unresolved
    }

    /// Resolves each host name to the first address the resolver returns, as
    /// `InetAddress.getByName` does, and remembers it for the next
    /// [`Self::update`]. A name that does not resolve is left out and is tried
    /// again on the next image.
    pub(super) async fn resolve(&self, hosts: &[String]) {
        for host in hosts {
            let Ok(addrs) = krabka_client_core::transport::resolve(&format!("{host}:0")).await
            else {
                continue;
            };
            if let Some(addr) = addrs.first() {
                self.lock()
                    .hosts
                    .insert(host.clone(), addr.ip().to_canonical());
            }
        }
    }

    /// Records that `host` resolved to `addr`, for a test that needs a host
    /// name to stand for an address without asking a resolver.
    #[cfg(test)]
    pub(super) fn insert_host(&self, host: &str, addr: IpAddr) {
        self.lock().hosts.insert(host.to_owned(), addr);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::quota::test_support::{image_with_quota, image_with_quotas, quota_record};

    /// An IP literal names one address whatever its spelling, and an
    /// IPv4-mapped IPv6 address is the IPv4 address it maps to.
    #[test]
    fn a_literal_is_the_canonical_address_of_any_spelling() {
        let v4 = |a: u8, b: u8, c: u8, d: u8| Some(IpAddr::from([a, b, c, d]));
        let cases = [
            ("127.0.0.1", v4(127, 0, 0, 1)),
            ("::1", Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]))),
            (
                "0:0:0:0:0:0:0:1",
                Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1])),
            ),
            ("[::1]", Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]))),
            (
                "2001:DB8::A",
                Some(IpAddr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0xa])),
            ),
            ("::ffff:10.1.2.3", v4(10, 1, 2, 3)),
            ("::FFFF:A01:203", v4(10, 1, 2, 3)),
            // Java reads one to three parts the way `inet_aton` does.
            ("127.1", v4(127, 0, 0, 1)),
            ("10.1.515", v4(10, 1, 2, 3)),
            ("2130706433", v4(127, 0, 0, 1)),
            ("10.0.0.255", v4(10, 0, 0, 255)),
            ("1.16777215", v4(1, 255, 255, 255)),
            ("1.16777216", None),
            ("256.1", None),
            ("10.1.65536", None),
            ("4294967296", None),
            ("1.2.3.4.5", None),
            ("1..2", None),
            ("1.2.", None),
            ("-1", None),
            ("localhost", None),
            ("kafka.example.com", None),
            ("", None),
        ];
        for (name, want) in cases {
            check!(parse_ip_literal(name) == want, "{name:?}");
        }
    }

    /// The index maps each address to the entity names that stand for it, and
    /// a host name stands for its address once it has resolved.
    #[test]
    fn update_indexes_literals_and_resolved_hosts() {
        let names = IpNames::default();
        let image = image_with_quotas(vec![
            quota_record(
                vec![("ip", Some("0:0:0:0:0:0:0:1"))],
                "connection_creation_rate",
                1.0,
            ),
            quota_record(vec![("ip", Some("::1"))], "connection_creation_rate", 2.0),
            quota_record(vec![("ip", Some("db"))], "connection_creation_rate", 3.0),
            quota_record(vec![("ip", None)], "connection_creation_rate", 4.0),
            quota_record(vec![("user", Some("alice"))], "producer_byte_rate", 5.0),
        ]);
        let localhost = IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]);
        let db = IpAddr::from([10, 0, 0, 9]);

        let unresolved = names.update(&image);
        names.insert_host("db", db);
        let after_resolution = names.update(&image);

        check!(
            (
                unresolved,
                after_resolution,
                names.entity_names(localhost),
                names.entity_names(db),
                names.entity_names(IpAddr::from([10, 0, 0, 1])),
            ) == (
                vec!["db".to_owned()],
                Vec::<String>::new(),
                vec!["0:0:0:0:0:0:0:1".to_owned(), "::1".to_owned()],
                vec!["db".to_owned()],
                Vec::<String>::new(),
            )
        );
    }

    /// A host whose entity is gone is forgotten, so a record applied again
    /// resolves again.
    #[test]
    fn update_forgets_a_host_whose_entity_is_gone() {
        let names = IpNames::default();
        let with_host = image_with_quota(vec![("ip", Some("db"))], "connection_creation_rate", 1.0);
        let without =
            image_with_quota(vec![("ip", Some("other"))], "connection_creation_rate", 1.0);
        let db = IpAddr::from([10, 0, 0, 9]);

        names.insert_host("db", db);
        let _ = names.update(&without);
        let unresolved_again = names.update(&with_host);

        check!((unresolved_again, names.entity_names(db)) == (vec!["db".to_owned()], vec![]));
    }

    /// A host name resolves through the system resolver to an address the
    /// resolver itself lists for it. `localhost` is in every hosts file.
    #[tokio::test]
    async fn resolve_maps_a_host_name_to_an_address_the_resolver_returns() {
        let names = IpNames::default();
        let image = image_with_quota(
            vec![("ip", Some("localhost"))],
            "connection_creation_rate",
            1.0,
        );
        let unresolved = names.update(&image);
        names.resolve(&unresolved).await;
        let unresolved_after = names.update(&image);

        let listed: Vec<IpAddr> = tokio::net::lookup_host("localhost:0")
            .await
            .expect("localhost resolves")
            .map(|addr| addr.ip().to_canonical())
            .collect();
        check!(unresolved == vec!["localhost".to_owned()]);
        check!(unresolved_after.is_empty());
        check!(
            listed
                .iter()
                .any(|addr| names.entity_names(*addr) == vec!["localhost".to_owned()])
        );
    }
}
