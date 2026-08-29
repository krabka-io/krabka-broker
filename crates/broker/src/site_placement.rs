//! Replica placement for a stretch cluster. A "site" is the `broker.rack`
//! value of a broker, so two brokers with the same rack are in the same site.
//!
//! The placement gives two properties:
//!
//! - **Site spread.** The replicas of one partition go in different sites. The
//!   loss of a full site then costs one replica, and a producer with
//!   `acks=all` and `min.insync.replicas=2` continues to commit.
//! - **Leadership pinning.** In Kafka, `replicas[0]` *is* the preferred leader
//!   of the partition. The order of the replica list is thus the full
//!   mechanism that pins leadership to a site. This module puts a broker of
//!   the preferred site first. The KIP-460 auto-rebalance in
//!   [`crate::leader_rebalance`] and `kafka-leader-election --election-type
//!   preferred` then keep the leader there. A leader in the site of the
//!   producers removes one inter-site trip from the write path.
//!
//! A witness is a data-bearing node in a third, cheap site. It replicates the
//! partition data and it votes in `KRaft`, but it does not serve clients. Thus
//! a witness is a replica, but it is never `replicas[0]`.
//!
//! The module is pure. It does no I/O, it reads no clock, and the same input
//! always gives the same output: it sorts the brokers by node id and it never
//! iterates a hash container.
//!
//! Like the WAL voter placement in `crate::wal::quorum::placement`, this code
//! fails closed. When it cannot satisfy the site guarantee, it returns an
//! empty outer vec, and the caller reports `INVALID_REPLICATION_FACTOR`.

use krabka_raft::NodeId;

use crate::handlers::create_topics::round_robin_replicas;

/// One broker as the placement code sees it.
#[derive(Debug, Clone)]
pub(crate) struct SiteBrokerView {
    /// The node id of the broker.
    pub node_id: NodeId,
    /// The broker's configured rack, which is its site. `None` means the
    /// broker declared no site.
    pub site: Option<String>,
    /// A witness replicates data but never leads, so it is never `replicas[0]`.
    pub is_witness: bool,
}

/// One placement decision: a broker and the site that holds it. Both values
/// are indexes, into the node-id-sorted broker slice and into the site table.
#[derive(Debug, Clone, Copy)]
struct Placed {
    site: usize,
    broker: usize,
}

/// The brokers of the cluster, grouped by site.
struct SiteTable {
    /// One entry per site, in the order of the first broker of that site.
    /// Each entry holds the indexes of the brokers of the site, in node-id
    /// order.
    sites: Vec<Vec<usize>>,
    /// The site of each broker, in node-id order. `None` marks a broker that
    /// declared no site.
    site_of: Vec<Option<usize>>,
}

impl SiteTable {
    /// Groups the sorted brokers by site.
    fn new(sorted: &[&SiteBrokerView]) -> Self {
        let mut names: Vec<&str> = Vec::new();
        let mut sites: Vec<Vec<usize>> = Vec::new();
        let mut site_of: Vec<Option<usize>> = Vec::new();
        for (index, broker) in sorted.iter().enumerate() {
            let Some(name) = broker.site.as_deref() else {
                site_of.push(None);
                continue;
            };
            let known = names.iter().position(|candidate| *candidate == name);
            let site = if let Some(site) = known {
                site
            } else {
                names.push(name);
                sites.push(Vec::new());
                sites.len() - 1
            };
            sites[site].push(index);
            site_of.push(Some(site));
        }
        Self { sites, site_of }
    }
}

/// Places the replicas of `num_partitions` partitions across the sites of
/// `brokers`.
///
/// The result holds one replica list per partition, and `replicas[0]` of each
/// list is the preferred leader of that partition. The rules, in priority
/// order:
///
/// 1. When no broker declares a site, the cluster is not a stretch cluster.
///    The result is then the plain Kafka placement,
///    [`round_robin_replicas`] over the same brokers in node-id order. A
///    cluster without a site has no witness site either, so this rule also
///    ignores `is_witness` and `preferred_site`.
/// 2. In every other cluster, the replicas of a partition go in different
///    sites. The rotation of the sites advances with the partition index, so
///    the partitions spread over the sites. Inside a site, the brokers also
///    rotate with the partition index.
/// 3. A `replication_factor` above the site count takes a second broker from
///    a site. The site that holds the fewest replicas of that partition comes
///    first.
/// 4. `replicas[0]` is a non-witness broker of `preferred_site`. When the
///    preferred site has no such broker, or when `preferred_site` is `None`,
///    `replicas[0]` is any non-witness broker.
/// 5. A witness is never `replicas[0]`.
///
/// The result is an empty outer vec, which makes the caller report
/// `INVALID_REPLICATION_FACTOR`, when the request is impossible:
///
/// - `replication_factor` is 0, or it is more than the broker count. This is
///   the guard of [`round_robin_replicas`].
/// - Every broker is a witness, thus no broker can lead. This is a degenerate
///   cluster.
/// - The sites do not hold enough brokers for one partition. A broker that
///   declared no site is not placeable, because the code cannot show that it
///   is in a different site from another such broker.
pub(crate) fn stretch_replicas(
    brokers: &[SiteBrokerView],
    num_partitions: i32,
    replication_factor: i16,
    preferred_site: Option<&str>,
) -> Vec<Vec<NodeId>> {
    let replicas_per_partition = usize::try_from(replication_factor).unwrap_or(0);
    if replicas_per_partition == 0 || replicas_per_partition > brokers.len() {
        return Vec::new();
    }
    let partition_count = usize::try_from(num_partitions).unwrap_or(0);

    let mut sorted = brokers.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|broker| broker.node_id);

    if sorted.iter().all(|broker| broker.site.is_none()) {
        let node_ids = sorted
            .iter()
            .map(|broker| broker.node_id)
            .collect::<Vec<_>>();
        return round_robin_replicas(&node_ids, num_partitions, replication_factor);
    }

    let table = SiteTable::new(&sorted);
    let leaders = leader_candidates(&sorted, &table, preferred_site);
    (0..partition_count)
        .map(|partition| {
            partition_replicas(&sorted, &table, &leaders, partition, replicas_per_partition)
        })
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
}

/// The brokers that can take `replicas[0]`.
///
/// A witness never leads, and a broker without a site is not placeable, so
/// neither is a candidate. The result holds only the brokers of
/// `preferred_site` when that site has at least one candidate. Otherwise it
/// holds every candidate of the cluster.
///
/// The list interleaves the sites: it takes the first candidate of every
/// site, then the second candidate of every site, and so on. Because the
/// leader of a partition comes from this list at the partition index, a topic
/// with fewer partitions than brokers still leads in every site.
fn leader_candidates(
    sorted: &[&SiteBrokerView],
    table: &SiteTable,
    preferred_site: Option<&str>,
) -> Vec<Placed> {
    let mut per_site = vec![Vec::new(); table.sites.len()];
    for (broker, _) in sorted
        .iter()
        .enumerate()
        .filter(|(_, broker)| !broker.is_witness)
    {
        if let Some(site) = table.site_of[broker] {
            per_site[site].push(Placed { site, broker });
        }
    }
    let deepest = per_site.iter().map(Vec::len).max().unwrap_or(0);
    let candidates = (0..deepest)
        .flat_map(|rank| per_site.iter().filter_map(move |site| site.get(rank)))
        .copied()
        .collect::<Vec<_>>();
    let Some(preferred) = preferred_site else {
        return candidates;
    };
    let in_preferred = candidates
        .iter()
        .copied()
        .filter(|placed| sorted[placed.broker].site.as_deref() == Some(preferred))
        .collect::<Vec<_>>();
    if in_preferred.is_empty() {
        candidates
    } else {
        in_preferred
    }
}

/// Selects the replicas of one partition, leader first.
///
/// Returns `None` when the cluster cannot hold the partition: no broker can
/// lead it, or the sites do not hold enough brokers.
fn partition_replicas(
    sorted: &[&SiteBrokerView],
    table: &SiteTable,
    leaders: &[Placed],
    partition: usize,
    replicas_per_partition: usize,
) -> Option<Vec<NodeId>> {
    if leaders.is_empty() {
        return None;
    }
    // The leader rotates with the partition index, so the partitions of a
    // topic do not all lead on one broker of the preferred site.
    let leader = leaders[partition % leaders.len()];
    let mut chosen = vec![leader];
    let mut load = vec![0_usize; table.sites.len()];
    load[leader.site] += 1;
    while chosen.len() < replicas_per_partition {
        let follower = next_follower(table, &load, &chosen, partition)?;
        load[follower.site] += 1;
        chosen.push(follower);
    }
    Some(
        chosen
            .into_iter()
            .map(|placed| sorted[placed.broker].node_id)
            .collect(),
    )
}

/// The next replica of a partition, from the site that holds the fewest
/// replicas of it. Returns `None` when every site is exhausted.
fn next_follower(
    table: &SiteTable,
    load: &[usize],
    chosen: &[Placed],
    partition: usize,
) -> Option<Placed> {
    let site_count = table.sites.len();
    // The rotation start advances with the partition index, so consecutive
    // partitions do not stack on the first site. `min_by_key` keeps the first
    // minimum, thus the rotation is also the tie-break between two sites that
    // hold the same number of replicas.
    (0..site_count)
        .map(|step| (partition + step) % site_count)
        .filter_map(|site| {
            free_broker(table, site, partition, chosen).map(|broker| Placed { site, broker })
        })
        .min_by_key(|placed| load[placed.site])
}

/// The first broker of `site` that this partition does not use yet. The scan
/// starts at an offset that advances with the partition index, so the
/// partitions spread over the brokers of the site.
fn free_broker(
    table: &SiteTable,
    site: usize,
    partition: usize,
    chosen: &[Placed],
) -> Option<usize> {
    let brokers = &table.sites[site];
    (0..brokers.len())
        .map(|step| brokers[(partition + step) % brokers.len()])
        .find(|broker| !chosen.iter().any(|placed| placed.broker == *broker))
}

#[cfg(test)]
mod tests;
