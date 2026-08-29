//! How a shard picks the durable prefix it opens on, and how it makes every
//! replica agree on that prefix before the engine serves a request.
//!
//! Recovery reads the frontier a majority already holds; bootstrap takes the
//! frontier of a named donor. Both then truncate every replica to that offset
//! and copy the donor's bytes back over the shorter ones, so the choice of
//! prefix and its enforcement stay together.

use krabka_ids::Offset;
use krabka_kraft_core::NodeId;

use super::{
    WalReplica, read_batches_exact, replica_end_offset, replica_io::sync_replica_blocking,
};
use crate::error::BrokerError;

pub(super) fn recover_durable_prefix(
    replicas: &[WalReplica],
    majority: usize,
) -> Result<Offset, BrokerError> {
    let ends = replicas.iter().map(replica_end_offset).collect::<Vec<_>>();
    let (donor_index, donor_end) = ends
        .iter()
        .enumerate()
        .max_by_key(|(_, offset)| offset.0)
        .map(|(index, offset)| (index, *offset))
        .ok_or_else(|| BrokerError::Replication("wal quorum has no recovery donor".into()))?;
    let follower_ends = ends
        .iter()
        .enumerate()
        .filter_map(|(index, offset)| (index != donor_index).then_some(offset.0))
        .collect::<Vec<_>>();
    let durable = Offset(krabka_verified::recompute_high_watermark(
        donor_end.0,
        &follower_ends,
        majority,
        -1,
        0,
    ));

    normalize_durable_prefix(replicas, &ends, donor_index, durable)?;
    Ok(durable)
}

pub(super) fn bootstrap_durable_prefix(
    replicas: &[WalReplica],
    source: NodeId,
) -> Result<Offset, BrokerError> {
    let ends = replicas.iter().map(replica_end_offset).collect::<Vec<_>>();
    let source_index = replicas
        .iter()
        .position(|replica| replica.id == source)
        .ok_or_else(|| {
            BrokerError::Replication(format!(
                "wal quorum bootstrap source {} is not a voter",
                source.0
            ))
        })?;
    let durable = ends[source_index];
    normalize_durable_prefix(replicas, &ends, source_index, durable)?;
    Ok(durable)
}

fn normalize_durable_prefix(
    replicas: &[WalReplica],
    ends: &[Offset],
    donor_index: usize,
    durable: Offset,
) -> Result<(), BrokerError> {
    for replica in replicas {
        let mut log = replica.log.lock();
        log.truncate_to(durable)?;
    }
    for (replica, end) in replicas.iter().zip(ends) {
        let batches = read_batches_exact(&replicas[donor_index].log, (*end).min(durable), durable)?;
        sync_replica_blocking(&replica.log, &batches)?;
    }
    Ok(())
}
