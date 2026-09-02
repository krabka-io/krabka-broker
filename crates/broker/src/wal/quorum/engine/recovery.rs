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
    BatchBytes, WalReplica, read_batches_exact, replica_end_offset,
    replica_io::sync_replica_blocking, replica_start_offset,
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
        true,
    ));
    let recovery_start = recovery_start(replicas, &ends, durable)?;
    let donor_index = quorum_donor(replicas, &ends, majority, recovery_start, durable)?;

    normalize_durable_prefix(replicas, &ends, donor_index, recovery_start, durable)?;
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
    let recovery_start = recovery_start(replicas, &ends, durable)?;
    normalize_durable_prefix(replicas, &ends, source_index, recovery_start, durable)?;
    Ok(durable)
}

fn recovery_start(
    replicas: &[WalReplica],
    ends: &[Offset],
    durable: Offset,
) -> Result<Offset, BrokerError> {
    let start = replicas
        .iter()
        .map(replica_start_offset)
        .max()
        .ok_or_else(|| BrokerError::Replication("wal quorum has no recovery start".into()))?;
    if durable < start || ends.iter().any(|end| *end < start) {
        return Err(BrokerError::Replication(format!(
            "wal quorum cannot reconcile recovery range {}..{}",
            start.0, durable.0
        )));
    }
    Ok(start)
}

fn quorum_donor(
    replicas: &[WalReplica],
    ends: &[Offset],
    majority: usize,
    start: Offset,
    durable: Offset,
) -> Result<usize, BrokerError> {
    for (candidate_index, candidate) in replicas.iter().enumerate() {
        if ends[candidate_index] < durable {
            continue;
        }
        let Ok(candidate_prefix) = read_batches_exact(&candidate.log, start, durable) else {
            continue;
        };
        let supporters = replicas
            .iter()
            .enumerate()
            .filter(|(index, replica)| {
                ends[*index] >= durable
                    && read_batches_exact(&replica.log, start, durable)
                        .is_ok_and(|prefix| same_batches(&prefix, &candidate_prefix))
            })
            .count();
        if supporters >= majority {
            return Ok(candidate_index);
        }
    }
    Err(BrokerError::Replication(format!(
        "wal quorum has no byte-identical majority for durable range {}..{}",
        start.0, durable.0
    )))
}

fn same_batches(left: &[BatchBytes], right: &[BatchBytes]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.base_offset == right.base_offset
                && left.last_offset == right.last_offset
                && left.verbatim.bytes == right.verbatim.bytes
        })
}

fn normalize_durable_prefix(
    replicas: &[WalReplica],
    ends: &[Offset],
    donor_index: usize,
    recovery_start: Offset,
    durable: Offset,
) -> Result<(), BrokerError> {
    let mut current_offsets = Vec::with_capacity(replicas.len());
    for (index, replica) in replicas.iter().enumerate() {
        let retained_end = ends[index].min(durable);
        let donor_prefix =
            read_batches_exact(&replicas[donor_index].log, recovery_start, retained_end)?;
        let matching = read_batches_exact(&replica.log, recovery_start, retained_end)
            .is_ok_and(|prefix| same_batches(&prefix, &donor_prefix));
        replica
            .log
            .lock()
            .truncate_to(if matching { durable } else { recovery_start })?;
        current_offsets.push(if matching {
            retained_end
        } else {
            recovery_start
        });
    }
    for (replica, current) in replicas.iter().zip(current_offsets) {
        let batches = read_batches_exact(&replicas[donor_index].log, current, durable)?;
        sync_replica_blocking(&replica.log, &batches)?;
    }
    Ok(())
}
