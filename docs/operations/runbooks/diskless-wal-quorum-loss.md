# Diskless WAL quorum loss

**Alert:** `KrabkaDisklessWalQuorumLoss`, `rate(krabka_broker_diskless_wal_quorum_loss_events_total[5m]) > 0`.

## What it means

A leader of a `krabka.diskless=true` partition tried to acknowledge an append
and could not gather a quorum of WAL voters. The append is not durable and
the `acks=all` produce fails. Each event is one append that could not be
acknowledged. A diskless partition places its WAL on
`diskless_wal_local_replica_count` voters (default 3) across racks and needs a
majority of them.

## Confirm

`krabka_broker_diskless_wal_voter_lag{topic_id, partition, voter}` shows each
voter's distance from the leader log end. A voter whose series disappears left
the quorum. `krabka_broker_topic_failed_produce_requests_total` climbs on the
topic.

## Diagnose

1. Find the shard. The `topic_id` label is the topic's UUID;
   `kafka-topics --describe` prints it beside the name.
2. Check which voters are alive. A voter that is down shows no
   `krabka_broker_partitions_total` series. A voter that is up but lagging
   shows a rising `krabka_broker_diskless_wal_voter_lag`.
3. Check the inter-broker path between the leader and each voter. The WAL
   replication runs over the inter-broker listener.
4. Check disk on the voters. A voter writes the WAL under
   `__diskless_wal_quorum` in its log directory and cannot acknowledge when
   the disk is full.

## Fix

- Restart the failed voters. The quorum reforms and the next append
  succeeds. Producers retry on their own.
- Free disk on a voter that is out of space.
- With fewer live racks than `diskless_wal_local_replica_count`, the placement
  cannot form a full voter set. Bring a rack back, or lower the count on every
  broker and roll.

## Escalate

Quorum loss with every voter alive and unlagged is a bug. Capture the leader's
log at `RUST_LOG=krabka_broker::diskless=debug` and open an issue.
