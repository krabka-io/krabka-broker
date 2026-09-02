# Under-replicated partitions

**Alert:** `KrabkaUnderReplicatedPartitions`, `krabka_broker_under_replicated_partitions > 0` for 5m.

## What it means

A partition this broker leads has an ISR that is smaller than its assigned
replica set. A follower stopped fetching, or it fell more than
`replica_lag_time_max` (default 30s) behind and the ISR-maintenance loop
shrank the ISR. The partition still serves reads and writes. It has less
redundancy than the operator asked for, and a second failure can make it
unavailable.

## Confirm

```
kafka-topics --bootstrap-server <broker>:9092 --describe --under-replicated-partitions
```

The output names each partition, its leader, its replicas and its ISR. The
replica missing from the ISR is the follower to look at.

## Diagnose

1. Check whether the missing follower is alive. `krabka_broker_partitions_total`
   for that broker disappears from Prometheus when the process is down.
2. On the leader, compare `rate(krabka_broker_replication_bytes_out_total[1m])`
   for the partition with the same partition's
   `rate(krabka_broker_partition_bytes_in_total[1m])`. Outbound that trails
   ingest is a follower that fetches too slowly.
3. On the follower, read `rate(krabka_broker_replication_bytes_in_total[1m])`.
   A rate of zero while the leader serves other followers is a network or
   authentication failure on the inter-broker listener. Check
   `krabka_broker_failed_authentication_total` on the leader.
4. Read `krabka_broker_isr_shrinks_total` and `krabka_broker_isr_expands_total`
   on the leader. A pair that both climb is a follower that flaps at the edge
   of `replica_lag_time_max`, which is a capacity problem and not an outage.

## Fix

- A dead follower: restart it. The ISR expands on its own after the follower
  catches up. No operator action moves it back.
- A follower that cannot keep up: see [capacity](../capacity.md). Raise
  `replica_lag_time_max` only as a stopgap. It widens the window in which an
  `acks=all` produce can be acknowledged by fewer live copies.
- A follower that will not return: reassign the partition with
  `kafka-reassign-partitions` to a live broker.

## Escalate

If `krabka_broker_under_min_isr_partition_count` also rises, follow
[under min ISR partitions](under-min-isr-partitions.md). That alert is the one
that blocks writes.
