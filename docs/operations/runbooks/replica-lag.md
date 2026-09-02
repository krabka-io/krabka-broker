# Replica lag

**Alert:** `KrabkaReplicaLag`, `krabka_broker_replica_lag_max_records > 10000` for 10m.

The metric's comment names no threshold. Ten thousand records for ten minutes
is a starting point; a partition with a high produce rate needs a larger
number. The rule reads the max rollup, as the comment recommends, so the
alert names the broker and not the follower.

## What it means

A follower of a partition this broker leads trails the leader's log end by
more than the threshold. The follower is still in the ISR, so the partition
is not under-replicated yet. It is drifting toward the ISR shrink that
`replica_lag_time_max` (default 30s) triggers when a fetch stops arriving.
This alert fires before that one, while the fix is still a capacity change
and not a recovery.

Both lag families are sampled every 30 seconds by the leader. A value is at
most one interval old. Shutdown clears the family, so a broker that stopped
sampling reports nothing rather than a stale number.

## Confirm

`krabka_broker_replica_lag_records` on the same broker names the partition and
the follower:

```
topk(10, krabka_broker_replica_lag_records{instance="<broker>"})
```

The `replica` label is the follower's node id. One follower lagging on many
partitions is a slow broker. Many followers lagging on one partition is a hot
partition.

## Diagnose

1. Compare the follower's `rate(krabka_broker_replication_bytes_in_total[1m])`
   for the partition with the leader's
   `rate(krabka_broker_partition_bytes_in_total[1m])`. Inbound that trails
   ingest is a follower that fetches too slowly.
2. Read the follower's `krabka_broker_request_duration_seconds` for `Fetch`
   and its `krabka_broker_partition_cpu_micros_total`. A follower that also
   leads many busy partitions shares its handler time with them.
3. Check disk write latency on the follower. A follower appends each fetched
   batch before it fetches the next.
4. Read `krabka_broker_isr_shrinks_total` and `krabka_broker_isr_expands_total`
   on the leader. A pair that both climb is a follower already at the edge of
   `replica_lag_time_max`.

## Fix

- A follower with a slow disk or a saturated link needs the
  [capacity](../capacity.md) fix for that resource.
- A hot partition: add partitions to the topic, or move leadership of other
  partitions off the leader with `kafka-leader-election`.
- Raise the threshold when the partition's normal produce rate makes ten
  thousand records a few seconds of traffic.

## Escalate

If `krabka_broker_under_replicated_partitions` rises on the same broker,
follow [under-replicated partitions](under-replicated-partitions.md). A
follower that lags with an idle disk, an idle link and idle CPU is a bug.
Capture both brokers' logs at `RUST_LOG=krabka_broker::replication=debug` and
open an issue.
