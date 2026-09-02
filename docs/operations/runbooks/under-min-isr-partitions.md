# Partitions below min.insync.replicas

**Alert:** `KrabkaUnderMinIsrPartitions`, `krabka_broker_under_min_isr_partition_count > 0` for 1m.

## What it means

A partition this broker leads has fewer ISR members than the topic's
`min.insync.replicas`. The broker rejects every `acks=all` produce to it with
`NOT_ENOUGH_REPLICAS`. Producers with `acks=1` or `acks=0` still write, and
those writes have no replication guarantee until the ISR recovers. Reads are
not affected.

This alert always comes with
[under-replicated partitions](under-replicated-partitions.md). It is the
stronger signal, because writes are blocked and clients are retrying.

## Confirm

```
kafka-topics --bootstrap-server <broker>:9092 --describe --under-min-isr-partitions
```

`rate(krabka_broker_topic_failed_produce_requests_total{topic="<topic>"}[1m])`
climbs on the same topic while clients retry.

## Diagnose

Follow the diagnosis in
[under-replicated partitions](under-replicated-partitions.md). More than one
replica is missing, or the topic runs with `min.insync.replicas` equal to its
replication factor, which gives it no headroom.

## Fix

- Bring the missing followers back. The block lifts as soon as the ISR
  reaches `min.insync.replicas` again.
- If a follower cannot return soon, reassign the partition to a live broker
  with `kafka-reassign-partitions`. The new replica joins the ISR after it
  catches up.
- Lowering `min.insync.replicas` on the topic unblocks producers at once. Do
  it only with the consent of the topic's owner. It reduces the durability the
  owner asked for, and every write accepted in the meantime has that lower
  guarantee.

## Escalate

If the missing replicas are on brokers that are up and reachable, and the
partition still shows them out of the ISR after `replica_lag_time_max`,
capture the leader's log at `RUST_LOG=krabka_broker=debug` and open an issue.
