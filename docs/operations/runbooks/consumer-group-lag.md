# Consumer group lag

**Alert:** `KrabkaConsumerGroupLag`, `krabka_broker_consumer_group_lag_records > 10000` for 15m.

The metric's comment names no threshold. Ten thousand records for fifteen
minutes is a starting point; a group that reads a busy topic needs a larger
number, and a group that must stay current needs a smaller one. Tune the
rule per group with a `group_id` matcher rather than one number for all.

## What it means

A consumer group's committed offset on one partition trails the partition's
high watermark by more than the threshold. The group is not keeping up, or
it stopped committing. The broker that coordinates the group computes the
value, so the alert names the coordinator's `instance` and the group's
`group_id`, `topic` and `partition`.

The family is sampled every 30 seconds. A series exists only for a
partition the group committed an offset for. A group that never commits
has no series and never fires this alert.

## Confirm

```
kafka-consumer-groups --bootstrap-server <broker>:9092 --describe --group <group>
```

The `LAG` column is the same subtraction. `CONSUMER-ID` shows whether a
member owns the lagging partition. An empty column is a partition with no
member, which is a rebalance that has not finished or a group with fewer
members than partitions.

## Diagnose

1. Read the same group's other partitions. One partition lagging is a hot
   key or a stuck member. Every partition lagging is a group that is too
   slow for the topic.
2. Compare `rate(krabka_broker_partition_bytes_out_total[5m])` for the
   partition with `rate(krabka_broker_partition_bytes_in_total[5m])`. A read
   rate below the write rate is a consumer that cannot keep up. A read rate
   of zero is a consumer that stopped.
3. Read the group's `Fetch` and `OffsetCommit` rates in
   `krabka_broker_api_requests_total` on the coordinator. Fetches without
   commits is a client that reads and does not commit, which the lag counts
   as unconsumed.
4. Check the consumer's own logs for rebalances. A group that rebalances
   repeatedly makes no progress between them.

## Fix

- A slow group: add members up to the partition count, or add partitions
  and members together.
- A stuck member: restart it. The group rebalances and another member takes
  the partition.
- A client that does not commit: fix the client. Raising the threshold hides
  the gap and does not close it.

## Escalate

A group that lags while its members are idle, its fetches succeed and the
partition's leader is healthy is a bug in the coordinator or the client.
Capture the coordinator's logs at `RUST_LOG=krabka_broker::coordinator=debug`
and open an issue.
