# Diskless WAL voter lag

**Alert:** `KrabkaDisklessWalVoterLag`, `krabka_broker_diskless_wal_voter_lag > 10000` for 10m.

The metric's comment names no threshold. Ten thousand offsets for ten minutes
is a starting point; a shard with a high produce rate needs a larger number.

## What it means

One WAL voter's durable offset trails the leader's log end by more than the
threshold. The quorum still forms as long as a majority keeps up, so produces
succeed. The lagging voter is not a safe copy of the tail, and a second
failure turns this into [quorum loss](diskless-wal-quorum-loss.md).

## Confirm

The labels name the shard and the voter. The other voters' series for the
same shard show whether one voter lags or all do. All voters lagging together
is the leader outrunning its own quorum, which the quorum rule prevents;
that shape points at a stale gauge and a leader that stopped reporting.

## Diagnose

1. Check the inter-broker path from the leader to the voter. The voter takes
   the WAL over the inter-broker listener.
2. Check disk write latency on the voter. The voter fsyncs each WAL append
   before it acknowledges.
3. Check CPU on the voter. A voter that also leads many partitions shares its
   handler time. `krabka_broker_partition_cpu_micros_total` on the voter shows
   what else it does.

## Fix

- A voter with a slow disk or a saturated link needs the [capacity](../capacity.md)
  fix for that resource.
- Move leadership of other partitions off an overloaded voter with
  `kafka-leader-election`.

## Escalate

A voter that lags with an idle disk, an idle link and idle CPU is a bug.
Capture both brokers' logs at `RUST_LOG=krabka_broker::diskless=debug` and
open an issue.
