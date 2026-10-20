# Topic-backed RLMM partition not ready

**Alert:** `KrabkaRlmmPartitionNotReady`, `krabka_broker_tiered_storage_rlmm_partition_ready == 0 and (krabka_broker_tiered_storage_rlmm_partition_target_offset == -1 or changes(krabka_broker_tiered_storage_rlmm_partition_applied_offset[5m]) == 0)` for 5m.

## What it means

Each broker consumes the `__remote_log_metadata` partitions that cover the
topic partitions it leads or follows. When the broker is assigned a metadata
partition, it reads that partition's high-water mark. Until its consumer has
applied every event up to that mark, the broker answers reads for the
partition with the retriable `NotReady`. Catch-up normally takes seconds.

The alert fires when one assigned metadata partition is not ready and one of
these is also true:

- The high-water mark has been unknown (`target_offset` is -1) for 5 minutes,
  because the `ListOffsets` call that reads it keeps failing.
- The consumer has applied no new event for 10 minutes: a 5-minute window
  with no change, held for 5 minutes.

A partition that is catching up moves its applied offset on each reconcile,
so a slow replay after a restart or a new assignment does not fire the alert.

While a metadata partition is not ready, these stop for every topic partition
that maps to it:

- Remote retention, remote copy, and local retention. Each pass fails with
  `remote log metadata partition N not ready (assigned but not caught up)`,
  so local disk keeps segments that it would otherwise release.
- `Fetch` for offsets in the remote band. The broker answers
  `REPLICA_NOT_AVAILABLE`, which the client retries, so the consumer of cold
  history makes no progress.

Produce and reads of local offsets are not affected until the disk fills.

## Confirm

Find the stuck partitions and compare their two offsets:

```
krabka_broker_tiered_storage_rlmm_partition_ready == 0
krabka_broker_tiered_storage_rlmm_partition_target_offset
krabka_broker_tiered_storage_rlmm_partition_applied_offset
```

The broker log repeats a `WARN` line with the `NotReady` message above for the
same `partition`. The reconciler samples the gauges after each reconcile, on
each assignment change and each `rlmm_reconcile_tick` (30s by default). If you
set the tick above 5 minutes, the applied offset can look flat while the
consumer moves.

## Diagnose

1. **Target is -1.** The broker cannot read the metadata topic's high-water
   marks. Look in the log for `topic-based RLMM: high_water_marks fetch
   failed`. Check that `__remote_log_metadata` has a leader for every
   partition (`kafka-topics --describe --topic __remote_log_metadata`) and
   that the `bootstrap` listener and credentials in
   `[remote_storage.kafka_metadata]` still work. Each reconcile tries the
   fetch again, so the partition recovers by itself when the cause is gone.
2. **Applied is flat below `target_offset - 1`.** The consumer has stopped.
   Look in the log for `metadata consumer:` lines with the same `partition`:
   `fetch failed; retrying`, `metadata refresh failed`, or `partition leader is
   unavailable`. A leader that is offline or a listener that refuses the
   broker's credentials stops the consumer while the target stays fixed.
3. **Applied moves, but the alert fired.** This should not happen, because
   any change in the applied offset in the 10 minute window clears the
   condition. Check `rlmm_reconcile_tick` (see Confirm), and check that the
   scrape is not missing samples.
4. **No log errors and applied is flat.** Applied stops one or more offsets
   below `target_offset - 1` and the consumer reports no error. Record the two
   offsets and the last offset of the partition from `kafka-get-offsets`, and
   add them to
   [krabka-io/krabka-broker#1542](https://github.com/krabka-io/krabka-broker/issues/1542).

## Fix

- Fix the metadata topic's leadership, the listener, or the credentials. The
  consumer and the reconciler retry, and no restart is needed.
- If the partition stays stuck with no errors, restart the broker. The
  restart starts a new consumer from the on-disk snapshot and reads the
  high-water marks again. Before the restart, record the gauge values and
  the log for #1542.
