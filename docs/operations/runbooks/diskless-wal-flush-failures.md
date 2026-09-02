# Diskless WAL flush and cold-read failures

**Alerts:** `KrabkaDisklessWalFlushFailures`, `rate(krabka_broker_diskless_wal_flush_failures_total[5m]) > 0` for 5m;
`KrabkaDisklessWalColdReadErrors`, `rate(krabka_broker_diskless_wal_cold_read_errors_total[5m]) > 0` for 5m.

## What it means

The object store behind the diskless WAL is failing. A flush failure is a
WAL object the flusher could not write. The voters' local WAL directories keep
the records, so nothing acknowledged is lost. The flusher cannot trim behind
an object it did not write, so `krabka_broker_diskless_wal_trim_frontier`
stops moving and the local WAL grows on every voter until a flush succeeds.

A cold-read error is a read of an offset below the trim frontier that failed
in the object store. The consumer that asked gets an error and retries. A
cold-read miss is different: the committed index has no entry for the offset,
which happens for an offset that was never flushed or was deleted by
retention.

## Confirm

`krabka_broker_diskless_wal_flush_attempts_total` beside the failure counter
gives the failure ratio. `krabka_broker_diskless_wal_flush_bytes_total` stops
climbing during an outage.
`krabka_broker_diskless_wal_durable_watermark` minus
`krabka_broker_diskless_wal_trim_frontier` is the hot tail in offsets.

## Diagnose

1. The broker log records each failure with the object store's error. An
   authentication or permission error is a credential problem; a timeout is a
   network or a store problem.
2. Check the `[remote_storage]` section. The S3 or GCS backend and its
   credentials are shared with KIP-405 tiered storage, so
   `krabka_broker_tiered_storage_rlmm_bootstrap_attempts_total` often climbs
   at the same time.
3. Check `krabka_broker_diskless_wal_index_projection_lag`. Objects that were
   written but whose index records did not commit show as durable offsets the
   index does not cover. That is the index topic, not the object store.

## Fix

- Restore the object store or its credentials. The flusher retries on its
  own and the trim frontier moves again.
- Watch local disk on the voters while the store is down. The WAL grows at
  the ingest rate, and a voter that fills its disk stops acknowledging, which
  turns this into [quorum loss](diskless-wal-quorum-loss.md).
- Cold-read errors with a healthy store are an object that was deleted out
  from under the index. Check the bucket's lifecycle rules.
