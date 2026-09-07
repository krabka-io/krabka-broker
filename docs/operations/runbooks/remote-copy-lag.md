# Remote tier falling behind

**Alerts:** `KrabkaRemoteCopyErrors`
(`rate(krabka_broker_remote_copy_errors_total[5m]) > 0`),
`KrabkaRemoteCopyLagGrowing`
(`deriv(krabka_broker_remote_copy_lag_segments[15m]) > 0` for 30m), and
`KrabkaRemoteFetchErrors`
(`rate(krabka_broker_remote_fetch_errors_total[5m]) > 0`).

## What it means

KIP-405 tiered storage moves a sealed segment to the object store, and only
then may local retention delete it. The three alerts are the three ways that
loop breaks.

- **Copy errors.** A segment copy is failing outright. History stops reaching
  the tier, and because a segment is only evicted locally once it is archived,
  the local disk keeps everything the tier has not taken.
- **Copy lag growing.** Copies still succeed, but the topic produces faster
  than the tier absorbs. `remote_copy_lag_segments` is the count of sealed
  local segments this broker has not finished copying, and
  `remote_copy_lag_bytes` is the disk they hold. A lag that climbs for half an
  hour will not recover on its own.
- **Fetch errors.** The tier will not serve a read. A consumer reading records
  the local log has already evicted is answered `UNKNOWN_SERVER_ERROR` on that
  partition, which the client retries with its position intact, so the history
  is stalled rather than lost. The consumer makes no progress for as long as
  the tier stays broken, and only the partitions whose offsets are cold are
  affected.

Produce and local reads are unaffected in all three cases until the disk
fills.

## Confirm

Copy errors and copy lag are per topic, so start there:

```
topk(5, rate(krabka_broker_remote_copy_errors_total[5m]))
topk(5, krabka_broker_remote_copy_lag_segments)
```

Compare `rate(krabka_broker_remote_copy_requests_total[5m])` with the error
rate: equal rates mean every copy is failing, and a small ratio means the
object store is flaky rather than misconfigured. The broker log carries the
backend's own error on each failed copy.

## Diagnose

1. **The object store.** Copy, fetch and delete all go through the same
   backend, so check whether all three are failing
   (`remote_delete_errors_total` and `remote_fetch_errors_total`) or only one.
   All three failing points at credentials, networking or the bucket; copy
   alone points at a write policy such as object lock or a bucket that is
   full.
2. **A write-once archive.** Under `[remote_storage.worm]` every object is a
   conditional create, so a key that already exists fails the copy. A partial
   copy left behind by an earlier failure is the usual cause; the log names the
   key.
3. **Throughput.** If there are no errors and the lag still climbs, the tier is
   simply slower than the topic. Check
   `rate(krabka_broker_remote_copy_bytes_total[5m])` against
   `rate(krabka_broker_topic_bytes_in_total[5m])` for the same topic. The copy
   task runs one pass per `remote_log_manager_interval` (30 s by default), so a
   partition rolling segments faster than that cannot catch up. A pass copies
   `remote_storage.copier_threads` partitions at a time (10 by default), so a
   broker leading far more tiered partitions than that is uploading a tenth of
   them at any moment however much bandwidth the store has left.
4. **Fetch errors alone.** Read `krabka_broker_remote_log_reader_rejected_total`
   first: a nonzero rate there is the reader pool refusing cold reads because
   its pending queue is full, which is a saturation problem rather than a
   backend one. Then check
   `krabka_broker_remote_index_cache_misses_total` against the hits — a cache
   too small for the working set turns every batch into two or three extra
   object GETs and can push the backend into throttling.

## Fix

- Repair the backend: credentials, network path, bucket policy, quota. The copy
  task retries every tick, so no restart is needed once the cause is gone.
- Under WORM, remove the orphaned objects of the failed copy so the conditional
  create can succeed, or let the segment be re-copied under a fresh segment id;
  the copy task already does the latter for a segment stuck in
  `CopySegmentStarted`.
- For a throughput shortfall, raise `remote_storage.copier_threads`
  (Kafka's `remote.log.manager.copier.thread.pool.size`) so more partitions
  upload at once. That is the knob to reach for when the object store has
  bandwidth to spare and the broker leads many tiered partitions. Lowering
  `remote_log_manager_interval` so copy passes run more often, or raising the
  topic's `segment.bytes` so it rolls fewer, larger segments, help a single
  partition that cannot keep up on its own.
- If the deletes are what lags — remote retention falling behind rather than
  the copy — raise `remote_storage.expiration_threads` (Kafka's
  `remote.log.manager.expiration.thread.pool.size`), which bounds the
  partitions running their retention passes at once the same way.
- For rejected cold reads, raise `remote_storage.reader_threads` and
  `remote_storage.reader_max_pending_tasks`. For a low index-cache hit ratio,
  raise `remote_storage.index_cache_size`.
- Watch the local disk while the lag is high: `krabka_broker_partition_disk_bytes`
  keeps climbing for as long as the tier is behind, because local retention
  cannot evict a segment the tier does not hold.

## Escalate

A copy lag that is still climbing after the backend is healthy, or a fetch
error rate that persists with an empty reader queue and a warm index cache,
means the tier is not the bottleneck. Capture the broker log around one failed
copy — it carries the backend's own error verbatim — and the output of
`krabka-worm-verify` if the archive is write-once.
