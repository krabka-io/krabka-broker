# Log cleaner stalled

**Alerts:** `KrabkaLogCleanerFailures`
(`sum by (instance, topic, reason) (rate(krabka_broker_log_cleaner_failures_total[15m])) > 0`
for 15m) and `KrabkaUncleanablePartitions`
(`krabka_broker_log_cleaner_uncleanable_partitions > 0` for 30m).

## What it means

A compaction pass failed on a partition this broker leads, and the partition
has not compacted since. The cleaner keeps sweeping; the partition keeps
failing.

On a compacted topic the cleaner is the only thing that bounds the log.
`cleanup.policy=compact` has no size or time retention behind it, and even
`compact,delete` deletes only whole sealed segments the cleaner has already
rewritten. So compaction and local retention stop together: the partition
grows at the full produce rate, and nothing else on the broker says so. The
disk is the next thing to fail, and it takes every partition on it, not the
one that stalled.

A `reason` of `io` is a storage failure. The broker has already asked its
log-dir registry to take the owning directory offline, so
`KrabkaOfflineLogDirs` should be firing beside this one — read
[offline-log-dir](offline-log-dir.md) first, and treat this rule as the
symptom. `writer` is a partition whose writer actor is gone, which is a broker
bug. `other` is the log layer refusing the rewrite, usually a segment it
cannot read.

## Confirm

```
sum by (topic, reason) (rate(krabka_broker_log_cleaner_failures_total[15m]))
krabka_broker_log_cleaner_uncleanable_partitions
rate(krabka_broker_log_cleaner_runs_total[15m])
```

The run counter counts only sweeps that failed no partition, so a flat run
rate beside a positive failure rate means every sweep is failing. A run rate
that is still healthy means one partition is failing and the rest are fine.

`krabka_broker_partition_disk_bytes` for the topic the first query names is
what turns this from a background warning into a deadline: a compacted
partition that climbs on a straight line has no upper bound.

## Diagnose

1. Read the broker log. Every failed pass logs one `compaction failed for
   partition` line with the topic, the partition and the underlying error.
2. For `io`, the error names the file. A dead disk, a filesystem remounted
   read-only, and a full filesystem all read differently, and the third is
   the one that a full disk caused rather than the one that will cause it.
3. For `other`, a `Corrupt` error names the segment the rewrite could not
   read. That segment is unreadable to consumers too; check whether fetches
   of that offset range fail.
4. `krabka_broker_log_compactions_total` for the same partition tells you
   when it last succeeded. A partition that has never compacted since broker
   start was broken before this alert fired.

## Fix

- For `io`, follow [offline-log-dir](offline-log-dir.md). The cleaner
  recovers on its own once writes to the directory succeed again: the next
  sweep compacts the partition, the uncleanable count falls, and the run
  counter resumes.
- For a corrupt segment, move the partition to another broker with
  `kafka-reassign-partitions` and let the new replica build its log from the
  leader. Do not delete segment files under a running broker.
- While the cause is being fixed, buy disk headroom by lowering the topic's
  `segment.bytes` — smaller segments seal sooner and the first successful
  pass reclaims more — or by moving the largest partitions off the broker.
- `writer` is a broker bug. Collect the log lines around the failure and
  restart the broker; the partition's writer is respawned on start.

## Escalate

A partition that is still uncleanable after the storage fault is fixed, or a
compacted partition whose disk bytes will reach the volume's capacity before
the fix lands, is an outage on a timer. Escalate with the topic, the
partition, `krabka_broker_partition_disk_bytes` for it, and the free space on
the log dir.
