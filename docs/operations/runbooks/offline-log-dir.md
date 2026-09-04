# Offline log directory

**Alert:** `KrabkaOfflineLogDirs`, `krabka_broker_offline_log_dirs > 0` for 1m.

## What it means

The broker has taken one of its configured log directories offline. Two things
do that: the startup writability probe, which creates, fsyncs and removes a
sentinel file in each dir before any handler accepts traffic, and a live
write or fsync failure under traffic. Either way the broker keeps running on
the directories that are healthy.

Everything on the offline dir is gone from this broker until it restarts:

- Produce and Fetch on its partitions answer `KAFKA_STORAGE_ERROR`.
- `kafka-log-dirs --describe` reports the directory with the same code, which
  is where the reason string ends up.
- JBOD placement stops choosing it, so newly materialized partitions land
  elsewhere.
- Background work on those partitions stops too, which is why
  [log-cleaner-stalled](log-cleaner-stalled.md) usually fires next.

A dir goes offline once. The flip is logged with its reason and the reason
does not change afterwards, so the first `log dir flipped to OFFLINE at
runtime` line in the log is the one that matters.

## Confirm

```
krabka_broker_offline_log_dirs
krabka_broker_under_replicated_partitions
krabka_broker_offline_partitions_count
```

`kafka-log-dirs --bootstrap-server <broker> --describe` names the directory
and carries the reason the broker recorded. Compare the count with the number
of `log.dirs` entries the broker was started with: all of them offline is a
broker serving nothing.

## Diagnose

1. Read the reason. `partition write/fsync failed:` is a runtime flip; the
   text after it is the operating system's own error. `create_all`, `open
   probe`, `write probe`, `sync probe` and `remove probe` are the five
   startup-probe steps.
2. `EIO` or an `Input/output error` is the device. Check `dmesg` and the
   disk's SMART data on the node.
3. `EROFS` or `Read-only file system` is a filesystem the kernel remounted
   after an error. The mount is the symptom; the error that caused the
   remount is earlier in `dmesg`.
4. `ENOSPC` is a full filesystem. Look for a partition whose local retention
   or compaction stopped — the cleaner alerts and
   `krabka_broker_partition_disk_bytes` name it — and at the tiered-storage
   copy lag, because a segment the tier has not taken cannot be evicted
   locally.
5. `EACCES` after a deployment change is a permission or ownership change on
   the mount, not a hardware fault.

## Fix

- Restore the leadership first, not the disk: the partitions on the offline
  dir are served from their other replicas, and a broker that lost one of
  several dirs is still serving the rest. Confirm the topic's ISR elsewhere
  before touching the node.
- Replace or repair the device, or remount the filesystem read-write, then
  **restart the broker**. There is no online recovery: the registry marks a
  dir offline for the life of the process, so a repaired disk stays unused
  until the restart.
- For `ENOSPC`, free space before the restart, or the probe fails again on
  start and the dir comes back offline.
- After the restart, the partitions on that dir rebuild from their leaders.
  Watch `krabka_broker_under_replicated_partitions` fall to zero rather than
  assuming the restart finished the job.

## Escalate

Every log dir offline on one broker, or one log dir offline on enough brokers
that a partition's ISR is down to `min.insync.replicas`, is an availability
incident rather than a maintenance task. Escalate with the reason strings,
the `kafka-log-dirs --describe` output and the node's `dmesg`.
