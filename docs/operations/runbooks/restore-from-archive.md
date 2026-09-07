# Restore from the archive

**Trigger:** a data-loss event. The cluster's disks are gone, encrypted or
corrupted, or a bad write reached every replica and the log has to be rebuilt to
a point before it. No alert fires for this; a person decides it.

## What it means

`krabka restore` reads the KIP-405 tiered-storage archive and writes a complete,
bootable log directory out of it, up to a bound the operator names. The tool
runs when the cluster does not, and it does not depend on the broker.

Three things a restore needs are not in the archive: the RLMM snapshot, the
controller's metadata checkpoint, and the committed offsets of every consumer
group. They come from the newest `krabka-backup` capture. Without them a restore
still succeeds, and it produces a cluster with released segments back in it, no
topic configuration, no ACLs, no quotas, no SCRAM credentials, and every group
starting from `auto.offset.reset`.
[Backup and restore](../backup-restore.md) says what each one carries.

The restore writes a single-node cluster: every restored partition names the
target node as its leader and its sole replica. Growing that back to the
production shape is a reassignment afterwards, not part of the restore.

## Confirm

Confirm that this is a restore and not a failover. A restore is right when the
records themselves are gone or wrong. A partition that is offline with its data
intact is [offline-partitions](offline-partitions.md), and a node whose disk
failed is [offline-log-dir](offline-log-dir.md). A restore replaces a cluster;
neither of those needs one.

Find the capture that is going to be used, and prove it before anything else:

```sh
krabka-backup list --archive-s3-bucket krabka-tier --archive-prefix prod/
krabka-backup verify --capture <id> \
  --archive-s3-bucket krabka-tier --archive-prefix prod/
```

Exit `0` means the capture is whole. Exit `5` names an artifact that does not
match its digest, and a capture that fails here is not the capture to restore
from. Use the newest one that verifies, and note how old it is: everything
committed to a consumer group after it is going to be read again.

For a bad-write incident, decide the bound now. It is an offset, a timestamp, or
a set of exclude predicates, and it is the whole reason to use this tool rather
than a snapshot of a disk.

## Diagnose

Answer four questions before writing anything.

1. **What is the target?** An empty directory on a node with room for the whole
   restored history. `--log-dir` must be empty or absent, and the tool exits `3`
   when it is not.
2. **What is the cluster id?** Pass the old cluster's id with `--cluster-id` if
   clients or a schema registry are pinned to it. Without the flag the restore
   generates a new one, and the report states which was used.
3. **What is the bound?** `--to-timestamp` for "just before the incident",
   `--to-offset` for one partition cut at a known offset, and the
   `--exclude-*` predicates for a producer or a key that has to be dropped
   wherever it appears.
4. **Which topics?** `--topic` restores a subset. Absent, every topic in the
   archive is restored.

Run the plan first. `--dry-run` runs discovery, verification and the bound, and
writes no partition data:

```sh
krabka restore --dry-run --report json \
  --archive-s3-bucket krabka-tier --archive-s3-region eu-west-1 \
  --archive-prefix prod/ \
  --log-dir /var/lib/krabka-restored \
  --node-id 1 --standalone --controller-listener 127.0.0.1:9093 \
  --to-timestamp 2026-08-24T09:15:00Z
```

Read the counts. A partition missing from the plan, or a record count far from
what the topic should hold, is a wrong `--archive-prefix` or a wrong bound.

## Fix

1. **Fetch the capture's two snapshots** to the machine that runs the restore.
   They are `restore-inputs/<id>/rlmm-snapshot` and
   `restore-inputs/<id>/cluster-metadata.checkpoint` in the archive.

2. **Restore**, with both:

   ```sh
   krabka restore \
     --archive-s3-bucket krabka-tier --archive-s3-region eu-west-1 \
     --archive-prefix prod/ \
     --rlmm-snapshot ./rlmm-snapshot \
     --metadata-snapshot ./cluster-metadata.checkpoint \
     --log-dir /var/lib/krabka-restored \
     --cluster-id <the old cluster id> \
     --node-id 1 --standalone --controller-listener 127.0.0.1:9093 \
     --to-timestamp 2026-08-24T09:15:00Z \
     --report json
   ```

   The exit code says what to do next. `0` is success. `2` is a bad argument.
   `3` is a target directory that is not empty. `4` is an archive that cannot be
   read, which is usually the prefix or the credentials. `5` is an integrity
   failure in the archive, and a second run against the same archive fails the
   same way; `--continue-on-corrupt` turns a damaged segment into a skipped one,
   and the report then names exactly what was skipped. `6` is a target that
   could not be written, and a different target may work.

3. **Boot a broker** on the restored directory. It is an ordinary krabka data
   directory and needs no restore-specific configuration. Confirm that
   `kafka-topics --describe` lists the topics and that `kafka-configs
   --describe` shows the topic configuration the metadata checkpoint carried. If
   the configuration is missing, the restore ran without
   `--metadata-snapshot`, and the report says so.

4. **Put the group offsets back, before any consumer starts.**

   ```sh
   krabka-backup restore-offsets --capture <id> \
     -b restored-broker:9092 \
     --archive-s3-bucket krabka-tier --archive-prefix prod/
   ```

   Run it with `--dry-run` first to see the positions it will write. It commits
   as a simple consumer, so the coordinator fences it on any group that already
   has live members. That is why this step comes before the consumers: a group
   that is running has to be stopped for its position to be set.

5. **Start the clients**, and check group lag against the restored end offsets
   before declaring the incident over.

6. **Grow the cluster back.** The restored cluster is one node holding one
   replica of everything. Add brokers and reassign partitions with
   `kafka-reassign-partitions` as a separate, planned piece of work.

7. **Capture immediately.** The restored cluster has a new cluster id, possibly
   a new node identity, and a fresh metadata log. Run `krabka-backup capture`
   against it as soon as it serves traffic, so the next incident is not restored
   from the dead cluster's inputs.

## Escalate

Escalate when the archive itself is the problem. Exit `5` on a full restore,
after `--continue-on-corrupt` shows the damage is not one segment, means the
archive is not what it claims to be, and a second copy of the bucket, or the
bucket's own object versions, is the next place to look. Escalate too when the
newest verifying capture is old enough that the restored ACLs or quotas no
longer match what production had: those are a security decision, not an
operations one.
