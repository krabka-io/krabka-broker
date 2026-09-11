# Backup and restore

What to copy off a krabka cluster, how often to copy it, and how to check that
a copy would restore. The day this page matters is the day the disks are gone,
so everything here has to already be running before then.

`krabka restore` rebuilds a log directory out of a KIP-405 tiered-storage
archive. The archive holds the records. It holds nothing else, and the three
inputs that turn a pile of records back into the cluster an operator lost live
on the disks the disaster destroys. This page is about those three.

## What a restore needs

| Input | Where it lives | What is lost without it |
| :--- | :--- | :--- |
| Archived segments | The tiered-storage bucket. Already off the node. | Everything. There is no restore. |
| RLMM snapshot | `<log.dir>/remote-log-metadata/snapshot` on each broker. | Segment lifecycle. A segment the old cluster had already released is indistinguishable from a live one, and the restore includes it, so records that retention had dropped come back. |
| Controller metadata checkpoint | `<log.dir>/__cluster_metadata/@metadata-0/<end-offset>-<epoch>.checkpoint` on a controller, or `<log.dir>/__cluster_metadata/observer/` on a broker-only node. | Topic configuration, ACLs, client quotas, SCRAM credentials and finalized feature levels. The topics come back with their ids, their partition counts and default settings, and nothing else. |
| Committed group offsets | The `__consumer_offsets` topic in the running cluster. | Every consumer group's position. `__consumer_offsets` is compacted and internal, so it is never tiered and no archive holds it. Each group restarts from its own `auto.offset.reset`. |

Two more things never come back and cannot be captured: the cluster id and the
node identity. A restore writes a new cluster id unless `--cluster-id` names
one, and it names the target node as the leader and sole replica of every
partition. Record the cluster id of every production cluster somewhere outside
it, and pass it to the restore.

## Capture the inputs

`krabka-backup capture` copies all three into the archive, beside the segments
the restore reads:

```sh
krabka-backup capture \
  --log-dir /var/lib/krabka \
  --bootstrap-server broker-1:9092 \
  --archive-s3-bucket krabka-tier \
  --archive-s3-region eu-west-1 \
  --archive-prefix prod/
```

The capture lands under `restore-inputs/<capture-id>/` with a `manifest.json`
that records each artifact's size and SHA-256. The capture id is the epoch
millisecond, zero-padded, so the alphabetical order of the directories is their
time order.

There is no way to copy those files out of a running broker pod with `kubectl`.
The image has no shell, no `cp` and no `tar`, which is what `kubectl exec` and
`kubectl cp` need. Run `krabka-backup` instead: it is in the same image, and a
`CronJob` in the same namespace can mount the broker's volume read-only and run
it. The broker writes both files with a temporary file and a rename, so a reader
that opens one by name always gets a whole file, and a capture never has to stop
a broker.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: krabka-capture-restore-inputs
spec:
  # Hourly, offset from the top of the hour so the capture does not compete
  # with whatever else the cluster does at :00.
  schedule: "17 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          securityContext:
            runAsUser: 65532
            runAsGroup: 65532
          containers:
            - name: capture
              image: ghcr.io/krabka-io/krabka-broker:latest
              command: ["/usr/bin/krabka-backup"]
              args:
                - capture
                - --log-dir=/var/lib/krabka
                - --bootstrap-server=krabka-0.krabka:9092
                - --archive-s3-bucket=krabka-tier
                - --archive-s3-region=eu-west-1
                - --archive-prefix=prod/
              volumeMounts:
                - name: data
                  mountPath: /var/lib/krabka
                  readOnly: true
          volumes:
            - name: data
              persistentVolumeClaim:
                # The claim of one broker's StatefulSet pod. A capture reads
                # one node's files, and one node's are enough: every broker's
                # RLMM snapshot describes the same archive, and every
                # controller's checkpoint describes the same metadata log.
                claimName: data-krabka-0
```

`--log-dir` and `--bootstrap-server` are both optional, and each is a source of
its own. A capture with only `--bootstrap-server` takes group offsets, which is
useful from a node that has no volume mounted. A capture that finds nothing at
all fails rather than writing an empty capture.

Credentials work the way they do for the broker's own tiered storage. Leave
`--archive-s3-access-key-id` and `--archive-s3-secret-access-key` off and the
AWS credential chain applies, so a capture under an instance role or under
Workload Identity puts no secret on a command line.

## How often

The right interval is the amount of recovery quality the operator will accept
losing, and it is different for each input.

| Input | Suggested interval | What a stale copy costs |
| :--- | :--- | :--- |
| Group offsets | Hourly, and again before any planned change to the cluster. | Every group resumes at the older position and reads the records after it a second time. Kafka consumers are already at-least-once, so this is a cost in duplicate work, not in correctness. |
| RLMM snapshot | Daily, or hourly beside the offsets. It is cheap and small. | A restore includes segments that remote retention released after the snapshot was taken. Records the old cluster had already dropped come back. |
| Metadata checkpoint | Daily, and again after any ACL, quota, credential or topic-config change. | The restore brings back the configuration as it was at the capture. A permission granted after it is gone, and, worse, a permission revoked after it comes back. |

One `krabka-backup capture` run takes all three, so the simple schedule is one
hourly `CronJob` and nothing else to reason about. The metadata checkpoint is a
whole controller snapshot: it is the largest artifact of the three, and the
controller writes a new one on its own schedule, so an hourly capture often
re-uploads bytes it already has.

Keep at least as many captures as the recovery window the business asks for. An
attacker who can write to the cluster can also grant themselves an ACL, and a
capture taken after that carries the ACL. Object-lock or versioning on the
archive bucket is what makes an older capture survive them; the tiered archive
already needs the same protection for the same reason.

## Check a copy

A backup nobody checks is a backup nobody has. There are two checks, and they
answer different questions.

**The bytes arrived.** `krabka-backup verify` re-reads every artifact of a
capture and compares its size and SHA-256 against the manifest:

```sh
krabka-backup verify --archive-s3-bucket krabka-tier --archive-prefix prod/
```

It exits `0` when the capture is whole and `5` when it is not, and it names the
artifact that does not match. Run it as its own scheduled job, not only after a
capture: this is what catches a bucket lifecycle rule that expired an object and
a half-finished upload alike. `krabka-backup list` names the captures the
archive holds, which is the check that the `CronJob` is still running at all.

**The copy restores.** Nothing but a restore proves a restore. Rehearse it on a
schedule the same way an on-call rotation is rehearsed:

1. `krabka restore` from the production archive with the newest capture's two
   snapshots into a scratch directory, with `--dry-run` first to see the plan.
2. Boot a broker on the result.
3. `krabka-backup restore-offsets` against it.
4. Read one topic from a group's restored position and compare against
   production.

The [restore-from-archive](runbooks/restore-from-archive.md) runbook is that
sequence written out for the day it is not a rehearsal.
`crates/restore/tests/dr_roundtrip.rs` runs the whole of it in CI on every
change, so the path in the runbook is a path that is exercised.

The reusable candidate drill is the `disaster-recovery` leg of the
`ecosystem qualification` workflow. It uses a digest-pinned broker image, an
RF=3 TLS and SASL/SCRAM-SHA-512 source cluster and a locked S3-compatible
bucket. Its backup principal has only the cluster, group, user-topic and
`__diskless_wal_index` permissions capture needs. It records the
capture manifest, independently held WORM chain heads, source ledger, topic
settings and consumer positions before deleting every source data directory.
The fresh cluster must reproduce those values through the capture boundary and
the evidence bundle records measured RPO and RTO. Separate copies of the
archive prove that changed data, missing manifests or objects, an untrusted
chain head, and unavailable credentials fail closed. Both classic and diskless
topics are part of a schema-2 qualification; the fast hermetic roundtrip remains
the per-change gate.

## What comes back

The restore's contract is stated in full in
[KFC-3](../KFCs/KFC-3-point-in-time-restore.md). In short:

- **Records, at their own offsets.** Every record the restore keeps sits at the
  offset it held in the archive, so an offset another system recorded still
  names the record it named.
- **Topic ids and partition counts**, from the archive itself.
- **Topic configuration, ACLs, client quotas, SCRAM credentials and finalized
  feature levels**, from `--metadata-snapshot`, and only from it. A topic config
  comes back for a topic the archive also holds; ACLs, quotas, credentials and
  feature levels come back whole.
- **Committed group offsets**, from `krabka-backup restore-offsets`, and only
  from it.
- **Committed diskless WAL state**, from the capture's
  `diskless-wal-index.json`, when the archive contains diskless WAL objects.
  Its capture boundary, delete floors and unavailable uncommitted tail are
  reported separately from classic KIP-405 segments.

What does not come back at all: the cluster id, unless it is passed; the node
identity and the replica assignment, which the restore rewrites to the target
node; in-flight transaction state, which lives in the never-tiered
`__transaction_state`; and any classic active-segment or diskless uncommitted
tail beyond the reported capture boundary.

## Related

- [restore-from-archive](runbooks/restore-from-archive.md): the runbook.
- [`krabka-backup`](../../crates/backup-cli/README.md): the capture tool.
- [`krabka-restore`](../../crates/restore/README.md): the restore tool.
- [KFC-3](../KFCs/KFC-3-point-in-time-restore.md): what a restore promises.
