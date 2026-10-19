# Backup and restore

What to copy off a krabka cluster, how often to copy it, and how to check that
a copy would restore. The day this page matters is the day the disks are gone,
so everything here has to already be running before then.

`krabka-restore` rebuilds a log directory out of a KIP-405 tiered-storage
archive. The image and the [binary install](deploy.md#install) supply it and
`krabka-backup` under those names. The archive holds the records. It holds
nothing else, and the three inputs that turn a pile of records back into the
cluster an operator lost live on the disks the disaster destroys. This page is
about those three.

## What a restore needs

| Input | Where it lives | What is lost without it |
| :--- | :--- | :--- |
| Archived segments | The tiered-storage bucket. Already off the node. | Everything. There is no restore. |
| RLMM snapshot | `<log.dir>/remote-log-metadata/snapshot` on each broker. | Segment lifecycle. A segment the old cluster had already released is indistinguishable from a live one, and the restore includes it, so records that retention had dropped come back. |
| Controller metadata checkpoint | `<metadata.log.dir>/__cluster_metadata-0/<end-offset>-<epoch>.checkpoint` on a controller, or `<metadata.log.dir>/__cluster_metadata-0/observer/` on a broker-only node. The metadata log directory is the broker's first log directory unless its `metadata_log_dir` names another one. | Topic configuration, ACLs, client quotas, SCRAM credentials and finalized feature levels. The topics come back with their ids, their partition counts and default settings, and nothing else. |
| Committed group offsets | The `__consumer_offsets` topic in the running cluster. | Every consumer group's position. `__consumer_offsets` is compacted and internal, so it is never tiered and no archive holds it. Each group restarts from its own `auto.offset.reset`. |

Two more things never come back and cannot be captured: the cluster id and the
node identity. A restore writes a new cluster id unless `--cluster-id` names
one, and it names the target node as the leader and sole replica of every
partition. Record the cluster id of every production cluster somewhere outside
it, and pass it to the restore.

### Topics the archive does not hold

The archive holds only the topics that tier their segments, the topics with
`remote.storage.enable=true`. A topic that does not tier is not in the
archive, and `krabka-restore` does not bring it back. Two kinds of topic
cannot tier at all:

- **A compacted topic.** krabka refuses `remote.storage.enable` together with
  `cleanup.policy=compact`, as Kafka does. Every compacted user topic is
  outside the archive. Examples are a table of latest values per key, a
  Kafka Streams changelog, and the internal topics of Kafka Connect.
- **A schema registry's `_schemas` topic.** It is compacted. A restored topic
  of Avro, Protobuf or JSON Schema records carries schema ids that no
  registry can resolve until the registry has its schemas back.

`krabka-backup` captures none of these. The group offsets are the only
compacted data that it captures. Protect each such topic separately, before
the day you need it:

- Mirror it to a second cluster with MirrorMaker 2, as
  [Migrate from Apache Kafka](migrate-from-kafka.md) describes for a
  migration. The mirror is a copy of the topic, not a point-in-time backup,
  so a bad write reaches it too.
- Or rebuild it from its source of truth. A Kafka Streams changelog rebuilds
  from the restored input topics when the application starts with a new
  `application.id`, if those topics still hold every record that the state
  needs. A schema registry can register its schemas again from the schema
  files that its producers build from. A plain registration gives a schema a
  new id, and the restored records name schemas by their old ids. Use a
  registry mode that registers a schema under a given id, such as the
  `IMPORT` mode of Confluent Schema Registry, and check every id.

A restore rehearsal checks these topics as well as the archive. Read one
record from each restored tiered topic, decode it with the restored registry,
and compare it with production.

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

A capture reads both files under one `--log-dir`. That holds when the metadata
log directory is the broker's first log directory, which is the default. For a
broker with a separate `metadata_log_dir`, run a second capture with `--log-dir`
set to the metadata log directory to take the checkpoint.

Credentials work the way they do for the broker's own tiered storage. Leave
`--archive-s3-access-key-id` and `--archive-s3-secret-access-key` off and the
AWS credential chain applies, so a capture under an instance role or under
Workload Identity puts no secret on a command line.

### Secured clusters

On a cluster with TLS, SASL or an authorizer, pass a Kafka client-properties
file with `--command-config`, as for the JVM tools. `capture` and
`restore-offsets` read the same file. The tool reads `security.protocol`,
`sasl.mechanism` and `sasl.jaas.config`, and it reads TLS trust and client
identity from PEM files:

```properties
security.protocol=SASL_SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/krabka/backup/ca.pem
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="backup" password="<password>";
```

For mutual TLS, set `security.protocol=SSL` and add
`ssl.keystore.type=PEM`, `ssl.keystore.location=<client certificate PEM>` and
`ssl.key.location=<client private-key PEM>`. The tool never copies the file
into the archive or prints it. Keep its permissions tight, because it holds
the password or the key path.

Give the backup principal only what the capture reads:

| Resource | Operations | Why |
| :--- | :--- | :--- |
| Each captured group | `Describe` | Read the committed offsets. |
| Each captured topic | `Describe`, `Read` | Resolve the partitions of the committed offsets. |
| Each diskless topic | `DescribeConfigs` | Keep empty diskless partitions in the recovery topology. |
| `__diskless_wal_index` | `Describe`, `Read` | Read the diskless index. A capture never writes to it. |

Restoring offsets into the restored cluster needs `Describe` and `Read` on
each captured group and each captured topic. Grant every row on the group and
topic prefixes that the backup policy covers, not on `*`.

In Kubernetes, put the properties file and the CA in a Secret, and mount it
into the `CronJob` beside the data volume. The changes to the example above:

```yaml
              args:
                - capture
                - --log-dir=/var/lib/krabka
                - --bootstrap-server=krabka-0.krabka:9094
                - --command-config=/etc/krabka/backup/client.properties
                - --archive-s3-bucket=krabka-tier
                - --archive-s3-region=eu-west-1
                - --archive-prefix=prod/
              volumeMounts:
                - name: data
                  mountPath: /var/lib/krabka
                  readOnly: true
                - name: backup-client
                  mountPath: /etc/krabka/backup
                  readOnly: true
          volumes:
            - name: data
              persistentVolumeClaim:
                claimName: data-krabka-0
            - name: backup-client
              secret:
                # Holds client.properties and ca.pem, and for mutual TLS the
                # client certificate and key.
                secretName: krabka-backup-client
```

Point `--bootstrap-server` at the listener that the properties file speaks
to. On the three-node example in [Deploy](deploy.md#configure), that is the
`CLIENT` listener on 9094.

## How often

The right interval is the amount of recovery quality the operator will accept
losing, and it is different for each input.

| Input | Suggested interval | What a stale copy costs |
| :--- | :--- | :--- |
| Group offsets | Hourly, and again before any planned change to the cluster. | Every group resumes at the older position and reads the records after it a second time. Kafka consumers are already at-least-once, so this is a cost in duplicate work, not in correctness. |
| RLMM snapshot | Daily, or hourly beside the offsets. It is cheap and small. | A restore includes segments that remote retention released after the snapshot was taken. Records the old cluster had already dropped come back. |
| Metadata checkpoint | Daily, and again after the controller writes its next checkpoint after any ACL, quota, credential or topic-config change. | The restore brings back the configuration as it was at the checkpoint. A permission granted after it is gone, and, worse, a permission revoked after it comes back. |

One `krabka-backup capture` run takes all three, so the simple schedule is one
hourly `CronJob` and nothing else to reason about. The metadata checkpoint is a
whole controller snapshot: it is the largest artifact of the three, and the
controller writes a new one on its own schedule, so an hourly capture often
re-uploads bytes it already has.

A capture takes the newest checkpoint on disk, and that file can be up to an
hour old. The controller writes one each `metadata_max_snapshot_interval`
(default 1 hour) or each `metadata_snapshot_interval_records` (default 10,000
records), both in `[runtime]`. Until its first one, the newest file is the
format-time `00000000000000000000-0000000000.checkpoint`, which holds no
topic. `krabka-backup capture` refuses that checkpoint and exits `7`, so a
young cluster has no capture until the controller writes its first checkpoint
([#1514](https://github.com/krabka-io/krabka-broker/issues/1514)). Lower
`metadata_max_snapshot_interval` to shorten that window. A cluster that
`krabka-restore` formatted is captured at once: the restore seeds the restored
topics into the same offset-zero checkpoint.

A capture that succeeds prints a `NOTE` line with the age of the newest record
in its checkpoint. A change committed after that record is not in the capture.
The restore names a topic created after the checkpoint in
`topics_without_configuration` and warns about it. It cannot name a config
change to a topic the checkpoint already knows. So a capture right after a
topic-config change carries the change only once the controller has written its
next checkpoint.

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

1. `krabka-restore` from the production archive with the newest capture's two
   snapshots into a scratch directory, with `--dry-run` first to see the plan.
   Compare `metadata.topic_configs` in the plan with the number of topics that
   have config overrides in production.
2. Boot a broker on the result.
3. `krabka-backup restore-offsets` against it.
4. Read one topic from a group's restored position and compare against
   production.

The rehearsal reads the production archive while production keeps writing
to it, and that is expected. After the capture, remote copy uploads segments
that the capture's RLMM snapshot never saw. Remote retention deletes the
oldest segments first, including segments that the snapshot still lists as
live. `krabka-restore` accepts both changes:

- It keeps a segment the snapshot does not list when that segment starts
  after every segment the snapshot lists for its partition.
- It leaves out a live segment that is gone from the archive when that
  segment ends before every segment the archive still holds.

So each restored partition starts at the oldest segment that the archive holds
when the restore reads it, not at the capture. In step 4, compare from the
restored partition's earliest offset: `kafka-get-offsets --time -2` gives it.
Production's earliest offset, read after the restore, is never below it,
because retention only moves it forward. Compaction does not change the
archive, because a compacted topic never tiers.

A captured group position can lie below the restored partition's earliest
offset, when retention passed it after the capture. `restore-offsets` commits
it as captured. The group's first fetch is then out of range, and the
consumer resets as its `auto.offset.reset` says, as it would on production.

Some changes that a running cluster makes still stop a rehearsal:

- **Retention emptied a partition** after the capture. The restore stops with
  a metadata disagreement for that partition, because an archive that lost a
  whole partition looks the same.
- **A partition archived its first segment** after the capture, for example
  a topic created or tiered after it. The snapshot lists no segment for the
  partition, so the restore cannot tell a new upload from the leftover bytes
  of a deleted one, and stops with a metadata disagreement.
- **An unclean leader election** after the capture. The new leader deletes
  the remote segments outside its epoch lineage and uploads its own copies
  of those offsets, and the restore stops with a metadata disagreement.
- **Retention deleted a segment while the restore was reading it.** The
  restore stops with a torn copy, exit `5`, of the oldest segment of a
  partition.

For each, take a new capture and run the rehearsal again. Take the capture
just before the rehearsal, so the archive has little time to change. If the
same failure comes back, the archive itself is the problem. Any other
disagreement is a finding. Examples are a segment missing from the middle of
a partition, or a segment inside the snapshot's range that the snapshot does
not list, with no unclean election since the capture.

The [restore-from-archive](runbooks/restore-from-archive.md) runbook is that
sequence written out for the day it is not a rehearsal.
`crates/restore/tests/dr_roundtrip.rs` runs the whole of it in CI on every
change, so the path in the runbook is a path that is exercised.

The reusable candidate drill is the `disaster-recovery` leg of the
`ecosystem qualification` workflow. It uses a digest-pinned broker image, an
RF=3 TLS and SASL/SCRAM-SHA-512 source cluster and a locked S3-compatible
bucket. Its backup principal has only the cluster, group, user-topic and
`__diskless_wal_index` permissions capture needs: READ and DESCRIBE on the
index, and no WRITE, because capture only reads it.

The drill runs under a declared load,
[`bench/soak/dr-secured.yaml`](../../bench/soak/dr-secured.yaml): paced
idempotent producers and committing consumer groups on one tiered and one
diskless topic, sent over `SASL_SSL` by a principal that holds only ACLs. The
load runs through the capture and past it, until every source broker is killed.
The drill records the capture manifest, the WORM chain heads, the source ledger,
topic settings, ACLs, SCRAM users, quotas and consumer positions, then deletes
every source data directory. The fresh cluster must reproduce those values
through the capture boundary, and the soak judge (`krabka-soak-check judge`)
reconciles the load's records against it.

The capture boundary is the instant the capture starts. Every record
acknowledged before it must come back, and a missing one fails the drill.
`rpo_ms` is the time from the loss back to the acknowledgement of the oldest
acknowledged record that did not come back, and zero when every one did. The
capture's own age at the loss is kept beside it as `capture_age_ms`. `rto_ms`
runs from the loss through listing, verifying and fetching the capture,
`krabka-restore`, broker start and `restore-offsets`, to a cluster that has
served every captured record, each topic's settings and the group positions
back to a reader, and has served an authenticated produce and consume to the
ACL-only load principal. Comparing what it served with the source, proving the
resumed WORM chain and judging the load come after recovery completes, and the
drill records that time separately as `verification_ms`.

Access control is reconciled, not assumed: the restored cluster's ACL listing
and its users' SCRAM mechanisms and quotas must equal the source's, and a
principal with no ACL, or the load principal on a topic it has no ACL for, is
refused on both clusters. Separate copies of the archive prove that changed
data, missing manifests or objects, an untrusted chain head, and unavailable
credentials fail closed. Separate copies of the capture prove that a changed
`group-offsets.json` and a missing `rlmm-snapshot` fail `krabka-backup verify`,
and that `restore-offsets` refuses the changed one and commits nothing. Both
classic and diskless topics are part of a schema-2 qualification; the fast
hermetic roundtrip remains the per-change gate.
[Disaster-recovery declaration](../qualification.md#disaster-recovery-declaration)
states the drill's backup boundary, load, objectives and recorded limits before
any run.

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
  feature levels come back whole. An authenticated restore accepts the
  checkpoint only when its digest is bound into the signed diskless capture.
- **Remote-segment lifecycle and WORM chain receipts**, from `--rlmm-snapshot`.
  Restore resets its metadata-topic cursors before seeding it into the new
  broker. An authenticated restore accepts the snapshot only when its digest is
  bound into the signed diskless capture.
- **Committed group offsets**, from `krabka-backup restore-offsets`, and only
  from it.
- **Committed diskless WAL state**, from the capture's
  `diskless-wal-index.json`, when the archive contains diskless WAL objects.
  Its capture boundary, delete floors and unavailable uncommitted tail are
  reported separately from classic KIP-405 segments.

What does not come back at all: the cluster id, unless it is passed; the node
identity and the replica assignment, which the restore rewrites to the target
node; in-flight transaction state, which lives in the never-tiered
`__transaction_state`; every topic that does not tier, compacted topics and
`_schemas` among them, as
[Topics the archive does not hold](#topics-the-archive-does-not-hold) says;
and any classic active-segment or diskless uncommitted tail beyond the
reported capture boundary.

The metadata checkpoint holds more than the restore carries. Dynamic broker
configs, delegation tokens, group configs and client-metrics configs are in the
checkpoint, and the restore drops all four. Re-apply dynamic broker configs to
the restored cluster, issue new delegation tokens, and re-apply group and
client-metrics configs.

## Related

- [restore-from-archive](runbooks/restore-from-archive.md): the runbook.
- [`krabka-backup`](../../crates/backup-cli/README.md): the capture tool.
- [`krabka-restore`](../../crates/restore/README.md): the restore tool.
- [KFC-3](../KFCs/KFC-3-point-in-time-restore.md): what a restore promises.
