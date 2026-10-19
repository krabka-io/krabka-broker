# Ecosystem qualification

[`qualification/ecosystem-eight-gate-baseline.json`](../qualification/ecosystem-eight-gate-baseline.json) is the
qualified baseline: krabka-broker v0.6.1 with krabka-operator v0.4.1 and the
rest of its sibling set, which passed all eight gates in
[`qualification-36293960620`](https://github.com/krabka-io/krabka-broker/releases/tag/qualification-36293960620).
It is the default manifest for the workflow and for `aspect
check-qualification`. Schema 2 binds each executed check to a candidate and the
exact qualification adapters. It requires the disaster-recovery gate owned by
issue 555 and the schema-evolution, registry-migration, and snapshot-retention
gates owned by issues 556-558.

[`qualification/original-four-gate-baseline.json`](../qualification/original-four-gate-baseline.json) is the
completed historical schema-1 candidate for the original four gates. Keep its
revisions, evidence and report intact.

## Run a subsequent candidate

The `ecosystem qualification` workflow accepts a repository-relative `manifest`
and an `execute` switch. With `execute=false`, it verifies the existing published
result and its hashes. With `execute=true`, it creates a fresh draft from that
component set, runs eight independent jobs, and publishes a qualification release
only after every job succeeds.

Supply both `broker_revision` and `broker_digest` to qualify a newly delivered
broker using the same sibling set. The prepare job applies those overrides,
freezes the complete input, verifies revisions against each recorded default
branch, and distributes that exact input to every job. This needs no candidate
commit after the delivery image has been published. To change sibling revisions
or artifacts, provide a reviewed manifest containing that full candidate set.

The overrides replace the broker component only. Before you use them, run
`aspect freeze-candidate` with the baseline manifest: a `DRIFT` line means the
broker compiles in a sibling revision that the manifest does not name, and the
run needs a reviewed manifest instead.
[Release candidate qualification](release-candidate-qualification.md) gives
the whole freeze procedure and every gate a release candidate passes.

To qualify a new broker delivery against the baseline sibling set, run the
baseline with the delivered revision and digest. When a run with a new sibling
set publishes, check in its qualified manifest as the next baseline; do not
invent a draft manifest or mutable image tag to make a run appear green.

```sh
gh workflow run qualification.yml \
  -f manifest=qualification/ecosystem-eight-gate-baseline.json \
  -F execute=true \
  -f broker_revision="$DELIVERED_COMMIT" \
  -f broker_digest="$DELIVERED_IMAGE_DIGEST"
```

The broker digest must come from the successful delivery run for that commit.
The qualification workflow exercises those bytes; release promotion separately
verifies the delivery record. Do not infer a delivery digest from a mutable tag.

The matrix has `fail-fast: false`. GitHub's **Re-run failed jobs** retries only
failed gates with the original uploaded candidate; successful gates remain
available. Re-running the entire workflow creates a new candidate timestamp and
reruns every gate. The jobs require Linux amd64, Docker, and sufficient capacity
for the existing stack; installation and lifecycle also install pinned Helm and
Kind. Their timeouts bound the run rather than turning a timeout into a pass.

## What runs

| Gate | Executed boundary |
| :--- | :--- |
| Installation | The pinned public Compose and Helm recipes with candidate images and downloaded, checksum-verified charts; produce/readback and uninstall. The existing Go and Java integration suites run against the installed broker, and the pinned schema registry's RF=3 live-registry suite plus the broker's incompatible-protocol build probe rerun the original compatibility supporting checks. |
| Operator lifecycle | The pinned operator `packaging/kind-lifecycle.sh`, using published chart contents and the candidate broker, operator and rebalancer images; upgrade, disruption, scale-down, certificate rotation and acknowledged-record reconciliation. |
| Authenticated CLI | The pinned CLI's real `candidate_broker` test, against a disposable three-broker SASL cluster. Its one executable lookup is adapted in the isolated checkout to invoke the downloaded CLI binary instead of a rebuilt binary; the patch is retained. |
| Observability recovery | The pinned demo Compose recipe with explicit candidate broker, o11y and demo-app image overrides on linux/amd64, followed by its `qualify-failover.sh`; signal queries, WAL recovery, offset reconciliation and alert firing/resolution. |
| Disaster recovery | A TLS and SASL/SCRAM-SHA-512 RF=3 cluster created from the candidate broker image, a locked MinIO archive, and the image's bundled backup, restore, and WORM verifier. The declared load of [`bench/soak/dr-secured.yaml`](../bench/soak/dr-secured.yaml) runs as an ACL-only principal through the checks and the capture, and past it, until every source broker is killed. The job captures offsets and configuration at an explicit boundary, destroys every source data directory, restores a fresh cluster, reconciles records at their original offsets, settings, ACLs, SCRAM users and consumer positions, and records measured RPO/RTO. The soak judge (`krabka-soak-check judge`) reconciles the load's records against the capture boundary and holds RPO and RTO against the declared objectives. Tampered, missing, untrusted-head, invalid-certificate, invalid-credential, denied-ACL and tampered-capture probes must fail without a complete capture or restore. Tiered classic-log and diskless topics are both required. [Disaster-recovery declaration](#disaster-recovery-declaration) states the boundary, the load and the objectives. |
| Schema evolution | Confluent-derived Avro, JSON Schema, and Protobuf compatibility matrices, serializer reference/cache tests, RF=3 broker validation, and an old/new record round trip across broker and registry restarts. |
| Registry migration | Full compacted `_schemas` lifecycle replay plus forward and reverse handoff against the pinned Confluent Schema Registry image. |
| Snapshot retention | Exact retained-epoch diagnostics, bounded reclamation, concurrent restore protection, and process-kill recovery during save and reclaim. |

The lifecycle harness expects a local operator tag. Its private alias is checked
against the pulled candidate image ID before execution. It packages source
charts as ancillary evidence, but installation uses the verified downloaded
chart contents copied into the isolated harness tree. No user checkout is
modified. Supporting source tests remain distinguishable from installed-image
checks in the command log.

A test process exiting successfully is insufficient: adapters check named live
test results or behavioral outcomes and record the checks that actually ran.
Skipped or zero-test runs cannot produce a valid gate receipt. Historical recovery and CDC evidence remains historical; this workflow does not
relabel those old runs as fresh candidate results. The monthly schedule is enabled only after
the first schema-2 eight-gate manifest is checked in as the immutable baseline.

## Loaded lifecycle

The eight gates qualify the ecosystem around one broker image. The loaded
lifecycle gate qualifies the move between two broker images: the newest
published 1.0.x release and the candidate. It runs under the sustained load of
the soak contract's executor, and the soak judge grades it. Issue 1396 owns it.

### Design Goals

- **Published bytes only.** The source and the target are images by digest. The
  gate never builds a broker. The executor and the judge are the adapter: the
  reviewed checkout that runs the task, as for every other gate.
- **One load, one judge.** The rolling upgrade, the certificate rotation, the
  reassignment and the rollback are transitions in one soak contract,
  [`bench/soak/lifecycle.yaml`](../bench/soak/lifecycle.yaml). The executor
  measures the disruption of each transition as it measures a fault. The judge
  reconciles the records and the committed offsets as it does for a soak. There
  is no second load generator and no second reconciler.
- **Reconcile after each transition, not only at the end.** A record that a
  later transition loses must not hide behind a record that an earlier one
  lost, and a configuration that a rollback drops must not wait for the end of
  the run.
- **Refuse a rollback that the formats do not allow, before it writes.**

### The lifecycle contract

| | Value |
| :--- | :--- |
| Source | `v1.0.5`, `ghcr.io/krabka-io/krabka-broker@sha256:2a81d1ba38de4e8ffb032fedf67952845185158a7607ac76ba38fa162dc37e50`, the multi-platform index that the release workflow signed |
| Target | The candidate: the `broker_digest` of a successful delivery run of a `main` commit |
| Cluster | Four nodes, each a controller and a broker, RF=3, `min.insync.replicas=2`, one TLS listener per node |
| Load | Three classic paths: idempotent, transactional and compacted, at 40 records per second each, with committing consumers at both isolation levels |
| Transitions | Upgrade brokers 0 to 3 to the target, one at a time. Rotate the TLS certificate of brokers 0 to 3, one at a time. Drain broker 3 of `classic-idempotent` by reassignment: each of its replicas moves to the broker that holds none of that partition. Roll brokers 0 to 3 back to the source, one at a time. |
| After each transition | The image that each node runs, the finalized feature levels, whether a rollback is still supported, every record that a producer had been acknowledged, the committed offsets of every group, the topic configuration, and the dynamic listener configuration of every rotated broker |
| Deadlines | Leader failover 25s, ISR rejoin 60s, controller failover 30s, certificate served 30s, reassignment complete 180s. The controller deadline covers the source: a `v1.0.5` controller leader that stops gracefully does not resign (#1446, fixed after `v1.0.5`), so the quorum replaces it only after its fetch timeouts. |

The source declares the feature ranges that it advertises in `ApiVersions`. The
executor reads the ranges back from the running source, and the judge compares
them. For `v1.0.5`, `metadata.version` is 7 to 30 and `krabka.version` is 0 to
1, and the candidate advertises the same ranges.

To move the source to a newer release, change the `lifecycle.source` section of
the contract: the version, the digest of the release's signed index, and the
ranges that it advertises. Do not use a tag. The contract validation refuses an
image that has no digest.

### Rollback eligibility

[`persisted_formats.md`](persisted_formats.md) states the rule: a 1.x broker
writes a new format only after the operator finalizes the feature level that
introduces it. So the formats on disk tell whether a rollback is safe, and the
finalized levels tell which formats are on disk.

| Cluster state | Rollback to the source |
| :--- | :--- |
| Every finalized level is in the source's supported range | Supported. The source reads every byte that the target wrote. |
| A `metadata.version` above the source's maximum is finalized | Not supported. A KIP-1155 downgrade of `metadata.version` must come first. |
| A `krabka.version` above the source's maximum is finalized, which is level 2 or more for every 1.0.x release | Not supported. No downgrade path exists for a krabka-only format. |
| A feature that the source does not name is finalized above 0 | Not supported. The source supports level 0 only of a feature that it does not know, as Kafka's `QuorumFeatures.DISABLED` does. |

`krabka_bench_driver::soak::lifecycle::rollback` applies the rule, and each
checkpoint records its answer. The judge fails a run that rolls back after a
checkpoint that ruled the rollback out. The lifecycle contract finalizes no
level, so its rollback is supported.

The broker enforces the same rule, so an operator cannot take an unsupported
rollback by mistake. A node that runs a controller replays the finalized levels
when it opens its log, and it stops with Kafka's fatal-fault message before it
binds a listener:
`Tried to apply FeatureLevelRecord FeatureLevelRecord(name='metadata.version', featureLevel=33), but this controller only supports versions 7-30`.
A broker-only node cannot register: the controller answers its
`BrokerRegistration` with `UNSUPPORTED_VERSION`, as Kafka's
`ClusterControlManager.registerBroker` does. Every 1.0.x release has both
checks. `crates/broker/tests/fatal_feature_level_exit.rs` and
`crates/raft/tests/unsupported_feature_level.rs` test them in process.

The gate also proves the refusal on the release images. The candidate formats a
node at `metadata.version` `4.4-IV2` (33) with
`unstable.feature.versions.enable`, and writes a record. The source then starts
on the same directory without that setting. The source must exit with a failure
status and Kafka's message. It must not change one byte of a log segment, a
checkpoint, `quorum-state` or another file. Only producer-state snapshots and
the offset and time indexes of a segment may change, because a broker rebuilds
them from the segment when it opens the log, as Kafka does. The candidate must then open the directory again
and read the record back.

### What runs

The `loaded lifecycle` job of the `ecosystem qualification` workflow runs the
gate. It runs when a dispatch sets `loaded_lifecycle` or `execute`. It needs
`broker_revision` and `broker_digest`, because the baseline manifest names a
broker older than the source. The job freezes the candidate input as the
`candidate` job does, then runs:

```sh
aspect check-qualification --manifest .qualification-input.json \
  --gate loaded-lifecycle --output qualification-results
```

`loaded-lifecycle` is a gate that a candidate can execute but that its manifest
does not list. So the qualified eight-gate baselines stay valid as they were
published, and the receipt binds to the candidate and the adapters as every
other receipt does.

The gate runs `the_sustained_load_contract_holds` and
`an_unsupported_rollback_is_refused_before_it_damages_data` of the broker's
`soak_contract` suite with Cargo, against the two image digests. Then it checks
the verdict and the evidence:

| Check | Passes when |
| :--- | :--- |
| `loaded-lifecycle`, `lifecycle-verdict` | The suite passes, and the verdict has no contract error, no reconciliation finding, no missed or unobserved deadline and no lifecycle failure. |
| `rolling-upgrade` | After the first to the fourth upgrade, exactly one to four nodes run the target image. |
| `certificate-rotation` | Four rotations ran, and each broker served a certificate that only its new CA verifies. |
| `reassignment` | The reassignment ran and completed. |
| `supported-rollback`, `rollback-eligible` | After the first to the fourth rollback, exactly one to four nodes run the source image again, and every checkpoint found the rollback supported. |
| `checkpoint-reconciliation` | No checkpoint found a configuration that differs from what the run set. |
| `unsupported-rollback-refused`, `unsupported-rollback-undamaged` | The source refused the unsupported rollback, changed nothing, and the candidate read the record back. |

### Evidence

The job uploads `qualification-loaded-lifecycle`, which holds the receipt and:

- `candidate.json` and `commands.txt`: the frozen candidate and the exact
  adapter commands, and `adapter-revision.txt`.
- `source-image.json` and `krabka-broker-image.json`: `docker image inspect` of
  the two images.
- `lifecycle/`: the contract as run, the schedule, the fault log, the
  telemetry, the alert history, `evidence.json` with every checkpoint, and
  `verdict.json`.
- `disruption.json`: each transition with the time that it held its broker, and
  each measured recovery in milliseconds.
- `unsupported-rollback/unsupported-rollback.json`: the exit code, the refusal,
  and the files that the refused rollback changed.

### What this does not cover

- **One host.** The four nodes are containers on one runner. A network
  partition between hosts, and a rollback of a node on a different kernel or
  disk, need a multi-host cluster, which a hosted runner does not give.
- **Classic topics only.** The upgrade smoke test (`packaging/upgrade-smoke.sh`)
  covers a diskless topic and a `DeleteRecords` floor across an upgrade, without
  load. The contract can add tiered and diskless paths; the executor already
  runs them for the soak.
- **The operator.** The `operator-lifecycle` gate upgrades and rotates through
  `krabka-operator`. This gate drives the release images directly, so that a
  failure names the broker and not the operator.

## Disaster-recovery declaration

The `disaster-recovery` gate declares three things before any run: what the capture holds, the load the drill runs under, and the recovery point and recovery time it must meet. This section holds the backup boundary. [`bench/soak/dr-secured.yaml`](../bench/soak/dr-secured.yaml) holds the load and the objectives, and says why each number is what it is. Change either one in a reviewed commit, never after looking at a result. The gate script in [`.aspect/check_qualification_gates.axl`](../.aspect/check_qualification_gates.axl) (`_DISASTER_RECOVERY`) is the drill itself.

The load and its judge are qualification adapters, like the gate script. The gate builds `krabka-soak-drill` and `krabka-soak-check` from the qualification checkout, not from the candidate, so every candidate is loaded and judged by the same reviewed code.

### Backup boundary

One `krabka-backup capture` writes four artifacts under `restore-inputs/<capture-id>/`, and a `manifest.json` that records the SHA-256 of each. The fourth, `diskless-wal-index.json`, is signed with the WORM key and binds the digests of the other three and the committed WAL cutoffs. Partition data is not in the capture: it is already in the archive.

| Item | Captured from | Restored by |
| :--- | :--- | :--- |
| Partition logs of tiered topics (`remote.storage.enable=true`) | The KIP-405 segments the source brokers archive, each with a signed WORM manifest | `krabka-restore`, at their original offsets, after each partition's chain is verified against its expected head |
| Remote-segment lifecycle and WORM chain receipts | `rlmm-snapshot`, the broker's RLMM snapshot | `krabka-restore --rlmm-snapshot`; archival resumes the same WORM chain |
| Diskless records | The WAL objects in the archive, up to the committed cutoffs that `diskless-wal-index.json` names | `krabka-restore --diskless-wal-capture` |
| Topic configs (for topics the archive holds), ACLs, SCRAM credentials, client quotas and finalized feature levels | `cluster-metadata.checkpoint`, the controller metadata checkpoint | `krabka-restore --metadata-snapshot` |
| Consumer group offsets | `group-offsets.json`, read by the `backup` principal over the SASL coordinator path | `krabka-backup restore-offsets` |

Everything else is outside the boundary. A drill does not claim to restore it, and the contract does not load it.

| Outside the boundary | Why |
| :--- | :--- |
| Classic topics without `remote.storage.enable` | Their segments are never archived. |
| Records of a tiered topic that were not in an archived segment at the loss | The active segment is archived only after it rolls. |
| Diskless records past the captured committed cutoffs | The capture names the cutoffs as of its own start. The uncommitted tail, and everything committed after the capture, is lost. |
| Dynamic broker configs | Restore drops them from the checkpoint. The operator re-applies them. |
| Delegation tokens, group configs and client-metrics configs | Restore drops them from the checkpoint. |
| Transaction state | `__transaction_state` is never captured. |
| Compacted topics | A compacted topic is never tiered. |
| Cluster id and node identity | Restore writes a new cluster id unless `--cluster-id` names one, and makes the target node the sole replica of every partition. |

`Contract::validate` holds a drill contract to this boundary. It refuses a classic path, because that path is outside the boundary, and a path that is not idempotent, because transaction state is never captured and a compacted topic is never tiered. It also refuses faults other than the loss, and an `rpo` shorter than `post_capture_load`.

### Declared load

The contract `dr-secured` (profile `disaster_recovery`) has two paths:

| Path | Storage | Partitions | Rate (floor) | Payload | Readers | Commits offsets |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `tiered-idempotent` | tiered (KIP-405) | 3 | 30/s (15/s) | 512 B | `read_uncommitted`, `read_committed` | yes |
| `diskless-idempotent` | diskless | 3 | 30/s (15/s) | 512 B | `read_uncommitted`, `read_committed` | yes |

Producers are idempotent, with `acks=all` and LZ4. The tiered topic rolls a segment every 2 s, so a record is in a closed, archivable segment within seconds of its acknowledgement.

The load connects over `SASL_SSL` with SCRAM-SHA-512 as the principal `soak`. `soak` is not a super user. It holds only these ACLs: WRITE, READ and DESCRIBE on the two soak topics, IdempotentWrite on the cluster, and READ and DESCRIBE on the prefixed group `soak-`. Every read and write it makes, before and after the loss, is an authorization decision of the cluster that serves it. The `backup` principal also gets READ, DESCRIBE and DescribeConfigs on the soak topics, and READ and DESCRIBE on the `soak-` groups, so the capture includes their offsets.

Before the load starts, the gate changes settings beyond the topics' own: a client quota (`producer_byte_rate` for the user `soak`) and a cluster-wide dynamic broker default (`log.retention.ms`). The quota is inside the boundary. The broker default is not.

The load starts once the topics exist. It runs unchanged through the fixed classic and diskless ledgers, the `dr-group` read, the TLS, SASL and ACL probes and the capture. It keeps running for `post_capture_load` (20 s) after the capture starts. Then the gate kills all three source brokers at once and stops the load with `SIGTERM`, which abandons every send in flight and writes what the load recorded.

### Recovery objectives

| Term | Declared | Definition |
| :--- | :--- | :--- |
| Capture boundary | | The instant `krabka-backup capture` starts (`capture_boundary_ms`). The manifest's own `captured_at_ms` is later and is kept as `capture_manifest_ms`. |
| Inside the boundary | zero loss | Every record acknowledged before the boundary must be read back, exactly once, by every declared reader of the restored cluster. The judge reports a missing one as `lost`, and the gate fails. Each committing group's restored offset must be no lower than its last commit acknowledged before the boundary, and no higher than the log end. |
| Beyond the boundary | | A record acknowledged after the boundary may be lost. `records_lost_beyond_boundary` counts the ones that were. |
| RPO | 60 s | The loss instant (`disaster_ms`) minus the acknowledgement time of the oldest acknowledged record that did not come back. Zero when every acknowledged record came back. |
| RTO | 5 min | From the loss to the instant the restored cluster has served every captured fixed-ledger record, each topic's settings, the `dr-group` positions, and an authenticated produce and consume by `soak`, a principal that holds only restored ACLs and a restored SCRAM credential, on each soak topic (`restore_ready_ms`). |
| `post_capture_load` | 20 s | How long the load runs after the boundary before the loss. |

RPO is not the capture's age. A diskless topic loses everything after the capture's committed cutoffs, which is about `post_capture_load` plus the capture's own run, so RPO measures what the restore actually lost. The capture's age at the loss is kept beside it as `capture_age_ms`.

RTO covers what an operator runs after the loss: reading the WORM heads, listing, verifying and fetching the capture, `krabka-restore`, starting the restored node, and `restore-offsets`. Comparing what the restored cluster served with the source, the access-control and denied-access checks, the resumed WORM chain, and the load's read-back and judgement come after it, and are timed as `verification_ms`.

The gate passes only when `krabka-soak-check judge` prints `dr-secured: PASS`, the verdict reports no `lost` record and no committed-offset finding, and `rpo-rto.json` holds a measured RPO and an RTO equal to the judge's. `rpo-rto.json` carries `capture_boundary_ms`, `capture_manifest_ms`, `disaster_ms`, `rpo_ms` and `rpo_objective`, `records_lost_beyond_boundary`, `capture_age_ms`, `rto_ms` and `rto_objective`, and `verification_ms`.

Access control must come back as it was. The sorted ACL listing and the users listing (SCRAM mechanisms and quotas) of the restored cluster must equal the source's (`acls-restored`, `scram-users-restored`).

### Negative checks

Each of these must fail closed. The names are the check names in the gate's evidence.

| Check | Must be refused |
| :--- | :--- |
| `invalid-credentials` | A capture with a wrong `backup` password. It writes no manifest and logs no secret. |
| `denied-acl` | A capture as `denied`, a principal with no ACL. It writes no manifest and logs no secret. |
| `invalid-certificate` | A client that trusts a different CA. |
| `denied-access-source`, `denied-access-restored` | A produce and consume by `denied` on `tiered-idempotent`, with an authorization error, on the source and again on the restored cluster. |
| `unlisted-topic-source`, `unlisted-topic-restored` | A produce and consume by `soak` on `classic`, a topic it holds no ACL for, with an authorization error, on both clusters. |
| `tampered-rejected`, `tampered-restore-rejected` | An archive copy with one segment overwritten, by `krabka-worm-verify` and by `krabka-restore`. |
| `missing-rejected`, `missing-restore-rejected` | An archive copy with one WORM manifest deleted, by both. |
| `bad-credentials` | Listing captures with wrong archive credentials. |
| `untrusted-worm-key` | A restore that trusts a different WORM key. |
| `capture-tampered-refused` | A capture copy whose `group-offsets.json` moves `dr-group` forward, by `krabka-backup verify`. |
| `capture-missing-refused` | A capture copy without its `rlmm-snapshot`, by `krabka-backup verify`. |
| `capture-tampered-restore-offsets-refused`, `tampered-offsets-not-committed` | `restore-offsets` from the tampered capture. The restored group positions stay unchanged. |

The archive and capture copies are separate, mutable buckets. The locked source bucket is never weakened.

### Recorded limits

The drill records these in `limits.txt`. They do not fail the gate.

- **WORM heads are read after the loss.** Restore requires each expected head to equal its archive tip exactly. Under load the tips move until the loss, so no head retained before the loss can match. The gate reads the heads from the locked bucket after the loss, before anything else touches it. The quiesced drill retained them before the loss.
- **Dynamic broker configs do not come back.** The gate compares the cluster-wide broker defaults of both clusters and records `broker-defaults: not restored (outside the declared boundary)`. Delegation tokens, group configs and client-metrics configs do not come back either, and the drill does not create them.
- **An authenticated restore that omits `--metadata-snapshot` is accepted.** The signed capture binds the checkpoint's digest, but the current release restores without it, and the restored cluster then has no ACLs and no SCRAM users. This is a restore defect, tracked in #1510, fixed by #1511. The gate records the outcome, not a check, until it is fixed.

### Known defects the drill fails on

These are defects, not limits. The gate fails on them.

- **A restore refuses an archive that grew after its RLMM snapshot** (#1520). Under load the source keeps uploading tiered segments between the capture and the loss. v1.0.5 refuses those segments instead of restoring them as the newer state. Fixed by #1523.
- **An upload without its WORM manifest refuses the whole archive** (#1521). A source lost while it uploads leaves a segment no signed claim names. Fixed by #1523, which skips that segment and reports it.
- **A restored diskless partition refuses writes** (#1524). The restored broker serves the captured diskless records, but a new produce fails with `offset mismatch`. The `authenticated-round-trip-diskless-idempotent` check fails on it. Not yet fixed.
- **A restored diskless partition serves only some of its records** (#1525). Under the declared load, the restore materializes every committed diskless record, but a read from the earliest offset returns a small fraction of them. The soak judge reports the rest `lost`, and the RPO exceeds its objective. Not yet fixed.

### Run the drill alone

The `dr_drill` input runs only the prepare job and the `disaster-recovery` gate against the same frozen candidate. It uploads the evidence and writes the measured RPO and RTO to the step summary. It never publishes a qualification release.

```sh
gh workflow run qualification.yml \
  -f manifest=qualification/ecosystem-eight-gate-baseline.json \
  -F dr_drill=true \
  -f broker_revision="$DELIVERED_COMMIT" \
  -f broker_digest="$DELIVERED_IMAGE_DIGEST"
```

## Local execution and evidence

```sh
aspect check-qualification --new-candidate qualification/candidate.json
# Edit candidate.json to name the intended immutable revisions and artifacts.
aspect check-qualification --manifest qualification/candidate.json
aspect check-qualification --manifest qualification/candidate.json \
  --gate cli-admin --output qualification-run-1
```

Every gate invocation uses a new temporary checkout. Choose a fresh `--output`
for retries; existing evidence is never silently replaced. Failed attempts keep
their command log and scratch directory for diagnosis. Successful receipts name
the gate, candidate SHA-256, adapter SHA-256, executed check count and names,
command hash, and log hash. The candidate identity covers its timestamp,
architecture, all repository revisions/default branches, and artifact
references/digests. Reordering components or adding output evidence does not
change that identity.

Run all eight gates into the same output directory, then assemble:

```sh
aspect check-qualification --manifest qualification/candidate.json \
  --assemble --output qualification-run-1 \
  --publish-prefix https://github.com/krabka-io/krabka-broker/releases/download/qualification-RUN_ID
aspect check-qualification --manifest qualification-run-1/manifest.json --final
```

Assembly verifies every receipt and retained log/command hash before writing a
qualified manifest, report and per-gate bundles. It rejects another candidate's
receipt, missing gates, failed commands and zero executed checks. Local assembly
does not publish anything. The workflow uploads the assembled artifacts to a
draft `qualification-RUN_ID` release, downloads them to verify checksums, then
publishes it as a prerelease without changing `latest`. Published qualification
releases are never overwritten by retries.
