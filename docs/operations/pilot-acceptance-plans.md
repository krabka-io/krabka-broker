# Operator Pilot Acceptance Plans Design

Two workload plans that an operator who did not build krabka follows to install it, migrate onto it from Apache Kafka 4.3.1, cut over, roll back, and recover, with the checks that decide whether the run passed.

This document is the M29 contract for [#1398](https://github.com/krabka-io/krabka-broker/issues/1398). [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) executes it. It names no operator and arranges no access. The [maintainer actions](#maintainer-actions) table lists what a person has to do before a run can start. Each `TBD(Mn)` marker in this document is a value that action `Mn` supplies.

## Design Goals

- **Two access patterns that fail differently.** Plan A is a high-throughput event pipeline. It stresses partition count, fan-out, compaction, the remote tier and schema-encoded records. Plan B is a transactional stream processor. It stresses exactly-once visibility, per-key order, the next-generation group protocol and certificate identities. A defect that one plan hides, the other plan should show.
- **Only published instructions.** The operator follows the documents under `docs/operations/` at one pinned revision. A step that needs knowledge that is not in those documents is a defect in the documents, and the run records it.
- **Criteria fixed before the run.** Every number that decides a verdict is in this document, or in the sign-off record that fills its `TBD` markers, before the run starts. This is the rule of the [sustained-load contract](../../crates/bench-driver/docs/soak-contract-design.md): change a criterion in a reviewed commit, never after a result.
- **Existing tools only.** The plans use the krabka image and its bundled tools, the krabka-operator chart, the JVM admin tools and MirrorMaker 2 from the Apache Kafka 4.3.1 distribution, and the clients the [ecosystem support](ecosystem-support.md) matrix names. They add no harness, no framework and no script.
- **Honest labels.** An external pilot and an independent acceptance run produce evidence of different strength. The evidence says which one it is, and an independent run never counts as an external pilot.

## Architecture Overview

A run is a sequence of phases. Each phase has entry checks, the runbook sections that the operator follows, and exit checks. A phase that fails its exit checks stops the run. The operator does not skip forward.

| Phase | What happens | Exit check |
| :--- | :--- | :--- |
| P0. Prepare | The maintainer completes the blocking [maintainer actions](#maintainer-actions). The operator records the plan revision, the candidate and the source cluster's shape. | Sign-off record is complete. Every blocking gap issue is closed. |
| P1. Install | The operator installs krabka from the candidate's image digest and configures security. | Health and security checks of the plan pass. |
| P2. Mirror | MirrorMaker 2 copies the source cluster onto krabka. | Mirror lag is at or near zero for the plan's settle window. Parity checks pass. |
| P3. Cut over | Clients move to krabka in the plan's cutover style. | Cutover checks pass. |
| P4. Run | The workload runs on krabka for the plan's observation window. One alert-response exercise takes place. | Success criteria hold for the whole window. |
| P5. Roll back (exercise) | The operator rehearses the plan's rollback on a declared subset, inside its boundary. | Rollback checks pass. The subset returns to krabka. |
| P6. Recover | The operator runs the plan's recovery drill against an isolated restore target. | Measured RPO and RTO are inside the declared values. Reconciliation passes. |
| P7. Report | The operator assembles the evidence and the intervention log. The maintainer verifies the evidence. | Evidence is complete and labeled. |

A run passes when every phase passes and the intervention log has no entry for P1 or P6. P1 is the installation, and P6 is the recovery drill. The M29 completion criteria need both of them "without maintainer intervention". An intervention in P2 to P5 does not fail the run, but each one opens a fix, and [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) reruns the affected phase after the fix is verified.

Every run produces one evidence set:

| Item | Content |
| :--- | :--- |
| Identity | Run kind (external pilot or independent acceptance run), plan letter, run number, plan commit SHA, sign-off record. |
| Candidate | Broker release tag and image digest, krabka-operator chart and image digests where used, the qualified manifest that names them. |
| Source | Apache Kafka 4.3.1 image digest or package checksum, broker count, topic and partition list, `kafka-features describe` output. |
| Commands | Every command the operator ran, with its UTC start time and exit code. Secrets are redacted. |
| Checks | The output of every check in the plan, with UTC timestamps. |
| Canary ledgers | The JSON line output of the canary producers and consumers. |
| Metrics | A `/metrics` scrape from every broker at the end of each phase. |
| Intervention log | Every entry, as defined in [maintainer intervention](#what-counts-as-a-maintainer-intervention). |
| Findings | Every confusing step, client mismatch and defect, each with its issue link. |

## Key Design Decisions

### Choose the workloads from the ecosystem matrix

Both plans use only rows of the [What works](ecosystem-support.md#what-works) table: the admin command line, the JVM clients and librdkafka, the protocol-based lag monitors, and MirrorMaker 2 with a schema registry. Neither plan uses a tool from the [What does not work](ecosystem-support.md#what-does-not-work) table. A pilot that needed Cruise Control or a JMX exporter would measure a known limit, not the migration.

Plan A takes the "consumer-lag monitors" row. Those monitors decode `__consumer_offsets` themselves, and the matrix calls them the class with the sharpest failure mode. Plan B takes the transaction and group rows that the matrix covers through the JVM clients: Kafka Streams with `exactly_once_v2` ([KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging), [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)) and a [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) consumer. The suites `jvm_streams_app`, `jvm_consumer_group_next_gen` and `mtls_principal_mapping` already cover each part in isolation. The pilot covers them together, under an operator.

Plan B does not use share groups ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)). A share group gives no per-key order, and per-key order is the property that Plan B exists to check. The KIP matrix also pins the share-group evidence to Kafka 4.1.0 only.

### Use two different installation paths

Plan A installs on Kubernetes with the krabka-operator chart that the qualified manifest names. Plan B installs the container image with a TOML file on hosts that the operator manages, as [Deploy](deploy.md) describes. The two paths share no installation step, so one plan cannot hide a defect in the other plan's path. Both paths start from an image digest, never from a mutable tag.

### Use two different cutover styles

Plan A uses the procedure of [Migrate from Apache Kafka](migrate-from-kafka.md) as it is written. MirrorMaker 2 runs with `DefaultReplicationPolicy`, so mirrored topics carry the `prod.` prefix. Consumer groups move one at a time, and the producers move last. Per-key order across the cutover point is not a requirement of an event pipeline, so the short period when a consumer reads both the mirrored topic and the new local topic is acceptable.

Plan B cannot accept that period. A Kafka Streams aggregation that reads one key from two topics can count a record twice or in the wrong window. Plan B uses `IdentityReplicationPolicy`, a shadow run of the Streams application on krabka, and one quiesced switch. All writers stop, the mirror drains, the shadow output is compared with the source output, and then every client moves in one declared window.

### Measure loss with canary ledgers from Kafka's own tools

An operator's production application is not an instrument. It may not log what it acknowledged, and its owner may not consent to share its data. So each plan adds canary topics that run beside the real workload through every phase. `kafka-verifiable-producer` writes sequential integer values and prints one `producer_send_success` JSON line for each acknowledged record. `kafka-verifiable-consumer` prints the offsets it consumed and committed. Both tools ship in the Apache Kafka 4.3.1 distribution, and the `jvm_acceptance_sasl` suite already runs `kafka-verifiable-producer` against krabka. The ledgers give an exact count of acknowledged, mirrored, consumed and re-read records without a new tool. The operator's own metrics give the throughput and latency criteria.

### Tie rollback to persisted-format eligibility

There are two kinds of rollback, and the plans keep them apart.

- **Migration rollback** moves clients back to the Apache Kafka source. Each plan defines where that stops being possible.
- **Version rollback** moves krabka from build N to build N-1. [#1396](https://github.com/krabka-io/krabka-broker/issues/1396) defines its eligibility from [Persisted formats](../persisted_formats.md). Within 1.x, a roll back to an earlier build works until the operator finalizes a feature level that the earlier build does not support. [Rolling upgrade](deploy.md#rolling-upgrade) states the same rule.

So both plans forbid `kafka-features upgrade` during the run, except where a plan step names it. The operator records `kafka-features describe` at P1 and at the end of each phase, and a change between two records is a finding. If the maintainer ships a fix build during the run, the operator rolls it with [Rolling upgrade](deploy.md#rolling-upgrade). That roll stays version-rollback-eligible only while the finalized levels do not change. When #1396 publishes its rollback eligibility table, the sign-off record links the row for the candidate (action M7).

### Recover into an isolated target

[#1397](https://github.com/krabka-io/krabka-broker/issues/1397) destroys source data, but only in its own isolated qualification cluster. A pilot never destroys an operator's data. Each plan's drill restores the newest verifying capture from the production archive into a scratch target, as [Check a copy](backup-restore.md#check-a-copy) and the [restore-from-archive](runbooks/restore-from-archive.md) runbook describe. The drill then reconciles the scratch cluster against the production cluster at the capture boundary. The drill measures RPO and RTO with the definitions of the `disaster-recovery` qualification gate, so a pilot result and a #1397 result use the same units. `rpo_ms` is the age of the capture when the drill declares the loss. `rto_ms` runs from the declared loss to a cluster that has served every captured record, each topic's settings and the group positions back to a reader.

The archive bounds what a drill can restore. A record reaches the archive only when its segment rolls and the remote copy completes. Compacted topics cannot be tiered, so no archive holds them. The plans set `segment.ms` on every tiered topic to bound the first limit, and they protect compacted topics another way (see [gap issues](#gap-issues)).

### Define a maintainer intervention narrowly and record every one

M29 asks for an install and a recovery drill "without maintainer intervention". The term needs a definition that a reviewer can check from the evidence.

#### What counts as a maintainer intervention

A maintainer intervention is any of these, from a person who has built or maintained krabka, or from an agent that acts for that person:

1. A command that the maintainer runs on the pilot's clusters, hosts or buckets, or a configuration value that the maintainer writes there.
2. Information that changes what the operator does next, and that the published documents at the plan revision do not contain. A private message, a call, a screen share and a public issue comment all count.
3. A build, a patch, a configuration file or an image that is not the declared candidate.
4. A decision about an ambiguous runbook step or about a check result, made for the operator.
5. A change to a success criterion, a deadline or a `TBD` value after P0.

These are not interventions:

- A maintainer answer that points only to a section that already exists in the documents at the plan revision. The log still records it as a findability finding.
- A maintainer action that the plan itself requires, such as the sign-off in P0 and the evidence verification in P7.
- An escalation that a runbook's own **Escalate** section tells the operator to make. The answer to the escalation is an intervention if it meets one of the five conditions above.

#### How an intervention is recorded

The operator, or the independent runner, keeps the intervention log as a part of the evidence. A maintainer who intervenes tells the operator at once, so the log is complete. Each entry has:

| Field | Content |
| :--- | :--- |
| Time | UTC start and end. |
| Phase and step | The phase, and the runbook section in effect, as a link with its anchor. |
| Category | One of the five numbered conditions above. |
| Asked | What the operator needed, in the operator's words. |
| Given | What the maintainer did or said. |
| In the docs | `no`, or the link to the section that already had the answer. |
| Issue | The issue in milestone 29 that fixes the cause. Every entry with `In the docs: no` has one. |
| Rerun | The phase that #1399 reruns after the fix, and the result of that rerun. |

## Plan A: High-Throughput Event Pipeline

### Workload

The workload is an order and click event pipeline. Many producers append events, several independent consumer groups read every event, and one compacted topic holds the latest profile of each customer. Values are Avro, registered in a schema registry.

| Property | Plan value | Operator value |
| :--- | :--- | :--- |
| Source cluster | Apache Kafka 4.3.1, KRaft, 3 or more brokers, RF=3, `min.insync.replicas=2` | TBD(M1) |
| Event topics | 20 or more topics, 1,500 to 3,000 partitions in total, `cleanup.policy=delete`, tiered | TBD(M1) |
| Compacted topic | 1 topic, 24 to 48 partitions, 1,000,000 or more distinct keys, not tiered | TBD(M1) |
| Produce rate | 20 MiB/s steady, with one 1-hour peak at 2 times steady, records about 1 KiB | TBD(M1) |
| Consumer fan-out | 3 or more groups for each event topic, 60 MiB/s or more egress at steady state | TBD(M1) |
| Retention | Local `local.retention.ms` of 6 hours, total `retention.ms` of 7 days, `segment.ms` of 10 minutes | TBD(M1) |
| Retained bytes to mirror | At most what the target's archive and disks hold, sized with [Capacity](capacity.md) | TBD(M1) |

The partition count stays inside the 10,000-partition tier that [Broker performance results](performance-results.md#partition-envelope) records as passing. That page states that the tier is the highest tested, not a supported limit, and the plan does not claim more.

### Clients and versions

| Role | Client | Settings |
| :--- | :--- | :--- |
| Producers | Apache Kafka Java client 4.3.1, and one librdkafka 2.x based client | `enable.idempotence=true`, `acks=all`, `compression.type=lz4` |
| Consumers | Java 4.3.1 and librdkafka 2.x, classic group protocol | `enable.auto.commit=false`, commit after processing |
| Schema registry | The Confluent Schema Registry image that the `registry-migration` qualification gate pins | Avro, `BACKWARD` compatibility |
| Lag monitor | `kafka-lag-exporter` or Burrow | Reads `__consumer_offsets` |
| Admin | The JVM admin tools from 4.3.1 | Through a SCRAM admin principal |
| Mirror | MirrorMaker 2 from 4.3.1, dedicated mode, `connect-mirror-maker.sh` | See [Migration](#migration-and-cutover-checks) |
| Canary | `kafka-verifiable-producer` and `kafka-verifiable-consumer` from 4.3.1 | One canary event topic with 12 partitions, at 100 records/s |

### Security

- The client listener is `SaslSsl` with a server certificate (one-way TLS) and `SCRAM-SHA-512`. The inter-broker listener follows the operator's own network policy.
- The authorizer is `type = "simple"`, with `allow_everyone_if_no_acl_found = false`. The admin principal is the only `super_users` entry.
- Each producer, consumer group, the registry, the lag monitor, MirrorMaker 2 and `krabka-backup` has its own SCRAM principal and only the ACLs it needs.
- The operator creates each SCRAM principal on krabka as a `KafkaUser`, which gives it a generated password in a Secret. MirrorMaker 2 does not carry credentials. A client that has to keep its source password gets it set with `kafka-configs` instead, and the evidence records which principals took which path.

### Installation

The operator installs the krabka-operator chart and the broker image by digest from the candidate's qualified manifest, as [Kubernetes operator](deploy.md#kubernetes-operator) describes. The `Kafka` resource sets `tieredStorage` for the archive bucket, a client listener with `tls: true` and `scram-sha-512`, and `authorization` of `type: simple`. Each principal is a `KafkaUser`, and each topic is a `KafkaTopic`. The operator then runs the `krabka-backup capture` CronJob from [Capture the inputs](backup-restore.md#capture-the-inputs), hourly, with the backup principal.

Installation exit checks:

1. `/readyz` answers 200 on every broker, and `kafka-metadata-quorum describe --status` names one leader and every voter.
2. `krabka_broker_under_replicated_partitions` and `krabka_broker_offline_partitions_count` are zero on every broker.
3. A client with no credentials fails to authenticate. A client with a valid credential and no ACL gets `TOPIC_AUTHORIZATION_FAILED`. The [authentication-failures](runbooks/authentication-failures.md) and [authorization-denials](runbooks/authorization-denials.md) alerts fire for those attempts and resolve after them.
4. The first `krabka-backup verify` exits `0`.
5. `kafka-features describe` output is recorded.

### Migration and cutover checks

MirrorMaker 2 follows [Configure MirrorMaker 2](migrate-from-kafka.md#configure-mirrormaker-2) with `DefaultReplicationPolicy`, `sync.group.offsets.enabled = true`, `offset.lag.max = 100` and `offset-syncs.topic.location = target`. The operator creates the target topics before MirrorMaker 2 starts. Each target topic has the source's partition count and the target's tier and retention settings. Gap issue [#1497](https://github.com/krabka-io/krabka-broker/issues/1497) asks for that step in the migration guide. The `_schemas` topic is mirrored, and the target registry stays read-only until the producer cutover, as the `registry-migration` gate's forward handoff does. Gap issue [#1498](https://github.com/krabka-io/krabka-broker/issues/1498) asks for that procedure in the guide.

Mirror parity checks, at the end of P2:

| Check | Pass condition |
| :--- | :--- |
| Mirror lag | MirrorMaker 2's own group on the source has lag of at most 1,000 records on every partition for 30 minutes. |
| Record counts | For each canary partition, every acknowledged value in the source producer ledger is present on the mirrored partition. Missing values: 0. |
| Partition counts | Equal for every mirrored topic. |
| Topic configs | `kafka-configs --describe` on each mirrored topic matches the source for every key the plan does not set on purpose. Each intended difference is in the sign-off record. |
| ACL parity | Every source ACL, with the topic name mapped to its `prod.` name, exists on krabka. MirrorMaker 2's ACL sync copies topic ACLs only, and gap issue [#1496](https://github.com/krabka-io/krabka-broker/issues/1496) records that it does not copy a `WRITE` grant. The operator creates the rest with `kafka-acls` and records the diff, which has to be empty. |
| SCRAM principals | Every client principal authenticates against krabka. |
| Schemas | Every subject and version on the source registry has the same schema id on the target registry. |

Cutover follows [Cut the consumers over](migrate-from-kafka.md#cut-the-consumers-over) and then [Cut the producers over](migrate-from-kafka.md#cut-the-producers-over). Consumers subscribe to the pattern `(prod\.)?<topic>`, so after the producer cutover they read both the mirrored topic and the local topic that the producers now write. Gap issue [#1496](https://github.com/krabka-io/krabka-broker/issues/1496) asks the guide to state which topic name producers write after the cutover.

Cutover checks, for each consumer group:

| Check | Pass condition |
| :--- | :--- |
| Position | `kafka-consumer-groups --describe` on krabka names the mirrored topic, and the group's lag falls for 5 minutes after the start. |
| Skipped records | The canary consumer's first value after the cutover is at most its last value before the cutover plus 1. Skipped values: 0. |
| Re-read records | The canary consumer re-reads at most `offset.lag.max` values for each partition. The run records the measured count. |
| Lag monitor | The lag monitor reports the group on krabka with a non-zero member count and a lag within 10% of `kafka-consumer-groups`. A report of zero consumers on a group that runs is a failure. |

### Success criteria

The observation window P4 is 7 days on krabka after the last producer moves.

| Criterion | Target |
| :--- | :--- |
| Acknowledged canary records lost | 0 |
| Duplicates in the canary ledger, other than re-reads at the cutover and documented producer retries | 0 |
| Produce p99 latency, `acks=all`, at steady rate | 250 ms or less, measured by the operator's producer metrics, excluding the declared fault exercise |
| End-to-end p99 latency of the canary | 2 s or less |
| Steady throughput | 95% or more of the produce rate in the workload table, for every hour of P4 |
| Remote copy lag | `KrabkaRemoteCopyLagGrowing` is not firing at the end of P4 |
| Log cleaner | `KrabkaLogCleanerFailures` and `KrabkaUncleanablePartitions` do not fire |
| Consumer lag | Every group's lag returns under 10,000 records within 15 minutes after the peak hour |
| Alert response | During P4 the operator stops one broker with `SIGTERM` at a time that the runner does not announce. The operator follows [broker-down](runbooks/broker-down.md) and [under-replicated-partitions](runbooks/under-replicated-partitions.md). Leadership moves within 15 s, and the ISR is full again within 2 minutes after the restart. These are the deadlines of `bench/soak/rf3-72h.yaml`. |

### Rollback boundaries

| Boundary | Migration rollback | How |
| :--- | :--- | :--- |
| Before the first consumer group moves | Free | Stop MirrorMaker 2. Delete the target topics. |
| A group has moved, and producers still write to the source | Per group, with re-reads | Stop the group on krabka. Start it on the source. It resumes from its last source commit, so it re-reads what it read on krabka. |
| Producers write to krabka | Until the end of P4 plus 7 days, with a reverse flow | Follow [Roll back](migrate-from-kafka.md#roll-back). The reverse flow carries the local topics that producers write on krabka back to the source as `krabka.<topic>`. The source consumers then subscribe to `(krabka\.)?<topic>`. |
| The source cluster is gone | None | Recovery from now on is the [recovery drill](#recovery-drill) path. |

P5 exercises the second row on one canary group and one real group that the operator selects. The rollback checks are the same skipped-record and re-read checks as the cutover, in the other direction. Version rollback follows [Tie rollback to persisted-format eligibility](#tie-rollback-to-persisted-format-eligibility).

### Recovery drill

The drill is a full restore rehearsal from the production archive into a scratch cluster on separate storage. It follows [Check a copy](backup-restore.md#check-a-copy) and, for each step, the [restore-from-archive](runbooks/restore-from-archive.md) runbook: [Confirm](runbooks/restore-from-archive.md#confirm), [Diagnose](runbooks/restore-from-archive.md#diagnose) and steps 1 to 5 of [Fix](runbooks/restore-from-archive.md#fix). Step 6 and step 7 do not apply to a scratch target. The drill uses `--cluster-id` with the production cluster id and no time bound.

| Declared value | Proposed | Basis |
| :--- | :--- | :--- |
| RPO for tiered records | 30 minutes or less | `segment.ms` of 10 minutes, plus the remote copy, plus margin |
| RPO for group offsets | 70 minutes or less | The hourly capture, plus margin |
| RTO to a serving scratch cluster | TBD(M7), from the #1397 published value for the same data volume | #1397 declares RPO and RTO before its own run |

Reconciliation checks:

| Check | Pass condition |
| :--- | :--- |
| Capture | `krabka-backup verify` exits `0` for the capture the drill uses. |
| Records | For each canary partition, every value acknowledged before the capture boundary and older than the RPO is present at its original offset. |
| Topic settings | `kafka-configs --describe` on the scratch cluster matches production at the capture for every restored topic. |
| ACLs and credentials | `kafka-acls --list` matches production at the capture. A pilot principal authenticates with its SCRAM password. |
| Group offsets | After `krabka-backup restore-offsets`, every captured group's position equals the captured position. |
| Compacted topic and schemas | Restored from the protection that gap issue [#1499](https://github.com/krabka-io/krabka-broker/issues/1499) defines. The archive does not hold them, so a drill without that protection fails this check. |
| Measured values | `rpo_ms` and `rto_ms` are inside the declared values. |

### Runbook sections the operator follows

| Phase | Sections |
| :--- | :--- |
| P1 | [Install](deploy.md#install), [Kubernetes operator](deploy.md#kubernetes-operator), [Health checks](deploy.md#health-checks), [Capture the inputs](backup-restore.md#capture-the-inputs), [How often](backup-restore.md#how-often) |
| P2 | [Before you start](migrate-from-kafka.md#before-you-start), [Configure MirrorMaker 2](migrate-from-kafka.md#configure-mirrormaker-2), [Watch the mirror catch up](migrate-from-kafka.md#watch-the-mirror-catch-up), [ACL sync](migrate-from-kafka.md#acl-sync) |
| P3 | [Cut the consumers over](migrate-from-kafka.md#cut-the-consumers-over), [Cut the producers over](migrate-from-kafka.md#cut-the-producers-over), [Clean up](migrate-from-kafka.md#clean-up) |
| P4 | [broker-down](runbooks/broker-down.md), [under-replicated-partitions](runbooks/under-replicated-partitions.md), [consumer-group-lag](runbooks/consumer-group-lag.md), [remote-copy-lag](runbooks/remote-copy-lag.md), [log-cleaner-stalled](runbooks/log-cleaner-stalled.md), and any runbook whose alert fires |
| P5 | [Roll back](migrate-from-kafka.md#roll-back), [Rolling upgrade](deploy.md#rolling-upgrade) if a fix build is rolled |
| P6 | [Check a copy](backup-restore.md#check-a-copy), [What comes back](backup-restore.md#what-comes-back), [restore-from-archive](runbooks/restore-from-archive.md) |

## Plan B: Transactional Stream Processing

### Workload

The workload is a payment ledger. Upstream services write payment events in transactions. A Kafka Streams application re-keys them by account, counts and sums them in 1-minute tumbling windows, and writes the results. A downstream service reads the results with `read_committed` and needs per-account order. The topology has the shape of the `jvm_streams_app` suite's application: source, `selectKey`, repartition, windowed aggregate, sink.

| Property | Plan value | Operator value |
| :--- | :--- | :--- |
| Source cluster | Apache Kafka 4.3.1, KRaft, 3 or more brokers, RF=3, `min.insync.replicas=2`, `transaction.version` and `group.version` as the source finalized them | TBD(M2) |
| Input topics | 2 to 4 topics, 48 partitions each, tiered, `segment.ms` of 10 minutes | TBD(M2) |
| Output topic | 1 topic, 48 partitions, tiered | TBD(M2) |
| Streams internal topics | One repartition topic and one changelog topic, as Streams creates them | Created by the application |
| Rate | 2,000 records/s steady, records about 512 B, 20 records for each transaction, 1 abort in 50 | TBD(M2) |
| Keys | 100,000 or more accounts | TBD(M2) |
| Window retention | Grace of 1 minute, so the input retention covers every open window | TBD(M2) |

### Clients and versions

| Role | Client | Settings |
| :--- | :--- | :--- |
| Upstream producers | Java client 4.3.1, transactional | One `transactional.id` for each producer instance |
| Stream processor | Kafka Streams 4.3.1 | `processing.guarantee=exactly_once_v2`. The rebalance protocol is TBD(M6): `classic` is the default, and `group.protocol=streams` ([KIP-1071](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071%3A+Streams+Rebalance+Protocol)) is the variant. |
| Downstream consumer | Java client 4.3.1 | `group.protocol=consumer` (KIP-848), `isolation.level=read_committed` |
| Admin | The JVM admin tools from 4.3.1, `kafka-transactions` included ([KIP-664](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions)) | Through an mTLS admin principal |
| Mirror | MirrorMaker 2 from 4.3.1, dedicated mode | See [Migration](#migration-and-cutover-checks-1) |
| Canary | `kafka-verifiable-producer` into a canary input topic, the same Streams application on it, and `kafka-verifiable-consumer` with `isolation.level=read_committed` on its output | 6 partitions, 50 records/s |

### Security

- The client listener is `Ssl` with `client_auth = "Required"` and a `client_ca_path`. Every client presents a certificate.
- `principal_mapping_rules` maps the Subject DN to a short name, for example `RULE:^CN=([a-z0-9-]+),OU=payments,.*$/$1/L` followed by `DEFAULT`. [KIP-371](https://cwiki.apache.org/confluence/display/KAFKA/KIP-371%3A+Add+a+configuration+to+build+custom+SSL+principal+name) defines the rule syntax, and the [configuration reference](../config-reference.md) states where krabka reads the rules.
- The authorizer is `type = "simple"`. The ACLs name `Topic`, `Group` and `TransactionalId` resources for each principal. The Streams principal has `Write` and `Describe` on its `TransactionalId` prefix and `Read` on its group.
- The operator installs certificates with the operator's own PKI. Certificate rotation during P4 is part of the alert-response exercise.

### Installation

The operator installs the broker image by digest on three hosts, formats each log directory with `krabka-format` and a KIP-853 initial voter set, and writes the TOML file. [Format the log directory](deploy.md#format-the-log-directory), [Configure](deploy.md#configure) and [Start](deploy.md#start) describe each step. The `krabka-backup capture` job runs hourly through the operator's own scheduler with an mTLS client properties file. Gap issue [#1500](https://github.com/krabka-io/krabka-broker/issues/1500) asks the backup guide to show that file.

Installation exit checks:

1. The same quorum, health and replication checks as Plan A.
2. A client with no certificate fails the TLS handshake. A client with a certificate from another CA fails the handshake. A valid client whose mapped principal has no ACL gets an authorization error.
3. `kafka-acls --list` shows every ACL under the mapped short names, not under full DNs.
4. `kafka-features describe` shows `transaction.version` and `group.version` at the levels that the plan's clients need. The output is recorded.

### Migration and cutover checks

MirrorMaker 2 runs with `IdentityReplicationPolicy`, `offset-syncs.topic.location = target` and `sync.group.offsets.enabled = false`. It mirrors the input topics only. It does not mirror the output topic or the Streams internal topics, and it does not sync the Streams group or the downstream group. The source-side consumer of MirrorMaker 2 reads with `isolation.level=read_committed`, so the mirror carries no aborted record. Gap issue [#1495](https://github.com/krabka-io/krabka-broker/issues/1495) asks the migration guide to describe each of these settings for a transactional source.

The cutover has three stages.

1. **Shadow.** The operator starts the Streams application on krabka with the same `application.id` and `auto.offset.reset=earliest`. It reads the mirrored inputs, rebuilds its state, and writes the output topic on krabka. The source application keeps running and stays the system of record.
2. **Quiesce.** The operator stops the upstream producers on the source and starts the write freeze. The declared freeze is TBD(M2), proposed 15 minutes or less. The mirror drains. Both Streams applications reach zero lag. `kafka-transactions list` on both clusters shows no ongoing transaction.
3. **Switch.** The operator compares the outputs, then points the upstream producers and the downstream consumer at krabka, then stops the source application and MirrorMaker 2.

Shadow and quiesce checks:

| Check | Pass condition |
| :--- | :--- |
| Aborted records | The canary input on krabka, read with `read_uncommitted`, holds no value that the source producer ledger reports as aborted. |
| Committed records | Every committed canary input value is on krabka. Missing values: 0. |
| Output equality | For every closed window that ends before the freeze, the count and the sum for each account on the krabka output equal the source output. Differences: 0. |
| Order | For each account, the downstream canary on krabka reads windows in increasing window-start order. Violations: 0. |
| ACL parity | Every source ACL has an equal ACL on krabka under the mapped principal. The diff is empty. |
| Freeze | The write freeze ends inside the declared value. |

Switch checks:

| Check | Pass condition |
| :--- | :--- |
| Downstream position | The downstream group starts on krabka at the first output window after the last window it read on the source. The operator sets the position with `kafka-consumer-groups --reset-offsets --to-datetime` at the freeze time. Gaps and repeats in window sequence: 0. |
| First transactions | The first transaction of each upstream producer on krabka commits. `kafka-transactions describe` shows its `transactional.id` in `CompleteCommit`. |

### Success criteria

The observation window P4 is 7 days on krabka after the switch.

| Criterion | Target |
| :--- | :--- |
| Committed canary records lost | 0 |
| Aborted canary records visible to a `read_committed` reader | 0 |
| Duplicates in the `read_committed` canary output | 0 |
| Per-account order violations in the output | 0 |
| End-to-end p99 latency, input commit to output visible | 5 s or less, at steady rate, excluding the declared fault exercise |
| Steady throughput | 95% or more of the rate in the workload table, for every hour of P4 |
| Hanging transactions | `kafka-transactions find-hanging` reports none at the end of each day |
| Alert response | During P4 the operator rotates the broker certificates and then restarts the controller leader at a time that the runner does not announce. The operator follows [authentication-failures](runbooks/authentication-failures.md), [no-active-controller](runbooks/no-active-controller.md) and [controller-leader-flapping](runbooks/controller-leader-flapping.md) if their alerts fire. A new controller leader is elected within 15 s, and the canary shows no loss and no duplicate. |

### Rollback boundaries

| Boundary | Migration rollback | How |
| :--- | :--- | :--- |
| During the shadow stage | Free | Stop the krabka application and MirrorMaker 2. The source never stopped. |
| During the write freeze, before the switch | Free | Restart the upstream producers on the source. Discard the krabka topics. |
| After the switch, up to 24 hours | With duplicate output for the krabka period | Quiesce krabka the same way. Run a reverse MirrorMaker 2 flow, krabka to source, with `IdentityReplicationPolicy` and `read_committed`, for the input topics only. The forward flow is already stopped, so the identity policy cannot loop. The source application resumes from its own committed offsets and processes the records that were written on krabka. The downstream consumer re-reads the windows of the krabka period from the source, so it has to tolerate one repeat of each window. |
| After 24 hours | None | The plan does not offer a migration rollback. Recovery is the [recovery drill](#recovery-drill-1) path. |

P5 exercises the third row with the canary pipeline only: canary producers, the canary Streams application and the canary consumer. The checks are the shadow and switch checks in the other direction, with the one declared repeat for each window. Version rollback follows [Tie rollback to persisted-format eligibility](#tie-rollback-to-persisted-format-eligibility). In Plan B, a change of `transaction.version` or `group.version` is also a change of client behavior, so the operator does not change either level during the run.

### Recovery drill

The drill is a point-in-time restore, which a bad-write incident needs. The operator picks a timestamp T inside P4 and restores the input and output topics into a scratch cluster with `--to-timestamp` T. The drill follows [restore-from-archive](runbooks/restore-from-archive.md): [Confirm](runbooks/restore-from-archive.md#confirm), [Diagnose](runbooks/restore-from-archive.md#diagnose) with the `--dry-run` plan, and steps 1 to 5 of [Fix](runbooks/restore-from-archive.md#fix).

The Streams changelog is compacted, so the archive does not hold it. A restore also does not keep in-flight transaction state, as [What comes back](backup-restore.md#what-comes-back) states. So the drill starts the canary Streams application on the scratch cluster with a new `application.id`. The application rebuilds its state from the restored inputs.

| Declared value | Proposed | Basis |
| :--- | :--- | :--- |
| RPO for committed records | 30 minutes or less before T | `segment.ms` of 10 minutes, plus the remote copy, plus margin |
| RTO to a serving scratch cluster | TBD(M7), from the #1397 published value for the same data volume | #1397 declares RPO and RTO before its own run |

Reconciliation checks:

| Check | Pass condition |
| :--- | :--- |
| Bound | No restored record has a timestamp after T. |
| Committed records | Every canary value committed before T minus the RPO is present at its original offset. |
| Transactional visibility | A `read_committed` reader of the restored canary input sees no aborted value. |
| Output rebuild | For every window that closes before T minus the RPO, the rebuilt output of the new application equals the production output. |
| ACLs and principals | `kafka-acls --list` matches production at the capture. An mTLS client maps to the same principal. |
| Measured values | `rpo_ms` and `rto_ms` are inside the declared values. |

### Runbook sections the operator follows

| Phase | Sections |
| :--- | :--- |
| P1 | [Install](deploy.md#install), [Format the log directory](deploy.md#format-the-log-directory), [Configure](deploy.md#configure), [Start](deploy.md#start), [Health checks](deploy.md#health-checks), [Capture the inputs](backup-restore.md#capture-the-inputs) |
| P2 and P3 | [Before you start](migrate-from-kafka.md#before-you-start), [Configure MirrorMaker 2](migrate-from-kafka.md#configure-mirrormaker-2), [Watch the mirror catch up](migrate-from-kafka.md#watch-the-mirror-catch-up), [Cut the consumers over](migrate-from-kafka.md#cut-the-consumers-over), [Cut the producers over](migrate-from-kafka.md#cut-the-producers-over), [ACL sync](migrate-from-kafka.md#acl-sync) |
| P4 | [Stop](deploy.md#stop), [authentication-failures](runbooks/authentication-failures.md), [authorization-denials](runbooks/authorization-denials.md), [no-active-controller](runbooks/no-active-controller.md), [controller-leader-flapping](runbooks/controller-leader-flapping.md), [consumer-group-lag](runbooks/consumer-group-lag.md), and any runbook whose alert fires |
| P5 | [Roll back](migrate-from-kafka.md#roll-back), [Rolling upgrade](deploy.md#rolling-upgrade) if a fix build is rolled |
| P6 | [Check a copy](backup-restore.md#check-a-copy), [What comes back](backup-restore.md#what-comes-back), [restore-from-archive](runbooks/restore-from-archive.md) |

## Independent Acceptance Runs

An independent acceptance run is the fallback when no external operator is available for a plan by the date in action M9. It runs the same plan, phases, checks and criteria. It is weaker evidence than an external pilot, and the evidence says so.

### Who may run one

The runner is a person who meets every condition below. The maintainer records the check in the sign-off record.

- The runner did not author, co-author or approve a change to the paths that the plan exercises in the 6 months before the run. The paths are `docs/operations/`, `crates/backup-cli/`, `crates/restore/`, `crates/format/`, `packaging/k8s/`, the krabka-operator repository for Plan A, and the broker handlers for the plan's features. `git log` and the pull request review history are the record.
- The runner is not the primary maintainer.
- The runner gets no briefing beyond this document and the published documents at the plan revision.
- The runner is a person. An AI coding agent is not a runner. A runner may use an agent as a tool, and the command log records each use. An agent that has built krabka features cannot supply independence.

### Workload

An independent run has no operator workload, so the runner builds the plan's workload from tools that already exist:

- The source cluster is the pinned `apache/kafka:4.3.1` image, at the digest in `MODULE.bazel`.
- Plan A load comes from `kafka-producer-perf-test` and the canary tools, at the rates in the workload table, with Avro values from the registry's own serializer.
- Plan B load comes from the canary tools and the Streams application of the `jvm_streams_app` suite, with `processing.guarantee=exactly_once_v2`.

### How the evidence is labeled

- The issue that holds the run is titled `acceptance: Plan <A|B>, independent acceptance run <n> (not an external pilot)`. An external pilot uses `pilot: Plan <A|B>, external pilot <n>`.
- The first line of every evidence file, report and summary is `Independent acceptance run. Not an external pilot.`
- A summary, a release note or a roadmap update that counts pilots counts independent runs on a separate line. An independent run never satisfies a criterion that names an external pilot or an operator.

## Maintainer Actions

A person does each of these through the project's normal process. Neither this document nor an agent can do them. Actions marked **blocks** have to be complete before P0 ends.

| Id | Action | Blocks |
| :--- | :--- | :--- |
| M1 | Name the Plan A operator and get pilot access through the normal project process. Record the operator's consent to the evidence list and to publication. Fill every `TBD(M1)` value. | Plan A |
| M2 | Do the same for Plan B. Fill every `TBD(M2)` value, the write freeze included. | Plan B |
| M3 | Agree with each operator what data may leave their environment. A maintainer gets no shell, credential or bucket access to a pilot environment. | Both |
| M4 | Accept or change each proposed number in this document in a reviewed commit, and record the plan commit SHA in the sign-off record. | Both |
| M5 | Pick the candidate: a release tag, its image digest and the sibling set, from a qualified manifest of the month-four qualification. | Both |
| M6 | Pick the Plan B Streams rebalance protocol: `classic` or `streams`. | Plan B |
| M7 | Link the #1396 rollback eligibility row for the candidate and the #1397 declared RPO and RTO. Fill the RTO values. | Both |
| M8 | Triage the [gap issues](#gap-issues). Close each issue that a plan lists as blocking. | Both |
| M9 | Set the date after which a plan without an operator becomes an independent acceptance run. Proposed: 2027-01-15, 8 weeks before the M29 target. Name each runner and record the independence check. | Fallback only |
| M10 | In P7, verify each evidence set against this document and publish it after the operator's consent review. | Both |

## Gap Issues

Writing the plans found these gaps and defects in the existing documents. Each one is a small issue in milestone 29. This document does not fix them, because [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) owns the validation of the runbooks.

| Issue | Gap | Blocks |
| :--- | :--- | :--- |
| [#1494](https://github.com/krabka-io/krabka-broker/issues/1494) | The migration guide shows no MirrorMaker 2 client settings for a SASL or mTLS cluster, and no ACLs for MirrorMaker 2's own principal. | A and B |
| [#1495](https://github.com/krabka-io/krabka-broker/issues/1495) | The migration guide does not cover a transactional source or a Kafka Streams application: the mirror's isolation level, aborted records, and the Streams internal topics. | B |
| [#1496](https://github.com/krabka-io/krabka-broker/issues/1496) | The migration guide does not say which topic name producers write after the cutover, what the reverse flow carries back, or that ACL sync skips `WRITE` grants. | A |
| [#1497](https://github.com/krabka-io/krabka-broker/issues/1497) | The migration guide does not say how to create tiered or compacted target topics before the mirror starts. | A |
| [#1498](https://github.com/krabka-io/krabka-broker/issues/1498) | No document describes the schema registry cutover that the migration guide says is needed. | A |
| [#1499](https://github.com/krabka-io/krabka-broker/issues/1499) | The backup guide does not say that compacted user topics and `_schemas` are never in the archive, so a restore does not bring them back. | A drill |
| [#1500](https://github.com/krabka-io/krabka-broker/issues/1500) | The backup guide and the restore runbook show no `--command-config` for a secured cluster, show the `--worm-*` flags as always needed, and say that a restored cluster has a new cluster id after a step that keeps the old one. | A and B drills |
| [#1501](https://github.com/krabka-io/krabka-broker/issues/1501) | [Deploy](deploy.md) had no krabka-operator installation path, but [Scale a cluster](scaling.md) and the qualified manifest use the operator. [Kubernetes operator](deploy.md#kubernetes-operator) now describes it. | None. Closed by the same pull request as this document. |
| [#1502](https://github.com/krabka-io/krabka-broker/issues/1502) | [Kubernetes](deploy.md#kubernetes) says the image is published for amd64 only, but [Install](deploy.md#install) says release tags are amd64 and arm64 indexes. | None |
| [#1503](https://github.com/krabka-io/krabka-broker/issues/1503) | The ecosystem matrix does not name Kafka Streams, KIP-848 consumers or share-group consumers, though suites cover each. | None |

## Integration

- **Deployment.** Plan A depends on the krabka-operator chart and image in the qualified manifest, and on the `[remote_storage]` and `[authorization]` tables of the broker configuration. Plan B depends on `krabka-format`, the TOML listener tables and `principal_mapping_rules`.
- **Migration.** Both plans depend on MirrorMaker 2 as the `mirror_maker2` suite runs it: the stock 4.3.1 distribution against krabka, with `OffsetCommit` for the translated group positions and `IncrementalAlterConfigs` for the topic configuration sync.
- **Backup and restore.** Both drills depend on `krabka-backup capture`, `verify` and `restore-offsets`, and on `krabka restore`, as [Backup and restore](backup-restore.md) and [KFC-3](../KFCs/KFC-3-point-in-time-restore.md) define them.
- **Lifecycle and recovery qualification.** The plans take version rollback eligibility from #1396 and the RPO and RTO definitions and declared values from #1397. A pilot repeats neither qualification. It shows that an operator can do the same work from the published documents.
- **Sustained load.** The alert-response deadlines come from `bench/soak/rf3-72h.yaml`, so a pilot and the 72-hour soak of #1393 judge failover with the same numbers.

## Kafka / KIP Compliance

- **MirrorMaker 2** ([KIP-382](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0)). Both plans run the unmodified 4.3.1 MirrorMaker 2. The offset translation and its replay bound are MirrorMaker 2's behavior, not krabka's. The plans measure the re-reads and never accept a skipped record. The ACL sync behavior that Plan A relies on is MirrorMaker 2's own: it copies topic ACLs and does not copy a `WRITE` grant.
- **Transactions** (KIP-98, KIP-447, and [KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)). Plan B checks `read_committed` visibility and the absence of hanging transactions with Kafka's own tools. A transaction never crosses clusters. The quiesce stage makes sure that none is open at the switch, because MirrorMaker 2 carries no transaction state.
- **Groups** (KIP-848, and KIP-1071 if M6 picks it). Plan B's downstream consumer uses `group.protocol=consumer`. Plan A keeps the classic protocol, so the two plans cover both group protocols.
- **Security** (KIP-371, and [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API)). Plan A creates SCRAM principals as `KafkaUser` resources, or with `kafka-configs` where a client keeps its source password. Plan B maps certificate DNs with `ssl.principal.mapping.rules`, which krabka reads per listener as `principal_mapping_rules`.
- **Tiered storage** ([KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)). As in Kafka, a compacted topic cannot be tiered. The plans keep compacted topics local and protect them outside the archive.
- **Feature levels** ([KIP-584](https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features), [KIP-778](https://cwiki.apache.org/confluence/display/KAFKA/KIP-778%3A+KRaft+to+KRaft+Upgrades)). Version rollback eligibility follows the finalized levels, as in Kafka.

## Testing

The plans are not code, so no suite tests them. Three things keep them honest:

- **The suites behind each step.** Each step uses a path that an existing suite already runs on every change or in the container lane: `mirror_maker2` for the mirror and the offset translation, `jvm_streams_app` and `jvm_consumer_group_next_gen` for Plan B's clients, `mtls_principal_mapping` for the principal rules, `jvm_acceptance_sasl` for SCRAM and the canary producer, and `crates/restore/tests/dr_roundtrip.rs` for the restore path. The [KIP matrix](../KIP_MATRIX.md) links each suite.
- **The qualification gates.** The [ecosystem qualification](../qualification.md) gates `installation`, `operator-lifecycle`, `disaster-recovery` and `registry-migration` exercise the candidate that a pilot installs. A pilot does not start on a candidate that has not passed them.
- **The rehearsal.** Before the first external run, the maintainer runs each plan once against the pinned `apache/kafka:4.3.1` source to confirm that every check can be measured with the named tools. That rehearsal is labeled `maintainer rehearsal`. It is neither a pilot nor an independent acceptance run, and its intervention log is not counted.
