# Released Kafka 4.4 DLQ component qualification (#1974)

This records released/candidate handler, real share-consumer exact-boundary,
JMX-counter, restart and three-node crash comparisons. Both pre-append failure
and append-before-confirmation schedules are measured. It is not exhaustive
network-fault qualification. Historical terminal-prefix differences remain below;
see the terminal-prefix ordering follow-up for the subsequent implementation fix.

## Reproduction

Image: `apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f`.
Extract `/opt/kafka/libs/` from that image to `$KAFKA_LIBS`, then run with JDK 25:

```sh
javac -cp "$KAFKA_LIBS/*" -d $DLQ_CLASSES qualification/kafka44-dlq/Boundary.java
java -cp "$DLQ_CLASSES:$KAFKA_LIBS/*" org.apache.kafka.server.share.dlq.Boundary
# Optional: send the handler-built records to a fresh released broker:
java -cp "$DLQ_CLASSES:$KAFKA_LIBS/*" org.apache.kafka.server.share.dlq.Boundary 127.0.0.1:39744
```

`boundary.txt` is observed output from these released jars, not a reimplementation.
The probe invokes the real `ProduceRequestHandler.topicProduceData`. Metadata and
transport are test doubles; the source record is injected after resolution, so
`copy=true/false` labels denote resolved/absent source data, **not an end-to-end
exercise of the copy-record configuration or source fetch**. Reflection accesses
package/private state. The optional bootstrap argument uses Admin/Producer to create six fresh topics
and send each handler-built record to a real broker with no compression,
idempotence disabled, acks=all and retries=0. This does not run a share consumer.

For source topic `orders`, partition 3, offset 4, group `g1`, delivery count 5,
client-reject cause, one-byte key and 100-byte value, the released encoded batch
is 344 bytes. At limits 343, 344 and 345 it retains both key and value. Without
resolved source data the headers-only batch is 242 bytes, tested at 241/242/243.
Observed live destination Produce agrees: below-size limits return
`MESSAGE_TOO_LARGE`; exactly-at/above-size limits succeed. Krabka previously silently removed the
payload when the first copied record exceeded the destination limit; it now
keeps the record intact, allowing the destination to reject it.

The same binary probe injects produce responses. `MESSAGE_TOO_LARGE` completes
exceptionally, increments the failed meter once, writes zero records, and
schedules no retry. Success increments the written meter by one. A
`NOT_LEADER_OR_FOLLOWER` response schedules one retry without completing or
counting a failure. The manager metrics/retry observations remain component observations: the
live destination sends do not execute the manager transport or archival path.
The observed live broker was the pinned image, single combined KRaft node,
PLAINTEXT, one partition/replica per fresh topic; no changes to production.

## Released source audit and extension boundary

Pinned release sources:

- [State manager](https://github.com/apache/kafka/blob/4.4.0/server/src/main/java/org/apache/kafka/server/share/dlq/ShareGroupDLQStateManager.java): first record is sent intact even oversized; terminal errors count failure, leader errors retry.
- [Manager interface](https://github.com/apache/kafka/blob/4.4.0/server/src/main/java/org/apache/kafka/server/share/dlq/ShareGroupDLQManager.java): `enqueue(ShareGroupDLQRecordParameter)` returns `CompletableFuture<Void>` and `stop()` handles cleanup. Parameters include group, topic-id/partition, inclusive offsets, optional delivery count and cause. This is a JVM extension, not a wire API.
- [Default implementation](https://github.com/apache/kafka/blob/4.4.0/server/src/main/java/org/apache/kafka/server/share/dlq/DefaultShareGroupDLQManager.java): validation and delegation to the state manager.
- [SharePartition](https://github.com/apache/kafka/blob/4.4.0/core/src/main/java/kafka/server/share/SharePartition.java): `initiateDLQAndArchive` archives even when enqueue fails; `maybeResumeDlqArchiving` re-enqueues recovered ARCHIVING ranges. This establishes a source-level duplicate window after DLQ append but before durable ARCHIVED. Failure can leave no DLQ record although the source is archived. The two measured crash schedules below confirm restart re-enqueue and its duplicate window; these are not universal crash guarantees.

The separately tracked [native manager follow-up](https://github.com/krabka-io/krabka-broker/issues/1982)
adds public per-broker factory registration and enqueue/stop lifecycle; see
[the native API contract](../../docs/compatibility/kafka-4.4-native-dlq-manager.md).
This is a native Rust embedding API, not JVM class/JAR loading or a class-name
loader in the stock executable. Keep that deployment boundary explicit.

## Remaining qualification boundary

Terminal-prefix persistence remains different: released retains ARCHIVED
rows/start=0/count=1; candidate prunes/start=1/count=0. Both measured restart
schedules have zero source redelivery and lag0, but exact state parity must not
be claimed. Native manager registration/lifecycle is implemented and tested
in this branch (#1982). Arbitrary partitions, uncooperative plugin IO and
exhaustive Jepsen schedules remain outside these bounds; #1969 must retain them.

## Candidate verification

`cargo test -p krabka-broker --lib share_partition::dlq:: -- --test-threads=2`:
43 passed, 0 failed (including both copied and headers-only exact boundaries).

`cargo test -p krabka-broker --test share_consume a_copied_record_too_big_for_the_topic_fails_and_still_archives -- --nocapture`:
1 passed, 0 failed; verifies source advancement, empty DLQ, zero written records,
one produce attempt and one failed attempt for a 1000-byte copied source value
at destination limit 1100. This is the in-process candidate regression, not a
released-reference share-consumer run.

`cargo test -p krabka-broker --lib share_partition::manager::dead_letter:: -- --test-threads=2`:
6 passed, 0 failed, including durability-before-write and failed-write/recovered-
cause component cases. These are not process-restart or multi-broker tests.

## Local release broker command

Run in an isolated qualification environment; choose a fresh container name and port.
The container is local test infrastructure, not production.

```sh
docker run -d --name kafka1974-boundary -p 127.0.0.1:39744:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:39744 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f
```

## Real released share-consumer boundary run (2026-11-02)

`ShareBoundary.java` uses the released 4.4.0 `KafkaShareConsumer` in explicit
acknowledgement mode to reject a real produced source record. It configures
`errors.deadletterqueue.copy.record.enable` on each actual share group and
reads the resulting DLQ topic with a normal Kafka consumer. No source fetch,
manager transport or acknowledgement is mocked. Each case uses a fresh source,
DLQ and group, with names of identical length; those names affect header size.

Observed output is retained in `share-boundary-release.txt`: copied batches are
352 bytes; headers-only batches are 250 bytes. Both have the expected payload
at the exact limit and one byte above it; one byte below it produces no DLQ
record. Released broker logs show `RecordTooLargeException` and
`Failed to write to DLQ, proceeding to ARCHIVED regardless` for the two rejected
DLQ produces. All eight groups report lag=0 before and after a clean whole-broker
restart (`share-boundary-release-before.txt` and `share-boundary-release-after.txt`).
Their reported start offsets remain zero, so startOffset alone is not archival
proof. This is not a controlled crash during ARCHIVING and does not demonstrate
the duplicate/loss window or multi-broker failover.

The isolated release broker uses the digest above and **host networking** with
PLAINTEXT and controller listeners on 39745/39746. Its share coordinator state
topic has one partition, replication factor one and min ISR one; offsets and
transaction state topics also have replication factor one (transaction min ISR
one). The earlier published mapped-port command is suitable only for simple
external Produce: the broker's own DLQ/persister clients must be able to reach
its advertised address as well. Do not use a container-loopback advertised host
port that has no listener inside that container for this share probe.

```sh
mkdir -p "$DLQ_CLASSES"
javac -cp "$KAFKA_LIBS/*" -d "$DLQ_CLASSES" qualification/kafka44-dlq/ShareBoundary.java
java -cp "$DLQ_CLASSES:$KAFKA_LIBS/*" ShareBoundary 127.0.0.1:39745 fresh01
java -cp "$DLQ_CLASSES:$KAFKA_LIBS/*" ShareBoundary 127.0.0.1:39745 fresh01 verify
# Restart the owned isolated release broker, then repeat verify.
```

The verify mode asserts zero share-group lag; it does not decode persisted
ARCHIVED state. This initial run alone did not collect manager meters or inject errors. The
later actual JMX and three-node crash schedules below supply those additional
observations; the released component probe isolates retry/error classification.

## Candidate end-to-end differential (2026-11-02)

The identical released-client `ShareBoundary` program ran successfully against
Krabka's isolated standalone broker, source branch `fix/kafka44-dlq-payload-1974`
(before the pure batch-admission helper extraction). The executable SHA-256 is
recorded in `candidate-binary.sha256`. Its directory was formatted at
`4.4-IV2`, `share.version=2`, with both unstable switches enabled. Runtime
settings set offsets/transaction/share-state replication factors and min ISR
to one, and share-state partitions to one. Client/controller/metrics/health
ports are 39747/39748/39749/39750. There are no production changes.

`share-boundary-candidate.txt` exactly matches the eight released behavioral
outcomes after substituting fresh equal-length names: copy=true 352 bytes,
copy=false 250 bytes, rejection below the boundary and intact output at/above.
`share-boundary-candidate-metrics.txt` records one actual DLQ produce attempt
per group, one failure for each of the two undersized destinations, and one
written record for each of the other six groups. Released component counters agree; the later actual released JMX run below
confirms the same eight per-group meter outcomes.

Candidate group offsets have startOffset=1 and lag=0 for all eight groups both
before and after clean process restart, unlike released startOffset=0/lag=0.
Do not hide that administrative reporting difference or equate it to a DLQ
payload/archival difference without deeper persister analysis. This restart is
**after** completion, not a crash during ARCHIVING. The later decoded state and controlled three-node crash schedules below
provide additional evidence beyond this clean-restart run.

## Bounded Stateright model and proof boundary

`crates/broker/src/share_partition/dlq_model.rs` executes the real acquisition
machine for one rejected offset. It separates ARCHIVING persistence, sink send,
retry, successful append or terminal oversized response, terminal archival and
ARCHIVED confirmation. One crash/leader reload may occur at every persisted
phase, including append-before-ARCHIVED confirmation. Up to two attempts per
leadership generation and at most two appends are reachable. The graph has a
32-depth / 10,000-generated-state hard backstop and must exhaust before either
cap. A negative-control model drops recovered pending work and must discover
the missing-recovery invariant, rather than silently reducing explored actions.

The model drives real reject/take-pending/persist/load/finish transitions;
network response scheduling and durable-write confirmation are modeled. Its
coverage is not an end-to-end crash or Jepsen result. In particular it does not
cover two rejected offsets sharing a batch, concurrent adjacent ranges,
multi-node state-coordinator durability, topic deletion/recreation, timed retry
backoff, concurrent leadership fencing, broker JMX metrics, truncation/corrupted
storage, or authentication/authorization failures. These require separate
production-path regressions and fault-injection qualification; no finite model
establishes absence of all coverage gaps.

The production `build_round` now calls `krabka_verified::share::dlq_batch_admission`.
Its unconditional Creusot contracts state precisely: nonfirst oversized records
flush before append, first oversized records append and stop, and records at or
below the limit append and continue. This proves the admission decision, not
that an IO write succeeds. Saturated projected byte size, record encoding,
original key/value cloning and destination errors are adapter obligations
exercised by the real exact-boundary differential above. The proof runtime must
match repository pin v0.14.0; installed local0.13.0 refuses std0.14.0, so contracts
were initially blocked locally; the pinned-image discharge is recorded below.

## Durable state decoded using released Kafka tooling

The released 4.4 `kafka-dump-log.sh --share-group-state-decoder` reads both
brokers' actual `__share_group_state` log segments. Filtered raw records are
retained as `release-persisted-state.txt` and `candidate-persisted-state.txt`;
`persisted-state-summary.txt` records assertions across all eight groups each.
Every group first persists deliveryState=3 (ARCHIVING), including both oversized
failure cases. Released Kafka then persists deliveryState=4 (ARCHIVED), retains
startOffset=0 and deliveryCompleteCount=1, including after clean restart.
Krabka instead persists startOffset=1 with no remaining state batches and
completionCount=0, and recovers that pruned state on restart. Thus final terminal
outcomes are durably measured, not inferred merely from lag or source code.

This terminal-prefix pruning is a **measured retained deviation**, not byte/state
parity. It explains the administrative startOffset difference recorded earlier;
the current evidence does not justify hiding it or claiming identical persisted
archival representation. Both finish with zero deliverable lag. Crash-window
and multi-broker semantics remain unqualified.

### Observed model checks

The bounded normal model exhausted **42 unique / 55 generated states**, maximum
observed depth10; the lost-recovery mutant exhausted **20 unique / 31 generated
states**, maximum depth7, and was detected. Both tests passed (2/2). The normal
and mutant unique counts are pinned in source; the recovery counterexample is
pinned at two actions (Persist, Crash). The hard caps remain depth32/generated
10,000, not the observed completion counts. This is small by design: it proves
the isolated one-offset transition graph, not arbitrary multi-record schedules.
Verified-share runtime regression filter:16 passed. The pinned0.14 formal replay
completed successfully across617 files, including the new functional and Clone
VCs. Runtime tests are not formal proofs.

## Measured leader failure and mid-ARCHIVING restart

`ShareFault.java` drives real source production, share rejection and DLQ
verification. Fresh names use `-Ddlq.run=...` (default `fault02`). Prepare the
share-state topic with one partition on replicas [3,1,2], min ISR2. The probe
creates a source on broker1 and DLQ on replicas [2,3], min ISR1; it asserts
source/DLQ leaders1/2 before the fault. Three combined broker/controller nodes
run the same pinned release, with private data, RF3 internal topics and two
replicas required for state persistence. Their PLAINTEXT listeners are39761–3,
controllers39771–3. Candidate listeners are39791–3/controllers39781–3, same
RF/ISR settings, metadata4.4-IV2/share2 and unstable switches. Candidate config
must explicitly name all three `controller_quorum_voters`.

After the source is acquired, pause destination leader2 and release the probe's
acknowledgement gate file. The observer decodes the actual state log on broker3,
asserts the latest group record contains deliveryState3, then immediately kills
source broker1 and restarts it from the same disk. This observation and kill
must be one orchestrated operation, not separate human/model turns. The first
released attempt missed the window (ARCHIVED preceded the kill) and is **not**
used as crash evidence. The successful second attempt and candidate mirror are
retained as `fault-release-*` / `fault-candidate-*`.

Released: ARCHIVING write1793585113581ms, observed1793585114532ms, Docker kill
signal1793585115008ms (actual Docker events retained). No ARCHIVED record
precedes that signal. After restart, before another source share fetch, the
group still has lag1; re-polling the same group resumes archival. Final ARCHIVED
write1793585251074ms, one intact DLQ record, lag0 and no source redelivery.
Candidate: ARCHIVING write1793585653494ms, observation1793585653762ms, SIGKILL
1793585653763ms. Source re-poll yields one intact DLQ record, zero redelivery and
lag0; durable terminal-prefix pruning remains start1/no state rows. Candidate
acquisition deliveryCount2 versus released1 reflects an earlier expired probe
acquisition; it is not a claim of identical fault-run delivery counters.

Both destinations failed over while their old leader remained paused. These
runs demonstrate the measured **pre-append** crash schedule, not exactly-once
archival. The append-before-confirmation window is measured below. Duplicate/loss
guarantees beyond these schedules remain unqualified. The rejected commit can return UNKNOWN_SERVER_ERROR during source
death; recovery is verified through the persisted state and later consumer run.

## Formal proof execution

Pinned image `ghcr.io/krabka-io/creusot:v0.14.0-4b4c83aaef59d968`, digest
`sha256:992bd1a7a51c58e188dc2b31a6eef36be106217dcf24450f9a4a52cfdeee561b`.
Actual source compilation:
`cargo creusot --package krabka-verified --output-dir /proof-output --why3find-arg=-j --why3find-arg=2 -- --offline --ignore-rust-version --target-dir /proof-target`.
Creusot emits `/proof-output/verif`; its automatic why3 phase still selected the
read-only source checkout's `verif`, so that wrapper phase failed on filesystem
writes. The emitted production Coma was then proved directly using
`why3find prove -j 2 --summary --no-autodetect-provers verif/krabka_verified_rlib`
from the private output directory with the image's `DUNE_DIR_LOCATIONS` and
`WHY3CONFIG`. The functional admission VC passed Alt-Ergo in0.009s and generated
Clone VC in0.008s. These are **two obligations, one functional**, covering the
three postconditions jointly. Only the four new proof artifacts were copied
back; no unrelated proof-session churn. Full fresh package replay completed:
`Proved (617 files)`. This is a file count, not617 newly introduced obligations.

## Actual released-broker JMX meters

`JmxCounts.java` reads the released broker's actual `ShareGroupMetrics` MBeans
using a loopback-only RMI endpoint, not injected component metrics. A fresh
`j01` exact-boundary run against pinned4.4 completed all8 cases. Retained
`share-boundary-release-jmx-run.txt` and `share-boundary-release-jmx-counts.txt`
assert one produce attempt per group, one failure for each undersized
copied/headers-only destination, zero writes there, and one write in each of
six successful groups. These match the previously retained actual candidate
counters. Missing zero-valued meter series are read as zero, not fabricated
positive counts. No broker restart occurs between the run and the meter read.

The isolated released server uses client39902/controller39903 and JMX39904.
JMX binds127.0.0.1 with both registry and RMI port39904, auth/SSL disabled only
on that private test endpoint. The same RF1/minISR1 setup as the earlier single
node is used, with a512MiB heap. Run `JmxCounts` with
`service:jmx:rmi:///jndi/rmi://127.0.0.1:39904/jmxrmi j01` after `ShareBoundary`.
This closes the earlier released-JMX counter gap for the size scenarios; it is
not a rate/timer calibration or post-crash counter continuity guarantee.

## Actual append-before-ARCHIVED source crash

`WireResponseGate.java` is a loopback framed TCP proxy, not a production hook.
Advertise destination leader2 through it; it forwards requests unchanged and
holds the first actual DLQ Produce response (API0/v13), after backend response.
The one-shot gate does not block a new recovered producer connection. Run with
`listen-port backend-port gate-dir`. Use a fresh `-Ddlq.run=dup03` namespace
for `ShareFault setup/reject/count/resume/verify`; pass `-Ddlq.expected.count=2`
to final verification. Every original key/value byte is compared, not just size.
After `held.txt`, count must read one DLQ copy, decode state3 from actual state
log and immediately kill source1/restart same disk. Recovery polling must return
no source record. Both clusters use source1, destination2 and state3 leaders.

Released response62bytes held1793588692002ms; readable append observed
1793588693611130259ns, kill completed1793588693754167475ns. Candidate response
62bytes held1793589159276ms; readable append observed1793589160914798322ns,
SIGKILL1793589160915003055ns. Both latest durable rows were ARCHIVING at kill.
`append-{release,candidate}-timeline.txt` retains response SHA256 and timelines;
count-before/state-before/resume/state-final/verify retain actual observations.
Released final ARCHIVED timestamp1793588960989ms and candidate terminal-prefix
timestamp1793589469759ms strictly follow the kills. Both deliveryCount1.

**Both finish with two intact DLQ copies, no source redelivery and lag0.**
Released retains start0/state4/count1; candidate prunes start1/count0. This is a
real duplicate witness, not exactly-once archival. Readable append plus state3
observation excludes the earlier missed-window failure. Lost responses may
duplicate; terminal errors may archive without a copy, as boundary runs show.

## Native regression execution

After registration/lifecycle changes,154 share_partition library tests and28
share_consume wire integration tests passed. This includes three public-facade
registration/delegation/refusal/start-error examples, three lifecycle cases,
and coalescer cancellation with blocked transport and queued work. Strict
library+share-consumer Clippy passed; CPD0clones and script accounting passed.
Wider all-integration-target Clippy found one test-doc backtick omission (fixed);
its rerun exhausted host disk writing compiler caches. Do not report that
infrastructure failure as a passing wider check.

## Terminal-prefix ordering follow-up (2026-11-05)

The historical receipts above are unchanged. The production phase-2 owner now
persists ARCHIVED before pruning the live terminal prefix. This follows
[released SharePartition.initiateDLQAndArchive](https://github.com/apache/kafka/blob/4.4.0/core/src/main/java/kafka/server/share/SharePartition.java#L3403):
archive and increment deliveryCompleteCount, writeShareGroupState, then
maybeUpdateCachedStateAndOffsets after either persistence success or failure.
The retrieved release source SHA-256 is
`06441b270d9fe9b3fa0f88b61fdd612af5a62042b4f9340dd70f7c1ccfe9d34b`.

The regression `archived_prefix_is_persisted_before_live_cache_pruning` checks
the production acquisition machine's projected durable row (state4, start0,
count1), live cache pruning without a redundant dirty write, and reload with no
second DLQ enqueue. The manager invokes these operations around its actual
persistence attempt. No encoding or feature level changes. A failed persistence
attempt still leaves dirty state for retry; its live terminal prefix advances,
as in the release. This is a source-aligned ordering fix with a state-machine
regression, not a new paired live oracle execution or RF3 denial receipt.

Verification: `cargo test -p krabka-broker --test share_consume --features
test-helpers dead_letter_queue` passed all 12 end-to-end cases, including the
registered manager's successful delegation and terminal-refusal variants.
Both variants assert the actual coordinator summary remains start0/count3
after rejecting three source records. BDD cases observe completed progress as
SPSO plus the delivery-complete count, allowing either a retained terminal
prefix or an already-pruned prefix, including the share.version1 control.
