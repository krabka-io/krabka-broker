# Released Kafka 4.4 DLQ component qualification (#1974)

This is **partial qualification**: released handler components, live destination Produce boundaries,
and a real released share-consumer rejection/copy boundary run. Exact-boundary candidate differential is measured below; broader
mid-ARCHIVING crash and multi-broker qualification remain incomplete.

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
- [SharePartition](https://github.com/apache/kafka/blob/4.4.0/core/src/main/java/kafka/server/share/SharePartition.java): `initiateDLQAndArchive` archives even when enqueue fails; `maybeResumeDlqArchiving` re-enqueues recovered ARCHIVING ranges. This establishes a source-level duplicate window after DLQ append but before durable ARCHIVED. Failure can leave no DLQ record although the source is archived. **These are source-derived expectations, not measured crash guarantees.**

Krabka's internal `DlqSink` is not a user-loadable manager extension. No native
custom-manager configuration or JVM-class loading counterpart is supported by
this change. Designing a native registration/lifecycle/error contract requires
a separately scoped follow-up; do not advertise this capability as parity.

## Still required before closing #1974

- Full released-broker request/failure/written JMX meters; actual candidate
  counters and released component counters are measured, not equivalent evidence.
- Resolve/accept the terminal-prefix persistence difference measured below:
  released retains ARCHIVED rows/start=0/count=1; candidate prunes/start=1/count=0.
- Controlled leader changes and process restarts while ARCHIVING, including
  append-before-ARCHIVED crash point, with measured duplicate/loss outcomes.
- Agree a separately scoped extension implementation if native plugins are wanted.

Existing Krabka tests cover failed writes still archiving, recovered cause
inference, and per-attempt leader lookup. They do not replace multi-broker crash
qualification. #1969's qualification lane must retain this limitation.

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
ARCHIVED state. Actual manager request meters and controlled retry/error
injection remain covered only by the earlier released component probe, not
this end-to-end run. Keep those distinctions when evaluating closure.

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
written record for each of the other six groups. Released component counters
agree; full released broker JMX counters were not collected.

Candidate group offsets have startOffset=1 and lag=0 for all eight groups both
before and after clean process restart, unlike released startOffset=0/lag=0.
Do not hide that administrative reporting difference or equate it to a DLQ
payload/archival difference without deeper persister analysis. This restart is
**after** completion, not a crash during ARCHIVING. Persisted raw state codes,
controlled mid-flight crashes and multi-broker leader change remain required
for the stronger acceptance criteria.

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
are not discharged merely by adding them.

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
Verified-share runtime regression filter:16 passed. The full Creusot discharge
remains pending the matching0.14 runtime; runtime tests are not formal proofs.
