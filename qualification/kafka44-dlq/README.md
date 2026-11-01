# Released Kafka 4.4 DLQ component qualification (#1974)

This is **partial qualification**, not released-broker interoperability proof.

## Reproduction

Image: `apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f`.
Extract `/opt/kafka/libs/` from that image to `$KAFKA_LIBS`, then run with JDK 25:

```sh
javac -cp "$KAFKA_LIBS/*" -d /tmp/dlq-classes qualification/kafka44-dlq/Boundary.java
java -cp "/tmp/dlq-classes:$KAFKA_LIBS/*" org.apache.kafka.server.share.dlq.Boundary
```

`boundary.txt` is observed output from these released jars, not a reimplementation.
The probe invokes the real `ProduceRequestHandler.topicProduceData`. Metadata and
transport are test doubles; the source record is injected after resolution, so
`copy=true/false` labels denote resolved/absent source data, **not an end-to-end
exercise of the copy-record configuration or source fetch**. Reflection accesses
package/private state. No producer or share consumer connects to a broker.

For source topic `orders`, partition 3, offset 4, group `g1`, delivery count 5,
client-reject cause, one-byte key and 100-byte value, the released encoded batch
is 344 bytes. At limits 343, 344 and 345 it retains both key and value. Without
resolved source data the headers-only batch is 242 bytes, tested at 241/242/243.
Only the below-size limit is oversized. Krabka previously silently removed the
payload when the first copied record exceeded the destination limit; it now
keeps the record intact, allowing the destination to reject it.

The same binary probe injects produce responses. `MESSAGE_TOO_LARGE` completes
exceptionally, increments the failed meter once, writes zero records, and
schedules no retry. Success increments the written meter by one. A
`NOT_LEADER_OR_FOLLOWER` response schedules one retry without completing or
counting a failure. These are component observations; there is no measured
network request count or server-side size rejection here.

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

- Real released Kafka and Krabka source fetch/copy enabled and disabled at exact
  destination limits; observe actual produce errors, request counts and meters.
- Durable final ARCHIVED state after both success and failure on both brokers.
- Controlled leader changes and process restarts while ARCHIVING, including
  append-before-ARCHIVED crash point, with measured duplicate/loss outcomes.
- Agree a separately scoped extension implementation if native plugins are wanted.

Existing Krabka tests cover failed writes still archiving, recovered cause
inference, and per-attempt leader lookup. They do not replace multi-broker crash
qualification. #1969's qualification lane must retain this limitation.
