# KFC-1: Deliver-at-Time Visibility

Scheduled delivery for a Kafka topic, with no wire-protocol change and no client change.

## Status

**Adopted.** The implementation lands on branch `claude/kafka-deliver-at-time-0415vy`.

No KIP defines scheduled delivery, so this document is the specification for it. Each section below states where the semantics sit next to Kafka's. The [Compatibility](#compatibility-deprecation-and-migration-plan) section names the one place where krabka overloads a Kafka meaning.

## Motivation

Delayed and scheduled delivery is one of the longest-requested Kafka capabilities. Users proposed it upstream many times over more than a decade, and no proposal landed. The capability is still missing, and the demand did not go away.

Three common systems sit on the same missing primitive.

- **Scheduled command execution.** A service writes a command now and wants a worker to receive it at a set time.
- **Retry queues with backoff.** A failed record goes back to a topic. It has to stay invisible until the backoff time ends.
- **SLA timers.** A record is a deadline. A completion before the deadline cancels it. No completion means the record comes due and a handler acts on it.

Each of the three is built today outside the broker. The pattern is always the same: an external scheduler plus a re-produce loop. The producer writes the record to a database or to a delay topic. A scheduler wakes up when the record comes due. A second produce then puts the record into the topic that the real consumer reads.

That loop is expensive in the ways that matter. It holds a second copy of the record data. It adds a second failure domain, with its own leader election and its own durable state. It drops the log's order and its exactly-once guarantees across the hop. An operator who wants a scheduled record to survive a crash now has two systems to reason about instead of one.

The broker already holds everything the feature needs. Records in the log are durable, replicated, ordered, and each one carries a timestamp. The missing part is small: a rule that says when a record becomes visible to a consumer. This KFC adds that rule and nothing else.

## Public Interfaces

The feature adds three topic configs, one broker config, and four metrics. It adds no API key, no new error code, and no field to any request or response.

### Topic Configs

| Config | Default | Values | Meaning |
| :--- | :--- | :--- | :--- |
| `delivery.mode` | `immediate` | `immediate`, `scheduled` | `scheduled` makes the record timestamp the delivery time. `immediate` is Kafka's behaviour. |
| `delivery.max.delay.ms` | `604800000` | a non-negative number of milliseconds, or `-1` | The largest delay the broker accepts, measured forward from produce time. The default is 7 days. `-1` removes the limit. |
| `delivery.schedule.monotonic` | `false` | `true`, `false` | `true` makes the broker reject a batch whose delivery time is before the largest delivery time already in the partition. |

### The Record Timestamp Is the Delivery Time

On a topic with `delivery.mode=scheduled`, the record's own timestamp is the time at which the record becomes visible to a consumer. There is no new field, and there is no new header.

This choice is what lets a stock client work unchanged. The JVM producer has carried a timestamp argument since Kafka 0.10. It sits in the `ProducerRecord` constructor that takes a topic, a partition, a timestamp, a key, and a value. librdkafka and the clients built on it carry the same argument. A producer that wants to schedule a record sets the timestamp it already knows how to set, and the broker does the rest.

### Rejections

The broker rejects a produce request in two cases, and it uses the existing Kafka error `INVALID_TIMESTAMP`, code 32, for both.

The first case is a delivery time further ahead than `delivery.max.delay.ms`. The second case is a delivery time that goes backwards on a partition with `delivery.schedule.monotonic=true`. The [head-of-line section](#why-visibility-is-offset-ordered-for-classic-groups) explains why the second rejection is worth having.

`INVALID_TIMESTAMP` is the right code because every client already knows it. Kafka has returned it for `message.timestamp.difference.max.ms` violations since 0.10, so client error tables, retry classifiers, and admin tools all handle it. A new error code would arrive at old clients as an unknown code, and an unknown code is where retry logic goes wrong.

### Metrics

- A per-partition gauge for the delivery watermark.
- A per-partition gauge for the count of pending records. A pending record is produced but not yet active.
- A histogram of activation lateness. Lateness is the time from the delivery time to the first moment the record was visible.
- A counter of scheduler wakeups.

The lateness histogram is the one an operator watches. It measures the promise this design makes. A rising tail points at clock skew, or at a scheduler that does not get enough CPU.

## Proposed Changes

### Activation

The unit of activation is the record batch, not the record.

A batch activates at its `max_timestamp`. That field sits in the v2 batch header at a fixed position, in front of the compressed record data. The broker reads it without decompression and without a decode of the records inside, so both the produce path and the scheduler stay cheap. A producer that wants per-record precision should lower `linger.ms` or produce one record per batch.

A batch is active once `max_timestamp + delivery_clock_uncertainty` is at or before the broker's clock reading. The bound is what turns a local clock into a safe decision.

Call the broker's clock reading `c` and the declared bound `e`. The bound says true time lies somewhere in the interval from `c - e` to `c + e`. The broker activates a batch with delivery time `T` only when `c` is at or past `T + e`. In that case true time is at least `c - e`, and `c - e` is at least `T`. So true time is at or past the delivery time whenever the broker activates a batch.

Two properties follow. **Delivery is never early.** Delivery is late by at most `e` plus one scheduler tick. The broker waits out the full bound, and then it acts on its next wakeup. Early delivery is the failure that breaks a retry backoff or an SLA timer, and this design rules it out. Bounded lateness is the price, and an operator can measure that price on the lateness histogram.

### The Delivery Watermark

Each partition gets one new derived offset: the **delivery watermark**. It is the largest offset such that every batch below it is active. Equally, it is the base offset of the first pending batch, or the log end offset when no batch is pending.

The watermark is a read cap, and nothing else.

- A `read_uncommitted` consumer fetch stops at the lower of the high watermark and the delivery watermark.
- A `read_committed` consumer fetch stops at the lowest of the last stable offset, the high watermark, and the delivery watermark.
- A follower fetch is not capped. See [Followers Replicate Everything](#followers-replicate-everything).

The high watermark and the last stable offset that the fetch response reports do not change. The broker computes them exactly as it does today and sends the same values. Two things follow from that. Consumer lag stays honest, because a client measures lag against a high watermark that still describes what the leader holds. KIP-227 watermark monotonicity is untouched, because the two reported values are the same values that KIP-227 constrains, computed the same way.

```
offset  |   100   |   101   |   102   |   103   |   104   |   105   |
        +---------+---------+---------+---------+---------+---------+
batch   |  active |  active |  active | pending | pending |  active |
        +---------+---------+---------+---------+---------+---------+
                                      ^                             ^
                                      delivery watermark = 103      log end = 106
```

The figure shows the rule that matters. The batch at 105 is due, but it sits behind two pending batches, so the watermark stops at 103 and no consumer sees 105 yet. The next section explains why that is the contract and not a defect.

### Why Visibility Is Offset-Ordered for Classic Groups

A classic Kafka consumer's position in a partition is a single offset. The group commits one number per partition, and the fetch loop reads forward from it. Everything to the left of the position is unreachable, forever, for that group.

That fact decides the design. Suppose the broker let a later record overtake an earlier pending one, so a consumer read offset 105 while 103 was still pending. The consumer's position is now 106. When 103 comes due, no fetch will ever ask for it again. The broker would have delivered a scheduled record to nobody, and it would have done so silently.

So head-of-line order is the contract. A scheduled topic is a schedule: the broker delivers its records in offset order, and the timestamps say when each one comes due. A record with an earlier offset and a later delivery time holds up the records behind it. That is by design, because the alternative is to lose them.

`delivery.schedule.monotonic=true` is the guard for the operator who does not want that. A partition whose delivery times go backwards is a partition whose schedule stalls. A stall is hard to see from the outside. The topic looks healthy, the lag is real, and nothing is wrong with the broker. The config turns that silent stall into an `INVALID_TIMESTAMP` at produce time, where the producer that caused it can see it and fix it.

### Share Groups Deliver Out of Order

Share groups from KIP-932 do not have the constraint that forces offset order.

A share group tracks per-record delivery state instead of one position. The share-partition state machine already knows how to skip a record and come back to it later. An in-flight acquisition, a lock timeout, and a redelivery all need that. A record the group has not acknowledged stays acquirable.

The broker uses that. A range of offsets that is not yet active is marked `Deferred`, and acquisition skips a `Deferred` range. So a share consumer on a scheduled topic gets the records that are due now. It gets the pending ones on a later acquire pass, in delivery-time order rather than offset order.

The mechanism already exists in the state machine. Transaction control batches occupy offsets that hold no user record. The share-fetch path marks those ranges on every acquire pass, so acquisition steps over them. Deferral works the same way: the broker re-derives it from the log and the clock on each acquire pass. Nothing caches a decision that a later clock reading would change, and a `Deferred` range becomes acquirable on the first pass after the batch activates.

`Deferred` is a derived, in-memory refinement of `Available`, and it persists as `Available`. The `__share_group_state` record encoding does not change. A share coordinator that reloads its state after a leader change reads back `Available`. It then re-derives the deferral against its own clock, and not against the old leader's.

### Durability Needs No New State

The schedule needs no durable timer store, because the schedule is the replicated records themselves. Each delivery time arrives in the log, replicates to the ISR, and survives a crash exactly as the record data does.

The delivery watermark is a derived value, in the same sense that the high watermark is derived. A leader that starts, or a follower that takes over, recomputes the watermark from the log and the clock. A recomputation of a derived value cannot disagree with the log it comes from. So a restart and a leader change need no special case, and no fence around a separate store.

The recovery walk is bounded. Each segment carries a maximum timestamp. Recovery skips a whole segment whose records are all active, and starts its record-level work at the first segment that can hold a pending batch. The walk touches the tail of the log, not the log.

### Followers Replicate Everything

Replication is not gated. A follower fetches, writes, and acknowledges a pending record as soon as the leader has it. The ISR, the high watermark, and durability all behave as they do on any other topic.

Only consumer delivery is gated. The split between the two is what makes the feature cheap. A scheduled record is a normal replicated record. Every guarantee the replication protocol gives applies to it before it is ever visible.

### Retention, Compaction, and DeleteRecords

A pending record that retention deletes is a record the broker promised to deliver and then dropped. Three paths can do that, and each one is closed.

Time retention closes itself. It compares a segment's maximum timestamp against the cutoff, and a record scheduled for the future raises that maximum above the cutoff by definition. So a segment that holds an undelivered record is never old enough to delete.

Size retention does not read timestamps, so it needs a guard. The guard stops size retention from deleting a segment that holds an undelivered record. An operator should know the consequence. A partition with a long schedule can hold more bytes than `retention.bytes`. The broker keeps a promised record and gives up the exact byte limit.

`DeleteRecords` closes the third path. Its `-1` sentinel means "delete up to the current end of the log", and the broker caps that at the delivery watermark. An admin call cannot delete a record that no consumer could have read yet.

Compaction cannot be closed the same way, so the broker rejects the combination. `cleanup.policy=compact` and `delivery.mode=scheduled` together are a configuration error at topic creation and at config alter time. Compaction deletes a record when a later record carries the same key. On a scheduled topic that later record can arrive long before the earlier one comes due. The earlier record would then be deleted without a single delivery, which is the failure the whole design exists to prevent.

### ListOffsets

On a scheduled topic, `LATEST` returns the delivery watermark.

The reason is the same one that forces offset order. A consumer that seeks to end sets its position to the value `LATEST` gives it. If that value were the log end offset, the consumer would step over every pending record in one move. Those records would then be unreachable for it forever. With the delivery watermark, seek-to-end lands the consumer on the first pending record, and the consumer receives it when it activates. Seek-to-end means "start with what is not yet delivered", which is what a caller on a scheduled topic wants.

Two honest notes belong with that.

First, this value can move backwards across a leader change, by up to twice the declared clock bound. The old leader and the new leader can read clocks that differ by up to `2e` and still be inside their bounds. The new leader can then compute a watermark up to `2e` behind the one a client saw a moment earlier. A client that calls `LATEST` on both sides of a leader change can see the smaller value second. The effect is bounded, and it gives a re-delivery and not a loss. An operator who wants a smaller bound lowers `delivery_clock_uncertainty`.

Second, krabka's `LATEST` returns the log end offset rather than the high watermark. That is a divergence from Kafka that exists today, and this KFC does not change it. On a scheduled topic the delivery watermark is the cap that applies on top of that behaviour.

### Cost on an Ordinary Topic Is Zero

A topic with `delivery.mode=immediate` pays nothing.

The watermark call returns the log end offset before it does any work. It reads no batch header, it walks no segment, and it takes no extra lock. So that cap never limits a fetch. The fetch path reaches `sendfile` on the same branch it reaches today. Zero-copy reads and byte-exact record passthrough are untouched for every topic that does not ask for scheduled delivery.

## Compatibility, Deprecation, and Migration Plan

There is no wire change, no new API key, no new error code, and no client change. A stock JVM producer sets a timestamp it can already set. A stock JVM consumer polls a topic that behaves like any other topic with a slower leader. The admin tools set the new configs the same way they set `retention.ms`.

krabka is greenfield and undeployed, so there is no migration. No on-disk format changes. No record already in a log means something different after this change.

One semantic overload is deliberate and worth stating plainly. On a topic with `delivery.mode=scheduled`, the record timestamp means the delivery time, and every Kafka feature built on the record timestamp inherits that meaning. The `.timeindex` maps activation times to offsets. `ListOffsets` by timestamp finds the first record that activates at or after the given time. `MAX_TIMESTAMP` returns the latest activation time in the partition. Those are not accidents of the implementation. They follow from the choice to use the timestamp, and each one is the more useful answer on a schedule.

The overload applies only to topics that an operator configured for it. A topic with `delivery.mode=immediate` keeps every Kafka meaning exactly as it is.

## Test Plan

The design makes claims about a state space, so the strongest tests enumerate that state space rather than sample it.

**Model checks.** The fetch read path already carries an exhaustive `stateright` model over the visibility window and its watermarks. The model gains the delivery watermark. The checker then enumerates the extended window, and proves that the clamp contract and KIP-227 monotonicity still hold under the new cap. A second `stateright` model covers the share-partition state machine with `Deferred` added. That machine holds the most reachable interleavings in the design.

**Formal verification.** The visibility decision is a pure kernel in `crabka-verified`, called by the fetch handler. The kernel gains a Creusot contract for the new cap. The proof covers every input, and not only the inputs a test picks.

**Integration.** In-process broker tests drive a mock clock. They produce a scheduled batch, move the clock across the activation boundary, and assert what a fetch returns on each side of it. A mock clock is what turns the lateness bound into a deterministic assertion, and not a race against real time.

**Differential.** A Docker-gated test runs a stock JVM producer and a stock JVM consumer against krabka. It is the direct proof of this KFC's central compatibility claim: a scheduled topic needs no client change.

## Rejected Alternatives

### A `krabka.deliver.at` Record Header

An explicit header is the first design most readers propose. It is per-record, it is self-describing, and it does not overload the timestamp.

The produce hot path rules it out. A header lives inside the record data, under the batch's compression, so the broker would have to decompress and decode every batch to find it. Today the broker writes the producer's bytes through without a look inside, and that is a large part of why the write path is fast.

The fetch path rules it out a second time. Per-record delivery times do not land on batch boundaries, and the fetch path enforces its offset cap against a batch's base offset. A cap that falls inside a batch still ships that batch whole, so a mixed batch would leak records that are not yet due. To honour per-record times exactly, the broker would have to re-encode batches on fetch, which is the next rejected alternative.

The batch's `max_timestamp` gives the same capability from a field that is already in the header. That field sits outside the compressed data, and every producer already maintains it.

### A Broker-Side Byte Filter on Fetch

The broker could decode each batch on the read path, drop the records that are not yet due, re-encode the remainder, and recompute the CRC.

That abandons zero-copy `sendfile` and the byte-exact passthrough guarantee, which are two of the broker's most valuable properties. Every fetch on a scheduled topic would cost a decompress, a re-compress, and a checksum over the whole batch.

The cost gives nothing back, because the filter does not solve the problem. A consumer whose position has moved past an offset never asks for that offset again. A filter that drops a record from a response makes it invisible now and unreachable later. That is the loss described in [Why Visibility Is Offset-Ordered](#why-visibility-is-offset-ordered-for-classic-groups). The filter hides the failure and does not prevent it.

### Record Relocation at Activation Time

The broker could accept a scheduled record into a staging area and append it into the topic when it comes due. This is the design that gives true out-of-order delivery for classic consumers, with no record held up behind an earlier one.

It breaks the produce contract. A produce response carries the record's final offset, and the producer's idempotence and the transaction machinery are built on that offset. A staged record has no final offset yet, because the offset depends on what else is appended between the produce and the activation. The broker would have to answer the produce with an offset it cannot know, or with no offset at all.

A produce response without the offset loses idempotent produce, and it loses exactly-once semantics with it. That loss is much larger than head-of-line order. It also falls on every producer to the topic, and not on the schedule that caused it.

### A Separate Durable Timer Store

The broker could keep a persisted timer wheel or a checkpoint file that lists the pending activations, and consult it on restart.

It is redundant state. The activation times are already in the replicated log. A second copy of them can corrupt, can fall behind, and can survive a record that a leader change rolled back. Any disagreement between the store and the log is a bug. It produces either an early delivery or a lost one, and it gives the operator a new file to repair.

A derived watermark cannot disagree with the log, because the log is its only input. The design keeps one source of truth and recomputes from it, which is the same choice the broker already makes for the high watermark.
