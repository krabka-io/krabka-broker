# KFC-7: Broker-Side Schema Validation

The broker refuses a record that does not match the schema its topic is bound to, with no wire-protocol change and no client change.

## Status

**Adopted.** The implementation lands on branch `claude/broker-schema-validation-registry-s451bp`.

The registry the broker reads is [`krabka-schema-registry`](https://github.com/krabka-io/krabka-schema-registry), extracted into its own repository on the same branch. The broker depends on one crate from it, `crabka-schema-serde`, for the wire framing and the REST client.

No KIP defines broker-side schema validation. Confluent Server has a proprietary form of it, and this document states where krabka sits next to that as well as next to Apache Kafka.

## Motivation

A schema registry is a promise between a producer and a consumer, and today nothing keeps it.

The promise is made in the producer's serializer. A registry-aware serializer resolves the subject, gets a schema id, frames the payload as `0x00 | id | body`, and writes it. Every consumer of that topic is then written against the schema, because the schema is what the topic is understood to hold.

The promise is kept only by convention. The broker accepts any bytes. A producer that skips the serializer, a service that writes with a plain `StringSerializer`, a misconfigured connector, a test fixture pointed at the wrong cluster, a `kafka-console-producer` in an incident: each one writes bytes that no consumer of that topic can decode, and the broker records them as durable, replicated, committed data.

The cost falls on the consumer, and it falls late. A consumer reads the bad record minutes or days later, fails to deserialize it, and stops. Its group stops with it, because the record is at the head of a partition and the position cannot move past a record the application cannot handle. Recovery is a person deciding to skip an offset in a system of record. The producer that caused it is long gone, and often nobody can say which one it was.

The broker is the right place to solve this for one reason: **it is the only place that every write passes through.** A client-side check protects the clients that run it, which is the set of clients that were never the problem. Kafka already rejects records for shape — a bad CRC, a malformed record body, a control batch from a client — and this is the same kind of rule, one level up: the topic declares what its records are, and a record that is not that is rejected where it is cheapest to reject, at produce time, to the producer that can still see the error.

## Public Interfaces

The feature adds three topic configs, one broker config section, and three metrics. It adds no API key, no new error code, and no field to any request or response.

### Topic Configs

| Config | Default | Values | Meaning |
| :--- | :--- | :--- | :--- |
| `schema.validation.key` | `false` | `true`, `false` | `true` validates every record key on this topic. |
| `schema.validation.value` | `false` | `true`, `false` | `true` validates every record value on this topic. |
| `schema.validation.mode` | `id` | `id`, `full` | What "validate" means. See [The Two Modes](#the-two-modes). |

`schema.validation.mode` has no effect on a topic where both of the other two are `false`. It is accepted there rather than rejected, so an operator can set the mode on a topic before turning validation on.

### Broker Config

Validation needs a registry to ask. It is configured in `broker.toml`:

```toml
[schema_registry]
url = "http://schema-registry:8081"
# Security-sensitive. See "When the Registry Is Unreachable".
fail_open = false
maximum_cache_size = 50000
expire_after_ms = 300000
```

`[runtime] schema_registry_http_timeout` bounds one registry call. It defaults to 5 seconds, the same as `opa_http_timeout`.

A topic that turns validation on while `[schema_registry]` is absent fails its produces with `INVALID_RECORD` and the reason `registry_unavailable`. It is not a topic that quietly accepts everything.

The check cannot live at startup, which would be the better place for it: a topic can be created, or have its config altered, at any time after a broker boots, so there is no moment at which the set of validated topics is known and fixed. Rejecting at produce time is what is left, and it fails closed for the same reason `fail_open` defaults to `false` — a record the broker could not validate has not been validated.

### Rejections

The broker rejects a produce request with the existing Kafka error `INVALID_RECORD`, code 87.

`INVALID_RECORD` is the right code because KIP-467 added it for exactly this sentence: one or more records in the batch were invalid. Every client that speaks Produce v8 or later already has it in its error table, and every one of them classifies it as it should be classified: not retriable. A producer that retries a record the broker called invalid would retry forever.

KIP-467 also added `RecordErrors` and `ErrorMessage` to the partition response, and the broker has never populated either. A schema rejection is the first thing that does. The response names the index of the offending record inside the batch and says what was wrong with it:

```
error_code:    87
record_errors: [{ batch_index: 3,
                  batch_index_error_message: "value schema id 42 is not registered under subject orders-value" }]
error_message: "one or more records failed schema validation"
```

A batch is rejected whole. That is Kafka's existing behaviour for `INVALID_RECORD` and it is what the batch's own CRC requires: the broker appends the producer's exact bytes, so it cannot append a batch with one record removed without re-encoding it and breaking the guarantee that the log holds what the producer wrote. The record errors say which records to fix; the producer resends the batch without them.

The fields are encoded only for Produce v8 and later. A v7 client sees the bare error code, which is what it would see for any other `INVALID_RECORD`.

### Metrics

- `schema_validation_rejections`, a counter per topic and reason. The reasons are `unframed`, `unknown_id`, `wrong_subject`, `body_mismatch`, and `registry_unavailable`.
- `schema_validation_cache_hits` and `schema_validation_cache_misses`, two counters. A miss is a registry round trip on the produce path, so the ratio is what says whether this feature costs anything at steady state.
- `schema_registry_errors`, a counter per reason. A rising value means the produce path is now failing for a reason outside the cluster.

`schema_validation_rejections` split by reason is the one an operator watches during a rollout. A run of `unframed` is a producer that never used a serializer. A run of `wrong_subject` is a producer writing the right format to the wrong topic.

## Proposed Changes

### What "Valid" Means

A validated field must carry the Confluent wire framing: a `0x00` magic byte, then a four-byte big-endian schema id, then the body. A Protobuf payload carries a message-index between the id and the body. This is the framing every Confluent-compatible serializer writes and `crabka-schema-serde` already reads.

Two shapes are accepted without a schema, because neither can carry one:

- **A null field.** A null key is ordinary, and a null value is a tombstone, which a compacted topic needs. Rejecting a tombstone would make validation and compaction mutually exclusive.
- **A field of zero length.** This is distinct from null on the wire and some clients produce it for an absent value.

Everything else on a validated field must be framed, and an unframed field is rejected with reason `unframed`.

### The Two Modes

**`id`, the default.** The broker checks three things: the framing is well formed, the schema id resolves in the registry, and that id is registered under the subject for this topic and role. The subject comes from Confluent's `TopicNameStrategy`: `<topic>-key` for a key and `<topic>-value` for a value.

The subject check is the one that carries the weight. Without it, validation only asks whether the bytes were framed by *some* registered schema, which a producer writing Avro-framed records to the wrong topic passes. With it, the record's schema is the topic's schema.

This is Confluent Server's behaviour, and it is the default here for the same reason it is the default there: it decides from the five-byte header alone. The body is never decoded, so the check costs one cache lookup per record.

**`full`.** Everything `id` does, and then the body is decoded against the resolved schema. Avro is decoded to a generic value, Protobuf into a `DynamicMessage`, and a JSON body is validated against its JSON Schema. Nothing is decoded into a Rust type — the broker has no type for a user's record — so what is proved is conformance and nothing more.

`full` exists because `id` has an honest gap: a producer that frames with a valid id and then writes a body that its own schema does not describe passes `id` mode. That producer is a bug rather than a misconfiguration, and it is rarer, but a system of record is exactly where it matters. The cost is a decode of every validated field on the produce path, so it is a choice rather than the default.

`full` is not equally strict across the three formats, and the difference is in the formats rather than in this design. Avro and JSON Schema both reject a body their schema does not describe. Protobuf rejects a declared field carried with the wrong wire type, and a body that is not a protobuf message at all, but it keeps an *undeclared* field number as an unknown field rather than failing — that is what the wire format says to do, and it is what makes protobuf forward-compatible. An operator running `full` on a Protobuf topic gets less than one running it on an Avro topic. `crabka-schema-serde`'s `validate_protobuf` states the same limit, and a test asserts it so it stays a known property.

### Where the Check Runs

The check runs in `process_partition`, after `prepare_batch` has validated the batch's CRC and record structure and before the batch is dispatched to the log.

That position matters in three ways.

`prepare_batch` is synchronous and a registry lookup is not. `process_partition` is async, and the transactional-produce check already awaits in the same window, so validation needs no blocking bridge and parks no runtime worker.

Structure is checked first. A batch that is not a well-formed batch is rejected as one, and validation never sees bytes that failed CRC. Every schema decision is made over bytes the broker has already proved intact.

The gate is resolved once per topic, next to the KFC-1 delivery gate, and threaded down. A topic with validation off costs one boolean test per partition and nothing else. There is no schema-validation code on the produce path of a topic that has not asked for it.

### The Verbatim Path Is Kept

The broker's produce fast path appends the producer's exact bytes. It validates the CRC and walks the records to check their structure, but it materializes no record, so there is no per-record key or value to look at.

Validation therefore decodes the batch a second time, for inspection only, and then throws the decoded view away and appends the original bytes. The log still holds exactly what the producer wrote, byte for byte, on a validated topic as on any other.

The cost is real and worth stating plainly: on a validated topic, a compressed batch is decompressed twice, once to check its structure and once to read its records. The alternative is to fuse the two walks in `crabka-protocol`, whose record walk already parses each record and discards it — nearly free, and a change in a third repository. That is the right follow-up and it is not this change.

### The Schema Cache

The broker holds one bounded cache, keyed by schema id, holding the schema text, its format, and the set of subjects the id is registered under. Entries expire on a TTL. The design is the OPA authorizer's decision cache, for the same reasons: an LRU bounds the memory a hostile or careless producer can make the broker hold, and a TTL is what makes a registry change observable.

A cache miss is one awaited HTTP round trip. It happens once per schema id per TTL, so a topic with one schema costs one registry call every `expire_after_ms` and nothing else. This is why the default TTL is 5 minutes rather than the authorizer's hour: a newly registered schema has to become usable without an operator restarting a broker, and a producer that registers a schema and immediately produces with it is the ordinary case.

Failures are cached too, for the same TTL. A produce storm against one unknown id then costs one registry call rather than one per record.

### When the Registry Is Unreachable

`fail_open` decides, and it defaults to `false`.

`false` is fail-closed: a registry that times out or errors makes the produce fail with `INVALID_RECORD` and reason `registry_unavailable`. The topic asked that its records be validated, and a record that could not be validated has not been.

`true` is fail-open: the record is admitted. This makes the registry stop being a produce-path dependency for availability, at the cost of a window where unvalidated records enter a topic that is supposed to be validated. That is a real trade and some operators will want it — a schema registry outage should not stop a payments pipeline — but it must be chosen, not inherited, so the default is the safe one.

This is the same knob, with the same default and the same argument, as `allow_on_error` on the OPA authorizer. An operator who has reasoned about one has reasoned about the other.

Turning validation on does make the registry a dependency of the produce path for the topics that use it. That is inherent: a broker cannot check a record against a schema it cannot read. The cache is what keeps the dependency from being per-record, and `fail_open` is what lets an operator decide what an outage means.

### Idempotent and Transactional Produce

Validation runs before the idempotent-sequence gate and before the transactional checks, so a rejected batch never reaches either. The producer's sequence number is not advanced by a batch the broker did not accept, which is the same handling every other pre-append rejection gets, and a rejected batch inside a transaction leaves the transaction open for the client to abort.

A duplicate batch that the idempotent gate would answer from its cache is not re-validated. It was validated when it was first accepted, and the answer cannot have changed for bytes that are already in the log.

## Compatibility, Deprecation, and Migration Plan

**Nothing changes for a topic that does not opt in.** All three configs default off, and the produce path of an unvalidated topic is the path it is today.

**No client changes.** There is no new error code, no new field, and no protocol version. A client that already handles `INVALID_RECORD` — every client does, it is what a malformed batch returns — handles this.

**A pre-v8 client loses the explanation, not the rejection.** `RecordErrors` and `ErrorMessage` were added in Produce v8. An older client sees error code 87 and no message. This is a limitation of KIP-467 rather than of this design, and the broker logs the same detail so an operator can recover it.

**Turning validation on is not retroactive.** Records already in the log were not checked and are not rechecked. Compaction, retention, and replication do not consult the registry: a follower replicates what the leader accepted, and a validated topic's history is only as clean as the moment validation was turned on. An operator who wants to know whether the existing data conforms has to read it.

**Turning it on can break a producer, which is the point.** The rollout path is to set `schema.validation.value=true` on a copy of the topic, or to watch `schema_validation_rejections` on a canary, before setting it on a topic with live writers. The metric is split by reason precisely so this is a measurement rather than a guess.

There is nothing to deprecate and nothing to migrate. The feature is new, it is off, and it stays off until a topic asks for it.

## Test Plan

Four layers cover the feature.

**Unit.** The framing parser over the shapes that matter: a good frame, a bad magic byte, a truncated header, a null field, an empty field, and a Protobuf message-index in both its long and its single-byte optimized forms. The cache over hit, miss, TTL expiry and negative caching, driven by a mock clock so an expiry is an assertion and not a sleep.

**Component, against a faked registry.** `wiremock` serves the registry REST API, which is how the OPA authorizer's tests already work. This is the tier that covers the decision table — accepted, unknown id, wrong subject, body mismatch — and both settings of `fail_open` against a registry that errors and one that hangs.

**Integration, against a live broker.** The in-process broker harness, driving real `CreateTopics` and `Produce`. Each case runs against a validated topic and an unvalidated control topic, in the shape KFC-1's suite established, so a result is shown to be the configuration rather than a path every topic now takes. Every rejection asserts two things: the error code, and that the partition's log end offset did not move. A rejection that still appended would be the worst possible failure of this feature, so no test takes the error code as proof on its own.

**End to end, against the real registry.** A `krabka-schema-registry` and a broker, with a record registered through the registry's own REST API and then produced. This is the tier that proves the two halves agree about subjects, ids and framing, rather than agreeing with a mock of each other.

## Rejected Alternatives

### The `confluent.*` Config Names

Confluent Server calls these `confluent.key.schema.validation` and `confluent.value.schema.validation`. Taking those names would make every Confluent tutorial, `kafka-configs` recipe and Terraform module work unchanged against krabka.

It loses on what a config name says. A `confluent.` prefix in krabka's topic-config table names a vendor that has nothing to do with this broker, and it would have to stay there forever, because a config name is a compatibility surface. The KFC namespace already has a convention — KFC-1 added `delivery.mode`, not `confluent.delivery.mode` — and this follows it.

The compatibility that matters here is the registry's, and that is kept exactly: the REST API, the subject strategy and the wire framing are Confluent's. What an operator types into `kafka-configs` is a smaller surface than what a client library speaks, and it is the one that costs least to differ on.

### Aliasing Both Names

Accepting `confluent.value.schema.validation` as an alias would give the tooling compatibility with none of the naming cost.

It loses on the project's own rule. An alias kept for another product's spelling is a compatibility shim, and krabka is greenfield and undeployed: there is no deployment that has the old name written down. Two spellings for one setting also means two things to validate, two things to document, and a question about what happens when they disagree.

### Validation in the Client Serde Only

`crabka-schema-serde` could refuse to serialize a value its schema does not describe, and every krabka client would then produce only valid records.

It loses on the producers this is about. A client-side check protects the clients that run it, and those clients were already using a serializer, which is why their records were already valid. It does nothing about `kafka-console-producer`, a connector with the wrong converter, a service in another language, or a test fixture pointed at production. The broker is the only place every write passes through, and that is the whole argument for putting the check there.

The client-side check is still worth having, because an error at serialize time is better than an error at produce time. It is not a substitute.

### Full Payload Validation as the Default

`full` mode could be the only mode, and then a topic that turns validation on gets the strongest guarantee available.

It loses on cost and on choice. Decoding every record against its schema on the produce path is a real per-record cost, paid on the leader, on the hot path, for a failure — a producer whose own serializer wrote a body its own schema does not describe — that is much rarer than the misconfiguration `id` mode catches. Making it the only mode would mean an operator who wants the common protection has to pay for the rare one. The mode config lets each topic decide, and the default is the one Confluent Server made for the same reasons.

### Rejecting the Single Bad Record and Appending the Rest

The broker could drop the offending record and append the others, so one bad record from a shared producer would not fail its batch-mates.

It loses on the verbatim guarantee. The broker appends the producer's exact bytes, and a batch is one CRC-checked unit. Removing a record means re-encoding the batch, which changes what the log holds and costs the pass-through path on every validated topic. It would also silently discard data the producer believes it wrote, and a producer that gets a success for a record the broker deleted is worse off than one that gets an error. KIP-467's `RecordErrors` exists to name the bad records inside a rejected batch, which is the same information without the silent loss.

### A Broker That Blocks on a Cold Registry Miss Without a Cache

The simplest implementation asks the registry per record. It is correct and it is trivially consistent with the registry.

It loses on throughput by an amount that makes the feature unusable: an HTTP round trip per record turns a produce of a thousand records into a thousand round trips. The cache is not an optimisation here, it is what makes the design possible, and the TTL is what keeps it honest.

### A New Error Code for a Schema Rejection

A dedicated code would let a client tell a schema rejection from a malformed batch without reading the message.

It loses at the client. An unknown error code arrives at a client as unknown, and unknown is where retry logic goes wrong — some clients retry it, some fail the send, and none of them do the right thing. `INVALID_RECORD` is already the code for "this record is not acceptable", every client classifies it as non-retriable, and KIP-467's `RecordErrors` carries the detail a new code would have carried, to the clients new enough to have asked for it.

### Storing the Topic's Schema in Topic Config Instead

The topic could carry its schema, or a schema id, directly in its config, and the broker would need no registry at all.

It loses on evolution, which is the reason a registry exists. A schema is a history of versions with a compatibility rule between them, not one value. Putting the current version in topic config gives no way to accept the previous version during a rolling producer upgrade, no compatibility check when the schema changes, and no place for the consumers to read the writer's schema from. It would also mean the metadata log carried schema text, which can be large, and which every broker would then hold in its metadata image.
