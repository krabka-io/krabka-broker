# KFC-6: Coordination Primitives as a Client API

Leader election, leases, and fencing epochs as a first-class krabka API, with the epoch minted by the metadata quorum and readable by any system that has to check a writer's authority.

## Status

**Under discussion.** No implementation exists. This document is the specification for one, and it lands on branch `claude/coordination-primitives-api-f9pk11`.

No KIP defines a coordination API. Kafka gives a client three mechanisms that come close to one, and none of them is one. The [Motivation](#motivation) states what each of them misses. The [Compatibility](#compatibility-deprecation-and-migration-plan) section states the one Kafka surface this design reuses, which is transactional-id fencing.

## Motivation

Every team that runs an active-standby service beside Kafka builds leader election by hand, out of parts that Kafka gives for another purpose.

Three recipes cover almost all of them.

- **A compacted lock topic.** Each contender produces a claim record under the lock's key, with its own id as the value. The contender whose record lands last believes it won. A contender that read the log a moment earlier believes the same thing.
- **A consumer group with one member.** The service creates a topic with one partition and joins a group. The member that gets the partition calls itself the leader.
- **A shared `transactional.id`.** Each contender calls `InitProducerId` with the same transactional id. The call bumps the producer epoch, so the previous holder's next write fails.

Each recipe fails in a way that is quiet, and that the author of the recipe usually does not know about.

**A claim record is not mutual exclusion.** A produce that the leader accepts tells the producer that its record is in the log. It does not tell the producer that no other record is in the log. Two contenders can both produce a claim, both get success, and both read a log in which their own record is last, because each one read at a different time. Compaction then keeps one key and one value, and it keeps them long after both processes acted.

**Group membership is advice, not a fence.** A member that a rebalance removed keeps running until its own poll loop notices. A JVM member in a long garbage-collection pause notices nothing for the length of the pause. The group's generation id moves on without it, and the member goes on writing. `max.poll.interval.ms` is the size of that window, and it is a client-side timer measured on the client's own clock.

**A generation id is not a fencing epoch.** It resets when the group is deleted, and it resets when the offsets partition that holds the group is recreated. A number that can go backwards fails at exactly the moment it matters. An external system that kept the highest generation it saw rejects the new leader forever, or it accepts an old one.

**A producer epoch fences krabka and nothing else.** It is the only one of the three that is a real fence, and its reach stops at the broker. A leader that writes to Postgres, to S3, or to a payment gateway gets no protection from it. Its width is also fixed: the epoch is an `i16` per transactional id, so a service that fails over often exhausts it and gets a new producer id, which resets the comparison.

**No recipe gives a third party a number to check.** This is the part that gets built wrong most often. A fencing epoch works only when the **resource** checks it, and the resource can check it only when the resource can learn it. In every recipe above, the epoch lives in the leader's memory. An external system that receives a write with an epoch attached has no way to find out what the current epoch is, so it trusts whatever number the writer sends.

The result is a class of incident that reads the same every time. Two processes both believe they hold a role. Both write. The state that they write is interleaved, and no single log shows the interleave, because the two writes went to a system that does not order them.

krabka already holds everything the primitive needs. The metadata quorum is a Raft log with a single writer, no unclean election, and a strict order over every record it commits. The active controller already tracks broker liveness on heartbeats that write no record. The transaction coordinator already fences a producer by id. This KFC binds those three together and gives the result a client API.

### What This Is Not

This is a control-plane primitive for roles that a service holds for minutes or hours. It is not a lock service for fine-grained keys.

Every heartbeat reaches the active controller, so the controller's request rate is the count of contenders divided by the heartbeat interval. A cluster with 200 contenders on a 3-second interval adds about 67 requests per second to the controller. A cluster with 200,000 contenders does not work. The [broker configs](#broker-configs) bound the counts so that a misuse fails at role creation and not in production.

## Public Interfaces

The feature adds three krabka-private api keys, five krabka-private error codes, four broker configs, four role configs, one internal topic, and eight metrics. It adds no Kafka api key, no field to any Kafka request or response, and no value to any Kafka error table.

### Concepts

| Term | Meaning |
| :--- | :--- |
| Role | A named, cluster-scoped object that at most one member holds. `payments-controller` is one. |
| Member | One process that contends for a role. It carries a `member_id` that the client generates, as KIP-848 members do. |
| Succession line | The ordered set of members that contend for one role. The head of the line holds it. |
| Grant | The record that binds a role to a member. It carries the epoch and the lease term. |
| Epoch | The fencing epoch of a grant. It is an `i64`, it is unique across the cluster, and it never decreases. |
| Lease | The time for which a grant is good. The holder renews it with a heartbeat. |

### Api Keys

Three keys sit in the krabka-private range at 1000 and above, beside the barrier keys at 1010 to 1014. Each one speaks version 0 only, with flexible framing. The broker registers them for dispatch and never advertises them, so `kafka-broker-api-versions.sh` prints no row for them and a JVM `AdminClient` cannot send them.

| Api key | Name | Purpose |
| :--- | :--- | :--- |
| 1020 | `CoordinationHeartbeat` | Join a succession line, renew a lease, or leave. It is the only api that grants a role. |
| 1021 | `DescribeCoordination` | Read roles, holders, epochs, lease deadlines, and succession lines. |
| 1022 | `AlterCoordinationRoles` | Create, alter, and delete roles. |

One RPC covers join, renew, and release, in the shape KIP-848 gave `ConsumerGroupHeartbeat`. The reason is a client-side one. A grant appears in exactly one place, which is a heartbeat response, so a client cannot end up with two code paths that both believe they took the role.

`DescribeCoordination` reads the local metadata image and forwards nothing. Every broker holds the image, so a caller gets an answer from the broker it is already connected to. `CoordinationHeartbeat` and `AlterCoordinationRoles` change state, so the receiving broker forwards them to the active controller on the path that `CreateTopics` already uses. A broker that cannot reach the active controller answers `NOT_CONTROLLER` (41), which is the code Kafka already uses for that condition.

### Error Codes

Apache Kafka assigns error codes upward from 0, so krabka reserves 1000 and above. `BARRIER_INJECTION_IN_PROGRESS` (1000) already sits there. A broker returns a code in this range only on a krabka-private api key, so none of these reaches a JVM client.

| Code | Name | Meaning |
| :--- | :--- | :--- |
| 1001 | `COORDINATION_ROLE_NOT_FOUND` | The role does not exist. Kafka's `GROUP_ID_NOT_FOUND` (69) drives JVM group state machines, so it does not carry this meaning. |
| 1002 | `COORDINATION_ROLE_EXISTS` | A create names a role that exists. |
| 1003 | `STALE_COORDINATION_EPOCH` | The caller sent an epoch that is not the current one. Its grant ended. |
| 1004 | `SUCCESSION_LINE_FULL` | The line holds `succession.max` members already. |
| 1005 | `COORDINATION_FENCE_UNAVAILABLE` | The bound transactional ids could not be fenced, so no grant was written. |

The rest of the surface reuses Kafka codes that every client already handles: `NOT_CONTROLLER` (41), `INVALID_CONFIG` (40), `INVALID_REQUEST` (42), `CLUSTER_AUTHORIZATION_FAILED` (31), and `GROUP_AUTHORIZATION_FAILED` (30).

### Broker Configs

| Config | Default | Meaning |
| :--- | :--- | :--- |
| `coordination.max.roles` | `256` | The largest number of roles the cluster holds. A create past the bound fails with `INVALID_CONFIG`. |
| `coordination.max.contenders.per.role` | `16` | The ceiling on a role's `succession.max`. |
| `coordination.lease.min.ms` | `5000` | The floor on a role's `lease.ms`. |
| `coordination.clock.uncertainty.ms` | `250` | The bound on the difference between the controller's clock and a member's clock. It carries the same meaning and the same default as `delivery_clock_uncertainty` in [KFC-1](KFC-1-deliver-at-time-visibility.md). |

The lease floor exists because a short lease costs twice. It multiplies controller heartbeat traffic, and it makes a controller failover more likely to expire a holder that is alive.

### Role Configs

An operator sets these with `AlterCoordinationRoles`, and reads them back with `DescribeCoordination`.

| Config | Default | Meaning |
| :--- | :--- | :--- |
| `lease.ms` | `15000` | The lease term of a grant. |
| `heartbeat.interval.ms` | `3000` | The interval the broker asks a member to heartbeat at. |
| `fence.transactional.ids` | empty | Transactional ids the coordinator fences when a grant ends. |
| `succession.max` | `8` | The largest succession line for this role. |

### The Coordination State Topic

`__coordination_state` is a compacted internal topic with one partition. The active controller writes one record to it for each grant and each vacancy, keyed by the role name.

The topic exists for readers that cannot send a krabka-private api key. A JVM consumer, a Flink job, a Postgres change-data pipeline, and a shell script with `kafka-console-consumer` all read it with no krabka client at all. One partition means such a reader finds every role without a partitioner and without a `FindCoordinator` call.

The value layout is deliberately plain, with fixed-width big-endian integers and no compact-varint framing, because the Java and Go clients decode it by hand. It carries the epoch, the holder's `member_id`, the lease deadline, and the reason the last change happened. A tombstone means an operator deleted the role. The [Test Plan](#test-plan) states where the byte-exactness is asserted.

The topic is a projection and not the source of truth. The metadata log is the source of truth, and the topic can lag it. [What a Third Party Must Check](#what-a-third-party-must-check) states why the lag is safe.

### Authorization

Role administration needs `Alter` on `Cluster:kafka-cluster`, which is the resource barrier group administration already authorizes against.

A role's own operations map onto the `Group` resource type, with the resource name `krabka-role.<role>`. `CoordinationHeartbeat` needs `Read`, and `DescribeCoordination` needs `Describe`. Joining a succession line is joining a named set of members, so the `Group` type is the closest fit that Kafka has.

The name prefix is the whole reason the mapping is safe. Without it, an ACL that grants `Read` on group `payments` would also grant the role `payments`, and an operator would give away leadership while they meant to give away a consumer group. With the prefix, the two collide only when a real consumer group is named `krabka-role.payments`. An operator should not create such a group, and krabka cannot stop them, because Kafka groups have no reserved names.

### Metrics

| Metric | Kind | Meaning |
| :--- | :--- | :--- |
| `coordination_roles` | gauge | Roles the cluster holds. |
| `coordination_current_epoch` | gauge, per role | The epoch of the live grant, or `-1` when the role is vacant. |
| `coordination_succession_depth` | gauge, per role | Members in the line behind the holder. |
| `coordination_lease_remaining_seconds` | gauge, per role | Time left on the live grant, on the controller's clock. |
| `coordination_grants_total` | counter, per role | Grants written. |
| `coordination_revocations_total` | counter, per role and reason | Grants ended, by reason: `expired`, `released`, `role_deleted`. |
| `coordination_vacancy_duration_seconds` | histogram, per role | Time from the end of one grant to the start of the next. |
| `coordination_fence_failures_total` | counter, per role | Grants that a fence failure stopped. |

`coordination_vacancy_duration_seconds` is the one an operator watches, because it is the failover time this design promises. `coordination_succession_depth` at zero on a role that matters is the alert: the role has a holder and no standby.

## Proposed Changes

### The Epoch Comes From the Metadata Quorum

The epoch is allocated by the active controller and committed to `__cluster_metadata` in the same batch as the grant it belongs to.

That single choice gives four properties that no recipe in the [Motivation](#motivation) has.

**It cannot go backwards.** Raft has no unclean election. A record that the quorum committed is in the log of every future leader. So an epoch that a resource accepted stays accepted.

**It is monotonic across the whole cluster, not per role.** One counter serves every role. A role that an operator deletes and recreates with the same name gets epochs above the ones the old role used, so a resource that kept its high-water mark keeps working. A per-role counter would restart at zero on the recreate, and a resource that kept its mark would then reject the new holder forever. That failure is silent at the moment it is introduced and loud a year later, which is the class of defect this KFC exists to remove.

**It is comparable with no context.** A resource guarded by two roles during a migration keeps one number, not a map from role name to number.

**It is readable from every broker.** The metadata image reaches every broker over the standard metadata fetch, so `DescribeCoordination` is a local read.

The counter lives in its own record, and a tombstone never removes it. A counter recovered as the maximum over live grant records would reset when the last role is deleted, which is the same defect as the per-role counter.

One counter serves two purposes. A grant consumes a value and calls it the epoch. A join consumes a value and calls it the member's place in the succession line. Values are not dense, and nothing needs them to be. The gain is that the order of grants and the order of the line come from one source, so the two can never disagree after a controller failover.

### Heartbeats Write No Record

A lease renewal writes nothing to the metadata log. The active controller holds the last-seen time for each member in memory, exactly as `ControllerLivenessState` holds it for broker heartbeats under KIP-500.

The metadata log then carries traffic proportional to failovers, and not to holders multiplied by heartbeat rate. A cluster with 200 contenders on a 3-second interval writes zero metadata records per second while nothing changes. A failover writes four: the vacancy, the departed member's tombstone, the counter, and the new grant.

Four events write records: a member joins a line, a member leaves a line, the controller grants a role, and a grant ends. All four are rare.

### A Controller Failover Extends Every Lease

In-memory liveness state does not survive a controller failover. The new active controller has no last-seen time for any member.

Kafka answers this for brokers by starting the session clock when the controller becomes active, and this design copies the answer. A new active controller gives every live grant a full lease term measured from the moment it took leadership.

The consequence is worth stating plainly, because a reader who is checking the safety argument needs it. **A controller failover can extend a live lease by up to one lease term.** A holder that died one millisecond before the controller failover keeps its grant for a further `lease.ms`. The design accepts that, because the alternative is to expire every live holder on every controller failover, which turns one controller fault into a failover of every role in the cluster.

### The Coordinator Fences Before It Grants

A role can name transactional ids in `fence.transactional.ids`. When a grant ends, the controller fences each of them through the transaction coordinator, on the same epoch-bump path that `InitProducerId` uses. It does that **before** it writes the next grant record.

The old holder's next produce then fails with `INVALID_PRODUCER_EPOCH` (47), which is the code `from_broker_error` already maps a fenced producer epoch onto. It is an existing Kafka code that every client treats as fatal for that producer. The data path needs no new code, no new error, and no krabka client.

The order is the whole guarantee. A fence that ran after the grant would leave a window in which the new holder writes and the old holder is still able to write.

A fence that fails stops the grant. The role stays vacant, the heartbeat that would have taken it gets `COORDINATION_FENCE_UNAVAILABLE`, and `coordination_fence_failures_total` counts it. An operator should know the trade this makes: **a role with bound transactional ids does not fail over while its transaction coordinator is unavailable.** The design prefers a vacant role over two writers, because a vacant role is an outage and two writers is corruption.

The reach of this mechanism stops at krabka. It fences writes into the cluster. It fences nothing in Postgres, in S3, or in a payment gateway. For those, the epoch check in the next section is the only mechanism.

### What a Third Party Must Check

A resource outside krabka must keep the highest epoch it has accepted, and it must reject a write whose epoch is below that value. It accepts a write whose epoch is at or above it, and it raises its stored value to match.

That rule is the whole protocol, and it needs no query at all.

The rule that teams write instead is "ask the coordinator who holds the role, and accept the write if the answer matches the writer". That rule is wrong for two separate reasons, and both of them are quiet.

It is a race. The answer describes the moment the coordinator read its state. A write from a holder whose grant ended a microsecond later still matches, and the resource accepts it. Making the query faster narrows the window and never closes it.

It is also unnecessary. The monotonic check needs nothing from the network. It compares two integers that the resource already has, and it is correct even when the coordinator is unreachable.

`DescribeCoordination` and `__coordination_state` exist for two other jobs.

The first is recovery. A resource that lost its stored high-water mark reads the current epoch and sets its mark to that value. Every older writer is then rejected, and the live holder is not.

The second is diagnosis. An operator asks which member holds a role and how long its lease has left.

Lag in `__coordination_state` is safe because of this split. A reader that sees a stale epoch under the monotonic rule never accepts a write it should reject, because the rule compares the writer's epoch against the resource's own mark and not against the topic. A stale read can only give a stale answer to "who holds it now", which is a diagnosis question.

### What the Design Guarantees, and What It Does Not

**Two members can believe they hold a role at the same instant.** The design does not prevent that, and no lease-based design can. A holder can be paused by its operating system for longer than its lease, and it wakes up believing nothing changed.

What the design does guarantee is exact:

1. Epochs strictly increase, and the quorum never reuses one.
2. At most one member holds a given epoch.
3. The controller fences a role's bound transactional ids before it writes the next grant.
4. The controller grants a role only to the head of its succession line.

The gap between the guarantee and mutual exclusion is closed at the resource, by the monotonic check, and nowhere else. A design that claims to close it in the coordinator is claiming to control a clock it does not own.

The lease still earns its place. It bounds the time a dead holder blocks the role, and it gives a live holder a deadline it can act on locally. A client marks its own handle revoked at the lease deadline minus `coordination.clock.uncertainty.ms`, on its own clock, and it does that without waiting for any server reply. That local rule is the one every client library must implement the same way, and [Client API Parity](#client-api-parity) states it again for that reason.

### The Succession Recipe

A role's contenders form one ordered line. The order is the order of the sequence numbers that the quorum minted when each member joined. The head of the line holds the role.

Four rules define it, and together they give flap-free failover. A flap is a second leadership change that one fault causes.

1. **The controller grants only to the head.** No preference, no weight, and no configuration selects a different member.
2. **A member that joins enters at the tail.** Its sequence number is above every number the quorum has minted, so its place is arithmetic and not policy.
3. **A member whose grant ends leaves the line.** It does not keep its old place. A holder that returned to the head after a lease expiry would take the role straight back, which is the flap this rule removes.
4. **A recovered member re-enters at the tail.** It calls acquire again, gets a new sequence number, and stands behind every member that stayed up.

The arithmetic is the argument. Take three nodes A, B, and C, with A holding the role. A crashes and recovers.

With a static preference list, the cluster does two transitions: A to B when A crashes, then B to A when A recovers. Each transition costs a fence, a vacancy, and whatever a handover costs the application. The second transition buys nothing, because B was serving correctly.

With the succession line, the cluster does one transition. A crashes, B takes the role, and A re-enters behind C. A takes the role again only when B and C have both gone. One fault, one transition, and no failback.

This is the recipe that ZooKeeper's sequential ephemeral znodes gave, and it is the reason that recipe stayed in use. The design puts it in the coordinator rather than in each client, because a rule that every application re-implements is a rule that some applications get wrong.

A planned handover uses the same path. The holder releases, which removes it from the line, and it then calls acquire again to stand at the tail as a standby. There is no separate "step down and stay preferred" operation, because that operation is failback with another name.

The design deliberately has no equivalent of KIP-345 static membership. A static id exists to let a restarted process reclaim its old place, and reclaiming an old place is exactly what rule 4 forbids.

### Metadata Records

Four record types carry the state. They live in `crabka-metadata`, in the sibling repository `krabka-protocol`.

| Record | Content | Tombstoned |
| :--- | :--- | :--- |
| `V1CoordinationSequence` | The next value of the cluster-wide counter. | Never. |
| `V1CoordinationRole` | The role name and its four configs. | On role delete. |
| `V1CoordinationMember` | A member's place in one line: role, `member_id`, and sequence number. | When the member leaves or its session ends. |
| `V1CoordinationGrant` | Role, holder, epoch, lease term, and grant time. A vacancy is a grant record with no holder and a reason. | Never. |

A grant is one batch that carries a `V1CoordinationSequence` and a `V1CoordinationGrant`. `submit_change` applies a batch atomically, so the counter and the grant cannot disagree.

[KFC-2](KFC-2-witness-broker-stretch-cluster.md) put a broker flag into an existing `V1BrokerConfig` record rather than add a field to `BrokerRegistrationRecord`, because a sibling-repository change means a revision bump in two places. That reasoning does not carry here. Four new record types are not a flag, and encoding them as configuration values would give an operator a `kafka-configs` surface that mutates leadership. The [Compatibility](#compatibility-deprecation-and-migration-plan) section states the revision-bump work this needs.

### Client API Parity

`krabka-client-rs` gains a `crabka-client-coordination` crate. `krabka-client-java` and `krabka-client-go` gain the same primitive.

The Rust surface is a guard.

```rust
let leadership = coordination.acquire_leadership("payments-controller").await?;
let epoch: i64 = leadership.epoch();

// `is_current` is a local deadline check. It sends nothing.
while leadership.is_current() {
    resource.write_with_epoch(epoch, next_payload()).await?;
}

leadership.revoked().await;
```

`acquire_leadership` returns when the role is granted. The handle owns the heartbeat task. A drop releases the role, so a process that panics on one task does not hold a role until its lease runs out.

The handle carries the two values a grant is made of. `epoch` is the number a resource checks, and `lease_deadline` is the instant the grant ends on the caller's own clock. `is_current` compares that instant against the clock bound, so a caller reads the lease without restating the arithmetic.

Each language keeps its own idiom for the same state machine. Java returns an `AutoCloseable` handle with a `CompletionStage` for revocation, so a caller uses try-with-resources. Go returns a handle whose `Done()` channel closes on revocation, so a caller uses `select`. The names differ and the semantics do not.

One rule is identical in all three, and it is the rule a hand-written client gets wrong. **A client marks its handle revoked at `lease_deadline - clock_uncertainty`, measured on its own clock, with no server round trip.** A client that waits for the server to tell it that the lease ended keeps `is_current` true through exactly the network partition that made the lease end.

Two further rules follow from it. `is_current` reads a local deadline and never sends a request, because a check that can block is a check that gets skipped in a hot path. A failed heartbeat marks the handle revoked rather than retrying past the deadline, because a retry that outlives the lease is indistinguishable from a live grant to the code that calls `is_current`.

The operator surface is `krabka coordination`, in the shape that `krabka barrier` set: a library and a binary in one crate, so a test calls `run_from_args` in process instead of spawning a binary that a Bazel test sandbox cannot build.

## Compatibility, Deprecation, and Migration Plan

Nothing in Kafka's wire protocol changes. No api key gains a version, no request or response gains a field, and no Kafka error table gains a value.

The three new api keys sit at 1020 to 1022 and the broker never advertises them. A client that finds no row negotiates version `(0, 0)`, and `kafka-broker-api-versions.sh` prints no row that a real Kafka broker would not print. A JVM `AdminClient` cannot reach the control plane at all.

One surface is visible to a stock Kafka client, and it is visible on purpose. `__coordination_state` is an ordinary compacted topic that any consumer reads. `is_internal_topic` lists it, so `kafka-topics --list --exclude-internal` hides it and an application cannot delete it by accident.

A JVM transactional producer bound to a role sees `INVALID_PRODUCER_EPOCH` (47) after a revoke. KIP-98 introduced that code with transactions, and the JVM producer already treats it as fatal for the producer instance. A client that follows the Kafka error table needs no change.

krabka is greenfield, so there is no migration and no feature flag. The primitive is opt-in by use: a cluster with no roles carries no coordination state, writes no metadata records, and runs no extra task.

An operator who already runs an election built by hand keeps it. Nothing in this design changes any existing behaviour, and nothing forces a move.

The work reaches one sibling repository. `crabka-metadata` gains four `MetadataRecord` variants, so this needs a `krabka-protocol` revision in the `[patch.crates-io]` block at the bottom of the root `Cargo.toml`, a regenerated lockfile, and both files committed together. `crabka-raft` needs no new RPC, because every state change rides the existing `submit_change` path.

## Test Plan

Five layers cover the feature, in the shape that [KFC-2](KFC-2-witness-broker-stretch-cluster.md) set.

**Formal proof.** `crates/verified/src/coordination.rs` carries Creusot contracts for the arithmetic the safety claim rests on. `epochs_strictly_increase` states that the counter never repeats a value across an arbitrary sequence of grants, joins, role deletes, and role recreates. `grant_follows_line_head` states that the granted member is the least sequence number in the line. Proof sessions live under `verif/crabka_verified_rlib/coordination/`.

**Model check.** An exhaustive stateright search over controller failover, heartbeat loss, message reorder, clock skew inside the declared bound, and a role delete during a live grant. The properties are the four in [What the Design Guarantees](#what-the-design-guarantees-and-what-it-does-not), plus the succession property that one fault produces one transition. The model drives the real decision functions and not a copy of them, and a deliberately broken grant function proves that the mutual-exclusion property fires.

**Integration.** A live cluster covers acquire, renew, release, lease expiry, controller failover during a live grant, and the fence-before-grant order. The fence test asserts that the old holder's produce fails before the new holder's first produce succeeds, which is the order the guarantee depends on.

**JVM differential.** A JVM consumer reads `__coordination_state` and decodes the values, and a JVM transactional producer bound to a role sees the fence with the error code its own table names. This is the tier that proves the two Kafka-visible surfaces really are Kafka.

**Cross-language.** The `__coordination_state` value layout is frozen, and the same bytes are asserted in `krabka-client-rs`, `krabka-client-java`, and `krabka-client-go`, as the barrier cut format already is. A client-parity suite runs the same succession scenario against all three and compares the epoch sequence each one observed.

## Rejected Alternatives

### A Compacted Lock Topic With Claim Records

This is the recipe most teams build. It loses on mutual exclusion, which is the property it exists to give.

A produce tells the producer that its own record is in the log. It does not tell the producer that no other claim is in the log, so two contenders can both succeed and both believe they won. There is no lease, so a dead holder holds the key until a live process overwrites it. The offset of the winning record is not a usable epoch either, because the topic is replicated by the in-sync replica set and an unclean election can roll it back.

### Consumer Group Membership as the Election

A single-partition topic and a one-member group is the cheapest recipe to write, and it gives no fence at all.

An assignment is advice that a member acts on when its own poll loop next runs. A member in a long pause keeps writing after the group moved on. The generation id resets when the group is deleted, so it is not usable as a fencing epoch. The eviction timer is `max.poll.interval.ms`, measured on the client's clock, which is the clock this recipe needs to distrust.

### A Shared Transactional Id Alone

This is the only recipe of the three that is a real fence, and its reach is the problem.

It fences writes into krabka and nothing else, so a leader that writes to any other system gets no protection. The epoch is an `i16` per transactional id, so a service that fails over often exhausts it and gets a fresh producer id, which resets the comparison a resource was keeping. There is also no way for a third party to learn the current epoch.

This design keeps the mechanism and adds what it lacks. `fence.transactional.ids` binds a role to it, so a krabka-only deployment gets data-path fencing with no client change.

### An External Coordination Service

ZooKeeper, etcd, and Consul all give this primitive, and they give it well.

They lose on operational surface. The cluster already runs a Raft quorum with a single writer and a strict commit order, and adding a second one gives the operator a second failure domain, a second upgrade path, and a second set of credentials. A role whose holder writes to krabka also gets no data-path fencing from them, because the broker knows nothing about their epochs.

An operator who already runs one of them should keep using it. This design exists for the operator who does not want to.

### An Internal Topic Behind a Coordinator

The state could live in `__coordination_state` behind a coordinator, in the shape `__transaction_state` uses. That shape scales further, because it partitions.

It loses on the property this KFC is about. An internal topic is replicated by the in-sync replica set, so an unclean election can roll an epoch back, and an epoch that goes backwards defeats every resource that kept a high-water mark. A read would also need a `FindCoordinator` hop instead of a local image read, and the epoch would need an allocated counter with its own recovery path.

What the metadata log gives up is write scale, and [What This Is Not](#what-this-is-not) states the bound that follows.

### A Heartbeat Record for Each Renewal

Writing a record for each renewal would make lease state fully durable, so a controller failover would lose nothing and no lease would be extended.

It loses on volume and on latency. The metadata log would carry traffic proportional to holders multiplied by heartbeat rate, and every renewal would become a quorum write on the hot path of every holder. KIP-500 already rejected this for broker heartbeats, and the reasoning is the same one.

### A Static Preference List for Succession

An ordered list of preferred members is what an operator asks for first, because it makes leadership predictable.

It loses on flapping. A recovered member takes the role back, so one fault produces two transitions and the second one buys nothing. [The Succession Recipe](#the-succession-recipe) gives the arithmetic. Predictability is real, and `DescribeCoordination` gives it back: the line is readable at any time, so an operator can see who takes the role next.

### A New ACL Resource Type for Roles

A `ResourceType::CoordinationRole` would be the clean model, and it would give per-role ACLs with no name collision.

It loses on the JVM tools. `kafka-acls` maps resource type values it knows, and it renders an unknown value as an unknown type. An operator who lists ACLs on a krabka cluster would see rows that they cannot read and cannot edit. The `krabka-role.` prefix on the `Group` type keeps every ACL readable by the tool an operator already has.

### An `AcquireLeadership` Call That Blocks Until Granted

A long poll would cut acquisition latency to near zero, because a member would learn of a grant the moment the controller wrote it.

It loses twice. It holds one open request for each waiting contender on the active controller, which is the node this design works hardest to keep cheap. It also adds a second path on which a grant can arrive, and a client with two such paths can take a role twice. The heartbeat interval bounds acquisition latency instead, and a role that needs faster failover lowers `heartbeat.interval.ms`.
