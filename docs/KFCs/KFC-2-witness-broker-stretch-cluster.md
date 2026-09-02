# KFC-2: Witness Broker Role and Stretch Cluster

A broker role that replicates and votes, but never serves clients and never leads a partition, and the three-site profile that uses it.

## Status

**Adopted.** The implementation merged to `main` through pull request #8 and ships in release `v0.5.0` and every later release.

No KIP defines a witness role or a stretch profile, so this document is the specification for both. The [Compatibility](#compatibility-deprecation-and-migration-plan) section names the one place where krabka answers an election differently from Kafka. The [Rejected Alternatives](#rejected-alternatives) section states why the two shapes Kafka already offers for a third site are not enough.

## Motivation

A stretch cluster spans more than one site. The common shape is two sites that hold the data and a third site that is small and cheap. The third site exists to break a tie. Krabka must keep synchronous commit alive through the loss of any one of the three sites.

Kafka gives two tools for this shape, and neither one is enough on its own.

A KRaft observer holds no vote. It replicates the metadata log through fetch, and it never joins the voter set. See `crates/broker/src/metadata_observer.rs`. An observer in the third site does not help a split between the two data sites, because it cannot vote.

A KRaft voter that is not a broker holds a vote but no partition data. A controller-only node never sends a `BrokerRegistrationRecord`, so it never holds a replica. See the role gate in `crates/broker/src/broker.rs`. Such a node keeps the metadata quorum alive, but it adds nothing to the in-sync replica set. With `replication.factor=3` across two sites and `min.insync.replicas=2`, the loss of one data site drops the in-sync replica set to one member. Krabka then rejects every `acks=all` write with `NOT_ENOUGH_REPLICAS`.

The gap is a node that does both. A **data-bearing witness** is an in-sync replica set member and a KRaft voter at the same time. One replica for each of three sites, with `min.insync.replicas=2`, then survives the loss of any single site with synchronous commit intact. The witness carries a full copy of the data, so it satisfies `min.insync.replicas`. The witness never serves a client and never leads a partition, so it needs no client bandwidth and no leader-side CPU. It runs on small hardware.

The design has four goals.

1. Keep the quorum alive through the loss of any one site.
2. Keep `acks=all` writes accepted through the loss of any one site.
3. Keep partition leadership in one preferred site, to save an inter-site round trip on the write path.
4. Change no Kafka wire format, and keep the JVM admin tools correct.

## Public Interfaces

Every surface below already exists in Kafka or is a krabka configuration. There is no new wire element.

**Broker configuration.**

- `process.roles` accepts a `witness` value next to `broker` and `controller`. A witness node carries all three, and the broker refuses to start with `witness` alone.
- The `[stretch]` section of the broker configuration names `sites`, `witness_site`, and `preferred_leader_site`. The broker validates the section at startup and refuses an incoherent topology. See `crates/broker/src/config/stretch.rs`.
- `broker.rack` names the site a node belongs to. It is the KIP-392 rack, unchanged.
- `min.insync.replicas` must be 2 on a three-site profile. The broker rejects any other value at startup.

**Controller-managed broker configuration.** Two read-only keys appear in `DescribeConfigs` for the broker entity type and reject `AlterConfigs` and `IncrementalAlterConfigs` with `INVALID_CONFIG`.

- `broker.witness`, per broker, set by the node for itself.
- `stretch.preferred.leader.site`, a cluster default, published by the controller leader.

**Client-visible behaviour.**

- `Metadata.brokers[]` lists the witness with its rack, and partition metadata lists it in `replicas[]` and `isr[]`. It never appears as `leader`.
- A client `Produce` or `Fetch` sent to a witness returns `NOT_LEADER_OR_FOLLOWER`.
- An `acks=all` write with the in-sync replica set under `min.insync.replicas` returns `NOT_ENOUGH_REPLICAS`, as in Kafka.

**Metrics.** `witness_role` is 1 on a node whose `broker.witness` value reached the controller. `leader_site_drift_partitions` counts the partitions a broker leads from a site other than the preferred leader site.

**Admin tools.** `kafka-topics --describe`, `kafka-configs --entity-type brokers --describe`, `kafka-reassign-partitions`, and `kafka-leader-election --election-type preferred` work unchanged.

## Proposed Changes

### Architecture Overview

Three parts make up the feature.

**The role.** `NodeRole::Witness` joins `NodeRole::Broker` and `NodeRole::Controller` in `process.roles`. The witness role is a modifier and not a replacement. A witness node also carries `Broker`, because it holds partition replicas. A witness node also carries `Controller`, because it votes. `BrokerConfig::validate` rejects a witness that lacks either one.

**The site.** A site is an existing `broker.rack` value. Krabka already sends the rack in the `BrokerRegistrationRecord` and already returns it to clients in `MetadataResponseBroker.rack`. The rack-aware replica selector for KIP-392 already reads it. A stretch site needs no new placement dimension and no new wire field.

**The profile.** The `[stretch]` configuration section names the three sites, the witness site, and the preferred leader site. `BrokerConfig::validate` checks that the topology is coherent before the broker starts. The controller leader publishes the preferred leader site as a cluster-default broker configuration, so every node that later becomes controller reads the same value.

The controller learns which nodes are witnesses from a per-broker configuration record. Each broker publishes `broker.witness` for itself in the same metadata batch that carries its registration record. The controller reads the value from the metadata image when it elects a leader.

### Key Design Decisions

#### The witness stays visible to clients

The witness appears in `Metadata.brokers[]` with its rack, exactly like any other broker. `crates/broker/src/handlers/metadata.rs` needs no change.

The other option was to hide the witness from client metadata. That option loses. Partition metadata names the witness in `replicas[]` and in `isr[]`, and a client that cannot resolve that node id sees a broken cluster. The JVM tools `kafka-topics`, `kafka-reassign-partitions`, and `kafka-leader-election` all render replica lists. They stay correct only if the witness resolves.

The witness rejects a client `Produce` or a client `Fetch` with `NOT_LEADER_OR_FOLLOWER`. That is the Kafka error code that makes a client refresh its metadata and go to another node. A follower `Fetch`, which carries a replica id of zero or more, still succeeds, because that is how the witness replicates.

#### Leadership pinning is replica ordering, not a new election rule

In Kafka the preferred leader is `replicas[0]`. Site-aware placement puts a broker from the preferred site first in the replica list. That single ordering choice is the whole pinning mechanism.

Two existing subsystems then keep leadership in place with no new code. The KIP-460 automatic rebalance in `crates/broker/src/leader_rebalance.rs` moves leadership back to `replicas[0]` when the imbalance crosses its threshold. The operator command `kafka-leader-election --election-type preferred` does the same on demand.

The rejected option was a controller-side site preference applied at every election. It gives a stronger guarantee under churn. It also breaks the Kafka contract that the preferred replica is `replicas[0]`, which is the contract the JVM tools report against. An operator who reads `kafka-topics --describe` must see a replica list that predicts the leader.

#### A witness is never elected leader, in any election path

Krabka excludes a witness from all three leader-selection points in `crates/broker/src/leader_election.rs`: the failover scan, the preferred election, and the controlled-shutdown handoff. The exclusion has no exception. An unclean election to a witness would accept data loss. It would still give a node that serves no client. The unclean path excludes the witness too.

One case needs care. If every alive in-sync replica set member is a witness, krabka marks the partition unavailable. It does not start an unclean recovery. A witness that is still alive holds every committed record, and an unclean election to a stale data replica would discard those records. Unavailable is the safer answer, and an operator can still force an unclean election.

The consequence is explicit. The witness keeps the **quorum** alive through the loss of both data sites. It does not keep the **data plane** alive. Partitions go offline until a data site returns. The profile promises the loss of one site, and this case is the loss of two.

#### The witness flag rides an existing metadata record

`BrokerRegistrationRecord` lives in the sibling repository `krabka-protocol`. A new field there means a change in two repositories and a revision bump.

Krabka publishes `broker.witness` as a per-broker configuration record instead. The record type `V1BrokerConfig` and its submit path already exist. Each broker adds the record to the same batch that carries its own registration, so the two values commit together. A node that no longer has the witness role clears the value, so a stale flag cannot survive a role change.

The key is controller-managed and read-only. `AlterConfigs` and `IncrementalAlterConfigs` reject it with `INVALID_CONFIG`. `DescribeConfigs` returns it with `read_only` set to true. An operator reads it with `kafka-configs --entity-type brokers --describe`, which is the same command Kafka uses for every other read-only broker configuration.

#### The profile validates and does not mutate

`BrokerConfig::validate` rejects an incoherent topology at startup. It checks the site count, the site names, the role of the node against its rack, and the durability arithmetic. It never rewrites an operator value to make the profile fit. A silent change to `min.insync.replicas` would defeat the guarantee the operator asked for.

The durability check calls `krabka_verified::stretch::min_insync_is_site_loss_safe`. That function is formally verified. The broker calls it and does not restate the arithmetic.

One consequence reaches every operator who adopts the profile. The safe range has a lower bound and an upper bound, and both are stated against the replicas that one site holds. The lower bound is that `min.insync.replicas` must be more than the replicas any single site holds. The upper bound is `site_loss_survivors(rf, sites)`, which is what a site loss leaves. Three replicas over three sites put one replica in each site, so the bounds become `> 1` and `<= 2`. They meet, and 2 is the only safe value. The broker default is 1, and the profile rejects it at startup. An operator who sets `[stretch]` must also set `min.insync.replicas` to 2.

The lower bound is a durability rule and not an availability rule, and it is the reason the profile takes exactly one replica for each site. `min.insync.replicas` counts replicas, and it does not place them. If one site can hold that many in-sync replicas, then the in-sync replica set can shrink into that one site, and the leader acknowledges a write that only that site holds. Reaching such an in-sync replica set needs no second site loss: one site down and one lagging replica is enough, and the metadata quorum lives in the other two sites, so the shrink commits normally. A witness on a cheap link is the replica most likely to lag. The loss of the site that holds the write then loses an acknowledged write, and the two surviving sites still hold the voter majority that elects a leader which never saw it.

Four replicas over three sites is the configuration this rule rejects. The placement puts two replicas in one site, so `min.insync.replicas=2` is satisfiable inside that site alone. Raising the value to 3 repairs the placement and breaks availability instead, because the loss of the doubled site leaves 2. No value lies between them, so krabka refuses the topology at startup rather than accept one that reads as safe and is not.

The rejection is deliberate. `acks=all` at `min.insync.replicas=1` acknowledges a write that one broker holds, so a single site loss can lose it. That gives no cross-site durability, which is the one property the profile exists to supply.

### Integration

**KRaft quorum.** A witness is a voter, so it appears in `controller.quorum.voters` and in the `VotersRecord`. `QuorumStateMachine` derives voter status from voter-set membership alone, so the witness needs no consensus-layer change. See `crates/kraft-core/src/core.rs`.

**Replication.** A witness runs the standard replica fetcher in `crates/broker/src/replicator.rs`, and the leader tracks it in `ReplicaState` like any other follower. It enters and leaves the in-sync replica set through the normal `isr_maintenance` scan. It counts toward `min.insync.replicas` in `validate_partition_gate`, which is the mechanism the whole feature depends on.

**Placement.** `crates/broker/src/site_placement.rs` replaces the round-robin helper when the brokers report racks. It falls back to `round_robin_replicas` when no broker reports a rack, which keeps a non-stretch cluster on the Kafka behaviour. A manual assignment always wins, as it does in Kafka.

**Reads.** The KIP-392 replica selector skips a witness, so a rack-aware consumer in the witness site reads from the leader instead. A witness serves no client read.

## Compatibility, Deprecation, and Migration Plan

No wire format changes. No new API key, no new request or response version, and no new record field. A stock Kafka client and every JVM admin tool work unchanged against a stretch cluster.

krabka is greenfield and undeployed, so there is no migration. A cluster with no `[stretch]` section and no witness node keeps the Kafka behaviour in every path this document touches: placement stays round-robin, and every in-sync replica is eligible to lead. There is nothing to deprecate.

- KIP-392 supplies `broker.rack` and the rack-aware replica selector. The site model reuses both.
- KIP-460 supplies the preferred election that keeps leadership pinned.
- KIP-841 supplies the unclean leader election toggle. The witness exclusion applies inside it.
- KIP-966 supplies the unclean recovery strategies. A partition with only witnesses alive skips recovery and reports unavailable.
- KIP-919 supplies `DescribeCluster` with fencing state. The witness is not fenced, and that surface is unchanged.

Krabka diverges from Kafka in one place, and the divergence is the feature. Kafka has no witness role, so Kafka elects any in-sync replica as leader. Krabka excludes a witness. An operator who reads `kafka-topics --describe` sees the witness in `Replicas` and in `Isr`, and never in `Leader`.

## Test Plan

Three layers cover the feature.

**Formal proof.** `crates/verified/src/stretch.rs` carries Creusot contracts for the arithmetic that the durability claim rests on. `quorum_survives_any_single_site_loss` states the central property: the voters that remain after the loss of any one site still form a strict majority. A two-site cluster fails that check for every split, which is the formal reason the third site needs a vote. Proof sessions live under `verif/krabka_verified_rlib/stretch/`.

**Model check.** `crates/broker/src/stretch_cluster_model.rs` runs an exhaustive stateright search over site failures, site partitions, elections, and writes. It drives the real `failover_one` and the real placement function, and not a copy. A deliberately broken election function proves that the no-witness-leader property fires.

**Integration.** `crates/broker/tests/witness_role.rs` covers the role on a live cluster. `crates/broker/tests/stretch_cluster.rs` covers site loss and minority partitions on a three-site cluster. The minority cases use a test-only TCP relay. A stopped site and a partitioned site are different failures, and only the second one leaves a live minority.

## Rejected Alternatives

Each alternative below is one that the stateright model in `crates/broker/src/stretch_cluster_model.rs` can state, and most of them are a configuration the model or the startup check refuses. The model's two `#[should_panic]` tests in `stretch_cluster_model/red_witness.rs` are the record of what two of them cost.

### A KRaft observer in the third site

Kafka's own shape for a cheap third site is an observer. It replicates the metadata log through fetch and holds no vote.

It loses on the metadata quorum. A split between the two data sites leaves each side with one voter of two, and no side has a majority. `quorum_survives_any_single_site_loss` in `crates/verified/src/stretch.rs` fails for every two-site split, which is the formal form of the same statement. A third site that cannot vote cannot break a tie.

### A controller-only voter in the third site

The second Kafka shape gives the third site a vote and no data. It keeps the metadata quorum alive through the loss of a data site.

It loses on the data plane. With the data in two sites and `min.insync.replicas=2`, the loss of one data site drops the in-sync replica set to one member, and every `acks=all` write fails with `NOT_ENOUGH_REPLICAS`. The quorum survives and the cluster still cannot commit. The witness exists to close that gap by holding data as well as a vote.

### A witness that any election path can elect

The obvious simplification is to treat the witness as an ordinary in-sync replica and let the controller elect it. The pinning to `replicas[0]` would keep leadership in a data site most of the time.

The model states what "most of the time" costs. `legacy_elect` in `stretch_cluster_model/red_witness.rs` is the pre-witness controller: it takes the first alive in-sync member with no witness filter. When both data sites are down, the witness is the only alive in-sync member, and that election hands leadership to a node that serves no client. `leader_never_witness` fires, and the test `red_witness_unaware_election_elects_a_witness` records it. The real `failover_one` answers `Unavailable` in the same state, so the exclusion is a rule and not a preference.

### Unclean recovery when only witnesses are alive

When every alive in-sync member is a witness, the controller could start an unclean election to a stale data replica and keep the partition writable.

It loses on the data. A witness that is still alive holds every committed record. An unclean election to a stale data replica discards the records the witness holds, and it does so to serve clients from a site that has already lost the leader once. Unavailable is the safer answer, and an operator can still force an unclean election by hand. The profile promises the loss of one site, and this state is the loss of two.

### `min.insync.replicas=1` on the three-site profile

The broker default is 1, and leaving it alone would make the profile a pure placement change with no durability rule to explain.

The model rejects it directly. With `min.insync.replicas=1`, a lone surviving replica commits an `acks=all` write while its site holds one voter of three. `minority_never_commits` fires, and the test `red_min_insync_one_commits_in_a_minority` records it. The loss of that site then loses an acknowledged write while the two other sites still hold the majority that elects a leader which never saw it. That is the failure the profile exists to prevent, so `BrokerConfig::validate` refuses the value at startup instead of accepting a topology that reads as safe and is not.

### More replicas than sites

A replication factor of four over three sites looks like more durability, because one more copy exists.

The placement puts two replicas in one site, so `min.insync.replicas=2` is satisfiable inside that site alone. The in-sync replica set can shrink to those two while a real KRaft quorum holds: one site down plus one lagging replica is enough, and the quorum lives in the other two sites. The leader then acknowledges a write that one site holds. Raising the value to 3 repairs the placement and breaks availability instead, because the loss of the doubled site leaves 2. No value satisfies both bounds, and `min_insync_is_site_loss_safe` says so for every replication factor above the site count. The model stays at one replica per site because that is the only shape the startup check admits.

### A hidden witness

The witness could be left out of `Metadata.brokers[]` so that a client never sees a node it cannot use.

Partition metadata names the witness in `replicas[]` and `isr[]`, and a client that cannot resolve that node id sees a broken cluster. `kafka-topics`, `kafka-reassign-partitions`, and `kafka-leader-election` render replica lists and stay correct only if every id resolves. The witness stays visible and answers a client `Produce` or `Fetch` with `NOT_LEADER_OR_FOLLOWER`, which is the Kafka error that sends the client elsewhere.

### A controller-side site preference at every election

A rule that applies the preferred site inside every election gives a stronger pinning guarantee under churn than replica ordering does.

It breaks the Kafka contract that the preferred replica is `replicas[0]`. The JVM tools report against that contract, and an operator who reads `kafka-topics --describe` must see a replica list that predicts the leader. Site-aware ordering plus the KIP-460 rebalance gives the same steady state and keeps the contract.

### A witness field on `BrokerRegistrationRecord`

The natural home for a witness flag is the registration record itself.

The record lives in the sibling repository `krabka-protocol`, so a new field is a change in two repositories and a revision bump for a single boolean. The per-broker configuration record `broker.witness` already exists as a type and a submit path, commits in the same batch as the registration, and is readable through `kafka-configs`. It gives the same guarantee with no protocol change.

### A profile that repairs `min.insync.replicas` on the operator's behalf

The startup check could rewrite an unsafe `min.insync.replicas` to 2 instead of refusing to start.

A silent change to a durability setting defeats the guarantee the operator asked for, and it hides the arithmetic that the operator needs to understand before the profile is safe to run. The check refuses and names the bounds, and the operator sets the value.
