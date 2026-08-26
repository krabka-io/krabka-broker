# Witness Broker Role and Stretch Cluster Design

A broker role that replicates and votes, but never serves clients and never leads a partition, and the three-site profile that uses it.

## Design Goals

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

## Architecture Overview

Three parts make up the feature.

**The role.** `NodeRole::Witness` joins `NodeRole::Broker` and `NodeRole::Controller` in `process.roles`. The witness role is a modifier and not a replacement. A witness node also carries `Broker`, because it holds partition replicas. A witness node also carries `Controller`, because it votes. `BrokerConfig::validate` rejects a witness that lacks either one.

**The site.** A site is an existing `broker.rack` value. Krabka already sends the rack in the `BrokerRegistrationRecord` and already returns it to clients in `MetadataResponseBroker.rack`. The rack-aware replica selector for KIP-392 already reads it. A stretch site needs no new placement dimension and no new wire field.

**The profile.** The `[stretch]` configuration section names the three sites, the witness site, and the preferred leader site. `BrokerConfig::validate` checks that the topology is coherent before the broker starts. The controller leader publishes the preferred leader site as a cluster-default broker configuration, so every node that later becomes controller reads the same value.

The controller learns which nodes are witnesses from a per-broker configuration record. Each broker publishes `broker.witness` for itself in the same metadata batch that carries its registration record. The controller reads the value from the metadata image when it elects a leader.

## Key Design Decisions

### The witness stays visible to clients

The witness appears in `Metadata.brokers[]` with its rack, exactly like any other broker. `crates/broker/src/handlers/metadata.rs` needs no change.

The other option was to hide the witness from client metadata. That option loses. Partition metadata names the witness in `replicas[]` and in `isr[]`, and a client that cannot resolve that node id sees a broken cluster. The JVM tools `kafka-topics`, `kafka-reassign-partitions`, and `kafka-leader-election` all render replica lists. They stay correct only if the witness resolves.

The witness rejects a client `Produce` or a client `Fetch` with `NOT_LEADER_OR_FOLLOWER`. That is the Kafka error code that makes a client refresh its metadata and go to another node. A follower `Fetch`, which carries a replica id of zero or more, still succeeds, because that is how the witness replicates.

### Leadership pinning is replica ordering, not a new election rule

In Kafka the preferred leader is `replicas[0]`. Site-aware placement puts a broker from the preferred site first in the replica list. That single ordering choice is the whole pinning mechanism.

Two existing subsystems then keep leadership in place with no new code. The KIP-460 automatic rebalance in `crates/broker/src/leader_rebalance.rs` moves leadership back to `replicas[0]` when the imbalance crosses its threshold. The operator command `kafka-leader-election --election-type preferred` does the same on demand.

The rejected option was a controller-side site preference applied at every election. It gives a stronger guarantee under churn. It also breaks the Kafka contract that the preferred replica is `replicas[0]`, which is the contract the JVM tools report against. An operator who reads `kafka-topics --describe` must see a replica list that predicts the leader.

### A witness is never elected leader, in any election path

Krabka excludes a witness from all three leader-selection points in `crates/broker/src/leader_election.rs`: the failover scan, the preferred election, and the controlled-shutdown handoff. The exclusion has no exception. An unclean election to a witness would accept data loss. It would still give a node that serves no client. The unclean path excludes the witness too.

One case needs care. If every alive in-sync replica set member is a witness, krabka marks the partition unavailable. It does not start an unclean recovery. A witness that is still alive holds every committed record, and an unclean election to a stale data replica would discard those records. Unavailable is the safer answer, and an operator can still force an unclean election.

The consequence is explicit. The witness keeps the **quorum** alive through the loss of both data sites. It does not keep the **data plane** alive. Partitions go offline until a data site returns. The profile promises the loss of one site, and this case is the loss of two.

### The witness flag rides an existing metadata record

`BrokerRegistrationRecord` lives in the sibling repository `krabka-protocol`. A new field there means a change in two repositories and a revision bump.

Krabka publishes `broker.witness` as a per-broker configuration record instead. The record type `V1BrokerConfig` and its submit path already exist. Each broker adds the record to the same batch that carries its own registration, so the two values commit together. A node that no longer has the witness role clears the value, so a stale flag cannot survive a role change.

The key is controller-managed and read-only. `AlterConfigs` and `IncrementalAlterConfigs` reject it with `INVALID_CONFIG`. `DescribeConfigs` returns it with `read_only` set to true. An operator reads it with `kafka-configs --entity-type brokers --describe`, which is the same command Kafka uses for every other read-only broker configuration.

### The profile validates and does not mutate

`BrokerConfig::validate` rejects an incoherent topology at startup. It checks the site count, the site names, the role of the node against its rack, and the durability arithmetic. It never rewrites an operator value to make the profile fit. A silent change to `min.insync.replicas` would defeat the guarantee the operator asked for.

The durability check calls `crabka_verified::stretch::min_insync_is_site_loss_safe`. That function is formally verified. The broker calls it and does not restate the arithmetic.

## Integration

**KRaft quorum.** A witness is a voter, so it appears in `controller.quorum.voters` and in the `VotersRecord`. `QuorumStateMachine` derives voter status from voter-set membership alone, so the witness needs no consensus-layer change. See `crates/kraft-core/src/core.rs`.

**Replication.** A witness runs the standard replica fetcher in `crates/broker/src/replicator.rs`, and the leader tracks it in `ReplicaState` like any other follower. It enters and leaves the in-sync replica set through the normal `isr_maintenance` scan. It counts toward `min.insync.replicas` in `validate_partition_gate`, which is the mechanism the whole feature depends on.

**Placement.** `crates/broker/src/site_placement.rs` replaces the round-robin helper when the brokers report racks. It falls back to `round_robin_replicas` when no broker reports a rack, which keeps a non-stretch cluster on the Kafka behaviour. A manual assignment always wins, as it does in Kafka.

**Reads.** The KIP-392 replica selector skips a witness, so a rack-aware consumer in the witness site reads from the leader instead. A witness serves no client read.

## Kafka and KIP Compliance

No wire format changes. No new API key, no new request or response version, and no new record field.

- KIP-392 supplies `broker.rack` and the rack-aware replica selector. The site model reuses both.
- KIP-460 supplies the preferred election that keeps leadership pinned.
- KIP-841 supplies the unclean leader election toggle. The witness exclusion applies inside it.
- KIP-966 supplies the unclean recovery strategies. A partition with only witnesses alive skips recovery and reports unavailable.
- KIP-919 supplies `DescribeCluster` with fencing state. The witness is not fenced, and that surface is unchanged.

Krabka diverges from Kafka in one place, and the divergence is the feature. Kafka has no witness role, so Kafka elects any in-sync replica as leader. Krabka excludes a witness. An operator who reads `kafka-topics --describe` sees the witness in `Replicas` and in `Isr`, and never in `Leader`.

## Testing

Three layers cover the feature.

**Formal proof.** `crates/verified/src/stretch.rs` carries Creusot contracts for the arithmetic that the durability claim rests on. `quorum_survives_any_single_site_loss` states the central property: the voters that remain after the loss of any one site still form a strict majority. A two-site cluster fails that check for every split, which is the formal reason the third site needs a vote. Proof sessions live under `verif/crabka_verified_rlib/stretch/`.

**Model check.** `crates/broker/src/stretch_cluster_model.rs` runs an exhaustive stateright search over site failures, site partitions, elections, and writes. It drives the real `failover_one` and the real placement function, and not a copy. A deliberately broken election function proves that the no-witness-leader property fires.

**Integration.** `crates/broker/tests/witness_role.rs` covers the role on a live cluster. `crates/broker/tests/stretch_cluster.rs` covers site loss and minority partitions on a three-site cluster. The minority cases use a test-only TCP relay. A stopped site and a partitioned site are different failures, and only the second one leaves a live minority.
