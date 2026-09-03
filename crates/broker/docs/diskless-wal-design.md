# Diskless WAL Design

The diskless data path makes a partition durable through a quorum of broker-local write-ahead logs and an object store, instead of through the partition's ISR and its local segments.

This document follows the [design document style guide](../../../docs/style_guides/design_doc_style_guide.md). It records the durability model, the placement rules, the fencing and authorization rules, the reclaim and trim rules, and the slices the [`diskless_crash_model`](../src/diskless_crash_model.rs) composes. Every claim names the module that enforces it.

## Design Goals

A diskless partition must give a producer the same durability promise as an `acks=all` write on a replicated Kafka partition, and it must do so with a bounded amount of local disk. The goals that shaped the implementation are:

- **Durability before acknowledgement.** A record counts as committed only when a strict majority of the WAL voters has fsynced it. The `acks=-1` high-watermark gate in [`handlers/produce/append.rs`](../src/handlers/produce/append.rs) waits for that watermark, so a client that asked for durability never receives an offset that the quorum can lose.
- **Bounded local storage.** The local log is a trimmed projection of what the object store holds. The flusher in [`diskless/flusher`](../src/diskless/flusher.rs) copies committed tails into objects and then advances the local log start behind the committed object frontier.
- **KRaft as the offset authority.** Offsets are reserved through the metadata controller, in [`wal/offset_sequencer.rs`](../src/wal/offset_sequencer.rs), so a crash between the reservation and the fsync cannot reuse or skip an offset.
- **Rack-loss durability.** Voters are placed on distinct racks by [`wal/quorum/placement.rs`](../src/wal/quorum/placement.rs). An incomplete placement fails closed rather than weakening the guarantee.
- **Kafka byte exactness at every boundary a client sees.** The WAL replicates verbatim v2 record batches, the object format embeds those batches unchanged, and the WAL replication traffic rides the KIP-595 `Fetch` envelope. Only the object framing and the index records are Krabka-private.

Why not reuse the ISR for durability? An ISR is a leader-driven set whose membership the controller changes on lag, and a follower joins it only after it has caught up to the leader's end offset. A WAL quorum is a fixed, metadata-selected voter set whose majority rule needs no membership churn to stay safe. [`replica_state.rs`](../src/replica_state.rs) records the consequence: for a diskless partition the WAL quorum replaces the partition ISR as the durability authority, and the two are never combined.

## Architecture Overview

A diskless partition keeps its ordinary `Partition` runtime and `krabka-log` directory. Three pieces sit beside it:

1. **The quorum WAL.** [`wal/mod.rs`](../src/wal/mod.rs) defines the `WalStore` seam with three operations: `sync_durable` makes a log prefix quorum-durable, `trim_to_offset` discards a durable prefix the object index already covers, and `invalidate_hot_tail` drops cached bytes after a log rewrite. The production implementation is `QuorumWalStore` in [`wal/quorum/mod.rs`](../src/wal/quorum/mod.rs), which wraps a `WalShardEngine` from [`wal/quorum/engine.rs`](../src/wal/quorum/engine.rs).
2. **The object flusher and its index.** The flusher uploads committed tails as combined objects, records each object's byte ranges on the compacted `__diskless_wal_index` topic, and trims the local logs that the committed index now covers. [`diskless/wal_object.rs`](../src/diskless/wal_object.rs) frames the object, [`diskless/wal_index.rs`](../src/diskless/wal_index.rs) holds the index records and their in-memory projection, and [`diskless/index_log.rs`](../src/diskless/index_log.rs) drives the projection off the topic.
3. **The read side.** [`diskless/hot_tail.rs`](../src/diskless/hot_tail.rs) caches recently committed batches in memory. [`diskless/read.rs`](../src/diskless/read.rs) serves a fetch below the local log start out of the object store.

The control flow for one produce is:

1. The partition writer, in [`partition_writer/produce.rs`](../src/partition_writer/produce.rs), asks the `OffsetSequencer` for a base offset. The sequencer submits a `PartitionOffsetAdvance` record to the controller and receives a committed reservation.
2. The writer appends the batches at that base offset with `run_produce_append_batch_at`, then answers each producer job with its offset.
3. The writer calls `WalStore::sync_durable(leo)`. The engine fsyncs the local log, records its own durable offset, and waits for the quorum watermark to reach `leo`. That watermark advances when remote voters fetch and fsync the range.
4. The durable watermark feeds `ReplicaState::recompute_hw_for_wal_durable`, which advances the partition high watermark. A producer that asked for `acks=-1` is released here.

Remote voters run a pull-based follower task, in [`wal/quorum/follower.rs`](../src/wal/quorum/follower.rs), that fetches the leader's uncommitted tail, fsyncs it, records the durable range in a checkpoint file, and reports the fsynced frontier back through its next fetch offset.

## Slices

The implementation landed in six slices. The crash model in [`diskless_crash_model.rs`](../src/diskless_crash_model.rs) names Slice 5 and Slice 6, and the `Slice 1` comment on the diskless flag in [`partition.rs`](../src/partition.rs) names the first one. The numbering below is the reference those comments resolve to.

### Slice 1: The diskless topic flag and partition runtime

The per-topic key `krabka.diskless` in [`config_keys/diskless.rs`](../src/config_keys/diskless.rs) turns the path on. The comparison is exact: only the value `true` counts. A partition reads the flag once, when it is opened, and builds either the WAL runtime or the local-log runtime from it. So `validate_diskless_unchanged` refuses every alter that would flip the resolved flag, and the error text tells the operator to create a new topic instead.

Two combinations are refused at topic creation and alteration by `validate_diskless_combination`. `remote.storage.enable=true` would add a second uploader over the same local log whose local retention could delete segments the diskless trim frontier still accounts for. `delivery.mode=scheduled` (KFC-1) would let the flusher copy a batch that is not yet due into an object, and the cold-read path serves object runs with no delivery gate.

[`broker/partition_spawn.rs`](../src/broker/partition_spawn.rs) builds the runtime. For a diskless partition it constructs `QuorumWalStore::for_distributed_partition`, registers the engine in the `WalShardRegistry`, seeds the partition high watermark from the engine's recovered durable watermark, and spawns a task that forwards every durable-watermark advance into `ReplicaState`.

### Slice 2: Controller-assigned offsets

`ControllerSequencer` in [`wal/offset_sequencer.rs`](../src/wal/offset_sequencer.rs) submits one `V1PartitionOffsetAdvance { topic, partition, count }` record per produce group. The controller's submit path, in [`crates/raft/src/kraft/controller/submit.rs`](../../raft/src/kraft/controller/submit.rs), refuses the record until the leader's own epoch marker is committed (`wal_reservation_epoch_ready`), folds every uncommitted reservation for the same partition into the next offset (`wal_reservation_frontier`), reserves the range with `reserve_offsets`, and releases the `OffsetReservation` only after the batch commits and applies. The response carries the controller's leader epoch.

The broker binds the single response row to the exact topic, partition, and count it asked for, and checks the epoch it observed before and after the call with `wal_reservation_response`. A broker-only observer cannot read a controller epoch, so it accepts any nonnegative response epoch and relies on the private one-shot RPC to correlate the committed answer. Every malformed, stale, or failed reservation fails the produce group closed.

This is what the crash model calls a stateless appender: the reservation is a controller decision, not a local counter, so the sequence of reservations stays gap-free and unique across restarts and across leader changes.

### Slice 3: The object flusher and the index log

The flusher runs on every broker, in [`diskless/flusher.rs`](../src/diskless/flusher.rs), and acts only on the diskless partitions the broker currently leads. It waits for the index projection to replay the index topic before its first tick. A tick that ran earlier would read a stale frontier, upload a prefix the store already holds under a fresh key, and leave the earlier object unreferenced forever.

One tick, in [`diskless/flusher/object_flush.rs`](../src/diskless/flusher/object_flush.rs), builds one combined object from the committed tails: for each led partition, the raw bytes from the flushed frontier (or the local log start) up to the high watermark, subject to the per-object size budget. The object goes to `diskless-wal/<broker_id>/<uuid>.ckwl`. The tick then publishes one keyed `WalFlushRecord` per batch to `__diskless_wal_index`, waits until its own records come back through the projection, and only then trims each partition's local log. A failed tick moves no frontier, so the next tick retries the same tail.

The object framing in [`diskless/wal_object.rs`](../src/diskless/wal_object.rs) is `[CKWL magic + version] + runs + manifest + [footer length + magic]`, little-endian and Krabka-private. The runs are verbatim v2 batches, so a cold read can hand the bytes to a client unchanged.

The index key is `(topic_id, partition, first_offset)`, so Kafka compaction keeps the newest record per logical range and a deleted topic's ranges can be tombstoned. `WalIndexCache` in [`diskless/wal_index.rs`](../src/diskless/wal_index.rs) applies keyed values, legacy unkeyed values, and tombstones through the proved `diskless_wal_replay_decision` kernel, so a keyed value stays authoritative whatever order the partitions of the index topic deliver in.

#### Retention

Retention on a diskless topic is the index's job, because the index is the
only thing that says which objects a partition's records live in. On every
flush tick, before the flush itself, the flusher runs the three Kafka
predicates from `UnifiedLog.deleteOldSegments` over each led partition's
projected ranges -- unless a KFC-9 write freeze covers the topic, which stops
this pass as it stops the cleaner and the remote-log-manager's two retention
passes -- through the proved `diskless_retention_prefix` kernel: a
range whose `max_timestamp_ms` is older than `retention.ms`, an oldest range
the `retention.bytes` budget cannot keep, and a range that ends below the
partition's `DeleteRecords` floor. The kernel walks oldest first and stops at
the first range it must keep, as Kafka does, and it never expires the newest
range -- Kafka keeps the active segment for the same reason, and here that
range is also what keeps `flushed_frontier` pointing past the last flushed
offset. Each expired range gets one keyed tombstone on the index topic and
leaves the projection, and `krabka_broker_diskless_wal_expired_ranges_total`
counts them.

The object the ranges lived in is freed by the reclaimer, on a later sweep and
only once nothing references it. One object carries runs from several
partitions, so a partition whose ranges all expire can still leave that object
in the bucket until its co-tenants expire too. The bucket therefore trails
retention by up to one object's worth of co-tenancy plus the reclaim grace.

`DeleteRecords` needs no extra RPC, but it does need a record of its own. The
trim moves the WAL frontier and the local log start already, and the floor
predicate tombstones the index ranges on the next tick. The floor itself cannot
be read off the local log, because a diskless partition's log start is the
flusher's trim frontier rather than a delete point. For the same reason the
handler measures the request against the offset the partition actually starts
at -- the one `ListOffsets(EARLIEST)` answers -- rather than against a local
log start the flusher has usually already moved past everything an operator
would want to delete.

So the floor is published to the index topic as a keyed
`WalDeleteFloorRecord`, under a key whose encoding is a different length from a
range key so the two decoders on the replay path cannot read one another's. The
handler publishes it and waits for the projection to carry it *before* the trim
is acknowledged: a client told its records are gone must not see them again.
From that moment a cold read below the floor misses, the fetch stays
`OFFSET_OUT_OF_RANGE`, and `ListOffsets(EARLIEST)` answers the floor -- on
every broker, because every projection replays the same record.

The range tombstones could not stand in for that record, which is why it
exists. A range that straddles the floor still holds live records at and above
it, so retention must keep it; the newest range is never expired either. Both
come back out of a replay still covering the offsets below the floor, so a
projection rebuilt without the floor record would serve deleted offsets again
and go on doing it, not merely until the next tick. The floor only ever moves
forward, so neither a stale retry nor a compacted replay that delivers an older
record last can widen what a consumer sees, and a deleted topic's floors are
tombstoned alongside its ranges.

### Slice 4: Reads from the hot tail and the object store

A fetch on a diskless partition first consults the `HotTailCache` in [`diskless/hot_tail.rs`](../src/diskless/hot_tail.rs). `QuorumWalStore::sync_durable` inserts every batch that just became quorum-durable, so the cache only ever holds committed bytes. The lookup in [`handlers/fetch/read.rs`](../src/handlers/fetch/read.rs) runs only for `read_uncommitted` fetches, answers with whole batches, and honours the fetch window's limit offset, so it can never hand out a batch the log read path would have held back. Trim, truncate, and reset all invalidate the partition's cache entries through `WalStore::invalidate_hot_tail`.

A fetch whose offset is below the local log start would answer `OFFSET_OUT_OF_RANGE` on an ordinary partition. [`diskless/read.rs`](../src/diskless/read.rs) intercepts that case: it looks the offset up in the index projection, extends the byte span across contiguous ranges inside the same object up to the request's byte cap, issues one ranged object GET, and returns the first whole batch at or after the requested offset. The three selection steps are the proved kernels `diskless_logical_range`, `diskless_span_extension`, and `diskless_batch_step`. A miss falls back to the ordinary out-of-range answer. A storage error answers `KAFKA_STORAGE_ERROR`.

`ListOffsets` earliest includes the smallest offset the index still answers for -- the smallest indexed first offset, raised to the partition's `DeleteRecords` floor -- through the `list_offsets_earliest` kernel, so a consumer that seeks to the beginning lands on an object-backed offset rather than the trimmed local start, and never below a floor an operator set.

### Slice 5: Crash windows and recovery

The diskless path has three crash windows that an ordinary partition does not have, and one that it shares.

- **Between the offset reservation and the fsync.** The controller has committed `next_offset`, but the local log ends earlier. [`diskless/recovery.rs`](../src/diskless/recovery.rs) re-anchors the log's append-at position from the committed `partition_next_offset` in the metadata image when the partition is opened, so the next reservation lands where KRaft says it must. The crash model's `crash_in_kraft_fsync_gap` witness covers this window, and its `reservations_gap_free_and_unique` property states the invariant.
- **A torn active tail.** `open_config` forces `validate_on_open` for a diskless log whatever the broker's log configuration says, so the open CRC-checks the active segment and truncates a torn tail. The producer sequence map is then rebuilt from the surviving raw batches. The `crash_mid_fsync` witness and the `producer_dedup_no_regress` property cover this window.
- **Between the object PUT and the index commit.** An object that was uploaded but never indexed is unreferenced. The reclaimer in [`diskless/flusher.rs`](../src/diskless/flusher.rs) deletes it after a grace period, and the next flush re-uploads the same tail under a new key. The `crash_between_put_and_index` witness covers this window.
- **A shard reopen.** The test-only local harness in [`wal/quorum/engine/recovery.rs`](../src/wal/quorum/engine/recovery.rs) shows the rule every reopen follows: the durable prefix is the frontier a strict majority holds, the donor is a replica whose bytes over that prefix match a majority byte for byte, and every replica is truncated to that prefix and re-synced from the donor before the engine serves a request. In production the same rule is split between the leader's canonical log and the followers' checkpoints, below.

A follower keeps a `wal-durable-offset.checkpoint` file beside its log, written by [`wal/quorum/follower/checkpoint.rs`](../src/wal/quorum/follower/checkpoint.rs) after every fsynced mutation. On reopen the follower truncates its log to the checkpointed end and trims it to the checkpointed start, so it never advertises more than it has on disk. The write goes through a temporary file and a backup, so a crash inside the rename still leaves one readable checkpoint.

When a follower is promoted to leader, [`wal/quorum/follower/promotion.rs`](../src/wal/quorum/follower/promotion.rs) copies its checkpointed prefix into the canonical partition log after it has checked that the two agree byte for byte on the range they share. A disagreement is an error, not a repair. [`broker/storage.rs`](../src/broker/storage.rs) repeats the hydration at startup for every shard this broker leads, so a crash in the middle of an earlier hydration is a safe retry. During a live reconcile, [`replicator_supervisor/local_partitions.rs`](../src/replicator_supervisor/local_partitions.rs) runs the hydration under the partition's produce transition barrier, so no produce can observe a half-adopted prefix.

The shared window is the trim. `Trim` in the crash model may only advance to `min(index_frontier, wal_acked)`, and the `trim_at_committed_index_frontier` property states that the local log start never passes what the committed index covers.

### Slice 6: The diskless WAL quorum and stateless appenders

The production engine is constructed by `WalShardEngine::new_distributed` with the partition's canonical log as its only local replica and an odd, positive voter count from `diskless_wal_local_replica_count`. Everything else about the quorum comes from metadata:

- **Placement.** [`replicator_supervisor/desired_sets.rs`](../src/replicator_supervisor/desired_sets.rs) derives one `WalPlacement { voters, leader_epoch }` per diskless partition from each metadata image. The partition leader is the first voter. The remaining voters come from `select_voters`, which walks registered brokers in node-id order and takes the first unused broker on an unused rack. A broker without a rack is never a candidate, and a leader without a rack yields an empty placement. The `select_wal_voter_index` kernel proves the selection.
- **Installation.** `WalShardRegistry::replace_placements` in [`wal/quorum/registry.rs`](../src/wal/quorum/registry.rs) installs the placement map atomically on every image and pushes each shard's voter list into its engine through `configure_distributed`. An engine whose placement is missing, is not led by this broker, or has the wrong voter count clears its quorum. `replicate_and_sync` then fails with "placement is not available" until a complete placement arrives, which is the fail-closed half of the rack rule.
- **Followers.** [`replicator_supervisor/wal_followers.rs`](../src/replicator_supervisor/wal_followers.rs) starts one follower task per shard where the placement is complete, the leader is first, and this broker is a non-leader voter. It stops a task whose target changed, and it removes the shard directory when this broker is no longer a voter. [`wal/quorum/shard_dirs.rs`](../src/wal/quorum/shard_dirs.rs) lays the directories out as `<log_dir>/__diskless_wal_quorum/<topic>-<topic_id>-<partition>/voter-<node_id>/`, so a recreated topic never adopts its predecessor's replica logs, and `prune_orphaned_shard_dirs` removes follower-only roots the image no longer assigns.
- **The wire.** [`wal/quorum/wire.rs`](../src/wal/quorum/wire.rs) reuses the KIP-595 `Fetch` v17 body. The data topic id and partition address the shard, `replica_id` carries the follower's node id, and the fixed rack id `__krabka_diskless_wal` separates WAL traffic from a client fetch. The response's `last_stable_offset` field carries the leader's log end offset, because a follower needs the LEO as well as the committed watermark and the field is otherwise unused on this private path.
- **Acknowledgement.** The leader serves its uncommitted tail, not only the committed prefix, because a follower must fsync bytes before the watermark can advance. A follower's fetch offset is its acknowledgement: `record_follower_ack` in [`wal/quorum/engine/distributed.rs`](../src/wal/quorum/engine/distributed.rs) accepts it only from a remote voter in the current placement and only inside the leader's `[log_start, log_end]`. The watermark is `recompute_high_watermark(leader_end, follower_ends, strict_majority(voters), current, log_start, leader_counts = true)`, so the leader's own fsync counts as one vote and the result never regresses.
- **Fencing.** Every WAL fetch is classified by `wal_fetch_admission` in [`crates/verified/src/wal.rs`](../../verified/src/wal.rs). A request epoch below the placement's leader epoch answers `FENCED_LEADER_EPOCH`, one above it answers `UNKNOWN_LEADER_EPOCH`, and the follower task exits on either so the supervisor can restart it against the new placement. A request whose `last_fetched_epoch` diverges from the leader's epoch checkpoint receives the KIP-320 `diverging_epoch` hint, and [`wal/quorum/follower/log.rs`](../src/wal/quorum/follower/log.rs) truncates or resets before it fetches again.
- **Authorization.** A WAL fetch on the broker listener, in [`handlers/fetch.rs`](../src/handlers/fetch.rs), first needs `ClusterAction` on the cluster resource. The registry then maps the authenticated principal to a node id: an anonymous principal is refused, a name listed in `inter_broker_principal_node_ids` maps to its configured id on any listener, and the conventions `broker-<id>` and `CN=broker-<id>` apply only while that map is empty and only to connections on `inter_broker_listener_name`. Kafka takes replica identity from the request body alone, so the convention is an extra binding rather than a weaker one: without the listener rule, a principal that can create a SCRAM user could name itself `broker-3` on a client listener and read any diskless partition node 3 votes on. The admission kernel accepts the request only when the authenticated node equals the claimed `replica_id`, that this broker is the first voter, and that the claimed node is a voter. A denial answers `UNKNOWN_TOPIC_OR_PARTITION`, and the authorization check runs before the epoch classification, so an unauthorized caller learns nothing about the placement epoch. The controller listener routes the same request through `WalShardRouter`, which implements `krabka_raft::RaftShardRouter`.
- **Stateless appenders.** [`handlers/produce/leadership.rs`](../src/handlers/produce/leadership.rs) skips the metadata-leader check for a diskless partition and instead checks `diskless_role_ready`: the partition's installed leader and leader epoch must match the image. The offset comes from the controller, and the acknowledgement comes from the quorum, so the broker that appends holds no authority of its own. The crash model's `two_appenders_race_gap_free` witness and `sequencer_handoff_regresses_only_advertised_hwm` witness describe the two consequences: reservations from different appenders interleave without gaps, and a sequencer handoff may lower the advertised high watermark while the quorum-durable frontier never moves back.

## Key Design Decisions

### The quorum watermark, not the ISR, is the durability authority

`ReplicaState::recompute_hw_for_wal_durable` sets the high watermark to `max(hw, durable_leo)` and never consults the ISR. Combining the two quorums would pin the client-visible watermark to a lagging Kafka follower after the WAL had already committed the records, and it would let an ISR shrink change what "committed" means. The trade-off is that the ordinary ISR machinery still runs for a diskless partition but no longer gates acknowledgement; its role is leadership and metadata, not durability.

### The controller reserves offsets

A local counter would be simplest, but a crash after the local append and before the fsync could reuse an offset that a follower had already replicated, and a leader change could reserve a range twice. Routing every reservation through one committed metadata record makes the sequence a consensus decision. The cost is one controller round trip per produce group, which the writer amortizes by draining up to `max_produce_group` jobs into one reservation.

### Followers pull, and the fetch offset is the acknowledgement

A push protocol would need a new RPC and a separate acknowledgement message. Reusing KIP-595 `Fetch` gives the WAL the same framing, the same epoch fencing, and the same divergence hint that the metadata quorum already uses, and it lets the existing inter-broker client, TLS, and SASL carry the traffic. The rack-id discriminator keeps a client fetch from ever reaching the shard engine.

### The leader serves its uncommitted tail

Capping a WAL fetch at the high watermark would deadlock: no follower could fetch the bytes whose fsync advances the watermark. A client fetch is still capped at the partition high watermark by the ordinary read path, so the uncommitted tail is visible only to authenticated voters.

### Placement fails closed

An incomplete rack-distinct placement could be filled with a same-rack broker, which would keep the partition writable at the cost of the rack-loss guarantee. The placement code returns the short list instead, and the engine refuses to acknowledge until the placement is complete. An operator sees the failure as `diskless_wal_quorum_loss_events_total` and a warning that names the shard.

### Trim behind the committed index, with a safety lag

The local log start advances only to `min(committed_index_frontier, high_watermark - safety_lag)`, by the proved `diskless_trim_decision` kernel, and only after the flusher has seen its own index records return through the projection. The safety lag, default one offset, keeps the newest committed batch local so the hot tail and the local read path stay warm. Followers trim to the leader's log start on every fetch, so a trim propagates without a separate RPC.

### Retention expires index ranges, not objects

An object is shared by several partitions, so retention cannot delete one: the
unit retention can act on is the index range. Expiring a range unreferences its
slice of the object, and the object goes when its last range does. This costs a
delay between what retention says is gone and what the bucket still holds, and
buys a flusher that can keep packing many partitions into one object.

### Reclaim waits for a grace period and a projection check

Every broker sweeps the shared `diskless-wal/` prefix every 30 seconds and deletes an object that no projected range references, but only after the object has been unreferenced for five minutes and only after a second check under the projection lock. The grace period lets an independent index consumer apply a replacement before any broker deletes the old object. The `diskless_object_reclaimable` kernel states the rule.

### The flusher waits for the index replay before its first tick

The projection appends a keyed fence record to every non-empty index partition when it starts and reports itself caught up only when the pump has walked past every fence. A stalled replay, which a dead fetch loop on the index topic can cause, is not recoverable on the same subscription. So [`broker/diskless_index.rs`](../src/broker/diskless_index.rs) rebuilds the index log from a fresh connection and restarts the flusher, with backoff, rather than waiting forever.

## Integration

- **Partition writer.** The writer in [`partition_writer.rs`](../src/partition_writer.rs) is the only caller of `WalStore`. It calls `sync_durable` after every produce group and on an explicit `SyncDurable` message, and it routes truncate, reset, and trim through the WAL so the hot tail and the quorum copies stay consistent with the canonical log. `DeleteRecords` applies its trim to the WAL first and then to the local log, by the `delete_records_trim_application` kernel described in [`partition_writer/mutations.rs`](../src/partition_writer/mutations.rs).
- **Replicator supervisor.** Every metadata image recomputes the placements, installs them in the registry, reconciles the follower tasks, and prunes orphaned shard directories, in [`replicator_supervisor/reconcile.rs`](../src/replicator_supervisor/reconcile.rs).
- **KRaft controller.** The controller commits offset reservations and publishes the topic configuration and partition records the placement derives from. See the [KRaft design](../../raft/docs/design.md).
- **Metadata observers.** A broker-only node reaches the controller through the `MetadataSource` seam in [`metadata_source.rs`](../src/metadata_source.rs). It cannot read the controller epoch, which is why the reservation check accepts a committed response epoch on such a node.
- **Fetch handler.** The broker listener routes a shard-addressed KIP-595 fetch to the registry before the ordinary fetch plan runs, and the ordinary read loop consults the hot tail and the cold-read path for diskless partitions.
- **Log.** The WAL relies on `krabka-log` for verbatim append, raw reads, truncation, trimming, the leader-epoch checkpoint, and fsync of the segment files and the directory. See the [log design](../../log/docs/design.md).
- **Metrics.** The engine, the flusher, and the read path report the `diskless_wal_*` families in [`metrics.rs`](../src/metrics.rs): the durable watermark and per-voter lag per shard, flush attempts, failures, and bytes, the projection lag and trim frontier, quorum-loss events, and cold-read hits, misses, and errors.

The configuration keys are `diskless_wal_local_replica_count` (default 3, must be odd), `diskless_wal_flush_interval` (250 ms), `diskless_wal_flush_max_size` (8 MiB), `diskless_wal_hot_tail_max_size` (64 MiB), `diskless_wal_trim_safety_lag` (1, must be nonnegative), `diskless_wal_index_projection_timeout` (5 s), and `inter_broker_principal_node_ids`. The defaults live in [`config.rs`](../src/config.rs) and the range checks in [`config/scalar_checks.rs`](../src/config/scalar_checks.rs).

## Kafka / KIP Compliance

The diskless path is a Krabka extension. No Kafka client sees a new API, error code, or response shape. The topic key `krabka.diskless` is the only new surface, and it is administered through the ordinary `CreateTopics`, `AlterConfigs`, and `IncrementalAlterConfigs` paths.

- **[KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum).** WAL replication reuses the `Fetch` v17 envelope that the metadata quorum uses. The interpretation decision is the address: the metadata quorum uses the fixed `__cluster_metadata` topic id, and a WAL shard uses the data topic id plus the rack-id discriminator. `last_stable_offset` carries the leader LEO on this private path.
- **[KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation)** and **[KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation).** The leader answers a diverging `last_fetched_epoch` with the `diverging_epoch` hint from the partition's leader-epoch checkpoint, and the follower truncates to it, exactly as a Kafka follower does.
- **[KIP-207](https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change).** A sequencer handoff may lower the advertised high watermark while the new leader re-derives its view from durable media. The crash model records that the durable frontier never moves back, which is the guarantee KIP-207 asks a leader change to keep.
- **[KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage).** Tiered storage is refused on a diskless topic. The two paths would otherwise keep two object-store copies and disagree about which local segments may be deleted.
- **Kafka batch bytes.** Every batch in a WAL replica log, in an object run, and in a hot-tail entry is the producer's verbatim v2 batch, with only `base_offset` and `partition_leader_epoch` patched outside the CRC region by the log's verbatim append.

## Testing

- The [quorum WAL model](../src/wal/quorum/engine/model.rs) drives the real engine's append, sync, acknowledgement, recovery, and truncation over three voters and eight ordered operations. The [diskless crash model](../src/diskless_crash_model.rs) composes the Slice 5 crash windows with the Slice 6 quorum rule and the stateless appenders. Both are listed in the [Stateright inventory](../../../docs/verification.md#stateright-model-check-tier).
- The proved kernels are listed in the [Creusot ledger](../../../docs/verification.md#creusot-proof-ledger): `wal_fetch_admission`, `select_wal_voter_index`, and `exact_wal_batch_range` in [`crates/verified/src/wal.rs`](../../verified/src/wal.rs); `diskless_trim_decision`, `diskless_logical_range`, `diskless_span_extension`, `diskless_batch_step`, `diskless_wal_replay_decision`, and `diskless_object_reclaimable` in [`crates/verified/src/diskless.rs`](../../verified/src/diskless.rs); the four `wal_reservation_*` and `reserve_offsets` kernels in [`crates/verified/src/offset_allocator.rs`](../../verified/src/offset_allocator.rs); and `recompute_high_watermark` in [`crates/verified/src/consensus.rs`](../../verified/src/consensus.rs).
- The engine's [durability](../src/wal/quorum/durability_tests.rs), [distributed](../src/wal/quorum/distributed_tests.rs), [fetch](../src/wal/quorum/fetch_tests.rs), and [recovery](../src/wal/quorum/recovery_tests.rs) tests, the follower and promotion tests beside their modules, and the flusher, index, hot-tail, and cold-read tests beside theirs cover the I/O the models do not drive.
- [`tests/diskless_e2e.rs`](../tests/diskless_e2e.rs) boots three in-process brokers on distinct racks with a shared local object store and a topic-backed index, creates a `krabka.diskless=true` topic through the real `CreateTopics` handler, and sends every produce and fetch over the wire. It stubs none of the network, the placement, or the object store.
