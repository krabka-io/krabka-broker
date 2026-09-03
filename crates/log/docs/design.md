# Log and Segment Format Design

The on-disk partition log: Kafka's segment and index files, byte for byte, with the recovery, retention, and compaction rules the broker builds on.

This document follows the [design document style guide](../../../docs/style_guides/design_doc_style_guide.md). `krabka-log` works on one partition directory at a time. Everything above it, such as leadership, replication, transaction visibility policy, and tiered-storage scheduling, belongs to the broker and is documented with that subsystem.

## Design Goals

- **Byte compatibility with Kafka 4.x log directories.** A directory that `krabka-log` writes must open in a JVM broker, and a directory a JVM broker wrote must open here. That covers the 20-digit zero-padded segment names, the v2 `RecordBatch` stream in `.log`, the sparse `.index` and `.timeindex` layouts, the `.txnindex` records, the `.snapshot` producer state, and the `.leader-epoch-checkpoint` text file. `kafka-dump-log` must be able to read every one of them.
- **Producer bytes reach disk unchanged.** The verbatim append path writes the producer's batch with only `base_offset` and `partition_leader_epoch` patched, both outside the CRC region, so the stored CRC is the producer's CRC.
- **A reopened log is exactly the log the append path left.** Recovery truncates a torn tail, rebuilds the sparse indexes from the retained prefix, restores producer state from the newest valid snapshot plus a replay of the tail, and heals an interrupted compaction swap.
- **One writer, many readers, no lock across an await.** `Log` takes `&mut self` for every mutation and `&self` for every read. The broker wraps it in a mutex and drives all mutations through one partition writer task.
- **Pure decision cores.** Retention, compaction retention, the leader-epoch lookup, and the index lookups are free functions or proved kernels, so the model checkers and the property tests drive the same rules the file paths apply.

## Architecture Overview

### Files

A partition lives in `<log_dir>/<topic>-<partition>/`, named by [`name.rs`](../src/name.rs). Each segment is a base offset and the files that share it:

| File | Contents | Module |
| :--- | :--- | :--- |
| `<base>.log` | Append-only v2 `RecordBatch` stream. | [`segment/append.rs`](../src/segment/append.rs), [`segment/read_raw.rs`](../src/segment/read_raw.rs) |
| `<base>.index` | Sparse 8-byte entries: relative offset to byte position, one per `index.interval.bytes`. | [`index/offset.rs`](../src/index/offset.rs) |
| `<base>.timeindex` | Sparse 12-byte entries: timestamp to relative offset. | [`index/time.rs`](../src/index/time.rs) |
| `<base>.txnindex` | 24-byte records, one per aborted transaction in the segment. | [`txn_index.rs`](../src/txn_index.rs) |
| `<base>.snapshot` | Producer state at the segment boundary, in Kafka's snapshot format. | [`producer_snapshot.rs`](../src/producer_snapshot.rs) |
| `<base>.stampindex` | Krabka-private commit-stamp sidecar, present only when a stamp source is injected. | [`stamp_index.rs`](../src/stamp_index.rs) |
| `.leader-epoch-checkpoint` | Per-partition two-column text file of `(epoch, start_offset)`. | [`leader_epoch_checkpoint.rs`](../src/leader_epoch_checkpoint.rs) |
| `log-start-offset-checkpoint` | The log start offset, when the segment names alone cannot express it. | [`log_start_offset_checkpoint.rs`](../src/log_start_offset_checkpoint.rs) |

Kafka keeps the log start offset per log dir, keyed by partition, and writes it on a `log.flush.start.offset.checkpoint.interval.ms` schedule; krabka keeps it per partition directory, which needs no key inside the file, and writes it on the trim itself.

The `.stampindex` sidecar is the one file Kafka does not have. It never changes the `.log` bytes, offset assignment, the LSO, or the high watermark, and nothing on any client-facing API reads it.

### The `Log`

[`log.rs`](../src/log.rs) holds the type: a sorted list of sealed `Segment`s and one active segment. Each concern lives in its own submodule with its own `impl Log` block. The two write paths are the owned append in [`log/append.rs`](../src/log/append.rs), which decodes and re-encodes a `RecordBatch`, and the verbatim append in [`log/verbatim.rs`](../src/log/verbatim.rs), which writes the producer's bytes. Both funnel through one private helper so the LSO, the producer state, the transaction index, the stamp index, and the leader-epoch checkpoint move identically.

A segment rolls when the active `.log` reaches `segment.bytes`, checked at append, or when its first record is older than `segment.ms`, checked by `tick`. The roll flushes the active segment, writes the boundary producer snapshot, seals the old segment, creates the new one, and marks the directory as needing an fsync. The snapshot is written after the flush so it can never become durable ahead of the records it describes.

The three read paths in [`log/read.rs`](../src/log/read.rs) return decoded batches, verbatim bytes, or file-region descriptors for the zero-copy `sendfile` fetch. Each walks the sealed segments and then the active one, so a read can span a boundary, and each asks a segment for at least one whole batch header, which is Kafka's anti-stall rule.

### Durability

[`log/sync.rs`](../src/log/sync.rs) flushes and fsyncs the active segment and, when a new segment file was created since the last sync, the directory. A segment name is durable only once its parent directory is synced, so the log tracks that debt and pays it in one place. `flush_on_append` is off by default; the broker decides when to sync, and a durable sidecar write, such as a transaction marker's `.txnindex` entry, forces a flush of its own.

### Recovery

`Log::open` in [`log/open.rs`](../src/log/open.rs) heals orphaned compaction `.swap` files first, then scans the directory for `.log` names, validates the base-offset chain with the proved `local_recovery_segment_chain` kernel, and marks every segment but the last as sealed. The active segment opens with tail recovery when `validate_on_open` is set, which is the default: [`segment/open.rs`](../src/segment/open.rs) scans from byte zero, stops at the first framing, CRC, overlap, or arithmetic failure, truncates the file there, and rebuilds both sparse indexes from the retained prefix.

Producer state comes back from the newest `.snapshot` at or below the log end, chosen by the proved `producer_snapshot_latest_index` kernel, followed by a replay of the batches the snapshot does not cover. A malformed or future-dated snapshot is removed and the next candidate is tried. The LSO, the pending transactions, and the coordinator epochs are rebuilt from that replay, because no sidecar holds them.

### Truncation and trimming

[`log/truncate.rs`](../src/log/truncate.rs) has two operations. `truncate_to` discards every record at or past an offset, cuts the leader-epoch checkpoint and the producer snapshots that begin at or after it, and is what replication and leader election use after a divergence. `trim_to_offset` moves the log start forward: it deletes every sealed segment whose last offset is below the target and, when the target falls inside the active segment, advances a start override without deleting the file. Segment deletion is its own witness across a restart, but the override is not, so `set_log_start_offset` checkpoints it to `log-start-offset-checkpoint` and `Log::open` reads it back clamped to the offsets the log actually holds. Without that a reopened log serves records a `DeleteRecords` already deleted. `reset_to` empties the log at a new base offset, which a follower needs when it has fallen below the leader's log start.

### Retention and compaction

`Log::tick` in [`log/tick.rs`](../src/log/tick.rs) rolls an old active segment and then applies time-based and size-based retention to the sealed segments through the proved `local_retention_prefix` kernel. Retention deletes only a contiguous oldest prefix, never the active segment, never the last segment, and never a segment that still holds a record whose delivery time has not arrived. A tiered topic skips local retention here, because the `RemoteLogManager` owns its segment lifecycle.

Compaction, in [`compact.rs`](../src/compact.rs), makes one pass over the sealed segments from oldest to newest, builds the key-to-newest-offset map, and rewrites the survivors into `.swap` files at the lowest input base offset. [`compact/swap.rs`](../src/compact/swap.rs) promotes them crash-safely: fsync each swap, delete the consumed segments, rename, fsync the directory. Records are dropped, never renumbered: [`filter.rs`](../src/filter.rs) keeps `base_offset` and every surviving `offset_delta`, so an offset a client holds still names the same record after the rewrite. The KIP-534 retain and delete-horizon rules are the proved `retain_decision` and `compute_horizon` kernels.

### Transactions, delivery, and stamps

[`log/transaction.rs`](../src/log/transaction.rs) keeps the last stable offset at the first offset of the earliest open transaction, computed by the proved `first_unstable_offset` kernel, and applies a commit or abort marker to close one. An abort appends the transaction's offset range to the active `.txnindex`. [`log/delivery.rs`](../src/log/delivery.rs) derives the deliver-at-time watermark for a KFC-1 scheduled topic from batch timestamps alone, so nothing persists it. [`log/stamp.rs`](../src/log/stamp.rs) folds an injected internal stamp coordinate into the `.stampindex` sidecar, and a log with no injected source stamps nothing.

## Key Design Decisions

### Verbatim passthrough rather than decode and re-encode

Decoding every producer batch and re-encoding it would cost CPU on the hottest path and would recompute a CRC the producer already computed. The verbatim path reads only the fixed batch header, validates the CRC once, patches the two fields outside the CRC region, and writes the bytes. The trade-off is that a control batch cannot take this path, because the LSO bookkeeping needs the inner marker record, so transaction markers use the owned append.

### Tail validation on open is the default

Skipping the scan makes open faster but leaves a torn tail in place, where the next append would land after garbage. The default scans the active segment and truncates at the first invalid byte. The diskless path forces the scan on regardless of the configured value.

### The producer snapshot is written at every roll

Kafka writes `.snapshot` at segment boundaries, and a reopened log must find the same files a JVM broker would. Writing the snapshot after the segment flush and before the seal means recovery can always trust a snapshot that exists, and a replay of at most one segment's tail closes the gap.

### Compaction never touches the active segment

The active segment is where the leader's log end lives, and a rewrite there would move an offset a producer has already been given. Compacting only sealed segments keeps the log end fixed and lets the swap promotion be a rename of files nobody is appending to.

### The `.stampindex` sidecar mirrors the `.txnindex` pattern

A second internal coordinate could have been a new record header or a change to the batch layout, either of which would break byte compatibility. A per-segment fixed-width sidecar that is retained and truncated with its segment adds the coordinate without touching a single wire-exact byte.

### Free functions and kernels for the decisions

Retention, compaction retention, the leader-epoch lookup, the index lookups, and the recovery steps are pure. That is what lets [`compact_model.rs`](../src/compact_model.rs) and [`leader_epoch_model.rs`](../src/leader_epoch_model.rs) enumerate them with Stateright, lets the proptest suites fuzz them, and lets Creusot prove the ones in the ledger, without a file in sight.

## Integration

- **Broker partition writer.** One task per partition owns the `Log` mutex and serializes every mutation, which is the "one writer" half of the `Log` contract. See the [replication and ISR design](../../broker/docs/replication-isr-design.md).
- **Replication and leader election.** `append_at` and `append_verbatim_at` keep a leader-assigned offset; `truncate_to` and `reset_to` are the follower's recovery moves; the leader-epoch checkpoint answers `OffsetForLeaderEpoch`.
- **KRaft.** `KraftLog` in [`krabka-raft`](../../raft/docs/design.md) is a facade over `Log` at `<dir>/@metadata-0`, adding only the high watermark and the committed-read filter.
- **Diskless WAL.** The WAL replica logs, the follower logs, and the object runs are all `Log` directories and verbatim batch runs. See the [diskless WAL design](../../broker/docs/diskless-wal-design.md).
- **Tiered storage.** [`log/tiering.rs`](../src/log/tiering.rs) describes sealed segments for upload and deletes the local copies the `RemoteLogManager` names. The log enforces no tiering invariant of its own.
- **Configuration.** [`config.rs`](../src/config.rs) carries the per-topic tunables with Kafka 4.2 defaults: `segment.bytes` 1 GiB, `segment.ms` 7 days, `retention.ms` 7 days, `index.interval.bytes` 4 KiB, `max.message.bytes` 1048588, `delete.retention.ms` 24 hours, `compression.type=producer`, and `remote.storage.enable=false`.

## Kafka / KIP Compliance

- **Storage format.** Segment naming, the v2 batch stream, the offset and time index entry layouts, the transaction index record, the producer snapshot, and the leader-epoch checkpoint match Apache Kafka 4.x. [`tests/integration.rs`](../tests/integration.rs) round-trips a real JVM broker's log directory through this crate.
- **[KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)** and **[KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation).** The checkpoint file and the `(found_epoch, end_offset)` lookup mirror `LeaderEpochFileCache`.
- **[KIP-534](https://cwiki.apache.org/confluence/display/KAFKA/KIP-534%3A+Retain+tombstones+and+transaction+markers+for+approximately+delete.retention.ms+milliseconds).** Batch attribute bit 6 is the delete horizon, stamped exactly once. Control batches are kept out of the dedup map, which is the fix for the marker-deletion bug the compaction model documents.
- **[KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage).** `local.retention.ms` and `local.retention.bytes` inherit the topic retention when unset, and a tiered topic's segment lifecycle is owned above this crate.
- **[KIP-734](https://cwiki.apache.org/confluence/display/KAFKA/KIP-734%3A+Improve+Kafka+ListOffsets+API+to+support+MaxTimestamp+and+MinTimestamp).** Timestamp searches resolve ties to the earliest offset.
- **Krabka extensions.** Deliver-at-time visibility is [KFC-1](../../../docs/KFCs/KFC-1-deliver-at-time-visibility.md). The commit-stamp sidecar and the barrier control record used by [KFC-4](../../../docs/KFCs/KFC-4-cross-topic-snapshots.md) leave the Kafka-visible bytes untouched.

## Testing

- The Stateright models [`compact_model.rs`](../src/compact_model.rs) and [`leader_epoch_model.rs`](../src/leader_epoch_model.rs) are listed in the [inventory](../../../docs/verification.md#stateright-model-check-tier).
- The proved kernels in the [Creusot ledger](../../../docs/verification.md#creusot-proof-ledger): the index lookups in [`crates/verified/src/log_index.rs`](../../verified/src/log_index.rs), the timestamp scan in [`crates/verified/src/timestamp.rs`](../../verified/src/timestamp.rs), the stamp ranges in [`crates/verified/src/stamp.rs`](../../verified/src/stamp.rs), the retention prefix in [`crates/verified/src/retention.rs`](../../verified/src/retention.rs), the compaction rules in [`crates/verified/src/compaction.rs`](../../verified/src/compaction.rs), the producer snapshot rules in [`crates/verified/src/producer_snapshot.rs`](../../verified/src/producer_snapshot.rs), the recovery steps in [`crates/verified/src/local_recovery.rs`](../../verified/src/local_recovery.rs), and the epoch lookup in [`crates/verified/src/leader_epoch.rs`](../../verified/src/leader_epoch.rs).
- [`tests/proptest_log.rs`](../tests/proptest_log.rs) fuzzes append, read, and truncation round trips. [`tests/restart.rs`](../tests/restart.rs) checks what a reopened log knows about segments it did not write. [`tests/delivery.rs`](../tests/delivery.rs) covers scheduled visibility. [`tests/integration.rs`](../tests/integration.rs) is the Docker-gated JVM round trip.
