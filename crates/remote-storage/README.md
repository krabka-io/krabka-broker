# crabka-remote-storage

[![Crates.io](https://img.shields.io/crates/v/crabka-remote-storage.svg)](https://crates.io/crates/crabka-remote-storage)
[![Docs.rs](https://docs.rs/crabka-remote-storage/badge.svg)](https://docs.rs/crabka-remote-storage)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

KIP-405 tiered-storage SPI (RemoteStorageManager / RemoteLogMetadataManager) and reference implementations for Crabka.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-remote-storage
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Copy a closed log segment into the filesystem-backed remote tier and fetch its offset index:

```rust,no_run
use std::{collections::BTreeMap, path::PathBuf};

use bytes::Bytes;
use crabka_ids::LeaderEpoch;
use crabka_remote_storage::{
    IndexType, LocalTieredStorage, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
};
use uuid::Uuid;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let storage = LocalTieredStorage::new(PathBuf::from("/var/lib/crabka-remote"));
let topic_partition = TopicIdPartition::new(Uuid::new_v4(), "orders", 0);
let segment_id = RemoteLogSegmentId::new(topic_partition, Uuid::new_v4());
let mut leader_epochs = BTreeMap::new();
leader_epochs.insert(LeaderEpoch(0), 0);
let metadata = RemoteLogSegmentMetadata::new(
    segment_id,
    0,
    999,
    1_713_000_000_000,
    1,
    1_713_000_000_000,
    RemoteLogSegmentDetails::new(
        1_048_576,
        RemoteLogSegmentState::CopySegmentStarted,
        leader_epochs,
    ),
)?;
let segment = LogSegmentData {
    log_segment: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.log"),
    offset_index: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.index"),
    time_index: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.timeindex"),
    transaction_index: None,
    producer_snapshot_index: None,
    leader_epoch_index: Bytes::new(),
};
let _custom_metadata = storage.copy_log_segment_data(&metadata, &segment)?;
let _offset_index = storage.fetch_index(&metadata, IndexType::Offset)?;
# Ok(())
# }
```

## WORM archive mode

WORM archive mode turns an object-store tier into a write-once compliance archive. The broker still tiers segments the way KIP-405 describes, but it never rewrites and never deletes what it wrote, and it leaves proof that an auditor can check years later without the cluster.

The mode changes four things:

- **Every object is a conditional create.** The backend writes with `PutMode::Create`, so a second write to the same key fails instead of replacing the first.
- **Every segment copy seals a manifest.** The `.manifest` object beside the segment names each object the copy wrote, records a SHA-256 digest for each one, and carries the partition's hash-chain head. An Ed25519 signature over that head binds the chain to a named key.
- **Every delete is refused.** The backend returns an error rather than remove an object, whatever asks it to.
- **Remote retention is disabled.** The `RemoteLogManager` does not run its remote-retention pass for a WORM partition, so `retention.bytes` and `retention.ms` never reach the archive.

Local retention is unaffected. The broker still evicts a local segment once the archive holds it, so a WORM deployment does not need disk for the whole history.

### Configuration

The `[remote_storage.worm]` table turns the mode on. Its presence is the switch, and the table applies to whichever object store `[remote_storage.s3]` or `[remote_storage.gcs]` selects. A `storage_dir` backend is rejected at load time, because a local directory cannot enforce write-once.

```toml
[remote_storage.s3]
bucket = "krabka-archive"
region = "us-east-1"

[remote_storage.worm]
signing_key_path = "/etc/krabka/worm-signing.pk8"
signing_key_id = "worm-2026-q3"
write_only = false
```

| Key | Default | Description |
|-----|---------|-------------|
| `signing_key_path` | unset | Path to the PKCS#8 Ed25519 key that signs each manifest. Set it together with `signing_key_id`. |
| `signing_key_id` | unset | Stable id recorded in every signature, so a chain stays verifiable across a key rotation. Set it together with `signing_key_path`. |
| `write_only` | `false` | Refuse every remote fetch from this archive. |

An unsigned archive is legal: leave both key fields unset and the archive keeps the per-object digests and the hash chain. It then proves continuity but not authorship, and `crabka-worm-verify` grades it as an incomplete attestation.

### The bucket enforces WORM, not the broker

The retention that stops an administrator with delete rights lives on the bucket. Configure S3 Object Lock in compliance mode with a default retention period, which is how a compliance deployment is built anyway. Crabka does not set the lock: `object_store` 0.13 models no `x-amz-object-lock-*` header, and the crate is pinned in lock-step with the datafusion revision and `parquet 59`, so the header is not reachable from this code.

The broker's contribution is narrower and still worth having. It never issues a delete, it never overwrites, and it leaves a signed chain that shows what the archive held. A bucket with Object Lock and a broker that writes once are the two halves of the guarantee. Neither half is enough alone.

### Conditional create binds only below the multipart threshold

`PutMode::Create` applies to a single-PUT write. `object_store` 0.13's `PutMultipartOptions` has no mode field, so a `.log` body above the multipart threshold uploads as a plain multipart put with no precondition. A large segment body is protected by the bucket's Object Lock and not by the broker's precondition.

Manifests are small and always take the single-PUT path, so every manifest is a conditional create. "One manifest per segment, and never rewritten" holds whatever the segment size is.

### `write_only = true` disables remote fetch

With `write_only = true` the archive is a compliance sink and not a storage tier. A consumer that asks for an offset whose local segment was already evicted gets an error, not a slow read. This is a cliff and not a slowdown: the data is in the bucket, and the broker refuses to serve it.

Set `write_only = true` only when the archive exists for the auditor and the read path never needs it. Leave it `false` when consumers still read history through the tier.

### Topic deletion leaves the archive intact

A `DeleteTopics` call removes the partition from the cluster. The objects and the manifests stay in the bucket, and the broker deletes nothing.

That is intended. Erasing a compliance archive must not be a side effect of a topic delete, and a retention rule the archive never agreed to must not arrive through the admin API. Removal of archived data is a bucket-side action, taken deliberately after the Object Lock retention period ends.

### Chain continuity across restarts needs a topic-backed RLMM

The broker reads its chain tip back from the remote-log metadata manager. With `RlmmKind::InMemory` that metadata does not survive a restart, so the broker starts a new epoch at genesis rather than continue a chain it cannot read. Nothing binds the manifests before the restart to the manifests after it.

`crabka-worm-verify` reports that as an attestation hole, and it is one. A WORM deployment should use the topic-backed `[remote_storage.kafka_metadata]` manager, which recovers its state from `__remote_log_metadata` after a restart.

### Tail truncation is not detectable from the archive alone

An attacker who deletes the newest manifests of a partition, and the objects they name, leaves a shorter chain that verifies perfectly. Every remaining head chains correctly and every remaining signature is valid. Nothing inside the archive says how long the chain should be, so no amount of reading the archive reveals what is gone.

Close that gap outside the archive. A successful verify prints each partition's tip head; record it somewhere the bucket's writers cannot reach, and pass it to the next run as `--expect-head`. A run that holds no expected head proves internal consistency and says nothing about completeness.

### Verify an archive with `crabka-worm-verify`

`crabka-worm-verify` audits an archive with read-only credentials, with no broker and no cluster. It takes its credentials from the ambient AWS chain, and it has no `--access-key` flag on purpose: an auditor should hold a read-only role and not a copy of the writer's keys.

```sh
crabka-worm-verify verify \
  --bucket krabka-archive \
  --region us-east-1 \
  --key-id worm-2026-q3 \
  --public-key /etc/krabka/worm-2026-q3.pub \
  --expect-head 9f2c1b0a... \
  --deep
```

Use `--local-dir PATH` in place of `--bucket` for a mounted copy, and `--endpoint URL` with `--allow-http` for an S3-compatible endpoint served without TLS. Narrow a run with `--prefix`, `--topic`, and `--partition`. `--allow-epoch-restarts` accepts a chain restart instead of grading it as a hole.

`--deep` downloads every object and recomputes its SHA-256. It is the only check that catches a body replaced with different bytes of the same length: a shallow run checks that each object exists with the recorded size, and a same-size substitution passes that check.

The verdict goes to stdout and the diagnostics go to stderr, so a script can read the grade without parsing the explanation.

| Verdict | Exit code | What it means |
|---------|-----------|---------------|
| `OK: N manifests over M partition(s), …` | 0 | The chain is continuous, every signature is valid, and every object is present. One line per partition follows, and it names the tip for the next `--expect-head`. |
| `OK: empty archive` | 0 | Nothing to verify. |
| `TAMPER DETECTED at KEY (seq N)` | 1 | A manifest was rewritten, reordered, or removed, or an object does not match what its manifest records. |
| `HEAD MISMATCH: expected X, archive tip Y` | 1 | The chain is internally perfect but stops short of the head the run was given. This is what tail truncation looks like. |
| `ORPHAN OBJECTS: N object(s) with no manifest` | 1 | An object that no manifest names makes no integrity claim. |
| `INCOMPLETE ATTESTATION: chain restarted N time(s)` | 1 | The chain has holes between epochs. Use a topic-backed RLMM, or accept them with `--allow-epoch-restarts`. |
| `INCOMPLETE ATTESTATION: N manifest(s) unsigned, M signed by an untrusted key` | 1 | The archive is not attested to the key the run trusts. |

A failure to read the archive is a different outcome from a broken archive, and the tool keeps them apart. "I could not look" exits with an error message and no verdict.

## Documentation

Read the API documentation at [docs.rs/crabka-remote-storage](https://docs.rs/crabka-remote-storage). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
