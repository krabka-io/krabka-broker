# Native topology filesystem concurrency follow-up (2026-11-07)

Base: `c51d444b3` (origin/main). This is a native-backend callback regression,
not a new RF3 wire differential, full KIP-1331 graduation, or power-loss test.

## Released contract checked

The [released Kafka 4.4.0 topology plugin interface](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/group-coordinator-api/src/main/java/org/apache/kafka/coordinator/group/api/streams/StreamsGroupTopologyDescriptionPlugin.java)
requires implementations to be thread-safe and accept concurrent same-group
`setTopology` calls. Repeated calls for the same group/epoch carry identical
payloads and must be idempotent. The source was fetched directly with
`gh api .../contents/.../StreamsGroupTopologyDescriptionPlugin.java?ref=4.4.0`.
No JVM broker execution is claimed by this artifact.

## Defect and fix

The native filesystem backend reused `<group>.pending` for every attempt.
Opening that pathname truncates the same inode held by an interrupted or delayed
writer. Renaming it to the final pathname can therefore publish an inode still
writable by the old descriptor. Concurrent attempts also race to rename a single
pathname: one can fail even when both submitted identical descriptions.

Each attempt now exclusively creates a UUID-named staging file in the same
backend directory, syncs its own inode and atomically renames it to the final
group pathname. Failed IO attempts remove their staging name where possible.
The version-1 JSON format and final group filename are unchanged.

Regressions execute real filesystem IO in
`crates/broker/src/coordinator/unified/streams/description/file_backend.rs`:

- A delayed descriptor for the former fixed staging pathname writes after a
  successful newer set; the published description must remain readable and
  unchanged. This deterministically exposes the old inode-aliasing defect.
- Sixteen independent backend handles concurrently set the same group/epoch and
  payload. Every callback must succeed, the final envelope must decode to that
  epoch/payload, and completed attempts must leave only the final file.

## Limits

Unique staging prevents byte aliasing; it does not fence a stale owner from
publishing a complete older description later, order concurrent different epochs,
serialize deletion against writes, or establish directory-fsync/power-loss
persistence. Host ownership fencing remains necessary. Cancellation/process death
can leave `.pending-<uuid>` files; they are never served as group data and can be
removed when backend activity is stopped. RF3 active-member movement and
broker-wide distributed cleanup scheduling remain separate qualification work.

## Executed checks and identities

- Base commit: `c51d444b3c290b307bb9cb43b2f4faed9344a603`.
- Released plugin API Git blob: `5f6409a40ac899546445370896ec0c122e37bfd2`.
- Fixed `file_backend.rs` SHA-256:
  `90b4d9087627481955bf0d4281d4fc9468a747d5609a84013f8fd174e0674722`.
- `CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0
  cargo test -p krabka-broker --lib streams::description::file_backend::tests`:
  **4 passed**, 0 failed.
- The same fixed-source broker test executable with filter `streams`:
  **420 passed**, 0 failed (4.33s), including cleanup/reconciliation actor models.
- Changed-file nightly Rustfmt and `git diff --check`: passed.

Execution used a private `TMPDIR` and an existing shared Cargo target directory;
these are agent execution settings, not repository requirements. Full Clippy,
released JVM broker execution, and a fresh distributed schedule were not run in
this narrow lane.

Negative control: retained the new regression but restored the original
`path.with_extension("pending")` plus `File::create` staging selection.
`cargo test -p krabka-broker --lib
 delayed_staging_writer_cannot_corrupt_a_published_description` then failed as
expected (**0 passed, 1 failed**, Cargo exit 101): the published envelope read
returned `Err(Permanent)` after the old descriptor's delayed write. Restoring the
fixed source reproduced the exact SHA-256 above. Fixed-source checks were run
before this temporary control; no mutated production source is committed.
