# Native topology namespace durability and stale-mailbox follow-up

Base: `234dadc77` (2026-11-08). This is a native filesystem and actual-actor
qualification follow-up, not a new released Kafka/RF3 wire differential.

## Implementation

The file backend previously synced each staging inode before atomic rename, but
never synced the containing directory. A successful rename is not by itself a
power-loss durability acknowledgement of the new name. Set now awaits directory
sync after rename. Delete syncs the directory after removing the group and legacy
staging names, including when both were already absent: a retry must finish a
prior attempt that removed a name but failed before namespace sync. IO errors,
including directory-sync failures, remain transient. File format and final
filenames are unchanged.

This relies on filesystem directory-sync support and a durably provisioned root
and ancestor directories. `new` may create directories but does not durably sync
all newly created ancestors. Unsupported directory sync is an error, not a
silent durability downgrade. Sync does not impose winner order on overlapping
sets or fence writers; the host must prevent simultaneous old/new owners.

## Directed regression changes

- A filesystem delete removes its final value, then fails on a deliberately
  invalid legacy staging entry. Reopen and repair followed by repeated delete
  must succeed without resurrecting the value.
- During a blocked backend cleanup, queue an actual stale `PushDescription`
  behind the deletion/conversion turn as well as a join. Both mailbox replies
  must be dropped at retirement, and the backend remains empty.
- During an admitted backend write, queue shutdown and then a stale push. The
  admitted write completes; shutdown drops the queued stale mutation before
  replay into a replacement coordinator with an active member.

These exercise real storage and actors, not a replacement transition model.

## Boundaries

Existing actual-actor coverage establishes bounded graceful active-member handoff,
serialized delete/conversion, retired-mailbox rejection and cleanup ownership
admission. It does not establish arbitrary simultaneous coordinator owners,
RF3 active-member movement under network faults, distributed fencing tokens or
storage CAS. The backend SPI carries no ownership generation; comparing topology
epochs is not sufficient because a recreated group may restart its epoch.

File/directory sync ordering is stronger than clean reopen but not an executed
power-cut test, storage-device flush guarantee, non-POSIX filesystem qualification,
or arbitrary crash-window proof. No universal KIP-1331 graduation is claimed.

## Executed checks and identities

- Base: `234dadc77`.
- `file_backend.rs` SHA-256:
  `53179ea3cc2bf286b4474b59a34bef6317c7a7088124824f6af44bd1f9870000`.
- `actor/description/tests.rs` SHA-256:
  `9b4ab4bdb264f8184572a71c8f9a30e2453c13820d415968205ee344b31ba12d`.
- Broker test executable SHA-256:
  `71cc01f65ffd77e24fbdd52041d6ffd754ceb597b6274ae9b4a9da24bf7f692e`.
- Rust 1.99.1; `CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0
  CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo test -p krabka-broker --lib
  streams::description::file_backend::tests`: **5 passed, 0 failed**.
- Same fixed-source executable, filter `streams::actor::description::tests`:
  **29 passed, 0 failed**, including the bounded Stateright actor model and both
  strengthened queued-push regressions (3.35 seconds).
- Same executable traced with `strace -f -yy -e
  trace=fsync,rename,renameat,unlink,unlinkat`, filter
  `filesystem_store_survives_reopen_and_fences_epochs --test-threads=1`:
  **1 passed**. [Normalized trace](filesystem.strace) shows inode fsync, rename,
  directory fsync, removal, directory fsync, absent retry, directory fsync.
- Same trace with `-e inject=fsync:error=EIO:when=2`: **expected exit 101**,
  with `set: Transient` rather than a successful callback. The
  [fault trace](directory-sync-error.strace) confirms the injected failure hit
  the directory fsync after successful inode sync and rename, not the inode.
  Strace invocation counts are per tracee; future scheduling can change which
  fsync receives an ordinal injection. This receipt claims only the recorded run.
- Changed-file nightly Rustfmt and `git diff --check`: passed.

Paths, process IDs and per-attempt UUIDs in the two traces are normalized;
operation order and syscall results are preserved. Full Clippy, released JVM
execution, and physical power cuts were not run in this bounded lane. A private
temporary directory and an existing Cargo target were execution settings only,
not software acceptance requirements.

## Recovery verification (2026-11-08)

After runtime recovery, reviewed commit `aed78cd51f139322d82d9add00fab3220545107d`
and independently matched both source SHA-256 values above. The retained test
executable at `projects/kafka44-storage-gap/target/debug/deps/krabka_broker-bb7bc4369afd524b`
(relative to the coordinator workspace) exactly matches the executable SHA-256
above. Direct execution against the retained source, with a private `TMPDIR`,
again passed **5/5 filesystem tests** and **29/29 actor-description tests**
(including the bounded Stateright model, 3.24 seconds). Changed-file nightly
Rustfmt and `git diff --check` passed. This was a replay of the identified binary,
not a fresh Cargo compilation or a rerun of the recorded strace fault injection.
Source review confirmed file sync precedes rename, directory sync precedes
successful mutation acknowledgement, and directory-sync errors are transient.
No additional source defect was found; the qualification boundaries above remain.
