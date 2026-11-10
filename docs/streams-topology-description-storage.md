# Streams topology-description storage

Krabka serves released Kafka 4.4 KIP-1331 without unstable flags. No plugin is
configured by default: heartbeats never request a description, updates return
`UNSUPPORTED_VERSION`, and included descriptions report `NOT_STORED`.

## Native extension contract

The Kafka configuration value
`org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin` selects
Krabka's broker-local memory implementation. Descriptions and solicitation
backoff are owned by the coordinator and shared across replacement group actors,
so actor reload does not lose them. A new coordinator/process starts with empty
runtime state. Use an explicit shared native backend when movement to another
broker must retain data.

An application embedding the Rust broker can install
`StreamsGroupConfig.topology_description_backend = Some(Arc::new(backend))`.
This takes precedence over the reference plugin. Implement the public
`TopologyDescriptionBackend` asynchronous `set`, `get`, and `delete` methods:

- One cluster-scoped namespace; distinguish exact group IDs and topology epochs.
- `set` replaces a group's description and is idempotent under replay/retry.
- `get(group, epoch)` returns `None` for a missing or mismatched epoch.
- `delete` removes the group's state, succeeding if already absent.
- Operations must be cancellation-safe, non-blocking and have bounded IO timeouts.
- Share the backend across all potential coordinators. Only the active group
  coordinator may write. The SPI does not replace Kafka's ownership fencing.
- Return `StorageFailure::Permanent` only for a topology that cannot ever be
  stored. All other store failures are transient. Get failures become describe
  status `ERROR`; an absent value becomes `NOT_STORED`.
- Own connection pools in the backend and release resources in `Drop`.

This is a source-level Rust embedding interface, not a stable shared-library ABI.
It does **not** load Java classes, implement JNI, or accept arbitrary Kafka JVM
plugin JARs. Native backend installation is not exposed as a Kafka broker config
or hot-reload facility; existing unrecognized JVM class names still fail startup.

## Filesystem implementation

`FileTopologyDescriptionBackend::new(directory).await` provides a native backend
beyond the reference memory store. It stores version-1 JSON envelopes containing
a description and its epoch; golden-byte fixtures cover the initial reader/writer
and unknown versions are rejected. The directory is an explicitly selected
plugin-private namespace, not an alteration of existing internal-topic or log
formats. It uses an injective hex encoding of group IDs (no path traversal), and writes via a
synced, exclusively created per-attempt staging file and rename, followed by a
parent-directory sync before reporting success. Deletion also syncs the backend
directory, including retries when the group file is already absent. A failed
sync returns a transient failure even if the visible mutation has already
happened; retries remain safe. The operator must provision the backend directory
and durably persist its ancestors before use. Filesystems/platforms that cannot
open and sync a directory fail mutation callbacks rather than acknowledge an
unsynced namespace change. Independent
backend instances can concurrently publish identical group/epoch descriptions
without reusing an open staging inode; a delayed writer cannot alter the bytes
of an envelope already published by another attempt. This is atomic-publication
integrity, not coordinator fencing or ordering between different epochs.
Reopening the same directory retains data. IO failures remove the attempt's
staging file where possible. Cancellation/process death may leave private
`.pending-<uuid>` files; these are never read as group data and may be removed
while no backend operations are running.
Operators own directory access permissions, disk capacity, and backups. Filename
length is bounded by the filesystem; long group IDs can fail storage. The format
is private and not Kafka's JVM serialization format. Process restart is covered;
file-and-directory sync ordering is implemented, but actual power-loss durability
(including device write caches) and non-POSIX rename semantics are not qualified.

A local disk directory does not follow a coordinator to another machine. Use a
shared directory with atomic rename semantics, or implement a distributed native
backend. Do not point unrelated Kafka clusters at the same directory.

## Lifecycle and evidence

The actor persists stored epoch `UNCERTAIN` (-2) **before** mutation and the
successful epoch **after** it. Permanent store failure records the failed epoch;
transient failure retains solicitation backoff. Heartbeat backoff starts at 30
seconds, doubles to one hour and has 20% jitter. The offset-retention sweep
routes through the Streams membership actor: live members and unexpired or
pending transactional offsets prevent cleanup. After all offsets expire, cleanup
retries every sweep, including when the stored epoch equals the current epoch. Failed deletes retain uncertainty; an
explicit DeleteGroups failure preserves the group and reports
`GROUP_DELETION_FAILED` with a generic per-group message on DeleteGroups v3;
legacy versions 0–2 return `UNKNOWN_SERVER_ERROR`. Backend error details
are not disclosed to clients.

Released baseline:
[Kafka 4.4 manager](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/streams/StreamsGroupTopologyDescriptionManager.java),
[4.4 error codes](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java).

Regression evidence lives in `streams/actor/description/tests.rs`,
`streams/description/file_backend.rs`, and the existing no-plugin wire tests.
The bounded released Kafka 4.4 durable-backend, minimum-feature and RF3
shared-backend movement runs are indexed in
[the qualification ledger](../tools/kafka44/COVERAGE.md). They retain their
actual historical source/binary identities, not fresh execution at every head.
Cleanup uses the existing offset-retention sweep. Kafka's broker-wide cleanup
worker uses the same offsets-retention-check interval. The native periodic-runner
regression below covers a graceful handoff and retry schedule; distributed
scheduling equivalence under coordinator movement remains unqualified. Native callback counters and duration histograms are documented below;
Kafka JMX name/aggregation equivalence remains unqualified.
Classic-protocol conversion deletes storage first; failures retain the Streams
group, throttle repeated cleanup calls, and answer `REBALANCE_IN_PROGRESS`.


## Verification boundary

`natural_cleanup_waits_for_offsets_and_retries_current_epoch_delete` drives the
production sweep with a live member, a retained offset, offset expiry, a failing
delete, and a successful retry. `filesystem_description_survives_coordinator_restart`
reopens a real backend and restores the durable Streams seed into a new coordinator.
The Stateright model under `actor/description/tests/model.rs` replays 2,801
schedules of up to four operations through real actors: member departure,
retained/expired offsets, failed delete and repair, coordinator reconstruction,
and delayed stale pushes. It checks safe retention and progress on every healthy
eligible sweep. Bounds are exhaustive within that operation window, not an
unbounded liveness or distributed-network proof. Offset persistence is seeded
from a known committed fixture; replication and transaction-marker recovery are
outside this model and require their own integration/Jepsen evidence.

Production cleanup admission and store-result epoch selection call
`krabka_verified::streams_cleanup`. Its Creusot contracts have no preconditions:
cleanup requires empty membership, expired offsets and initialized state; failed
stores remain UNCERTAIN and permanent failures record the rejected epoch.
Storage calls, ownership fencing, atomicity of writes and scheduling fairness are
host obligations, not consequences of these pure contracts. Proof discharge status
must be checked separately from the presence of annotations.


Actual matching-toolchain discharge completed: both `vc_may_reclaim` and
`vc_finish_store` prove with Creusot0.14/Alt-Ergo. Receipts are under
`verif/krabka_verified_rlib/streams_cleanup/`; the qualification artifact records
the pinned image and exact invocation. Copied-source mutations with unchanged
contracts (wrong failed-store sentinel and off-by-one replica limit) are rejected.
The directed transaction-reservation regression verifies that unresolved offsets
block topology cleanup and release permits later progress. It does not claim
transaction-marker replication or LSO recovery has been qualified.


## Explicit lifecycle follow-through

Explicit DeleteGroups and classic conversion now serialize empty-group
validation, backend cleanup, durable append and retirement in the Streams actor
turn. Explicit deletion includes committed and pending offset tombstones;
conversion preserves offsets. Retired mailboxes reject queued joins and replay
retirement follows successful durable append, not uncertain failure. Coordinator
runtime survives actor replacement and is cleared only for the retiring actor's
owned instance.

The expanded actual-actor model executes 7,381 bounded histories including
explicit deletion and conversion; blocked-backend join/delete/conversion and
committed-replay regressions are retained in
[the native qualification record](../qualification/streams-topology-native.md).
These supersede the original source-audit findings and 2,801-history model above.
They do not establish arbitrary distributed interleavings or plugin IO safety;
the separate wire/restart/shared-backend movement receipts retain those runs'
actual bounds and revisions.

## Native callback telemetry

Native backend callbacks export broker-wide Prometheus metrics:

- `krabka_broker_streams_topology_storage_operations_total`
- `krabka_broker_streams_topology_storage_duration_seconds`

Both have bounded `operation="set|get|delete"` and
`result="success|failure"` labels: at most six combinations, with no group IDs,
backend messages or plugin class strings. Duration spans awaiting the returned callback future, including its IO, and uses
seconds. Synchronous future construction and metadata-log appends are excluded. A successful missing `get` is a success,
not a backend failure. Counters and histogram observations record completed
callbacks only; cancelled or panicking futures do not falsely increment success.
Rejected pushes and the reference in-memory store do not count as native IO.
These are native broker metrics, not a claim of Kafka JMX naming or aggregation
parity; use Prometheus `rate()` for callback and failure rates.

Bounded handoff regressions additionally cover an admitted `set` blocked during
queued shutdown, active-member heartbeat resumption on the replacement owner,
non-owner cleanup exclusion, and a failed delete retried by the actual periodic
retention runner on its next configured tick. The runner test uses real controller
ownership metadata and virtual Tokio time, not a distributed-clock/fairness proof.
Graceful shutdown drains admitted writes before replacement replay; arbitrary
concurrent owners and stale backend writes still require host fencing. These
native regressions do not replace an RF3 released-broker concurrent-write
wire differential or establish filesystem power-loss guarantees.

## Isolated Java embedding adapter

A separate opt-in adapter is documented in [Java extensions](compatibility/kafka-4.4-jvm-extensions.md).
The native SPI and CLI class-name boundary described above remain unchanged.
