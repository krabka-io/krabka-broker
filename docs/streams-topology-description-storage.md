# Streams topology-description storage

Krabka serves KIP-1331 behind `unstable.api.versions.enable`. No plugin is
configured by default: heartbeats never request a description, updates return
`UNSUPPORTED_VERSION`, and included descriptions report `NOT_STORED`.

## Native extension contract

The Kafka configuration value
`org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin` selects
Krabka's actor-local memory implementation. It loses descriptions on process
restart or coordinator movement, just like Kafka's reference plugin.

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
beyond the reference memory store. It stores JSON descriptions with their epoch,
uses an injective hex encoding of group IDs (no path traversal), and writes via a
synced staging file and rename. Reopening the same directory retains data.
Operators own directory access permissions, disk capacity, and backups. Filename
length is bounded by the filesystem; long group IDs can fail storage. The format
is private and not Kafka's JVM serialization format. Process restart is covered;
power-loss durability and non-POSIX rename semantics are not qualified.

A local disk directory does not follow a coordinator to another machine. Use a
shared directory with atomic rename semantics, or implement a distributed native
backend. Do not point unrelated Kafka clusters at the same directory.

## Lifecycle and evidence

The actor persists stored epoch `UNCERTAIN` (-2) **before** mutation and the
successful epoch **after** it. Permanent store failure records the failed epoch;
transient failure retains solicitation backoff. Heartbeat backoff starts at 30
seconds, doubles to one hour and has 20% jitter. Periodic actor session checks
retry uncertain or stale-epoch deletes. Failed deletes retain uncertainty; an
explicit DeleteGroups failure preserves the group and reports
`GROUP_DELETION_FAILED` with a generic per-group message. Backend error details
are not disclosed to clients.

Released baseline:
[Kafka 4.4 manager](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/streams/StreamsGroupTopologyDescriptionManager.java),
[4.4 error codes](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java).

Regression evidence lives in `streams/actor/description/tests.rs`,
`streams/description/file_backend.rs`, and the existing no-plugin wire tests.
This is not a claim that a released Kafka 4.4 interoperability suite has run.
Cleanup uses actor session checks rather than Kafka's broker-wide cleanup worker;
native plugin metrics and natural offset-expiry cleanup remain unqualified.
Classic-protocol conversion deletes storage first; failures retain the Streams
group, throttle repeated cleanup calls, and answer `REBALANCE_IN_PROGRESS`.
