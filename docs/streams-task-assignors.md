# Native Streams task assignors

Kafka 4.4 supports an ordered broker-side `TaskAssignor` registry and group selection via `streams.assignor.name`. The first configured implementation is the default; KIP-1357 only adds reporting of that selection. See the released [GroupCoordinatorConfig](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java#L328-L340) and [registration validation](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java#L819-L876).

Krabka embedders can implement `coordinator::unified::streams::assignor::TaskAssignor` and build a mutable `StreamsGroupConfig`, and register each `Arc<dyn TaskAssignor>` with `streams.task_assignors.register(...)`. Set `BrokerConfig.streams_group = Arc::new(streams)` before starting the broker. The first registered custom implementation becomes the default; group configuration can select another registered name. Duplicate names and the reserved built-in `sticky` name fail registration. New unknown group selections fail config validation; if a previously stored selection is unavailable on a node, that node uses its broker default without discarding unrelated overrides, matching the released coordinator. With no extensions registered, the existing sticky algorithm remains the default. When not registered as custom implementations, legacy `auto`, `highly_available` and `highly-available` settings are aliases for that algorithm. A registered custom name takes precedence over these aliases, matching released Kafka's reservation of only the actual built-in `sticky` name.

The extension receives resolved tasks, stateful subtopologies, standby count and current member/task state. It returns active and standby target maps. Implementations must be deterministic, non-blocking, non-panicking and return valid assignments. Unknown members/tasks, missing or duplicated active tasks, and process-collocated copies are rejected before installing a target. An assignment error retains the old target and leaves the group dirty for retry; it never silently switches algorithms. Selection changes take effect during the next assignment, through the existing revocation/reconciliation path. Describe v1 reports the selected implementation name.

This API is a native Rust embedding interface, not a stable dynamic-library ABI. The stock executable does not load arbitrary JVM classes, native shared libraries or external processes. Kafka's Java `Configurable`/`AutoCloseable` lifecycle and arbitrary JVM plugins are not compatible with this mechanism. Native extension lifetime is owned by `Arc`; startup configuration belongs to the embedding application. Assignment offloading and panic isolation are not provided. Cross-broker deployments must register identical names and behavior on every broker before storing a group override. A bounded actual-broker [custom-callback wire differential](../tools/kafka44/STREAMS_ASSIGNOR_WIRE.md) now executes equivalent released-JVM and native implementations; arbitrary JVM-plugin ABI/lifecycle compatibility and the remaining distributed/authorization combinations are not claimed.


Verification: the registry Stateright test exhausts all 36 combinations of three
selections (including a failing implementation), four member sets and three task
universes. Every transition executes the real registry and assignment validator,
and checks exact selected placement or rejection. Existing actor tests cover
cooperative reassignment and retaining the previous target on failure. The native
validator calls `krabka_verified::streams_assignment::next_standby_count`, whose
precondition-free Creusot contract specifies exact admission up to the configured
replica count and rejects negative counters without overflow. Proof discharge is
separate from annotations; host member/task identity and distributed coordinator
ownership are not proved by that arithmetic kernel or this bounded model.


A component probe against the pinned released Kafka 4.4.0 JARs confirms the
ordered registry, first-entry default, distinct real custom assignment execution,
duplicate/reserved-name rejection, and unknown/wrong-class errors. It also shows
that Kafka accepts a custom `auto` name; the native registry therefore permits
that name ahead of its legacy alias. Kafka also accepts a blank plugin-provided name; the native registry accepts
that name and tests its actual execution rather than silently choosing sticky.
Names are bytewise unique. Group override values use Kafka STRING trimming;
registration itself does not rewrite plugin-provided names. These measurements
do not claim arbitrary JVM-plugin compatibility.
