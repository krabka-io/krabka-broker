# Isolated Kafka 4.4 assignor JAR worker (opt-in prototype)

**Broker integration remains unqualified:** first joins without an explicit
process ID livelock because pending rollback regenerates the native fallback
UUID. Four broker checks pass and the heartbeat regression fails; see
`qualification/jvm-assignor-bridge-20261108/broker-test-output.txt`. A typed
pending-admission path is needed before this adapter is considered supported.

This executes real released `TaskAssignor` implementations from a supplied JAR.
The Rust broker adapter in `streams/jvm_assignor.rs` registers through the native
registry but dispatches Java work asynchronously; it never blocks the coordinator
on a process RPC. While a job is pending the existing assignor-error rollback
path preserves the old target and returns a retryable heartbeat error. Subsequent
heartbeats consume only completed jobs with an exactly identical immutable
member/process/current-task/offset/topology/config input. Changed inputs cannot
consume stale results; equal inputs may reuse results under the deterministic
native SPI contract. There is no detached callback mutating an actor or epoch.

Programmatic broker registration:

```rust
use krabka_broker::coordinator::unified::streams::jvm_assignor::{
    JvmAssignorOptions, JvmTaskAssignor,
};
let plugin = JvmTaskAssignor::new("java-sticky".into(), JvmAssignorOptions {
    java: "java".into(),
    classpath: "/path/to/worker:/path/to/kafka/libs/*:/path/to/plugin.jar".into(),
    class_name: "org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor".into(),
    timeout: std::time::Duration::from_secs(5),
})?;
streams_config.task_assignors.register(std::sync::Arc::new(plugin))?;
streams_config.task_assignor_name = Some("java-sticky".into());
```

The adapter bounds outstanding/cached inputs to 32 and expires jobs after 60s.
Per-job deadline, byte and heap caps apply. Dropping/expiring a pending job aborts
its task and kills the JVM; Tokio reaps it. Cached failures are retained until
expiry rather than automatically retrying side effects. This bridge is for pure,
deterministic assignment plugins, not stateful services. There is no automatic
broker configuration-string class loading or plugin deployment.

## Run and verify

Requires Python 3.11+, JDK 21+ and the pinned released Kafka 4.4 distribution
libraries (including Jackson). Tested with OpenJDK 25.0.4.1.

```sh
KAFKA44_LIBS=/absolute/path/to/kafka/libs bash tools/jvm-assignor/check.sh
```

The checks compile worker classes separately from the fixture, package the fixture
as a JAR, and put only that JAR on its plugin classpath. No Maven, network, Rust
build or Docker image is needed. Generated files are ignored under `build/`.

Import `bridge.assign(classpath, plugin_class, request)` from Python and await it.
Classpath contains `build/worker`, Kafka's `libs/*` and the plugin's JAR(s).
Each call gets a fresh Java process and plugin instance. Constructor, callback,
class-loading and close exceptions produce `BridgeError`; there is no retry.
`AutoCloseable` plugins close after normal/exceptional callbacks. Deadline or
caller cancellation kills and reaps the process; a hung plugin's close cannot
be guaranteed. Successful return requires successful close and process exit.
Plugin stdout is redirected to stderr so it cannot contaminate the protocol.
Heap is capped at 128 MiB; request, stdout and stderr each have 1 MiB limits.
Output overflow kills/reaps the worker. This is process isolation, not a security
sandbox; only execute trusted plugins. No worker reuse or cross-request state.

## Version 1 conversion

Request: `version: 1`, `members` (array), `input` (object). Members carry
`member_id`, `process_id`, `current_active`, `current_standby`, `current_warmup`
(task maps `{subtopology: [partition, ...]}`), and `task_offsets` (nested
`{subtopology: {partition_string: signed_64_bit_offset}}`). `input` carries
`tasks`, `stateful` (subtopology array), and `num_standby_replicas`.

Every nonempty task universe must be contiguous from zero, because released
`TopologyDescriber` exposes partition counts, not arbitrary partition lists.
Missing instance ID/rack/client tags, end offsets and rack-aware tags are
represented as empty, matching the native input's current expressiveness; this
is not full metadata fidelity for arbitrary Java plugins. Defaults for absent
current task maps and offsets are empty. Response contains `version`, plugin
`name`, `active` and `standby` member/task maps. Host validation rejects unknown
owners, missing/duplicate active tasks, invalid partitions, invalid standby
replicas and same-process duplicate placements before exposing a result.

## Evidence and remaining scope

Six unittest methods cover first/last owner JARs, exact signed-64-bit offset
conversion, process-ID conversion, exception, worker death, unknown-owner output,
output flooding, close failure, unsupported version, deadline and explicit
cancellation with observed process reaping. The released Java StickyTaskAssignor also executes against the converted inputs.
The fixture calls every adapter
accessor. This is lane-scoped executable evidence, not full Kafka parity.

No topology-storage or DLQ-manager Java adapter, distributed
plugin deployment, restart/state reuse, production metrics or full metadata
conversion is implemented. The actor target installer and validation remain unchanged; the adapter uses
nonblocking job submission and retry instead of introducing a new actor mailbox.

## Broker integration checks

Compile worker classes with `check.sh` first, then:

```sh
export KAFKA44_JVM_CLASSPATH="$PWD/tools/jvm-assignor/build/worker:$KAFKA44_LIBS/*:$PWD/tools/jvm-assignor/build/fixture.jar"
CARGO_BUILD_JOBS=2 cargo test -p krabka-broker --lib jvm_assignor -- --include-ignored
```

The optional released-JAR tests traverse the actual Streams actor heartbeat
handle and offsets log persistence, as well as the production target installer.
They exercise first-join pending rollback/retry, committed task assignment,
changed-membership completion fencing and failed-class-load rollback. Linux
lifecycle tests use a hanging worker process to observe actual `/proc` removal
after deadlines and adapter drop; they do not need Java jars.
