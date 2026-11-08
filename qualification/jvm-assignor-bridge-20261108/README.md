# Released JVM/JAR assignor bridge qualification (2026-11-08)

Implementation: [`tools/jvm-assignor/README.md`](../../tools/jvm-assignor/README.md).
Local retained base: `5dd04e2ef`. No publication, merge or production change.

## Architecture and scope

- A compiled Java worker loads an actual supplied JAR implementing the released
  Kafka 4.4 `TaskAssignor` interface; no native alias substitutes for Java code.
- The broker's opt-in `JvmTaskAssignor` registers through the existing registry,
  serializes immutable resolved inputs and dispatches an async bounded worker.
  The actor never blocks on JVM IO. While pending, existing assignor-error
  rollback preserves the prior target and the client retries its heartbeat.
- Result consumption is keyed by the entire assignor input: member IDs/process
  IDs/current role maps/offsets, resolved topology tasks/statefulness and standby
  count. Changed inputs cannot consume obsolete completions. No async task can
  mutate an actor/epoch; only the current target installer consumes a matching
  result and applies the normal validation/rollback rules.
- Work is bounded by deadlines, 32 outstanding/cached inputs, per-stream/input
  1 MiB limits and a 128 MiB JVM heap. Drop/expiry cancels pending jobs; child
  kill-on-drop prevents a hung process surviving cancellation. Java plugin
  lifecycle uses a fresh instance and closes `AutoCloseable` implementations.

## Evidence

[`test-output.txt`](test-output.txt) records six Python test methods: real fixture
JAR first/last owners, released StickyTaskAssignor, precise i64/process conversion,
throw/death/unknown-owner/flood/close-failure, bad protocol version, deadline and
explicit cancellation with observed process reaping.

Broker tests are recorded separately in [`broker-test-output.txt`](broker-test-output.txt).
**Subsequent configured combined-tree check: 300 Streams tests pass, none fail.**
[`streams-recheck-output.txt`](streams-recheck-output.txt) includes the released-JAR
first-join, offsets persistence, membership fencing and lifecycle tests.
The first join now uses a stable member-scoped fallback process identity when
`process_id` is omitted. Recreating a rejected actor therefore submits the same
immutable input and can consume its JVM result; explicit process IDs still win.
No admission state is retained after a rejected assignment, and normal rollback
remains unchanged. This fixes the livelock without introducing a pending-state
protocol or persisting a partially admitted member.

The original 4-pass/1-failure output above remains historical evidence.
[`broker-recheck-output.txt`](broker-recheck-output.txt) additionally retains an
initial setup mistake (missing classpath); the complete configured suite passed
subsequently. The six Python fixture methods also passed again in
[`python-recheck-output.txt`](python-recheck-output.txt).

The passing checks cover:
production actor target installation and membership-change fencing, actual actor
heartbeat plus offsets-log commit, class-load rollback, input-key coverage, and
Linux deadline/drop child-process reaping. Source and binary identities are in
[`receipt.json`](receipt.json).

## Limits

This is opt-in programmatic registration for pure deterministic Java assignors,
not universal JVM extension parity. Pending work currently surfaces the existing
retryable assignment error, not a new async-assignment protocol status. There is
no automatic classpath distribution or config-string class discovery. Optional
rack/instance/client tags and end offsets unavailable in the native SPI remain
empty. Identical pure inputs may reuse a result across epochs; changed inputs
never do. Cached failures expire after 60 seconds, not automatic immediate retry.

Topology-description and DLQ-manager Java adapters are **not implemented**.
Those asynchronous, stateful plugin contracts require persistent-worker resource
ownership and uncertainty/cancellation mapping; this assignor-only bridge must
not be presented as their implementation. No full broker suite or broad Clippy
run is claimed.
