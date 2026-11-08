# Native versus JVM extension compatibility assessment (2026-11-08)

## Subsequent implementation

The isolated assignor prototype is now implemented as an opt-in broker adapter:
[`tools/jvm-assignor/README.md`](../../tools/jvm-assignor/README.md),
[`streams/jvm_assignor.rs`](../../crates/broker/src/coordinator/unified/streams/jvm_assignor.rs).
It executes released Java/JAR assignors via nonblocking async jobs and consumes
only exact-input-matching results through the existing actor rollback/retry path.
This supersedes the assessment's assignor-only "no worker" statement below;
topology-storage and DLQ Java adapters are still absent. The original assessment
is retained to explain the native/JVM boundary, not as the latest support matrix.
See [implementation receipt](../jvm-assignor-bridge-20261108/receipt.json).

## Verified architecture and boundary (pre-bridge assessment)

This is a source-and-released-signature assessment, not a JVM loader implementation.

| Surface | Existing native contract | JVM gap |
| --- | --- | --- |
| Streams assignor | [`TaskAssignor`](../../crates/broker/src/coordinator/unified/streams/assignor.rs), ordered Arc registry, synchronous `assign` returning native task maps | Java GroupSpec/TopologyDescriber/GroupAssignment object conversion, class loading and exception translation; no Java ABI is accepted |
| Topology storage | [`TopologyDescriptionBackend`](../../crates/broker/src/coordinator/unified/streams/description/backend.rs), borrowed Rust Future, explicit transient/permanent failures, resources owned by Drop | CompletableFuture completion/cancellation and Java object ownership; see retained released javap signatures |
| DLQ manager | [`DlqManagerFactory`/`DlqSink`](../../crates/broker/src/share_partition/dlq.rs), per-broker create, async write, explicit async stop/drain | Java manager construction, lifecycle and asynchronous enqueue adapter are separate implementation work |

`crates/broker/Cargo.toml` and root Cargo.toml have no JNI/JVM loader dependency.
The inspected native SPI declarations explicitly deny JVM-loader/stable-library
ABI support. The fixture Java implementations are loaded by the **released Java
broker**, not by native. KIP-1357 name reporting is not class-loading support.
`released-signatures.txt` was generated with javap against pinned Kafka4.4.0 jars
from the previously retained qualification image extraction. It is primary local
interface evidence; no web-based version inference is used.

## Concrete blocker and bounded next step

Arbitrary existing JAR compatibility cannot be achieved by mapping a config key
or adding a registry alias: it requires a JVM runtime or an isolated JVM worker,
a versioned representation for the Java-only input/output types, lifecycle and
exception mapping, bounded callback execution, cancellation/stop semantics, and
distributed deployment of the same plugin implementation. The synchronous native
assignor's documented non-blocking/no-panic constraint means a blocking worker
RPC cannot simply be placed inside `assign` without revisiting execution design.
This is an architectural feasibility blocker for a small parity patch, **not**
a claim that a bridge is impossible. No arbitrary loader is added in this lane.

A scoped, actionable improvement is to prototype one **opt-in isolated assignor
bridge** using a fixed Kafka4.4 TaskAssignor artifact, first/last/failure fixtures,
and explicit versioned GroupSpec/TopologyDescriber conversion. Before production
use, acceptance must include worker death/hang, malformed output validation,
unknown-member divergence, broker restart and lifecycle shutdown. Storage and
DLQ bridges should remain separately scoped due to async IO/cancellation needs.
Choosing and supporting such a bridge versus explicitly supporting only native
Rust plugins is a product/architecture decision; this assessment does not make
that decision or expand the current support claim.

## Completed compatibility evidence improvement

The current lane adds real active-member RF3 callback recovery and combined
ACL/callback fault qualification. It also records a concrete malformed-output
**divergence**: Kafka4.4 accepts an unknown owner while native safely rejects it.
See sibling assignor receipts dated20261108. These results strengthen behavioral
compatibility evidence without misrepresenting JVM/JAR ABI compatibility.
