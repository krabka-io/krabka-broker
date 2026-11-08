# Native share DLQ managers

Scope: [#1982](https://github.com/krabka-io/krabka-broker/issues/1982), following
[#1974](https://github.com/krabka-io/krabka-broker/issues/1974).

Rust embeddings register `BrokerConfig::share_dlq_manager_factory`. Each broker
start constructs an independent manager; `None` selects the stock writer. This
is a supported native counterpart for Kafka 4.4's enqueue/stop contract, **not**
JVM class/JAR loading, a dynamic-library ABI, or a class-name loader in the CLI.
The public types are re-exported from `krabka_broker::share_partition`.

```rust
use std::sync::Arc;
use async_trait::async_trait;
use krabka_broker::share_partition::{
    DlqError, DlqManagerContext, DlqManagerFactory, DlqRequest, DlqSink,
};
#[derive(Debug)]
struct Factory;
struct Manager { stock: Arc<dyn DlqSink> }
impl DlqManagerFactory for Factory {
    fn create(&self, context: DlqManagerContext) -> Result<Arc<dyn DlqSink>, DlqError> {
        Ok(Arc::new(Manager { stock: context.stock }))
    }
}
#[async_trait]
impl DlqSink for Manager {
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
        self.stock.write(request).await
    }
    async fn stop(&self) { self.stock.stop().await; }
}
// Before Broker::start(config):
// config.share_dlq_manager_factory = Some(Arc::new(Factory));
```

## Contract

A factory receives broker identity, a configured stock writer and standard
per-group metrics. Its error fails startup without silent fallback. Carry
validated native settings in the factory; none are serialized into broker
metadata. Stock delegation preserves source resolution, validation, routing,
copying, retries and metrics, including remote/diskless readers attached later
in startup. A replacement backend owns its source access and IO.

`write` receives an inclusive source range, source topic identity/partition,
group, delivery count and cause. Calls may run concurrently; do not block the
async executor. Success means durable archival. An error still archives the
source, matching released Kafka; failure can leave no DLQ copy.

The broker persists ARCHIVING before invoking the selected manager. Shutdown
stops admissions, cancels pending enqueue futures without prematurely making
their source ranges terminal, drains owned tasks and calls `stop`. The stop
callback is an owned task: cancelling a shutdown waiter does not repeat or
cancel it. Abandoned startup/dropped handles request best-effort asynchronous
cleanup; awaiting `BrokerHandle::shutdown` waits for completion. A real process
kill cannot call a callback. The stock coalescer also cancels/drains owned sends.

Implementations must stop/join their own workers in `stop`. Panics are not
isolated. A cancelled remote append has unknown outcome; recovery re-enqueues
durable ARCHIVING and may append a duplicate. Neither this API nor the stock
manager provides exactly-once archival. The host cannot prove arbitrary plugin
IO correctness or force an uncooperative callback to complete.

Stock delegation already records metrics; avoid double counting. Replacements
can call `context.metrics.record_share_dlq_produce`,
`record_share_dlq_produce_failed` and `record_share_dlq_records`. Count actual
attempts/written records, not merely enqueue calls. Install compatible factories
on every possible source leader; factory identity is broker-local, not a group
setting replicated to other nodes.

## Verification

External-crate wire tests select the registered manager for real rejects,
exercise actual stock delegation and custom refusal, and reject a factory error.
Lifecycle tests cancel a blocked enqueue while retaining replayable ARCHIVING,
refuse post-stop work, clean up abandoned startup, and cancel a shutdown waiter
without repeating `stop`. The coalescer fault test holds a real transport future
and queued work, then checks stop cancels both and submits no further requests.

[Qualification artifacts](../../qualification/kafka44-dlq/README.md) record
released/candidate boundaries, actual counters, decoded durable states and
multi-broker crash schedules. Terminal-prefix pruning remains an explicit
measured deviation. The pure Creusot decision contract and bounded acquisition
model do not prove plugin IO or replace these runtime tests.

## Isolated Java embedding adapter

A separate opt-in adapter is documented in [Java extensions](kafka-4.4-jvm-extensions.md).
The native SPI and CLI class-name boundary described above remain unchanged.
