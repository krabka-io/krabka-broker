# Opt-in isolated Kafka 4.4 Java extensions

Embedding applications can install the asynchronous adapters in
`krabka_broker::jvm_extensions` for trusted Java plugins. This is not CLI
class-name discovery, a security sandbox, distributed plugin deployment or
universal JVM broker compatibility. Compile `ExtensionWorker.java` using
`tools/jvm-assignor/check.sh` and deploy worker classes, pinned Kafka 4.4 libraries
and plugin JARs on every host that needs them.

```rust,ignore
use std::{sync::Arc, time::Duration};
use krabka_broker::jvm_extensions::{
    JvmExtensionOptions, JvmTopologyDescriptionBackend, JvmDlqManagerFactory,
};
let options = JvmExtensionOptions {
    java: "java".into(),
    classpath: "/opt/worker:/opt/kafka/libs/*:/opt/plugins/storage.jar".into(),
    class_name: "example.SharedDurableTopologyPlugin".into(),
    timeout: Duration::from_secs(5),
};
let storage = Arc::new(JvmTopologyDescriptionBackend::new(
    options.clone(), serde_json::json!({"cluster": "example"}),
).await?);
// streams_config.topology_description_backend = Some(storage.clone());
// Prepare a different classpath/class for a custom standalone DLQ implementation:
// broker_config.share_dlq_manager_factory = Some(Arc::new(
//     JvmDlqManagerFactory::new(dlq_options).await?,
// ));
// After draining the coordinator, before dropping the embedding runtime:
storage.close().await?;
```

## Topology storage

Call `JvmTopologyDescriptionBackend::new(options, config).await` and install its
`Arc` in `StreamsGroupConfig::topology_description_backend`. Construction loads
and configures one persistent Java `StreamsGroupTopologyDescriptionPlugin`.
Set/get/delete await its CompletableFuture. Subtopologies, all three node kinds,
nullable sink topics, successors, source topics, stores and global stores are
converted in both directions. A permanent Java topology exception maps to native
`StorageFailure::Permanent`; other failures are transient. Missing descriptions
are `None`. Epoch lookup semantics and durability belong to the plugin.

Call `close().await` for graceful plugin shutdown. The native storage SPI has no
async close callback; dropping without explicit close kills the worker instead.
The Java in-memory plugin is broker-local and loses state after process death;
it is not a shared or restart-persistent backend.

## Custom DLQ managers

Prepare `JvmDlqManagerFactory::new(options).await` before broker startup, then
register its `Arc` in `BrokerConfig::share_dlq_manager_factory`. Each factory is
one-shot: prepare a separate factory for each broker lifecycle. Construction
loads a `ShareGroupDLQManager` with a public no-argument constructor before the
factory can be registered; reuse fails startup instead of falling back.

Enqueue carries group, topic UUID, partition, inclusive signed-64-bit offset
range, delivery count and the exact released cause constants. The native SPI
has no source topic name; `TopicIdPartition.topic()` is null. Plugins requiring
a topic name must resolve it themselves from the UUID. `stop` waits for the Java
callback and process termination; protocol and cleanup waits are each bounded
by the configured deadline; the native stop interface
cannot report close errors. Java plugin side effects and retries must be
idempotent: cancellation is uncertain delivery and source recovery may replay.

The stock Java DefaultShareGroupDLQManager is **not** constructible here: its
factory requires an in-process KafkaClient, metadata helper, timer, metrics and
LogReader. Custom managers must own their IO/resources. The existing native
stock writer remains the default and is not replaced by a Java no-op.

## Resource and failure contract

One persistent process and serial channel per plugin; no shell. IO is bounded to
1 MiB per request/response and heap to 128 MiB. Plugin stdout is redirected away
from the protocol; stderr is discarded. Timeouts include channel contention.
Cancellation or timeout while waiting for the serial channel only fails that
waiting call. Cancellation/deadline during process IO, a malformed/oversized
reply or worker death kills the process and permanently fences that session; it never silently
restarts and discards acknowledged plugin state. Explicit replacement/recovery
is the embedder's responsibility. Drop immediately requests worker termination and retains asynchronous process
waiters; explicit storage close and DLQ stop drain those waiters even after
a cancelled/fenced call. Drop outside a live Tokio runtime still requests kill
but cannot await cleanup; close before shutting down the embedding runtime. A deadline kill cannot guarantee plugin close or undo completed IO.

[Qualification](../../qualification/jvm-extensions-20261108/README.md) records
real released-API tests, constructor/future/close failures and cancellation.
