//! Opt-in, trusted Kafka 4.4 Java storage and custom DLQ extension adapters.
//!
//! A persistent process owns plugin state. Cancellation or transport failure
//! permanently fences the session; acknowledged in-memory state is never
//! silently discarded by an automatic worker restart.
use {
    crate::{
        coordinator::unified::streams::description::{
            BackendFuture, StorageFailure, TopologyDescription, TopologyDescriptionBackend,
        },
        share_partition::{
            DlqError, DlqManagerContext, DlqManagerFactory, DlqRequest, DlqSink, state::DlqCause,
        },
    },
    async_trait::async_trait,
    serde_json::{Value, json},
    std::{
        path::PathBuf,
        process::Stdio,
        sync::{Arc, Mutex as StdMutex},
        time::Duration,
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        process::{Child, Command},
        runtime::Handle,
        sync::Mutex,
        task::JoinHandle,
        time::timeout,
    },
};
const LIMIT: usize = 1024 * 1024;

/// Worker classes, released Kafka libraries and trusted plugin JARs must be
/// deployed identically wherever the embedding application uses this adapter.
#[derive(Clone, Debug)]
pub struct JvmExtensionOptions {
    pub java: PathBuf,
    pub classpath: String,
    pub class_name: String,
    pub timeout: Duration,
}
type Reapers = Arc<StdMutex<Vec<JoinHandle<()>>>>;
#[derive(Debug)]
struct ManagedChild {
    child: Option<Child>,
    reapers: Reapers,
}
impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            // Retain the waiter so explicit shutdown can observe process reaping.
            let Ok(runtime) = Handle::try_current() else {
                return;
            };
            let handle = runtime.spawn(async move {
                let _ = child.wait().await;
            });
            if let Ok(mut reapers) = self.reapers.lock() {
                reapers.push(handle);
            }
        }
    }
}
#[derive(Debug, Default)]
struct State {
    started: bool,
    child: Option<ManagedChild>,
}
#[derive(Debug)]
struct Worker {
    options: JvmExtensionOptions,
    kind: &'static str,
    reapers: Reapers,
    state: Mutex<State>,
}
impl Worker {
    fn new(options: JvmExtensionOptions, kind: &'static str) -> Result<Self, String> {
        if options.classpath.is_empty()
            || options.class_name.is_empty()
            || options.timeout.is_zero()
        {
            return Err("Java extension requires classpath, class and a nonzero timeout".into());
        }
        Ok(Self {
            options,
            kind,
            reapers: Arc::default(),
            state: Mutex::new(State::default()),
        })
    }

    async fn shutdown(&self) -> Result<(), String> {
        let result = self.call(json!({"op":"stop"})).await;
        let handles = self
            .reapers
            .lock()
            .map_err(|_| "Java cleanup lock poisoned")?
            .drain(..)
            .collect::<Vec<_>>();
        for handle in handles {
            timeout(self.options.timeout, handle)
                .await
                .map_err(|_| "Java process reap deadline exceeded")?
                .map_err(|_| "Java process reap failed")?;
        }
        result.map(|_| ())
    }

    async fn call(&self, mut request: Value) -> Result<Value, String> {
        request
            .as_object_mut()
            .ok_or("Java request must be an object")?
            .insert("version".into(), json!(1));
        let mut bytes = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
        if bytes.len() > LIMIT {
            return Err("Java extension request too large".into());
        }
        bytes.push(b'\n');
        timeout(self.options.timeout, async {
            let mut state = self.state.lock().await;
            let mut child = if let Some(child) = state.child.take() {
                child
            } else {
                if state.started {
                    return Err(
                        "Java extension session fenced; explicit replacement required".into(),
                    );
                }
                state.started = true;
                let child = Command::new(&self.options.java)
                    .args([
                        "-Xmx128m",
                        "-cp",
                        &self.options.classpath,
                        "ExtensionWorker",
                        self.kind,
                        &self.options.class_name,
                    ])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|e| e.to_string())?;
                ManagedChild {
                    child: Some(child),
                    reapers: Arc::clone(&self.reapers),
                }
            };
            // Taking the child out of state is intentional: cancellation drops
            // and kills it instead of allowing another request to read stale IO.
            child
                .child
                .as_mut()
                .ok_or("missing Java child")?
                .stdin
                .as_mut()
                .ok_or("missing Java stdin")?
                .write_all(&bytes)
                .await
                .map_err(|e| e.to_string())?;
            let stdout = child
                .child
                .as_mut()
                .ok_or("missing Java child")?
                .stdout
                .as_mut()
                .ok_or("missing Java stdout")?;
            let mut response = Vec::new();
            loop {
                let byte = stdout.read_u8().await.map_err(|e| e.to_string())?;
                if byte == b'\n' {
                    break;
                }
                if response.len() == LIMIT {
                    return Err("Java extension response too large".into());
                }
                response.push(byte);
            }
            let response: Value = serde_json::from_slice(&response).map_err(|e| e.to_string())?;
            if request.get("op").and_then(Value::as_str) == Some("stop") {
                if !child
                    .child
                    .as_mut()
                    .ok_or("missing Java child")?
                    .wait()
                    .await
                    .map_err(|e| e.to_string())?
                    .success()
                {
                    return Err("Java extension close failed".into());
                }
            } else {
                state.child = Some(child);
            }
            drop(state);
            if response.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(
                    if response.get("permanent").and_then(Value::as_bool) == Some(true) {
                        "Java extension permanent rejection"
                    } else {
                        "Java extension callback failed"
                    }
                    .into(),
                );
            }
            Ok(response)
        })
        .await
        .map_err(|_| "Java extension deadline exceeded".to_owned())?
    }
}

fn storage_failure(error: &str) -> StorageFailure {
    if error == "Java extension permanent rejection" {
        StorageFailure::Permanent
    } else {
        StorageFailure::Transient
    }
}

/// One persistent configured `StreamsGroupTopologyDescriptionPlugin` instance.
///
/// Explicit close is available; Drop kills the worker if graceful close cannot
/// run. Durability and cross-broker availability remain the plugin's contract.
#[derive(Debug)]
pub struct JvmTopologyDescriptionBackend {
    worker: Worker,
}
impl JvmTopologyDescriptionBackend {
    /// Start and configure a plugin before exposing the backend to a broker.
    /// # Errors
    /// Class loading, configuration and bounded process IO may fail.
    pub async fn new(options: JvmExtensionOptions, config: Value) -> Result<Self, String> {
        if !config.is_object() {
            return Err("plugin config must be an object".into());
        }
        let backend = Self {
            worker: Worker::new(options, "storage")?,
        };
        backend
            .worker
            .call(json!({"op":"configure", "config":config}))
            .await?;
        Ok(backend)
    }

    /// Gracefully close the Java plugin, bounded by the configured deadline.
    /// # Errors
    /// Returns an error on callback failure, process death or deadline.
    pub async fn close(&self) -> Result<(), String> { self.worker.shutdown().await }
}
impl TopologyDescriptionBackend for JvmTopologyDescriptionBackend {
    fn set<'a>(
        &'a self,
        group_id: &'a str,
        epoch: i32,
        description: &'a TopologyDescription,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.worker
                .call(json!({"op":"set","group":group_id,"epoch":epoch,"description":description}))
                .await
                .map(|_| ())
                .map_err(|error| storage_failure(&error))
        })
    }

    fn get<'a>(
        &'a self,
        group_id: &'a str,
        epoch: i32,
    ) -> BackendFuture<'a, Option<TopologyDescription>> {
        Box::pin(async move {
            let value = self
                .worker
                .call(json!({"op":"get","group":group_id,"epoch":epoch}))
                .await
                .map_err(|error| storage_failure(&error))?;
            serde_json::from_value(
                value
                    .get("description")
                    .cloned()
                    .ok_or(StorageFailure::Transient)?,
            )
            .map_err(|_| StorageFailure::Transient)
        })
    }

    fn delete<'a>(&'a self, group_id: &'a str) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.worker
                .call(json!({"op":"delete","group":group_id}))
                .await
                .map(|_| ())
                .map_err(|error| storage_failure(&error))
        })
    }
}

/// Adapter for custom Java `ShareGroupDLQManager` implementations.
///
/// Requires a public no-argument constructor. Java's stock manager requires in-process broker
/// services and is not supported by this isolated constructor contract.
#[derive(Debug)]
pub struct JvmDlqManagerFactory {
    prepared: StdMutex<Option<Worker>>,
}
impl JvmDlqManagerFactory {
    /// Start and initialize one independent Java manager before registration.
    /// Make one factory per broker start; a factory cannot be reused.
    /// # Errors
    /// Invalid settings, class loading or constructor failures abort preparation.
    pub async fn new(options: JvmExtensionOptions) -> Result<Self, String> {
        let worker = Worker::new(options, "dlq")?;
        worker.call(json!({"op":"initialize"})).await?;
        Ok(Self {
            prepared: StdMutex::new(Some(worker)),
        })
    }
}
impl DlqManagerFactory for JvmDlqManagerFactory {
    fn create(&self, _context: DlqManagerContext) -> Result<Arc<dyn DlqSink>, DlqError> {
        let worker = self
            .prepared
            .lock()
            .map_err(|_| DlqError::Config("Java manager factory poisoned".into()))?
            .take()
            .ok_or_else(|| {
                DlqError::Config(
                    "Java manager factory already consumed; prepare one per broker".into(),
                )
            })?;
        Ok(Arc::new(JvmDlqSink { worker }))
    }
}
#[derive(Debug)]
struct JvmDlqSink {
    worker: Worker,
}
#[async_trait]
impl DlqSink for JvmDlqSink {
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
        self.worker.call(json!({"op":"enqueue", "group":request.group, "topic_id":request.topic_id.to_string(), "partition":request.source_partition, "first":request.first.0, "last":request.last.0, "delivery_count":request.delivery_count, "cause": match request.cause { DlqCause::ClientReject => "rejected", DlqCause::DeliveryCountExceeded => "exceeded" }})).await.map(|_| ()).map_err(DlqError::Write)
    }

    async fn stop(&self) { let _ = self.worker.shutdown().await; }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn options(class_name: &str) -> JvmExtensionOptions {
        JvmExtensionOptions {
            java: "java".into(),
            classpath: std::env::var("KAFKA44_JVM_CLASSPATH")
                .expect("worker + Kafka libs + fixture JAR"),
            class_name: class_name.into(),
            timeout: Duration::from_secs(5),
        }
    }
    #[tokio::test]
    #[ignore = "requires released Kafka 4.4 jars and compiled ExtensionWorker"]
    async fn jvm_extensions_storage_roundtrip_close_and_fence() {
        let backend = JvmTopologyDescriptionBackend::new(
            options("org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"),
            json!({}),
        )
        .await
        .unwrap();
        let description = TopologyDescription {
            subtopologies: vec![],
            global_stores: vec![],
        };
        assert_eq!(backend.get("g", 2).await.unwrap(), None);
        backend.set("g", 2, &description).await.unwrap();
        assert_eq!(backend.get("g", 2).await.unwrap(), Some(description));
        assert_eq!(backend.get("g", 3).await.unwrap(), None);
        backend.delete("g").await.unwrap();
        assert_eq!(backend.get("g", 2).await.unwrap(), None);
        backend.close().await.unwrap();
        assert_eq!(backend.get("g", 2).await, Err(StorageFailure::Transient));
    }
    #[tokio::test]
    #[ignore = "requires released Kafka 4.4 jars and compiled ExtensionFixture"]
    async fn jvm_extensions_permanent_rejection_and_constructor_failure() {
        let backend = JvmTopologyDescriptionBackend::new(
            options("ExtensionFixture$PermanentStorageFixture"),
            json!({}),
        )
        .await
        .unwrap();
        let description = TopologyDescription {
            subtopologies: vec![],
            global_stores: vec![],
        };
        assert_eq!(
            backend.set("g", 1, &description).await,
            Err(StorageFailure::Permanent)
        );
        backend.close().await.unwrap();
        assert!(
            JvmDlqManagerFactory::new(options("missing.class.Name"))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    #[ignore = "requires released Kafka 4.4 jars and compiled ExtensionFixture"]
    async fn jvm_extensions_close_failure_and_deadline() {
        let worker = Worker::new(options("ExtensionFixture"), "dlq").unwrap();
        worker.call(json!({"op":"initialize"})).await.unwrap();
        assert!(worker.call(json!({"op":"stop"})).await.is_err());
        let mut opts = options("ExtensionFixture");
        opts.timeout = Duration::from_millis(200);
        let worker = Worker::new(opts, "dlq").unwrap();
        let outcome = worker.call(json!({"op":"enqueue", "group":"hang", "topic_id":"00112233-4455-6677-8899-aabbccddeeff", "partition":3, "first":0, "last":1, "delivery_count":1, "cause":"rejected"})).await;
        assert!(outcome.unwrap_err().contains("deadline"));
        assert!(
            worker
                .call(json!({"op":"initialize"}))
                .await
                .unwrap_err()
                .contains("fenced")
        );
    }
    fn request(cause: DlqCause) -> DlqRequest {
        DlqRequest {
            group: "g".into(),
            topic_id: "00112233-4455-6677-8899-aabbccddeeff".parse().unwrap(),
            source_partition: 3,
            first: krabka_log::Offset(9_007_199_254_740_993),
            last: krabka_log::Offset(i64::MAX),
            delivery_count: 7,
            cause,
        }
    }
    #[tokio::test]
    #[ignore = "requires released Kafka 4.4 jars and compiled ExtensionFixture"]
    async fn jvm_extensions_dlq_write_stop() {
        let factory = JvmDlqManagerFactory::new(options("ExtensionFixture"))
            .await
            .unwrap();
        let context = DlqManagerContext {
            broker: krabka_metadata::NodeId(1),
            stock: Arc::new(crate::share_partition::dlq::test_support::RecordingDlq::default()),
            metrics: crate::metrics::BrokerMetrics::new(),
        };
        let sink = factory.create(context.clone()).unwrap();
        assert!(factory.create(context).is_err());
        sink.write(request(DlqCause::ClientReject)).await.unwrap();
        sink.write(request(DlqCause::DeliveryCountExceeded))
            .await
            .unwrap();
        sink.stop().await;
        assert!(sink.write(request(DlqCause::ClientReject)).await.is_err());
    }
    #[tokio::test]
    #[ignore = "requires released Kafka 4.4 jars and compiled ExtensionFixture"]
    async fn jvm_extensions_cancel_fences_and_reaps_worker() {
        let worker = Worker::new(options("ExtensionFixture"), "dlq").unwrap();
        // A completed initialization request starts the process without enqueue.
        assert!(worker.call(json!({"op":"invalid"})).await.is_err());
        let pid = worker
            .state
            .lock()
            .await
            .child
            .as_ref()
            .unwrap()
            .child
            .as_ref()
            .unwrap()
            .id()
            .unwrap();
        let mut r = serde_json::to_value(json!({"op":"enqueue", "group":"hang", "topic_id":"00112233-4455-6677-8899-aabbccddeeff", "partition":3, "first":0, "last":1, "delivery_count":1, "cause":"rejected"})).unwrap();
        assert!(
            timeout(Duration::from_millis(50), worker.call(r.take()))
                .await
                .is_err()
        );
        assert!(
            worker
                .call(json!({"op":"invalid"}))
                .await
                .unwrap_err()
                .contains("fenced")
        );
        assert!(worker.shutdown().await.is_err()); // session fenced, cleanup still awaited
        #[cfg(target_os = "linux")]
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}
