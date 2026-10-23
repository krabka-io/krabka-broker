//! Kill-broker orchestration for the `failover` scenario. This module uses the
//! in-cluster `kube` API through the Job's `ServiceAccount`. That
//! `ServiceAccount` needs `pods: get,list,delete` in the target namespace, and
//! it is mounted only when a failover scenario asks for it.
//!
//! `partition0_leader` resolves the topic metadata first and then maps the
//! partition leader's broker id back to the matching `StatefulSet` pod. If that
//! metadata probe fails, the caller can fall back to the first broker pod by
//! name-sort for backwards-compatible smoke runs.

use {
    crate::{
        numeric::nonnegative_i64_to_u64,
        scenario::{FailoverSpec, Stack},
    },
    anyhow::{Context, Result, anyhow},
    chrono::Utc,
    k8s_openapi::api::core::v1::Pod,
    krabka_client_core::{
        Client as KafkaClient, ClientFrameMax, ConnectionDispatchQueueCapacity,
        security::ClientSecurity,
    },
    krabka_protocol::owned::metadata_response::MetadataResponse,
    krabka_units::convert::TimeExt,
    kube::{
        Client as KubeClient,
        api::{Api, DeleteParams, ListParams},
    },
    std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    },
    tokio::{task::JoinSet, time::Instant},
    tracing::{info, warn},
};

pub(crate) struct FaultConfig {
    pub stack: Stack,
    pub namespace: String,
    pub bootstrap: String,
    pub topic: String,
    pub security: Option<ClientSecurity>,
    pub dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub frame_max: ClientFrameMax,
}

#[derive(Debug, PartialEq)]
pub(crate) struct KillEvidence {
    pub pod: String,
    pub at_ms: u64,
}

#[derive(Debug, PartialEq)]
pub(crate) enum FaultOutcome {
    Cancelled,
    TaskFailed(String),
    ClientUnavailable(String),
    Finished {
        leader_lookup_error: Option<String>,
        deletion: Result<KillEvidence, String>,
    },
}

impl FaultOutcome {
    pub(crate) fn killed_at_ms(&self) -> Option<u64> {
        match self {
            Self::Finished {
                deletion: Ok(evidence),
                ..
            } => Some(evidence.at_ms),
            _ => None,
        }
    }

    pub(crate) fn note(&self) -> Option<String> {
        match self {
            Self::Cancelled => Some("failover: cancelled at measurement end".into()),
            Self::TaskFailed(error) => Some(format!("failover: fault task failed: {error}")),
            Self::ClientUnavailable(error) => {
                Some(format!("failover: in-cluster client unavailable: {error}"))
            }
            Self::Finished {
                leader_lookup_error,
                deletion,
            } => {
                let fallback = leader_lookup_error
                    .as_ref()
                    .map_or_else(String::new, |error| {
                        format!("leader lookup failed; using first broker pod: {error}; ")
                    });
                match deletion {
                    Ok(evidence) if leader_lookup_error.is_some() => Some(format!(
                        "failover: {fallback}killed broker {}",
                        evidence.pod
                    )),
                    Ok(_) => None,
                    Err(error) => Some(format!("failover: {fallback}broker kill failed: {error}")),
                }
            }
        }
    }
}

pub(crate) struct FaultTask {
    tasks: JoinSet<FaultOutcome>,
}

impl FaultTask {
    pub(crate) fn spawn(
        spec: &FailoverSpec,
        cfg: FaultConfig,
        started_at: Instant,
        kill_at: Arc<AtomicU64>,
    ) -> Self {
        let target_leader = spec.target == "partition0_leader";
        let deadline = started_at + spec.kill_after.to_std();
        Self::schedule(deadline, async move {
            Box::pin(execute_fault(
                target_leader,
                try_client(),
                partition0_leader_from_metadata(
                    &cfg.bootstrap,
                    &cfg.topic,
                    cfg.security,
                    cfg.dispatch_queue_capacity,
                    cfg.frame_max,
                ),
                |client, leader| async move {
                    kill_broker_pod(&client, cfg.stack, &cfg.namespace, leader).await
                },
                kill_at,
            ))
            .await
        })
    }

    fn schedule(
        deadline: Instant,
        operation: impl Future<Output = FaultOutcome> + Send + 'static,
    ) -> Self {
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            tokio::time::sleep_until(deadline).await;
            operation.await
        });
        Self { tasks }
    }

    pub(crate) async fn stop(mut self) -> FaultOutcome {
        // Keep completed evidence even if the measurement ends before this task is joined.
        let result = if let Some(result) = self.tasks.try_join_next() {
            Some(result)
        } else {
            self.tasks.abort_all();
            self.tasks.join_next().await
        };
        match result {
            Some(Ok(outcome)) => outcome,
            Some(Err(error)) if error.is_cancelled() => FaultOutcome::Cancelled,
            Some(Err(error)) => FaultOutcome::TaskFailed(error.to_string()),
            None => FaultOutcome::TaskFailed("fault task missing".into()),
        }
    }
}

async fn execute_fault<C, D, L, K, F>(
    target_leader: bool,
    discover: D,
    lookup: L,
    delete: F,
    kill_at: Arc<AtomicU64>,
) -> FaultOutcome
where
    D: Future<Output = Result<C>>,
    L: Future<Output = Result<i32>>,
    K: Future<Output = Result<String>>,
    F: FnOnce(C, Option<i32>) -> K,
{
    let client = match discover.await {
        Ok(client) => client,
        Err(error) => {
            warn!(%error, "failover: in-cluster client unavailable");
            return FaultOutcome::ClientUnavailable(error.to_string());
        }
    };
    let (leader_id, leader_lookup_error) = if target_leader {
        match lookup.await {
            Ok(id) => (Some(id), None),
            Err(error) => {
                warn!(%error, "failover: leader lookup failed; using first broker pod");
                (None, Some(error.to_string()))
            }
        }
    } else {
        (None, None)
    };
    let deletion = match delete(client, leader_id).await {
        Ok(pod) => {
            let at_ms = nonnegative_i64_to_u64(Utc::now().timestamp_millis());
            kill_at.store(at_ms, Ordering::SeqCst);
            info!(%pod, "failover: killed broker");
            Ok(KillEvidence { pod, at_ms })
        }
        Err(error) => {
            warn!(%error, "failover: broker kill failed");
            Err(error.to_string())
        }
    };
    FaultOutcome::Finished {
        leader_lookup_error,
        deletion,
    }
}

/// Discovers an in-cluster Kubernetes client, which uses the in-pod
/// `serviceAccount` token. Outside the cluster, which is useful for
/// `cargo test`, this returns an `Err` and the caller should report the
/// failover as skipped.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub async fn try_client() -> Result<KubeClient> {
    KubeClient::try_default()
        .await
        .context("build in-cluster kube client")
}

/// Queries the topic metadata and returns partition 0's current leader broker
/// id.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub async fn partition0_leader_from_metadata(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
) -> Result<i32> {
    let client = KafkaClient::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("bench-failover-targeter")
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .maybe_security(security)
        .build()
        .await
        .context("build metadata client")?;
    let md = client
        .refresh_metadata()
        .await
        .context("refresh metadata")?;
    let leader = partition0_leader_id(&md, topic)
        .ok_or_else(|| anyhow!("metadata did not contain {topic} partition 0 leader"))?;
    client.close();
    Ok(leader)
}

/// Deletes the requested broker pod. When `leader_id` is `Some`, this targets
/// the pod whose ordinal matches that broker id. Otherwise it deletes the first
/// matching pod, for backwards-compatible smoke runs. `grace_period_seconds = 0`
/// means SIGKILL.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub async fn kill_broker_pod(
    client: &KubeClient,
    stack: Stack,
    namespace: &str,
    leader_id: Option<i32>,
) -> Result<String> {
    let prefix = stack.broker_pod_regex().trim_start_matches('^');

    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list = pods
        .list(&ListParams::default())
        .await
        .context("list pods in namespace")?;
    let mut names: Vec<String> = list
        .items
        .iter()
        .filter_map(|p| p.metadata.name.clone())
        .filter(|n| n.starts_with(prefix))
        .collect();
    names.sort();
    let Some(target) = choose_target_pod(&names, stack, leader_id) else {
        return Err(anyhow!("no broker pod matched prefix {prefix}"));
    };

    let dp = DeleteParams::default().grace_period(0);
    pods.delete(&target, &dp)
        .await
        .with_context(|| format!("delete pod {target}"))?;

    Ok(target)
}

/// Deletes the first broker pod in alphabetical order that matches the stack's
/// pod-name regex root. This stays as a compatibility wrapper for runs that do
/// not target a partition.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub async fn kill_first_broker(
    client: &KubeClient,
    stack: Stack,
    namespace: &str,
) -> Result<String> {
    kill_broker_pod(client, stack, namespace, None).await
}

fn partition0_leader_id(md: &MetadataResponse, topic: &str) -> Option<i32> {
    md.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .map(|p| p.leader_id)
        .filter(|id| *id >= 0)
}

fn choose_target_pod(names: &[String], stack: Stack, leader_id: Option<i32>) -> Option<String> {
    let prefix = stack.broker_pod_regex().trim_start_matches('^');
    let mut matching: Vec<&String> = names.iter().filter(|n| n.starts_with(prefix)).collect();
    matching.sort();

    if let Some(id) = leader_id {
        let broker_suffix = format!("-{id}");
        let nodepool_suffix = format!("-{id}-0");
        if let Some(name) = matching
            .iter()
            .find(|n| n.ends_with(&broker_suffix) || n.ends_with(&nodepool_suffix))
        {
            return Some((*name).clone());
        }
    }

    matching.first().map(|n| (*n).clone())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        krabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
        },
    };

    #[tokio::test(start_paused = true)]
    async fn stopping_before_fault_deadline_prevents_later_deletion() {
        let kill_at = Arc::new(AtomicU64::new(0));
        let deletes = Arc::new(AtomicU64::new(0));
        let observed = deletes.clone();
        let marker = kill_at.clone();
        let task = FaultTask::schedule(
            Instant::now() + std::time::Duration::from_secs(60),
            async move {
                execute_fault(
                    true,
                    async { Ok(()) },
                    async { Ok(2) },
                    |(), _| async move {
                        observed.fetch_add(1, Ordering::SeqCst);
                        Ok("broker-2".into())
                    },
                    marker,
                )
                .await
            },
        );
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert2::assert!(task.stop().await == FaultOutcome::Cancelled);
        tokio::time::advance(std::time::Duration::from_secs(120)).await;
        assert2::assert!(deletes.load(Ordering::SeqCst) == 0);
        assert2::assert!(kill_at.load(Ordering::SeqCst) == 0);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_fault_owner_cancels_scheduled_operation() {
        let operations = Arc::new(AtomicU64::new(0));
        let observed = operations.clone();
        let task = FaultTask::schedule(
            Instant::now() + std::time::Duration::from_secs(30),
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                FaultOutcome::ClientUnavailable("injected".into())
            },
        );
        tokio::task::yield_now().await;
        drop(task);
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        assert2::assert!(operations.load(Ordering::SeqCst) == 0);
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_cancels_and_joins_an_in_flight_fault() {
        let kill_at = Arc::new(AtomicU64::new(0));
        let marker = kill_at.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = FaultTask::schedule(Instant::now(), async move {
            execute_fault(
                true,
                async { Ok(()) },
                async { Ok(2) },
                |(), _| async move {
                    assert2::assert!(entered_tx.send(()).is_ok());
                    release_rx.await.context("release injected deletion")?;
                    Ok("broker".into())
                },
                marker,
            )
            .await
        });
        entered_rx.await.expect("fault operation entered");
        assert2::assert!(task.stop().await == FaultOutcome::Cancelled);
        // Joining cancellation drops the operation, including its receiver.
        assert2::assert!(release_tx.send(()).is_err());
        assert2::assert!(kill_at.load(Ordering::SeqCst) == 0);
    }

    #[tokio::test]
    async fn fault_outcomes_preserve_failures_and_only_mark_successful_deletions() {
        for (discovery_fails, lookup_fails, deletion_fails, expected_note) in [
            (
                true,
                false,
                false,
                Some("failover: in-cluster client unavailable: discovery"),
            ),
            (
                false,
                true,
                false,
                Some(
                    "failover: leader lookup failed; using first broker pod: lookup; killed broker broker",
                ),
            ),
            (
                false,
                false,
                true,
                Some("failover: broker kill failed: deletion"),
            ),
            (
                false,
                true,
                true,
                Some(
                    "failover: leader lookup failed; using first broker pod: lookup; broker kill failed: deletion",
                ),
            ),
            (false, false, false, None),
        ] {
            let kill_at = Arc::new(AtomicU64::new(0));
            let deletes = Arc::new(AtomicU64::new(0));
            let observed = deletes.clone();
            let outcome = execute_fault(
                true,
                async {
                    if discovery_fails {
                        Err(anyhow!("discovery"))
                    } else {
                        Ok(())
                    }
                },
                async {
                    if lookup_fails {
                        Err(anyhow!("lookup"))
                    } else {
                        Ok(2)
                    }
                },
                |(), leader| async move {
                    assert2::assert!(leader == if lookup_fails { None } else { Some(2) });
                    observed.fetch_add(1, Ordering::SeqCst);
                    if deletion_fails {
                        Err(anyhow!("deletion"))
                    } else {
                        Ok("broker".into())
                    }
                },
                kill_at.clone(),
            )
            .await;
            assert2::assert!(outcome.note().as_deref() == expected_note);
            if discovery_fails {
                assert2::assert!(outcome == FaultOutcome::ClientUnavailable("discovery".into()));
                assert2::assert!(deletes.load(Ordering::SeqCst) == 0);
            } else {
                let deletion = if deletion_fails {
                    Err("deletion".into())
                } else {
                    Ok(KillEvidence {
                        pod: "broker".into(),
                        at_ms: kill_at.load(Ordering::SeqCst),
                    })
                };
                assert2::assert!(
                    outcome
                        == FaultOutcome::Finished {
                            leader_lookup_error: lookup_fails.then(|| "lookup".into()),
                            deletion,
                        }
                );
                assert2::assert!(deletes.load(Ordering::SeqCst) == 1);
            }
            assert2::assert!(
                (kill_at.load(Ordering::SeqCst) != 0) == (!discovery_fails && !deletion_fails)
            );
            assert2::assert!(
                outcome.killed_at_ms().is_some() == (!discovery_fails && !deletion_fails)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn completed_fault_evidence_survives_stop() {
        let task = FaultTask::schedule(Instant::now(), async {
            FaultOutcome::Finished {
                leader_lookup_error: None,
                deletion: Ok(KillEvidence {
                    pod: "broker".into(),
                    at_ms: 42,
                }),
            }
        });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert2::assert!(task.stop().await.killed_at_ms() == Some(42));
    }

    #[test]
    fn partition0_leader_is_read_from_metadata() {
        let md = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("bench-topic".into()),
                partitions: vec![
                    MetadataResponsePartition {
                        partition_index: 1,
                        leader_id: 0,
                        ..Default::default()
                    },
                    MetadataResponsePartition {
                        partition_index: 0,
                        leader_id: 2,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(partition0_leader_id(&md, "bench-topic") == Some(2));
    }

    #[test]
    fn target_pod_uses_actual_leader_id_for_each_stack_naming_shape() {
        let krabka_multi = vec![
            "demo-broker-0-0".to_string(),
            "demo-broker-1-0".to_string(),
            "demo-broker-2-0".to_string(),
        ];
        assert2::assert!(
            choose_target_pod(&krabka_multi, Stack::Krabka, Some(1)).as_deref()
                == Some("demo-broker-1-0")
        );

        let krabka_single_pool = vec![
            "demo-brokers-0".to_string(),
            "demo-brokers-1".to_string(),
            "demo-brokers-2".to_string(),
        ];
        assert2::assert!(
            choose_target_pod(&krabka_single_pool, Stack::Krabka, Some(2)).as_deref()
                == Some("demo-brokers-2")
        );

        let kafka = vec![
            "demo-kafka-0".to_string(),
            "demo-kafka-1".to_string(),
            "demo-kafka-2".to_string(),
        ];
        assert2::assert!(
            choose_target_pod(&kafka, Stack::Kafka, Some(1)).as_deref() == Some("demo-kafka-1")
        );
    }

    #[test]
    fn target_pod_falls_back_to_first_matching_broker_when_leader_unknown() {
        let pods = vec![
            "demo-kafka-2".to_string(),
            "demo-kafka-0".to_string(),
            "demo-kafka-1".to_string(),
        ];

        assert2::assert!(
            choose_target_pod(&pods, Stack::Kafka, None).as_deref() == Some("demo-kafka-0")
        );
    }
}
