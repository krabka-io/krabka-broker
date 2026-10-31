//! `DescribeLogDirs` (`api_key=35`, KIP-113).
//!
//! The handler reports, for each configured log directory, the partition logs
//! that the broker has loaded from it and their sizes, as Kafka's
//! `ReplicaManager.describeLogDirs` does from `LogManager.allLogs`. It reads
//! the logs in memory rather than the directory: the only filesystem call is
//! the capacity query. It backs the `kafka-log-dirs --describe` admin tool.
//!
//! The handler reports both current logs and the future logs of in-progress
//! KIP-113 intra-broker moves. It reports a future-log entry under the
//! destination dir, with `is_future_key = true` and an `offset_lag` equal to
//! `current_log.LEO − future_log.LEO`.
//!
//! This file holds the wire entry point, its ACL Deny response, and the
//! per-directory loop. The request's topic filter lives in `filter`, the two
//! lag readings in `lag`, and everything the handler reports about a
//! directory itself in `dirs`.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext},
    },
    krabka_protocol::owned::{
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
            DescribeLogDirsTopic,
        },
    },
    krabka_units::prelude::ByteSizeExt,
    std::collections::BTreeMap,
};

mod dirs;
mod filter;
mod lag;

use {
    self::{
        dirs::{absolute_path, cordoned_log_dirs_reported, error_result, log_dir_capacity},
        filter::{Filter, request_filter},
        lag::{future_offset_lag, offset_lag_for},
    },
    crate::{
        codes, future_log::FutureLogState, handlers::cluster_describe_denied, partition::Partition,
    },
    dashmap::DashMap,
    krabka_ids::PartitionIndex,
    std::{
        collections::HashMap,
        path::PathBuf,
        sync::{Arc, PoisonError},
    },
};

pub async fn handle(
    broker: &Broker,
    req: DescribeLogDirsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<DescribeLogDirsResponse, BrokerError> {
    let log_dirs = broker.config.all_log_dirs();
    let partitions = Arc::clone(&broker.partitions);
    let future_logs = Arc::clone(&broker.future_logs);
    let log_dir_status = broker.log_dir_status.clone();

    // ── ACL preamble ────────────────────────────────────────────
    // `Describe` on `Cluster("kafka-cluster")`. On Deny → whole-response
    // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
    if cluster_describe_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        ctx,
    ) {
        let resp = DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            results: Vec::new(),
            ..Default::default()
        };
        return Ok(resp);
    }

    let filter = request_filter(req);
    // KIP-1066: `IsCordoned` goes out only from `metadata.version`
    // `4.3-IV0`, as `ReplicaManager.describeLogDirs` gates it on
    // `isCordonedLogDirsSupported`. Below that level every directory
    // answers `false` whatever `cordoned.log.dirs` says.
    let cordoned = cordoned_log_dirs_reported(
        &broker.controller.current_image(),
        partitions.cordoned_log_dirs(),
    );
    let mut logs_by_dir = loaded_logs_by_dir(&partitions.arcs(), &future_logs, &filter);

    let mut results = Vec::with_capacity(log_dirs.len());
    for dir in &log_dirs {
        // KIP-113 offline-dir handling: an offline dir is reported with
        // `error_code = KAFKA_STORAGE_ERROR`, no partitions, and `-1` for
        // capacity. The JVM `kafka-log-dirs` tool expects this shape — it
        // prints the dir as "OFFLINE: …" rather than a row of zeros.
        if log_dir_status.is_offline(dir) {
            results.push(error_result(dir, codes::KAFKA_STORAGE_ERROR));
            continue;
        }
        // KIP-827 (Kafka 3.3+): v4 surfaces per-dir filesystem capacity,
        // from `statvfs` on unix and `-1` (Kafka's "unknown" sentinel)
        // elsewhere. A failed query fails this dir alone, as Kafka's
        // catch-all answers the `IOException` from `Files.getFileStore`
        // with `Errors.forException`, which is `UNKNOWN_SERVER_ERROR`. It
        // does not take the dir offline: only a failed log operation does.
        let (total_bytes, usable_bytes) = match log_dir_capacity(dir) {
            Ok(capacity) => capacity,
            Err(error) => {
                tracing::error!(
                    log_dir = %dir.display(),
                    %error,
                    "DescribeLogDirs: error while describing replicas in the log dir",
                );
                results.push(error_result(dir, codes::UNKNOWN_SERVER_ERROR));
                continue;
            }
        };

        let mut logs = logs_by_dir.remove(dir).unwrap_or_default();
        logs.sort_by_key(|log| (log.is_future_key, log.partition.0));
        let mut by_topic: BTreeMap<String, Vec<DescribeLogDirsPartition>> = BTreeMap::new();
        for LoadedLog {
            topic,
            partition,
            is_future_key,
            size,
        } in logs
        {
            // KIP-113: an in-progress future log reports the gap between
            // it and the source log; while the move is running this shrinks
            // toward zero, then the swap turns it into a current log.
            let offset_lag = if is_future_key {
                future_offset_lag(&partitions, &future_logs, &topic, partition)
            } else {
                offset_lag_for(&partitions, &topic, partition.0).await
            };
            by_topic
                .entry(topic)
                .or_default()
                .push(DescribeLogDirsPartition {
                    partition_index: partition.0,
                    partition_size: i64::try_from(size).unwrap_or(i64::MAX),
                    offset_lag,
                    is_future_key,
                    ..Default::default()
                });
        }

        let topics = by_topic
            .into_iter()
            .map(|(name, partitions)| DescribeLogDirsTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect();

        results.push(DescribeLogDirsResult {
            error_code: codes::NONE,
            log_dir: absolute_path(dir),
            topics,
            total_bytes,
            usable_bytes,
            is_cordoned: cordoned.iter().any(|cordoned| cordoned == dir),
            ..Default::default()
        });
    }

    Ok(DescribeLogDirsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        results,
        ..Default::default()
    })
}

/// A partition log loaded in memory, as one `DescribeLogDirs` entry reports it.
struct LoadedLog {
    topic: String,
    partition: PartitionIndex,
    is_future_key: bool,
    /// The bytes of the log's batches, Kafka's `UnifiedLog.size`.
    size: u64,
}

/// The loaded logs the request asks for, current and future, grouped by the
/// directory each lives in.
///
/// Kafka's `logManager.allLogs.groupBy(log => log.parentDir)`. A log's own
/// directory decides where it is reported, so a log mid-move shows under the
/// directory that holds it.
fn loaded_logs_by_dir(
    hosted: &[Arc<Partition>],
    future_logs: &DashMap<(String, PartitionIndex), Arc<FutureLogState>>,
    filter: &Filter,
) -> HashMap<PathBuf, Vec<LoadedLog>> {
    let mut logs: Vec<_> = hosted
        .iter()
        .map(|part| (part.topic.clone(), part.index, false, Arc::clone(&part.log)))
        .collect();
    logs.extend(future_logs.iter().map(|entry| {
        let (topic, partition) = entry.key();
        (
            topic.clone(),
            *partition,
            true,
            Arc::clone(&entry.value().future_log),
        )
    }));
    let mut by_dir: HashMap<PathBuf, Vec<LoadedLog>> = HashMap::new();
    for (topic, partition, is_future_key, log) in logs {
        if !filter.allows(&topic, partition.0) {
            continue;
        }
        let (parent, size) = {
            let log = log.lock().unwrap_or_else(PoisonError::into_inner);
            (
                log.dir().parent().map(PathBuf::from),
                log.size().bytes_u64(),
            )
        };
        let Some(parent) = parent else { continue };
        by_dir.entry(parent).or_default().push(LoadedLog {
            topic,
            partition,
            is_future_key,
            size,
        });
    }
    by_dir
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            authorizer::AllowAllAuthorizer,
            test_support::{
                DenyAll, RecordCount, StartedBroker, open_partition, repeated_records_batch,
                start_broker_with, start_broker_with_authorizer,
            },
        },
        assert2::assert,
        krabka_protocol::owned::describe_log_dirs_response,
        std::{fs, path::Path, sync::Arc},
    };

    krabka_macros::wire_codec_helpers! {
        test_context ["client-a"];
    }

    /// A principal without `Describe` on the cluster gets a whole-response
    /// `CLUSTER_AUTHORIZATION_FAILED` and no directories.
    #[tokio::test]
    async fn handle_denies_cluster_describe_for_whole_request() {
        let version = describe_log_dirs_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker_with_authorizer(Arc::new(DenyAll))];
            user ["ANONYMOUS"];
        };

        let resp = handle(&broker, DescribeLogDirsRequest::default(), version, &ctx)
            .await
            .expect("handle");

        let expected = DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            results: Vec::new(),
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// The row a describe gives `dir`, by its absolute path.
    fn row_for<'a>(
        resp: &'a DescribeLogDirsResponse,
        dir: &Path,
    ) -> Option<&'a DescribeLogDirsResult> {
        resp.results
            .iter()
            .find(|result| result.log_dir == dir.display().to_string())
    }

    /// A broker on the default config plus `extra` as a second log dir, with
    /// every request allowed.
    async fn start_with_extra(extra: &Path) -> StartedBroker {
        let extra = extra.to_path_buf();
        start_broker_with(move |config| {
            config.authorizer = Arc::new(AllowAllAuthorizer);
            config.extra_log_dirs = vec![extra];
        })
        .await
    }

    /// A healthy dir lists the logs the broker has loaded from it, with their
    /// in-memory size, as Kafka's `describeLogDirs` lists `LogManager.allLogs`.
    /// It does not list a partition directory that no loaded log owns.
    #[tokio::test]
    async fn handle_lists_the_loaded_logs_of_a_healthy_dir() {
        let version = describe_log_dirs_response::MAX_VERSION;
        let extra = tempfile::tempdir().expect("extra log dir");
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_with_extra(extra.path())];
            user ["admin"];
        };
        let hosted = open_partition()
            .log_dir(extra.path())
            .topic("orders")
            .partition(0)
            .call();
        let size = {
            let mut log = hosted.log.lock().expect("log");
            let mut batch = repeated_records_batch()
                .count(RecordCount(3))
                .timestamp(1_700_000_000)
                .call();
            log.append(&mut batch).expect("append records");
            log.size().bytes_u64()
        };
        // A partition directory no loaded log owns is not reported: the
        // listing is the logs in memory, not the directory's contents.
        fs::create_dir(extra.path().join("ghost-0")).expect("stray partition dir");
        broker
            .partitions
            .insert("orders".into(), PartitionIndex(0), Arc::clone(&hosted));

        // A null topic list asks for every partition.
        let all_partitions = DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        };

        let resp = handle(&broker, all_partitions, version, &ctx)
            .await
            .expect("handle");

        let row = row_for(&resp, extra.path()).expect("a row for the extra dir");
        let expected = DescribeLogDirsResult {
            error_code: codes::NONE,
            log_dir: extra.path().display().to_string(),
            topics: vec![DescribeLogDirsTopic {
                name: "orders".into(),
                partitions: vec![DescribeLogDirsPartition {
                    partition_index: 0,
                    partition_size: i64::try_from(size).expect("size fits"),
                    offset_lag: 0,
                    is_future_key: false,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            // Capacity is the live filesystem's; the dirs module tests it.
            total_bytes: row.total_bytes,
            usable_bytes: row.usable_bytes,
            is_cordoned: false,
            ..Default::default()
        };
        assert!(row == &expected);
        assert!(row.total_bytes > 0);
        broker_handle.shutdown().await;
    }

    /// A dir already offline in the registry answers `KAFKA_STORAGE_ERROR`
    /// and nothing else, as Kafka's `KafkaStorageException` arm does.
    #[tokio::test]
    async fn handle_reports_an_offline_dir_as_a_storage_error() {
        let version = describe_log_dirs_response::MAX_VERSION;
        let extra = tempfile::tempdir().expect("extra log dir");
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_with_extra(extra.path())];
            user ["admin"];
        };
        broker
            .log_dir_status
            .mark_offline(extra.path(), "test: EIO");

        let resp = handle(&broker, DescribeLogDirsRequest::default(), version, &ctx)
            .await
            .expect("handle");

        let expected = DescribeLogDirsResult {
            error_code: codes::KAFKA_STORAGE_ERROR,
            log_dir: extra.path().display().to_string(),
            topics: Vec::new(),
            total_bytes: -1,
            usable_bytes: -1,
            is_cordoned: false,
            ..Default::default()
        };
        assert!(row_for(&resp, extra.path()) == Some(&expected));
        broker_handle.shutdown().await;
    }

    /// A dir that cannot be described is an error for that dir alone, not a
    /// healthy empty dir, and the describe leaves it online: Kafka's
    /// catch-all answers `Errors.forException`, `UNKNOWN_SERVER_ERROR` for an
    /// `IOException`, and `describeLogDirs` never takes a dir offline.
    #[tokio::test]
    async fn handle_reports_a_dir_it_cannot_describe_as_an_unknown_error() {
        let version = describe_log_dirs_response::MAX_VERSION;
        let extra = tempfile::tempdir().expect("extra log dir");
        let extra_dir = extra.path().join("data");
        fs::create_dir(&extra_dir).expect("create extra log dir");
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_with_extra(&extra_dir)];
            user ["admin"];
        };
        // The capacity query fails with `ENOENT`, whoever the test runs as.
        fs::remove_dir_all(&extra_dir).expect("remove extra log dir");

        let resp = handle(&broker, DescribeLogDirsRequest::default(), version, &ctx)
            .await
            .expect("handle");

        let expected = DescribeLogDirsResult {
            error_code: codes::UNKNOWN_SERVER_ERROR,
            log_dir: extra_dir.display().to_string(),
            topics: Vec::new(),
            total_bytes: -1,
            usable_bytes: -1,
            is_cordoned: false,
            ..Default::default()
        };
        assert!(row_for(&resp, &extra_dir) == Some(&expected));
        assert!(!broker.log_dir_status.is_offline(&extra_dir));
        broker_handle.shutdown().await;
    }
}
