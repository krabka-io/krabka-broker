//! `DescribeLogDirs` (`api_key=35`, KIP-113).
//!
//! The handler reports, for each configured log directory, the partitions that
//! the directory physically holds and their on-disk sizes. It backs the
//! `kafka-log-dirs --describe` admin tool.
//!
//! The handler reports both current logs and the future logs of in-progress
//! KIP-113 intra-broker moves. It reports a future-log entry under the
//! destination dir, with `is_future_key = true` and an `offset_lag` equal to
//! `current_log.LEO − future_log.LEO`.
//!
//! This file holds the wire entry point and the per-directory scan loop. The
//! ACL gate lives in `authz`, the request's topic filter in `filter`, the two
//! lag readings in `lag`, and everything the handler reports about a directory
//! itself in `dirs`.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
            DescribeLogDirsTopic,
        },
    },
};

mod authz;
mod dirs;
mod filter;
mod lag;

use self::{
    authz::{cluster_describe_denied, denied_response},
    dirs::{absolute_path, log_dir_capacity, offline_result},
    filter::request_filter,
    lag::{future_offset_lag, offset_lag_for},
};
use crate::{
    broker::Broker, codes, disk_scanner::scan::sum_partition_dir, error::BrokerError, log_dir,
};

#[tracing::instrument(
    name = "handle_describe_log_dirs",
    level = "info",
    skip_all,
    fields(api = "DescribeLogDirs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let log_dirs = broker.config.all_log_dirs();
    let partitions = broker.partitions.clone();
    let future_logs = broker.future_logs.clone();
    let log_dir_status = broker.log_dir_status.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = DescribeLogDirsRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // `Describe` on `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        {
            let image = broker.controller.current_image();
            if cluster_describe_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
            ) {
                return denied_response(version);
            }
        }

        let filter = request_filter(req);

        let mut results = Vec::with_capacity(log_dirs.len());
        for dir in &log_dirs {
            // KIP-113 offline-dir handling: a dir the startup probe
            // flagged unwritable is reported with
            // `error_code = KAFKA_STORAGE_ERROR`, no partition scan,
            // and `-1` for capacity. The JVM `kafka-log-dirs` tool
            // expects this shape — it prints the dir as
            // "OFFLINE: …" rather than a row of zeros.
            if log_dir_status.is_offline(dir) {
                results.push(offline_result(dir));
                continue;
            }
            // Group the partitions physically present in this dir by topic.
            let mut by_topic: BTreeMap<String, Vec<DescribeLogDirsPartition>> = BTreeMap::new();
            let discovered = log_dir::scan(dir).unwrap_or_default();
            for (topic, partition) in discovered {
                if !filter.allows(&topic, partition) {
                    continue;
                }
                let part_dir = log_dir::partition_dir(dir, &topic, partition);
                let size = sum_partition_dir(&part_dir).unwrap_or(0);
                let offset_lag = offset_lag_for(&partitions, &topic, partition).await;
                by_topic
                    .entry(topic)
                    .or_default()
                    .push(DescribeLogDirsPartition {
                        partition_index: partition,
                        partition_size: i64::try_from(size).unwrap_or(i64::MAX),
                        offset_lag,
                        is_future_key: false,
                        ..Default::default()
                    });
            }

            // KIP-113: surface in-progress future logs (one per
            // `<topic>-<partition>-future` subdir) with
            // `is_future_key = true`. `offset_lag` is the gap between
            // the future log and the source log; while the move is
            // running this shrinks toward zero, then the directory
            // rename turns the entry into a regular current log.
            let future_discovered = log_dir::scan_future(dir).unwrap_or_default();
            for (topic, partition) in future_discovered {
                if !filter.allows(&topic, partition) {
                    continue;
                }
                let future_path = log_dir::future_partition_dir(dir, &topic, partition);
                let size = sum_partition_dir(&future_path).unwrap_or(0);
                let offset_lag = future_offset_lag(
                    &partitions,
                    &future_logs,
                    &topic,
                    krabka_ids::PartitionIndex(partition),
                );
                by_topic
                    .entry(topic)
                    .or_default()
                    .push(DescribeLogDirsPartition {
                        partition_index: partition,
                        partition_size: i64::try_from(size).unwrap_or(i64::MAX),
                        offset_lag,
                        is_future_key: true,
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

            let (total_bytes, usable_bytes) = log_dir_capacity(dir);

            results.push(DescribeLogDirsResult {
                error_code: codes::NONE,
                log_dir: absolute_path(dir),
                topics,
                // KIP-827 (Kafka 3.3+): v4 surfaces per-dir filesystem
                // capacity. We query the underlying filesystem via
                // `statvfs` on unix and report `-1` (Kafka's "unknown"
                // sentinel) on non-unix; the JVM admin tools tolerate
                // `-1` and skip the column.
                total_bytes,
                usable_bytes,
                ..Default::default()
            });
        }

        let resp = DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}
