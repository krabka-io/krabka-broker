//! Polling the surviving replicas for their log state.
//!
//! One `GetReplicaLogInfo` request per replica, driven concurrently and cut
//! off at the strategy's deadline. The module is separate from the manager so
//! that the fan-out and its partial-result contract can be tested without a
//! controller.

use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use krabka_protocol::{
    owned::get_replica_log_info_request::{GetReplicaLogInfoRequest, TopicPartitions},
    primitives::uuid::Uuid as WireUuid,
};
use krabka_raft::NodeId;

use super::ReplicaLogInfo;
use crate::network::client::InterBrokerClient;

/// Queries one replica for its log-end-offset and leader-epoch state with
/// `GetReplicaLogInfo` (`api_key` 93). Returns `None` on any connect, send, or
/// decode error, and also if the replica reports an error for this
/// partition.
pub(super) struct ReplicaQuery {
    pub(super) proto: krabka_security::ListenerProtocol,
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) my_broker_id: i32,
    pub(super) topic_id: WireUuid,
    pub(super) partition: i32,
    pub(super) replica: NodeId,
    pub(super) server_name: String,
}

pub(super) async fn query_replica(
    client: &InterBrokerClient,
    query: ReplicaQuery,
) -> Option<ReplicaLogInfo> {
    let opts = krabka_client_core::ConnectionOptions {
        client_id: "krabka-unclean-recovery".to_string(),
        ..krabka_client_core::ConnectionOptions::default()
    };
    let conn = client
        .connect_as_connection(
            &query.host,
            query.port,
            query.proto,
            &query.server_name,
            opts,
        )
        .await
        .ok()?;
    let req = GetReplicaLogInfoRequest {
        broker_id: query.my_broker_id,
        topic_partitions: vec![TopicPartitions {
            topic_id: query.topic_id,
            partitions: vec![query.partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = conn.send(req).await.ok()?;
    for t in &resp.topic_partition_log_info_list {
        for pli in &t.partition_log_info {
            if pli.partition == query.partition && pli.error_code == 0 {
                return Some(ReplicaLogInfo {
                    broker_id: query.replica,
                    last_written_leader_epoch: pli.last_written_leader_epoch,
                    log_end_offset: pli.log_end_offset,
                    current_leader_epoch: pli.current_leader_epoch,
                });
            }
        }
    }
    None
}

/// Drives the per-replica query futures concurrently.
///
/// It returns when all futures resolve OR when `deadline` passes, whichever
/// comes first. On a timeout it returns the responses that arrived so far, and
/// never silently discards partial data.
pub(super) async fn gather_responses<F>(futs: Vec<F>, deadline: Duration) -> Vec<ReplicaLogInfo>
where
    F: std::future::Future<Output = Option<ReplicaLogInfo>> + Send + 'static,
{
    let total = futs.len();
    let mut stream: FuturesUnordered<_> = futs.into_iter().collect();
    let mut out: Vec<ReplicaLogInfo> = Vec::with_capacity(total);
    let sleep = tokio::time::sleep(deadline);
    tokio::pin!(sleep);
    loop {
        if out.len() == total {
            break;
        }
        tokio::select! {
            () = &mut sleep => break,
            item = stream.next() => match item {
                Some(Some(info)) => out.push(info),
                Some(None) => {}
                None => break,
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use futures_util::FutureExt as _;

    use super::*;
    use crate::unclean_recovery::selection::select_best_replica;

    fn info(id: u64, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(id),
            last_written_leader_epoch: 1,
            log_end_offset: leo,
            current_leader_epoch: 1,
        }
    }

    #[tokio::test]
    async fn balanced_waits_for_all_then_picks_best() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_secs(5)).await;
        assert!(got.len() == 2);
        assert!(select_best_replica(&got) == Some(NodeId(2)));
    }

    #[tokio::test]
    async fn balanced_returns_partial_on_timeout() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_millis(50)).await;
        assert!(got.len() == 1, "must return what arrived before the cap");
        assert!(got[0].broker_id == krabka_audit::NodeId(1));
    }

    #[tokio::test]
    async fn aggressive_takes_early_responders() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_millis(50)).await;
        assert!(got == vec![info(1, 50)]);
    }
}
