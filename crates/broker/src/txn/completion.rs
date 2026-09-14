//! The background task that completes transactions whose `Prepare*` record is
//! durable.
//!
//! [`Broker::start`](crate::broker::Broker) spawns it on every broker. It
//! waits for a completion request, from recovery or from a request whose
//! marker fan-out or `Complete*` append failed. It then tries each queued
//! transaction, and it tries a failed one again after
//! [`RETRY_BACKOFF`], without a limit, as Kafka's
//! `TransactionMarkerChannelManager` does. A transaction leaves the queue when
//! it completes, when another caller changed it, or when this broker no longer
//! coordinates it.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    metadata_source::MetadataSource,
    txn::coordinator::{TxnCoordinator, completion::CompletionAttempt},
};

/// The wait before a failed completion runs again.
pub(crate) const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Entry point of the spawned task. It returns when `shutdown` is cancelled.
// cargo-mutants: orchestration only. Every decision is in
// `TxnCoordinator::complete_prepared_transaction` and its mutation-tested
// helpers; this loop only waits and hands transactional ids to it.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run(
    coordinator: Arc<TxnCoordinator>,
    controller: Arc<dyn MetadataSource>,
    shutdown: CancellationToken,
) {
    let mut retry = BTreeSet::new();
    loop {
        let mut queued: BTreeSet<String> =
            coordinator.take_completion_requests().into_iter().collect();
        queued.append(&mut retry);
        if !queued.is_empty() {
            retry = complete_once(&coordinator, controller.as_ref(), queued).await;
        }
        tokio::select! {
            () = coordinator.completion_requested() => {}
            () = tokio::time::sleep(RETRY_BACKOFF), if !retry.is_empty() => {}
            () = shutdown.cancelled() => {
                info!("transaction completion task shutting down");
                return;
            }
        }
    }
}

/// Try each queued transaction once, and return the ones to try again.
pub(crate) async fn complete_once(
    coordinator: &TxnCoordinator,
    controller: &dyn MetadataSource,
    queued: BTreeSet<String>,
) -> BTreeSet<String> {
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    coordinator.refresh_leader_partitions(&image).await;
    let mut retry = BTreeSet::new();
    for transactional_id in queued {
        if coordinator
            .complete_prepared_transaction(&transactional_id, txnv)
            .await
            == CompletionAttempt::Retry
        {
            retry.insert(transactional_id);
        }
    }
    retry
}
