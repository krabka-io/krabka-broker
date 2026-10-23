//! Coordinator leadership follows the replica driver's readiness handoff.
//! Share-state uncertainty has its own wakeup, so recovery never queues replica
//! configuration work behind the writer whose append is being recovered.

use {
    crate::{share_coordinator::coordinator::ShareCoordinator, txn::coordinator::TxnCoordinator},
    krabka_metadata::MetadataImage,
    std::sync::Arc,
    tokio::{sync::watch, task::JoinHandle},
    tokio_util::sync::CancellationToken,
};

pub(crate) struct CoordinatorDriver {
    ready: watch::Receiver<Option<Arc<MetadataImage>>>,
    txn: Option<Arc<TxnCoordinator>>,
    share: Option<Arc<ShareCoordinator>>,
    shutdown: CancellationToken,
}

impl CoordinatorDriver {
    pub(crate) fn new(
        ready: watch::Receiver<Option<Arc<MetadataImage>>>,
        txn: Option<Arc<TxnCoordinator>>,
        share: Option<Arc<ShareCoordinator>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            ready,
            txn,
            share,
            shutdown,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut loads: Vec<JoinHandle<()>> = Vec::new();
        let mut refresh = true;
        loop {
            let image = if refresh {
                self.ready.borrow_and_update().clone()
            } else {
                self.ready.borrow().clone()
            };
            if refresh && let Some(image) = &image {
                tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => break,
                    () = async {
                        if let Some(txn) = &self.txn {
                            loads.extend(txn.refresh_ready_leader_partitions(image).await.into_tasks());
                        }
                        if let Some(share) = &self.share {
                            loads.extend(share.refresh_leader_partitions(image).await.into_tasks());
                        }
                    } => {}
                }
                loads.retain(|task| !task.is_finished());
            }
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => break,
                changed = self.ready.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    refresh = true;
                }
                () = async {
                    match &self.share {
                        Some(share) => share.recovery_requested().await,
                        None => std::future::pending().await,
                    }
                }, if image.is_some() => {
                    // Only the last reconciled image is safe for recovery.
                    // No new metadata image or replica reconcile is required.
                    if let (Some(share), Some(image)) = (&self.share, &image) {
                        tokio::select! {
                            biased;
                            () = self.shutdown.cancelled() => break,
                            scheduled = share.refresh_leader_partitions(image) => {
                                loads.extend(scheduled.into_tasks());
                            }
                        }
                    }
                    refresh = false;
                }
            }
        }
        for task in &loads {
            task.abort();
        }
        for task in loads {
            let _ = task.await;
        }
    }
}

#[cfg(test)]
mod tests;
