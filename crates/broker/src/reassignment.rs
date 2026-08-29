//! KIP-455 reassignment-completion background task.
//!
//! This task runs on the controller leader and watches the metadata image.
//! When every one of a reassignment's `adding_replicas` is in the ISR, the
//! task moves atomically to the target replica set. When the current leader is
//! in `removing_replicas`, the task first hands leadership to a target replica
//! that is in the ISR.
//!
//! The pure per-partition decision that the task applies lives in
//! [`self::policy`].

use std::sync::Arc;

use async_trait::async_trait;
use krabka_metadata::{MetadataImage, MetadataRecord};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::heartbeat::controller_state::ControllerLivenessState;

mod policy;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

/// `reassign_one` itself is used only through `compute_reassignment_progress`
/// in a normal build. The re-export exists for `reassignment_model`, the
/// `stateright` model checker, which drives the decision directly.
#[cfg(test)]
pub(crate) use self::policy::reassign_one;
pub(crate) use self::policy::{compute_reassignment_progress, remap_directories};

/// Minimal trait for the controller surface that this task needs. It lets a
/// unit test inject a mock without a real raft cluster.
#[async_trait]
pub(crate) trait ReassignmentController: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// Background task entry point. Image-apply events drive it.
pub(crate) async fn run(
    controller: Arc<dyn ReassignmentController>,
    liveness: Arc<ControllerLivenessState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    loop {
        tokio::select! {
            result = watcher.changed() => {
                if result.is_err() {
                    // Channel closed — controller dropped.
                    break;
                }
            },
            () = shutdown.cancelled() => {
                info!("reassignment task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("reassignment tick skipped: not controller leader");
            continue;
        }
        let image = controller.current_image();
        let updates = compute_reassignment_progress(&image, &liveness).await;
        if !updates.is_empty() {
            info!(
                count = updates.len(),
                "reassignment: submitting completion updates"
            );
            if let Err(e) = controller.submit_change(updates).await {
                warn!(error = %e, "reassignment: submit failed");
            }
        }
    }
}

#[cfg(test)]
#[path = "reassignment_model.rs"]
mod reassignment_model;
