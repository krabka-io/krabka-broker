//! The periodic-injection task of the barrier coordinator.
//!
//! `Broker::start` spawns this task on every broker. On each tick it refreshes
//! the coordinator's leader-partition view, and then injects into every group
//! whose interval elapsed. A group runs only on the broker that coordinates it
//! now, so two brokers never inject the same epoch.
//!
//! A group with no interval injects only on demand, and the scheduler passes
//! over it.

use std::sync::Arc;

use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    barrier::coordinator::BarrierCoordinator, metadata_source::MetadataSource, time_util::now_ms,
};

/// Entry point of the spawned task. It returns when `shutdown` is cancelled.
pub(crate) async fn run(
    coordinator: Arc<BarrierCoordinator>,
    controller: Arc<dyn MetadataSource>,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(coordinator.scheduler_tick().to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => inject_due(&coordinator, controller.as_ref()).await,
            () = shutdown.cancelled() => {
                info!("barrier scheduler shutting down");
                return;
            }
        }
    }
}

/// Run one tick. It refreshes the leader-partition view, and then injects into
/// every group that is due.
async fn inject_due(coordinator: &BarrierCoordinator, controller: &dyn MetadataSource) {
    let image = controller.current_image();
    coordinator.refresh_leader_partitions(&image).await;
    let injected = coordinator.run_due_injections(now_ms()).await;
    if injected.is_empty() {
        debug!("barrier scheduler: no group is due");
    } else {
        info!(count = injected.len(), "barrier scheduler: injected");
    }
}
