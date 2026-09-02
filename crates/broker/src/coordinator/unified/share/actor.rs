//! Per-share-group tokio actor (KIP-932). Owns [`ShareGroupState`] for one
//! share group. Heartbeats arrive as mpsc messages, and responses go back
//! through oneshot channels.
//!
//! It mirrors the consumer next-gen [`crate::coordinator::unified::actor`]
//! without any offset-validation or partition-revocation machinery.
//! Share-group assignment is non-exclusive, so a member's epoch advances
//! straight to the group epoch with no acknowledgement round-trip.
//!
//! This file is the module root. It holds the actor's identity — the mailbox
//! protocol, the handle, and the `tokio::select!` loop — while each request
//! path and each persistence concern lives in its own submodule.

use std::sync::Arc;

use krabka_protocol::owned::{
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

mod admin_offsets;
mod assignment;
mod describe;
mod heartbeat;
mod records;
mod response;
mod seed;
mod session;
mod share_state;

#[cfg(test)]
mod test_support;

#[cfg(test)]
#[path = "actor/share_group_model.rs"]
mod share_group_model;

pub(crate) use self::admin_offsets::{DeleteTopic, ResetPartition};
pub use self::describe::{ShareDescribeMember, ShareDescribeView};
use self::{
    admin_offsets::{delete_offsets, reset_offsets},
    describe::build_describe,
    heartbeat::handle_heartbeat,
    records::{PendingShareRecords, chrono_now_ms, flush_pending, state_partition_metadata_from},
    seed::apply_seed,
    session::handle_session_tick,
};
use super::{config::ShareGroupConfig, state::ShareGroupState};
use crate::{
    codes,
    coordinator::unified::{actor::MetadataProvider, offsets_log::OffsetsLog},
};

#[derive(Debug)]
pub enum ShareGroupActorMessage {
    Heartbeat {
        request: ShareGroupHeartbeatRequest,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<ShareGroupHeartbeatResponse>,
    },
    Describe {
        reply: oneshot::Sender<ShareDescribeView>,
    },
    ResetOffsets {
        requests: Vec<ResetPartition>,
        reply: oneshot::Sender<Result<Vec<i16>, i16>>,
    },
    DeleteOffsets {
        requests: Vec<DeleteTopic>,
        reply: oneshot::Sender<Result<Vec<i16>, i16>>,
    },
    Seed(super::super::ShareGroupSeed),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct ShareGroupActorHandle {
    pub tx: mpsc::Sender<ShareGroupActorMessage>,
    _task: JoinHandle<()>,
}

impl ShareGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<ShareGroupConfig>,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<super::super::GroupCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.actor_mailbox_capacity);
        let task = tokio::spawn(actor_loop(
            group_id,
            config,
            metadata,
            offsets_log,
            coordinator,
            rx,
        ));
        Self { tx, _task: task }
    }
}

async fn actor_loop(
    group_id: String,
    config: Arc<ShareGroupConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<super::super::GroupCoordinator>,
    mut rx: mpsc::Receiver<ShareGroupActorMessage>,
) {
    let mut state = ShareGroupState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    ShareGroupActorMessage::Heartbeat { request, client_id, client_host, reply } => {
                        match handle_heartbeat(
                            &mut state,
                            &config,
                            &*metadata,
                            &*offsets_log,
                            &coordinator,
                            &request,
                            super::super::ClientIdentity {
                                id: &client_id,
                                host: &client_host,
                            },
                        )
                        .await
                        {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %state.group_id,
                                    error = %e,
                                    "share-group actor exiting after log-write failure",
                                );
                                let _ = reply.send(ShareGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                    ShareGroupActorMessage::Describe { reply } => {
                        let _ = reply.send(build_describe(&state));
                    }
                    ShareGroupActorMessage::ResetOffsets { requests, reply } => {
                        let result = reset_offsets(&state, &coordinator, requests).await;
                        let _ = reply.send(result);
                    }
                    ShareGroupActorMessage::DeleteOffsets { requests, reply } => {
                        let result = delete_offsets(&mut state, &coordinator, requests).await;
                        let _ = reply.send(result);
                    }
                    ShareGroupActorMessage::Seed(seed) => {
                        apply_seed(&mut state, seed);
                    }
                    ShareGroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if handle_session_tick(&mut state, &config, &*metadata, &*offsets_log, &coordinator).await.is_err() {
                    break;
                }
            }
        }
    }
}
