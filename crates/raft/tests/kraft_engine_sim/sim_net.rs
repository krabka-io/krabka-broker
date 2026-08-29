//! The in-memory network the simulated engines talk over: a registry of live
//! engines keyed by node id, and the [`PeerSender`] that routes an encoded
//! KIP-595 body into the target engine's inbound queue.
//!
//! Registering and removing an engine is how the acceptances model a node
//! booting, crashing, and coming back, so the whole notion of reachability in
//! this simulation lives here.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use krabka_raft::{
    RaftError,
    kraft::{
        KraftController, NodeId, PeerSender,
        transport::{Inbound, api_key},
    },
};
use tokio::sync::oneshot;

/// Shared registry of in-process engines, keyed by node id.
///
/// Each engine holds a clone of one of these, through [`SimNet`], so that its
/// outbound peer sends can reach the others. A drop from the registry removes a
/// node, which models a leader kill or a restart. Later sends to that node then
/// fail as unreachable, which mirrors a crash.
#[derive(Default)]
struct Registry {
    nodes: HashMap<NodeId, KraftController>,
}

/// An in-memory [`PeerSender`]. It routes the encoded request body to the target
/// engine's [`KraftController::deliver`] and awaits the oneshot reply.
#[derive(Clone)]
pub(crate) struct SimNet {
    registry: Arc<Mutex<Registry>>,
}

impl SimNet {
    pub(crate) fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
        }
    }

    pub(crate) fn register(&self, id: NodeId, ctrl: KraftController) {
        self.registry.lock().unwrap().nodes.insert(id, ctrl);
    }

    pub(crate) fn remove(&self, id: NodeId) {
        self.registry.lock().unwrap().nodes.remove(&id);
    }

    pub(crate) fn get(&self, id: NodeId) -> Option<KraftController> {
        self.registry.lock().unwrap().nodes.get(&id).cloned()
    }
}

#[async_trait::async_trait]
impl PeerSender for SimNet {
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError> {
        // Look up the target engine. A removed/crashed node is unreachable.
        let target = self.get(peer).ok_or(RaftError::NotLeader {
            current_leader: None,
        })?;
        let (reply, rx) = oneshot::channel();
        let inbound = match api_key {
            api_key::VOTE => Inbound::Vote { req: body, reply },
            api_key::BEGIN_QUORUM_EPOCH => Inbound::BeginQuorumEpoch { req: body, reply },
            api_key::END_QUORUM_EPOCH => Inbound::EndQuorumEpoch { req: body, reply },
            api_key::FETCH => Inbound::Fetch { req: body, reply },
            api_key::FETCH_SNAPSHOT => Inbound::FetchSnapshot { req: body, reply },
            other => panic!("sim: unexpected api_key {other}"),
        };
        // Deliver to the target loop (non-blocking enqueue) and await its reply.
        // The loop processes inbound concurrently with our caller's loop, so this
        // never deadlocks even when engines RPC each other reciprocally.
        target
            .deliver(inbound)
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}
