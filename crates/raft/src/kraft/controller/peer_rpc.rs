//! Outbound peer RPC. Every send is fire-and-forget on a spawned task that
//! posts the decoded response back to the loop, which is what keeps the engine
//! from ever awaiting a peer inline.

use std::sync::Arc;

use super::{Engine, engine_loop::response_to_event, replication::fetch_epoch_for_request};
use crate::kraft::{
    transport::{Command, api_key, wire},
    types::{Epoch, LogView, NodeId},
};

impl Engine {
    /// Voter ids other than self.
    pub fn other_voters(&self) -> Vec<NodeId> {
        self.core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .filter(|&id| id != self.me)
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch, pre_vote))]
    pub fn broadcast_vote(&self, epoch: Epoch, pre_vote: bool) {
        let last_epoch = self.log.last_epoch();
        let last_offset = self.log.end_offset();
        let state = self.core.quorum_state();
        let Some(candidate_voter) = state.voters.get(self.me) else {
            tracing::error!(candidate = self.me.0, "cannot encode Vote for a non-voter");
            return;
        };
        let cluster_id = state.cluster_id;
        let candidate_directory_id = candidate_voter.directory_id;
        // The wire top-level `voterId` must name the recipient voter; the JVM
        // rejects a Vote addressed to anyone else (or to the sentinel `-1`). So
        // build a per-recipient body inside the loop rather than broadcasting a
        // single shared body.
        for peer in self.other_voters() {
            let Some(voter_directory_id) = state.voters.get(peer).map(|voter| voter.directory_id)
            else {
                continue;
            };
            let request = wire::PeerRequest::Vote {
                cluster_id: Some(cluster_id),
                voter_id: peer,
                voter_directory_id,
                candidate_epoch: epoch,
                candidate: self.me,
                candidate_directory_id,
                last_epoch,
                last_offset,
                pre_vote,
            };
            let Some(body) = request.try_encode() else {
                tracing::error!(
                    voter = peer.0,
                    candidate = self.me.0,
                    epoch,
                    last_epoch,
                    "Vote fields exceed Kafka int32 wire range"
                );
                continue;
            };
            self.spawn_send(peer, api_key::VOTE, body);
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch))]
    pub fn broadcast_begin_quorum_epoch(&self, epoch: Epoch) {
        let body = wire::PeerRequest::BeginQuorumEpoch {
            leader_id: self.me,
            leader_epoch: epoch,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::BEGIN_QUORUM_EPOCH, body.clone());
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch))]
    pub fn broadcast_end_quorum_epoch(&self, epoch: Epoch) {
        let body = wire::PeerRequest::EndQuorumEpoch {
            leader_id: self.me,
            leader_epoch: epoch,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::END_QUORUM_EPOCH, body.clone());
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, leader_id = leader_id.0, fetch_offset = self.log.end_offset()))]
    pub fn send_fetch(&self, leader_id: NodeId) {
        if leader_id == self.me {
            return;
        }
        let fetch_offset = self.log.end_offset();
        // Post-install epoch hazard: right after installing a snapshot the log is
        // empty at the snapshot boundary, so it carries no epoch of its own and
        // `last_epoch()` would report 0. Sending `fetch_epoch = 0` from a
        // non-zero boundary makes the leader's divergence check emit a spurious
        // truncate hint → a re-fetch loop. While we hold a freshly-installed
        // epoch AND the log is still empty at the boundary, fetch with that
        // epoch instead. Cleared once a normal fetch appends past the boundary.
        let fetch_epoch = fetch_epoch_for_request(
            self.installed_snapshot_epoch,
            self.log.log_start_offset(),
            self.log.log_end_offset(),
            self.log.last_epoch(),
        );
        let body = wire::PeerRequest::Fetch {
            from: self.me,
            fetch_epoch,
            fetch_offset,
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH, body);
    }

    /// (Follower side) request a byte range of `snapshot_id` from `leader_id`.
    pub fn send_fetch_snapshot(&self, leader_id: NodeId, snapshot_id: (i64, i32), position: i64) {
        if leader_id == self.me {
            return;
        }
        let body = wire::PeerRequest::FetchSnapshot {
            from: self.me,
            snapshot_id,
            position,
            // KIP-595 `FetchSnapshot.MaxBytes` is an `int32`; the quantity
            // converts here, at the wire boundary.
            max_bytes: self.metadata_raft_fetch_max.bytes(),
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH_SNAPSHOT, body);
    }

    /// Fire-and-forget a peer send: spawn a task that performs the RPC, decodes
    /// the response into the matching `Receive*Response` core event, and posts
    /// it back to the loop. The loop NEVER awaits a peer RPC inline.
    pub fn spawn_send(&self, peer: NodeId, api_key: i16, body: bytes::Bytes) {
        let peers = Arc::clone(&self.peers);
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            match peers.send(peer, api_key, body).await {
                Ok(resp_body) => {
                    // A Fetch response carries log records the follower must
                    // truncate/append/apply before the core sees it, so it goes
                    // through the dedicated `FetchResponse` command. Every other
                    // response decodes to a pure `Receive*Response` event.
                    if api_key == self::api_key::FETCH {
                        let _ = cmd_tx
                            .send(Command::FetchResponse {
                                from: peer,
                                body: resp_body,
                            })
                            .await;
                    } else if api_key == self::api_key::FETCH_SNAPSHOT {
                        // A FetchSnapshot response carries snapshot bytes the
                        // follower reassembles + installs before resuming, so it
                        // takes its own command path (mirrors FetchResponse).
                        let _ = cmd_tx
                            .send(Command::FetchSnapshotResponse {
                                from: peer,
                                body: resp_body,
                            })
                            .await;
                    } else if let Some(event) = response_to_event(peer, api_key, &resp_body) {
                        let _ = cmd_tx.send(Command::Event(event)).await;
                    }
                }
                Err(e) => tracing::debug!(peer = peer.0, ?e, "kraft: peer send failed"),
            }
        });
    }
}
