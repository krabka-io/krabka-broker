//! Service of inbound peer RPCs: each request body is decoded, run through the
//! consensus core, and answered on its oneshot, including the leader's Fetch
//! and `FetchSnapshot` serve paths.

use krabka_ids::Offset;

use super::{
    Engine, checkpoint::load_checkpoint_by_id, checkpoint_dir,
    replication::should_serve_fetch_records,
};
use crate::kraft::{
    action::Action,
    event::{Event, LogEnd},
    transport::{Inbound, wire},
};

/// Krabka-internal "snapshot not available" signal in a `FetchSnapshot`
/// response (voter↔voter).
const SNAPSHOT_NOT_FOUND: i16 = 98;

impl Engine {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, role = self.core.role().name())
    )]
    pub fn on_inbound(&mut self, inbound: Inbound) {
        // Decode the request body, run it through the core, and encode the
        // produced reply back onto the oneshot.
        match inbound {
            // A body that does not decode drops `reply`, which closes the
            // connection, as Kafka's `RequestContext.parseRequest` does.
            Inbound::Vote { req, reply } => {
                if let Some(response) = self.answer_vote(&req) {
                    let _ = reply.send(response);
                }
            }
            Inbound::BeginQuorumEpoch { req, reply } => {
                if let Some(response) = self.answer_begin_quorum_epoch(&req) {
                    let _ = reply.send(response);
                }
            }
            Inbound::EndQuorumEpoch { req, reply } => {
                if let Some(response) = self.answer_end_quorum_epoch(&req) {
                    let _ = reply.send(response);
                }
            }
            Inbound::Fetch { req, reply } => {
                if let Some(wire::PeerRequest::Fetch {
                    from,
                    fetch_epoch,
                    fetch_offset,
                    replica_directory_id,
                }) = wire::decode_fetch(&req)
                {
                    self.replica_fetch_offsets.insert(from, fetch_offset);
                    if replica_directory_id != uuid::Uuid::nil() {
                        self.replica_directory_ids
                            .insert(from, replica_directory_id);
                    }
                    let now = self.now();
                    let prev_role = self.core.role().name();
                    let actions = self.core.on_event(
                        Event::ReceiveFetch {
                            from,
                            fetch_epoch,
                            fetch_offset,
                        },
                        &self.log,
                        now,
                    );
                    // A Fetch may yield a diverging epoch for the follower, or
                    // AdvanceHighWatermark for the leader. Encode the divergence
                    // into the response; apply HWM locally. The diverging epoch
                    // is a reply only: the leader's log keeps its records.
                    let mut diverging = None;
                    for action in &actions {
                        if let Action::ReplyDivergingEpoch(point) = action {
                            diverging = Some(*point);
                        }
                    }
                    self.execute(actions);
                    self.reconcile_timers(prev_role);
                    self.publish_leader();
                    // Serve the follower the batch bytes it is missing: every
                    // batch at/after its `fetch_offset` up to our log end (KRaft
                    // replicates up to the leader's log end, not just the HWM —
                    // the HWM rides separately in the response). Only the leader
                    // serves records; a divergent fetch sends none (the follower
                    // truncates first, then re-fetches).
                    // If the follower's fetch offset is below our pruned
                    // log-start, it cannot replicate from the log — point it at
                    // the latest snapshot instead (KIP-630).
                    // `fetch_offset` arrives raw on the KIP-595 wire; wrap it into
                    // the `KraftLog` offset domain to compare against log bounds.
                    let fetch_offset = Offset(fetch_offset);
                    let log_start = self.log.log_start_offset();
                    let snapshot_id = if fetch_offset >= 0 && fetch_offset < log_start {
                        self.latest_snapshot_id()
                    } else {
                        None
                    };
                    let records = if should_serve_fetch_records(
                        snapshot_id.is_some(),
                        diverging.is_some(),
                        self.core.role().is_leader(),
                    ) {
                        self.serve_fetch_records(fetch_offset)
                    } else {
                        bytes::Bytes::new()
                    };
                    // Advertise the ACTUAL current leader, not `self.me`: a
                    // follower serving a Fetch must redirect the fetcher to the
                    // real leader via `current_leader`. Returning `self.me` made a
                    // follower claim leadership of the current epoch — a strict
                    // KRaft follower (the JVM) caches that, then fatal-faults when
                    // the true leader's BeginQuorumEpoch arrives ("inconsistent
                    // leader at the same epoch"). With no known leader, return
                    // `NOT_LEADER_OR_FOLLOWER` without closing the transport;
                    // the caller keeps its fetch watchdog armed and elects.
                    let leader_epoch = self.core.quorum_state().leader_epoch;
                    let resp = if let Some(advertised_leader) = self.core.quorum_state().leader_id {
                        wire::PeerResponse::Fetch {
                            leader_id: advertised_leader,
                            leader_epoch,
                            diverging,
                            snapshot_id,
                            hwm: self.log.hwm().0,
                            records,
                        }
                    } else {
                        wire::PeerResponse::FetchError {
                            leader_epoch,
                            error_code: wire::NOT_LEADER_OR_FOLLOWER,
                        }
                    };
                    let _ = reply.send(resp.encode());
                }
            }
            Inbound::FetchSnapshot { req, reply } => {
                if let Some(wire::PeerRequest::FetchSnapshot {
                    from,
                    snapshot_id,
                    position,
                    max_bytes,
                }) = wire::decode_fetch_snapshot(&req)
                {
                    // A voter catching up through KIP-630 is in contact with us
                    // even though it sends no Fetch, so score it for
                    // check-quorum before serving the chunk. Kafka does the same
                    // in `handleFetchSnapshotRequest`; without it a leader whose
                    // only reachable follower is mid-snapshot resigns under a
                    // healthy quorum.
                    self.on_event(Event::ReceiveFetchSnapshot { from });
                    let (end_offset, epoch) = snapshot_id;
                    let resp = match load_checkpoint_by_id(
                        &checkpoint_dir(&self.data_dir),
                        end_offset,
                        epoch,
                    ) {
                        Some(bytes) => {
                            // KIP-595 `FetchSnapshot` addresses a byte window of
                            // the on-disk checkpoint. Both fields are slice
                            // indices straight off the wire, so they clamp to
                            // `usize` here rather than becoming quantities.
                            let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
                            let pos = usize::try_from(position.max(0)).unwrap_or(0);
                            let chunk =
                                crate::snapshot::SnapshotReader::byte_range(&bytes, pos, max);
                            wire::PeerResponse::FetchSnapshot {
                                snapshot_id,
                                size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                                position,
                                bytes: bytes::Bytes::copy_from_slice(chunk),
                                error_code: 0,
                            }
                        }
                        None => wire::PeerResponse::FetchSnapshot {
                            snapshot_id,
                            size: 0,
                            position,
                            bytes: bytes::Bytes::new(),
                            error_code: SNAPSHOT_NOT_FOUND,
                        },
                    };
                    let _ = reply.send(resp.encode());
                }
            }
        }
    }

    /// Run an inbound `ReceiveVoteRequest` and return whether the vote was
    /// granted. The loop side effects of the other actions apply as well.
    pub fn run_vote_request(&mut self, event: Event) -> bool {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        let mut granted = false;
        let mut local = Vec::new();
        for action in actions {
            if let Action::ReplyVote {
                granted: reply_granted,
                ..
            } = action
            {
                granted = reply_granted;
            } else {
                local.push(action);
            }
        }
        self.execute_local_only(local);
        self.reconcile_timers(prev_role);
        self.publish_leader();
        granted
    }
}
