//! Service of inbound peer RPCs: each request body is decoded, run through the
//! consensus core, and answered on its oneshot, including the leader's Fetch
//! and `FetchSnapshot` serve paths.

use krabka_ids::Offset;

use super::{Engine, replication::should_serve_fetch_records};
use crate::kraft::{
    action::Action,
    event::Event,
    transport::{Inbound, wire},
};

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
            Inbound::Vote {
                req,
                version,
                reply,
            } => {
                if let Some(response) = self.answer_vote(&req, version) {
                    let _ = reply.send(response);
                }
            }
            Inbound::BeginQuorumEpoch {
                req,
                version,
                reply,
            } => {
                if let Some(response) = self.answer_begin_quorum_epoch(&req, version) {
                    let _ = reply.send(response);
                }
            }
            Inbound::EndQuorumEpoch {
                req,
                version,
                reply,
            } => {
                if let Some(response) = self.answer_end_quorum_epoch(&req, version) {
                    let _ = reply.send(response);
                }
            }
            Inbound::Fetch {
                req,
                version,
                reply,
            } => {
                if let Some(response) = self.answer_fetch(&req, version) {
                    let _ = reply.send(response);
                }
            }
            Inbound::FetchSnapshot {
                req,
                version,
                reply,
            } => {
                if let Some(response) = self.answer_fetch_snapshot(&req, version) {
                    let _ = reply.send(response);
                }
            }
        }
    }

    /// Answer one Fetch as Kafka's `KafkaRaftClient.tryCompleteFetchRequest`
    /// does, or `None` when the body does not decode.
    ///
    /// `validateLeaderOnlyRequest` runs first: a request from another epoch,
    /// or one that reaches a replica that is not the leader, is answered with
    /// its error, the leader and epoch this replica knows, and that leader's
    /// endpoint, and changes nothing here. So a follower or an observer that
    /// is asked redirects the fetcher to the real leader rather than claiming
    /// the epoch, and the fetcher addresses that leader by the endpoint the
    /// response names, not by the responder's address. Only the leader runs
    /// the fetch through the core and serves records.
    fn answer_fetch(&mut self, req: &[u8], version: i16) -> Option<bytes::Bytes> {
        let request = wire::decode_fetch_request(req, version)?;
        let from = wire::node_from_wire(wire::fetch_replica_id(&request, version));
        let partition = request.topics.first()?.partitions.first()?;
        let current_leader_epoch = partition.current_leader_epoch;
        let fetch_epoch = wire::epoch_from_wire(partition.last_fetched_epoch);
        let fetch_offset = partition.fetch_offset;
        let replica_directory_id = uuid::Uuid::from_bytes(partition.replica_directory_id.0);
        let log_start = self.log.log_start_offset();
        if let Some(error_code) = self.leader_only_request_error(current_leader_epoch) {
            return Some(
                wire::FetchAnswer {
                    error_code,
                    leader: self.quorum_leader(),
                    diverging: None,
                    snapshot_id: None,
                    hwm: -1,
                    log_start_offset: log_start.0,
                    records: bytes::Bytes::new(),
                }
                .encode(version),
            );
        }
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
        // AdvanceHighWatermark for the leader. Encode the divergence into the
        // response; apply HWM locally. The diverging epoch is a reply only:
        // the leader's log keeps its records.
        let mut diverging = None;
        for action in &actions {
            if let Action::ReplyDivergingEpoch(point) = action {
                diverging = Some(*point);
            }
        }
        self.execute(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
        // Serve the follower the batch bytes it is missing: every batch
        // at/after its `fetch_offset` up to our log end (KRaft replicates up
        // to the leader's log end, not just the HWM -- the HWM rides
        // separately in the response). A divergent fetch sends none (the
        // follower truncates first, then re-fetches). If the follower's fetch
        // offset is below our pruned log-start, it cannot replicate from the
        // log -- point it at the latest snapshot instead (KIP-630).
        // `fetch_offset` arrives raw on the KIP-595 wire; wrap it into the
        // `KraftLog` offset domain to compare against log bounds.
        let fetch_offset = Offset(fetch_offset);
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
        // Handling the fetch may have ended this leadership (a check-quorum
        // resignation), so the leader view is read again for the answer.
        Some(
            wire::FetchAnswer {
                error_code: 0,
                leader: self.quorum_leader(),
                diverging,
                snapshot_id,
                hwm: self.log.hwm().0,
                log_start_offset: self.log.log_start_offset().0,
                records,
            }
            .encode(version),
        )
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
