//! `OffsetCommit` writes whose commit the actor waits for outside its turns.
//!
//! Kafka's `CoordinatorRuntime` appends a write and applies it to the shard's
//! newest state in one event, then completes the operation when the high
//! watermark passes the write. The coordinator thread is free in between: a
//! fetch, a heartbeat or another commit of the same group runs while the
//! write replicates. [`CommitWaits`] is that split for the group actor. A
//! commit's turn appends its records and stages its offsets in the group; the
//! actor loop then polls the commit's wait beside its mailbox, and a later
//! turn publishes the offsets and answers the client once the wait ends.
//!
//! An `OffsetFetch` reads the committed offsets only, the state as of the last
//! committed offset that Kafka's `scheduleReadOperation` serves, so it answers
//! at once with the offsets the group last acknowledged.

use {
    super::ErrorCode,
    crate::{
        codes,
        coordinator::unified::{
            classic_state::OffsetEntry,
            group::CoordinatorGroup,
            offsets_log::{OffsetsLog, write_timed_out},
        },
        error::BrokerError,
    },
    futures_util::{StreamExt as _, future::BoxFuture, stream::FuturesUnordered},
    krabka_protocol::records::RecordBatch,
    std::collections::HashMap,
    tokio::sync::oneshot,
};

/// The client's answer to an `OffsetCommit`.
pub(super) type CommitReply = oneshot::Sender<Result<(), ErrorCode>>;

/// One commit's place in the log, and the client waiting for it.
struct WaitingCommit {
    end_offset: i64,
    leader_epoch: i32,
    reply: CommitReply,
}

/// The end of one commit's wait for the high watermark.
pub(super) struct CommitOutcome {
    id: u64,
    result: Result<(), BrokerError>,
}

/// The commits of one group that are in the offsets log and that the high
/// watermark has not covered yet.
#[derive(Default)]
pub(super) struct CommitWaits {
    next_id: u64,
    waits: FuturesUnordered<BoxFuture<'static, CommitOutcome>>,
    /// The waiting commits, by the id their wait ends with. The replies are
    /// kept here rather than in the waits so the actor can answer them when
    /// it stops.
    waiting: HashMap<u64, WaitingCommit>,
}

impl CommitWaits {
    /// Appends one `OffsetCommit`'s records and stages its offsets in the
    /// group, without waiting for the write to commit.
    ///
    /// The append still runs in the actor's turn, so the commit takes its
    /// place in the log in mailbox order, before any tombstone a later turn
    /// writes for the same keys. An append the log refuses answers the client
    /// at once and stages nothing.
    pub(super) async fn begin(
        &mut self,
        group: &mut CoordinatorGroup,
        offsets_log: &dyn OffsetsLog,
        batch: RecordBatch,
        entries: Vec<((String, i32), OffsetEntry)>,
        reply: CommitReply,
    ) {
        let write = match offsets_log.append_pending(&group.group_id, batch).await {
            Ok(write) => write,
            Err(error) => {
                tracing::error!(group_id = %group.group_id, %error, "OffsetCommit append failed");
                let _ = reply.send(Err(codes::from_broker_error(&error)));
                return;
            }
        };
        group.stage_commit(write.end_offset, write.leader_epoch, entries);
        let id = self.next_id;
        self.next_id += 1;
        self.waiting.insert(id, WaitingCommit {
            end_offset: write.end_offset,
            leader_epoch: write.leader_epoch,
            reply,
        });
        let committed = write.committed;
        self.waits.push(Box::pin(async move {
            CommitOutcome {
                id,
                result: committed.await,
            }
        }));
    }

    /// The next commit whose wait ended. Pending for ever while none waits,
    /// so the actor loop can poll it beside its mailbox.
    pub(super) async fn next(&mut self) -> CommitOutcome {
        match self.waits.next().await {
            Some(outcome) => outcome,
            None => std::future::pending().await,
        }
    }

    /// Applies the end of one commit's wait to the group and answers its
    /// client.
    ///
    /// A committed write publishes every staged commit of its epoch up to it,
    /// in log order, so an earlier commit that finishes late never overwrites
    /// a later one. A write that timed out answers `COORDINATOR_NOT_AVAILABLE`
    /// and stays staged: its records are in the log, and a later write that
    /// commits publishes it. A write whose leadership term ended answers
    /// `NOT_COORDINATOR` and drops every staged commit.
    pub(super) fn complete(&mut self, group: &mut CoordinatorGroup, outcome: CommitOutcome) {
        let Some(WaitingCommit {
            end_offset,
            leader_epoch,
            reply,
        }) = self.waiting.remove(&outcome.id)
        else {
            return;
        };
        let answer = match outcome.result {
            Ok(()) => {
                group.publish_commits_through(end_offset, leader_epoch);
                Ok(())
            }
            Err(error) => {
                tracing::warn!(group_id = %group.group_id, %error, "OffsetCommit did not commit");
                if !write_timed_out(&error) {
                    group.abandon_pending_commits();
                }
                Err(codes::from_broker_error(&error))
            }
        };
        let _ = reply.send(answer);
    }

    /// Answers every commit still waiting with `NOT_COORDINATOR`, as Kafka's
    /// runtime fails the pending writes of a shard it unloads, so the clients
    /// look the coordinator up again instead of waiting on an actor that has
    /// stopped.
    pub(super) fn fail_all(self) {
        for waiting in self.waiting.into_values() {
            let _ = waiting.reply.send(Err(codes::NOT_COORDINATOR));
        }
    }
}
