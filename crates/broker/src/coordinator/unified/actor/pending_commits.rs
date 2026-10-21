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
            classic_state::OffsetEntry, group::CoordinatorGroup, offsets_log::OffsetsLog,
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
    /// `None` once the client has heard the write timeout: the actor still
    /// watches the write, to publish it if it commits after all.
    reply: Option<CommitReply>,
}

/// What ended one step of a commit's wait.
enum CommitEvent {
    /// The high watermark covered the write, or its term ended first.
    Ended(Result<(), BrokerError>),
    /// The write timeout passed first. The wait for the commit goes on.
    TimedOut(BoxFuture<'static, Result<(), BrokerError>>),
}

/// The end of one step of a commit's wait.
pub(super) struct CommitOutcome {
    id: u64,
    event: CommitEvent,
}

/// The commits of one group that are in the offsets log and that the high
/// watermark has not covered yet.
#[derive(Default)]
pub(super) struct CommitWaits {
    next_id: u64,
    waits: FuturesUnordered<BoxFuture<'static, CommitOutcome>>,
    /// The waiting commits, by the id their wait ends with. The replies are
    /// kept here rather than in the waits so the actor can answer them when
    /// it unloads.
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
            reply: Some(reply),
        });
        let (mut committed, deadline) = (write.committed, write.deadline);
        self.waits.push(Box::pin(async move {
            let event = tokio::select! {
                biased;
                result = &mut committed => CommitEvent::Ended(result),
                () = deadline => CommitEvent::TimedOut(committed),
            };
            CommitOutcome { id, event }
        }));
    }

    /// The next step of a commit's wait that ended. Pending for ever while
    /// none waits, so the actor loop can poll it beside its mailbox.
    pub(super) async fn next(&mut self) -> CommitOutcome {
        match self.waits.next().await {
            Some(outcome) => outcome,
            None => std::future::pending().await,
        }
    }

    /// Applies one step of a commit's wait to the group and answers its
    /// client, as Kafka's `CoordinatorRuntime` completes a write.
    ///
    /// - A committed write publishes every staged commit of its epoch up to
    ///   it, in log order, so an earlier commit that finishes late never
    ///   overwrites a later one.
    /// - A write whose timeout passed answers `COORDINATOR_NOT_AVAILABLE` and
    ///   stays staged; the actor keeps watching it, and publishes it if the
    ///   high watermark passes it after all.
    /// - A write whose leadership term ended answers `NOT_COORDINATOR` and
    ///   drops every staged commit of that term.
    pub(super) fn complete(&mut self, group: &mut CoordinatorGroup, outcome: CommitOutcome) {
        let CommitOutcome { id, event } = outcome;
        match event {
            CommitEvent::TimedOut(committed) => {
                if let Some(waiting) = self.waiting.get_mut(&id) {
                    answer(waiting.reply.take(), Err(codes::COORDINATOR_NOT_AVAILABLE));
                }
                self.waits.push(Box::pin(async move {
                    CommitOutcome {
                        id,
                        event: CommitEvent::Ended(committed.await),
                    }
                }));
            }
            CommitEvent::Ended(result) => {
                let Some(waiting) = self.waiting.remove(&id) else {
                    return;
                };
                let answered = match result {
                    Ok(()) => {
                        group.publish_commits_through(waiting.end_offset, waiting.leader_epoch);
                        Ok(())
                    }
                    Err(error) => {
                        tracing::warn!(group_id = %group.group_id, %error, "OffsetCommit did not commit");
                        group.abandon_pending_commits(waiting.leader_epoch);
                        Err(codes::from_broker_error(&error))
                    }
                };
                answer(waiting.reply, answered);
            }
        }
    }

    /// Answers every client still waiting with `NOT_COORDINATOR`, as Kafka's
    /// runtime fails the pending writes of a shard it unloads, so the clients
    /// look the coordinator up again. Nothing is watched any more.
    pub(super) fn fail_all(&mut self) {
        for (_, waiting) in self.waiting.drain() {
            answer(waiting.reply, Err(codes::NOT_COORDINATOR));
        }
        self.waits.clear();
    }

    /// Hands the commits still waiting to a task of their own when the actor
    /// stops for any reason but an unload, such as a deleted group.
    ///
    /// Their records are in the log, and the write that stopped the actor may
    /// even prove them committed, so each client hears the outcome of its own
    /// write, as it would from Kafka, rather than a guess. Nothing is
    /// published: no group is left to publish to.
    pub(super) fn detach(mut self) {
        if self.waits.is_empty() {
            return;
        }
        tokio::spawn(async move {
            while let Some(CommitOutcome { id, event }) = self.waits.next().await {
                let Some(waiting) = self.waiting.remove(&id) else {
                    continue;
                };
                let answered = match event {
                    CommitEvent::Ended(result) => {
                        result.map_err(|error| codes::from_broker_error(&error))
                    }
                    CommitEvent::TimedOut(_) => Err(codes::COORDINATOR_NOT_AVAILABLE),
                };
                answer(waiting.reply, answered);
            }
        });
    }
}

fn answer(reply: Option<CommitReply>, answered: Result<(), ErrorCode>) {
    if let Some(reply) = reply {
        let _ = reply.send(answered);
    }
}
