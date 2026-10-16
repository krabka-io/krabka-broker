//! Tests for the engine loop's Kafka event-queue measurements: every turn is
//! a started event, a write keeps its processing time open until its commit
//! reply, a command whose caller left counts as timed out, and the waits
//! between turns are idle time.

use {
    crate::{
        event_queue::test_support::{EventQueueRecord, RecordingEventQueue},
        kraft::{
            KraftController,
            controller::test_support::{
                build, commit_pending, elect_leader_with_helper, spawn_submit, topic_record,
            },
            transport::Command,
            types::NodeId,
        },
    },
    assert2::check,
    std::sync::Arc,
    tokio::{
        sync::oneshot,
        time::{Duration, sleep},
    },
};

/// A three-voter leader whose event queue reports to the returned recorder.
async fn measured_three_voter_leader()
-> (KraftController, tempfile::TempDir, Arc<RecordingEventQueue>) {
    let recorder = Arc::new(RecordingEventQueue::default());
    let (ctrl, dir) = build()
        .event_queue_recorder(Arc::clone(&recorder) as _)
        .call();
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    (ctrl, dir, recorder)
}

/// Events the loop started whose processing time is still running.
fn open_events(record: &EventQueueRecord) -> u64 {
    record.started - u64::try_from(record.processing_times.len()).expect("a small count")
}

#[tokio::test]
async fn a_write_keeps_its_processing_time_open_until_its_commit_reply() {
    let (ctrl, _dir, recorder) = measured_three_voter_leader().await;
    check!(open_events(&recorder.record()) == 0);

    let submit = spawn_submit(&ctrl, topic_record("measured"));
    let parked = Duration::from_millis(30);
    sleep(parked).await;
    check!(
        open_events(&recorder.record()) == 1,
        "the submit's turn ended, but its write waits for a follower to commit it"
    );

    commit_pending(&ctrl, NodeId(2)).await;
    submit
        .await
        .expect("the submit task")
        .expect("the follower's fetch commits the topic");
    let record = recorder.record();
    check!(open_events(&record) == 0);
    check!(
        record
            .processing_times
            .iter()
            .max()
            .copied()
            .unwrap_or_default()
            >= parked,
        "the write's processing time covers its wait for the commit"
    );
    ctrl.shutdown().await;
}

#[tokio::test]
async fn a_command_whose_caller_left_counts_as_timed_out() {
    let (ctrl, _dir, recorder) = measured_three_voter_leader().await;
    let before = recorder.record();
    check!(before.timed_out == 0);

    let (reply, abandoned) = oneshot::channel();
    drop(abandoned);
    ctrl.cmd_tx
        .send(Command::QuorumStateSnapshot { reply })
        .await
        .expect("the engine is running");
    // The engine takes its commands in order, so this reply means it has
    // run the abandoned one.
    ctrl.quorum_state().await.expect("a quorum snapshot");

    let after = recorder.record();
    check!(after.timed_out == 1);
    check!(
        after.queue_times.len() >= before.queue_times.len() + 2,
        "both commands record a queue time"
    );
    ctrl.shutdown().await;
}

#[tokio::test]
async fn the_waits_between_turns_are_idle_time() {
    let (ctrl, _dir, recorder) = measured_three_voter_leader().await;
    let idle = |record: &EventQueueRecord| record.idle_times.iter().sum::<Duration>();
    let before = idle(&recorder.record());

    let quiet = Duration::from_millis(60);
    sleep(quiet).await;
    // A command ends the wait in progress, so the idle time includes it.
    ctrl.quorum_state().await.expect("a quorum snapshot");

    check!(
        idle(&recorder.record()).saturating_sub(before) >= quiet / 2,
        "a leader with no traffic spends most of a quiet spell idle"
    );
    ctrl.shutdown().await;
}
