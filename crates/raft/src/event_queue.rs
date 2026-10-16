//! The controller's event-queue measurements: Kafka's
//! `ControllerEventManager` `EventQueueTimeMs`, `EventQueueProcessingTimeMs`
//! and `AvgIdleRatio`, and `KafkaController`'s
//! `EventQueueOperationsStartedCount` and `EventQueueOperationsTimedOutCount`.
//!
//! Kafka's `QuorumController` runs every operation as an event on one queue
//! thread. Krabka's engine loop is that thread: each turn handles one command
//! from the engine's mpsc or one timer, so a turn is one event. A command
//! waited on the mpsc for its queue time; a timer has none, as a Kafka
//! deferred event sets `DOES_NOT_UPDATE_QUEUE_TIME`. A write that parks for
//! its commit keeps its processing time running until the commit reply, as a
//! Kafka `ControllerWriteEvent` does.
//!
//! The engine reports to an [`EventQueueRecorder`]. The broker registers its
//! metrics as one; [`NoEventQueueRecorder`] is the default.

use {
    std::sync::Arc,
    tokio::time::{Duration, Instant},
};

/// Receives the engine loop's event-queue measurements. Every method is
/// called from the engine task, so an implementation must not block.
pub trait EventQueueRecorder: Send + Sync {
    /// A command waited `waited` between its send and the turn that started
    /// it: Kafka's `EventQueueTimeMs`.
    fn queue_time(&self, waited: Duration);
    /// An event ran for `took`, to its commit reply for a write: Kafka's
    /// `EventQueueProcessingTimeMs`.
    fn processing_time(&self, took: Duration);
    /// The loop started an event: Kafka's `EventQueueOperationsStartedCount`.
    fn operation_started(&self);
    /// The loop reached a command whose caller had already stopped waiting
    /// for the reply: Kafka's `EventQueueOperationsTimedOutCount`. Kafka drops
    /// an event whose deadline passed in the queue; the engine still runs the
    /// command, so it also counts as started.
    fn operation_timed_out(&self);
    /// The loop waited `idle` for its next event. Its rate is Kafka's
    /// `AvgIdleRatio`.
    fn idle_time(&self, idle: Duration);
}

/// The recorder of a controller nobody measures.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoEventQueueRecorder;

impl EventQueueRecorder for NoEventQueueRecorder {
    fn queue_time(&self, _waited: Duration) {}

    fn processing_time(&self, _took: Duration) {}

    fn operation_started(&self) {}

    fn operation_timed_out(&self) {}

    fn idle_time(&self, _idle: Duration) {}
}

/// How a command reached the turn that runs it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Arrival {
    /// When the command was sent.
    pub(crate) enqueued_at: Instant,
    /// Whether its caller had stopped waiting for the reply by now.
    pub(crate) caller_gone: bool,
}

/// Meters the engine loop's turns for an [`EventQueueRecorder`].
pub(crate) struct EventQueueMeter {
    recorder: Arc<dyn EventQueueRecorder>,
    idle_since: Instant,
}

impl EventQueueMeter {
    /// A meter whose loop starts idle now.
    pub(crate) fn new(recorder: Arc<dyn EventQueueRecorder>) -> Self {
        Self {
            recorder,
            idle_since: Instant::now(),
        }
    }

    /// The loop woke for one event: a command, with its `arrival`, or a timer
    /// with `None`. Records the idle wait that ended and the start, and
    /// returns the timer of the event's processing.
    pub(crate) fn start(&self, arrival: Option<Arrival>) -> EventTimer {
        let now = Instant::now();
        self.recorder
            .idle_time(now.saturating_duration_since(self.idle_since));
        self.recorder.operation_started();
        if let Some(arrival) = arrival {
            self.recorder
                .queue_time(now.saturating_duration_since(arrival.enqueued_at));
            if arrival.caller_gone {
                self.recorder.operation_timed_out();
            }
        }
        EventTimer {
            started: now,
            recorder: Arc::clone(&self.recorder),
        }
    }

    /// The turn is over; the loop is idle from now.
    pub(crate) fn finish_turn(&mut self) { self.idle_since = Instant::now(); }
}

/// The running processing time of one event. Dropping it records the time
/// since the event started, so a write keeps it in its commit waiter until it
/// replies.
pub(crate) struct EventTimer {
    started: Instant,
    recorder: Arc<dyn EventQueueRecorder>,
}

impl Drop for EventTimer {
    fn drop(&mut self) { self.recorder.processing_time(self.started.elapsed()); }
}

#[cfg(test)]
pub(crate) mod test_support {
    use {super::EventQueueRecorder, std::sync::Mutex, tokio::time::Duration};

    /// What a [`RecordingEventQueue`] has been told, in arrival order.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub(crate) struct EventQueueRecord {
        pub(crate) queue_times: Vec<Duration>,
        pub(crate) processing_times: Vec<Duration>,
        pub(crate) started: u64,
        pub(crate) timed_out: u64,
        pub(crate) idle_times: Vec<Duration>,
    }

    /// An [`EventQueueRecorder`] that keeps every measurement.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingEventQueue(Mutex<EventQueueRecord>);

    impl RecordingEventQueue {
        pub(crate) fn record(&self) -> EventQueueRecord {
            self.0.lock().expect("the record is not poisoned").clone()
        }

        fn with(&self, update: impl FnOnce(&mut EventQueueRecord)) {
            update(&mut self.0.lock().expect("the record is not poisoned"));
        }
    }

    impl EventQueueRecorder for RecordingEventQueue {
        fn queue_time(&self, waited: Duration) { self.with(|r| r.queue_times.push(waited)); }

        fn processing_time(&self, took: Duration) { self.with(|r| r.processing_times.push(took)); }

        fn operation_started(&self) { self.with(|r| r.started += 1); }

        fn operation_timed_out(&self) { self.with(|r| r.timed_out += 1); }

        fn idle_time(&self, idle: Duration) { self.with(|r| r.idle_times.push(idle)); }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            Arrival, EventQueueMeter,
            test_support::{EventQueueRecord, RecordingEventQueue},
        },
        assert2::check,
        std::sync::Arc,
        tokio::time::{Duration, Instant, advance},
    };

    /// One turn under a paused clock. A command, when there is one, is sent
    /// at the start; the previous turn then runs for `busy`, the loop idles
    /// for `idle`, and this turn runs for `runs`.
    struct TurnCase {
        name: &'static str,
        command: Option<Caller>,
        busy: Duration,
        idle: Duration,
        runs: Duration,
        expected: EventQueueRecord,
    }

    /// Whether the command's caller is still waiting when the turn starts.
    #[derive(Clone, Copy)]
    enum Caller {
        Waiting,
        Gone,
    }

    #[tokio::test(start_paused = true)]
    async fn one_turn_records_kafkas_event_queue_measurements() {
        let ms = Duration::from_millis;
        let cases = [
            TurnCase {
                name: "a timer has no queue time",
                command: None,
                busy: ms(9),
                idle: ms(40),
                runs: ms(3),
                expected: EventQueueRecord {
                    queue_times: vec![],
                    processing_times: vec![ms(3)],
                    started: 1,
                    timed_out: 0,
                    idle_times: vec![ms(40)],
                },
            },
            TurnCase {
                name: "a command queued behind a busy turn",
                command: Some(Caller::Waiting),
                busy: ms(25),
                idle: ms(0),
                runs: ms(7),
                expected: EventQueueRecord {
                    queue_times: vec![ms(25)],
                    processing_times: vec![ms(7)],
                    started: 1,
                    timed_out: 0,
                    idle_times: vec![ms(0)],
                },
            },
            TurnCase {
                name: "a command whose caller left still runs, and counts as timed out",
                command: Some(Caller::Gone),
                busy: ms(30_000),
                idle: ms(5),
                runs: ms(1),
                expected: EventQueueRecord {
                    queue_times: vec![ms(30_005)],
                    processing_times: vec![ms(1)],
                    started: 1,
                    timed_out: 1,
                    idle_times: vec![ms(5)],
                },
            },
        ];
        for case in cases {
            let recorder = Arc::new(RecordingEventQueue::default());
            let mut meter = EventQueueMeter::new(Arc::clone(&recorder) as _);
            let enqueued_at = Instant::now();
            advance(case.busy).await;
            meter.finish_turn();
            advance(case.idle).await;
            let timer = meter.start(case.command.map(|caller| Arrival {
                enqueued_at,
                caller_gone: matches!(caller, Caller::Gone),
            }));
            advance(case.runs).await;
            drop(timer);
            check!(recorder.record() == case.expected, "{}", case.name);
        }
    }
}
