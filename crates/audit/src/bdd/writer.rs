//! Step definitions for the audit writer: how often it checkpoints the chain,
//! and how it spools and replays events through an audit topic outage.
//!
//! [`AuditBroker`] drives one `AuditWriter` on a current-thread Tokio runtime
//! of its own, against a failable in-memory audit topic, a spool in a
//! temporary directory, and a manually advanced clock. Steps stay synchronous
//! and the driver blocks on the runtime until the writer has done the work a
//! step asked for.

use {
    crate::{
        AuditEventClass, NodeId,
        chain::{GENESIS_HEAD, chain_hash},
        checkpoint::Checkpoint,
        ids::Seq,
        log::{
            AuditLog, AuditReceiver, AuditWriter, AuditWriterParams,
            test_support::{
                ClockedParams, DORMANT, FailableSink, REPLAY_EVERY, ROOMY_CAP, await_until, header,
                life, params_with_clock, test_signer,
            },
        },
        sink::AuditRecord,
        spool::Spool,
        stats::AuditStats,
        test_support::TestSigner,
    },
    krabka_units::prelude::{Time, TimeExt as _, secs},
    qubit_clock::{ManualMonotonicClock, MonotonicClock as _, MonotonicInstant},
    std::{mem, num::ParseIntError, str::FromStr, sync::Arc, time::Duration},
    tokio::{
        runtime::{Builder, Runtime},
        task::JoinHandle,
    },
};

/// A count the shutdown scenario's broker never reaches.
const UNREACHED_COUNT: u64 = 1_000;

/// The writer's lifecycle inside [`AuditBroker`].
enum WriterPhase {
    /// Configured but not started, so a `Given` can still set its triggers.
    Configured {
        receiver: AuditReceiver,
        params: Box<AuditWriterParams>,
    },
    Running(JoinHandle<()>),
    Stopped,
}

/// The two tickers of a running writer, as offsets from the driver's start.
struct Deadlines {
    checkpoint: Duration,
    replay: Duration,
}

/// A broker's audit pipeline: the writer, the audit topic it writes to, and
/// the spool it falls back to.
///
/// A new broker signs checkpoints, keeps both checkpoint triggers dormant and
/// replays the spool every [`REPLAY_EVERY`]. The writer starts on the first
/// step that writes events or moves time.
struct AuditBroker {
    log: Arc<AuditLog>,
    writer: WriterPhase,
    topic: Arc<FailableSink>,
    stats: Arc<AuditStats>,
    clock: Arc<ManualMonotonicClock>,
    started_at: MonotonicInstant,
    elapsed: Duration,
    deadlines: Deadlines,
    cadence: Deadlines,
    public_key: Vec<u8>,
    written: u64,
    // Drop the runtime, and the writer task with it, before the spool
    // directory goes.
    runtime: Runtime,
    _spool_directory: tempfile::TempDir,
}

impl AuditBroker {
    fn new() -> Self {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let spool_directory = tempfile::tempdir().unwrap();
        let spool = Spool::open(spool_directory.path(), ROOMY_CAP).unwrap();
        let topic = Arc::new(FailableSink::default());
        let stats = Arc::new(AuditStats::new());
        let ClockedParams { mut params, clock } = params_with_clock(
            Arc::<FailableSink>::clone(&topic),
            spool,
            Arc::clone(&stats),
        );
        let TestSigner { signer, public_key } = test_signer();
        params.signer = Some(signer);
        params.checkpoint_every_n = 0;
        params.checkpoint_every = DORMANT;
        params.replay_every = REPLAY_EVERY;
        let (log, receiver) = AuditLog::new(64);
        let started_at = clock.now();
        Self {
            log,
            writer: WriterPhase::Configured {
                receiver,
                params: Box::new(params),
            },
            topic,
            stats,
            clock,
            started_at,
            elapsed: Duration::ZERO,
            deadlines: Deadlines {
                checkpoint: Duration::ZERO,
                replay: Duration::ZERO,
            },
            cadence: Deadlines {
                checkpoint: DORMANT.to_std(),
                replay: REPLAY_EVERY.to_std(),
            },
            public_key,
            written: 0,
            runtime,
            _spool_directory: spool_directory,
        }
    }

    fn configure(&mut self) -> &mut AuditWriterParams {
        let WriterPhase::Configured { params, .. } = &mut self.writer else {
            panic!("the broker's checkpoint settings are fixed once it writes events");
        };
        params
    }

    fn checkpoints_every_events(&mut self, count: u64) {
        self.configure().checkpoint_every_n = count;
    }

    /// Sets the interval trigger, and moves the replay ticker out of its way:
    /// the driver can only see one ticker fire per step.
    fn checkpoints_every(&mut self, interval: Time) {
        let params = self.configure();
        params.checkpoint_every = interval;
        params.replay_every = DORMANT;
        self.cadence = Deadlines {
            checkpoint: interval.to_std(),
            replay: DORMANT.to_std(),
        };
    }

    fn without_signing_key(&mut self) { self.configure().signer = None; }

    /// Starts the writer, if it has not started, and waits until it has armed
    /// both tickers.
    fn start(&mut self) {
        if !matches!(self.writer, WriterPhase::Configured { .. }) {
            return;
        }
        let WriterPhase::Configured { receiver, params } =
            mem::replace(&mut self.writer, WriterPhase::Stopped)
        else {
            unreachable!("the phase was checked above");
        };
        let handle = self
            .runtime
            .spawn(AuditWriter::new(receiver, *params).run());
        self.writer = WriterPhase::Running(handle);
        self.deadlines = Deadlines {
            checkpoint: self.elapsed + self.cadence.checkpoint,
            replay: self.elapsed + self.cadence.replay,
        };
        let clock = Arc::clone(&self.clock);
        self.runtime
            .block_on(await_until("the writer armed both tickers", || {
                clock.pending_waiters() == 2
            }));
    }

    /// Emits `count` events and waits until the writer has put each on the
    /// topic or in the spool.
    ///
    /// The wait counts every record the spool holds. The scenarios that spool
    /// keep both checkpoint triggers dormant, so each of those is an event.
    fn write_events(&mut self, count: u64) {
        self.start();
        for _ in 0..count {
            self.log.emit(life(NodeId(self.written)));
            self.written += 1;
        }
        let written = self.written;
        let topic = Arc::clone(&self.topic);
        let stats = Arc::clone(&self.stats);
        self.runtime
            .block_on(await_until("the writer took every written event", || {
                data_records(&topic.inner.records()).count() as u64 + stats.depth() >= written
            }));
    }

    /// Moves the clock on by `interval` and waits until the writer has handled
    /// the ticker that came due and armed it again.
    fn advance(&mut self, interval: Duration) {
        self.start();
        self.elapsed += interval;
        self.clock
            .advance(interval)
            .expect("manual time moves forward");
        let checkpoint_due = self.deadlines.checkpoint <= self.elapsed;
        let replay_due = self.deadlines.replay <= self.elapsed;
        let rearmed = match (checkpoint_due, replay_due) {
            (false, false) => return,
            (true, false) => {
                self.deadlines.checkpoint = self.elapsed + self.cadence.checkpoint;
                self.deadlines.checkpoint
            }
            (false, true) => {
                self.deadlines.replay = self.elapsed + self.cadence.replay;
                self.deadlines.replay
            }
            (true, true) => panic!("the driver cannot tell two tickers firing at once apart"),
        };
        assert2::assert!(
            rearmed == self.deadlines.checkpoint.min(self.deadlines.replay),
            "the re-armed ticker must be the next one due, or the driver cannot see it re-arm"
        );
        let clock = Arc::clone(&self.clock);
        let started_at = self.started_at;
        self.runtime.block_on(await_until(
            "the writer re-armed the ticker that fired",
            || {
                clock
                    .next_deadline()
                    .and_then(|next| next.duration_since(started_at).ok())
                    == Some(rearmed)
            },
        ));
    }

    /// Closes the event stream and waits for the writer's final flush.
    fn stop(&mut self) {
        self.start();
        self.log.close();
        let WriterPhase::Running(handle) = mem::replace(&mut self.writer, WriterPhase::Stopped)
        else {
            panic!("the broker stopped twice");
        };
        self.runtime.block_on(handle).unwrap();
    }

    fn topic_unavailable(&self) { self.topic.set_fail(true); }

    /// The topic accepts writes again, and no replay tick has run since.
    fn topic_recovered(&self) { self.topic.allow_unlimited(); }

    /// The topic accepts writes again, and one replay tick drains the spool.
    fn topic_available(&mut self) {
        self.topic_recovered();
        self.advance(self.cadence.replay);
        let stats = Arc::clone(&self.stats);
        self.runtime
            .block_on(await_until("the replay drained the spool", || {
                stats.depth() == 0
            }));
    }

    /// One replay tick against a topic that accepts `accepted` writes and
    /// then fails again.
    fn topic_accepts_only(&mut self, accepted: u64) {
        self.topic
            .allow_n(i64::try_from(accepted).expect("a step count fits an i64"));
        self.advance(self.cadence.replay);
    }

    fn topic_records(&self) -> Vec<AuditRecord> { self.topic.inner.records() }

    fn checkpoints(&self) -> Vec<AuditRecord> {
        self.topic_records()
            .into_iter()
            .filter(|record| record.class == AuditEventClass::Checkpoint)
            .collect()
    }

    fn spool_depth(&self) -> u64 { self.stats.depth() }
}

/// The records on the topic that are chained events, not checkpoints.
fn data_records(records: &[AuditRecord]) -> impl Iterator<Item = &AuditRecord> {
    records
        .iter()
        .filter(|record| record.class != AuditEventClass::Checkpoint)
}

fn sequence_of(record: &AuditRecord) -> u64 {
    header(record, "seq")
        .expect("a chained record carries its sequence")
        .parse()
        .expect("a sequence header is a number")
}

/// A comma-separated list of chain sequence numbers, as a step writes it.
#[derive(Debug, PartialEq, Eq)]
struct Sequences(Vec<u64>);

impl FromStr for Sequences {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.split(',')
            .map(|sequence| sequence.trim().parse())
            .collect::<Result<_, _>>()
            .map(Self)
    }
}

/// What a checkpoint on the topic attests, next to what it should attest.
#[derive(Debug, PartialEq, Eq)]
struct Attestation {
    seq_high: Seq,
    chain_head: [u8; 32],
    verifies: bool,
}

/// Every scenario starts from a fresh broker, so no step fills the broker in.
struct AuditWriterScenario {
    broker: AuditBroker,
}

#[rstest::fixture]
fn audit_writer() -> AuditWriterScenario {
    AuditWriterScenario {
        broker: AuditBroker::new(),
    }
}

#[rstest_bdd_macros::given("the broker checkpoints every {count} audit events")]
fn checkpoints_every_events(audit_writer: &mut AuditWriterScenario, count: u64) {
    audit_writer.broker.checkpoints_every_events(count);
}

#[rstest_bdd_macros::given("the broker checkpoints every {seconds} seconds")]
fn checkpoints_every_seconds(audit_writer: &mut AuditWriterScenario, seconds: u32) {
    audit_writer.broker.checkpoints_every(secs(seconds));
}

#[rstest_bdd_macros::given("the broker checkpoints at a count it does not reach")]
fn checkpoints_at_unreached_count(audit_writer: &mut AuditWriterScenario) {
    audit_writer
        .broker
        .checkpoints_every_events(UNREACHED_COUNT);
}

/// The count trigger fires on every event, so only the missing key keeps
/// checkpoints off the topic.
#[rstest_bdd_macros::given("the broker has no audit signing key")]
fn has_no_signing_key(audit_writer: &mut AuditWriterScenario) {
    let broker = &mut audit_writer.broker;
    broker.checkpoints_every_events(1);
    broker.without_signing_key();
}

#[rstest_bdd_macros::given("the broker has written {count} audit events")]
fn has_written_events(audit_writer: &mut AuditWriterScenario, count: u64) {
    audit_writer.broker.write_events(count);
}

#[rstest_bdd_macros::when("the broker writes {count} audit events")]
fn writes_events(audit_writer: &mut AuditWriterScenario, count: u64) {
    audit_writer.broker.write_events(count);
}

#[rstest_bdd_macros::when("the broker writes another audit event")]
fn writes_another_event(audit_writer: &mut AuditWriterScenario) {
    audit_writer.broker.write_events(1);
}

#[rstest_bdd_macros::when("{seconds} seconds pass")]
fn seconds_pass(audit_writer: &mut AuditWriterScenario, seconds: u32) {
    audit_writer.broker.advance(secs(seconds).to_std());
}

#[rstest_bdd_macros::when("the broker stops")]
fn broker_stops(audit_writer: &mut AuditWriterScenario) { audit_writer.broker.stop(); }

#[rstest_bdd_macros::then("the checkpoint count on the audit topic is {count}")]
fn checkpoint_count(audit_writer: &AuditWriterScenario, count: usize) {
    assert2::assert!(audit_writer.broker.checkpoints().len() == count);
}

#[rstest_bdd_macros::then("the last checkpoint signs the chain head at sequence {high}")]
fn last_checkpoint_signs_head(audit_writer: &AuditWriterScenario, high: u64) {
    let broker = &audit_writer.broker;
    let records = broker.topic_records();
    let expected_head = data_records(&records)
        .filter(|record| sequence_of(record) <= high)
        .fold(GENESIS_HEAD, |head, record| {
            chain_hash(&head, sequence_of(record), &record.value)
        });
    let last = broker
        .checkpoints()
        .pop()
        .expect("the audit topic holds a checkpoint");
    let value = serde_json::from_slice(&last.value).expect("a checkpoint record is JSON");
    let checkpoint = Checkpoint::from_value(&value).expect("a checkpoint record parses");
    let attested = Attestation {
        seq_high: checkpoint.seq_high,
        chain_head: checkpoint.chain_head,
        verifies: checkpoint.verify(&broker.public_key),
    };
    assert2::assert!(
        attested
            == Attestation {
                seq_high: Seq(high),
                chain_head: expected_head,
                verifies: true,
            }
    );
}

#[rstest_bdd_macros::given("the audit topic is unavailable")]
fn topic_is_unavailable(audit_writer: &AuditWriterScenario) {
    audit_writer.broker.topic_unavailable();
}

#[rstest_bdd_macros::given("the broker has spooled {count} audit events during an outage")]
fn has_spooled_events(audit_writer: &mut AuditWriterScenario, count: u64) {
    let broker = &mut audit_writer.broker;
    broker.topic_unavailable();
    broker.write_events(count);
}

#[rstest_bdd_macros::given("the broker has spooled an audit event during an outage")]
fn has_spooled_an_event(audit_writer: &mut AuditWriterScenario) {
    let broker = &mut audit_writer.broker;
    broker.topic_unavailable();
    broker.write_events(1);
}

#[rstest_bdd_macros::given("a replay stopped after the audit topic accepted {accepted} of them")]
fn replay_cut_short(audit_writer: &mut AuditWriterScenario, accepted: u64) {
    audit_writer.broker.topic_accepts_only(accepted);
}

#[rstest_bdd_macros::given("the audit topic has recovered before the next replay")]
fn topic_recovered_before_replay(audit_writer: &AuditWriterScenario) {
    audit_writer.broker.topic_recovered();
}

#[rstest_bdd_macros::when("the audit topic becomes available")]
fn topic_becomes_available(audit_writer: &mut AuditWriterScenario) {
    audit_writer.broker.topic_available();
}

#[rstest_bdd_macros::then("the audit topic holds the events at sequences {sequences}")]
fn topic_holds_sequences(audit_writer: &AuditWriterScenario, sequences: Sequences) {
    let records = audit_writer.broker.topic_records();
    let on_topic = Sequences(data_records(&records).map(sequence_of).collect());
    assert2::assert!(on_topic == sequences);
}

#[rstest_bdd_macros::then("the audit topic holds no events")]
fn topic_holds_no_events(audit_writer: &AuditWriterScenario) {
    let records = audit_writer.broker.topic_records();
    let on_topic = Sequences(data_records(&records).map(sequence_of).collect());
    assert2::assert!(on_topic == Sequences(Vec::new()));
}

#[rstest_bdd_macros::then("the spool holds {count} events")]
fn spool_holds(audit_writer: &AuditWriterScenario, count: u64) {
    assert2::assert!(audit_writer.broker.spool_depth() == count);
}

krabka_bdd::scenarios!(
    "tests/features/checkpoint_cadence",
    fixtures = [audit_writer: AuditWriterScenario]
);

krabka_bdd::scenarios!(
    "tests/features/spool_replay",
    fixtures = [audit_writer: AuditWriterScenario]
);
