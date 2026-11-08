//! Scenarios for `tests/features/store_faults`: the fault drill a broker's
//! tiered-storage and diskless suites run against a misbehaving object store.
#![cfg(test)]

use {
    bytes::Bytes,
    krabka_bdd::Given,
    krabka_object_store::{
        ObjectOps as _, ObjectStoreClient, ObjectStoreError, PutOutcome, PutRequest,
        fault::{FaultInjectingStore, FaultKind, FaultPolicy, OpFault, StoreOp},
    },
    object_store::{ObjectMeta, ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path},
    std::{fs, num::ParseIntError, str::FromStr, sync::Arc, time::Duration},
    tempfile::TempDir,
    tokio::{
        runtime::{Builder, Runtime},
        time::Instant,
    },
};

const ANSWER: &str = "When a broker attempts a request to the store";

/// Where the drill's segment lives.
const SEGMENT_KEY: &str = "cluster/orders-0/00000000000000000000.log";

/// The bytes of the archived segment.
const SEGMENT: &[u8] = b"archived segment";

/// The size from which a segment file uploads in parts, and the size of each
/// part. The rules only depend on the file being uploaded in parts at all.
const MULTIPART_THRESHOLD: u64 = 1024;
const PART_SIZE: usize = 1024;

/// The operation classes an incident names.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Operation {
    Put,
    Read,
    Listing,
    /// One part of an upload in parts.
    Part,
    /// The completion of an upload in parts.
    Completion,
}

impl Operation {
    const fn class(self) -> StoreOp {
        match self {
            Self::Put => StoreOp::Put,
            Self::Read => StoreOp::Get,
            Self::Listing => StoreOp::List,
            Self::Part => StoreOp::PutPart,
            Self::Completion => StoreOp::Complete,
        }
    }
}

/// The S3 answer a failing call gets.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Fault {
    #[step_param("SlowDown")]
    SlowDown,
    #[step_param("InternalError")]
    InternalError,
    #[step_param("NoSuchKey")]
    NoSuchKey,
}

impl Fault {
    const fn kind(self) -> FaultKind {
        match self {
            Self::SlowDown => FaultKind::Throttled,
            Self::InternalError => FaultKind::ServerError,
            Self::NoSuchKey => FaultKind::NotFound,
        }
    }

    /// The text an operator finds in the broker's log for this fault: the status
    /// line of a transport fault, the bare source of a lost object.
    const fn status(self) -> &'static str {
        match self {
            Self::SlowDown => "503 SlowDown",
            Self::InternalError => "500 InternalError",
            Self::NoSuchKey => "injected",
        }
    }
}

/// The one-based numbers of the puts in a sequence: `none`, or a
/// comma-separated list.
#[derive(Clone, Debug)]
struct PutNumbers(Vec<usize>);

impl FromStr for PutNumbers {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == "none" {
            return Ok(Self(Vec::new()));
        }
        text.split(',')
            .map(|number| number.trim().parse())
            .collect::<Result<_, _>>()
            .map(Self)
    }
}

/// The answer of one put in a sequence, as a drill classifies it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PutAnswer {
    Stored,
    Throttled,
    Other(String),
}

/// A count of seconds, as a step writes it.
#[derive(Clone, Copy, Debug)]
struct Seconds(Duration);

impl FromStr for Seconds {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Self(Duration::from_secs(text.parse()?)))
    }
}

/// What the store answered the broker.
#[derive(Debug)]
enum Answer {
    Write(Result<PutOutcome, ObjectStoreError>),
    Writes(Vec<Result<PutOutcome, ObjectStoreError>>),
    Read(Result<Bytes, ObjectStoreError>),
    Size(Result<ObjectMeta, ObjectStoreError>),
    Listing(Result<Vec<ObjectMeta>, ObjectStoreError>),
}

impl Answer {
    /// The error of a single request, if it failed.
    fn error(&self) -> Option<&ObjectStoreError> {
        match self {
            Self::Write(result) => result.as_ref().err(),
            Self::Read(result) => result.as_ref().err(),
            Self::Size(result) => result.as_ref().err(),
            Self::Listing(result) => result.as_ref().err(),
            Self::Writes(_) => None,
        }
    }
}

/// A fault drill: a bucket the broker reaches through a store that misbehaves
/// by a fault policy, on a paused clock so a stall takes no wall time.
struct Drill {
    runtime: Runtime,
    bucket: Arc<InMemory>,
    policy: FaultPolicy,
    store: Option<Arc<FaultInjectingStore>>,
    staging: TempDir,
}

impl Drill {
    fn new() -> Self {
        Self {
            runtime: Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap(),
            bucket: Arc::new(InMemory::new()),
            policy: FaultPolicy::none(),
            store: None,
            staging: tempfile::tempdir().unwrap(),
        }
    }

    /// Put a segment straight into the bucket, past the drill.
    fn archive(&self, key: &str, bytes: &'static [u8]) {
        self.runtime
            .block_on(
                self.bucket
                    .put(&key.into(), Bytes::from_static(bytes).into()),
            )
            .unwrap();
    }

    fn misbehave(&mut self, operation: Operation, fault: OpFault) {
        self.policy = self.policy.clone().with(operation.class(), fault);
    }

    /// The misbehaving store, wrapped around the bucket on first use.
    fn store(&mut self) -> Arc<FaultInjectingStore> {
        let bucket: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(&self.bucket);
        let policy = self.policy.clone();
        Arc::clone(
            self.store
                .get_or_insert_with(|| Arc::new(FaultInjectingStore::new(bucket, policy))),
        )
    }

    fn client(&mut self) -> ObjectStoreClient { ObjectStoreClient::new(self.store()) }

    /// Run `request` against the store and report how long the store took.
    fn timed<T>(&self, request: impl Future<Output = T>) -> (T, Duration) {
        self.runtime.block_on(async {
            let started = Instant::now();
            let answer = request.await;
            (answer, started.elapsed())
        })
    }

    fn write(&mut self, key: &str) -> (Answer, Duration) {
        let client = self.client();
        let (result, elapsed) = self.timed(client.put(
            &key.into(),
            Bytes::from_static(SEGMENT),
            PutRequest::default(),
        ));
        (Answer::Write(result), elapsed)
    }

    fn upload_in_parts(&mut self, key: &str) -> Answer {
        let file = self.staging.path().join("segment.log");
        fs::write(&file, vec![b'x'; 4 * PART_SIZE]).unwrap();
        let client = self.client();
        let (result, _) = self.timed(client.put_from_path(
            &key.into(),
            &file,
            MULTIPART_THRESHOLD,
            PART_SIZE,
            PutRequest::default(),
        ));
        Answer::Write(result)
    }

    fn stored(&self, key: &str) -> Option<Bytes> {
        self.runtime.block_on(async {
            match self.bucket.get(&key.into()).await {
                Ok(object) => Some(object.bytes().await.unwrap()),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(other) => panic!("the bucket failed outside the drill: {other}"),
            }
        })
    }

    fn attempts(&mut self, operation: Operation) -> u64 { self.store().attempts(operation.class()) }
}

struct DrillScenario {
    drill: Drill,
    answer: Given<Answer>,
    elapsed: Given<Duration>,
}

#[rstest::fixture]
fn drill_scenario() -> DrillScenario {
    DrillScenario {
        drill: Drill::new(),
        answer: Given::new("answer", ANSWER),
        elapsed: Given::new("elapsed", ANSWER),
    }
}

#[rstest_bdd_macros::given("a segment has been archived")]
fn segment_archived(drill_scenario: &mut DrillScenario) {
    drill_scenario.drill.archive(SEGMENT_KEY, SEGMENT);
}

#[rstest_bdd_macros::given("the store answers every {operation} with {fault}")]
fn every_call_fails(drill_scenario: &mut DrillScenario, operation: Operation, fault: Fault) {
    drill_scenario
        .drill
        .misbehave(operation, OpFault::always(fault.kind()));
}

#[rstest_bdd_macros::given("the store answers {rate} in every thousand puts with {fault}")]
fn puts_fail_at_rate(drill_scenario: &mut DrillScenario, rate: u32, fault: Fault) {
    drill_scenario
        .drill
        .misbehave(Operation::Put, OpFault::rate(fault.kind(), rate));
}

#[rstest_bdd_macros::given("the store holds every {operation} for {seconds} seconds")]
fn every_call_stalls(drill_scenario: &mut DrillScenario, operation: Operation, seconds: Seconds) {
    drill_scenario
        .drill
        .misbehave(operation, OpFault::stall(seconds.0));
}

#[rstest_bdd_macros::when("a broker writes eight segments one after another")]
fn write_eight(drill_scenario: &mut DrillScenario) {
    let writes = (0..8)
        .map(|n| {
            let key = format!("cluster/orders-0/{n:020}.log");
            match drill_scenario.drill.write(&key).0 {
                Answer::Write(result) => result,
                other => panic!("a write answered as {other:?}"),
            }
        })
        .collect();
    drill_scenario.answer.set(Answer::Writes(writes));
}

#[rstest_bdd_macros::when("a broker attempts to write a segment")]
fn write_segment(drill_scenario: &mut DrillScenario) {
    let (answer, elapsed) = drill_scenario.drill.write(SEGMENT_KEY);
    drill_scenario.answer.set(answer);
    drill_scenario.elapsed.set(elapsed);
}

#[rstest_bdd_macros::when("a broker attempts to upload a large segment file in parts")]
fn upload_in_parts(drill_scenario: &mut DrillScenario) {
    let answer = drill_scenario.drill.upload_in_parts(SEGMENT_KEY);
    drill_scenario.answer.set(answer);
}

#[rstest_bdd_macros::when("a broker attempts to read the archived segment")]
fn read_segment(drill_scenario: &mut DrillScenario) {
    let client = drill_scenario.drill.client();
    let (result, _) = drill_scenario.drill.timed(client.get(&SEGMENT_KEY.into()));
    drill_scenario.answer.set(Answer::Read(result));
}

#[rstest_bdd_macros::when("a broker attempts to look up the size of the archived segment")]
fn look_up_size(drill_scenario: &mut DrillScenario) {
    let client = drill_scenario.drill.client();
    let (result, _) = drill_scenario.drill.timed(client.head(&SEGMENT_KEY.into()));
    drill_scenario.answer.set(Answer::Size(result));
}

#[rstest_bdd_macros::when("a broker attempts to list the store")]
fn list_store(drill_scenario: &mut DrillScenario) {
    let client = drill_scenario.drill.client();
    let (result, elapsed) = drill_scenario.drill.timed(client.list(None));
    drill_scenario.answer.set(Answer::Listing(result));
    drill_scenario.elapsed.set(elapsed);
}

#[rstest_bdd_macros::then("the throttled puts are {throttled}")]
fn throttled_puts(drill_scenario: &DrillScenario, throttled: PutNumbers) {
    assert2::assert!(let Answer::Writes(writes) = drill_scenario.answer.get());
    let answers: Vec<PutAnswer> = writes
        .iter()
        .map(|write| match write {
            Ok(_) => PutAnswer::Stored,
            Err(ObjectStoreError::Backend(message))
                if message.contains(Fault::SlowDown.status()) =>
            {
                PutAnswer::Throttled
            }
            Err(other) => PutAnswer::Other(other.to_string()),
        })
        .collect();
    let expected: Vec<PutAnswer> = (1..=writes.len())
        .map(|number| {
            if throttled.0.contains(&number) {
                PutAnswer::Throttled
            } else {
                PutAnswer::Stored
            }
        })
        .collect();
    assert2::assert!(answers == expected);
}

#[rstest_bdd_macros::then("the read returns the archived segment")]
fn read_returns_segment(drill_scenario: &DrillScenario) {
    assert2::assert!(let Answer::Read(Ok(bytes)) = drill_scenario.answer.get());
    assert2::assert!(bytes.as_ref() == SEGMENT);
}

#[rstest_bdd_macros::then("the broker sees a backend failure naming {fault}")]
fn backend_failure(drill_scenario: &DrillScenario, fault: Fault) {
    let error = drill_scenario.answer.get().error();
    assert2::assert!(let Some(ObjectStoreError::Backend(message)) = error);
    assert2::assert!(message.contains(fault.status()));
}

#[rstest_bdd_macros::then("the broker sees the archived segment as not found")]
fn not_found(drill_scenario: &DrillScenario) {
    let error = drill_scenario.answer.get().error();
    assert2::assert!(let Some(ObjectStoreError::NotFound(key)) = error);
    assert2::assert!(key.as_ref() == SEGMENT_KEY);
}

#[rstest_bdd_macros::then("the store holds no segment")]
fn holds_no_segment(drill_scenario: &DrillScenario) {
    assert2::assert!(drill_scenario.drill.stored(SEGMENT_KEY) == None);
}

#[rstest_bdd_macros::then("the store holds the segment")]
fn holds_segment(drill_scenario: &DrillScenario) {
    assert2::assert!(let Answer::Write(Ok(_)) = drill_scenario.answer.get());
    assert2::assert!(drill_scenario.drill.stored(SEGMENT_KEY) == Some(Bytes::from_static(SEGMENT)));
}

#[rstest_bdd_macros::then("the listing returns the archived segment")]
fn listing_returns_segment(drill_scenario: &DrillScenario) {
    assert2::assert!(let Answer::Listing(Ok(listed)) = drill_scenario.answer.get());
    let keys: Vec<Path> = listed.iter().map(|meta| meta.location.clone()).collect();
    assert2::assert!(keys == [Path::from(SEGMENT_KEY)]);
}

#[rstest_bdd_macros::then("the store answers {seconds} seconds late")]
fn answers_late(drill_scenario: &DrillScenario, seconds: Seconds) {
    assert2::assert!(*drill_scenario.elapsed.get() >= seconds.0);
}

#[rstest_bdd_macros::then("the store has counted {count} {operation} attempts")]
fn counted_attempts(drill_scenario: &mut DrillScenario, count: u64, operation: Operation) {
    assert2::assert!(drill_scenario.drill.attempts(operation) == count);
}

#[rstest_bdd_macros::then("the store has counted {count} {operation} attempt")]
fn counted_attempt(drill_scenario: &mut DrillScenario, count: u64, operation: Operation) {
    counted_attempts(drill_scenario, count, operation);
}

krabka_bdd::scenarios!(
    "tests/features/store_faults",
    fixtures = [drill_scenario: DrillScenario]
);
