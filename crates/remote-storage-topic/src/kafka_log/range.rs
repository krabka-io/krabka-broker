//! Bounded offset-range reads for
//! [`MetadataEventLog::visit_range`](crate::log::MetadataEventLog::visit_range).
//!
//! A range read fetches one page at a time from the partition leader and hands
//! each record to the caller before it fetches the next page, so it holds one
//! page at most. It steps over compacted offsets by the batch progress each
//! `Fetch` reports, not by the records it returns. On a transient failure,
//! such as a leader change or a dropped connection, it looks the leader up
//! again and resumes at the offset it had reached. The read has no deadline of
//! its own, and the caller bounds it.
//!
//! [`RangeFetcher`] is the seam between that loop and the broker: the Kafka log
//! implements it over its broker pool, and the tests here script it.

use {
    crate::{
        error::MetadataLogError,
        log::{MetadataEventRecord, RangeVisitor},
    },
    krabka_client_core::{ClientError, FetchPartitionResult, FetchedRecord},
    std::time::Duration,
    tokio::time,
    tracing::warn,
};

/// The leader lookup and the `Fetch` that a range read makes.
pub(super) trait RangeFetcher: Sync {
    /// A connection to a partition leader.
    type Leader: Send + Sync;

    /// Look up the current leader of `partition` and connect to it.
    async fn leader(&self, partition: i32) -> Result<Self::Leader, RangeReadFailure>;

    /// Fetch one page of `partition` from `leader`, from `offset`.
    async fn fetch(
        &self,
        leader: &Self::Leader,
        partition: i32,
        offset: i64,
    ) -> Result<FetchPartitionResult, ClientError>;
}

/// A failed leader lookup or `Fetch` of a range read.
#[derive(Debug)]
pub(super) struct RangeReadFailure {
    /// Whether a fresh leader lookup and another `Fetch` can succeed.
    retriable: bool,
    error: MetadataLogError,
}

impl RangeReadFailure {
    /// A failure that another leader lookup can clear.
    pub(super) fn retriable(message: String) -> Self {
        Self {
            retriable: true,
            error: MetadataLogError::Other(message),
        }
    }

    /// A client failure, retriable when [`range_fetch_retriable`] says so.
    pub(super) fn client(context: &str, error: &ClientError) -> Self {
        Self {
            retriable: range_fetch_retriable(error),
            error: MetadataLogError::Other(format!("{context}: {error}")),
        }
    }
}

/// Whether a range read can look the leader up again and repeat a request
/// that failed with `error`.
///
/// The partition codes are the ones Kafka's consumer answers with a metadata
/// refresh or a plain retry in `FetchCollector.handleInitializeErrors`, plus
/// the leader, timeout and network codes a request can fail with. A
/// connection that failed, closed or timed out can be made again.
/// Authentication, authorization, version and decoding failures cannot clear
/// by themselves, so the read stops on them.
fn range_fetch_retriable(error: &ClientError) -> bool {
    match error {
        // UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
        // LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, REQUEST_TIMED_OUT,
        // REPLICA_NOT_AVAILABLE, NETWORK_EXCEPTION, KAFKA_STORAGE_ERROR,
        // FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH, OFFSET_NOT_AVAILABLE,
        // UNKNOWN_TOPIC_ID, INCONSISTENT_TOPIC_ID.
        ClientError::Server { error_code } => matches!(
            *error_code,
            -1 | 3 | 5 | 6 | 7 | 9 | 13 | 56 | 74 | 75 | 78 | 100 | 103
        ),
        ClientError::Connect { .. }
        | ClientError::Tls { .. }
        | ClientError::Sasl { .. }
        | ClientError::Disconnected
        | ClientError::Timeout(_)
        | ClientError::Io(_) => true,
        _ => false,
    }
}

/// Where a range read goes after one `Fetch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeStep {
    /// Fetch again from this offset.
    Continue(i64),
    /// The read reached the end of its range.
    Done,
    /// The fetch returned no batch past its offset, so the read cannot
    /// advance.
    Stalled,
}

/// The step after a `Fetch` at `current` that reported `next_offset`.
///
/// `next_offset` is one past the last offset of every batch the fetch decoded,
/// filtered or not, so a compacted hole cannot stall the read the way a cursor
/// derived from the returned records would.
fn next_range_step(current: i64, end: i64, next_offset: Option<i64>) -> RangeStep {
    match next_offset {
        Some(next) if next >= end => RangeStep::Done,
        Some(next) if next > current => RangeStep::Continue(next),
        _ => RangeStep::Stalled,
    }
}

/// The records of a page fetched at `offset` that lie in `[offset, end)`.
///
/// A fetch starts at the batch that holds `offset`, so a page can carry
/// records before it, and its last batch can run past `end`.
fn page_records(
    partition: i32,
    offset: i64,
    end: i64,
    records: Vec<FetchedRecord>,
) -> impl Iterator<Item = MetadataEventRecord> {
    records
        .into_iter()
        .filter(move |record| offset <= record.offset && record.offset < end)
        .map(move |record| MetadataEventRecord {
            partition,
            offset: record.offset,
            tombstone: record.value.is_none(),
            key: record.key,
            payload: record.value.unwrap_or_default(),
        })
}

/// What a range read needs to know about the topic it reads.
pub(super) struct RangeTopic<'a> {
    pub(super) name: &'a str,
    pub(super) partition_count: i32,
    /// How long to wait before a retry.
    pub(super) retry_backoff: Duration,
}

/// Hand every record of `partition` in `[start, end)` to `visit`, one fetched
/// page at a time.
pub(super) async fn visit_range_pages<F: RangeFetcher>(
    fetcher: &F,
    topic: &RangeTopic<'_>,
    partition: i32,
    start: i64,
    end: i64,
    visit: &mut RangeVisitor<'_>,
) -> Result<(), MetadataLogError> {
    if partition < 0 || partition >= topic.partition_count {
        return Err(MetadataLogError::PartitionOutOfRange {
            partition,
            count: topic.partition_count,
        });
    }
    if start >= end {
        return Ok(());
    }
    let mut leader = None;
    let mut offset = start;
    loop {
        let page = match fetch_page(fetcher, topic.name, &mut leader, partition, offset).await {
            Ok(page) => page,
            Err(failure) if failure.retriable => {
                warn!(
                    topic = topic.name,
                    partition,
                    offset,
                    error = %failure.error,
                    "range read: looking the leader up again"
                );
                time::sleep(topic.retry_backoff).await;
                continue;
            }
            Err(failure) => return Err(failure.error),
        };
        let step = next_range_step(offset, end, page.next_offset);
        page_records(partition, offset, end, page.records).try_for_each(&mut *visit)?;
        match step {
            RangeStep::Continue(next) => offset = next,
            RangeStep::Done => return Ok(()),
            RangeStep::Stalled => {
                return Err(MetadataLogError::Other(format!(
                    "{} partition {partition} Fetch at {offset} returned no batch before {end}",
                    topic.name
                )));
            }
        }
    }
}

/// One page from the cached leader, or from a freshly looked-up one. A
/// failure drops the cached leader, so the next attempt looks it up again.
async fn fetch_page<F: RangeFetcher>(
    fetcher: &F,
    topic: &str,
    leader: &mut Option<F::Leader>,
    partition: i32,
    offset: i64,
) -> Result<FetchPartitionResult, RangeReadFailure> {
    let connection = match leader.take() {
        Some(connection) => connection,
        None => fetcher.leader(partition).await?,
    };
    let page = fetcher
        .fetch(&connection, partition, offset)
        .await
        .map_err(|error| {
            RangeReadFailure::client(
                &format!("{topic} partition {partition} Fetch at {offset} failed"),
                &error,
            )
        })?;
    *leader = Some(connection);
    Ok(page)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        bytes::Bytes,
        krabka_units::prelude::secs,
        std::{collections::VecDeque, future, io::ErrorKind, mem, ops::Range, sync::Mutex},
    };

    const TOPIC: RangeTopic<'static> = RangeTopic {
        name: "__diskless_wal_index",
        partition_count: 2,
        retry_backoff: Duration::from_millis(200),
    };

    #[derive(Default)]
    struct RangeProgressScenario {
        current: i64,
        end: i64,
        next: Option<i64>,
        step: Option<RangeStep>,
        error: Option<ClientError>,
        retriable: Option<bool>,
        page: Vec<FetchedRecord>,
        records: Option<Vec<MetadataEventRecord>>,
    }
    #[rstest::fixture]
    fn range_progress() -> RangeProgressScenario {
        let page = Vec::new();
        RangeProgressScenario {
            page,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "a range at offset {current} ending before {end} receives next batch offset {next}"
    )]
    fn batch_progress(
        range_progress: &mut RangeProgressScenario,
        current: i64,
        end: i64,
        next: String,
    ) {
        range_progress.current = current;
        range_progress.end = end;
        range_progress.next = if next == "none" {
            None
        } else {
            Some(next.parse().unwrap())
        };
    }
    #[rstest_bdd_macros::when("the range read chooses its next offset")]
    fn choose_next_offset(range_progress: &mut RangeProgressScenario) {
        range_progress.step = Some(next_range_step(
            range_progress.current,
            range_progress.end,
            range_progress.next,
        ));
    }
    #[rstest_bdd_macros::then("range progress is {expected}")]
    fn check_range_progress(range_progress: &RangeProgressScenario, expected: String) {
        let expected = match expected.as_str() {
            "done" => RangeStep::Done,
            "stalled" => RangeStep::Stalled,
            _ => RangeStep::Continue(
                expected
                    .strip_prefix("continue at ")
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
        };
        check!(range_progress.step.unwrap() == expected);
    }
    #[rstest_bdd_macros::given("the range fetch failed with server error {code}")]
    fn range_server_error(range_progress: &mut RangeProgressScenario, code: i16) {
        range_progress.error = Some(ClientError::Server { error_code: code });
    }
    #[rstest_bdd_macros::given("the range fetch failed with {failure}")]
    fn range_connection_error(range_progress: &mut RangeProgressScenario, failure: String) {
        range_progress.error = Some(match failure.as_str() {
            "disconnected" => ClientError::Disconnected,
            "timeout" => ClientError::Timeout(secs(30)),
            "connection reset" => ClientError::Io(ErrorKind::ConnectionReset.into()),
            "connection refused" => ClientError::Connect {
                addr: "127.0.0.1:9092".parse().unwrap(),
                source: ErrorKind::ConnectionRefused.into(),
            },
            "invalid config" => ClientError::InvalidConfig("bad".into()),
            _ => panic!("unknown transport failure: {failure}"),
        });
    }
    #[rstest_bdd_macros::when("the range reader decides whether to retry")]
    fn classify_fetch_error(range_progress: &mut RangeProgressScenario) {
        range_progress.retriable = Some(range_fetch_retriable(
            range_progress.error.as_ref().unwrap(),
        ));
    }
    #[rstest_bdd_macros::then("retrying the range fetch is {expected}")]
    fn check_fetch_retry(range_progress: &RangeProgressScenario, expected: bool) {
        check!(range_progress.retriable.unwrap() == expected);
    }
    #[rstest_bdd_macros::given(
        "a fetched page has records before inside and after offsets 4 through 6 including a tombstone at 6"
    )]
    fn page_overlapping_range(range_progress: &mut RangeProgressScenario) {
        range_progress.page = vec![
            fetched(3, Some(b"before")),
            fetched(4, Some(b"first")),
            fetched(6, None),
            fetched(7, Some(b"past")),
        ];
    }
    #[rstest_bdd_macros::when("the page is filtered to partition 1 offsets 4 through 6")]
    fn filter_page(range_progress: &mut RangeProgressScenario) {
        range_progress.records =
            Some(page_records(1, 4, 7, mem::take(&mut range_progress.page)).collect());
    }
    #[rstest_bdd_macros::then(
        "only offset 4 and the tombstone at 6 are delivered with their original keys"
    )]
    fn check_page_records(range_progress: &RangeProgressScenario) {
        assert!(
            range_progress.records.as_ref().unwrap()
                == &vec![record(4, Some(b"first")), record(6, None)]
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/range_progress", fixtures=[range_progress: RangeProgressScenario]);

    struct RangeReadScenario {
        fetcher: Option<ScriptedFetcher>,
        partition: i32,
        start: i64,
        end: i64,
        results: Vec<Result<Vec<MetadataEventRecord>, MetadataLogError>>,
        visited: Vec<i64>,
    }
    #[rstest::fixture]
    fn range_read() -> RangeReadScenario {
        let results = Vec::new();
        RangeReadScenario {
            fetcher: None,
            partition: 1,
            start: 0,
            end: 4,
            results,
            visited: Vec::new(),
        }
    }
    #[rstest_bdd_macros::given(
        "leader 1 serves offsets 0 and 1 then moves to leader 2 serving offsets 2 and 3"
    )]
    fn moving_leader(range_read: &mut RangeReadScenario) {
        range_read.fetcher = Some(ScriptedFetcher::new([Ok(1), Ok(2)], [
            Ok(page(0..2)),
            Err(ClientError::Server { error_code: 6 }),
            Ok(page(2..4)),
        ]));
    }
    #[rstest_bdd_macros::given(
        "the partition initially has no leader then broker 3 serves offsets 0 and 1"
    )]
    fn initially_missing_leader(range_read: &mut RangeReadScenario) {
        range_read.end = 2;
        range_read.fetcher = Some(ScriptedFetcher::new(
            [
                Err(RangeReadFailure::retriable("no leader yet".into())),
                Ok(3),
            ],
            [Ok(page(0..2))],
        ));
    }
    #[rstest_bdd_macros::given(
        "the first range fetch fails with corrupt message and the next leader lookup is unauthorized"
    )]
    fn fatal_range_failures(range_read: &mut RangeReadScenario) {
        range_read.fetcher = Some(ScriptedFetcher::new(
            [
                Ok(1),
                Err(RangeReadFailure::client(
                    "Metadata failed",
                    &ClientError::Server { error_code: 29 },
                )),
            ],
            [Ok(page(0..2)), Err(ClientError::Server { error_code: 2 })],
        ));
    }
    #[rstest_bdd_macros::given("a range from 4 to 10 receives an empty page with no next offset")]
    fn stalled_range(range_read: &mut RangeReadScenario) {
        range_read.start = 4;
        range_read.end = 10;
        range_read.fetcher = Some(ScriptedFetcher::new([Ok(1)], [Ok(FetchPartitionResult {
            records: Vec::new(),
            next_offset: None,
        })]));
    }
    #[rstest_bdd_macros::given("a range from 0 to 6 has a page containing offsets 0 through 2")]
    fn visitor_page(range_read: &mut RangeReadScenario) {
        range_read.end = 6;
        range_read.fetcher = Some(ScriptedFetcher::new([Ok(1)], [Ok(page(0..3))]));
    }
    #[rstest_bdd_macros::given("an empty range from 5 to 5")]
    fn empty_range(range_read: &mut RangeReadScenario) {
        range_read.start = 5;
        range_read.end = 5;
        range_read.fetcher = Some(ScriptedFetcher::new([], []));
    }
    #[rstest_bdd_macros::given(
        "a range from 0 to 1 targets partition {partition} of a two-partition topic"
    )]
    fn outside_partition(range_read: &mut RangeReadScenario, partition: i32) {
        range_read.partition = partition;
        range_read.end = 1;
        range_read.fetcher = Some(ScriptedFetcher::new([], []));
    }
    #[rstest_bdd_macros::when("the requested range is read")]
    async fn read_requested_range(range_read: &mut RangeReadScenario) {
        range_read.results.push(
            read(
                range_read.fetcher.as_ref().unwrap(),
                range_read.partition,
                range_read.start,
                range_read.end,
            )
            .await,
        );
    }
    #[rstest_bdd_macros::when("the requested range is read twice")]
    async fn read_twice(range_read: &mut RangeReadScenario) {
        read_requested_range(range_read).await;
        read_requested_range(range_read).await;
    }
    #[rstest_bdd_macros::when("the range visitor closes on its first record")]
    async fn visitor_closes(range_read: &mut RangeReadScenario) {
        let seen = &mut range_read.visited;
        let result = visit_range_pages(
            range_read.fetcher.as_ref().unwrap(),
            &TOPIC,
            1,
            0,
            6,
            &mut |record| {
                seen.push(record.offset);
                Err(MetadataLogError::Closed)
            },
        )
        .await;
        range_read.results.push(result.map(|()| Vec::new()));
    }
    #[rstest_bdd_macros::then(
        "offsets 0 through 3 are delivered once and fetching resumes from 2 on leader 2"
    )]
    fn check_leader_resume(range_read: &RangeReadScenario) {
        assert!(
            range_read.results[0].as_ref().unwrap()
                == &(0..4)
                    .map(|offset| record(offset, Some(b"v")))
                    .collect::<Vec<_>>()
        );
        assert!(
            range_read.fetcher.as_ref().unwrap().calls()
                == vec![
                    Call::Leader,
                    Call::Fetch {
                        leader: 1,
                        offset: 0
                    },
                    Call::Fetch {
                        leader: 1,
                        offset: 2
                    },
                    Call::Leader,
                    Call::Fetch {
                        leader: 2,
                        offset: 2
                    }
                ]
        );
    }
    #[rstest_bdd_macros::then(
        "offsets 0 and 1 are delivered after two leader lookups and one fetch from broker 3"
    )]
    fn check_leader_discovery(range_read: &RangeReadScenario) {
        assert!(
            range_read.results[0].as_ref().unwrap()
                == &vec![record(0, Some(b"v")), record(1, Some(b"v"))]
        );
        assert!(
            range_read.fetcher.as_ref().unwrap().calls()
                == vec![Call::Leader, Call::Leader, Call::Fetch {
                    leader: 3,
                    offset: 0
                }]
        );
    }
    #[rstest_bdd_macros::then(
        "both reads stop with their precise fetch and metadata authorization errors"
    )]
    fn check_fatal_read_errors(range_read: &RangeReadScenario) {
        check!(
            range_read.results[0].as_ref().unwrap_err().to_string()
                == "metadata log error: __diskless_wal_index partition 1 Fetch at 2 failed: protocol error from server: 2"
        );
        check!(
            range_read.results[1].as_ref().unwrap_err().to_string()
                == "metadata log error: Metadata failed: protocol error from server: 29"
        );
    }
    #[rstest_bdd_macros::then("the read reports that no batch was returned at 4 before 10")]
    fn check_stalled_read(range_read: &RangeReadScenario) {
        assert!(
            range_read.results[0].as_ref().unwrap_err().to_string()
                == "metadata log error: __diskless_wal_index partition 1 Fetch at 4 returned no batch before 10"
        );
    }
    #[rstest_bdd_macros::then(
        "only offset 0 reaches the visitor and the closed error stops further fetches"
    )]
    fn check_closed_visitor(range_read: &RangeReadScenario) {
        check!(matches!(
            range_read.results[0],
            Err(MetadataLogError::Closed)
        ));
        check!(range_read.visited == vec![0]);
        check!(
            range_read.fetcher.as_ref().unwrap().calls()
                == vec![Call::Leader, Call::Fetch {
                    leader: 1,
                    offset: 0
                }]
        );
    }
    #[rstest_bdd_macros::then("no records are delivered and no network requests are made")]
    fn check_empty_range(range_read: &RangeReadScenario) {
        check!(range_read.results[0].as_ref().unwrap().is_empty());
        check!(range_read.fetcher.as_ref().unwrap().calls().is_empty());
    }
    #[rstest_bdd_macros::then("the partition is rejected with count 2 without network requests")]
    fn check_outside_partition(range_read: &RangeReadScenario) {
        check!(
            matches!(&range_read.results[0],Err(MetadataLogError::PartitionOutOfRange{partition,count:2}) if *partition==range_read.partition)
        );
        check!(range_read.fetcher.as_ref().unwrap().calls().is_empty());
    }

    fn fetched(offset: i64, value: Option<&'static [u8]>) -> FetchedRecord {
        FetchedRecord {
            offset,
            key: Some(Bytes::from_static(b"key")),
            value: value.map(Bytes::from_static),
            timestamp: 0,
            headers: Vec::new(),
        }
    }

    fn record(offset: i64, value: Option<&'static [u8]>) -> MetadataEventRecord {
        MetadataEventRecord {
            partition: 1,
            offset,
            key: Some(Bytes::from_static(b"key")),
            payload: value.map_or_else(Bytes::new, Bytes::from_static),
            tombstone: value.is_none(),
        }
    }

    /// One call a range read made through [`ScriptedFetcher`].
    #[derive(Debug, PartialEq, Eq)]
    enum Call {
        Leader,
        Fetch { leader: i32, offset: i64 },
    }

    /// A [`RangeFetcher`] that answers from scripts and records its calls.
    /// A leader is its broker id.
    struct ScriptedFetcher {
        leaders: Mutex<VecDeque<Result<i32, RangeReadFailure>>>,
        pages: Mutex<VecDeque<Result<FetchPartitionResult, ClientError>>>,
        calls: Mutex<Vec<Call>>,
    }

    impl ScriptedFetcher {
        fn new(
            leaders: impl IntoIterator<Item = Result<i32, RangeReadFailure>>,
            pages: impl IntoIterator<Item = Result<FetchPartitionResult, ClientError>>,
        ) -> Self {
            Self {
                leaders: Mutex::new(leaders.into_iter().collect()),
                pages: Mutex::new(pages.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Call> { mem::take(&mut *self.calls.lock().unwrap()) }
    }

    impl RangeFetcher for ScriptedFetcher {
        type Leader = i32;

        fn leader(&self, _partition: i32) -> impl Future<Output = Result<i32, RangeReadFailure>> {
            self.calls.lock().unwrap().push(Call::Leader);
            future::ready(self.leaders.lock().unwrap().pop_front().unwrap())
        }

        fn fetch(
            &self,
            leader: &i32,
            _partition: i32,
            offset: i64,
        ) -> impl Future<Output = Result<FetchPartitionResult, ClientError>> {
            self.calls.lock().unwrap().push(Call::Fetch {
                leader: *leader,
                offset,
            });
            future::ready(self.pages.lock().unwrap().pop_front().unwrap())
        }
    }

    fn page(offsets: Range<i64>) -> FetchPartitionResult {
        FetchPartitionResult {
            next_offset: Some(offsets.end),
            records: offsets.map(|offset| fetched(offset, Some(b"v"))).collect(),
        }
    }

    /// Every record the read hands over, or the error it stops with.
    async fn read(
        fetcher: &ScriptedFetcher,
        partition: i32,
        start: i64,
        end: i64,
    ) -> Result<Vec<MetadataEventRecord>, MetadataLogError> {
        let mut records = Vec::new();
        visit_range_pages(fetcher, &TOPIC, partition, start, end, &mut |record| {
            records.push(record);
            Ok(())
        })
        .await?;
        Ok(records)
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "A moved leader resumes at the first undelivered offset"
    )]
    #[tokio::test(start_paused = true)]
    async fn a_retriable_fetch_failure_looks_the_leader_up_again_and_resumes(
        range_read: RangeReadScenario,
    ) {
        let _ = range_read;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "Leader discovery retries until a leader exists"
    )]
    #[tokio::test(start_paused = true)]
    async fn a_partition_without_a_leader_is_looked_up_again(range_read: RangeReadScenario) {
        let _ = range_read;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "Fatal fetch and lookup errors retain their causes"
    )]
    #[tokio::test]
    async fn a_fatal_failure_stops_the_read_with_its_cause(range_read: RangeReadScenario) {
        let _ = range_read;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "A page without batch progress fails the range"
    )]
    #[tokio::test]
    async fn a_fetch_that_makes_no_progress_stalls_the_read(range_read: RangeReadScenario) {
        let _ = range_read;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "Visitor closure stops delivery immediately"
    )]
    #[tokio::test]
    async fn a_visitor_error_stops_the_read_before_the_next_record(range_read: RangeReadScenario) {
        let _ = range_read;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "An empty range performs no I O"
    )]
    #[tokio::test]
    async fn an_empty_read_sends_nothing(range_read: RangeReadScenario) { let _ = range_read; }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/range_reads/reads.feature",
        name = "An invalid partition performs no I O"
    )]
    #[tokio::test]
    async fn out_of_range_read_sends_nothing(range_read: RangeReadScenario, partition: i32) {
        let _ = (range_read, partition);
    }
}
