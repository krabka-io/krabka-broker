//! `CodSpeed` microbenchmarks for the broker's produce hot path.
//!
//! The verbatim pass-through is the design's central produce claim: a v2 batch
//! that satisfies the predicate reaches the log as the producer's own bytes,
//! with no decode, re-encode or re-CRC. The owned fallback behind it is a
//! complete second implementation, and any change that quietly widens the
//! predicate's misses moves every batch onto it. This suite puts a number on
//! that difference so the move is visible.
//!
//! Each shape runs through [`krabka_broker::produce_hot_path`], which is
//! `prepare_batch`, the writer's `build_produce_data`, and the log append
//! behind them — the same three functions the produce pipeline calls, without
//! the writer actor's hand-off.
//!
//! Three shapes, three paths:
//!   * `verbatim` — a native v2 batch on a producer-pass-through topic;
//!   * `owned` — the same bytes forced down the owned fallback;
//!   * `legacy_v1` — a v0/v1 `MessageSet`, which the fallback up-converts.
//!
//! `bench_ratio` prints the owned-over-verbatim and legacy-over-verbatim
//! ratios per shape, so a run answers "what does the fallback cost" without
//! reading the criterion report.
//!
//! `bench_produce_acks_all` measures the other axis: one Produce request
//! covering many partitions, at `acks=1` and at `acks=-1`, end to end against
//! a booted broker. The three shapes above stop at the log append and say
//! nothing about what a request pays per partition around it. The `acks=-1`
//! arm is the one that used to grow with the partition count: the handler
//! awaited each partition's high-watermark gate inline, before the next
//! partition was appended, where Kafka registers one `DelayedProduce` over the
//! whole request. The difference against the `acks=1` arm at the same
//! partition count is what that gate costs now.
//!
//! `bench_produce_conversion` measures the owned path end to end: one Produce
//! to a topic that forces gzip or zstd on an uncompressed batch, and one that
//! up-converts a legacy v1 `MessageSet`, each through the handler's
//! `max.message.bytes` and `segment.bytes` gates as well as the append.

use {
    assert2::assert,
    bytes::{Bytes, BytesMut},
    criterion::{BatchSize, Criterion, Throughput},
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle, log_write_back,
        metrics::BrokerMetrics,
        produce_hot_path::{
            HotPathSettings, PathChoice, ProducePath, TimestampPolicy, append_one_batch,
        },
        testing,
    },
    krabka_client_core::Client,
    krabka_compression::RecordDecompressionPolicy,
    krabka_log::{Log, LogConfig},
    krabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            produce_request::PartitionProduceData,
        },
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch, RecordsPayload},
    },
    krabka_records_legacy::Magic,
    std::{
        cell::RefCell,
        mem,
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::runtime::Builder,
};

mod support;
use support::{
    BatchShape, BenchError, BoundedResource, FirstError, LEADER_EPOCH, LOG_BUDGET, SHAPES, WARMUP,
    bounded_resource,
};

fn settings(metrics: &BrokerMetrics) -> HotPathSettings<'_> {
    HotPathSettings {
        topic_name: Arc::from("bench-topic"),
        // Producer pass-through: the topic asks for no recompression, which is
        // the configuration the verbatim path needs.
        topic_compression: None,
        // Kafka's timestamp defaults with neither window bounded: the
        // producer's own clock, and `max_timestamp` folded from the records in
        // the walk that validates them. Every batch here agrees with its
        // header, so none pays for the header rewrite.
        timestamps: TimestampPolicy::default(),
        decompression_policy: RecordDecompressionPolicy::default(),
        metrics,
        leader_epoch: LEADER_EPOCH,
        version: 13,
    }
}

fn make_batch(records: i32, payload: usize) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: records - 1,
        ..RecordBatch::default()
    };
    for i in 0..records {
        batch.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i:08}"))),
            // A repeating non-uniform pattern, so a compressed variant of this
            // suite would not measure an unrealistically compressible payload.
            value: Some(Bytes::from(
                (0..=250_u8).cycle().take(payload).collect::<Vec<u8>>(),
            )),
            ..Record::default()
        });
    }
    batch
}

/// The wire bytes of one partition's records field, as a producer sends it.
fn v2_records(records: i32, payload: usize) -> Result<Bytes, BenchError> {
    let batch = make_batch(records, payload);
    let mut buf = BytesMut::with_capacity(batch.encoded_batch_len());
    batch.encode_batch(&mut buf)?;
    Ok(buf.freeze())
}

/// The same records as a v1 `MessageSet`, which an older-message-format client
/// still sends over a v≥3 produce.
fn legacy_v1_records(records: i32, payload: usize) -> Result<Bytes, BenchError> {
    Ok(krabka_records_legacy::v2_to_legacy(
        &make_batch(records, payload),
        Magic::V1,
    )?)
}

/// A log that starts over once it has taken [`LOG_BUDGET`] bytes.
///
/// Every reset opens a fresh temp dir, so a long criterion run never grows the
/// log past the budget and never measures a log whose segment count keeps
/// climbing.
struct ProduceLog {
    _dir: TempDir,
    log: Log,
}

impl ProduceLog {
    fn new() -> Result<Self, BenchError> {
        let dir = tempfile::tempdir()?;
        let log = Log::open(dir.path(), LogConfig::default())?;
        Ok(Self { _dir: dir, log })
    }
}

type BoundedLog = BoundedResource<ProduceLog>;

/// The measured region: one records field through prepare, writer-data build
/// and append.
///
/// The path assertion is what keeps a ratio meaningful. Were a change to the
/// verbatim predicate to send the `verbatim` case down the fallback, the two
/// columns would converge and the table would report the fallback as free.
/// This fails the run instead. It compares two enum discriminants against an
/// append that costs tens of microseconds, so it stays inside the measured
/// region rather than buying back a nanosecond by checking only sometimes.
fn append(
    store: &mut BoundedLog,
    payload: Bytes,
    choice: PathChoice,
    expected: ProducePath,
    settings: &HotPathSettings<'_>,
) -> Result<(), BenchError> {
    let path = append_one_batch(payload, choice, settings, &mut store.get_mut().log)
        .map_err(|code| format!("the append was refused with error code {code}"))?;
    assert!(path == expected, "{choice:?} took the {path:?} path");
    Ok(())
}

/// Append `records` once, returning only the time the append took.
///
/// The log rotation and the `Bytes` clone sit outside the timer. The clone is
/// a refcount bump, and the produce pipeline hands the hot path an equivalent
/// zero-copy view of the request frame.
///
/// This is [`bench_ratio`]'s own timer. The criterion group leaves the timing
/// to criterion, which is what lets `CodSpeed` instrument it.
fn append_once(
    store: &mut BoundedLog,
    records: &Bytes,
    choice: PathChoice,
    expected: ProducePath,
    settings: &HotPathSettings<'_>,
) -> Result<Duration, BenchError> {
    store.rotate_if_full(records.len())?;
    let payload = records.clone();
    let start = Instant::now();
    append(store, payload, choice, expected, settings)?;
    Ok(start.elapsed())
}

/// Append `records` `iters` times, returning only the time the appends took.
fn timed_appends(
    records: &Bytes,
    choice: PathChoice,
    expected: ProducePath,
    iters: u64,
) -> Result<Duration, BenchError> {
    let metrics = BrokerMetrics::new();
    let settings = settings(&metrics);
    let mut store = bounded_resource()
        .budget(LOG_BUDGET)
        .factory(ProduceLog::new)
        .call()?;
    for _ in 0..WARMUP {
        append_once(&mut store, records, choice, expected, &settings)?;
    }
    let mut elapsed = Duration::ZERO;
    for _ in 0..iters {
        elapsed += append_once(&mut store, records, choice, expected, &settings)?;
    }
    Ok(elapsed)
}

/// One path a shape is measured on: its report name, the wire bytes it
/// appends, how the broker is told to dispatch them, and the path through
/// `prepare_batch` that this case has to actually take.
struct MeasuredPath {
    name: &'static str,
    wire: Bytes,
    choice: PathChoice,
    expected: ProducePath,
}

/// The three paths a shape is measured on, in report order.
fn paths(records: i32, payload: usize) -> Result<[MeasuredPath; 3], BenchError> {
    let v2 = v2_records(records, payload)?;
    Ok([
        MeasuredPath {
            name: "verbatim",
            wire: v2.clone(),
            choice: PathChoice::Dispatch,
            expected: ProducePath::Verbatim,
        },
        MeasuredPath {
            name: "owned",
            wire: v2,
            choice: PathChoice::ForceOwned,
            expected: ProducePath::Owned,
        },
        MeasuredPath {
            name: "legacy_v1",
            wire: legacy_v1_records(records, payload)?,
            choice: PathChoice::Dispatch,
            expected: ProducePath::Owned,
        },
    ])
}

/// Installs the log write-back that the broker installs at startup, so the
/// logs below pace their dirty data as the broker's logs do. It runs first.
fn install_log_write_back(_c: &mut Criterion) { log_write_back::install(); }

/// Time one append per iteration, with everything around it left untimed.
///
/// `iter_batched` rather than `iter_custom`, on purpose. `iter_custom` hands
/// the suite its own timer, which is the one API the `CodSpeed` compat shim
/// cannot instrument: its `Bencher::iter_custom` prints a skip line saying
/// custom iterations are unsupported and never calls the closure, so under
/// instrumentation every case here would be skipped and the regression this
/// suite exists to catch would go unmeasured. `iter_batched` is instrumented,
/// and it keeps the same split — the log rotation and the `Bytes` clone run in
/// the setup closure, outside the measurement, and only the append is timed.
fn bench_produce_hot_path(c: &mut Criterion) -> Result<(), BenchError> {
    let failure = FirstError::default();
    {
        let mut group = c.benchmark_group("broker/produce");

        for BatchShape {
            label: shape,
            records,
            payload,
        } in SHAPES
        {
            for MeasuredPath {
                name: path,
                wire,
                choice,
                expected,
            } in paths(records, payload)?
            {
                group.throughput(Throughput::Bytes(wire.len() as u64));
                group.bench_function(format!("{shape}/{path}"), |b| {
                    let metrics = BrokerMetrics::new();
                    let settings = settings(&metrics);
                    // Both closures reach the log: the setup rotates it, the
                    // routine appends into it.
                    let store = RefCell::new(
                        failure.keep(
                            bounded_resource()
                                .budget(LOG_BUDGET)
                                .factory(ProduceLog::new)
                                .call(),
                        ),
                    );
                    b.iter_batched(
                        || {
                            let mut store = store.borrow_mut();
                            let rotated = store
                                .as_mut()
                                .map(|store| store.rotate_if_full(wire.len()).map(|_| ()));
                            drop(store);
                            failure.keep(rotated?).map(|()| wire.clone())
                        },
                        |payload| {
                            let mut store = store.borrow_mut();
                            if let (Some(payload), Some(store)) = (payload, store.as_mut()) {
                                failure.keep(append(store, payload, choice, expected, &settings));
                            }
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }

        group.finish();
    }
    failure.into_result()
}

/// Print the owned-over-verbatim and legacy-over-verbatim ratios per shape.
///
/// Criterion compares a benchmark against its own previous run, not against a
/// sibling, so the one number this suite exists for — what the fallback costs
/// against the pass-through — has to be computed here.
fn bench_ratio(_c: &mut Criterion) -> Result<(), BenchError> {
    /// Appends per measurement run.
    const SAMPLES: u32 = 100;
    /// Measurement runs per case, of which the fastest is the one reported.
    ///
    /// A single run is noise-dominated on a machine that is doing anything
    /// else — enough, observed, to invert a ratio and report the fallback as
    /// cheaper than the pass-through. Interference only ever adds time, so the
    /// minimum over a few runs is the estimator it cannot pull the wrong way.
    const RUNS: u32 = 5;

    println!(
        "\nbroker/produce — nanoseconds per batch (fastest of {RUNS} runs), and the cost of the fallback"
    );
    println!(
        "{:<14} {:>12} {:>12} {:>12} {:>16} {:>16}",
        "shape", "verbatim", "owned", "legacy_v1", "owned/verbatim", "legacy/verbatim"
    );
    for BatchShape {
        label: shape,
        records,
        payload,
    } in SHAPES
    {
        let [verbatim, owned, legacy] = paths(records, payload)?.map(
            |MeasuredPath {
                 wire,
                 choice,
                 expected,
                 ..
             }| {
                let mut fastest = f64::INFINITY;
                for _ in 0..RUNS {
                    let elapsed = timed_appends(&wire, choice, expected, u64::from(SAMPLES))?;
                    fastest = fastest.min(elapsed.as_secs_f64() * 1.0e9 / f64::from(SAMPLES));
                }
                Ok::<f64, BenchError>(fastest)
            },
        );
        let (verbatim, owned, legacy) = (verbatim?, owned?, legacy?);
        println!(
            "{shape:<14} {verbatim:>12.0} {owned:>12.0} {legacy:>12.0} {:>15.2}x {:>15.2}x",
            owned / verbatim,
            legacy / verbatim
        );
    }
    println!();
    Ok(())
}

/// Partition counts one `acks=-1` request covers: a single partition, a
/// modest producer's spread, and the wide request whose per-partition cost is
/// the thing worth watching.
const ACKS_ALL_PARTITIONS: [i32; 3] = [1, 8, 32];

/// The topic the `acks=-1` arm produces to, and the partition count it is
/// created with, which is the largest of [`ACKS_ALL_PARTITIONS`].
const ACKS_ALL_TOPIC: &str = "bench-acks-all";

/// A booted broker, a connected client, and the topic's id, held together for
/// the lifetime of the `acks=-1` arm. The temp dir is dropped last, with the
/// log directory in it.
struct BenchBroker {
    broker: BrokerHandle,
    client: Client,
    topic_id: WireUuid,
    _dir: TempDir,
}

/// Boot a broker with a single-replica topic of the widest partition count.
///
/// Replication factor 1 on purpose: this arm measures what the handler does
/// per partition around the gate, not how long a follower takes. On rf=1 the
/// high watermark is already at the log end when the gate reads it, so what
/// the `acks=-1` arm times is the gate machinery itself.
async fn boot_bench_broker() -> Result<BenchBroker, BenchError> {
    let widest = *ACKS_ALL_PARTITIONS
        .iter()
        .max()
        .ok_or("the partition counts are empty")?;
    let (mut fixture, topic_ids) = boot_with_topics(&[BenchTopic {
        name: ACKS_ALL_TOPIC,
        partitions: widest,
        configs: &[],
    }])
    .await?;
    fixture.topic_id = first_topic_id(&topic_ids)?;
    Ok(fixture)
}

/// The first of `topic_ids`, which a fixture is created with.
fn first_topic_id(topic_ids: &[WireUuid]) -> Result<WireUuid, BenchError> {
    Ok(*topic_ids.first().ok_or("no topic was created")?)
}

/// One single-replica topic a bench broker boots with.
struct BenchTopic<'a> {
    name: &'a str,
    partitions: i32,
    /// `(key, value)` topic configs.
    configs: &'a [(&'a str, &'a str)],
}

/// Boot a broker and create each of `topics`, returning the topic ids in the
/// same order.
///
/// The fixture's own `topic_id` is the first topic's.
async fn boot_with_topics(
    topics: &[BenchTopic<'_>],
) -> Result<(BenchBroker, Vec<WireUuid>), BenchError> {
    let dir = TempDir::new()?;
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await?;
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder().bootstrap(bootstrap).build().await?;
    let topic_ids = create_topics(&broker, &client, topics).await?;
    Ok((
        BenchBroker {
            broker,
            client,
            topic_id: first_topic_id(&topic_ids)?,
            _dir: dir,
        },
        topic_ids,
    ))
}

/// Create each of `topics`, wait for its partitions to exist locally, and
/// return the topic ids in the same order.
async fn create_topics(
    broker: &BrokerHandle,
    client: &Client,
    topics: &[BenchTopic<'_>],
) -> Result<Vec<WireUuid>, BenchError> {
    let created = client
        .send(CreateTopicsRequest {
            topics: topics
                .iter()
                .map(|topic| CreatableTopic {
                    name: topic.name.into(),
                    num_partitions: topic.partitions,
                    replication_factor: 1,
                    configs: topic
                        .configs
                        .iter()
                        .map(|(key, value)| CreatableTopicConfig {
                            name: (*key).into(),
                            value: Some((*value).into()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await?;
    for topic in &created.topics {
        assert!(topic.error_code == 0, "{topic:?}");
    }
    for topic in topics {
        for index in 0..topic.partitions {
            broker.wait_until_partition_present(topic.name, index).await;
        }
    }
    // Produce v13 carries only the topic id, so resolve each once.
    let metadata = client
        .send(MetadataRequest {
            topics: Some(
                topics
                    .iter()
                    .map(|topic| MetadataRequestTopic {
                        name: Some(topic.name.into()),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        })
        .await?;
    topics
        .iter()
        .map(|wanted| {
            metadata
                .topics
                .iter()
                .find(|topic| topic.name.as_deref() == Some(wanted.name))
                .map(|topic| topic.topic_id)
                .ok_or_else(|| format!("{} is not in the metadata", wanted.name).into())
        })
        .collect()
}

/// Send one Produce covering `partitions` partitions and check every row.
async fn produce_across_partitions(
    fixture: &BenchBroker,
    partitions: i32,
    acks: i16,
    wire: &Bytes,
) -> Result<(), BenchError> {
    let response = fixture
        .client
        .send(
            testing::produce_partitions_request()
                .acks(acks)
                .timeout_ms(5_000)
                .topic(ACKS_ALL_TOPIC.into())
                .topic_id(fixture.topic_id)
                .partitions(
                    (0..partitions)
                        .map(|index| PartitionProduceData {
                            index,
                            records: Some(RecordsPayload::Raw(wire.clone())),
                            ..Default::default()
                        })
                        .collect(),
                )
                .call(),
        )
        .await?;
    let topic = response
        .responses
        .first()
        .ok_or("Produce answered no topic")?;
    for row in &topic.partition_responses {
        assert!(row.error_code == 0);
    }
    Ok(())
}

/// One Produce request across a sweep of partition counts, at `acks=1` and at
/// `acks=-1`.
fn bench_produce_acks_all(c: &mut Criterion) -> Result<(), BenchError> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let fixture = runtime.block_on(boot_bench_broker())?;
    // One modest batch per partition: the request's partition count is the
    // variable, so the bytes per partition stay small and constant.
    let wire = v2_records(10, 128)?;
    let failure = FirstError::default();

    {
        let mut group = c.benchmark_group("broker/produce_acks");
        for partitions in ACKS_ALL_PARTITIONS {
            for (label, acks) in [("acks1", 1_i16), ("acks_all", -1_i16)] {
                let per_request = u64::try_from(wire.len())? * u64::try_from(partitions)?;
                group.throughput(Throughput::Bytes(per_request));
                group.bench_function(format!("{partitions}part/{label}"), |b| {
                    // `block_on` rather than criterion's `to_async`, which needs
                    // its `async_tokio` feature: the request is one await, and the
                    // runtime is the same one the fixture booted on.
                    b.iter(|| {
                        failure.keep(runtime.block_on(produce_across_partitions(
                            &fixture, partitions, acks, &wire,
                        )));
                    });
                });
            }
        }
        group.finish();
    }

    runtime.block_on(fixture.broker.shutdown());
    failure.into_result()
}

/// One converting topic the full-handler arm produces to: its report label,
/// the base name its [`RotatingTopic`] generations take, the
/// `compression.type` it forces, and what its producer sends.
struct ConversionCase {
    label: &'static str,
    topic: &'static str,
    compression_type: &'static str,
    wire: fn(i32, usize) -> Result<Bytes, BenchError>,
}

/// The produce shapes that reach the owned path through the handler: a topic
/// whose `compression.type` forces gzip or zstd on a producer that sent the
/// batch uncompressed, and a legacy v1 `MessageSet` up-converted on a
/// producer-pass-through topic.
const CONVERSION_CASES: [ConversionCase; 3] = [
    ConversionCase {
        label: "forced_gzip",
        topic: "bench-forced-gzip",
        compression_type: "gzip",
        wire: v2_records,
    },
    ConversionCase {
        label: "forced_zstd",
        topic: "bench-forced-zstd",
        compression_type: "zstd",
        wire: v2_records,
    },
    ConversionCase {
        label: "legacy_v1",
        topic: "bench-legacy-v1",
        compression_type: "producer",
        wire: legacy_v1_records,
    },
];

/// Send one single-partition Produce and check its row.
async fn produce_one(
    fixture: &BenchBroker,
    topic: &str,
    topic_id: WireUuid,
    wire: &Bytes,
) -> Result<(), BenchError> {
    let response = fixture
        .client
        .send(
            testing::produce_partitions_request()
                .acks(1)
                .timeout_ms(5_000)
                .topic(topic.into())
                .topic_id(topic_id)
                .partitions(vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::Raw(wire.clone())),
                    ..Default::default()
                }])
                .call(),
        )
        .await?;
    let row = response
        .responses
        .first()
        .and_then(|topic| topic.partition_responses.first())
        .ok_or("Produce answered no partition")?;
    assert!(row.error_code == 0, "{row:?}");
    Ok(())
}

/// The owned produce path end to end, through the handler and its size gates.
///
/// `bench_produce_hot_path` stops at `prepare_batch` and the append. A batch
/// the broker has to re-encode also crosses the `max.message.bytes` and
/// `segment.bytes` gates in between, each of which measures the stored form,
/// and only an encode can measure a compressed one. This arm times one
/// single-partition `acks=1` Produce per iteration against a booted broker, so
/// what it reports includes every encode the request pays for.
fn bench_produce_conversion(c: &mut Criterion) -> Result<(), BenchError> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let (fixture, _) = runtime.block_on(boot_with_topics(&[BenchTopic {
        name: "bench-conversion-boot",
        partitions: 1,
        configs: &[],
    }]))?;
    let failure = FirstError::default();

    {
        let mut group = c.benchmark_group("broker/produce_conversion");
        for BatchShape {
            label: shape,
            records,
            payload,
        } in SHAPES
        {
            for case in &CONVERSION_CASES {
                let wire = (case.wire)(records, payload)?;
                let topic =
                    RefCell::new(runtime.block_on(RotatingTopic::create(&fixture, case, 0))?);
                group.throughput(Throughput::Bytes(wire.len() as u64));
                group.bench_function(format!("{shape}/{}", case.label), |b| {
                    b.iter_batched(
                        || {
                            let mut topic = topic.borrow_mut();
                            let rotated =
                                runtime.block_on(topic.rotate_if_full(&fixture, case, wire.len()));
                            let target = (topic.name.clone(), topic.topic_id);
                            drop(topic);
                            failure.keep(rotated).map(|()| target)
                        },
                        |target| {
                            if let Some((name, topic_id)) = target {
                                failure.keep(
                                    runtime.block_on(produce_one(&fixture, &name, topic_id, &wire)),
                                );
                            }
                        },
                        BatchSize::PerIteration,
                    );
                });
                runtime.block_on(topic.into_inner().delete(&fixture))?;
            }
        }
        group.finish();
    }

    runtime.block_on(fixture.broker.shutdown());
    failure.into_result()
}

/// The topic one conversion case produces to, replaced by a fresh one once it
/// has taken [`LOG_BUDGET`] bytes.
///
/// Each case appends to its partition for as long as criterion measures it,
/// which for the 100 KiB shapes is gigabytes. Replacing the topic outside the
/// timed region keeps every case's log, and the disk under it, bounded and
/// comparable, as the hot-path arm's [`BoundedLog`] does for its bare log.
struct RotatingTopic {
    name: String,
    topic_id: WireUuid,
    generation: u32,
    written: usize,
}

impl RotatingTopic {
    async fn create(
        fixture: &BenchBroker,
        case: &ConversionCase,
        generation: u32,
    ) -> Result<Self, BenchError> {
        let name = format!("{}-{generation}", case.topic);
        let topic_ids = create_topics(&fixture.broker, &fixture.client, &[BenchTopic {
            name: &name,
            partitions: 1,
            configs: &[("compression.type", case.compression_type)],
        }])
        .await?;
        Ok(Self {
            name,
            topic_id: first_topic_id(&topic_ids)?,
            generation,
            written: 0,
        })
    }

    /// Replace the topic when `incoming` more bytes would take it past the
    /// budget, and count them.
    async fn rotate_if_full(
        &mut self,
        fixture: &BenchBroker,
        case: &ConversionCase,
        incoming: usize,
    ) -> Result<(), BenchError> {
        if self.written + incoming > LOG_BUDGET {
            let next = Self::create(fixture, case, self.generation + 1).await?;
            mem::replace(self, next).delete(fixture).await?;
        }
        self.written += incoming;
        Ok(())
    }

    async fn delete(self, fixture: &BenchBroker) -> Result<(), BenchError> {
        let response = fixture
            .client
            .send(DeleteTopicsRequest {
                topics: vec![DeleteTopicState {
                    name: Some(self.name.clone()),
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await?;
        for topic in &response.responses {
            assert!(topic.error_code == 0, "{topic:?}");
        }
        Ok(())
    }
}

krabka_macros::bench_main!(
    install_log_write_back,
    bench_produce_hot_path,
    bench_produce_acks_all,
    bench_produce_conversion,
    bench_ratio
);
