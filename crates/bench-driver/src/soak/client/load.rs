//! The load: one paced producer per path and one live consumer group per path
//! and isolation level.
//!
//! Every producer numbers what it sends from zero and writes
//! [`record_tag`]`(producer, seq)` at the head of each value, so a reader can
//! say which send a record came from. Sends are pipelined -- each tick
//! enqueues one record and the acknowledgement is awaited beside the next
//! ticks -- because a sequential send caps the rate at one per round trip,
//! below the 72-hour contract's rates. Missed ticks are skipped, not caught
//! up, so a stall during a fault never turns into a burst that inflates the
//! steady rate after it.
//!
//! The transactional producer is the exception. It stops ticking while it
//! waits for a transaction's deliveries and for its commit or abort, every
//! transaction, on a healthy cluster as much as during a fault. Skipping the
//! ticks missed in those waits would hold it below its rate, so it catches
//! them up instead.

use {
    super::{
        Endpoint, now_ms,
        windows::{Latencies, Windows, measurement},
    },
    crate::{
        scenario::{Acks as ContractAcks, Compression as ContractCompression},
        soak::{
            contract::{Contract, Isolation, LoadPath, Semantics},
            evidence::{PathMeasurement, ProducerLedger, TxnLedger, TxnOutcome, record_tag},
            ledger::SeqSet,
            recovery::{Ack, CommitAck},
        },
    },
    bytes::Bytes,
    futures::{
        StreamExt as _,
        future::{OptionFuture, try_join_all},
        stream::FuturesUnordered,
    },
    krabka_client_consumer::{
        AutoOffsetReset, Consumer, ConsumerError, IsolationLevel, OffsetAndMetadata,
    },
    krabka_client_producer::{
        Acks, Compression, Producer, ProducerError, ProducerRecord, RecordMetadata,
    },
    krabka_units::prelude::*,
    std::{
        collections::{BTreeMap, HashMap, VecDeque},
        future::Future,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::{sync::Notify, task::JoinHandle, time::MissedTickBehavior},
};

/// How long a client that could not be built waits before it tries again.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// What every load task shares.
pub struct LoadContext {
    pub contract: Contract,
    pub endpoint: Endpoint,
    pub windows: Arc<Windows>,
    /// Whether each producer records when each of its sends was acknowledged.
    /// A disaster-recovery drill needs it to place every record on one side of
    /// the capture boundary; a 72-hour soak does not, and could not afford it.
    record_ack_times: bool,
    running: AtomicBool,
    /// Every path's acknowledged records so far, for a lifecycle checkpoint
    /// that reconciles them while the load runs. The producers' own ledgers
    /// are only handed back when the load stops.
    acknowledged: Mutex<BTreeMap<String, SeqSet>>,
    abandoned: AtomicBool,
    abandon: Notify,
}

#[bon::bon]
impl LoadContext {
    #[builder]
    pub fn new(
        contract: Contract,
        endpoint: Endpoint,
        windows: Arc<Windows>,
        #[builder(default)] record_ack_times: bool,
    ) -> Self {
        Self {
            contract,
            endpoint,
            windows,
            record_ack_times,
            running: AtomicBool::new(true),
            acknowledged: Mutex::new(BTreeMap::new()),
            abandoned: AtomicBool::new(false),
            abandon: Notify::new(),
        }
    }

    /// Record that `path`'s producer was acknowledged `seq`.
    fn acknowledge(&self, path: &str, seq: u64) {
        if let Ok(mut acknowledged) = self.acknowledged.lock() {
            acknowledged.entry(path.to_owned()).or_default().insert(seq);
        }
    }

    /// `path`'s acknowledged records so far.
    pub fn acknowledged(&self, path: &str) -> SeqSet {
        self.acknowledged
            .lock()
            .ok()
            .and_then(|acknowledged| acknowledged.get(path).cloned())
            .unwrap_or_default()
    }

    fn running(&self) -> bool { self.running.load(Ordering::Relaxed) }

    fn is_abandoned(&self) -> bool { self.abandoned.load(Ordering::Relaxed) }

    /// Stop every producer and consumer at its next step.
    pub fn stop(&self) { self.running.store(false, Ordering::Relaxed); }

    /// Stop every idempotent producer and every consumer now, without waiting
    /// for a send in flight or closing a client: the cluster they talk to is
    /// gone. A send in flight is then neither acknowledged nor failed, which
    /// the reconciler reads as a record that may or may not have been written.
    pub fn abandon(&self) {
        self.stop();
        self.abandoned.store(true, Ordering::Relaxed);
        self.abandon.notify_waiters();
    }

    /// Resolves once [`Self::abandon`] has been called.
    async fn abandoned(&self) {
        let notified = self.abandon.notified();
        if self.is_abandoned() {
            return;
        }
        notified.await;
    }
}

/// The one producer of `path`, named as the evidence names it.
#[must_use]
pub fn producer_name(path: &str) -> String { format!("{path}-0") }

/// The consumer group of `path` read at `isolation`.
#[must_use]
pub fn group_name(path: &str, isolation: Isolation) -> String { format!("soak-{path}-{isolation}") }

/// A record value: the tag, padded with `x` to `payload` bytes. A tag longer
/// than the payload is sent whole.
#[must_use]
pub fn record_value(producer: &str, seq: u64, payload: usize) -> Bytes {
    let mut value = record_tag(producer, seq).into_bytes();
    if value.len() < payload {
        value.resize(payload, b'x');
    }
    Bytes::from(value)
}

/// The key of record `seq` on a compacted path: `<producer>/k<seq mod keys>`.
#[must_use]
pub fn compacted_key(producer: &str, seq: u64, key_space: u64) -> String {
    format!("{producer}/k{}", seq % key_space.max(1))
}

/// The key index a compacted key carries.
#[must_use]
pub fn key_index(key: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(key).ok()?;
    let (_, index) = text.rsplit_once("/k")?;
    index.parse().ok()
}

/// Whether a failed compacted send has been superseded: a later value of its
/// key was already sent, so resending it would put an older value after a
/// newer one, which is a regression the application itself would cause.
#[must_use]
pub fn superseded(seq: u64, key_space: u64, next_seq: u64) -> bool {
    seq.saturating_add(key_space) < next_seq
}

/// Whether transaction `index` (counting from zero) is aborted on purpose.
#[must_use]
pub fn aborted_on_purpose(index: u64, abort_every: u32) -> bool {
    (index + 1).is_multiple_of(u64::from(abort_every.max(1)))
}

/// What a producer task returns when the load stops.
pub struct ProducerOutput {
    pub ledger: ProducerLedger,
    pub latencies: Latencies,
    /// When each acknowledged send was acknowledged, in the order the acks
    /// arrived. Empty unless the context records ack times.
    pub acked_at: Vec<Ack>,
}

fn acks(acks: ContractAcks) -> Acks {
    match acks {
        ContractAcks::None => Acks::Zero,
        ContractAcks::Leader => Acks::One,
        ContractAcks::All => Acks::All,
    }
}

fn compression(compression: ContractCompression) -> Compression {
    match compression {
        ContractCompression::None => Compression::None,
        ContractCompression::Gzip => Compression::Gzip,
        ContractCompression::Snappy => Compression::Snappy,
        ContractCompression::Lz4 => Compression::Lz4,
        ContractCompression::Zstd => Compression::Zstd,
    }
}

/// A producer configured as the contract declares, transactional when
/// `transactional_id` is set.
async fn build_producer(
    ctx: &LoadContext,
    name: &str,
    transactional_id: Option<String>,
) -> Result<Producer, ProducerError> {
    let settings = &ctx.contract.clients.producer;
    let transaction_timeout = transactional_id
        .as_ref()
        .map(|_| ctx.contract.clients.transactions.timeout.to_std());
    Producer::builder()
        .bootstrap(ctx.endpoint.bootstrap.clone())
        .maybe_security(ctx.endpoint.security.clone())
        .client_id(name.to_owned())
        .acks(acks(settings.acks))
        .enable_idempotence(true)
        .compression(compression(settings.compression))
        .linger(settings.linger.to_std())
        .batch_size(settings.batch_size.bytes_usize())
        .request_timeout(settings.request_timeout.to_std())
        .delivery_timeout(settings.delivery_timeout.to_std())
        .maybe_transactional_id(transactional_id)
        .maybe_transaction_timeout(transaction_timeout)
        .build()
        .await
}

/// A producer that is ready to send, or `None` once the load has stopped.
/// A transactional one has run `InitProducerId`, which also fences and ends
/// whatever transaction a previous producer of the same id left open.
async fn connect_producer(ctx: &LoadContext, name: &str, transactional: bool) -> Option<Producer> {
    let transactional_id = transactional.then(|| format!("soak-{name}"));
    while ctx.running() {
        match build_producer(ctx, name, transactional_id.clone()).await {
            Ok(producer) if !transactional => return Some(producer),
            Ok(producer) => match producer.init_transactions().await {
                Ok(()) => return Some(producer),
                Err(error) => {
                    eprintln!("soak-contract: {name}: init_transactions: {error}");
                    let _ = producer.close().await;
                }
            },
            Err(error) => eprintln!("soak-contract: {name}: build producer: {error}"),
        }
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
    None
}

/// One send's acknowledgement: its sequence number, which attempt it was,
/// when it was enqueued, and how it ended.
type Delivery =
    Pin<Box<dyn Future<Output = (u64, u32, i64, Result<RecordMetadata, ProducerError>)> + Send>>;
type Admission<'a> = Pin<Box<dyn Future<Output = Delivery> + Send + 'a>>;

async fn enqueue(producer: &Producer, record: ProducerRecord, seq: u64, attempt: u32) -> Delivery {
    let sent = now_ms();
    match producer.enqueue(record).await {
        Ok(handle) => Box::pin(async move { (seq, attempt, sent, handle.await) }),
        Err(error) => Box::pin(async move { (seq, attempt, sent, Err(error)) }),
    }
}

/// The pace of a pipelined producer: one tick per `period`, skipping the ticks
/// it missed.
fn pipelined_ticker(period: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

/// The pace of a transactional producer: one tick per `period` on average,
/// with the ticks missed while a transaction ended fired at once.
fn transactional_ticker(period: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
    ticker
}

fn empty_ledger(path: &LoadPath, producer: &str) -> ProducerLedger {
    ProducerLedger {
        path: path.name.clone(),
        producer: producer.to_owned(),
        semantics: path.semantics,
        attempted: 0,
        acked: SeqSet::new(),
        failed: SeqSet::new(),
        retried: SeqSet::new(),
        transactions: Vec::new(),
    }
}

/// The record `seq` of `path`.
fn record(path: &LoadPath, producer: &str, seq: u64) -> ProducerRecord {
    let key = path
        .key_space
        .filter(|_| path.semantics == Semantics::Compacted)
        .map(|keys| Bytes::from(compacted_key(producer, seq, keys)));
    ProducerRecord {
        topic: path.name.clone(),
        key,
        value: Some(record_value(producer, seq, path.payload.bytes_usize())),
        ..Default::default()
    }
}

/// The idempotent or compacted producer of `path`, until the load stops.
///
/// A send that fails is sent again, up to `max_send_attempts` in all, and is
/// recorded as failed and as retried. A compacted send whose key has a later
/// value in flight is not sent again: see [`superseded`].
pub async fn run_idempotent(ctx: Arc<LoadContext>, path: LoadPath) -> ProducerOutput {
    let name = producer_name(&path.name);
    let mut output = ProducerOutput {
        ledger: empty_ledger(&path, &name),
        latencies: Latencies::new(),
        acked_at: Vec::new(),
    };
    let max_attempts = ctx.contract.clients.producer.max_send_attempts;
    let Some(producer) = connect_producer(&ctx, &name, false).await else {
        return output;
    };
    let mut ticker = pipelined_ticker(path.rate.period().to_std());
    let mut inflight: FuturesUnordered<Delivery> = FuturesUnordered::new();
    let mut next_seq = 0_u64;
    let mut admission: Option<Admission<'_>> = None;
    let mut retries = VecDeque::new();
    loop {
        if admission.is_none()
            && let Some((seq, attempt)) = retries.pop_front()
        {
            admission = Some(Box::pin(enqueue(
                &producer,
                record(&path, &name, seq),
                seq,
                attempt,
            )));
        }
        if !ctx.running() && inflight.is_empty() && admission.is_none() {
            break;
        }
        tokio::select! {
            () = ctx.abandoned() => break,
            _ = ticker.tick(), if ctx.running() && admission.is_none() => {
                let seq = next_seq;
                next_seq += 1;
                admission = Some(Box::pin(enqueue(&producer, record(&path, &name, seq), seq, 1)));
            }
            Some(delivery) = OptionFuture::from(admission.as_mut()), if admission.is_some() => {
                admission = None;
                inflight.push(delivery);
            }
            Some((seq, attempt, sent, result)) = inflight.next() => {
                let at = now_ms();
                match result {
                    Ok(_) => {
                        output.ledger.acked.insert(seq);
                        ctx.acknowledge(&path.name, seq);
                        output.latencies.record(ctx.windows.disturbed(at), Time::from_millis(at - sent));
                        if ctx.record_ack_times {
                            output.acked_at.push(Ack { seq, unix_ms: at });
                        }
                    }
                    Err(error) => {
                        output.ledger.failed.insert(seq);
                        let stale = path
                            .key_space
                            .is_some_and(|keys| superseded(seq, keys, next_seq));
                        if attempt < max_attempts && !stale {
                            eprintln!("soak-contract: {name}: send {seq} attempt {attempt} failed, resending: {error}");
                            output.ledger.retried.insert(seq);
                            retries.push_back((seq, attempt + 1));
                        } else {
                            eprintln!("soak-contract: {name}: send {seq} attempt {attempt} failed: {error}");
                        }
                    }
                }
            }
        }
    }
    output.ledger.attempted = next_seq;
    drop(admission);
    // A producer of an abandoned cluster is dropped, not closed: closing waits
    // for the sends in flight, which can no longer be answered.
    if !ctx.is_abandoned() {
        let _ = producer.close().await;
    }
    output
}

/// The transactional producer of `path`, until the load stops.
///
/// Each transaction carries `records_per_transaction` paced sends, and every
/// `abort_every`-th one is aborted on purpose. A transaction with a failed send
/// is aborted. A commit or abort that returns an error leaves the producer in
/// an unknown state, so it is closed and rebuilt under the same transactional
/// id, whose `InitProducerId` settles what it left open. A commit that
/// returned an error is recorded as [`TxnOutcome::Unknown`]; a transaction
/// whose commit was never sent is aborted, whatever ended it.
pub async fn run_transactional(ctx: Arc<LoadContext>, path: LoadPath) -> ProducerOutput {
    let mut run = TransactionalRun {
        name: producer_name(&path.name),
        ledger: empty_ledger(&path, &producer_name(&path.name)),
        latencies: Latencies::new(),
        acked_at: Vec::new(),
        ticker: transactional_ticker(path.rate.period().to_std()),
        next_seq: 0,
        index: 0,
    };
    let mut producer: Option<Producer> = None;
    while ctx.running() {
        let Some(current) = producer.as_ref() else {
            producer = connect_producer(&ctx, &run.name, true).await;
            continue;
        };
        if !run.transaction(&ctx, &path, current).await
            && let Some(stale) = producer.take()
        {
            let _ = stale.close().await;
        }
    }
    run.ledger.attempted = run.next_seq;
    if let Some(producer) = producer {
        let _ = producer.close().await;
    }
    ProducerOutput {
        ledger: run.ledger,
        latencies: run.latencies,
        acked_at: run.acked_at,
    }
}

/// The state of a transactional producer across its transactions.
struct TransactionalRun {
    name: String,
    ledger: ProducerLedger,
    latencies: Latencies,
    acked_at: Vec<Ack>,
    ticker: tokio::time::Interval,
    next_seq: u64,
    index: u64,
}

impl TransactionalRun {
    /// Run one transaction on `producer`, and say whether the producer is
    /// still usable.
    async fn transaction(
        &mut self,
        ctx: &LoadContext,
        path: &LoadPath,
        producer: &Producer,
    ) -> bool {
        let settings = &ctx.contract.clients.transactions;
        let name = self.name.clone();
        let transaction = match producer.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(error) => {
                eprintln!("soak-contract: {name}: begin_transaction: {error}");
                return false;
            }
        };
        let start = self.next_seq;
        let failed = self
            .deliveries(ctx, path, |seq| {
                enqueue(producer, record(path, &name, seq), seq, 1)
            })
            .await;
        let index = self.index;
        let (outcome, usable) = if failed || aborted_on_purpose(index, settings.abort_every) {
            match transaction.abort().await {
                Ok(()) => (TxnOutcome::Aborted, true),
                Err(error) => {
                    eprintln!("soak-contract: {name}: abort of transaction {index}: {error}");
                    (TxnOutcome::Aborted, false)
                }
            }
        } else {
            match transaction.commit().await {
                Ok(()) => (TxnOutcome::Committed, true),
                Err(error) => {
                    eprintln!("soak-contract: {name}: commit of transaction {index}: {error}");
                    (TxnOutcome::Unknown, false)
                }
            }
        };
        self.ledger.transactions.push(TxnLedger {
            id: index,
            seqs: SeqSet::range(start, self.next_seq),
            outcome,
        });
        self.index += 1;
        usable
    }

    /// Observe acknowledgements alongside pacing, rather than after filling
    /// the transaction, so the measured latency excludes the driver's wait.
    async fn deliveries<F: Future<Output = Delivery> + Send>(
        &mut self,
        ctx: &LoadContext,
        path: &LoadPath,
        mut enqueue_record: impl FnMut(u64) -> F + Send,
    ) -> bool {
        let mut deliveries = FuturesUnordered::new();
        let mut admission: Option<Pin<Box<F>>> = None;
        let mut admitted = 0;
        let count = ctx.contract.clients.transactions.records_per_transaction;
        let mut failed = false;
        loop {
            if (admitted == count || !ctx.running()) && deliveries.is_empty() && admission.is_none()
            {
                break;
            }
            tokio::select! {
                _ = self.ticker.tick(), if admitted < count && ctx.running() && admission.is_none() => {
                    if !ctx.running() {
                        continue;
                    }
                    let seq = self.next_seq;
                    self.next_seq += 1;
                    admitted += 1;
                    admission = Some(Box::pin(enqueue_record(seq)));
                }
                Some(delivery) = OptionFuture::from(admission.as_mut()), if admission.is_some() => {
                    admission = None;
                    deliveries.push(delivery);
                }
                Some((seq, _, sent, result)) = deliveries.next(), if !deliveries.is_empty() => {
                    let at = now_ms();
                    match result {
                        Ok(_) => {
                            self.ledger.acked.insert(seq);
                            ctx.acknowledge(&path.name, seq);
                            self.latencies.record(ctx.windows.disturbed(at), Time::from_millis(at - sent));
                            if ctx.record_ack_times {
                                self.acked_at.push(Ack { seq, unix_ms: at });
                            }
                        }
                        Err(error) => {
                            eprintln!("soak-contract: {}: transactional send {seq} failed: {error}", self.name);
                            self.ledger.failed.insert(seq);
                            failed = true;
                        }
                    }
                }
                else => break,
            }
        }
        failed
    }
}

/// The last commit the coordinator acknowledged for one consumer group.
pub struct CommitTracker {
    pub group: String,
    pub topic: String,
    pub partitions: i32,
    acknowledged: Mutex<BTreeMap<i32, i64>>,
    /// Every acknowledged commit, in order, so a drill can ask which commit
    /// was the last one before its capture boundary.
    history: Mutex<Vec<CommitAck>>,
}

impl CommitTracker {
    #[must_use]
    pub fn new(group: String, topic: String, partitions: i32) -> Self {
        Self {
            group,
            topic,
            partitions,
            acknowledged: Mutex::new(BTreeMap::new()),
            history: Mutex::new(Vec::new()),
        }
    }

    /// Every acknowledged commit so far, oldest first.
    #[must_use]
    pub fn history(&self) -> Vec<CommitAck> {
        self.history
            .lock()
            .map(|history| history.clone())
            .unwrap_or_default()
    }

    /// The last acknowledged commit of every partition committed so far.
    pub fn snapshot(&self) -> BTreeMap<i32, i64> {
        self.acknowledged
            .lock()
            .map(|acked| acked.clone())
            .unwrap_or_default()
    }

    fn acknowledge(&self, positions: &BTreeMap<i32, i64>) {
        if let Ok(mut acked) = self.acknowledged.lock() {
            acked.extend(positions);
        }
        if let Ok(mut history) = self.history.lock() {
            history.push(CommitAck {
                unix_ms: now_ms(),
                offsets: positions.clone(),
            });
        }
    }
}

async fn build_consumer(
    contract: &Contract,
    endpoint: &Endpoint,
    path: &LoadPath,
    isolation: Isolation,
) -> Result<Consumer, ConsumerError> {
    let consumer = &contract.clients.consumer;
    Consumer::builder()
        .bootstrap(endpoint.bootstrap.clone())
        .maybe_security(endpoint.security.clone())
        .client_id(group_name(&path.name, isolation))
        .group_id(group_name(&path.name, isolation))
        .subscribe(vec![path.name.clone()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(isolation_level(isolation))
        .session_timeout(consumer.session_timeout)
        .enable_auto_commit(false)
        .default_api_timeout(consumer.session_timeout)
        .build()
        .await
}

#[must_use]
pub fn isolation_level(isolation: Isolation) -> IsolationLevel {
    match isolation {
        Isolation::ReadCommitted => IsolationLevel::ReadCommitted,
        Isolation::ReadUncommitted => IsolationLevel::ReadUncommitted,
    }
}

/// The live consumer group of `path` at `isolation`, until the load stops.
///
/// It measures create-time-to-poll latency for every record it polls, and,
/// with a `tracker`, commits what it has polled every `commit_interval` and
/// records each commit the coordinator acknowledged.
pub async fn run_consumer(
    ctx: Arc<LoadContext>,
    path: LoadPath,
    isolation: Isolation,
    tracker: Option<Arc<CommitTracker>>,
    mut consumer: Consumer,
) -> Latencies {
    let mut latencies = Latencies::new();
    let settings = ctx.contract.clients.consumer.clone();
    let group = group_name(&path.name, isolation);
    let mut positions: BTreeMap<i32, i64> = BTreeMap::new();
    let mut committed: BTreeMap<i32, i64> = BTreeMap::new();
    let mut last_commit = Instant::now();
    while ctx.running() {
        let polled = tokio::select! {
            () = ctx.abandoned() => break,
            polled = consumer.poll(settings.poll_timeout) => polled,
        };
        match polled {
            Ok(records) => {
                let at = now_ms();
                let disturbed = ctx.windows.disturbed(at);
                for record in records {
                    latencies.record(disturbed, Time::from_millis((at - record.timestamp).max(0)));
                    let next = positions.entry(record.partition).or_insert(0);
                    *next = (*next).max(record.offset + 1);
                }
            }
            Err(error) => {
                eprintln!("soak-contract: {group}: poll: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        if let Some(tracker) = &tracker
            && last_commit.elapsed() >= settings.commit_interval.to_std()
            && positions != committed
        {
            last_commit = Instant::now();
            let offsets: HashMap<(String, i32), OffsetAndMetadata> = positions
                .iter()
                .map(|(partition, offset)| {
                    (
                        (path.name.clone(), *partition),
                        OffsetAndMetadata::new(*offset),
                    )
                })
                .collect();
            let commit = tokio::select! {
                () = ctx.abandoned() => break,
                commit = consumer.commit_offsets_sync(offsets) => commit,
            };
            match commit {
                Ok(()) => {
                    tracker.acknowledge(&positions);
                    committed.clone_from(&positions);
                }
                Err(error) => eprintln!("soak-contract: {group}: commit: {error}"),
            }
        }
    }
    if !ctx.is_abandoned() {
        let _ = consumer.close().await;
    }
    latencies
}

/// Every task of a running load: one producer per path, one consumer group
/// per path and isolation level, and the commit tracker of every group that
/// commits.
pub struct RunningLoad {
    pub ctx: Arc<LoadContext>,
    pub trackers: Vec<Arc<CommitTracker>>,
    producers: Vec<JoinHandle<ProducerOutput>>,
    consumers: Vec<(String, JoinHandle<Latencies>)>,
}

/// What a load left behind once every task ended.
pub struct LoadOutcome {
    pub producers: Vec<ProducerOutput>,
    /// Every consumer group's latency, merged per path.
    pub consume_latency: BTreeMap<String, Latencies>,
}

/// Consumers whose group assignment completed before the measured load.
pub struct PreparedLoad {
    consumers: Vec<(LoadPath, Isolation, Consumer)>,
}

/// Join every consumer group before starting the measured load and its fault
/// clock, so coordinator startup does not age the first produced records.
///
/// # Errors
///
/// Returns a consumer startup error before any producer starts.
pub async fn prepare(
    contract: &Contract,
    endpoint: &Endpoint,
) -> Result<PreparedLoad, ConsumerError> {
    let consumers = contract.paths.iter().flat_map(|path| {
        path.consumers.iter().map(move |&isolation| async move {
            let consumer = build_consumer(contract, endpoint, path, isolation).await?;
            Ok::<_, ConsumerError>((path.clone(), isolation, consumer))
        })
    });
    Ok(PreparedLoad {
        consumers: try_join_all(consumers).await?,
    })
}

/// Start the load `ctx` declares. It runs until [`LoadContext::stop`] or
/// [`LoadContext::abandon`], and [`RunningLoad::finish`] then collects it.
#[must_use]
pub fn start(ctx: &Arc<LoadContext>, prepared: PreparedLoad) -> RunningLoad {
    let contract = &ctx.contract;
    let producers = contract
        .paths
        .iter()
        .map(|path| match path.semantics {
            Semantics::Transactional => {
                tokio::spawn(run_transactional(Arc::clone(ctx), path.clone()))
            }
            Semantics::Idempotent | Semantics::Compacted => {
                tokio::spawn(run_idempotent(Arc::clone(ctx), path.clone()))
            }
        })
        .collect();
    let mut trackers = Vec::new();
    let mut consumers = Vec::new();
    for (path, isolation, consumer) in prepared.consumers {
        let tracker = path.commit_offsets.then(|| {
            Arc::new(CommitTracker::new(
                group_name(&path.name, isolation),
                path.name.clone(),
                path.partitions,
            ))
        });
        if let Some(tracker) = &tracker {
            trackers.push(Arc::clone(tracker));
        }
        consumers.push((
            path.name.clone(),
            tokio::spawn(run_consumer(
                Arc::clone(ctx),
                path,
                isolation,
                tracker,
                consumer,
            )),
        ));
    }
    RunningLoad {
        ctx: Arc::clone(ctx),
        trackers,
        producers,
        consumers,
    }
}

impl RunningLoad {
    /// Wait for every task of a load that was stopped or abandoned.
    ///
    /// # Panics
    ///
    /// When a load task panicked: its ledger is gone, and a run without it
    /// cannot be judged.
    pub async fn finish(self) -> LoadOutcome {
        let mut producers = Vec::new();
        for task in self.producers {
            producers.push(task.await.expect("a producer task"));
        }
        let mut consume_latency: BTreeMap<String, Latencies> = BTreeMap::new();
        for (path, task) in self.consumers {
            let latencies = task.await.expect("a consumer task");
            consume_latency.entry(path).or_default().merge(&latencies);
        }
        LoadOutcome {
            producers,
            consume_latency,
        }
    }
}

impl LoadOutcome {
    /// Each path's measurement over `steady` time.
    #[must_use]
    pub fn measurements(&self, contract: &Contract, steady: Time) -> Vec<PathMeasurement> {
        let empty = Latencies::new();
        contract
            .paths
            .iter()
            .map(|path| {
                let produce = self
                    .producers
                    .iter()
                    .find(|output| output.ledger.path == path.name)
                    .map_or(&empty, |output| &output.latencies);
                measurement(
                    &path.name,
                    produce,
                    self.consume_latency.get(&path.name).unwrap_or(&empty),
                    steady,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            Delivery, Endpoint, Latencies, LoadContext, ProducerError, RecordMetadata,
            TransactionalRun, Windows, aborted_on_purpose, compacted_key, empty_ledger, key_index,
            now_ms, pipelined_ticker, record_value, superseded, transactional_ticker,
        },
        crate::soak::{
            contract::{Semantics, fixture},
            evidence::parse_record_tag,
            ledger::SeqSet,
        },
        assert2::check,
        futures::FutureExt as _,
        std::{
            sync::{Arc, Mutex},
            time::Duration,
        },
        tokio::time::{Instant, Interval},
    };

    const PERIOD: Duration = Duration::from_millis(10);

    #[yare::parameterized(
        acknowledged = { false, false },
        rejected = { true, false },
        stalled_admission = { false, true },
    )]
    #[test_macro(tokio::test(start_paused = true))]
    async fn transaction_deliveries_are_observed_before_the_next_paced_send(
        reject_last: bool,
        stall_admission: bool,
    ) {
        let mut contract = fixture::self_check();
        contract.clients.transactions.records_per_transaction = 3;
        let path = contract
            .paths
            .iter()
            .find(|path| path.semantics == Semantics::Transactional)
            .unwrap()
            .clone();
        let ctx = LoadContext::builder()
            .contract(contract)
            .endpoint(Endpoint::plaintext("unused:9092"))
            .windows(Arc::new(Windows::new(Vec::new())))
            .build();
        let mut run = TransactionalRun {
            name: "paced".into(),
            ledger: empty_ledger(&path, "paced"),
            latencies: Latencies::new(),
            acked_at: Vec::new(),
            ticker: transactional_ticker(PERIOD),
            next_seq: 0,
            index: 0,
        };
        let start = Instant::now();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let failed = run
            .deliveries(&ctx, &path, |seq| {
                let observed = observed.clone();
                let delay = if stall_admission && seq == 1 {
                    PERIOD * 3
                } else {
                    Duration::ZERO
                };
                tokio::time::sleep(delay).map(move |()| {
                    let ready_at = Instant::now() + PERIOD / 2;
                    let delivery: Delivery = Box::pin(async move {
                        tokio::time::sleep_until(ready_at).await;
                        observed.lock().unwrap().push(start.elapsed());
                        let result = if reject_last && seq == 2 {
                            Err(ProducerError::Server(19))
                        } else {
                            Ok(RecordMetadata {
                                topic: "paced".into(),
                                partition: 0,
                                offset: i64::try_from(seq).unwrap(),
                                timestamp_ms: 0,
                                serialized_key_size: -1,
                                serialized_value_size: 1,
                            })
                        };
                        (seq, 1, now_ms(), result)
                    });
                    delivery
                })
            })
            .await;
        check!(
            *observed.lock().unwrap()
                == if stall_admission {
                    vec![PERIOD / 2, PERIOD * 4 + PERIOD / 2, PERIOD * 4 + PERIOD / 2]
                } else {
                    vec![PERIOD / 2, PERIOD + PERIOD / 2, PERIOD * 2 + PERIOD / 2]
                }
        );
        check!(failed == reject_last);
        check!(run.ledger.acked == SeqSet::range(0, if reject_last { 2 } else { 3 }));
        check!(
            run.ledger.failed
                == if reject_last {
                    SeqSet::range(2, 3)
                } else {
                    SeqSet::new()
                }
        );
        check!(run.latencies.steady_count == if reject_last { 2 } else { 3 });
    }

    /// How long ten ticks take when the ticker stalls for five periods after
    /// the first, as a transactional producer does while a transaction ends.
    async fn ten_ticks_across_a_stall(mut ticker: Interval) -> Duration {
        let start = Instant::now();
        ticker.tick().await;
        tokio::time::sleep(PERIOD * 5).await;
        for _ in 1..10 {
            ticker.tick().await;
        }
        start.elapsed()
    }

    #[yare::parameterized(
        transactional_keeps_its_average_rate = { transactional_ticker, PERIOD * 9 },
        pipelined_drops_what_it_missed = { pipelined_ticker, PERIOD * 13 },
    )]
    #[test_macro(tokio::test(start_paused = true))]
    async fn only_a_pipelined_producer_drops_the_ticks_a_stall_missed(
        ticker: fn(Duration) -> Interval,
        took: Duration,
    ) {
        check!(ten_ticks_across_a_stall(ticker(PERIOD)).await == took);
    }

    #[yare::parameterized(
        padded = { 64, 64 },
        tag_longer_than_payload = { 4, 13 },
    )]
    fn a_value_carries_its_tag_padded_to_the_payload(payload: usize, length: usize) {
        let value = record_value("path-0", 12_345, payload);
        check!(value.len() == length);
        check!(parse_record_tag(&value) == Some(("path-0", 12_345)));
    }

    #[test]
    fn a_compacted_key_names_its_index() {
        let key = compacted_key("classic-compacted-0", 130, 64);
        check!(key == "classic-compacted-0/k2");
        check!(key_index(key.as_bytes()) == Some(2));
        check!(key_index(b"no-index") == None);
    }

    #[yare::parameterized(
        nothing_later_sent = { 10, 64, 70, false },
        next_value_of_the_key_not_yet_sent = { 10, 64, 74, false },
        next_value_of_the_key_sent = { 10, 64, 75, true },
    )]
    fn a_failed_compacted_send_is_superseded_once_its_key_moved_on(
        seq: u64,
        keys: u64,
        next: u64,
        expected: bool,
    ) {
        check!(superseded(seq, keys, next) == expected);
    }

    #[yare::parameterized(
        first = { 0, false },
        third = { 2, false },
        fourth = { 3, true },
        eighth = { 7, true },
    )]
    fn every_fourth_transaction_is_aborted(index: u64, aborted: bool) {
        check!(aborted_on_purpose(index, 4) == aborted);
    }
}
