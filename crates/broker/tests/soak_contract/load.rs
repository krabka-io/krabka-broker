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

use {
    crate::{
        host::now_ms,
        windows::{Latencies, Windows},
    },
    bytes::Bytes,
    futures_util::{StreamExt as _, stream::FuturesUnordered},
    krabka_bench_driver::{
        scenario::{Acks as ContractAcks, Compression as ContractCompression},
        soak::{
            contract::{Contract, Isolation, LoadPath, Semantics},
            evidence::{ProducerLedger, TxnLedger, TxnOutcome, record_tag},
            ledger::SeqSet,
        },
    },
    krabka_client_consumer::{
        AutoOffsetReset, Consumer, ConsumerError, IsolationLevel, OffsetAndMetadata,
    },
    krabka_client_producer::{
        Acks, Compression, Producer, ProducerError, ProducerRecord, RecordMetadata,
    },
    krabka_units::prelude::*,
    std::{
        collections::{BTreeMap, HashMap},
        future::Future,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::time::MissedTickBehavior,
};

/// How long a client that could not be built waits before it tries again.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// What every load task shares.
pub(crate) struct LoadContext {
    pub(crate) contract: Contract,
    pub(crate) bootstrap: String,
    pub(crate) windows: Arc<Windows>,
    running: AtomicBool,
}

impl LoadContext {
    pub(crate) fn new(contract: Contract, bootstrap: String, windows: Arc<Windows>) -> Self {
        Self {
            contract,
            bootstrap,
            windows,
            running: AtomicBool::new(true),
        }
    }

    fn running(&self) -> bool { self.running.load(Ordering::Relaxed) }

    /// Stop every producer and consumer at its next step.
    pub(crate) fn stop(&self) { self.running.store(false, Ordering::Relaxed); }
}

/// The one producer of `path`, named as the evidence names it.
pub(crate) fn producer_name(path: &str) -> String { format!("{path}-0") }

/// The consumer group of `path` read at `isolation`.
pub(crate) fn group_name(path: &str, isolation: Isolation) -> String {
    format!("soak-{path}-{isolation}")
}

/// A record value: the tag, padded with `x` to `payload` bytes. A tag longer
/// than the payload is sent whole.
pub(crate) fn record_value(producer: &str, seq: u64, payload: usize) -> Bytes {
    let mut value = record_tag(producer, seq).into_bytes();
    if value.len() < payload {
        value.resize(payload, b'x');
    }
    Bytes::from(value)
}

/// The key of record `seq` on a compacted path: `<producer>/k<seq mod keys>`.
pub(crate) fn compacted_key(producer: &str, seq: u64, key_space: u64) -> String {
    format!("{producer}/k{}", seq % key_space.max(1))
}

/// The key index a compacted key carries.
pub(crate) fn key_index(key: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(key).ok()?;
    let (_, index) = text.rsplit_once("/k")?;
    index.parse().ok()
}

/// Whether a failed compacted send has been superseded: a later value of its
/// key was already sent, so resending it would put an older value after a
/// newer one, which is a regression the application itself would cause.
pub(crate) fn superseded(seq: u64, key_space: u64, next_seq: u64) -> bool {
    seq.saturating_add(key_space) < next_seq
}

/// Whether transaction `index` (counting from zero) is aborted on purpose.
pub(crate) fn aborted_on_purpose(index: u64, abort_every: u32) -> bool {
    (index + 1).is_multiple_of(u64::from(abort_every.max(1)))
}

/// What a producer task returns when the load stops.
pub(crate) struct ProducerOutput {
    pub(crate) ledger: ProducerLedger,
    pub(crate) latencies: Latencies,
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
        .bootstrap(ctx.bootstrap.clone())
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

async fn enqueue(producer: &Producer, record: ProducerRecord, seq: u64, attempt: u32) -> Delivery {
    let sent = now_ms();
    match producer.enqueue(record).await {
        Ok(handle) => Box::pin(async move { (seq, attempt, sent, handle.await) }),
        Err(error) => Box::pin(async move { (seq, attempt, sent, Err(error)) }),
    }
}

fn ticker(path: &LoadPath) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(path.rate.period().to_std());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
pub(crate) async fn run_idempotent(ctx: Arc<LoadContext>, path: LoadPath) -> ProducerOutput {
    let name = producer_name(&path.name);
    let mut ledger = empty_ledger(&path, &name);
    let mut latencies = Latencies::new();
    let max_attempts = ctx.contract.clients.producer.max_send_attempts;
    let Some(producer) = connect_producer(&ctx, &name, false).await else {
        return ProducerOutput { ledger, latencies };
    };
    let mut ticker = ticker(&path);
    let mut inflight: FuturesUnordered<Delivery> = FuturesUnordered::new();
    let mut next_seq = 0_u64;
    loop {
        if !ctx.running() && inflight.is_empty() {
            break;
        }
        tokio::select! {
            _ = ticker.tick(), if ctx.running() => {
                let seq = next_seq;
                next_seq += 1;
                let delivery = enqueue(&producer, record(&path, &name, seq), seq, 1).await;
                inflight.push(delivery);
            }
            Some((seq, attempt, sent, result)) = inflight.next() => {
                let at = now_ms();
                match result {
                    Ok(_) => {
                        ledger.acked.insert(seq);
                        latencies.record(ctx.windows.disturbed(at), Time::from_millis(at - sent));
                    }
                    Err(error) => {
                        ledger.failed.insert(seq);
                        let stale = path
                            .key_space
                            .is_some_and(|keys| superseded(seq, keys, next_seq));
                        if attempt < max_attempts && !stale {
                            eprintln!("soak-contract: {name}: send {seq} attempt {attempt} failed, resending: {error}");
                            ledger.retried.insert(seq);
                            let delivery = enqueue(&producer, record(&path, &name, seq), seq, attempt + 1).await;
                            inflight.push(delivery);
                        } else {
                            eprintln!("soak-contract: {name}: send {seq} attempt {attempt} failed: {error}");
                        }
                    }
                }
            }
        }
    }
    ledger.attempted = next_seq;
    let _ = producer.close().await;
    ProducerOutput { ledger, latencies }
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
pub(crate) async fn run_transactional(ctx: Arc<LoadContext>, path: LoadPath) -> ProducerOutput {
    let mut run = TransactionalRun {
        name: producer_name(&path.name),
        ledger: empty_ledger(&path, &producer_name(&path.name)),
        latencies: Latencies::new(),
        ticker: ticker(&path),
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
    }
}

/// The state of a transactional producer across its transactions.
struct TransactionalRun {
    name: String,
    ledger: ProducerLedger,
    latencies: Latencies,
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
        let name = &self.name;
        let transaction = match producer.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(error) => {
                eprintln!("soak-contract: {name}: begin_transaction: {error}");
                return false;
            }
        };
        let start = self.next_seq;
        let mut deliveries: FuturesUnordered<Delivery> = FuturesUnordered::new();
        for _ in 0..settings.records_per_transaction {
            self.ticker.tick().await;
            if !ctx.running() {
                break;
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            let delivery = enqueue(producer, record(path, name, seq), seq, 1).await;
            deliveries.push(delivery);
        }
        let mut failed = false;
        while let Some((seq, _, sent, result)) = deliveries.next().await {
            let at = now_ms();
            match result {
                Ok(_) => {
                    self.ledger.acked.insert(seq);
                    self.latencies
                        .record(ctx.windows.disturbed(at), Time::from_millis(at - sent));
                }
                Err(error) => {
                    eprintln!("soak-contract: {name}: transactional send {seq} failed: {error}");
                    self.ledger.failed.insert(seq);
                    failed = true;
                }
            }
        }
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
}

/// The last commit the coordinator acknowledged for one consumer group.
pub(crate) struct CommitTracker {
    pub(crate) group: String,
    pub(crate) topic: String,
    pub(crate) partitions: i32,
    acknowledged: Mutex<BTreeMap<i32, i64>>,
}

impl CommitTracker {
    pub(crate) fn new(group: String, topic: String, partitions: i32) -> Self {
        Self {
            group,
            topic,
            partitions,
            acknowledged: Mutex::new(BTreeMap::new()),
        }
    }

    /// The last acknowledged commit of every partition committed so far.
    pub(crate) fn snapshot(&self) -> BTreeMap<i32, i64> {
        self.acknowledged
            .lock()
            .map(|acked| acked.clone())
            .unwrap_or_default()
    }

    fn acknowledge(&self, positions: &BTreeMap<i32, i64>) {
        if let Ok(mut acked) = self.acknowledged.lock() {
            acked.extend(positions);
        }
    }
}

async fn build_consumer(
    ctx: &LoadContext,
    path: &LoadPath,
    isolation: Isolation,
) -> Result<Consumer, ConsumerError> {
    let consumer = &ctx.contract.clients.consumer;
    Consumer::builder()
        .bootstrap(ctx.bootstrap.clone())
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

pub(crate) fn isolation_level(isolation: Isolation) -> IsolationLevel {
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
pub(crate) async fn run_consumer(
    ctx: Arc<LoadContext>,
    path: LoadPath,
    isolation: Isolation,
    tracker: Option<Arc<CommitTracker>>,
) -> Latencies {
    let mut latencies = Latencies::new();
    let settings = ctx.contract.clients.consumer.clone();
    let group = group_name(&path.name, isolation);
    let mut consumer = None;
    while consumer.is_none() && ctx.running() {
        match build_consumer(&ctx, &path, isolation).await {
            Ok(built) => consumer = Some(built),
            Err(error) => {
                eprintln!("soak-contract: {group}: build consumer: {error}");
                tokio::time::sleep(RECONNECT_BACKOFF).await;
            }
        }
    }
    let Some(mut consumer) = consumer else {
        return latencies;
    };
    let mut positions: BTreeMap<i32, i64> = BTreeMap::new();
    let mut committed: BTreeMap<i32, i64> = BTreeMap::new();
    let mut last_commit = Instant::now();
    while ctx.running() {
        match consumer.poll(settings.poll_timeout).await {
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
            match consumer.commit_offsets_sync(offsets).await {
                Ok(()) => {
                    tracker.acknowledge(&positions);
                    committed.clone_from(&positions);
                }
                Err(error) => eprintln!("soak-contract: {group}: commit: {error}"),
            }
        }
    }
    let _ = consumer.close().await;
    latencies
}

#[cfg(test)]
mod tests {
    use {
        super::{aborted_on_purpose, compacted_key, key_index, record_value, superseded},
        assert2::check,
        krabka_bench_driver::soak::evidence::parse_record_tag,
    };

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
