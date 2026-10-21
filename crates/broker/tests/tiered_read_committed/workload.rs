//! The transactional workload: committed and aborted transactions that
//! interleave across many 1 KiB segments, plain records between them, an
//! abort whose marker lands past the last stable offset, and one transaction
//! left open with plain records after it.
//!
//! Every transaction is its own transactional id, so each has its own
//! producer id at epoch 0 and needs no epoch bump between transactions.

use {
    crate::support::{
        produce::{SinglePartitionProduceSetup, batch_request},
        records::batch_from_records,
        transaction_wire::{
            add_partition_request, coordinator_loading, initialized_identity, partition_added,
            retry_coordinator, settled_produce,
        },
        transactions::{
            ProducerIdentity, TransactionOutcome, end_transaction_request, init_producer_request,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_core::Client,
    krabka_protocol::{
        owned::produce_request::ProduceRequest,
        primitives::uuid::Uuid,
        records::{Attributes, Record, RecordBatch},
    },
    std::{collections::BTreeSet, time::Duration},
};

/// Kafka's `CONCURRENT_TRANSACTIONS`: the previous transaction's markers are
/// still being written.
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a coordinator request may take to see the coordinator settle.
const SETTLE: Duration = Duration::from_secs(30);

/// Long enough that the coordinator never aborts the open transaction while
/// the test reads around it. Kafka's `transaction.max.timeout.ms` is 15 min.
const OPEN_TRANSACTION_TIMEOUT_MS: i32 = 600_000;

/// Committed/aborted transaction pairs the workload writes.
const ROUNDS: usize = 5;

/// Batches each transaction of a round writes.
const BATCHES_PER_TRANSACTION: usize = 3;

/// Batches `a-last` writes: more than one segment holds, so the tier holds
/// some of its data while its abort marker stays in the local tail.
const LAST_ABORT_BATCHES: usize = 8;

/// Records in each batch.
const RECORDS_PER_BATCH: usize = 2;

/// A key on every record, so a dozen batches fill several 1 KiB segments.
const PADDING: &[u8] = &[b'k'; 48];

/// One record the workload wrote, at the offset the broker gave it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Written {
    pub offset: i64,
    pub value: String,
}

/// Who wrote a batch, which says whether a `read_committed` reader sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Writer {
    Committed,
    Aborted,
    Plain,
    Open,
}

/// What the workload wrote and what a `read_committed` reader must make of it.
pub struct Workload {
    batches: Vec<(Writer, Vec<Written>)>,
    /// `(producer_id, first_offset)` of every aborted transaction: the rows
    /// Kafka's `FetchResponse.AbortedTransactions` carries.
    pub aborted: BTreeSet<(i64, i64)>,
    /// The first offset of the transaction left open: the last stable offset.
    pub open_first_offset: i64,
    /// `(producer_id, first_offset)` of `a-last`, whose abort marker lands
    /// past the open transaction's first offset.
    pub last_abort: (i64, i64),
    /// The transaction left open, so the test can end it.
    pub open: Transaction,
}

impl Workload {
    /// The log end offset once every marker but the open transaction's has
    /// landed: every data record, plus one marker for each ended transaction.
    pub fn log_end_while_open(&self) -> i64 {
        let records: usize = self.batches.iter().map(|(_, records)| records.len()).sum();
        let ended = 2 * ROUNDS + 1;
        i64::try_from(records + ended).expect("offset")
    }

    /// The records a `read_committed` reader sees below the open transaction,
    /// in offset order: committed and plain data.
    pub fn visible_below_open(&self) -> Vec<Written> {
        self.visible(|writer, offset| {
            matches!(writer, Writer::Committed | Writer::Plain) && offset < self.open_first_offset
        })
    }

    /// The records a `read_committed` reader sees from the open transaction's
    /// first offset on, once that transaction commits.
    pub fn visible_once_open_commits(&self) -> Vec<Written> {
        self.visible(|writer, offset| {
            matches!(writer, Writer::Open | Writer::Plain) && offset >= self.open_first_offset
        })
    }

    fn visible(&self, keep: impl Fn(Writer, i64) -> bool) -> Vec<Written> {
        let mut visible: Vec<Written> = self
            .batches
            .iter()
            .flat_map(|(writer, records)| {
                records
                    .iter()
                    .filter(|record| keep(*writer, record.offset))
                    .cloned()
            })
            .collect();
        visible.sort();
        visible
    }
}

/// One transaction, under a transactional id of its own.
pub struct Transaction {
    transactional_id: String,
    producer: ProducerIdentity,
    next_sequence: i32,
    first_offset: Option<i64>,
}

impl Transaction {
    /// `InitProducerId` and `AddPartitionsToTxn` for `transactional_id`.
    async fn begin(client: &Client, transactional_id: String, topic: &str) -> Self {
        let (producer_id, epoch) = initialized_identity(
            || async {
                client
                    .send(
                        init_producer_request()
                            .transactional_id(transactional_id.clone())
                            .timeout(OPEN_TRANSACTION_TIMEOUT_MS)
                            .call(),
                    )
                    .await
                    .expect("InitProducerId")
            },
            |code| coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS,
            SETTLE,
        )
        .await;
        partition_added(client.send(add_partition_request(
            &transactional_id,
            topic,
            (producer_id, epoch),
        )))
        .await;
        Self {
            transactional_id,
            producer: ProducerIdentity::from_wire((producer_id, epoch)),
            next_sequence: 0,
            first_offset: None,
        }
    }

    /// `EndTxn` with `outcome`, retried while the coordinator is busy.
    pub async fn end(self, client: &Client, outcome: TransactionOutcome) {
        let response = retry_coordinator(
            || async {
                client
                    .send(
                        end_transaction_request()
                            .transactional_id(self.transactional_id.clone())
                            .producer(self.producer)
                            .outcome(outcome)
                            .call(),
                    )
                    .await
                    .expect("EndTxn")
            },
            |response| response.error_code,
            |code| coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS,
            SETTLE,
        )
        .await;
        assert!(response.error_code == 0, "EndTxn: {response:?}");
    }
}

/// Writes batches to one partition and keeps the ledger of what it wrote.
struct Writes<'a> {
    client: &'a Client,
    topic: &'a str,
    topic_id: Uuid,
    batches: Vec<(Writer, Vec<Written>)>,
}

impl Writes<'_> {
    /// Appends one batch of `values`, transactional when `transaction` is
    /// given, and records the offsets the broker assigned.
    async fn append(
        &mut self,
        writer: Writer,
        transaction: Option<&mut Transaction>,
        values: Vec<String>,
    ) {
        let records: Vec<Record> = values
            .iter()
            .zip(0..)
            .map(|(value, offset_delta)| Record {
                offset_delta,
                key: Some(Bytes::from_static(PADDING)),
                value: Some(Bytes::from(value.clone())),
                ..Default::default()
            })
            .collect();
        let count = i32::try_from(records.len()).expect("record count");
        let mut batch = RecordBatch {
            last_offset_delta: count - 1,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            ..batch_from_records(records)
        };
        let transactional_id = transaction.as_ref().map(|txn| txn.transactional_id.clone());
        if let Some(txn) = transaction.as_deref() {
            batch.attributes = Attributes::default().with_transactional(true);
            batch.producer_id = txn.producer.id.0;
            batch.producer_epoch = txn.producer.epoch.0;
            batch.base_sequence = txn.next_sequence;
        }
        let request = ProduceRequest {
            transactional_id,
            ..batch_request(batch, SinglePartitionProduceSetup {
                topic: self.topic.into(),
                topic_id: self.topic_id,
                ..SinglePartitionProduceSetup::replicated()
            })
        };
        let response = settled_produce(self.client, request, SETTLE).await;
        let row = &response.responses[0].partition_responses[0];
        assert!(row.error_code == 0, "Produce: {response:?}");
        if let Some(txn) = transaction {
            txn.next_sequence += count;
            txn.first_offset.get_or_insert(row.base_offset);
        }
        let written = values
            .into_iter()
            .zip(row.base_offset..)
            .map(|(value, offset)| Written { offset, value })
            .collect();
        self.batches.push((writer, written));
    }
}

/// The values of batch `batch` of a transaction labelled `label`.
fn batch_values(label: &str, batch: usize) -> Vec<String> {
    (0..RECORDS_PER_BATCH)
        .map(|record| format!("{label}-{batch}-{record}"))
        .collect()
}

/// Writes the workload to partition 0 of `topic`:
///
/// 1. [`ROUNDS`] rounds, each a transaction `c{n}` that commits and one
///    `a{n}` that aborts, their batches interleaved, then a plain record
///    `p{n}`.
/// 2. A transaction `a-last` whose data all lies below the open transaction
///    and whose abort marker lands after it, past the last stable offset.
/// 3. The open transaction `o`, and plain records `q` after it.
pub async fn write_workload(client: &Client, topic: &str, topic_id: Uuid) -> Workload {
    let mut writes = Writes {
        client,
        topic,
        topic_id,
        batches: Vec::new(),
    };
    let mut aborted = BTreeSet::new();
    for round in 0..ROUNDS {
        let mut commit = Transaction::begin(client, format!("rc-commit-{round}"), topic).await;
        let mut abort = Transaction::begin(client, format!("rc-abort-{round}"), topic).await;
        for batch in 0..BATCHES_PER_TRANSACTION {
            let committed = batch_values(&format!("c{round}"), batch);
            writes
                .append(Writer::Committed, Some(&mut commit), committed)
                .await;
            let aborted_values = batch_values(&format!("a{round}"), batch);
            writes
                .append(Writer::Aborted, Some(&mut abort), aborted_values)
                .await;
        }
        writes
            .append(Writer::Plain, None, vec![format!("p{round}")])
            .await;
        aborted.insert(aborted_row(&abort));
        abort.end(client, TransactionOutcome::Abort).await;
        commit.end(client, TransactionOutcome::Commit).await;
    }
    let mut last_abort = Transaction::begin(client, "rc-abort-last".into(), topic).await;
    for batch in 0..LAST_ABORT_BATCHES {
        let values = batch_values("a-last", batch);
        writes
            .append(Writer::Aborted, Some(&mut last_abort), values)
            .await;
    }
    let mut open = Transaction::begin(client, "rc-open".into(), topic).await;
    for batch in 0..2 {
        writes
            .append(Writer::Open, Some(&mut open), batch_values("o", batch))
            .await;
    }
    let last_abort_row = aborted_row(&last_abort);
    aborted.insert(last_abort_row);
    last_abort.end(client, TransactionOutcome::Abort).await;
    for record in 0..2 {
        writes
            .append(Writer::Plain, None, vec![format!("q{record}")])
            .await;
    }
    Workload {
        batches: writes.batches,
        aborted,
        last_abort: last_abort_row,
        open_first_offset: open.first_offset.expect("the open transaction wrote"),
        open,
    }
}

/// The aborted-transaction row Kafka reports for `transaction`.
fn aborted_row(transaction: &Transaction) -> (i64, i64) {
    (
        transaction.producer.id.0,
        transaction.first_offset.expect("the transaction wrote"),
    )
}
