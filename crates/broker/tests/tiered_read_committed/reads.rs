//! The two `read_committed` readers: a classic `Fetch` filtered as Kafka's
//! consumer filters it, and a share group whose `share.isolation.level` is
//! `read_committed`.

use {
    crate::{
        support::{
            configs::{
                ConfigOperation, incremental_config, incremental_request, incremental_resource,
            },
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            share::{ShareSessionEpoch, ShareSessionSetup, join, share_fetch_req},
        },
        tiered_partition::{TOPIC, TieredPartition},
        workload::Written,
    },
    assert2::assert,
    krabka_protocol::{
        owned::{
            fetch_request::FetchRequest, fetch_response::AbortedTransaction,
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        },
        records::RecordBatch,
    },
    std::{
        cmp::Reverse,
        collections::{BTreeMap, BTreeSet},
        time::{Duration, Instant},
    },
};

/// Kafka's `IsolationLevel.READ_COMMITTED` on the wire.
const READ_COMMITTED: i8 = 1;

/// Kafka's `ConfigResource.Type.GROUP`.
const GROUP_RESOURCE: i8 = 32;

/// Kafka's `ControlRecordType.ABORT`, the type field of a marker's key.
const ABORT_MARKER: i16 = 0;

/// How long one reader may take to reach the last stable offset.
const READ_DEADLINE: Duration = Duration::from_secs(30);

/// What a `read_committed` `Fetch` of `[from, LSO)` gave a Kafka consumer.
#[derive(Debug, PartialEq, Eq)]
pub struct CommittedRead {
    /// The `LastStableOffset` of the last response: where the read stopped.
    pub last_stable_offset: i64,
    /// The records the consumer hands to the application, in order.
    pub visible: Vec<Written>,
    /// Every `(producer_id, first_offset)` row of every response's
    /// `AbortedTransactions`.
    pub aborted: BTreeSet<(i64, i64)>,
    /// The base offsets of returned batches that start at or past the LSO
    /// the same response reported. Kafka never returns one.
    pub batches_past_last_stable_offset: Vec<i64>,
    /// The fetch offsets of responses that returned records with a null
    /// `AbortedTransactions`. Kafka's `addAbortedTransactions` always sets
    /// a list, empty or not, on a `read_committed` read that found records.
    pub records_without_aborted_list: Vec<i64>,
}

/// Reads `[from, LSO)` with `read_committed` fetches, each from where the
/// last one ended, and filters each response as Kafka's consumer does.
pub async fn read_committed_fetch(tiered: &TieredPartition, from: i64) -> CommittedRead {
    let mut read = CommittedRead {
        last_stable_offset: -1,
        visible: Vec::new(),
        aborted: BTreeSet::new(),
        batches_past_last_stable_offset: Vec::new(),
        records_without_aborted_list: Vec::new(),
    };
    let mut offset = from;
    let deadline = Instant::now() + READ_DEADLINE;
    loop {
        let response = tiered
            .client
            .send(FetchRequest {
                isolation_level: READ_COMMITTED,
                ..single_partition_fetch()
                    .topic(TOPIC.into())
                    .topic_id(tiered.topic_id)
                    .partition(fetch_partition().offset(offset).call())
                    .limits(FetchLimits::wait_for_data(RequestWaitMillis(100)))
                    .call()
            })
            .await
            .expect("Fetch");
        let partition = &response.responses[0].partitions[0];
        assert!(
            partition.error_code == 0,
            "Fetch at {offset}: {partition:?}"
        );
        read.last_stable_offset = partition.last_stable_offset;
        let aborted = partition.aborted_transactions.clone().unwrap_or_default();
        read.aborted.extend(
            aborted
                .iter()
                .map(|txn| (txn.producer_id, txn.first_offset)),
        );
        let batches = partition
            .records
            .as_ref()
            .and_then(|records| records.as_v2())
            .unwrap_or_default();
        read.batches_past_last_stable_offset.extend(
            batches
                .iter()
                .map(|batch| batch.base_offset)
                .filter(|base| *base >= partition.last_stable_offset),
        );
        if !batches.is_empty() && partition.aborted_transactions.is_none() {
            read.records_without_aborted_list.push(offset);
        }
        read.visible
            .extend(consumer_records(batches, aborted, offset));
        if let Some(last) = batches.last() {
            offset = last.base_offset + i64::from(last.last_offset_delta) + 1;
        }
        if offset >= partition.last_stable_offset {
            return read;
        }
        assert!(
            Instant::now() <= deadline,
            "the read stalled at {offset}, below the LSO {}",
            partition.last_stable_offset
        );
    }
}

/// Kafka's `CompletedFetch.nextFetchedRecord` under `read_committed`: the
/// records of one response that a consumer hands out.
///
/// Before each batch of a producer, the aborted transactions that start at
/// or before the batch's last offset mark their producer aborted. A
/// transactional batch of an aborted producer is skipped. The producer's
/// abort marker clears it again. Control batches are never handed out, nor
/// are records below the fetch offset.
fn consumer_records(
    batches: &[RecordBatch],
    mut aborted: Vec<AbortedTransaction>,
    fetch_offset: i64,
) -> Vec<Written> {
    aborted.sort_by_key(|txn| Reverse(txn.first_offset));
    let mut aborted_producers = BTreeSet::new();
    let mut records = Vec::new();
    for batch in batches {
        let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        if batch.producer_id >= 0 {
            while aborted
                .last()
                .is_some_and(|txn| txn.first_offset <= last_offset)
            {
                let txn = aborted.pop().expect("peeked");
                aborted_producers.insert(txn.producer_id);
            }
            if is_abort_marker(batch) {
                aborted_producers.remove(&batch.producer_id);
            } else if batch.attributes.is_transactional()
                && aborted_producers.contains(&batch.producer_id)
            {
                continue;
            }
        }
        if batch.attributes.is_control_batch() {
            continue;
        }
        records.extend(
            batch
                .records
                .iter()
                .map(|record| Written {
                    offset: batch.base_offset + i64::from(record.offset_delta),
                    value: value_of(record.value.as_deref()),
                })
                .filter(|record| record.offset >= fetch_offset),
        );
    }
    records
}

/// Whether `batch` is a control batch whose marker is `ABORT`.
fn is_abort_marker(batch: &RecordBatch) -> bool {
    batch.attributes.is_control_batch()
        && batch
            .records
            .first()
            .and_then(|record| record.key.as_deref())
            .is_some_and(|key| {
                key.get(2..4)
                    .is_some_and(|kind| i16::from_be_bytes([kind[0], kind[1]]) == ABORT_MARKER)
            })
}

fn value_of(value: Option<&[u8]>) -> String {
    String::from_utf8_lossy(value.unwrap_or_default()).into_owned()
}

/// One member of a share group whose `share.isolation.level` is
/// `read_committed`, and the share session it fetches in.
pub struct ShareReader {
    group: &'static str,
    member: String,
    epoch: i32,
    /// Every offset the member acquired, with the value of its record, or
    /// `None` for an offset whose record bytes did not come back.
    pub acquired: BTreeMap<i64, Option<String>>,
}

/// The acquisitions a share group should make: exactly the `visible`
/// records.
pub fn acquisitions_of(visible: &[Written]) -> BTreeMap<i64, Option<String>> {
    visible
        .iter()
        .map(|record| (record.offset, Some(record.value.clone())))
        .collect()
}

impl ShareReader {
    /// Configures `group` to read from the earliest offset under
    /// `read_committed`, joins one member, and waits until the share
    /// coordinator has initialized the group's state for the partition.
    pub async fn join(tiered: &TieredPartition, group: &'static str) -> Self {
        let response = tiered
            .client
            .send(
                incremental_request()
                    .resources(vec![incremental_resource(GROUP_RESOURCE, group, vec![
                        incremental_config(
                            "share.auto.offset.reset",
                            Some("earliest".into()),
                            ConfigOperation::Set,
                        ),
                        incremental_config(
                            "share.isolation.level",
                            Some("read_committed".into()),
                            ConfigOperation::Set,
                        ),
                    ])])
                    .call(),
            )
            .await
            .expect("IncrementalAlterConfigs(group)");
        assert!(response.responses[0].error_code == 0, "{response:?}");
        let session = join(&tiered.client, group, TOPIC).await;
        let deadline = Instant::now() + READ_DEADLINE;
        // intentional: the share coordinator initializes the group's state
        // on a later heartbeat, and nothing signals it to the test.
        while tiered
            .broker
            .share_state_summary_for_test(group, tiered.share_topic_id, 0)
            .await
            .is_none()
        {
            assert!(Instant::now() <= deadline, "{group} was never initialized");
            let _ = tiered
                .client
                .send(ShareGroupHeartbeatRequest {
                    group_id: group.into(),
                    member_id: session.member.clone(),
                    member_epoch: session.epoch,
                    subscribed_topic_names: Some(vec![TOPIC.into()]),
                    ..Default::default()
                })
                .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Self {
            group,
            member: session.member,
            epoch: 0,
            acquired: BTreeMap::new(),
        }
    }

    /// Share-fetches until the member has acquired every offset of `want`,
    /// then fetches a few more times so an acquisition past it would show.
    pub async fn acquire_through(&mut self, tiered: &TieredPartition, want: &BTreeSet<i64>) {
        let deadline = Instant::now() + READ_DEADLINE;
        while !want.is_subset(&self.acquired.keys().copied().collect()) {
            assert!(
                Instant::now() <= deadline,
                "{} acquired only {:?}",
                self.group,
                self.acquired
            );
            if self.fetch(tiered).await == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        for _ in 0..3 {
            self.fetch(tiered).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// One `ShareFetch`; returns how many offsets it acquired.
    async fn fetch(&mut self, tiered: &TieredPartition) -> usize {
        let request = share_fetch_req()
            .session(ShareSessionSetup {
                group: self.group,
                ..ShareSessionSetup::at_epoch(
                    &self.member,
                    tiered.share_topic_id,
                    ShareSessionEpoch(self.epoch),
                )
            })
            .call();
        self.epoch += 1;
        let response = tiered.client.send(request).await.expect("ShareFetch");
        assert!(response.error_code == 0, "{response:?}");
        let Some(row) = response
            .responses
            .first()
            .and_then(|topic| topic.partitions.first())
        else {
            return 0;
        };
        assert!(row.error_code == 0, "{row:?}");
        let values: BTreeMap<i64, String> = row
            .records
            .as_ref()
            .and_then(|records| records.as_v2())
            .unwrap_or_default()
            .iter()
            .flat_map(|batch| {
                batch.records.iter().map(|record| {
                    (
                        batch.base_offset + i64::from(record.offset_delta),
                        value_of(record.value.as_deref()),
                    )
                })
            })
            .collect();
        let mut count = 0;
        for range in &row.acquired_records {
            for offset in range.first_offset..=range.last_offset {
                self.acquired.insert(offset, values.get(&offset).cloned());
                count += 1;
            }
        }
        count
    }
}
