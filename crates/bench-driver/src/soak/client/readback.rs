//! After the load: the committed offsets, and the read-back that the
//! reconciler compares with the producers' ledgers.
//!
//! The read-back is a fresh consumer with no group, assigned every partition
//! of a path and started from the earliest offset, that reads until it has
//! reached each partition's end. It runs once more only when the first pass
//! found a record twice, to find every position of those records; holding a
//! position for every record of a 72-hour run would cost more memory than
//! the ledger it checks.

use {
    super::{
        Endpoint,
        load::{CommitTracker, isolation_level, key_index},
        now_ms,
    },
    crate::soak::{
        contract::{Isolation, LoadPath, Semantics},
        evidence::{CommitCheck, DuplicateRead, ReaderLedger, parse_record_tag},
        ledger::SeqSet,
    },
    bytes::Bytes,
    krabka_client_consumer::{Consumer, ConsumerError, ConsumerRecord},
    krabka_client_producer::{Acks, Producer, ProducerRecord},
    krabka_units::prelude::*,
    std::{
        collections::{BTreeMap, HashMap},
        time::{Duration, Instant},
    },
};

/// How long one read-back pass may take before it gives up and reports what
/// it read. A shortfall is then a finding of the reconciler, not a hang.
const READBACK_LIMIT: Duration = Duration::from_mins(15);

/// How long a read-back pass may go without reading a record before it gives
/// up. A cold read from the remote tier is slow, but never this slow.
const READBACK_STALL: Duration = Duration::from_mins(2);

/// How many times a committed-offset or end-offset lookup is tried.
const LOOKUP_ATTEMPTS: u32 = 10;

/// Every partition of `path`.
#[must_use]
pub fn partitions(path: &LoadPath) -> Vec<(String, i32)> {
    (0..path.partitions)
        .map(|partition| (path.name.clone(), partition))
        .collect()
}

/// Whether every partition of `ends` has a next offset at or past its end.
/// A partition that ends at offset zero holds nothing and needs no read.
#[must_use]
pub fn caught_up(next: &BTreeMap<i32, i64>, ends: &BTreeMap<i32, i64>) -> bool {
    ends.iter()
        .all(|(partition, end)| *end <= 0 || next.get(partition).is_some_and(|next| next >= end))
}

/// Whether no transaction is open on any partition: the last stable offset
/// is the high watermark everywhere.
#[must_use]
pub fn stable(committed_ends: &BTreeMap<i32, i64>, uncommitted_ends: &BTreeMap<i32, i64>) -> bool {
    !uncommitted_ends.is_empty() && committed_ends == uncommitted_ends
}

/// A consumer with no group, for reading and for offset lookups.
///
/// # Errors
///
/// Returns the client's error when the consumer cannot be built.
pub async fn groupless(
    endpoint: &Endpoint,
    isolation: Isolation,
) -> Result<Consumer, ConsumerError> {
    Consumer::builder()
        .bootstrap(endpoint.bootstrap.clone())
        .maybe_security(endpoint.security.clone())
        .client_id(format!("soak-readback-{isolation}"))
        .isolation_level(isolation_level(isolation))
        .default_api_timeout(secs(15))
        .build()
        .await
}

/// Build a consumer, ask it one question, and close it, retrying the whole
/// round up to [`LOOKUP_ATTEMPTS`] times a second apart. A cluster that is
/// still recovering from the last fault answers the first attempts with a
/// connection error or a coordinator move.
async fn ask_consumer<T, B, Q>(build: B, query: Q) -> Result<T, String>
where
    B: AsyncFn() -> Result<Consumer, ConsumerError>,
    Q: AsyncFn(&Consumer) -> Result<T, ConsumerError>,
{
    let mut last = String::new();
    for _ in 0..LOOKUP_ATTEMPTS {
        match build().await {
            Ok(consumer) => {
                let answer = query(&consumer).await;
                let _ = consumer.close().await;
                match answer {
                    Ok(answer) => return Ok(answer),
                    Err(error) => last = error.to_string(),
                }
            }
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(last)
}

/// The end offset of every partition at `isolation`.
async fn end_offsets(
    endpoint: &Endpoint,
    isolation: Isolation,
    partitions: &[(String, i32)],
) -> Result<BTreeMap<i32, i64>, String> {
    let ends = ask_consumer(
        async || groupless(endpoint, isolation).await,
        async |consumer: &Consumer| consumer.end_offsets(partitions).await,
    )
    .await?;
    Ok(ends
        .into_iter()
        .map(|((_, partition), end)| (partition, end))
        .collect())
}

/// Wait until no transaction is open on `path`, so a `read_committed`
/// read-back is not cut short by a transaction that its producer left open
/// and the coordinator has yet to time out.
pub async fn wait_stable(endpoint: &Endpoint, path: &LoadPath, limit: Time) {
    if path.semantics != Semantics::Transactional {
        return;
    }
    let partitions = partitions(path);
    let deadline = Instant::now() + limit.to_std();
    loop {
        let committed = end_offsets(endpoint, Isolation::ReadCommitted, &partitions).await;
        let uncommitted = end_offsets(endpoint, Isolation::ReadUncommitted, &partitions).await;
        if let (Ok(committed), Ok(uncommitted)) = (&committed, &uncommitted)
            && stable(committed, uncommitted)
        {
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "soak-contract: {}: transactions still open after {limit:?}: last stable {committed:?}, high watermark {uncommitted:?}",
                path.name
            );
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Read every partition of `path` at `isolation` from the earliest offset to
/// its end, handing each record to `visit`.
async fn scan(
    endpoint: &Endpoint,
    path: &LoadPath,
    isolation: Isolation,
    mut visit: impl FnMut(&ConsumerRecord),
) -> Result<(), String> {
    let partitions = partitions(path);
    let ends = end_offsets(endpoint, isolation, &partitions).await?;
    let consumer = groupless(endpoint, isolation)
        .await
        .map_err(|error| error.to_string())?;
    let mut consumer = consumer;
    consumer
        .assign(&partitions)
        .await
        .map_err(|error| error.to_string())?;
    consumer
        .seek_to_beginning(&partitions)
        .await
        .map_err(|error| error.to_string())?;
    let mut next: BTreeMap<i32, i64> = BTreeMap::new();
    let started = Instant::now();
    let mut progressed = started;
    let outcome = loop {
        if caught_up(&next, &ends) || consumer.at_log_end().await {
            break Ok(());
        }
        if started.elapsed() >= READBACK_LIMIT || progressed.elapsed() >= READBACK_STALL {
            break Err(format!(
                "{} at {isolation}: not at the end after {:?}, the last {:?} of it reading nothing: \
                 read to {next:?} of {ends:?}",
                path.name,
                started.elapsed(),
                progressed.elapsed(),
            ));
        }
        match consumer.poll(secs(1)).await {
            Ok(records) => {
                if !records.is_empty() {
                    progressed = Instant::now();
                }
                for record in &records {
                    visit(record);
                    let reached = next.entry(record.partition).or_insert(0);
                    *reached = (*reached).max(record.offset + 1);
                }
            }
            Err(error) => {
                eprintln!(
                    "soak-contract: read-back of {} at {isolation}: {error}",
                    path.name
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    let _ = consumer.close().await;
    outcome
}

/// What one reader saw of `producer`'s records on `path`.
pub async fn read_back(
    endpoint: &Endpoint,
    path: &LoadPath,
    isolation: Isolation,
    producer: &str,
) -> ReaderLedger {
    let mut seen = SeqSet::new();
    let mut repeated = SeqSet::new();
    let mut latest_by_key: BTreeMap<u64, u64> = BTreeMap::new();
    let mut foreign = 0_u64;
    let first = scan(endpoint, path, isolation, |record| {
        let Some((owner, seq)) = record.value.as_deref().and_then(parse_record_tag) else {
            foreign += 1;
            return;
        };
        if owner != producer {
            foreign += 1;
            return;
        }
        if !seen.insert(seq) {
            repeated.insert(seq);
        }
        if let (Some(keys), Some(key)) = (path.key_space, record.key.as_deref()) {
            let index = key_index(key).unwrap_or(seq % keys.max(1));
            let latest = latest_by_key.entry(index).or_insert(seq);
            *latest = (*latest).max(seq);
        }
    })
    .await;
    if let Err(error) = first {
        eprintln!("soak-contract: {error}");
    }
    if foreign > 0 {
        eprintln!(
            "soak-contract: {} at {isolation}: {foreign} records no soak producer of this path wrote",
            path.name
        );
    }
    let mut duplicates = Vec::new();
    if !repeated.is_empty() {
        let mut positions: HashMap<u64, Vec<(i32, i64)>> = HashMap::new();
        let second = scan(endpoint, path, isolation, |record| {
            if let Some((owner, seq)) = record.value.as_deref().and_then(parse_record_tag)
                && owner == producer
                && repeated.contains(seq)
            {
                positions
                    .entry(seq)
                    .or_default()
                    .push((record.partition, record.offset));
            }
        })
        .await;
        if let Err(error) = second {
            eprintln!("soak-contract: {error}");
        }
        duplicates = repeated
            .iter()
            .map(|seq| DuplicateRead {
                seq,
                positions: positions.remove(&seq).unwrap_or_default(),
            })
            .collect();
    }
    ReaderLedger {
        path: path.name.clone(),
        isolation,
        producer: producer.to_owned(),
        seen,
        duplicates,
        latest_by_key,
    }
}

/// The committed offsets of `group` on `partitions`, as `OffsetFetch` returns
/// them. A partition with nothing committed is absent.
async fn committed(
    endpoint: &Endpoint,
    group: &str,
    partitions: &[(String, i32)],
) -> Result<BTreeMap<i32, i64>, String> {
    // A consumer with a group id that never subscribes never joins, so it
    // reads the group's offsets without becoming a member of it.
    let build = async || {
        Consumer::builder()
            .bootstrap(endpoint.bootstrap.clone())
            .maybe_security(endpoint.security.clone())
            .client_id(format!("{group}-checker"))
            .group_id(group.to_owned())
            .enable_auto_commit(false)
            .default_api_timeout(secs(15))
            .build()
            .await
    };
    let fetched = ask_consumer(build, async |consumer: &Consumer| {
        consumer.committed(partitions).await
    })
    .await?;
    Ok(fetched
        .into_iter()
        .filter_map(|((_, partition), offset)| offset.map(|offset| (partition, offset.offset)))
        .collect())
}

/// One [`CommitCheck`] per partition of `acknowledged`: the last commit the
/// coordinator acknowledged before `fault`, against what `OffsetFetch` returns
/// now and the partition's log end.
///
/// A group that had acknowledged no commit yet is checked on every one of its
/// `partitions` from zero, so each fault is checked for each group and a group
/// that never commits is a finding rather than an absence.
pub async fn commit_checks(
    endpoint: &Endpoint,
    tracker: &CommitTracker,
    fault: &str,
    acknowledged: &BTreeMap<i32, i64>,
) -> Vec<CommitCheck> {
    let (group, topic) = (tracker.group.as_str(), tracker.topic.as_str());
    let acknowledged = if acknowledged.is_empty() {
        (0..tracker.partitions)
            .map(|partition| (partition, 0))
            .collect()
    } else {
        acknowledged.clone()
    };
    let partitions: Vec<(String, i32)> = acknowledged
        .keys()
        .map(|partition| (topic.to_owned(), *partition))
        .collect();
    let fetched = committed(endpoint, group, &partitions).await;
    if let Err(error) = &fetched {
        eprintln!("soak-contract: OffsetFetch of {group} after {fault}: {error}");
    }
    let ends = end_offsets(endpoint, Isolation::ReadUncommitted, &partitions)
        .await
        .unwrap_or_else(|error| {
            eprintln!("soak-contract: end offsets of {topic} after {fault}: {error}");
            BTreeMap::new()
        });
    acknowledged
        .iter()
        .map(|(partition, acked)| CommitCheck {
            group: group.to_owned(),
            topic: topic.to_owned(),
            partition: *partition,
            fault: fault.to_owned(),
            acknowledged: *acked,
            fetched: fetched
                .as_ref()
                .ok()
                .and_then(|fetched| fetched.get(partition).copied()),
            log_end: ends.get(partition).copied().unwrap_or(-1),
        })
        .collect()
}

/// Produce one record to `topic`-`partition` with `acks=all`, then read it
/// back at the offset it was acknowledged at, and return that offset.
///
/// This is what "serving" means after a restore: a client of the restored
/// cluster, authenticated as an ordinary principal, can write and then read.
///
/// # Errors
///
/// Returns why the write or the read failed, or that the record did not come
/// back within a minute.
pub async fn round_trip(endpoint: &Endpoint, topic: &str, partition: i32) -> Result<i64, String> {
    let producer = Producer::builder()
        .bootstrap(endpoint.bootstrap.clone())
        .maybe_security(endpoint.security.clone())
        .client_id("soak-drill-probe".to_owned())
        .acks(Acks::All)
        .enable_idempotence(true)
        .request_timeout(Duration::from_secs(10))
        .delivery_timeout(Duration::from_secs(30))
        .build()
        .await
        .map_err(|error| format!("build a producer: {error}"))?;
    let value = format!("soak-drill-probe/{};", now_ms());
    let sent = producer
        .send(ProducerRecord {
            topic: topic.to_owned(),
            partition: Some(partition),
            value: Some(Bytes::from(value.clone())),
            ..Default::default()
        })
        .await
        .map_err(|error| format!("produce to {topic}-{partition}: {error}"));
    let _ = producer.close().await;
    let offset = sent?.offset;
    let assignment = [(topic.to_owned(), partition)];
    let mut consumer = groupless(endpoint, Isolation::ReadUncommitted)
        .await
        .map_err(|error| format!("build a consumer: {error}"))?;
    let read = async {
        consumer
            .assign(&assignment)
            .await
            .map_err(|error| error.to_string())?;
        consumer
            .seek(topic, partition, offset)
            .await
            .map_err(|error| error.to_string())?;
        let deadline = Instant::now() + Duration::from_mins(1);
        while Instant::now() < deadline {
            match consumer.poll(secs(1)).await {
                Ok(records) => {
                    if let Some(record) = records.iter().find(|record| record.offset == offset) {
                        return if record.value.as_deref() == Some(value.as_bytes()) {
                            Ok(offset)
                        } else {
                            Err(format!(
                                "{topic}-{partition} offset {offset} holds another record"
                            ))
                        };
                    }
                }
                Err(error) => eprintln!("soak-drill: probe read of {topic}-{partition}: {error}"),
            }
        }
        Err(format!(
            "{topic}-{partition} offset {offset} did not come back within a minute"
        ))
    }
    .await;
    let _ = consumer.close().await;
    read
}

#[cfg(test)]
mod tests {
    use {
        super::{caught_up, stable},
        assert2::check,
        std::collections::BTreeMap,
    };

    fn offsets(pairs: &[(i32, i64)]) -> BTreeMap<i32, i64> { pairs.iter().copied().collect() }

    #[yare::parameterized(
        every_partition_at_its_end = { &[(0, 10), (1, 4)], &[(0, 10), (1, 4)], true },
        one_short = { &[(0, 10), (1, 3)], &[(0, 10), (1, 4)], false },
        one_unread = { &[(0, 10)], &[(0, 10), (1, 4)], false },
        empty_partition_needs_no_read = { &[(0, 10)], &[(0, 10), (1, 0)], true },
        past_the_end = { &[(0, 12)], &[(0, 10)], true },
    )]
    fn the_read_back_ends_when_every_partition_reached_its_end(
        next: &[(i32, i64)],
        ends: &[(i32, i64)],
        expected: bool,
    ) {
        check!(caught_up(&offsets(next), &offsets(ends)) == expected);
    }

    #[yare::parameterized(
        settled = { &[(0, 10), (1, 4)], &[(0, 10), (1, 4)], true },
        open_transaction = { &[(0, 8), (1, 4)], &[(0, 10), (1, 4)], false },
        nothing_known = { &[], &[], false },
    )]
    fn a_topic_is_stable_when_the_last_stable_offset_is_the_high_watermark(
        committed: &[(i32, i64)],
        uncommitted: &[(i32, i64)],
        expected: bool,
    ) {
        check!(stable(&offsets(committed), &offsets(uncommitted)) == expected);
    }
}
