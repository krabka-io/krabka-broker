//! The reconciler: what the producers were told against what the readers saw.
//!
//! The question it answers is narrower than "did every record arrive". A
//! producer that was told a send failed may have written it anyway, and an
//! application that resends after a failure may legitimately write a record
//! twice. Neither is data loss, and a reconciler that called them failures
//! would teach its readers to ignore it. So every observation is classified
//! against the semantics the path declared:
//!
//! | observation | idempotent | transactional | compacted |
//! | :--- | :--- | :--- | :--- |
//! | acknowledged, not read | **lost** | **lost** if its transaction committed (`read_committed`) or at all (`read_uncommitted`) | -- |
//! | read twice, sent once | **unexpected duplicate** | **unexpected duplicate** | -- |
//! | read twice, sent again after a failure | permitted | -- | -- |
//! | read, send failed | permitted (maybe delivered) | permitted | permitted |
//! | read, never sent | **phantom** | **phantom** | **phantom** |
//! | aborted, read under `read_committed` | -- | **aborted visible** | -- |
//! | outcome unknown, partly read under `read_committed` | -- | **partial transaction** | -- |
//! | key's latest acknowledged value not read | -- | -- | **compaction regression** |
//!
//! Committed offsets are checked separately: after each fault, the offset a
//! group fetches back must be no lower than the last commit the coordinator
//! acknowledged before it, and no higher than the log end.

use {
    super::{
        contract::{Contract, Isolation, Semantics},
        evidence::{Evidence, ProducerLedger, ReaderLedger, TxnOutcome},
        ledger::SeqSet,
    },
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, fmt},
};

/// A class of reconciliation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    /// An acknowledged record no reader can find.
    Lost,
    /// A record from an aborted transaction, read under `read_committed`.
    AbortedVisible,
    /// Part, but not all, of a transaction whose outcome is unknown, read under
    /// `read_committed`.
    PartialTransaction,
    /// A record sent once and read more than once.
    UnexpectedDuplicate,
    /// A record no producer ever sent.
    Phantom,
    /// A compacted key whose latest acknowledged value is gone.
    CompactionRegression,
    /// A committed offset that went backwards across a fault.
    CommittedOffsetRegressed,
    /// A committed offset past the log end.
    CommittedOffsetBeyondLog,
    /// A committed offset that could not be fetched back at all.
    CommittedOffsetMissing,
    /// A declared consumer whose read-back is absent from the evidence.
    NoReadback,
    /// A path whose producer acknowledged nothing.
    NothingAcknowledged,
}

impl fmt::Display for FindingKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = serde_json::to_value(self)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_default();
        f.write_str(&text)
    }
}

/// One failure of the reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub kind: FindingKind,
    pub path: String,
    /// The producer or consumer group concerned.
    pub subject: String,
    pub isolation: Option<Isolation>,
    /// The sequence numbers concerned, for the kinds that are about records.
    pub seqs: SeqSet,
    pub detail: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} on {} ({})", self.kind, self.path, self.subject)?;
        if let Some(isolation) = self.isolation {
            write!(f, " under {isolation}")?;
        }
        if !self.seqs.is_empty() {
            write!(
                f,
                ": {} records, first {:?}",
                self.seqs.len(),
                self.seqs.sample(5)
            )?;
        }
        if !self.detail.is_empty() {
            write!(f, ": {}", self.detail)?;
        }
        Ok(())
    }
}

/// What reconciliation counted for one producer and reader, pass or fail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tally {
    pub path: String,
    pub producer: String,
    pub isolation: Isolation,
    pub acknowledged: u64,
    pub read: u64,
    /// Read, although the send reported a failure.
    pub maybe_delivered: u64,
    /// Read more than once after the application resent them.
    pub permitted_duplicates: u64,
    /// Records of aborted transactions, correctly hidden or correctly shown.
    pub aborted: u64,
}

/// The reconciler's whole answer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Reconciliation {
    pub findings: Vec<Finding>,
    pub tallies: Vec<Tally>,
}

/// Reconcile `evidence` against the semantics `contract` declares.
#[must_use]
pub fn reconcile(contract: &Contract, evidence: &Evidence) -> Reconciliation {
    let mut out = Reconciliation::default();
    let mut readers: BTreeMap<(&str, Isolation), &ReaderLedger> = BTreeMap::new();
    for reader in &evidence.readers {
        readers.insert((reader.producer.as_str(), reader.isolation), reader);
    }

    for path in &contract.paths {
        let producers: Vec<&ProducerLedger> = evidence
            .producers
            .iter()
            .filter(|producer| producer.path == path.name)
            .collect();
        if producers.iter().all(|producer| producer.acked.is_empty()) {
            out.findings.push(Finding {
                kind: FindingKind::NothingAcknowledged,
                path: path.name.clone(),
                subject: "-".to_owned(),
                isolation: None,
                seqs: SeqSet::new(),
                detail: "a path that acknowledged nothing proves nothing".to_owned(),
            });
        }
        for producer in producers {
            for &isolation in &path.consumers {
                let Some(reader) = readers.get(&(producer.producer.as_str(), isolation)) else {
                    out.findings.push(Finding {
                        kind: FindingKind::NoReadback,
                        path: path.name.clone(),
                        subject: producer.producer.clone(),
                        isolation: Some(isolation),
                        seqs: SeqSet::new(),
                        detail: "the declared consumer left no read-back".to_owned(),
                    });
                    continue;
                };
                reconcile_reader(path.key_space, producer, reader, &mut out);
            }
        }
    }

    for check in &evidence.commits {
        let path = contract
            .paths
            .iter()
            .find(|path| path.name == check.topic)
            .map_or_else(|| check.topic.clone(), |path| path.name.clone());
        let mut fail = |kind, detail: String| {
            out.findings.push(Finding {
                kind,
                path: path.clone(),
                subject: check.group.clone(),
                isolation: None,
                seqs: SeqSet::new(),
                detail,
            });
        };
        let place = format!("{}-{} across {}", check.topic, check.partition, check.fault);
        match check.fetched {
            None => fail(
                FindingKind::CommittedOffsetMissing,
                format!("{place}: no committed offset came back"),
            ),
            Some(fetched) if fetched < check.acknowledged => fail(
                FindingKind::CommittedOffsetRegressed,
                format!(
                    "{place}: fetched {fetched}, below the acknowledged commit {}",
                    check.acknowledged
                ),
            ),
            Some(fetched) if fetched > check.log_end => fail(
                FindingKind::CommittedOffsetBeyondLog,
                format!(
                    "{place}: fetched {fetched}, past the log end {}",
                    check.log_end
                ),
            ),
            Some(_) => {}
        }
    }
    out.findings
        .sort_by(|a, b| (a.kind, &a.path, &a.subject).cmp(&(b.kind, &b.path, &b.subject)));
    out
}

fn reconcile_reader(
    key_space: Option<u64>,
    producer: &ProducerLedger,
    reader: &ReaderLedger,
    out: &mut Reconciliation,
) {
    let mut emit = |kind, seqs: SeqSet, detail: String| {
        if !seqs.is_empty() || !detail.is_empty() {
            out.findings.push(Finding {
                kind,
                path: producer.path.clone(),
                subject: producer.producer.clone(),
                isolation: Some(reader.isolation),
                seqs,
                detail,
            });
        }
    };
    let sent = SeqSet::range(0, producer.attempted);
    emit(
        FindingKind::Phantom,
        reader.seen.difference(&sent),
        String::new(),
    );

    let mut aborted = SeqSet::new();
    let mut committed = SeqSet::new();
    for txn in &producer.transactions {
        match txn.outcome {
            TxnOutcome::Committed => committed = committed.union(&txn.seqs),
            TxnOutcome::Aborted => aborted = aborted.union(&txn.seqs),
            TxnOutcome::Unknown => {
                let visible = reader.seen.intersection(&txn.seqs);
                if reader.isolation == Isolation::ReadCommitted
                    && !visible.is_empty()
                    && visible.len() < txn.seqs.len()
                {
                    emit(
                        FindingKind::PartialTransaction,
                        txn.seqs.difference(&visible),
                        format!(
                            "transaction {} shows {} of its {} records",
                            txn.id,
                            visible.len(),
                            txn.seqs.len()
                        ),
                    );
                }
            }
        }
    }

    let must_read = match (producer.semantics, reader.isolation) {
        (Semantics::Transactional, Isolation::ReadCommitted) => {
            producer.acked.intersection(&committed)
        }
        (Semantics::Compacted, _) => SeqSet::new(),
        _ => producer.acked.clone(),
    };
    emit(
        FindingKind::Lost,
        must_read.difference(&reader.seen),
        String::new(),
    );
    if producer.semantics == Semantics::Transactional
        && reader.isolation == Isolation::ReadCommitted
    {
        emit(
            FindingKind::AbortedVisible,
            aborted.intersection(&reader.seen),
            String::new(),
        );
    }

    let resent = producer.retried.union(&producer.failed);
    let mut permitted = 0;
    let mut unexpected = SeqSet::new();
    for duplicate in &reader.duplicates {
        if resent.contains(duplicate.seq) {
            permitted += 1;
        } else {
            unexpected.insert(duplicate.seq);
        }
    }
    if producer.semantics != Semantics::Compacted {
        emit(FindingKind::UnexpectedDuplicate, unexpected, String::new());
    }

    if let (Semantics::Compacted, Some(space)) = (producer.semantics, key_space) {
        let mut latest_acked: BTreeMap<u64, u64> = BTreeMap::new();
        for seq in producer.acked.iter() {
            latest_acked.insert(seq % space, seq);
        }
        let regressed: SeqSet = latest_acked
            .iter()
            .filter(|(key, seq)| {
                reader
                    .latest_by_key
                    .get(*key)
                    .is_none_or(|read| read < *seq)
            })
            .map(|(_, seq)| *seq)
            .collect();
        emit(FindingKind::CompactionRegression, regressed, String::new());
    }

    out.tallies.push(Tally {
        path: producer.path.clone(),
        producer: producer.producer.clone(),
        isolation: reader.isolation,
        acknowledged: producer.acked.len(),
        read: reader.seen.len(),
        maybe_delivered: reader
            .seen
            .intersection(&producer.failed)
            .difference(&producer.acked)
            .len(),
        permitted_duplicates: permitted,
        aborted: aborted.len(),
    });
}

#[cfg(test)]
mod tests {
    use {
        super::{FindingKind, reconcile},
        crate::soak::{
            contract::{Contract, Isolation, Semantics, fixture},
            evidence::{
                CommitCheck, DuplicateRead, Evidence, ProducerLedger, ReaderLedger, TxnLedger,
                TxnOutcome,
            },
            ledger::SeqSet,
        },
        assert2::check,
        std::collections::BTreeMap,
    };

    const IDEMPOTENT: &str = "classic-idempotent";
    const TRANSACTIONAL: &str = "classic-transactional";

    fn producer(path: &str, semantics: Semantics, attempted: u64) -> ProducerLedger {
        ProducerLedger {
            path: path.to_owned(),
            producer: format!("{path}-0"),
            semantics,
            attempted,
            acked: SeqSet::range(0, attempted),
            failed: SeqSet::new(),
            retried: SeqSet::new(),
            transactions: Vec::new(),
        }
    }

    fn reader(path: &str, isolation: Isolation, seen: SeqSet) -> ReaderLedger {
        ReaderLedger {
            path: path.to_owned(),
            isolation,
            producer: format!("{path}-0"),
            seen,
            duplicates: Vec::new(),
            latest_by_key: BTreeMap::new(),
        }
    }

    /// Ten idempotent records and two transactions of five, the second
    /// aborted, every one read where its semantics say it should be.
    fn clean() -> (Contract, Evidence) {
        let contract = fixture::self_check();
        let mut txn = producer(TRANSACTIONAL, Semantics::Transactional, 10);
        txn.transactions = vec![
            TxnLedger {
                id: 0,
                seqs: SeqSet::range(0, 5),
                outcome: TxnOutcome::Committed,
            },
            TxnLedger {
                id: 1,
                seqs: SeqSet::range(5, 10),
                outcome: TxnOutcome::Aborted,
            },
        ];
        let evidence = Evidence {
            contract: contract.name.clone(),
            producers: vec![producer(IDEMPOTENT, Semantics::Idempotent, 10), txn],
            readers: vec![
                reader(IDEMPOTENT, Isolation::ReadUncommitted, SeqSet::range(0, 10)),
                reader(TRANSACTIONAL, Isolation::ReadCommitted, SeqSet::range(0, 5)),
                reader(
                    TRANSACTIONAL,
                    Isolation::ReadUncommitted,
                    SeqSet::range(0, 10),
                ),
            ],
            commits: vec![CommitCheck {
                group: "g".into(),
                topic: IDEMPOTENT.into(),
                partition: 0,
                fault: "leader-restart#0".into(),
                acknowledged: 4,
                fetched: Some(4),
                log_end: 6,
            }],
            ..Evidence::default()
        };
        (contract, evidence)
    }

    fn kinds(contract: &Contract, evidence: &Evidence) -> Vec<FindingKind> {
        reconcile(contract, evidence)
            .findings
            .iter()
            .map(|finding| finding.kind)
            .collect()
    }

    #[test]
    fn a_clean_run_has_no_findings() {
        let (contract, evidence) = clean();
        let outcome = reconcile(&contract, &evidence);
        check!(outcome.findings.is_empty(), "{:?}", outcome.findings);
        check!(outcome.tallies.len() == 3);
    }

    #[test]
    fn a_missing_acknowledged_record_is_lost_and_named() {
        let (contract, mut evidence) = clean();
        evidence.readers[0].seen.remove(3);
        let outcome = reconcile(&contract, &evidence);
        check!(outcome.findings.len() == 1);
        check!(outcome.findings[0].kind == FindingKind::Lost);
        check!(outcome.findings[0].seqs == [3].into_iter().collect());
    }

    type Edit = fn(&mut Evidence);

    #[yare::parameterized(
        committed_txn_record_missing = { |e: &mut Evidence| { e.readers[1].seen.remove(2); }, &[FindingKind::Lost] },
        aborted_record_under_read_uncommitted_missing = { |e: &mut Evidence| { e.readers[2].seen.remove(7); }, &[FindingKind::Lost] },
        aborted_record_visible = { |e: &mut Evidence| { e.readers[1].seen.insert(7); }, &[FindingKind::AbortedVisible] },
        phantom = { |e: &mut Evidence| { e.readers[0].seen.insert(99); }, &[FindingKind::Phantom] },
        duplicate_of_a_single_send = { |e: &mut Evidence| e.readers[0].duplicates.push(DuplicateRead { seq: 4, positions: vec![(0, 4), (0, 11)] }), &[FindingKind::UnexpectedDuplicate] },
        commit_regressed = { |e: &mut Evidence| e.commits[0].fetched = Some(2), &[FindingKind::CommittedOffsetRegressed] },
        commit_beyond_log = { |e: &mut Evidence| e.commits[0].fetched = Some(7), &[FindingKind::CommittedOffsetBeyondLog] },
        commit_gone = { |e: &mut Evidence| e.commits[0].fetched = None, &[FindingKind::CommittedOffsetMissing] },
        reader_absent = { |e: &mut Evidence| { e.readers.remove(2); }, &[FindingKind::NoReadback] },
        nothing_acked = { |e: &mut Evidence| e.producers[0].acked = SeqSet::new(), &[FindingKind::NothingAcknowledged] },
    )]
    fn each_violation_is_classified(edit: Edit, expected: &[FindingKind]) {
        let (contract, mut evidence) = clean();
        edit(&mut evidence);
        check!(kinds(&contract, &evidence) == expected);
    }

    #[yare::parameterized(
        failed_send_read = { |e: &mut Evidence| { e.producers[0].attempted = 11; e.producers[0].failed.insert(10); e.readers[0].seen.insert(10); } },
        failed_send_not_read = { |e: &mut Evidence| { e.producers[0].attempted = 11; e.producers[0].failed.insert(10); } },
        resent_record_read_twice = { |e: &mut Evidence| { e.producers[0].retried.insert(4); e.readers[0].duplicates.push(DuplicateRead { seq: 4, positions: vec![(0, 4), (0, 11)] }); } },
        commit_moved_forward = { |e: &mut Evidence| e.commits[0].fetched = Some(6) },
    )]
    fn permitted_retries_are_not_failures(edit: Edit) {
        let (contract, mut evidence) = clean();
        edit(&mut evidence);
        check!(kinds(&contract, &evidence).is_empty());
    }

    #[yare::parameterized(
        none_visible = { SeqSet::range(0, 5), &[] },
        all_visible = { SeqSet::range(0, 10), &[] },
        half_visible = { SeqSet::range(0, 7), &[FindingKind::PartialTransaction] },
    )]
    fn an_unknown_transaction_must_be_all_or_nothing(seen: SeqSet, expected: &[FindingKind]) {
        let (contract, mut evidence) = clean();
        evidence.producers[1].transactions[1].outcome = TxnOutcome::Unknown;
        evidence.readers[1].seen = seen;
        check!(kinds(&contract, &evidence) == expected);
    }

    #[test]
    fn a_failed_send_that_was_read_counts_as_maybe_delivered() {
        let (contract, mut evidence) = clean();
        evidence.producers[0].attempted = 12;
        evidence.producers[0].failed = SeqSet::range(10, 12);
        evidence.readers[0].seen.insert(11);
        let outcome = reconcile(&contract, &evidence);
        check!(outcome.tallies[0].maybe_delivered == 1);
    }

    #[test]
    fn compaction_must_keep_each_keys_latest_acknowledged_value() {
        let mut contract = fixture::self_check();
        contract.paths[0].semantics = Semantics::Compacted;
        contract.paths[0].key_space = Some(4);
        contract.paths.truncate(1);
        let mut compacted = producer(IDEMPOTENT, Semantics::Compacted, 10);
        compacted.semantics = Semantics::Compacted;
        // Keys 0..4: the latest acknowledged sequences are 8, 9, 6 and 7.
        let mut read = reader(
            IDEMPOTENT,
            Isolation::ReadUncommitted,
            [6, 7, 8, 9].into_iter().collect(),
        );
        read.latest_by_key = BTreeMap::from([(0, 8), (1, 9), (2, 6), (3, 7)]);
        let mut evidence = Evidence {
            producers: vec![compacted],
            readers: vec![read],
            ..Evidence::default()
        };
        check!(kinds(&contract, &evidence).is_empty());

        // Key 2's latest value was compacted away in favour of an older one.
        evidence.readers[0].latest_by_key.insert(2, 2);
        let outcome = reconcile(&contract, &evidence);
        check!(outcome.findings.len() == 1);
        check!(outcome.findings[0].kind == FindingKind::CompactionRegression);
        check!(outcome.findings[0].seqs == [6].into_iter().collect());
    }
}
