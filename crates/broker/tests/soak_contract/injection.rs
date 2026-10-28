//! The self-check's injected loss: one acknowledged record deleted from the
//! cluster with `DeleteRecords` once the load stops, so the reconciler has a
//! loss it must find.

use {
    crate::topology::{Nodes, leader_of},
    krabka_bench_driver::soak::{
        client::{Endpoint, readback::groupless},
        contract::{Isolation, LoadPath},
        evidence::{InjectedLoss, ProducerLedger, parse_record_tag},
    },
    krabka_client_consumer::ConsumerRecord,
    krabka_protocol::owned::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
    },
    krabka_units::prelude::*,
    std::time::{Duration, Instant},
};

/// The first record of `topic`-`partition`, at its log-start offset.
async fn first_record(
    endpoint: &Endpoint,
    topic: &str,
    partition: i32,
) -> Result<ConsumerRecord, String> {
    let assignment = [(topic.to_owned(), partition)];
    let mut consumer = groupless(endpoint, Isolation::ReadUncommitted)
        .await
        .map_err(|error| error.to_string())?;
    consumer
        .assign(&assignment)
        .await
        .map_err(|error| error.to_string())?;
    consumer
        .seek_to_beginning(&assignment)
        .await
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let found = loop {
        if Instant::now() >= deadline {
            break Err(format!("{topic}-{partition}: no record within a minute"));
        }
        match consumer.poll(secs(1)).await {
            Ok(records) => {
                if let Some(record) = records.into_iter().next() {
                    break Ok(record);
                }
            }
            Err(error) => eprintln!("soak-contract: first record of {topic}-{partition}: {error}"),
        }
    };
    let _ = consumer.close().await;
    found
}

/// Whether `seq` is one acknowledged send and nothing else: never failed and
/// never resent, so deleting its one copy is exactly one lost record.
pub fn deletable(ledger: &ProducerLedger, seq: u64) -> bool {
    ledger.acked.contains(seq) && !ledger.failed.contains(seq) && !ledger.retried.contains(seq)
}

/// Delete one acknowledged record of `path` from the cluster: the record at
/// the log-start offset of the first partition whose first record is
/// [`deletable`], deleted with `DeleteRecords` up to its offset plus one.
pub async fn delete_one_record(
    endpoint: &Endpoint,
    nodes: &Nodes,
    path: &LoadPath,
    ledger: &ProducerLedger,
) -> Result<InjectedLoss, String> {
    for partition in 0..path.partitions {
        let record = first_record(endpoint, &path.name, partition).await?;
        let Some((owner, seq)) = record.value.as_deref().and_then(parse_record_tag) else {
            continue;
        };
        if owner != ledger.producer || !deletable(ledger, seq) {
            continue;
        }
        let (_, states) = nodes
            .metadata_from_any(None)
            .await
            .ok_or_else(|| "no node answers Metadata".to_owned())?;
        let leader = leader_of(&states, &path.name, partition)
            .ok_or_else(|| format!("{}-{partition} has no leader", path.name))?;
        let response = nodes
            .send(leader, DeleteRecordsRequest {
                topics: vec![DeleteRecordsTopic {
                    name: path.name.clone(),
                    partitions: vec![DeleteRecordsPartition {
                        partition_index: partition,
                        offset: record.offset + 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            })
            .await?;
        let result = response
            .topics
            .iter()
            .flat_map(|topic| topic.partitions.iter())
            .find(|result| result.partition_index == partition)
            .ok_or_else(|| {
                format!(
                    "DeleteRecords answered nothing for {}-{partition}",
                    path.name
                )
            })?;
        if result.error_code != 0 || result.low_watermark != record.offset + 1 {
            return Err(format!(
                "DeleteRecords on {}-{partition} up to {}: error {}, low watermark {}",
                path.name,
                record.offset + 1,
                result.error_code,
                result.low_watermark
            ));
        }
        return Ok(InjectedLoss {
            path: path.name.clone(),
            producer: ledger.producer.clone(),
            seq,
            partition,
            offset: record.offset,
        });
    }
    Err(format!(
        "no partition of {} starts with a deletable record",
        path.name
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::deletable,
        assert2::check,
        krabka_bench_driver::soak::{
            contract::Semantics, evidence::ProducerLedger, ledger::SeqSet,
        },
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, then, when},
        std::iter,
    };

    #[derive(Default)]
    struct DeletionEligibility {
        ledger: Option<ProducerLedger>,
        eligible: Slot<bool>,
    }

    #[fixture]
    fn deletion_eligibility() -> DeletionEligibility {
        // A candidate's eligibility is observed only after evaluating its producer history.
        DeletionEligibility::default()
    }

    #[given(
        "an idempotent producer attempted 13 sequences, acknowledged 0 through 9, failed 5, and retried 6"
    )]
    fn deletion_history(deletion_eligibility: &mut DeletionEligibility) {
        deletion_eligibility.ledger = Some(ProducerLedger {
            path: "p".into(),
            producer: "p-0".into(),
            semantics: Semantics::Idempotent,
            attempted: 13,
            acked: SeqSet::range(0, 10),
            failed: iter::once(5).collect(),
            retried: iter::once(6).collect(),
            transactions: Vec::new(),
        });
    }

    #[when("the loss injector considers deleting sequence {sequence}")]
    fn evaluate_deletion(deletion_eligibility: &DeletionEligibility, sequence: u64) {
        deletion_eligibility.eligible.set(deletable(
            deletion_eligibility
                .ledger
                .as_ref()
                .expect("producer ledger"),
            sequence,
        ));
    }

    #[then("the record is eligible for deliberate deletion: {eligible}")]
    fn deletion_candidate(deletion_eligibility: &DeletionEligibility, eligible: bool) {
        check!(
            deletion_eligibility
                .eligible
                .get()
                .expect("deletion evaluated")
                == eligible
        );
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/tests/soak_contract/injection.feature", fixtures = [deletion_eligibility: DeletionEligibility]);
}
