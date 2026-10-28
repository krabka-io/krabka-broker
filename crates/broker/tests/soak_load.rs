//! The soak's load and read-back, against one in-process broker, with no
//! fault.
//!
//! `soak_contract` runs this load against three broker containers and
//! injects faults into them, so it needs Docker. Here the same producers,
//! consumer groups and readers run against one broker that nothing disturbs.
//! What every soak relies on then has to hold exactly: each acknowledged
//! record reads back once, under each declared isolation level. A transaction
//! aborted on purpose stays invisible to `read_committed`. Each committing
//! group's offset is where its last acknowledged commit put it.
#![cfg(test)]

mod support;

use {
    crate::support::{
        client::{connect_client, create_topic},
        manifest_dir,
        producer::{default_producer, producer_record},
        standalone_broker,
    },
    assert2::{assert, check},
    krabka_bench_driver::soak::{
        client::{
            Endpoint,
            load::{LoadContext, prepare, producer_name, record_value, start},
            readback::{commit_checks, read_back, round_trip, wait_stable},
            windows::Windows,
        },
        contract::{Contract, Isolation, Semantics},
        evidence::{DuplicateRead, Evidence, FINAL_COMMIT_CHECK, ReaderLedger, TxnOutcome},
        ledger::SeqSet,
        reconcile::reconcile,
    },
    krabka_units::prelude::*,
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::time,
};

/// Long enough that every consumer group has joined and had a commit
/// acknowledged, after the coordinator's initial rebalance delay.
const LOAD: Duration = Duration::from_secs(12);

fn self_check() -> Contract {
    Contract::from_file(&manifest_dir().join("../../bench/soak/self-check.yaml")).unwrap()
}

struct SoakLoad {
    broker: Option<krabka_broker::BrokerHandle>,
    directory: Option<tempfile::TempDir>,
    contract: Contract,
    bootstrap: String,
    evidence: Option<Evidence>,
    reader: Option<ReaderLedger>,
}

#[rstest::fixture]
fn soak_load() -> SoakLoad {
    // Each workload gets an isolated broker and ledger.
    SoakLoad {
        contract: self_check(),
        broker: None,
        directory: None,
        bootstrap: String::new(),
        evidence: None,
        reader: None,
    }
}

impl SoakLoad {
    fn endpoint(&self) -> Endpoint { Endpoint::plaintext(self.bootstrap.clone()) }
}

#[rstest_bdd_macros::given(
    "the self-check idempotent and transactional paths have matching topics on an undisturbed broker"
)]
async fn soak_load_topics(soak_load: &mut SoakLoad) {
    let (directory, broker) = standalone_broker().await;
    soak_load.bootstrap = broker.listen_addr().to_string();
    soak_load.directory = Some(directory);
    soak_load.broker = Some(broker);
    soak_load
        .contract
        .paths
        .retain(|path| path.semantics != Semantics::Compacted);
    let admin = connect_client(&soak_load.bootstrap, None).await;
    for path in &soak_load.contract.paths {
        create_topic(&admin, &path.name, path.partitions).await;
    }
    admin.close();
}

#[rstest_bdd_macros::when(
    "the workload runs for 12 seconds, stops, settles transactions and reads all declared isolation levels and acknowledged group commits"
)]
async fn run_soak_load(soak_load: &mut SoakLoad) {
    let contract = soak_load.contract.clone();
    let endpoint = soak_load.endpoint();
    let prepared = prepare(&contract, &endpoint).await.unwrap();
    let ctx = Arc::new(
        LoadContext::builder()
            .contract(contract.clone())
            .endpoint(endpoint.clone())
            .windows(Arc::new(Windows::new(Vec::new())))
            .build(),
    );
    let running = start(&ctx, prepared).expect("start the load");
    let trackers = running.trackers.clone();
    time::sleep(LOAD).await;
    ctx.stop();
    let outcome = running.finish().await.expect("the load tasks finish");

    let mut readers = Vec::new();
    for path in &contract.paths {
        // As the soak executor does: a transaction the stopped producer left
        // open would cut a `read_committed` read-back short.
        wait_stable(&endpoint, path, contract.clients.transactions.timeout * 2.0).await;
        for &isolation in &path.consumers {
            readers.push(read_back(&endpoint, path, isolation, &producer_name(&path.name)).await);
        }
    }
    let mut commits = Vec::new();
    for tracker in &trackers {
        commits.extend(
            commit_checks(&endpoint, tracker, FINAL_COMMIT_CHECK, &tracker.snapshot()).await,
        );
    }
    soak_load.evidence = Some(Evidence {
        producers: outcome
            .producers
            .into_iter()
            .map(|output| output.ledger)
            .collect(),
        readers,
        commits,
        ..Evidence::default()
    });
}

#[rstest_bdd_macros::then(
    "reconciliation has no findings, committed and aborted transactions both occurred and group commits were checked"
)]
fn reconciled_soak_load(soak_load: &SoakLoad) {
    let contract = &soak_load.contract;
    let evidence = soak_load.evidence.as_ref().expect("load read back");
    check!(reconcile(contract, &[], evidence).findings == []);
    // Both outcomes happened, so the hidden aborts above were really tested.
    let transactional = producer_name("classic-transactional");
    let ledger = evidence
        .producers
        .iter()
        .find(|ledger| ledger.producer == transactional)
        .unwrap();
    let outcomes = |wanted: TxnOutcome| {
        ledger
            .transactions
            .iter()
            .filter(|transaction| transaction.outcome == wanted)
            .count()
    };
    assert!(outcomes(TxnOutcome::Committed) > 0);
    assert!(outcomes(TxnOutcome::Aborted) > 0);
    check!(!evidence.commits.is_empty());
}

#[rstest_bdd_macros::when(
    "the idempotent path stores sequence 0 twice and an unrelated probe record at offset 2"
)]
async fn duplicate_soak_records(soak_load: &SoakLoad) {
    let bootstrap = soak_load.bootstrap.clone();
    let path = soak_load
        .contract
        .paths
        .iter()
        .find(|path| path.semantics == Semantics::Idempotent)
        .expect("idempotent path");
    let producer = producer_name(&path.name);
    {
        let sender = default_producer(bootstrap.clone()).await;
        for _ in 0..2 {
            let value = record_value(&producer, 0, path.payload.bytes_usize());
            sender
                .send(
                    producer_record()
                        .topic(path.name.clone())
                        .maybe_partition(Some(0))
                        .value(Some(value))
                        .call(),
                )
                .await
                .unwrap();
        }
        sender.close().await
    }
    .unwrap();
    let endpoint = soak_load.endpoint();
    // The drill's probe: a value no soak producer writes.
    check!(round_trip(&endpoint, &path.name, 0).await == Ok(2));
}

#[rstest_bdd_macros::when("the uncommitted reader reads the idempotent producer's ledger")]
async fn read_duplicate_soak_records(soak_load: &mut SoakLoad) {
    let path = soak_load
        .contract
        .paths
        .iter()
        .find(|path| path.semantics == Semantics::Idempotent)
        .expect("idempotent path");
    let producer = producer_name(&path.name);
    soak_load.reader = Some(
        read_back(
            &soak_load.endpoint(),
            path,
            Isolation::ReadUncommitted,
            &producer,
        )
        .await,
    );
}

#[rstest_bdd_macros::then(
    "the ledger contains sequence 0 at offsets 0 and 1, reports that duplicate and excludes the unrelated probe record"
)]
fn duplicated_load_ledger(soak_load: &SoakLoad) {
    let path = soak_load
        .contract
        .paths
        .iter()
        .find(|path| path.semantics == Semantics::Idempotent)
        .expect("idempotent path");
    let producer = producer_name(&path.name);
    let reader = soak_load.reader.as_ref().expect("duplicate read back");

    assert!(
        reader
            == &ReaderLedger {
                path: path.name.clone(),
                isolation: Isolation::ReadUncommitted,
                producer,
                seen: SeqSet::range(0, 1),
                duplicates: vec![DuplicateRead {
                    seq: 0,
                    positions: vec![(0, 0), (0, 1)],
                }],
                latest_by_key: BTreeMap::new(),
            }
    );
}

impl SoakLoad {
    async fn shutdown(self) {
        let Self {
            broker, directory, ..
        } = self;
        broker.expect("workload broker").shutdown().await;
        drop(directory);
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/soak_load.feature",
    name = "an undisturbed load reads back once and hides what it aborted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undisturbed_load_reads_back_once_and_hides_what_it_aborted(soak_load: SoakLoad) {
    soak_load.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/soak_load.feature",
    name = "a record stored twice is read back at both offsets and a stranger is skipped"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_stored_twice_is_read_back_at_both_offsets_and_a_stranger_is_skipped(
    soak_load: SoakLoad,
) {
    soak_load.shutdown().await;
}
