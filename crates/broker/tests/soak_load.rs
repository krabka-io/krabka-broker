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
};

/// Long enough that every consumer group has joined and had a commit
/// acknowledged, after the coordinator's initial rebalance delay.
const LOAD: Duration = Duration::from_secs(12);

fn self_check() -> Contract {
    Contract::from_file(&manifest_dir().join("../../bench/soak/self-check.yaml")).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undisturbed_load_reads_back_once_and_hides_what_it_aborted() {
    let (_dir, broker) = standalone_broker().await;
    let bootstrap = broker.listen_addr().to_string();
    let admin = connect_client(&bootstrap, None).await;
    let mut contract = self_check();
    // A compacted path needs a compacted topic and a cleaner pass to say
    // anything. The idempotent and transactional paths are what is under test.
    contract
        .paths
        .retain(|path| path.semantics != Semantics::Compacted);
    for path in &contract.paths {
        create_topic(&admin, &path.name, path.partitions).await;
    }
    let endpoint = Endpoint::plaintext(bootstrap);

    let prepared = prepare(&contract, &endpoint).await.unwrap();
    let ctx = Arc::new(
        LoadContext::builder()
            .contract(contract.clone())
            .endpoint(endpoint.clone())
            .windows(Arc::new(Windows::new(Vec::new())))
            .build(),
    );
    let running = start(&ctx, prepared);
    let trackers = running.trackers.clone();
    tokio::time::sleep(LOAD).await;
    ctx.stop();
    let outcome = running.finish().await;

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
    let evidence = Evidence {
        producers: outcome
            .producers
            .into_iter()
            .map(|output| output.ledger)
            .collect(),
        readers,
        commits,
        ..Evidence::default()
    };

    check!(reconcile(&contract, &[], &evidence).findings == []);
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

/// A record its producer sent once but that sits at two offsets is reported
/// with both, and a record no soak producer wrote is left out of the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_stored_twice_is_read_back_at_both_offsets_and_a_stranger_is_skipped() {
    let (_dir, broker) = standalone_broker().await;
    let bootstrap = broker.listen_addr().to_string();
    let admin = connect_client(&bootstrap, None).await;
    let contract = self_check();
    let path = contract
        .paths
        .iter()
        .find(|path| path.semantics == Semantics::Idempotent)
        .unwrap();
    create_topic(&admin, &path.name, path.partitions).await;
    let producer = producer_name(&path.name);
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
    sender.close().await.unwrap();
    let endpoint = Endpoint::plaintext(bootstrap);
    // The drill's probe: a value no soak producer writes.
    check!(round_trip(&endpoint, &path.name, 0).await == Ok(2));

    let reader = read_back(&endpoint, path, Isolation::ReadUncommitted, &producer).await;

    assert!(
        reader
            == ReaderLedger {
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
