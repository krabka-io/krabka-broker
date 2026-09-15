//! Partition enrollment into a locally-coordinated transaction.
//!
//! The module holds the `AddPartitionsToTxn` transition that validates the
//! producer identity, moves the entry to `Ongoing`, and records the partitions
//! the transaction writes. It also holds the KIP-890 path that routes an
//! offsets-partition enrollment to the broker that coordinates the
//! `transactional_id`, over the inter-broker client when that broker is remote.

use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_verified::transaction::{
    TransactionRegistrationDecision, TransactionRegistrationFacts,
    TransactionRegistrationIdentityFacts, TransactionRegistrationOwnershipFacts,
    TransactionRegistrationStateFacts, transaction_partition_registration,
};

use super::TxnCoordinator;
use crate::{
    coordinator::bootstrap::OFFSETS_TOPIC,
    txn::{state::TxnState, version::TxnVersion},
};

impl TxnCoordinator {
    /// Add partitions to the locally-coordinated transaction after validating
    /// its producer identity. This is shared by client `AddPartitionsToTxn` and
    /// the KIP-890 server-side `TxnOffsetCommit` path.
    pub(crate) async fn register_partitions(
        &self,
        tid: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        partitions: Vec<crate::txn::state::TopicPartition>,
        txnv: TxnVersion,
    ) -> i16 {
        let is_coordinator = self.is_coordinator_for(tid).await;
        if !is_coordinator {
            return registration_code(transaction_partition_registration(
                TransactionRegistrationFacts {
                    ownership: TransactionRegistrationOwnershipFacts {
                        is_coordinator: false,
                        producer_id_valid: true,
                        entry_exists: false,
                    },
                    identity: TransactionRegistrationIdentityFacts {
                        transactional_id_matches: false,
                        staged_identity: false,
                        producer_identity_matches: false,
                    },
                    state: TransactionRegistrationStateFacts {
                        state_allows_registration: false,
                        exact_partitions_registered: false,
                    },
                },
            ));
        }
        let Some(entry_mutex) = self.get(tid) else {
            return registration_code(transaction_partition_registration(
                TransactionRegistrationFacts {
                    ownership: TransactionRegistrationOwnershipFacts {
                        is_coordinator: true,
                        producer_id_valid: producer_id.get() >= 0,
                        entry_exists: false,
                    },
                    identity: TransactionRegistrationIdentityFacts {
                        transactional_id_matches: false,
                        staged_identity: false,
                        producer_identity_matches: false,
                    },
                    state: TransactionRegistrationStateFacts {
                        state_allows_registration: false,
                        exact_partitions_registered: false,
                    },
                },
            ));
        };
        let mut entry = entry_mutex.lock().await;
        let decision = transaction_partition_registration(TransactionRegistrationFacts {
            ownership: TransactionRegistrationOwnershipFacts {
                is_coordinator: true,
                producer_id_valid: producer_id.get() >= 0,
                entry_exists: true,
            },
            identity: TransactionRegistrationIdentityFacts {
                transactional_id_matches: entry.transactional_id == tid,
                staged_identity: entry.has_staged_producer_identity(),
                producer_identity_matches: entry.producer_id == producer_id
                    && entry.producer_epoch == producer_epoch,
            },
            state: TransactionRegistrationStateFacts {
                state_allows_registration: entry.state.can_transition_to(TxnState::Ongoing),
                exact_partitions_registered: partitions
                    .iter()
                    .all(|partition| entry.partitions.contains(partition)),
            },
        });
        if !matches!(
            decision,
            TransactionRegistrationDecision::PersistRetry
                | TransactionRegistrationDecision::PersistRegistration
        ) {
            return registration_code(decision);
        }
        let prior_state = entry.state;
        if matches!(
            prior_state,
            TxnState::CompleteCommit | TxnState::CompleteAbort
        ) {
            entry.partitions.clear();
        }
        entry.state = TxnState::Ongoing;
        if prior_state != TxnState::Ongoing {
            entry.start_ms = crate::txn::util::now_millis();
        }
        entry.partitions.extend(partitions);
        entry.last_update_ms = crate::txn::util::now_millis();
        let snapshot = entry.clone();
        drop(entry);

        if let Err(error) = self.put(snapshot, txnv).await {
            tracing::error!(tid, %error, "failed to persist registered transaction partitions");
            return crate::codes::UNKNOWN_SERVER_ERROR;
        }
        crate::codes::NONE
    }

    /// KIP-890: route the offsets partition enrollment to the transaction
    /// coordinator before a v5+ `TxnOffsetCommit` append.
    pub(crate) async fn register_offsets_partition(
        &self,
        tid: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        offsets_partition: PartitionIndex,
        txnv: TxnVersion,
    ) -> i16 {
        let code = self
            .add_or_verify_partition(
                super::produce_verification::PartitionCheck {
                    transactional_id: tid,
                    producer_id,
                    producer_epoch,
                    partition: crate::txn::state::TopicPartition {
                        topic: OFFSETS_TOPIC.to_string(),
                        partition: offsets_partition,
                    },
                    verify_only: false,
                },
                txnv,
            )
            .await;
        // `TxnOffsetCommit` has answered a coordinator it cannot reach with
        // COORDINATOR_NOT_AVAILABLE, which its clients retry.
        if code == crate::codes::NETWORK_EXCEPTION {
            crate::codes::COORDINATOR_NOT_AVAILABLE
        } else {
            code
        }
    }
}

fn registration_code(decision: TransactionRegistrationDecision) -> i16 {
    match decision {
        TransactionRegistrationDecision::RejectNotCoordinator => crate::codes::NOT_COORDINATOR,
        TransactionRegistrationDecision::RejectUnknownProducer => {
            crate::codes::INVALID_PRODUCER_ID_MAPPING
        }
        TransactionRegistrationDecision::RejectStagedIdentity
        | TransactionRegistrationDecision::RejectState => crate::codes::INVALID_TXN_STATE,
        TransactionRegistrationDecision::RejectStaleIdentity => {
            crate::codes::INVALID_PRODUCER_EPOCH
        }
        TransactionRegistrationDecision::PersistRetry
        | TransactionRegistrationDecision::PersistRegistration => crate::codes::NONE,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};
    use tokio::sync::Mutex;

    use super::{TxnCoordinator, TxnState, TxnVersion};
    use crate::txn::{bootstrap, coordinator::test_support::test_coordinator, state::TxnEntry};

    fn partition(topic: &str, index: i32) -> crate::txn::state::TopicPartition {
        crate::txn::state::TopicPartition {
            topic: topic.to_string(),
            partition: PartitionIndex(index),
        }
    }

    async fn install_entry(coordinator: &TxnCoordinator, entry: TxnEntry) {
        let coordinator_partition = coordinator.partition_for(&entry.transactional_id);
        coordinator
            .lead_state_partition_for_test(coordinator_partition)
            .await;
        coordinator
            .state
            .insert(entry.transactional_id.clone(), Arc::new(Mutex::new(entry)));
    }

    fn open_transaction_partition(
        coordinator: &TxnCoordinator,
        directory: &std::path::Path,
        transactional_id: &str,
    ) -> Arc<crate::partition::Partition> {
        let index = coordinator.partition_for(transactional_id);
        let partition_dir = crate::log_dir::partition_dir(directory, bootstrap::TOPIC, index.get());
        std::fs::create_dir_all(&partition_dir).expect("create transaction-state directory");
        let log = Log::open(&partition_dir, LogConfig::default()).expect("open transaction log");
        let opened = crate::broker::spawn_partition(
            bootstrap::TOPIC.to_string(),
            index,
            directory.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        coordinator
            .partitions
            .insert(bootstrap::TOPIC.into(), index, Arc::clone(&opened));
        opened
    }

    #[tokio::test]
    async fn registration_adapter_fences_coordinator_mapping_and_generation() {
        let coordinator = test_coordinator();
        let requested = vec![partition("orders", 4)];

        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    3,
                    requested.clone(),
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::NOT_COORDINATOR
        );
        coordinator
            .lead_state_partition_for_test(coordinator.partition_for("tid-a"))
            .await;
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    3,
                    requested.clone(),
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        let malformed = TxnEntry::new_empty("other-tid".into(), ProducerId(7), 3, 60_000, 0);
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(malformed)));
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    3,
                    requested.clone(),
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        entry.next_producer_epoch = 0;
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(entry)));
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MAX,
                    requested.clone(),
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::INVALID_TXN_STATE
        );

        let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        entry.state = TxnState::Dead;
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(entry)));
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MIN,
                    requested.clone(),
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::INVALID_PRODUCER_EPOCH
        );
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(-1),
                    i16::MAX,
                    requested.clone(),
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MAX,
                    requested,
                    TxnVersion::Classic,
                )
                .await
                == crate::codes::INVALID_TXN_STATE
        );
    }

    #[tokio::test]
    async fn exact_registration_and_retry_are_both_persisted() {
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = test_coordinator();
        let entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        install_entry(&coordinator, entry).await;
        let transaction_partition =
            open_transaction_partition(&coordinator, directory.path(), "tid-a");
        let requested = partition("orders", i32::MAX);

        for expected_end in [1, 2] {
            let code = coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MAX,
                    vec![requested.clone()],
                    TxnVersion::Classic,
                )
                .await;
            check!(code == crate::codes::NONE);
            check!(transaction_partition.log_end_offset().0 == expected_end);
        }
        let stored = coordinator.get("tid-a").expect("registered entry");
        let stored = stored.lock().await;
        assert!(stored.partitions.contains(&requested));
        assert!(!stored.partitions.contains(&partition("orders", 0)));
        drop(stored);

        let stale = coordinator
            .register_partitions(
                "tid-a",
                ProducerId(7),
                i16::MAX - 1,
                vec![partition("payments", 0)],
                TxnVersion::Classic,
            )
            .await;
        check!(stale == crate::codes::INVALID_PRODUCER_EPOCH);
        check!(transaction_partition.log_end_offset().0 == 2);
    }

    #[tokio::test]
    async fn failed_registration_retry_never_reports_success() {
        let coordinator = test_coordinator();
        let entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), 0, 60_000, 0);
        install_entry(&coordinator, entry).await;
        let requested = partition("orders", 9);

        for _ in 0..2 {
            check!(
                coordinator
                    .register_partitions(
                        "tid-a",
                        ProducerId(7),
                        0,
                        vec![requested.clone()],
                        TxnVersion::Classic,
                    )
                    .await
                    == crate::codes::UNKNOWN_SERVER_ERROR
            );
        }
        let stored = coordinator.get("tid-a").expect("entry remains present");
        assert!(stored.lock().await.partitions.contains(&requested));
    }
}
