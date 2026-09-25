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
    TransactionRegistrationIdentityFacts, TransactionRegistrationIdentityMatchFacts,
    TransactionRegistrationOwnershipFacts, TransactionRegistrationStateFacts,
    transaction_partition_registration,
};

use super::{TxnCoordinator, produce_verification::INTERNAL_REGISTRATION_VERSION};
use crate::{
    coordinator::bootstrap::OFFSETS_TOPIC,
    txn::{state::TxnState, version::TxnVersion},
};

/// `AddPartitionsToTxn` request version at and above which the wire protocol
/// carries `PRODUCER_FENCED` (90, KIP-360). Below it, Kafka's `KafkaApis`
/// downgrades that answer to the legacy `INVALID_PRODUCER_EPOCH` (47).
const PRODUCER_FENCED_MIN_VERSION: i16 = INTERNAL_REGISTRATION_VERSION;

impl TxnCoordinator {
    /// Add partitions to the locally-coordinated transaction after validating
    /// its producer identity. This is shared by client `AddPartitionsToTxn` and
    /// the KIP-890 server-side `TxnOffsetCommit` path.
    ///
    /// `version` is the `AddPartitionsToTxn` request version, which selects
    /// between `PRODUCER_FENCED` and the legacy `INVALID_PRODUCER_EPOCH` on a
    /// producer-epoch mismatch.
    pub(crate) async fn register_partitions(
        &self,
        tid: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        partitions: Vec<crate::txn::state::TopicPartition>,
        txnv: TxnVersion,
        version: i16,
    ) -> i16 {
        if tid.is_empty() {
            return crate::codes::INVALID_REQUEST;
        }
        if let Some(code @ crate::codes::COORDINATOR_LOAD_IN_PROGRESS) =
            self.coordinator_error(tid).await
        {
            return code;
        }
        let is_coordinator = self.is_coordinator_for(tid).await;
        if !is_coordinator {
            return registration_code(
                transaction_partition_registration(TransactionRegistrationFacts {
                    ownership: TransactionRegistrationOwnershipFacts {
                        is_coordinator: false,
                        producer_id_valid: true,
                        entry_exists: false,
                    },
                    identity: TransactionRegistrationIdentityFacts {
                        pending_transition: false,
                        matching: TransactionRegistrationIdentityMatchFacts {
                            transactional_id_matches: false,
                            producer_id_matches: false,
                            producer_epoch_matches: false,
                        },
                    },
                    state: TransactionRegistrationStateFacts {
                        state_allows_registration: false,
                        state_is_ongoing: false,
                        exact_partitions_registered: false,
                    },
                }),
                version,
            );
        }
        let Some(entry_mutex) = self.get(tid) else {
            return registration_code(
                transaction_partition_registration(TransactionRegistrationFacts {
                    ownership: TransactionRegistrationOwnershipFacts {
                        is_coordinator: true,
                        producer_id_valid: producer_id.get() >= 0,
                        entry_exists: false,
                    },
                    identity: TransactionRegistrationIdentityFacts {
                        pending_transition: false,
                        matching: TransactionRegistrationIdentityMatchFacts {
                            transactional_id_matches: false,
                            producer_id_matches: false,
                            producer_epoch_matches: false,
                        },
                    },
                    state: TransactionRegistrationStateFacts {
                        state_allows_registration: false,
                        state_is_ongoing: false,
                        exact_partitions_registered: false,
                    },
                }),
                version,
            );
        };
        let entry = entry_mutex.lock().await;
        let decision = transaction_partition_registration(TransactionRegistrationFacts {
            ownership: TransactionRegistrationOwnershipFacts {
                is_coordinator: true,
                producer_id_valid: producer_id.get() >= 0,
                entry_exists: true,
            },
            identity: TransactionRegistrationIdentityFacts {
                pending_transition: entry.has_staged_producer_identity(),
                matching: TransactionRegistrationIdentityMatchFacts {
                    transactional_id_matches: entry.transactional_id == tid,
                    producer_id_matches: entry.producer_id == producer_id,
                    producer_epoch_matches: entry.producer_epoch == producer_epoch,
                },
            },
            state: TransactionRegistrationStateFacts {
                state_allows_registration: entry.state.can_transition_to(TxnState::Ongoing),
                state_is_ongoing: entry.state == TxnState::Ongoing,
                exact_partitions_registered: partitions
                    .iter()
                    .all(|partition| entry.partitions.contains(partition)),
            },
        });
        match decision {
            // Kafka's optimization: every requested partition is already in
            // an Ongoing transaction's set, so the answer is NONE with no
            // append. A stale exact match from a completed or not-yet-started
            // transaction does not qualify: `state_is_ongoing` in the facts
            // above rules it out, so those cases fall to `PersistRegistration`
            // below and still write.
            TransactionRegistrationDecision::PersistRetry => return crate::codes::NONE,
            TransactionRegistrationDecision::PersistRegistration => {}
            other => return registration_code(other, version),
        }
        // Stage the mutation on a clone: `self.put` only replaces the live
        // `self.state[tid]` entry on a successful append. Mutating the locked
        // guard directly would leave the *live* entry looking registered
        // (Ongoing, with the requested partitions) even when the append
        // below fails, which would let a subsequent identical retry take the
        // no-write `PersistRetry` path above and wrongly report success for
        // a registration that was never made durable.
        let mut snapshot = entry.clone();
        drop(entry);
        let prior_state = snapshot.state;
        if matches!(
            prior_state,
            TxnState::CompleteCommit | TxnState::CompleteAbort
        ) {
            snapshot.partitions.clear();
        }
        snapshot.state = TxnState::Ongoing;
        if prior_state != TxnState::Ongoing {
            snapshot.start_ms = crate::txn::util::now_millis();
        }
        snapshot.partitions.extend(partitions);
        snapshot.last_update_ms = crate::txn::util::now_millis();

        if let Err(error) = self.put(snapshot, txnv).await {
            tracing::error!(tid, %error, "failed to persist registered transaction partitions");
            return self.append_error_code(tid).await;
        }
        crate::codes::NONE
    }

    /// KIP-890: route the offsets partition enrollment to the transaction
    /// coordinator before a v5+ `TxnOffsetCommit` append.
    pub(crate) async fn register_offsets_partition(
        self: &std::sync::Arc<Self>,
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
                INTERNAL_REGISTRATION_VERSION,
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

/// Maps a registration decision to its wire code, at the given
/// `AddPartitionsToTxn` request version. Kafka's
/// `TransactionCoordinator.handleAddPartitionsToTransaction`:
///
/// - a pending transition, and the `PrepareCommit`/`PrepareAbort` state
///   check, both answer the retriable `CONCURRENT_TRANSACTIONS` (51);
/// - a producer-id mismatch answers `INVALID_PRODUCER_ID_MAPPING` (49);
/// - a producer-epoch mismatch answers `PRODUCER_FENCED` (90), downgraded to
///   the legacy `INVALID_PRODUCER_EPOCH` (47) below request version 2
///   (`KafkaApis.scala`).
fn registration_code(decision: TransactionRegistrationDecision, version: i16) -> i16 {
    match decision {
        TransactionRegistrationDecision::RejectNotCoordinator => crate::codes::NOT_COORDINATOR,
        TransactionRegistrationDecision::RejectUnknownProducer
        | TransactionRegistrationDecision::RejectProducerId => {
            crate::codes::INVALID_PRODUCER_ID_MAPPING
        }
        TransactionRegistrationDecision::RejectPendingTransition
        | TransactionRegistrationDecision::RejectState => crate::codes::CONCURRENT_TRANSACTIONS,
        TransactionRegistrationDecision::RejectProducerEpoch => {
            if version < PRODUCER_FENCED_MIN_VERSION {
                crate::codes::INVALID_PRODUCER_EPOCH
            } else {
                crate::codes::PRODUCER_FENCED
            }
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
                    3,
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
                    3,
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
                    3,
                )
                .await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        // A pending transition (a staged producer identity) is
        // CONCURRENT_TRANSACTIONS, checked ahead of the producer id and
        // epoch, per Kafka's `TransactionCoordinator.handleAddPartitionsToTransaction`.
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
                    3,
                )
                .await
                == crate::codes::CONCURRENT_TRANSACTIONS
        );

        // A producer-epoch mismatch is PRODUCER_FENCED at request version 2
        // and above, downgraded to the legacy INVALID_PRODUCER_EPOCH below.
        let dead = || {
            let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
            entry.state = TxnState::Dead;
            entry
        };
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(dead())));
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MIN,
                    requested.clone(),
                    TxnVersion::Classic,
                    1,
                )
                .await
                == crate::codes::INVALID_PRODUCER_EPOCH
        );
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(dead())));
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MIN,
                    requested.clone(),
                    TxnVersion::Classic,
                    3,
                )
                .await
                == crate::codes::PRODUCER_FENCED
        );

        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(-1),
                    i16::MAX,
                    requested.clone(),
                    TxnVersion::Classic,
                    3,
                )
                .await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        // Matching identity but a state that cannot move to Ongoing (Dead)
        // is CONCURRENT_TRANSACTIONS, matching Kafka's PrepareCommit /
        // PrepareAbort retriable answer rather than a fatal one.
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(dead())));
        check!(
            coordinator
                .register_partitions(
                    "tid-a",
                    ProducerId(7),
                    i16::MAX,
                    requested,
                    TxnVersion::Classic,
                    3,
                )
                .await
                == crate::codes::CONCURRENT_TRANSACTIONS
        );
    }

    /// Kafka's `TransactionCoordinator.handleAddPartitionsToTransaction` never
    /// checks the transactional id for null/empty -- it is the very first
    /// check, and it answers `INVALID_REQUEST` whatever else is true.
    #[tokio::test]
    async fn empty_transactional_id_is_invalid_request() {
        let coordinator = test_coordinator();
        check!(
            coordinator
                .register_partitions(
                    "",
                    ProducerId(7),
                    0,
                    vec![partition("orders", 0)],
                    TxnVersion::Classic,
                    3,
                )
                .await
                == crate::codes::INVALID_REQUEST
        );
    }

    #[tokio::test]
    async fn exact_registration_persists_once_then_answers_none_without_a_write() {
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = test_coordinator();
        let entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        install_entry(&coordinator, entry).await;
        let transaction_partition =
            open_transaction_partition(&coordinator, directory.path(), "tid-a");
        let requested = partition("orders", i32::MAX);

        let first = coordinator
            .register_partitions(
                "tid-a",
                ProducerId(7),
                i16::MAX,
                vec![requested.clone()],
                TxnVersion::Classic,
                3,
            )
            .await;
        check!(first == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 1);

        // Kafka's optimization: every requested partition is already in this
        // Ongoing transaction's set, so the retry answers NONE without an
        // append (#848). The log end offset must not move.
        let retry = coordinator
            .register_partitions(
                "tid-a",
                ProducerId(7),
                i16::MAX,
                vec![requested.clone()],
                TxnVersion::Classic,
                3,
            )
            .await;
        check!(retry == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 1);

        // A new partition in the same request still writes.
        let grown = coordinator
            .register_partitions(
                "tid-a",
                ProducerId(7),
                i16::MAX,
                vec![requested.clone(), partition("orders", 0)],
                TxnVersion::Classic,
                3,
            )
            .await;
        check!(grown == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 2);

        let stored = coordinator.get("tid-a").expect("registered entry");
        let stored = stored.lock().await;
        assert!(stored.partitions.contains(&requested));
        assert!(stored.partitions.contains(&partition("orders", 0)));
        drop(stored);

        let stale = coordinator
            .register_partitions(
                "tid-a",
                ProducerId(7),
                i16::MAX - 1,
                vec![partition("payments", 0)],
                TxnVersion::Classic,
                3,
            )
            .await;
        check!(stale == crate::codes::PRODUCER_FENCED);
        check!(transaction_partition.log_end_offset().0 == 2);
    }

    /// The no-write retry optimization requires the *current* state to be
    /// exactly Ongoing (Kafka: `txnMetadata.state == ONGOING`). A stale exact
    /// match left over from a completed transaction's partition set must
    /// still transition and persist, not vacuously answer NONE (#848).
    #[tokio::test]
    async fn stale_partition_match_on_a_completed_entry_still_persists() {
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = test_coordinator();
        let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), 0, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        let requested = partition("orders", 0);
        entry.partitions.insert(requested.clone());
        install_entry(&coordinator, entry).await;
        let transaction_partition =
            open_transaction_partition(&coordinator, directory.path(), "tid-a");

        let code = coordinator
            .register_partitions(
                "tid-a",
                ProducerId(7),
                0,
                vec![requested.clone()],
                TxnVersion::Classic,
                3,
            )
            .await;
        check!(code == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 1);
        let stored = coordinator.get("tid-a").expect("registered entry");
        let stored = stored.lock().await;
        check!(stored.state == TxnState::Ongoing);
        assert!(stored.partitions.contains(&requested));
    }

    #[tokio::test]
    async fn failed_registration_retry_never_reports_success_or_leaves_dirty_state() {
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
                        3,
                    )
                    .await
                    == crate::codes::UNKNOWN_SERVER_ERROR
            );
        }
        // No partition, on the state-partition being unopened for this test,
        // ever becomes durable, so the live entry must stay exactly as it
        // was: no partition added, and no Ongoing transition either. Leaving
        // it mutated in place would let a second failing attempt take the
        // no-write PersistRetry path and wrongly answer NONE.
        let stored = coordinator.get("tid-a").expect("entry remains present");
        let stored = stored.lock().await;
        assert!(!stored.partitions.contains(&requested));
        check!(stored.state == TxnState::Empty);
    }
}
