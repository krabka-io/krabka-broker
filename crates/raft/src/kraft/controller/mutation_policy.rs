//! Synchronous metadata admission and normalization against the engine's current image.
//! The engine supplies log facts and owns the append that follows these decisions.

use {
    super::{applied_image::Overlay, offsets::assigned_record_offset},
    crate::{
        DelegationTokenMutation, OffsetReservation, SubmitChangeResult, error::RaftError,
        kraft::types::Epoch,
    },
    krabka_ids::Offset,
    krabka_metadata::{
        BreakGlassProposalRecord, DelegationToken, DelegationTokenRecord,
        DeleteDelegationTokenRecord, MetadataImage, MetadataRecord, PartitionRecord,
        TopicFreezeRecord, from_kraft_value, to_kraft_values,
    },
};

pub(super) struct MutationAdmission<'a> {
    pub(super) image: &'a MetadataImage,
    pub(super) uncommitted_tail: bool,
}

impl MutationAdmission<'_> {
    fn token_generation_matches(
        expected: &DelegationTokenRecord,
        replacement: &DelegationTokenRecord,
    ) -> bool {
        expected.token_id == replacement.token_id
            && expected.owner == replacement.owner
            && expected.requester == replacement.requester
            && expected.issue_timestamp_ms == replacement.issue_timestamp_ms
            && expected.max_timestamp_ms == replacement.max_timestamp_ms
            && expected.renewers == replacement.renewers
    }

    fn token_mutation_decision(
        image: &MetadataImage,
        mutation: &DelegationTokenMutation,
        now_ms: i64,
        uncommitted_tail: bool,
    ) -> krabka_verified::TokenMutationDecision {
        let (kind, expected, replacement) = match mutation {
            DelegationTokenMutation::Renew {
                expected,
                replacement,
            } => (
                krabka_verified::TokenMutationKind::Renew,
                expected,
                Some(replacement),
            ),
            DelegationTokenMutation::Expire {
                expected,
                replacement,
            } => (
                krabka_verified::TokenMutationKind::Expire,
                expected,
                Some(replacement),
            ),
            DelegationTokenMutation::Delete { expected } => {
                (krabka_verified::TokenMutationKind::Delete, expected, None)
            }
        };
        let stored = image
            .delegation_token_by_id(&expected.token_id)
            .map(DelegationToken::to_record);
        let generation_matches = replacement
            .is_none_or(|replacement| Self::token_generation_matches(expected, replacement));
        let state = match (&stored, replacement, generation_matches) {
            (None, _, true) => krabka_verified::TokenMutationState::Missing,
            (Some(stored), Some(replacement), true) if stored == replacement => {
                krabka_verified::TokenMutationState::Applied
            }
            (Some(stored), _, true) if stored == expected => {
                krabka_verified::TokenMutationState::Expected
            }
            (_, _, false) | (Some(_), _, true) => krabka_verified::TokenMutationState::Stale,
        };
        krabka_verified::token_mutation_decision(krabka_verified::TokenMutationFacts {
            kind,
            state,
            now_ms,
            expected_expiry_ms: expected.expiry_timestamp_ms,
            incoming_expiry_ms: replacement.map_or(expected.expiry_timestamp_ms, |record| {
                record.expiry_timestamp_ms
            }),
            max_timestamp_ms: expected.max_timestamp_ms,
            uncommitted_tail,
        })
    }

    fn token_mutation_record(mutation: &DelegationTokenMutation) -> MetadataRecord {
        match mutation {
            DelegationTokenMutation::Renew { replacement, .. }
            | DelegationTokenMutation::Expire { replacement, .. } => {
                MetadataRecord::V1DelegationToken(replacement.clone())
            }
            DelegationTokenMutation::Delete { expected } => {
                MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
                    token_id: expected.token_id.clone(),
                })
            }
        }
    }

    fn token_mutation_id(mutation: &DelegationTokenMutation) -> &str {
        match mutation {
            DelegationTokenMutation::Renew { expected, .. }
            | DelegationTokenMutation::Expire { expected, .. }
            | DelegationTokenMutation::Delete { expected } => &expected.token_id,
        }
    }

    pub(super) fn delegation_token_mutations(
        &self,
        mutations: &[DelegationTokenMutation],
        now_ms: i64,
    ) -> Result<Vec<MetadataRecord>, RaftError> {
        let mut scratch = MetadataImage::clone(self.image);
        let mut records = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            match Self::token_mutation_decision(&scratch, mutation, now_ms, self.uncommitted_tail) {
                krabka_verified::TokenMutationDecision::Append => {
                    let record = Self::token_mutation_record(mutation);
                    scratch.apply(&record);
                    records.push(record);
                }
                krabka_verified::TokenMutationDecision::Retry => {}
                krabka_verified::TokenMutationDecision::Reject => {
                    return Err(RaftError::ChangeRejected(format!(
                        "delegation-token mutation {} rejected",
                        Self::token_mutation_id(mutation)
                    )));
                }
            }
        }
        Ok(records)
    }

    fn consumption_matches_stored(
        stored: &BreakGlassProposalRecord,
        consumed: &BreakGlassProposalRecord,
    ) -> bool {
        let mut expected = stored.clone();
        expected.consumed_at_ms = consumed.consumed_at_ms;
        expected == *consumed
    }

    fn break_glass_consumption_decision(
        &self,
        consumed: &BreakGlassProposalRecord,
    ) -> krabka_verified::BreakGlassConsumptionDecision {
        let stored = self.image.break_glass_proposal(consumed.proposal_id);
        let proposal = match stored {
            None => krabka_verified::BreakGlassProposalState::Missing,
            Some(stored)
                if Self::consumption_matches_stored(stored, consumed)
                    && stored.consumed_at_ms == 0
                    && !stored.withdrawn =>
            {
                krabka_verified::BreakGlassProposalState::ExactPending
            }
            Some(_) => krabka_verified::BreakGlassProposalState::Stale,
        };
        krabka_verified::break_glass_consumption_decision(
            krabka_verified::BreakGlassConsumptionFacts {
                proposal,
                consumed_at_ms: consumed.consumed_at_ms,
                // A consume is a security-sensitive compare-and-set. Require
                // the committed image to cover the whole log prefix before
                // appending it. This conservative fence survives leadership
                // loss, where a waiter can disappear while its uncommitted
                // log entry remains and may later commit.
                uncommitted_tail: self.uncommitted_tail,
            },
        )
    }

    fn freeze_replacement_decision(
        &self,
        incoming: &TopicFreezeRecord,
        another_freeze_in_batch: bool,
    ) -> krabka_verified::FreezeReplacementDecision {
        let stored = self
            .image
            .topic_freezes()
            .find(|stored| {
                stored.pattern_type == incoming.pattern_type && stored.scope == incoming.scope
            })
            .map_or(krabka_verified::FreezeStoredState::Missing, |stored| {
                krabka_verified::FreezeStoredState::Present {
                    set_at_ms: stored.set_at_ms,
                }
            });
        krabka_verified::freeze_replacement_decision(krabka_verified::FreezeReplacementFacts {
            stored,
            incoming_frozen: incoming.frozen,
            incoming_set_at_ms: incoming.set_at_ms,
            // Like proposal consumption, replacement is a compare-and-set
            // against the committed image. A retained tail or a second
            // freeze in this batch must commit or fail before retry.
            uncommitted_tail: another_freeze_in_batch || self.uncommitted_tail,
        })
    }

    /// Refuse a batch whose compare-and-set records the committed image does
    /// not admit.
    ///
    /// A break-glass consume, a topic-freeze replacement, and an unguarded
    /// delegation-token write are checked against the committed image before
    /// append.
    ///
    /// # Errors
    ///
    /// Returns [`RaftError::UncommittedTail`] when only the uncommitted log
    /// tail stops a consume or a freeze replacement, and
    /// [`RaftError::ChangeRejected`] for every other refusal.
    pub(super) fn check_compare_and_set_records(
        &self,
        records: &[MetadataRecord],
        delegation_token_guarded: bool,
        registration_change_must_wait: impl Fn(krabka_metadata::NodeId) -> bool,
    ) -> Result<(), RaftError> {
        let mut freeze_in_batch = false;
        let mut token_create_in_batch = std::collections::HashSet::new();
        for record in records {
            match record {
                MetadataRecord::V1BreakGlassProposal(consumed) if consumed.consumed_at_ms != 0 => {
                    match self.break_glass_consumption_decision(consumed) {
                        krabka_verified::BreakGlassConsumptionDecision::Append => {}
                        // Only an uncommitted tail gives `InFlight`. It clears
                        // when the tail commits, so the caller can retry.
                        krabka_verified::BreakGlassConsumptionDecision::InFlight => {
                            return Err(RaftError::UncommittedTail);
                        }
                        decision => {
                            return Err(RaftError::ChangeRejected(format!(
                                "break-glass consume {} rejected: {decision:?}",
                                consumed.proposal_id
                            )));
                        }
                    }
                }
                MetadataRecord::V1TopicFreeze(freeze) => {
                    match self.freeze_replacement_decision(freeze, freeze_in_batch) {
                        krabka_verified::FreezeReplacementDecision::Append => {}
                        // A second freeze in the same batch also gives
                        // `InFlight`. That batch fails again on every retry,
                        // so only the uncommitted tail is a transient refusal.
                        krabka_verified::FreezeReplacementDecision::InFlight
                            if !freeze_in_batch =>
                        {
                            return Err(RaftError::UncommittedTail);
                        }
                        decision => {
                            return Err(RaftError::ChangeRejected(format!(
                                "topic-freeze mutation {:?}:{} rejected: {decision:?}",
                                freeze.pattern_type, freeze.scope
                            )));
                        }
                    }
                    freeze_in_batch = true;
                }
                MetadataRecord::V1DelegationToken(token) if !delegation_token_guarded => {
                    let create_is_unique =
                        self.image.delegation_token_by_id(&token.token_id).is_none()
                            && token_create_in_batch.insert(token.token_id.clone());
                    if !create_is_unique || self.uncommitted_tail {
                        return Err(RaftError::ChangeRejected(format!(
                            "delegation-token create {} rejected: replacement requires a guarded mutation",
                            token.token_id
                        )));
                    }
                }
                MetadataRecord::V1BrokerRegistration(amend)
                    if amend.broker_epoch >= 0 && registration_change_must_wait(amend.node_id) =>
                {
                    return Err(RaftError::UncommittedTail);
                }
                MetadataRecord::V1BrokerRegistrationChange(change)
                    if registration_change_must_wait(change.node_id) =>
                {
                    return Err(RaftError::UncommittedTail);
                }
                MetadataRecord::V1DeleteDelegationToken(token) if !delegation_token_guarded => {
                    return Err(RaftError::ChangeRejected(format!(
                        "delegation-token delete {} rejected: mutation is not generation-bound",
                        token.token_id
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub(super) struct PreparedRecords {
    pub(super) value_blobs: Vec<bytes::Bytes>,
    pub(super) result: SubmitChangeResult,
    pub(super) registration_nodes: Vec<krabka_metadata::NodeId>,
}

// Both overlays evolve record by record in one engine turn. Stamping uses the
// exact log end that the engine will append at, including prior fan-out blobs.
pub(super) fn prepare_records<'a>(
    records: &[MetadataRecord],
    mut scratch: Overlay<'_>,
    mut replica_view: Overlay<'_>,
    assign_base: Offset,
    leader_epoch: Epoch,
    pending_reservations: impl Iterator<Item = &'a OffsetReservation> + Clone,
) -> Result<PreparedRecords, RaftError> {
    let mut result = SubmitChangeResult::default();
    let mut value_blobs: Vec<bytes::Bytes> = Vec::new();
    // The brokers whose registration this batch writes.
    let mut registration_nodes = Vec::new();
    for (index, r) in records.iter().enumerate() {
        let more_records = index + 1 < records.len();
        let rebased = rebase_partition_directories(&scratch, r);
        let r = rebased.as_ref().unwrap_or(r);
        // A new registration carries no epoch (-1) and is stamped with
        // its committed offset. An amend of the registration (a
        // `RegisterBrokerRecord` at the epoch it already holds) and a
        // `BrokerRegistrationChangeRecord` apply only while the broker is
        // registered at the epoch they name, as Kafka's
        // `ClusterControlManager.replayRegistrationChange` requires. One
        // built from a registration that has since been replaced is
        // dropped rather than failing its batch: it must neither
        // overwrite the new registration nor register again, and the
        // partition changes it travels with still hold.
        let stamped;
        let r: &MetadataRecord = match r {
            MetadataRecord::V1BrokerRegistration(b) if b.broker_epoch < 0 => {
                registration_nodes.push(b.node_id);
                let delta = i64::try_from(value_blobs.len()).unwrap_or(i64::MAX);
                let mut b = b.clone();
                b.broker_epoch = assigned_record_offset(assign_base, delta);
                stamped = MetadataRecord::V1BrokerRegistration(b);
                &stamped
            }
            MetadataRecord::V1BrokerRegistration(b) => {
                if !registered_at(&scratch, b.node_id, b.broker_epoch, Some(b.incarnation_id)) {
                    continue;
                }
                registration_nodes.push(b.node_id);
                r
            }
            MetadataRecord::V1UnregisterBroker(unregister) => {
                registration_nodes.push(unregister.node_id);
                r
            }
            MetadataRecord::V1BrokerRegistrationChange(change) => {
                if !registered_at(&scratch, change.node_id, change.broker_epoch, None) {
                    continue;
                }
                registration_nodes.push(change.node_id);
                r
            }
            other => other,
        };
        scratch.validate(r).map_err(RaftError::Metadata)?;
        if let MetadataRecord::V1PartitionOffsetAdvance(r) = r {
            let mut next_offset = scratch
                .partition_next_offset(&r.topic, r.partition)
                .unwrap_or(0);
            // A multi-voter leader may have earlier reservations appended
            // but not committed into `scratch` yet. Fold their exact
            // contiguous ends so concurrent submissions cannot reuse the
            // same committed base.
            for pending in pending_reservations
                .clone()
                .filter(|pending| pending.topic == r.topic && pending.partition == r.partition)
            {
                let Some(frontier) = krabka_verified::wal_reservation_frontier(
                    next_offset,
                    pending.base_offset,
                    pending.count,
                ) else {
                    return Err(RaftError::ChangeRejected(
                        "pending offset reservation chain is invalid".to_string(),
                    ));
                };
                next_offset = frontier;
            }
            // The proved reservation admits exactly a nonnegative frontier,
            // a positive count, and an end that fits i64; untrusted
            // metadata outside that is rejected.
            let Some((base_offset, _next_offset)) =
                krabka_verified::reserve_offsets(next_offset, r.count)
            else {
                return Err(RaftError::ChangeRejected(format!(
                    "partition offset advance count {} is out of range at next offset {next_offset}",
                    r.count
                )));
            };
            result.offset_reservations.push(OffsetReservation {
                topic: r.topic.clone(),
                partition: r.partition,
                base_offset,
                count: r.count,
                leader_epoch: u64::from(leader_epoch),
            });
        }
        match to_kraft_values(r, &scratch) {
            Ok(mut blobs) => {
                replay_value_blobs(&blobs, &mut replica_view, more_records)?;
                value_blobs.append(&mut blobs);
            }
            Err(e) => {
                return Err(RaftError::ChangeRejected(format!("encode: {e}")));
            }
        }
        if more_records {
            scratch.apply(r);
        }
    }

    Ok(PreparedRecords {
        value_blobs,
        result,
        registration_nodes,
    })
}

/// KIP-858: while a partition keeps its replica list, only
/// `AssignReplicasToDirs` moves its `directories`.
///
/// A caller that writes a whole partition record (an election, an ISR change)
/// copies `directories` from the image it read. That image can predate a
/// directory assignment that has since committed. Written as it is, the record
/// would revert the assignment, and a stale empty list would encode a
/// `PartitionChangeRecord` whose directory count does not match its replicas,
/// which no replica can apply. So a record that keeps the replica list takes
/// the directories of the image it is encoded against. Kafka's controller
/// behaves the same way. `PartitionChangeBuilder` never takes directories from
/// the caller of an election or an ISR change: it derives them from the
/// partition's current directories, keyed by replica, and only the
/// `AssignReplicasToDirs` handler calls `setDirectory`.
///
/// Returns `None` when the record needs no change.
fn rebase_partition_directories(
    image: &MetadataImage,
    record: &MetadataRecord,
) -> Option<MetadataRecord> {
    let rebase = |partition: &PartitionRecord| -> Option<PartitionRecord> {
        let current = image.partition(&partition.topic, partition.partition)?;
        (current.replicas == partition.replicas && current.directories != partition.directories)
            .then(|| PartitionRecord {
                directories: current.directories.clone(),
                ..partition.clone()
            })
    };
    match record {
        MetadataRecord::V1Partition(partition) => {
            rebase(partition).map(MetadataRecord::V1Partition)
        }
        MetadataRecord::V1PartitionUpdate(update) => rebase(&update.partition).map(|partition| {
            MetadataRecord::V1PartitionUpdate(krabka_metadata::PartitionUpdateRecord {
                partition,
                ..update.clone()
            })
        }),
        _ => None,
    }
}

/// The names of the topics that `records` create: each `V1Topic` name that
/// `image` does not have. A `V1Topic` for a topic that `image` has is a
/// partition growth or a rejected re-create, so its name is not included.
/// Each name occurs one time only.
pub(super) fn created_topic_names(
    image: &MetadataImage,
    records: &[MetadataRecord],
) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for record in records {
        if let MetadataRecord::V1Topic(topic) = record
            && image.topic(&topic.name).is_none()
            && !names.contains(&topic.name)
        {
            names.push(topic.name.clone());
        }
    }
    names
}

/// Whether `node_id` is registered in `image` at `broker_epoch`, and, when
/// `incarnation_id` is given, as that incarnation: the condition under which
/// an amend or a `BrokerRegistrationChangeRecord` still applies. A change that
/// fails it is logged, and the caller drops it.
fn registered_at(
    image: &MetadataImage,
    node_id: krabka_metadata::NodeId,
    broker_epoch: i64,
    incarnation_id: Option<uuid::Uuid>,
) -> bool {
    let registered = image.broker(node_id).is_some_and(|existing| {
        existing.broker_epoch == broker_epoch
            && incarnation_id.is_none_or(|incarnation| existing.incarnation_id == incarnation)
    });
    if !registered {
        tracing::warn!(
            broker = node_id.0,
            broker_epoch,
            registered_epoch = ?image.broker_epoch(node_id),
            "dropping a broker registration change for an epoch the broker is no longer \
             registered at"
        );
    }
    registered
}

// Decode each blob against the view that preceding blobs produced.
fn replay_value_blobs(
    blobs: &[bytes::Bytes],
    image: &mut Overlay<'_>,
    more_blobs: bool,
) -> Result<(), RaftError> {
    for (index, blob) in blobs.iter().enumerate() {
        let decoded = from_kraft_value(blob, image).map_err(|e| {
            RaftError::ChangeRejected(format!("encoded record would not replay: {e}"))
        })?;
        // Nothing decodes against the image after the batch's last blob.
        if more_blobs || index + 1 < blobs.len() {
            image.apply(&decoded);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            test_support::{single_partition_topic, single_replica_partition},
            types::NodeId,
        },
        krabka_metadata::{DelegationTokenRecord, MetadataRecord, PartitionRecord},
        krabka_security::KafkaPrincipal,
        uuid::Uuid,
    };

    fn principal(name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".to_string(),
            name: name.to_string(),
        }
    }

    fn delegation_token(renewers: Vec<KafkaPrincipal>) -> DelegationTokenRecord {
        DelegationTokenRecord {
            token_id: "tok1".into(),
            owner: principal("alice"),
            requester: principal("alice"),
            issue_timestamp_ms: 100,
            max_timestamp_ms: 200,
            expiry_timestamp_ms: 150,
            renewers,
        }
    }

    #[test]
    fn token_generation_matches_checks_all_fields() {
        let t1 = delegation_token(vec![principal("bob")]);
        let mut t2 = t1.clone();
        assert2::check!(MutationAdmission::token_generation_matches(&t1, &t2));

        t2.token_id = "tok2".into();
        assert2::check!(!MutationAdmission::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.owner = principal("charlie");
        assert2::check!(!MutationAdmission::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.requester = principal("admin");
        assert2::check!(!MutationAdmission::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.issue_timestamp_ms = 101;
        assert2::check!(!MutationAdmission::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.max_timestamp_ms = 201;
        assert2::check!(!MutationAdmission::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.renewers = vec![principal("dan")];
        assert2::check!(!MutationAdmission::token_generation_matches(&t1, &t2));
    }

    #[test]
    fn token_mutation_id_extracts_token_id() {
        let rec = DelegationTokenRecord {
            token_id: "my-token".into(),
            owner: principal("alice"),
            requester: principal("alice"),
            issue_timestamp_ms: 0,
            max_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            renewers: vec![],
        };
        let m1 = DelegationTokenMutation::Renew {
            expected: rec.clone(),
            replacement: rec.clone(),
        };
        assert2::check!(MutationAdmission::token_mutation_id(&m1) == "my-token");

        let m2 = DelegationTokenMutation::Expire {
            expected: rec.clone(),
            replacement: rec.clone(),
        };
        assert2::check!(MutationAdmission::token_mutation_id(&m2) == "my-token");

        let m3 = DelegationTokenMutation::Delete { expected: rec };
        assert2::check!(MutationAdmission::token_mutation_id(&m3) == "my-token");
    }

    #[test]
    fn rebase_partition_directories_copies_image_directories() {
        let mut image = MetadataImage::default();
        let topic_id = Uuid::from_u128(42);
        image.apply(&MetadataRecord::V1Topic(single_partition_topic(
            "test-topic",
            topic_id,
        )));
        let dir_id = Uuid::from_u128(99);
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            directories: vec![dir_id],
            ..single_replica_partition("test-topic", krabka_ids::PartitionIndex(0), NodeId(1))
        }));

        let new_part = PartitionRecord {
            leader_epoch: krabka_metadata::LeaderEpoch(1),
            partition_epoch: 1,
            ..single_replica_partition("test-topic", krabka_ids::PartitionIndex(0), NodeId(1))
        };

        let rebased = rebase_partition_directories(&image, &MetadataRecord::V1Partition(new_part));
        if let Some(MetadataRecord::V1Partition(p)) = rebased {
            assert2::check!(p.directories == vec![dir_id]);
        } else {
            panic!("expected V1Partition");
        }
    }

    #[test]
    fn created_topic_names_lists_only_names_absent_from_the_image() {
        struct Scenario {
            case: &'static str,
            records: Vec<MetadataRecord>,
            expected: Vec<&'static str>,
        }

        let topic = |name: &str, id: u128| {
            MetadataRecord::V1Topic(single_partition_topic(name, Uuid::from_u128(id)))
        };
        let mut image = MetadataImage::default();
        image.apply(&topic("existing", 1));

        let cases = [
            Scenario {
                case: "new topic",
                records: vec![topic("new", 2)],
                expected: vec!["new"],
            },
            Scenario {
                case: "existing topic",
                records: vec![topic("existing", 1)],
                expected: vec![],
            },
            Scenario {
                case: "name repeated in one batch",
                records: vec![topic("new", 2), topic("new", 3), topic("other", 4)],
                expected: vec!["new", "other"],
            },
            Scenario {
                case: "no topic records",
                records: vec![MetadataRecord::V1DeleteTopic(
                    krabka_metadata::DeleteTopicRecord {
                        name: "existing".into(),
                    },
                )],
                expected: vec![],
            },
        ];
        for Scenario {
            case,
            records,
            expected,
        } in cases
        {
            assert2::check!(
                created_topic_names(&image, &records) == expected,
                "case: {case}"
            );
        }
    }

    #[test]
    fn unguarded_delegation_token_records_validation() {
        let mut image = MetadataImage::default();
        let t1 = delegation_token(vec![]);
        let tok = MetadataRecord::V1DelegationToken(t1);

        // Unguarded delete is always rejected
        let del = MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
            token_id: "tok1".into(),
        });
        let res = MutationAdmission {
            image: &image,
            uncommitted_tail: false,
        }
        .check_compare_and_set_records(&[del], false, |_| false);
        assert2::assert!(matches!(res, Err(RaftError::ChangeRejected(_))));

        // Duplicate token create in the same batch is rejected
        let res_dup = MutationAdmission {
            image: &image,
            uncommitted_tail: false,
        }
        .check_compare_and_set_records(&[tok.clone(), tok.clone()], false, |_| false);
        assert2::assert!(matches!(res_dup, Err(RaftError::ChangeRejected(_))));

        // Token that already exists in image is rejected
        image.apply(&tok);
        let res_exists = MutationAdmission {
            image: &image,
            uncommitted_tail: false,
        }
        .check_compare_and_set_records(std::slice::from_ref(&tok), false, |_| false);
        assert2::assert!(matches!(res_exists, Err(RaftError::ChangeRejected(_))));

        // Token create when hwm < log_end_offset (uncommitted tail) is rejected
        let image = MetadataImage::default();
        let t2 = DelegationTokenRecord {
            token_id: "tok2".into(),
            owner: principal("bob"),
            requester: principal("bob"),
            issue_timestamp_ms: 0,
            max_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            renewers: vec![],
        };
        let tok2 = MetadataRecord::V1DelegationToken(t2);
        let res_tail = MutationAdmission {
            image: &image,
            uncommitted_tail: true,
        }
        .check_compare_and_set_records(std::slice::from_ref(&tok2), false, |_| false);
        assert2::assert!(matches!(res_tail, Err(RaftError::ChangeRejected(_))));

        // When delegation_token_guarded is true, create is admitted
        let res_guarded = MutationAdmission {
            image: &image,
            uncommitted_tail: true,
        }
        .check_compare_and_set_records(&[tok2], true, |_| false);
        assert2::assert!(res_guarded.is_ok());
    }
}

#[cfg(test)]
#[path = "mutation_policy_tests.rs"]
mod behavior_tests;
