use {
    super::*,
    crate::{
        kraft::controller::{applied_image::AppliedImage, test_support::topic_record},
        types::NodeId,
    },
    assert2::assert,
    krabka_metadata::{
        BreakGlassAction, BrokerRegistrationRecord, LeaderRecoveryState,
        PartitionOffsetAdvanceRecord, PartitionUpdateRecord, PatternType,
    },
    krabka_security::KafkaPrincipal,
    std::{collections::BTreeMap, iter, slice},
    uuid::Uuid,
};

fn token(expiry_timestamp_ms: i64) -> DelegationTokenRecord {
    let owner = KafkaPrincipal {
        principal_type: "User".into(),
        name: "alice".into(),
    };
    DelegationTokenRecord {
        token_id: "token".into(),
        owner: owner.clone(),
        requester: owner,
        issue_timestamp_ms: 100,
        max_timestamp_ms: 1_000,
        expiry_timestamp_ms,
        renewers: vec![],
    }
}

#[test]
fn token_batch_uses_prior_mutations_and_skips_exact_retries() {
    let mut image = MetadataImage::default();
    image.apply(&MetadataRecord::V1DelegationToken(token(500)));
    let renew = DelegationTokenMutation::Renew {
        expected: token(500),
        replacement: token(600),
    };
    let again = DelegationTokenMutation::Renew {
        expected: token(600),
        replacement: token(700),
    };
    let admitted = MutationAdmission {
        image: &image,
        uncommitted_tail: false,
    }
    .delegation_token_mutations(&[renew.clone(), renew, again], 200)
    .unwrap();
    assert!(
        admitted
            == vec![
                MetadataRecord::V1DelegationToken(token(600)),
                MetadataRecord::V1DelegationToken(token(700)),
            ]
    );
    assert!(image.delegation_token_by_id("token").unwrap().to_record() == token(500));
}

#[test]
fn token_tail_refuses_even_an_applied_retry_or_missing_delete() {
    let mut image = MetadataImage::default();
    image.apply(&MetadataRecord::V1DelegationToken(token(600)));
    let retry = DelegationTokenMutation::Renew {
        expected: token(500),
        replacement: token(600),
    };
    let missing = DelegationTokenMutation::Delete {
        expected: DelegationTokenRecord {
            token_id: "missing".into(),
            ..token(500)
        },
    };
    for mutation in [retry, missing] {
        let idle = MutationAdmission {
            image: &image,
            uncommitted_tail: false,
        }
        .delegation_token_mutations(slice::from_ref(&mutation), 200)
        .unwrap();
        assert!(idle.is_empty());
        let busy = MutationAdmission {
            image: &image,
            uncommitted_tail: true,
        }
        .delegation_token_mutations(&[mutation], 200);
        assert!(matches!(busy, Err(RaftError::ChangeRejected(_))));
    }
}

fn freeze(scope: &str, set_at_ms: i64) -> TopicFreezeRecord {
    TopicFreezeRecord {
        scope: scope.into(),
        pattern_type: PatternType::Literal,
        frozen: true,
        reason: "incident".into(),
        set_by: "User:alice".into(),
        set_at_ms,
        proposal_id: Uuid::nil(),
        key_id: String::new(),
        signature: vec![],
    }
}

#[test]
fn freeze_staleness_precedes_tail_and_second_freeze_is_permanent_refusal() {
    let mut image = MetadataImage::default();
    image.apply(&MetadataRecord::V1TopicFreeze(freeze("orders", 10)));
    let policy = MutationAdmission {
        image: &image,
        uncommitted_tail: true,
    };
    let stale = policy.check_compare_and_set_records(
        &[MetadataRecord::V1TopicFreeze(freeze("orders", 10))],
        false,
        |_| false,
    );
    assert!(matches!(stale, Err(RaftError::ChangeRejected(_))));
    let newer = policy.check_compare_and_set_records(
        &[MetadataRecord::V1TopicFreeze(freeze("orders", 11))],
        false,
        |_| false,
    );
    assert!(matches!(newer, Err(RaftError::UncommittedTail)));
    let duplicate = MutationAdmission {
        image: &image,
        uncommitted_tail: false,
    }
    .check_compare_and_set_records(
        &[
            MetadataRecord::V1TopicFreeze(freeze("a", 11)),
            MetadataRecord::V1TopicFreeze(freeze("b", 11)),
        ],
        false,
        |_| false,
    );
    assert!(matches!(duplicate, Err(RaftError::ChangeRejected(_))));
}

#[test]
fn break_glass_consume_requires_exact_pending_proposal_before_tail_check() {
    let mut image = MetadataImage::default();
    let proposal = BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(1),
        action: BreakGlassAction::DeleteTopic,
        target: "orders".into(),
        proposer: "User:alice".into(),
        reason: "incident".into(),
        created_at_ms: 1,
        expires_at_ms: 1_000,
        approvals: vec![],
        consumed_at_ms: 0,
        withdrawn: false,
    };
    image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    let consumed = BreakGlassProposalRecord {
        consumed_at_ms: 10,
        ..proposal
    };
    for uncommitted_tail in [false, true] {
        let policy = MutationAdmission {
            image: &image,
            uncommitted_tail,
        };
        let result = policy.check_compare_and_set_records(
            &[MetadataRecord::V1BreakGlassProposal(consumed.clone())],
            false,
            |_| false,
        );
        if uncommitted_tail {
            assert!(matches!(result, Err(RaftError::UncommittedTail)));
        } else {
            assert!(result.is_ok());
        }
        let stale = BreakGlassProposalRecord {
            target: "other".into(),
            ..consumed.clone()
        };
        let result = policy.check_compare_and_set_records(
            &[MetadataRecord::V1BreakGlassProposal(stale)],
            false,
            |_| false,
        );
        assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
    }
    image.apply(&MetadataRecord::V1BreakGlassProposal(consumed.clone()));
    let result = MutationAdmission {
        image: &image,
        uncommitted_tail: false,
    }
    .check_compare_and_set_records(
        &[MetadataRecord::V1BreakGlassProposal(consumed)],
        false,
        |_| false,
    );
    assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
}

fn registration(id: u64) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: true,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: NodeId(id),
        broker_epoch: -1,
        incarnation_id: Uuid::from_u128(u128::from(id)),
        host: "broker".into(),
        port: 9092,
        rack: None,
        endpoints: vec![],
        log_dirs: vec![],
        features: BTreeMap::new(),
    })
}

#[test]
fn preparation_stamps_each_registration_at_its_replay_offset() {
    let (image, _rx) = AppliedImage::new(MetadataImage::default());
    let prepared = prepare_records(
        &[registration(7), registration(8)],
        image.overlay(),
        image.overlay(),
        Offset(42),
        3,
        &iter::empty(),
    )
    .unwrap();
    assert!(prepared.registration_nodes == vec![NodeId(7), NodeId(8)]);
    assert!(prepared.value_blobs.len() == 2);
    let mut replica = MetadataImage::default();
    for blob in prepared.value_blobs {
        let decoded = from_kraft_value(&blob, &replica).unwrap();
        replica.apply(&decoded);
    }
    assert!(replica.broker_epoch(NodeId(7)) == Some(42));
    assert!(replica.broker_epoch(NodeId(8)) == Some(43));
    assert!(image.broker(NodeId(7)).is_none());
}

#[test]
fn preparation_reserves_after_pending_contiguous_ends_and_refuses_gaps() {
    let mut metadata = MetadataImage::default();
    for record in topic_record("orders") {
        metadata.apply(&record);
    }
    let (image, _rx) = AppliedImage::new(metadata);
    let records = [MetadataRecord::V1PartitionOffsetAdvance(
        PartitionOffsetAdvanceRecord {
            topic: "orders".into(),
            partition: 0,
            count: 5,
        },
    )];
    let mut pending = [OffsetReservation {
        topic: "orders".into(),
        partition: 0,
        base_offset: 0,
        count: 3,
        leader_epoch: 3,
    }];
    let prepared = prepare_records(
        &records,
        image.overlay(),
        image.overlay(),
        Offset(42),
        3,
        &pending.iter(),
    )
    .unwrap();
    assert!(
        prepared.result.offset_reservations
            == vec![OffsetReservation {
                topic: "orders".into(),
                partition: 0,
                base_offset: 3,
                count: 5,
                leader_epoch: 3,
            }]
    );
    pending[0].base_offset = 1;
    let result = prepare_records(
        &records,
        image.overlay(),
        image.overlay(),
        Offset(42),
        3,
        &pending.iter(),
    );
    assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
}

/// The image every replica holds after applying the encoded `blobs` to `base`,
/// as `advance_and_apply` decodes and applies them.
fn replayed(base: &MetadataImage, blobs: &[bytes::Bytes]) -> MetadataImage {
    let mut replica = base.clone();
    for blob in blobs {
        let decoded = from_kraft_value(blob, &replica).unwrap();
        replica.apply(&decoded);
    }
    replica
}

/// A `PartitionChangeRecord` bumps the leader epoch on replay only when it names
/// a leader, so an update that keeps the leader and raises the epoch must not
/// travel as one: every replica would apply it at the old epoch, and a broker
/// that fetches at the epoch the controller meant would be fenced forever.
/// Whatever form the record takes on the log, each replica ends at the leader,
/// leader epoch, partition epoch, ISR, ELR and recovery state the update named.
#[test]
fn a_same_leader_epoch_bump_replays_at_the_new_epoch() {
    let mut metadata = MetadataImage::default();
    for record in topic_record("orders") {
        metadata.apply(&record);
    }
    let current = metadata.partition("orders", 0).unwrap().clone();
    let bumped = PartitionRecord {
        leader_epoch: krabka_metadata::LeaderEpoch(current.leader_epoch.0 + 1),
        partition_epoch: current.partition_epoch + 1,
        ..current
    };
    let cases = [
        ("leader epoch only", None, None),
        (
            "with the recovery and ELR state of an unclean election",
            Some(LeaderRecoveryState::Recovering),
            Some((Vec::new(), Vec::new())),
        ),
        (
            "with a published ELR",
            None,
            Some((vec![NodeId(1)], vec![NodeId(1)])),
        ),
    ];
    for (case, recovery_state, elr) in cases {
        let (image, _rx) = AppliedImage::new(metadata.clone());
        let update = MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
            partition: bumped.clone(),
            eligible_leader_replicas: elr.as_ref().map(|(eligible, _)| eligible.clone()),
            last_known_elr: elr.as_ref().map(|(_, last_known)| last_known.clone()),
            recovery_state,
        });
        let prepared = prepare_records(
            slice::from_ref(&update),
            image.overlay(),
            image.overlay(),
            Offset(42),
            3,
            &iter::empty(),
        )
        .unwrap();
        let mut intended = metadata.clone();
        intended.apply(&update);
        let replica = replayed(&metadata, &prepared.value_blobs);
        let partition = replica.partition("orders", 0).unwrap();
        assert!(
            (
                partition.leader,
                partition.leader_epoch,
                partition.partition_epoch,
                &partition.isr,
                replica.partition_elr("orders", 0),
                replica.leader_recovery_state("orders", 0),
            ) == (
                bumped.leader,
                bumped.leader_epoch,
                bumped.partition_epoch,
                &bumped.isr,
                intended.partition_elr("orders", 0),
                intended.leader_recovery_state("orders", 0),
            ),
            "{case}"
        );
    }
}
