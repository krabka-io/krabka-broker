use {
    super::*,
    crate::{
        kraft::controller::{applied_image::AppliedImage, test_support::topic_record},
        types::NodeId,
    },
    assert2::assert,
    krabka_metadata::{
        BreakGlassAction, BrokerRegistrationRecord, PartitionOffsetAdvanceRecord, PatternType,
    },
    krabka_security::KafkaPrincipal,
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
        .delegation_token_mutations(std::slice::from_ref(&mutation), 200)
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
        features: std::collections::BTreeMap::new(),
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
        &std::iter::empty(),
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
