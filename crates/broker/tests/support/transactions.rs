//! Transaction requests with explicit producer identity and deadlines.

use krabka_protocol::owned::{
    end_txn_request::EndTxnRequest,
    init_producer_id_request::InitProducerIdRequest,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};

pub fn init_producer_request(
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    (producer_id, producer_epoch): (i64, i16),
) -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id,
        transaction_timeout_ms,
        producer_id,
        producer_epoch,
        ..Default::default()
    }
}

pub fn end_transaction_request(
    transactional_id: impl Into<String>,
    (producer_id, producer_epoch): (i64, i16),
    committed: bool,
) -> EndTxnRequest {
    EndTxnRequest {
        transactional_id: transactional_id.into(),
        producer_id,
        producer_epoch,
        committed,
        ..Default::default()
    }
}

pub fn txn_offset_partition(
    partition_index: i32,
    committed_offset: i64,
) -> TxnOffsetCommitRequestPartition {
    TxnOffsetCommitRequestPartition {
        partition_index,
        committed_offset,
        ..Default::default()
    }
}

pub fn txn_offset_topic(
    name: impl Into<String>,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    partitions: Vec<TxnOffsetCommitRequestPartition>,
) -> TxnOffsetCommitRequestTopic {
    TxnOffsetCommitRequestTopic {
        name: name.into(),
        topic_id,
        partitions,
        ..Default::default()
    }
}

/// New producer identity with the protocol's exact default id and epoch fields.
pub fn new_producer_request(
    transactional_id: Option<String>,
    timeout_ms: i32,
) -> InitProducerIdRequest {
    init_producer_request(
        transactional_id,
        timeout_ms,
        (
            InitProducerIdRequest::default().producer_id,
            InitProducerIdRequest::default().producer_epoch,
        ),
    )
}

/// Return the complete idempotent allocation response without replacing caller assertions.
///
/// # Panics
/// Panics if the `InitProducerId` request fails.
pub async fn claim_idempotent_producer(
    client: &krabka_client_core::Client,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse {
    client
        .send(InitProducerIdRequest {
            transactional_id: None,
            ..Default::default()
        })
        .await
        .expect("InitProducerId")
}

/// A marker topic with the caller's exact ordered partition list.
pub fn marker_topic(
    name: String,
    partition_indexes: Vec<i32>,
) -> krabka_protocol::owned::write_txn_markers_request::WritableTxnMarkerTopic {
    krabka_protocol::owned::write_txn_markers_request::WritableTxnMarkerTopic {
        name,
        partition_indexes,
        ..Default::default()
    }
}

use krabka_protocol::owned::write_txn_markers_request::WritableTxnMarkerTopic;

/// Producer identity and fencing versions have distinct types at fixture boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct ProducerEpoch(pub i16);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct CoordinatorEpoch(pub i32);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct TransactionVersion(pub i8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub id: krabka_ids::ProducerId,
    pub epoch: ProducerEpoch,
}

impl Default for ProducerIdentity {
    fn default() -> Self {
        Self {
            id: krabka_ids::ProducerId(7),
            epoch: ProducerEpoch::default(),
        }
    }
}

impl ProducerIdentity {
    /// Wrap an identity returned by the Kafka codec.
    pub fn from_wire((id, epoch): (i64, i16)) -> Self {
        Self {
            id: krabka_ids::ProducerId(id),
            epoch: ProducerEpoch(epoch),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionOutcome {
    #[default]
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PartitionRegistration {
    #[default]
    Add,
    VerifyOnly,
}

/// Transaction marker identity, result and fencing fields.
#[derive(krabka_macros::FieldDefaults)]
pub struct TransactionMarkerSetup {
    pub producer: ProducerIdentity,
    pub outcome: TransactionOutcome,
    pub coordinator_epoch: CoordinatorEpoch,
    pub transaction_version: TransactionVersion,
    pub topics: Vec<WritableTxnMarkerTopic>,
}

pub fn transaction_marker(
    setup: TransactionMarkerSetup,
) -> krabka_protocol::owned::write_txn_markers_request::WritableTxnMarker {
    let TransactionMarkerSetup {
        producer,
        outcome,
        coordinator_epoch,
        transaction_version,
        topics,
    } = setup;
    krabka_protocol::owned::write_txn_markers_request::WritableTxnMarker {
        producer_id: producer.id.0,
        producer_epoch: producer.epoch.0,
        transaction_result: outcome == TransactionOutcome::Commit,
        coordinator_epoch: coordinator_epoch.0,
        transaction_version: transaction_version.0,
        topics,
        ..Default::default()
    }
}
