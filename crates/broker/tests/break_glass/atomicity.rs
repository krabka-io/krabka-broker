//! The consumed approval and the transition it authorizes commit in one raft
//! append, read back out of the committed metadata log.
//!
//! Nothing in the type system holds that rule: each gated handler prepends the
//! consumed record to its own change. The case therefore fetches the log off
//! the controller listener and asserts on the batch, which is the only place
//! a second append would show.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{BrokerHandle, NodeId, codes};
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_metadata::{
    BreakGlassProposalRecord, MetadataImage, MetadataRecord, UnregisterBrokerRecord,
};
use krabka_protocol::{primitives::uuid::Uuid as WireUuid, records::RecordBatch};

use crate::{
    cluster::boot,
    principals::ALICE,
    proposals::{ACTION_UNREGISTER_BROKER, approved_proposal},
    transitions::{BROKER_ID, unregister},
};

/// Read the committed metadata log back off the controller listener from
/// `from`, and answer the records of each batch, batch by batch.
///
/// `image` has to be the image that was current when the records were written:
/// a Kafka metadata record names a topic by id, and translating one back needs
/// an image that still knows the id.
async fn metadata_batches(
    broker: &BrokerHandle,
    from: i64,
    image: &MetadataImage,
) -> Vec<Vec<MetadataRecord>> {
    let connection = Connection::connect(
        broker.controller_addr(),
        ConnectionOptions {
            client_id: "break-glass-test".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("dial the controller listener");
    let mut body = Vec::new();
    krabka_raft::KrabkaMetadataFetchRequest {
        fetch_offset: from,
        max_bytes: 4 << 20,
    }
    .encode_v0(&mut body);
    let raw = connection
        .raw_request(krabka_raft::API_KEY_METADATA_FETCH, 0, Bytes::from(body))
        .await
        .expect("metadata fetch");
    connection.close();

    let mut cursor: &[u8] = &raw;
    let response = krabka_raft::KrabkaMetadataFetchResponse::decode_v0(&mut cursor)
        .expect("decode the metadata fetch response");
    assert!(response.error_code == 0, "the controller served the fetch");

    let mut bytes: &[u8] = &response.records;
    let mut batches = Vec::new();
    while !bytes.is_empty() {
        let batch = RecordBatch::decode(&mut bytes).expect("decode a metadata batch");
        if batch.attributes.is_control_batch() {
            continue;
        }
        batches.push(
            batch
                .records
                .iter()
                .filter_map(|record| record.value.as_ref())
                .filter_map(|value| krabka_metadata::from_kraft_value(value, image).ok())
                .collect(),
        );
    }
    batches
}

/// Whether `record` unregisters a broker.
fn is_unregister(record: &MetadataRecord) -> bool {
    matches!(record, MetadataRecord::V1UnregisterBroker(_))
}

/// Whether `record` is the spent form of proposal `id`.
fn is_consume_of(record: &MetadataRecord, id: WireUuid) -> bool {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => {
            proposal.proposal_id.as_bytes() == &id.0 && proposal.consumed_at_ms != 0
        }
        _ => false,
    }
}

/// The consumed approval and the transition it authorizes commit together, in
/// one raft append.
///
/// This is the reason a proposal lives in the metadata log at all rather than
/// in an internal topic or a separate service. Two appends would let a crash
/// between them either spend one approval twice or lose it. Nothing in the type
/// system enforces the rule — each gated handler prepends the consumed record
/// to its own — so a handler that called `submit_change` twice would keep every
/// other assertion in this file green and quietly break the guarantee. The case
/// reads the committed log back and asserts on the batch itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_consume_and_the_transition_land_in_one_raft_append() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;
    let id = approved_proposal(&cluster, ACTION_UNREGISTER_BROKER, &BROKER_ID.to_string()).await;

    // Snapshot the log position and the image before the transition runs.
    let image = cluster.broker.controller_image_for_test();
    let from = i64::try_from(
        cluster
            .broker
            .controller_quorum_state_for_test()
            .last_applied_index,
    )
    .expect("a log offset inside i64");
    let held = image
        .break_glass_proposal(uuid::Uuid::from_bytes(id.0))
        .expect("the approved proposal is in the image")
        .clone();
    check!(held.consumed_at_ms == 0);

    check!(unregister(&alice, BROKER_ID).await == codes::NONE);
    cluster
        .broker
        .wait_for_image(|img| img.broker(NodeId(1)).is_none())
        .await;

    let batches = metadata_batches(&cluster.broker, from, &image).await;
    let carrying: Vec<&Vec<MetadataRecord>> = batches
        .iter()
        .filter(|batch| batch.iter().any(is_unregister))
        .collect();
    assert!(
        carrying.len() == 1,
        "exactly one append carries the unregistration, found {}",
        carrying.len()
    );

    let consumed_at_ms = match carrying[0].first() {
        Some(MetadataRecord::V1BreakGlassProposal(proposal)) => proposal.consumed_at_ms,
        other => panic!("the consume leads the append; found {other:?}"),
    };
    check!(consumed_at_ms != 0);
    check!(
        carrying[0]
            == &vec![
                MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
                    consumed_at_ms,
                    ..held
                }),
                MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id: NodeId(1) }),
            ],
        "the append is the consume followed by the transition, and nothing else"
    );
    check!(
        batches
            .iter()
            .filter(|batch| batch.iter().any(|record| is_consume_of(record, id)))
            .count()
            == 1,
        "the approval is spent in one append and no other"
    );

    cluster.broker.shutdown().await;
}
