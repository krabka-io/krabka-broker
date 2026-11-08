//! Pure Produce fixtures that retain each caller's wire and transport handling.

use {
    krabka_broker::codes,
    krabka_protocol::{
        owned::{
            produce_request::ProduceRequest,
            produce_response::{LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponse},
        },
        records::RecordBatch,
    },
};
krabka_broker_macros::single_partition_produce_fixture!(single_partition_produce);

/// Whether a Produce answer says the partition's leadership has not settled
/// yet, rather than that the request failed: a broker that does not lead the
/// partition yet, a partition without a leader, or a request that timed out.
/// These are the codes a Kafka producer retries after a metadata refresh.
pub fn leadership_unsettled(code: i16) -> bool {
    matches!(
        code,
        codes::LEADER_NOT_AVAILABLE | codes::NOT_LEADER_OR_FOLLOWER | codes::REQUEST_TIMED_OUT
    )
}

/// Verify the response cardinality before a scenario checks its partition row.
///
/// # Panics
/// Panics unless the response contains exactly one topic and one partition.
pub fn single_partition_response(response: &ProduceResponse) -> &PartitionProduceResponse {
    assert2::assert!(response.responses.len() == 1, "one topic in response");
    assert2::assert!(
        response.responses[0].partition_responses.len() == 1,
        "one partition row in response"
    );
    &response.responses[0].partition_responses[0]
}

/// The complete expected partition row after a successful non-duplicate append.
/// Kept independent from the broker's actual response construction.
pub fn expected_partition_success(
    base_offset: i64,
    log_start_offset: i64,
) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// Build and encode one v2-batch produce request for a raw-wire test.
#[bon::builder]
pub fn encoded_batch_request(
    batch: RecordBatch,
    topic: String,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
    version: i16,
) -> bytes::BytesMut {
    let request = single_partition_produce()
        .records(batch)
        .topic(topic)
        .partition(partition)
        .call();
    let mut body = bytes::BytesMut::new();
    krabka_protocol::Encode::encode(&request, &mut body, version).expect("encode ProduceRequest");
    body
}

/// Encode one partition's Produce body for callers that do not need to adjust builder defaults.
pub fn encode_batch_request(
    batch: RecordBatch,
    topic: &str,
    partition: i32,
    version: i16,
) -> bytes::BytesMut {
    encoded_batch_request()
        .batch(batch)
        .topic(topic.to_owned())
        .partition(partition)
        .version(version)
        .call()
}

/// Both client transports retain their normal Produce routing and error handling.
pub trait BatchSender: Sync {
    fn send_produce_request(
        &self,
        request: ProduceRequest,
    ) -> impl Future<Output = ProduceResponse> + Send;
}

impl BatchSender for krabka_client_core::Client {
    async fn send_produce_request(&self, request: ProduceRequest) -> ProduceResponse {
        self.send(request).await.expect("Produce")
    }
}

impl BatchSender for krabka_client_core::Connection {
    async fn send_produce_request(&self, req: ProduceRequest) -> ProduceResponse {
        let resp = self.send(req).await;
        resp.expect("Connection Produce")
    }
}

impl<T: BatchSender + ?Sized> BatchSender for &T {
    async fn send_produce_request(&self, request: ProduceRequest) -> ProduceResponse {
        (**self).send_produce_request(request).await
    }
}
