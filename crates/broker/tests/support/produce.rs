//! Pure Produce fixtures that retain each caller's wire and transport handling.

krabka_macros::single_partition_produce_fixture!(single_partition_produce);

/// Verify the response cardinality before a scenario checks its partition row.
///
/// # Panics
/// Panics unless the response contains exactly one topic and one partition.
pub fn single_partition_response(
    response: &krabka_protocol::owned::produce_response::ProduceResponse,
) -> &krabka_protocol::owned::produce_response::PartitionProduceResponse {
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
) -> krabka_protocol::owned::produce_response::PartitionProduceResponse {
    krabka_protocol::owned::produce_response::PartitionProduceResponse {
        index: 0,
        error_code: krabka_broker::codes::NONE,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset,
        record_errors: vec![],
        error_message: None,
        current_leader: krabka_protocol::owned::produce_response::LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}
