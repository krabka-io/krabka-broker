//! The `ListOffsets` timestamp sentinels, their response placeholders, and the
//! protocol version each sentinel needs.
//!
//! Kafka encodes what a partition row asks for in the request timestamp, so
//! every sentinel here is a wire value that must keep its exact number. A
//! client that names a sentinel its request version does not carry receives
//! `UNSUPPORTED_VERSION`, which is what [`timestamp_supported`] decides.

/// Request timestamp sentinel (-2): resolve the earliest available offset.
/// Kafka's `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
#[cfg(test)]
pub(super) const EARLIEST_TIMESTAMP: i64 = -2;

/// Request timestamp sentinel (-1): resolve the log-end (next) offset.
/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
#[cfg(test)]
pub(super) const LATEST_TIMESTAMP: i64 = -1;

/// Request timestamp sentinel (-3, KIP-734): resolve the offset of the record
/// with the highest timestamp. Kafka's `ListOffsetsRequest.MAX_TIMESTAMP`.
#[cfg(test)]
pub(super) const MAX_TIMESTAMP: i64 = -3;

/// Request timestamp sentinel (-4, KIP-405): resolve the earliest offset still
/// in local storage. Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP`.
#[cfg(test)]
pub(super) const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

/// Request timestamp sentinel (-5, KIP-1005): resolve the highest offset in a
/// finished remote segment.
#[cfg(test)]
pub(super) const LATEST_TIERED_TIMESTAMP: i64 = -5;

/// Request timestamp sentinel (-6, KIP-1023): resolve the first offset that
/// has not been uploaded to the remote tier.
#[cfg(test)]
pub(super) const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

/// Response placeholder (-1) meaning "no record timestamp matched/echoed".
/// Kafka's `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
pub(super) const UNKNOWN_TIMESTAMP: i64 = -1;

/// Response placeholder (-1) meaning "no offset was resolved".
/// Kafka's `ListOffsetsResponse.UNKNOWN_OFFSET`.
pub(super) const UNKNOWN_OFFSET: i64 = -1;

/// Response placeholder (-1) meaning "no leader epoch is being reported".
/// Kafka's `ListOffsetsResponse.UNKNOWN_EPOCH`.
pub(super) const UNKNOWN_EPOCH: i32 = -1;

pub(super) fn timestamp_kind(timestamp: i64, version: i16) -> krabka_verified::ListOffsetsKind {
    krabka_verified::list_offsets_kind(timestamp, version)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, krabka_verified::ListOffsetsKind};

    struct OffsetTimestampContract {
        position: String,
        timestamp: i64,
        version: i16,
        encoded: Option<i64>,
        classified: Option<ListOffsetsKind>,
    }

    #[rstest::fixture]
    fn offset_timestamp() -> OffsetTimestampContract {
        // Start this scenario with fresh inputs and observations.

        OffsetTimestampContract {
            position: String::new(),
            timestamp: 0,
            version: 0,
            encoded: None,
            classified: None,
        }
    }

    #[rstest_bdd_macros::given("the client requests the {position} log position")]
    fn requested_position(offset_timestamp: &mut OffsetTimestampContract, position: String) {
        offset_timestamp.position = position;
    }

    #[rstest_bdd_macros::given(
        "a ListOffsets version {version} request carries timestamp {timestamp}"
    )]
    fn timestamp_request(
        offset_timestamp: &mut OffsetTimestampContract,
        version: i16,
        timestamp: i64,
    ) {
        offset_timestamp.version = version;
        offset_timestamp.timestamp = timestamp;
    }

    #[rstest_bdd_macros::when("the position is encoded as a Kafka request timestamp")]
    fn encode_position(offset_timestamp: &mut OffsetTimestampContract) {
        offset_timestamp.encoded = Some(match offset_timestamp.position.as_str() {
            "earliest" => EARLIEST_TIMESTAMP,
            "latest" => LATEST_TIMESTAMP,
            "maximum-timestamp" => MAX_TIMESTAMP,
            "earliest-local" => EARLIEST_LOCAL_TIMESTAMP,
            "latest-tiered" => LATEST_TIERED_TIMESTAMP,
            "earliest-pending-upload" => EARLIEST_PENDING_UPLOAD_TIMESTAMP,
            other => panic!("unknown ListOffsets position {other}"),
        });
    }

    #[rstest_bdd_macros::when("the broker classifies the ListOffsets timestamp")]
    fn classify_timestamp(offset_timestamp: &mut OffsetTimestampContract) {
        offset_timestamp.classified = Some(timestamp_kind(
            offset_timestamp.timestamp,
            offset_timestamp.version,
        ));
    }

    #[rstest_bdd_macros::then("the wire timestamp is {timestamp}")]
    fn wire_timestamp(offset_timestamp: &OffsetTimestampContract, timestamp: i64) {
        assert!(offset_timestamp.encoded == Some(timestamp));
    }

    #[rstest_bdd_macros::then("the timestamp is {support} by that protocol version")]
    fn timestamp_support(offset_timestamp: &OffsetTimestampContract, support: String) {
        let supported = match support.as_str() {
            "supported" => true,
            "unsupported" => false,
            _ => panic!("unknown support expectation {support}"),
        };
        let classified = offset_timestamp
            .classified
            .as_ref()
            .expect("the request was classified");
        assert!((*classified != ListOffsetsKind::Unsupported) == supported);
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/handlers/list_offsets/sentinels.feature", fixtures = [offset_timestamp: OffsetTimestampContract]);
}
