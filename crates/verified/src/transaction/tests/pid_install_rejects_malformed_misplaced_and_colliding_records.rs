use assert2::assert;

use super::*;

#[test]
fn pid_install_rejects_malformed_misplaced_and_colliding_records() {
    use TransactionPidInstallDecision::{
        Apply, RejectCollision, RejectCurrentIdentity, RejectStagedIdentity, RejectWrongPartition,
    };

    for (arguments, expected) in [
        ((false, 1, 0, -1, -1, true, true), RejectWrongPartition),
        ((true, -1, 0, -1, -1, true, true), RejectCurrentIdentity),
        ((true, 1, -1, -1, -1, true, true), RejectCurrentIdentity),
        ((true, 1, 0, 2, -1, true, true), RejectStagedIdentity),
        ((true, 1, 0, -1, 0, true, true), RejectStagedIdentity),
        ((true, 1, 0, 1, 0, true, true), Apply),
        ((true, 1, 0, -1, -1, false, true), RejectCollision),
        ((true, 1, 0, 2, 0, true, false), RejectCollision),
        ((true, 1, 0, -1, -1, true, true), Apply),
        ((true, 0, 0, -1, -1, true, true), Apply),
        ((true, 1, 0, 2, 0, true, true), Apply),
    ] {
        assert!(
            transaction_pid_install_decision(
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                arguments.6,
            ) == expected
        );
    }
}

/// Kafka `ProducerStateManager`'s `appendEndTxnMarker` fencing, plus the
/// exact-retry and offset-publication rules.
#[test]
fn marker_materialization_fences_retries_and_offset_publication() {
    use TransactionMarkerMaterializationDecision::{
        AppendAndPublishOffsets, AppendWithoutOffsetPublication, RejectCoordinatorEpoch,
        RejectMalformed, RejectProducerEpoch, Retry,
    };

    let cases = [
        // A negative producer ID names no producer.
        (
            TransactionMarkerRequest {
                producer_id: -1,
                ..marker(0, 0, true, true)
            },
            partition(-1, -1, true),
            RejectMalformed,
        ),
        // A negative marker epoch.
        (
            marker(-1, 0, true, true),
            partition(0, 0, true),
            RejectMalformed,
        ),
        // A coordinator epoch below the `-1` sentinel.
        (
            marker(0, -2, false, false),
            partition(0, -1, true),
            RejectMalformed,
        ),
        // Partition state below the sentinels.
        (
            marker(0, 0, true, true),
            partition(-2, -1, true),
            RejectMalformed,
        ),
        (
            marker(0, 0, true, true),
            partition(0, -2, false),
            RejectMalformed,
        ),
        // A pending transaction without a producer epoch.
        (
            marker(0, 0, true, true),
            partition(-1, 0, true),
            RejectMalformed,
        ),
        // `ProducerFencedException`: the marker's epoch is older.
        (
            marker(2, 9, true, true),
            partition(3, 8, true),
            RejectProducerEpoch,
        ),
        // `TransactionCoordinatorFencedException`: an older coordinator.
        (
            marker(3, 7, true, true),
            partition(3, 8, true),
            RejectCoordinatorEpoch,
        ),
        // The same generation already closed the transaction: append
        // nothing.
        (marker(3, 8, true, true), partition(3, 8, false), Retry),
        (marker(0, 0, true, true), partition(0, 0, false), Retry),
        // A pending COMMIT on `__consumer_offsets` publishes its offsets.
        (
            marker(3, 8, true, true),
            partition(3, 7, true),
            AppendAndPublishOffsets,
        ),
        // An ABORT, a data partition, or no pending transaction appends
        // without publishing.
        (
            marker(3, 8, false, true),
            partition(3, 7, true),
            AppendWithoutOffsetPublication,
        ),
        (
            marker(3, 8, true, false),
            partition(3, 7, true),
            AppendWithoutOffsetPublication,
        ),
        (
            marker(0, -1, false, false),
            partition(0, -1, true),
            AppendWithoutOffsetPublication,
        ),
        // A newer generation than the closed one is not a retry.
        (
            marker(4, 9, true, true),
            partition(3, 8, false),
            AppendWithoutOffsetPublication,
        ),
        (
            marker(0, 0, true, true),
            partition(-1, 0, false),
            AppendWithoutOffsetPublication,
        ),
    ];
    for (request, current, expected) in cases {
        assert!(
            transaction_marker_materialization_decision(request, current) == expected,
            "request={request:?}, current={current:?}"
        );
    }
}
