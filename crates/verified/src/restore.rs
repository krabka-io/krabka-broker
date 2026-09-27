//! Batch-offset continuity, record placement, and rewrite admission for
//! offline restore verification.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Batch fate after folding the exact per-record selection results.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RestoreFilterDecision {
    Keep,
    Empty,
    Filter,
}

/// Which operator exclusion predicates matched one record.
///
/// The host evaluates each pattern against the record; the kernel owns only
/// how the matches combine.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreExclusions {
    /// `--exclude-producer-id` names the batch's producer.
    pub producer: bool,
    /// An `--exclude-offset` range covers the record's absolute offset.
    pub offset: bool,
    /// Regex matches over the record's own bytes.
    pub content: RestoreContentExclusions,
}

/// Which content patterns matched one record's raw key and header bytes.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreContentExclusions {
    /// An `--exclude-key` pattern matches the record key.
    pub key: bool,
    /// An `--exclude-header` pattern matches one record header.
    pub header: bool,
}

/// Exclusion predicates are OR-combined: any one match drops the record.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn restore_excluded(exclusions: RestoreExclusions) -> bool {
    pearlite! {
        exclusions.producer
            || exclusions.offset
            || exclusions.content.key
            || exclusions.content.header
    }
}

/// Keep one record exactly when it is inside both bounds and no exclusion
/// predicate matches. `--to-offset` is inclusive; `--to-timestamp` is
/// exclusive.
#[ensures(result == (
    match offset_bound { Some(bound) => offset@ <= bound@, None => true }
        && match timestamp_bound { Some(bound) => timestamp@ < bound@, None => true }
        && !restore_excluded(exclusions)
))]
#[must_use]
pub fn restore_record_selected(
    offset: i64,
    offset_bound: Option<i64>,
    timestamp: i64,
    timestamp_bound: Option<i64>,
    exclusions: RestoreExclusions,
) -> bool {
    (match offset_bound {
        Some(bound) => offset <= bound,
        None => true,
    }) && (match timestamp_bound {
        Some(bound) => timestamp < bound,
        None => true,
    }) && !(exclusions.producer
        || exclusions.offset
        || exclusions.content.key
        || exclusions.content.header)
}

/// Fold whether the record walk saw at least one keep and one drop.
#[ensures(match result {
    RestoreFilterDecision::Keep => !saw_drop,
    RestoreFilterDecision::Empty => saw_drop && !saw_keep,
    RestoreFilterDecision::Filter => saw_keep && saw_drop,
})]
#[must_use]
pub fn restore_batch_filter_decision(saw_keep: bool, saw_drop: bool) -> RestoreFilterDecision {
    if saw_keep && saw_drop {
        RestoreFilterDecision::Filter
    } else if saw_drop {
        RestoreFilterDecision::Empty
    } else {
        RestoreFilterDecision::Keep
    }
}

/// A sorted batch stream may stop exactly when the next batch base is past an
/// inclusive offset bound.
#[ensures(result == match offset_bound {
    Some(bound) => batch_base@ > bound@,
    None => false,
})]
#[must_use]
pub fn restore_batch_past_offset_bound(batch_base: i64, offset_bound: Option<i64>) -> bool {
    match offset_bound {
        Some(bound) => batch_base > bound,
        None => false,
    }
}

/// One remote segment's lifecycle state in the RLMM snapshot, or its absence.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RestoreSnapshotState {
    /// The snapshot does not list the segment.
    Missing,
    /// `COPY_SEGMENT_STARTED` or `COPY_SEGMENT_FINISHED`.
    Live,
    /// `DELETE_SEGMENT_STARTED`.
    DeleteStarted,
    /// `DELETE_SEGMENT_FINISHED`.
    DeleteFinished,
}

/// What restore does with one segment after comparing scan and snapshot.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RestoreReconcileDecision {
    /// The scanned bytes are restored.
    Keep,
    /// The segment is absent or being deleted; nothing is restored for it.
    Exclude,
    /// The archive scan and the snapshot contradict each other.
    Disagree,
}

/// Reconcile one archive-scan observation with one snapshot lifecycle state.
///
/// A live segment must have been scanned. A segment whose deletion started is
/// excluded whether or not its objects remain. A scanned segment that the
/// snapshot omits, or reports as fully deleted, disagrees; an unscanned one
/// is simply excluded.
#[ensures((result == RestoreReconcileDecision::Keep)
    == (scanned && snapshot_state == RestoreSnapshotState::Live))]
#[ensures((result == RestoreReconcileDecision::Exclude)
    == ((!scanned && snapshot_state != RestoreSnapshotState::Live)
        || (scanned && snapshot_state == RestoreSnapshotState::DeleteStarted)))]
#[ensures((result == RestoreReconcileDecision::Disagree)
    == ((!scanned && snapshot_state == RestoreSnapshotState::Live)
        || (scanned && (snapshot_state == RestoreSnapshotState::Missing
            || snapshot_state == RestoreSnapshotState::DeleteFinished))))]
#[must_use]
pub fn restore_archive_reconcile(
    scanned: bool,
    snapshot_state: RestoreSnapshotState,
) -> RestoreReconcileDecision {
    match snapshot_state {
        RestoreSnapshotState::Live => {
            if scanned {
                RestoreReconcileDecision::Keep
            } else {
                RestoreReconcileDecision::Disagree
            }
        }
        RestoreSnapshotState::DeleteStarted => RestoreReconcileDecision::Exclude,
        RestoreSnapshotState::Missing | RestoreSnapshotState::DeleteFinished => {
            if scanned {
                RestoreReconcileDecision::Disagree
            } else {
                RestoreReconcileDecision::Exclude
            }
        }
    }
}

/// Validate one archived batch and return its exclusive next offset.
///
/// The caller supplies the minimum base offset the next batch may carry. Kafka
/// compaction preserves absolute offsets and may leave gaps, so a later base is
/// valid; overlap/regression, a negative span, and an exclusive end outside
/// `i64` fail closed.
#[ensures(match result {
    Some(next) => last_delta@ >= 0
        && base@ >= minimum_base@
        && next@ == base@ + last_delta@ + 1
        && next@ > base@
        && next@ <= i64::MAX@,
    None => last_delta@ < 0
        || base@ < minimum_base@
        || base@ + last_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn restore_batch_step(minimum_base: i64, base: i64, last_delta: i32) -> Option<i64> {
    if last_delta < 0 || base < minimum_base {
        return None;
    }

    let delta = i64::from(last_delta);
    if base > i64::MAX - delta - 1 {
        None
    } else {
        Some(base + delta + 1)
    }
}

/// Kafka's batch timestamp type: attributes bit 3
/// (`DefaultRecordBatch.TIMESTAMP_TYPE_MASK`).
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RestoreTimestampType {
    CreateTime,
    LogAppendTime,
}

/// The batch-header fields that place a record in offset and time.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreBatchFrame {
    pub base_offset: i64,
    pub last_offset_delta: i32,
    pub timestamp_type: RestoreTimestampType,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
}

/// One record's encoded offset and timestamp deltas.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreRecordDeltas {
    pub offset_delta: i32,
    pub timestamp_delta: i64,
}

/// The timestamp Kafka reports for a record: `baseTimestamp + timestampDelta`
/// under `CreateTime`, and the batch `maxTimestamp` under `LogAppendTime`,
/// whose record deltas stay as the producer wrote them
/// (`DefaultRecord.readFrom(..., logAppendTime)`,
/// `DefaultRecordBatch.RecordIterator`).
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn kafka_record_timestamp(frame: RestoreBatchFrame, record: RestoreRecordDeltas) -> Int {
    pearlite! {
        match frame.timestamp_type {
            RestoreTimestampType::CreateTime => frame.base_timestamp@ + record.timestamp_delta@,
            RestoreTimestampType::LogAppendTime => frame.max_timestamp@,
        }
    }
}

/// A record lies inside its batch's declared offset span, and both its
/// absolute offset and its Kafka timestamp are representable as `i64`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn restore_record_placeable(frame: RestoreBatchFrame, record: RestoreRecordDeltas) -> bool {
    pearlite! {
        frame.last_offset_delta@ >= 0
            && record.offset_delta@ >= 0
            && record.offset_delta@ <= frame.last_offset_delta@
            && frame.base_offset@ + record.offset_delta@ <= i64::MAX@
            && kafka_record_timestamp(frame, record) >= i64::MIN@
            && kafka_record_timestamp(frame, record) <= i64::MAX@
    }
}

/// Compute one record's absolute offset and Kafka timestamp within its batch.
///
/// Malformed ranges and either kind of overflow fail closed. Under
/// `LogAppendTime` the producer's `base_timestamp + timestamp_delta` is never
/// read, as in Kafka, so it cannot make a legal batch fail.
#[ensures(match result {
    Some((offset, timestamp)) => restore_record_placeable(frame, record)
        && offset@ == frame.base_offset@ + record.offset_delta@
        && timestamp@ == kafka_record_timestamp(frame, record),
    None => !restore_record_placeable(frame, record),
})]
#[must_use]
pub fn restore_record_coordinates(
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
) -> Option<(i64, i64)> {
    if frame.last_offset_delta < 0
        || record.offset_delta < 0
        || record.offset_delta > frame.last_offset_delta
    {
        return None;
    }
    let offset = frame
        .base_offset
        .checked_add(i64::from(record.offset_delta))?;
    let timestamp = match frame.timestamp_type {
        RestoreTimestampType::CreateTime => {
            frame.base_timestamp.checked_add(record.timestamp_delta)?
        }
        RestoreTimestampType::LogAppendTime => frame.max_timestamp,
    };
    Some((offset, timestamp))
}

/// Producer identity and batch kind of one rewritten batch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreProducer {
    /// Attributes bit 5.
    pub control: bool,
    /// Attributes bit 4.
    pub transactional: bool,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

/// Offset layout of one rewritten batch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreLayout {
    pub base_offset: i64,
    pub last_offset_delta: i32,
    pub records_count: i32,
}

/// Kafka's legal producer identities (`RecordBatch.NO_PRODUCER_ID`,
/// `NO_PRODUCER_EPOCH`, `NO_SEQUENCE` are all `-1`).
///
/// A data batch is either non-idempotent, with every identity field at its
/// `-1` sentinel and no transactional bit, or idempotent, with a nonnegative
/// producer id, epoch and base sequence. A transaction marker is a
/// transactional control batch with a nonnegative producer and epoch and the
/// `-1` sequence (`EndTransactionMarker` batches carry no sequence). A
/// non-transactional control batch carries the non-idempotent sentinels.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn legal_producer(producer: RestoreProducer) -> bool {
    pearlite! {
        if producer.control {
            non_idempotent_producer(producer)
                || (producer.transactional
                    && producer.producer_id@ >= 0
                    && producer.producer_epoch@ >= 0
                    && producer.base_sequence@ == -1)
        } else {
            non_idempotent_producer(producer)
                || (producer.producer_id@ >= 0
                    && producer.producer_epoch@ >= 0
                    && producer.base_sequence@ >= 0)
        }
    }
}

/// Every identity field at its `-1` sentinel and no transactional bit.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn non_idempotent_producer(producer: RestoreProducer) -> bool {
    pearlite! {
        !producer.transactional
            && producer.producer_id@ == -1
            && producer.producer_epoch@ == -1
            && producer.base_sequence@ == -1
    }
}

/// A batch header's offset layout is legal: nonnegative base, span and
/// record count, an exclusive end that fits `i64`, and for a control batch
/// Kafka's single-offset span holding at most its one marker. A control batch
/// may hold zero records because Kafka's `LogCleaner` (and krabka's
/// compaction) keeps a producer's last batch as an empty header
/// (`BatchRetention.RETAIN_EMPTY`) once the marker record itself is discarded.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn legal_header(layout: RestoreLayout, control: bool) -> bool {
    pearlite! {
        layout.base_offset@ >= 0
            && layout.last_offset_delta@ >= 0
            && layout.records_count@ >= 0
            && (control ==> layout.last_offset_delta@ == 0 && layout.records_count@ <= 1)
            && layout.base_offset@ + layout.last_offset_delta@ + 1 <= i64::MAX@
    }
}

/// Admit a batch header synthesized or re-encoded by restore and return its
/// exclusive offset frontier.
#[ensures(match result {
    Some(frontier) => legal_header(layout, producer.control)
        && legal_producer(producer)
        && frontier@ == layout.base_offset@ + layout.last_offset_delta@ + 1,
    None => !(legal_header(layout, producer.control) && legal_producer(producer)),
})]
#[must_use]
pub fn restore_rewritten_batch_header(
    layout: RestoreLayout,
    producer: RestoreProducer,
) -> Option<i64> {
    let non_idempotent = !producer.transactional
        && producer.producer_id == -1
        && producer.producer_epoch == -1
        && producer.base_sequence == -1;
    let producer_legal = if producer.control {
        non_idempotent
            || (producer.transactional
                && producer.producer_id >= 0
                && producer.producer_epoch >= 0
                && producer.base_sequence == -1)
    } else {
        non_idempotent
            || (producer.producer_id >= 0
                && producer.producer_epoch >= 0
                && producer.base_sequence >= 0)
    };
    if !producer_legal
        || layout.base_offset < 0
        || layout.last_offset_delta < 0
        || layout.records_count < 0
        || (producer.control && (layout.last_offset_delta != 0 || layout.records_count > 1))
    {
        return None;
    }

    layout
        .base_offset
        .checked_add(i64::from(layout.last_offset_delta))?
        .checked_add(1)
}

/// One retained record is legal in a rewritten batch: it is placeable, its
/// offset delta strictly follows the previous retained record's, and its
/// Kafka timestamp does not exceed the preserved archived `max_timestamp`.
/// The bound may be loose when the record that set it was filtered out;
/// under `LogAppendTime` it holds with equality.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn restore_rewrite_record_legal(
    previous_offset_delta: Option<i32>,
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
) -> bool {
    pearlite! {
        restore_record_placeable(frame, record)
            && match previous_offset_delta {
                Some(previous) => previous@ < record.offset_delta@,
                None => true,
            }
            && kafka_record_timestamp(frame, record) <= frame.max_timestamp@
    }
}

/// Validate one retained record against the synthesized header and return its
/// absolute offset and Kafka timestamp.
#[ensures(match result {
    Some((offset, timestamp)) => restore_rewrite_record_legal(previous_offset_delta, frame, record)
        && offset@ == frame.base_offset@ + record.offset_delta@
        && timestamp@ == kafka_record_timestamp(frame, record),
    None => !restore_rewrite_record_legal(previous_offset_delta, frame, record),
})]
#[must_use]
pub fn restore_rewritten_record(
    previous_offset_delta: Option<i32>,
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
) -> Option<(i64, i64)> {
    if previous_offset_delta.is_some_and(|previous| previous >= record.offset_delta) {
        return None;
    }
    let coordinates = restore_record_coordinates(frame, record)?;
    if coordinates.1 > frame.max_timestamp {
        None
    } else {
        Some(coordinates)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    const NO_CONTENT: RestoreContentExclusions = RestoreContentExclusions {
        key: false,
        header: false,
    };

    const NONE: RestoreExclusions = RestoreExclusions {
        producer: false,
        offset: false,
        content: NO_CONTENT,
    };

    fn frame(
        timestamp_type: RestoreTimestampType,
        base_timestamp: i64,
        max_timestamp: i64,
    ) -> RestoreBatchFrame {
        RestoreBatchFrame {
            base_offset: 10,
            last_offset_delta: 4,
            timestamp_type,
            base_timestamp,
            max_timestamp,
        }
    }

    const fn deltas(offset_delta: i32, timestamp_delta: i64) -> RestoreRecordDeltas {
        RestoreRecordDeltas {
            offset_delta,
            timestamp_delta,
        }
    }

    const NON_IDEMPOTENT: RestoreProducer = RestoreProducer {
        control: false,
        transactional: false,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
    };

    const fn layout(base_offset: i64, last_offset_delta: i32, records_count: i32) -> RestoreLayout {
        RestoreLayout {
            base_offset,
            last_offset_delta,
            records_count,
        }
    }

    #[test]
    fn archive_reconciliation_covers_every_scan_and_snapshot_pair() {
        use RestoreReconcileDecision::{Disagree, Exclude, Keep};
        use RestoreSnapshotState::{DeleteFinished, DeleteStarted, Live, Missing};
        for (scanned, state, expected) in [
            (true, Live, Keep),
            (true, DeleteStarted, Exclude),
            (true, Missing, Disagree),
            (true, DeleteFinished, Disagree),
            (false, Live, Disagree),
            (false, DeleteStarted, Exclude),
            (false, DeleteFinished, Exclude),
            (false, Missing, Exclude),
        ] {
            check!(restore_archive_reconcile(scanned, state) == expected);
        }
    }

    #[test]
    fn batch_step_is_contiguous_and_overflow_safe() {
        check!(restore_batch_step(10, 10, 2) == Some(13));
        check!(restore_batch_step(10, 11, 0) == Some(12));
        check!(restore_batch_step(10, 9, 0) == None);
        check!(restore_batch_step(10, 10, -1) == None);
        check!(restore_batch_step(i64::MAX, i64::MAX, 0) == None);
        check!(restore_batch_step(i64::MAX - 1, i64::MAX - 1, 0) == Some(i64::MAX));
    }

    #[test]
    fn record_coordinates_follow_kafka_timestamp_type() {
        use RestoreTimestampType::{CreateTime, LogAppendTime};
        for (name, frame, record, expected) in [
            (
                "create time adds the delta",
                frame(CreateTime, 100, 110),
                deltas(1, -5),
                Some((11, 95)),
            ),
            (
                "log append time reports the batch max timestamp",
                frame(LogAppendTime, 2_000, 1_000),
                deltas(1, 50),
                Some((11, 1_000)),
            ),
            (
                "log append time ignores an overflowing producer delta",
                frame(LogAppendTime, i64::MAX, 1_000),
                deltas(0, 1),
                Some((10, 1_000)),
            ),
            (
                "create time overflow above",
                frame(CreateTime, i64::MAX, i64::MAX),
                deltas(0, 1),
                None,
            ),
            (
                "create time overflow below",
                frame(CreateTime, i64::MIN, i64::MAX),
                deltas(0, -1),
                None,
            ),
            (
                "create time reaches i64::MIN exactly",
                frame(CreateTime, i64::MIN + 5, i64::MAX),
                deltas(0, -5),
                Some((10, i64::MIN)),
            ),
            (
                "negative offset delta",
                frame(CreateTime, 100, 110),
                deltas(-1, 0),
                None,
            ),
            (
                "offset delta past the span",
                frame(CreateTime, 100, 110),
                deltas(5, 0),
                None,
            ),
            (
                "negative span",
                RestoreBatchFrame {
                    last_offset_delta: -1,
                    ..frame(CreateTime, 100, 110)
                },
                deltas(0, 0),
                None,
            ),
            (
                "offset overflow",
                RestoreBatchFrame {
                    base_offset: i64::MAX,
                    ..frame(CreateTime, 100, 110)
                },
                deltas(1, 0),
                None,
            ),
            (
                "extreme but representable",
                RestoreBatchFrame {
                    base_offset: i64::MAX,
                    last_offset_delta: 0,
                    ..frame(CreateTime, i64::MAX, i64::MAX)
                },
                deltas(0, 0),
                Some((i64::MAX, i64::MAX)),
            ),
        ] {
            check!(
                restore_record_coordinates(frame, record) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn rewritten_headers_admit_exactly_kafka_producer_and_control_layouts() {
        let idempotent = RestoreProducer {
            producer_id: 7,
            producer_epoch: 2,
            base_sequence: 4,
            ..NON_IDEMPOTENT
        };
        let transactional = RestoreProducer {
            transactional: true,
            ..idempotent
        };
        let marker = RestoreProducer {
            control: true,
            base_sequence: -1,
            ..transactional
        };
        let barrier = RestoreProducer {
            control: true,
            ..NON_IDEMPOTENT
        };
        for (name, layout, producer, expected) in [
            (
                "non-idempotent data",
                layout(10, 2, 2),
                NON_IDEMPOTENT,
                Some(13),
            ),
            (
                "idempotent emptied data",
                layout(10, 2, 0),
                idempotent,
                Some(13),
            ),
            (
                "transactional data",
                layout(10, 2, 3),
                transactional,
                Some(13),
            ),
            ("transaction marker", layout(10, 0, 1), marker, Some(11)),
            (
                "compacted-away marker kept empty",
                layout(10, 0, 0),
                marker,
                Some(11),
            ),
            ("barrier control batch", layout(0, 0, 1), barrier, Some(1)),
            (
                "control spanning two offsets",
                layout(10, 1, 1),
                barrier,
                None,
            ),
            ("control with two records", layout(10, 0, 2), barrier, None),
            (
                "marker carrying a sequence",
                layout(10, 0, 1),
                RestoreProducer {
                    base_sequence: 4,
                    ..marker
                },
                None,
            ),
            (
                "transactional marker without producer",
                layout(10, 0, 1),
                RestoreProducer {
                    transactional: true,
                    ..barrier
                },
                None,
            ),
            (
                "transactional data without producer",
                layout(10, 2, 2),
                RestoreProducer {
                    transactional: true,
                    ..NON_IDEMPOTENT
                },
                None,
            ),
            (
                "partial sentinel epoch",
                layout(10, 2, 2),
                RestoreProducer {
                    producer_epoch: 0,
                    ..NON_IDEMPOTENT
                },
                None,
            ),
            (
                "producer without epoch",
                layout(10, 2, 2),
                RestoreProducer {
                    producer_epoch: -1,
                    ..idempotent
                },
                None,
            ),
            ("negative span", layout(0, -1, 1), NON_IDEMPOTENT, None),
            ("negative count", layout(10, 2, -1), NON_IDEMPOTENT, None),
            ("negative base", layout(-1, 0, 1), NON_IDEMPOTENT, None),
            (
                "frontier overflow",
                layout(i64::MAX, 0, 0),
                NON_IDEMPOTENT,
                None,
            ),
        ] {
            check!(
                restore_rewritten_batch_header(layout, producer) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn rewritten_records_are_strict_bounded_and_timestamp_checked() {
        use RestoreTimestampType::{CreateTime, LogAppendTime};
        let create = frame(CreateTime, 100, 110);
        // A LogAppendTime batch whose producer clock ran ahead of the broker:
        // every base + delta exceeds max_timestamp, which Kafka never reads.
        let append = frame(LogAppendTime, 2_000, 1_000);
        for (name, previous, frame, record, expected) in [
            ("first record", None, create, deltas(1, 5), Some((11, 105))),
            (
                "last record at the max",
                Some(1),
                create,
                deltas(4, 10),
                Some((14, 110)),
            ),
            ("repeated delta", Some(1), create, deltas(1, 5), None),
            ("regressing delta", Some(3), create, deltas(2, 5), None),
            ("past the span", None, create, deltas(5, 5), None),
            (
                "create time above the max",
                None,
                frame(CreateTime, 100, 104),
                deltas(1, 5),
                None,
            ),
            (
                "create time overflow",
                None,
                frame(CreateTime, i64::MAX, i64::MAX),
                deltas(1, 1),
                None,
            ),
            (
                "log append time with producer clock ahead",
                None,
                append,
                deltas(0, 0),
                Some((10, 1_000)),
            ),
            (
                "log append time later record",
                Some(0),
                append,
                deltas(3, 900),
                Some((13, 1_000)),
            ),
            (
                "log append time still strict",
                Some(3),
                append,
                deltas(3, 0),
                None,
            ),
        ] {
            check!(
                restore_rewritten_record(previous, frame, record) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn record_selection_uses_inclusive_offset_and_exclusive_time_bounds() {
        for (name, offset, timestamp, exclusions, expected) in [
            ("inside both bounds", 10, 99, NONE, true),
            ("past the offset bound", 11, 99, NONE, false),
            ("at the timestamp bound", 10, 100, NONE, false),
            (
                "producer excluded",
                10,
                99,
                RestoreExclusions {
                    producer: true,
                    ..NONE
                },
                false,
            ),
            (
                "offset excluded",
                10,
                99,
                RestoreExclusions {
                    offset: true,
                    ..NONE
                },
                false,
            ),
            (
                "key excluded",
                10,
                99,
                RestoreExclusions {
                    content: RestoreContentExclusions {
                        key: true,
                        ..NO_CONTENT
                    },
                    ..NONE
                },
                false,
            ),
            (
                "header excluded",
                10,
                99,
                RestoreExclusions {
                    content: RestoreContentExclusions {
                        header: true,
                        ..NO_CONTENT
                    },
                    ..NONE
                },
                false,
            ),
        ] {
            check!(
                restore_record_selected(offset, Some(10), timestamp, Some(100), exclusions)
                    == expected,
                "{name}"
            );
        }
        check!(restore_record_selected(
            i64::MIN,
            None,
            i64::MAX,
            None,
            NONE
        ));
    }

    #[test]
    fn batch_fold_and_skip_are_exact() {
        check!(restore_batch_filter_decision(false, false) == RestoreFilterDecision::Keep);
        check!(restore_batch_filter_decision(true, false) == RestoreFilterDecision::Keep);
        check!(restore_batch_filter_decision(false, true) == RestoreFilterDecision::Empty);
        check!(restore_batch_filter_decision(true, true) == RestoreFilterDecision::Filter);
        check!(!restore_batch_past_offset_bound(10, Some(10)));
        check!(restore_batch_past_offset_bound(11, Some(10)));
        check!(!restore_batch_past_offset_bound(i64::MAX, None));
    }
}
