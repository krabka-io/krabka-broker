//! The KFC-1 scheduled-delivery gate, which decides whether a batch's
//! delivery time is one the target partition accepts.

use std::sync::{Mutex, PoisonError};

use krabka_log::Log;
use krabka_units::{Time, convert::TimeExt};

use crate::config_keys::{
    DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, resolve_delivery_max_delay,
    resolve_delivery_schedule_monotonic,
};

/// The KFC-1 produce-time delivery settings of one topic.
///
/// [`resolve_delivery_gate`] builds this once per topic, and only for a topic
/// whose `delivery.mode` is `scheduled`. An immediate topic resolves to `None`,
/// so no partition of it reads a clock, takes the log mutex, or looks at a
/// batch timestamp.
///
/// On a scheduled topic a batch's `max_timestamp` is its delivery time. Both
/// rejections read that one v2 header field, which
/// [`krabka_protocol::records::validate_one_v2_batch`] already extracted into
/// `prepare::ValidatedHeader::max_timestamp`. Neither decodes a record,
/// decompresses a body, or changes the verbatim-passthrough decision, so a
/// scheduled topic keeps the same zero-copy append an immediate topic gets.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct DeliveryGate {
    /// `delivery.max.delay.ms`: the largest delay accepted, measured forward
    /// from produce time. `None` is the `-1` sentinel and removes the bound.
    max_delay: Option<Time>,
    /// `delivery.schedule.monotonic`: reject a batch that would make the
    /// partition's schedule run backwards.
    monotonic: bool,
}

impl DeliveryGate {
    /// Whether this batch earns `INVALID_TIMESTAMP` (32).
    ///
    /// `delivery_ms` is the batch's `max_timestamp`, `produced_at_ms` is the
    /// broker's clock reading for this produce, and `log` is the target
    /// partition's log.
    pub(super) fn rejects(self, delivery_ms: i64, produced_at_ms: i64, log: &Mutex<Log>) -> bool {
        self.exceeds_max_delay(delivery_ms, produced_at_ms)
            || (self.monotonic && schedule_runs_backwards(log, delivery_ms))
    }

    /// Whether `delivery_ms` sits further ahead of `produced_at_ms` than
    /// `delivery.max.delay.ms` allows.
    ///
    /// The bound is one-sided. It limits how far ahead a producer may schedule
    /// a batch and says nothing about a delivery time in the past, which comes
    /// due at once.
    fn exceeds_max_delay(self, delivery_ms: i64, produced_at_ms: i64) -> bool {
        self.max_delay
            .is_some_and(|delay| delivery_ms.saturating_sub(produced_at_ms) > delay.millis_i64())
    }
}

/// Whether `log` already holds a delivery time later than `delivery_ms`.
///
/// This is the `delivery.schedule.monotonic` test. KFC-1 defines it against
/// "the largest delivery time already in the partition", and the log answers
/// that as an existence query: one record scheduled strictly after this batch
/// is one record this batch would hold up. Delivery is offset-ordered for a
/// classic group, so such a batch stalls the partition's schedule instead of
/// overtaking, and the config turns that silent stall into an error the
/// producer that caused it can see.
///
/// Every batch still waiting has a delivery time above the broker's activation
/// cutoff and every batch already delivered has one at or below it, so whenever
/// the partition holds a waiting batch at all, the largest delivery time in it
/// *is* the largest waiting one.
///
/// [`Log::offset_for_timestamp`] skips a segment whose own cached maximum sits
/// below the target, so a schedule that runs forward — the accepted case —
/// costs one integer comparison per segment and no disk read. Only a rejected
/// batch pays for an index lookup and a bounded scan.
fn schedule_runs_backwards(log: &Mutex<Log>, delivery_ms: i64) -> bool {
    let Some(later) = delivery_ms.checked_add(1) else {
        // Nothing can be scheduled after `i64::MAX`.
        return false;
    };
    // Recover a poisoned guard rather than fail the produce. The log data stays
    // consistent enough to read a timestamp out of, and the partition writer
    // takes the same view of a poisoned lock.
    log.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .offset_for_timestamp(later)
        .is_some()
}

/// Resolve a topic's KFC-1 delivery settings from the metadata image.
///
/// `None` means `delivery.mode=immediate`, the default and Kafka's behavior:
/// the produce path then does no delivery work for the topic at all. The two
/// settings come from [`resolve_delivery_max_delay`] and
/// [`resolve_delivery_schedule_monotonic`], which fall back to their defaults
/// on a corrupt value exactly as the other produce-side config reads do.
pub(super) fn resolve_delivery_gate(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<DeliveryGate> {
    let scheduled = image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_MODE))
        .map(String::as_str)
        == Some(DELIVERY_MODE_SCHEDULED);
    scheduled.then(|| DeliveryGate {
        max_delay: resolve_delivery_max_delay(image, topic),
        monotonic: resolve_delivery_schedule_monotonic(image, topic),
    })
}

#[cfg(test)]
mod tests;
