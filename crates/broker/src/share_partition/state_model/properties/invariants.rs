use super::*;

impl ShareModel {
    pub(super) fn model_properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("window_integrity", |_, s: &ShareState| {
                window_integrity(&s.sm)
            }),
            Property::always("mutual_exclusion", |_, s: &ShareState| {
                mutual_exclusion(&s.sm)
            }),
            Property::always("lock_consistency", |_, s: &ShareState| {
                lock_consistency(&s.sm)
            }),
            Property::always(
                "delivery_complete_count_is_terminal_in_window",
                |_, s: &ShareState| delivery_complete_count_is_terminal_in_window(&s.sm),
            ),
            Property::always(
                "delivery_count_bounded",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.batches
                        .iter()
                        .all(|b| b.delivery_count <= m.max_attempts)
                },
            ),
            Property::always("spso_in_range", |m: &ShareModel, s: &ShareState| {
                0 <= s.sm.start_offset
                    && s.sm.start_offset <= s.sm.end_offset
                    && s.sm.end_offset <= m.max_offset
            }),
            Property::sometimes("can_advance_spso", |_, s: &ShareState| {
                s.sm.start_offset > 0
            }),
            Property::sometimes("can_acknowledge", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Acknowledged)
            }),
            Property::sometimes("can_archive", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Archived)
            }),
            Property::sometimes("can_redeliver", |_, s: &ShareState| {
                s.sm.batches.iter().any(|b| b.delivery_count >= 2)
            }),
            // The count invariant is not vacuous: a terminal record can sit in
            // the window above a record that still holds the SPSO.
            Property::sometimes("can_count_delivery_complete", |_, s: &ShareState| {
                s.sm.delivery_complete_count > 0
            }),
        ];
        if !self.allow_defer {
            // Kafka's `numInFlightRecords` never passes the record lock limit
            // (`group.share.partition.max.record.locks`): the window is at most
            // `max_inflight` long. A deferred run is not in flight and is
            // promoted into the window later, so the claim is for the models
            // without deferral.
            properties.push(Property::always(
                "window_within_record_lock_limit",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.end_offset.0 - s.sm.start_offset.0 <= i64::from(m.max_inflight)
                },
            ));
        }
        if self.allow_log_start_advance {
            // The log start offset never runs ahead of what was produced: it
            // can only move over records that exist.
            properties.push(Property::always(
                "log_start_never_exceeds_hwm",
                |_, s: &ShareState| s.log_start <= s.hwm,
            ));
            // The scenario `advance_past_log_start_leaves_other_members_acquired_records_alone`
            // covers by hand: the log start moves past the SPSO while an
            // Acquired run still blocks the SPSO itself from following it.
            properties.push(Property::sometimes(
                "log_start_advance_blocked_by_acquired",
                |_, s: &ShareState| {
                    s.log_start > s.sm.start_offset
                        && s.sm
                            .batches
                            .iter()
                            .any(|b| b.state == RecordState::Acquired)
                },
            ));
        }
        if self.allow_defer {
            properties.push(Property::sometimes("can_defer", |_, s: &ShareState| {
                !deferred_offsets(&s.sm).is_empty()
            }));
            // The claim KFC-1 makes for share groups, and the one a classic
            // group cannot have: a record is handed out while a record below it
            // waits for its delivery time.
            properties.push(Property::sometimes(
                "can_deliver_behind_a_deferred_record",
                |_, s: &ShareState| {
                    deferred_offsets(&s.sm).first().is_some_and(|waiting| {
                        s.sm.batches
                            .iter()
                            .any(|b| b.state == RecordState::Acquired && b.first_offset > *waiting)
                    })
                },
            ));
        }
        properties
    }
}
