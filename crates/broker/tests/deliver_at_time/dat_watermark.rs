//! Where the delivery watermark sits while a record is still pending: what
//! `ListOffsets` LATEST reports, and what a batch scheduled far out does to a
//! later batch that is already due.
//!
//! Both cases turn on the same rule — a scheduled partition serves a prefix of
//! its log and nothing past the first pending record — so they are written
//! against the same immediate-topic control.

use crate::{
    dat_fixtures::{
        ALREADY_DUE_MS, IMMEDIATE, Mode, PENDING_HORIZON_MS, SCHEDULED, Visible, batch_at, now_ms,
    },
    dat_wire::{assert_visible, produce, ready_topic},
    support,
};

#[cfg(test)]
#[rstest_bdd_macros::then(
    "deliver at time dat watermark: list offsets latest lands a seek to end on the first pending record"
)]
async fn bdd_verify_list_offsets_latest_lands_a_seek_to_end_on_the_first_pending_record() {
    struct Case {
        mode: Mode,
        expected: Visible,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            expected: Visible::of(2, &["due-a", "due-b"]),
        },
        Case {
            mode: IMMEDIATE,
            expected: Visible::of(3, &["due-a", "due-b", "pending"]),
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-latest-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let now = now_ms();
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now - ALREADY_DUE_MS, &["due-a", "due-b"]),
        )
        .await;
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now + PENDING_HORIZON_MS, &["pending"]),
        )
        .await;

        // On a scheduled topic LATEST is the delivery watermark, so a consumer
        // that seeks to end lands on offset 2 and receives the pending record
        // when it activates. On an immediate topic it is the log end offset.
        assert_visible(&p.client, &topic, topic_id, case.expected).await;
    }

    p.broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "deliver at time dat watermark: a batch scheduled far out holds back a later batch that is already due"
)]
async fn bdd_verify_a_batch_scheduled_far_out_holds_back_a_later_batch_that_is_already_due() {
    struct Case {
        mode: Mode,
        expected: Visible,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            expected: Visible::of(0, &[]),
        },
        Case {
            mode: IMMEDIATE,
            expected: Visible::of(2, &["pending", "already-due"]),
        },
    ];

    for case in cases {
        let mode_val = case.mode.value;
        let topic = format!("deliver-at-time-head-of-line-{mode_val}");
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let pending_batch = batch_at(now_ms() + PENDING_HORIZON_MS, &["pending"]);
        produce(&p.client, &topic, topic_id, pending_batch).await;
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now_ms() - ALREADY_DUE_MS, &["already-due"]),
        )
        .await;

        // The record at offset 1 is due, and a scheduled topic still serves
        // nothing: a classic group's position is one offset per partition, so
        // delivering offset 1 first would put offset 0 permanently behind that
        // position. Head-of-line order is the contract, not a defect.
        assert_visible(&p.client, &topic, topic_id, case.expected).await;
    }

    p.broker.shutdown().await;
}

// BDD scenario entry points; acceptance steps are defined above.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_watermark.feature",
    name = "list offsets latest lands a seek to end on the first pending record"
)]
#[tokio::test]
async fn list_offsets_latest_lands_a_seek_to_end_on_the_first_pending_record() {}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_watermark.feature",
    name = "a batch scheduled far out holds back a later batch that is already due"
)]
#[tokio::test]
async fn a_batch_scheduled_far_out_holds_back_a_later_batch_that_is_already_due() {}
