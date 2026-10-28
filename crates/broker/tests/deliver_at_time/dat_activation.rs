//! What happens to a record between its produce and its delivery time: a read
//! taken before that time must not serve it, and a consumer already parked in a
//! long poll must be woken by the delivery advance rather than by the poll
//! expiring.
//!
//! Both cases run once on a scheduled topic and once on an immediate one, and
//! the immediate half is what shows the behaviour is the configuration.

use {
    crate::{
        dat_fixtures::{
            ACTIVATION_DELAY_MS, DeliveryScenario, Visible, batch_at, delivery, now_ms,
        },
        dat_wire::{fetch_values, produce, wait_until_visible},
    },
    assert2::check,
    krabka_protocol::primitives::uuid::Uuid,
    std::time::Instant,
};

async fn produce_due_soon(client: &krabka_client_core::Client, topic: &str, topic_id: Uuid) -> i64 {
    let deliver_at_ms = now_ms() + ACTIVATION_DELAY_MS;
    produce(
        client,
        topic,
        topic_id,
        batch_at(deliver_at_ms, &["due-soon"]),
    )
    .await;
    deliver_at_ms
}

/// `max.wait.ms` of the long poll in
/// [`a_parked_long_poll_wakes_when_the_record_comes_due`]. A consumer that the
/// delivery advance does not wake waits all of it out.
const LONG_POLL_MS: i32 = 20_000;

/// What the long poll has to beat to prove that it woke rather than expired.
/// The record it waits for comes due after [`ACTIVATION_DELAY_MS`] plus the
/// declared clock bound, so the two outcomes are seconds apart.
const LONG_POLL_WOKE_MS: u128 = 10_000;

#[rstest_bdd_macros::when("the producer appends due-soon stamped 2000 milliseconds in the future")]
async fn append_due_soon(delivery: &mut DeliveryScenario) {
    delivery.due_at = Some(
        produce_due_soon(
            delivery.client.as_ref().expect("delivery client connected"),
            &delivery.topic,
            delivery.topic_id.expect("delivery topic created"),
        )
        .await,
    );
}
#[rstest_bdd_macros::then("the visibility read completed before the delivery timestamp")]
fn pending_read_is_timely(delivery: &DeliveryScenario) {
    let read_at_ms = delivery.read_at.expect("visibility read measured");
    let deliver_at_ms = delivery.due_at.expect("due-soon appended");
    check!(
        read_at_ms < deliver_at_ms,
        "read finished {} ms after delivery, proving nothing",
        read_at_ms - deliver_at_ms
    );
}
#[rstest_bdd_macros::when("the consumer waits until due-soon is visible at LATEST 1")]
async fn await_due_soon(delivery: &mut DeliveryScenario) {
    delivery.served_at = Some(
        wait_until_visible(
            delivery.client.as_ref().expect("delivery client connected"),
            &delivery.topic,
            delivery.topic_id.expect("delivery topic created"),
            &Visible::of(1, &["due-soon"]),
        )
        .await,
    );
}
#[rstest_bdd_macros::then("first visibility is at or after the delivery timestamp {held}")]
fn due_soon_first_visibility(delivery: &DeliveryScenario, held: bool) {
    let served_at_ms = delivery.served_at.expect("visibility arrival measured");
    let deliver_at_ms = delivery.due_at.expect("due-soon appended");
    check!(
        (served_at_ms >= deliver_at_ms) == held,
        "first served at {served_at_ms}, delivery time {deliver_at_ms}"
    );
}
#[rstest_bdd_macros::when("the consumer starts a 20000 millisecond long poll for the record")]
async fn long_poll_due_soon(delivery: &mut DeliveryScenario) {
    let started = Instant::now();
    delivery.values = Some(
        fetch_values(
            delivery.client.as_ref().expect("delivery client connected"),
            &delivery.topic,
            delivery.topic_id.expect("delivery topic created"),
            LONG_POLL_MS,
        )
        .await,
    );
    delivery.elapsed = Some(started.elapsed());
    delivery.served_at = Some(now_ms());
}
#[rstest_bdd_macros::then("the long poll serves only due-soon within 10000 milliseconds")]
fn due_soon_long_poll_awakened(delivery: &DeliveryScenario) {
    check!(delivery.values.as_ref().expect("long poll completed") == &vec!["due-soon".to_owned()]);
    check!(delivery.elapsed.expect("long poll measured").as_millis() < LONG_POLL_WOKE_MS);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_activation.feature",
    name = "a record stamped in the future waits for its delivery time"
)]
#[tokio::test]
async fn a_record_stamped_in_the_future_waits_for_its_delivery_time(
    delivery: DeliveryScenario,
    mode: String,
    topic: String,
    held: bool,
    latest: i64,
    values: String,
) {
    let _ = (mode, topic, held, latest, values);
    delivery.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/deliver_at_time/dat_activation.feature",
    name = "a parked long poll wakes when the record comes due"
)]
#[tokio::test]
async fn a_parked_long_poll_wakes_when_the_record_comes_due(
    delivery: DeliveryScenario,
    mode: String,
    topic: String,
    held: bool,
) {
    let _ = (mode, topic, held);
    delivery.shutdown().await;
}
