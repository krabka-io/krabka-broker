//! `fetch.min.bytes` is a floor, at the wire.
//!
//! A Fetch whose `min_bytes` is above everything the log holds must be held
//! until `max_wait_ms` expires and must then answer with what is there. A
//! broker that treats the field as a hint answers on the first append instead,
//! which is a partial response per round trip for every consumer that raised
//! the floor -- Streams and Connect do.
//!
//! The same exchange runs against a pinned Apache Kafka broker in
//! `fetch_min_bytes_jvm`, which is where the claim that this is Kafka's
//! behavior and not just krabka's is checked. That half needs Docker and is
//! `#[ignore]`d; this one is hermetic.
//!
//! Cargo compiles this file as its own test binary, so the shared exchange
//! carries an explicit `#[path]` onto the sibling `fetch_min_bytes/`
//! directory; `support` is a `tests/<name>/mod.rs` helper, which the crate-root
//! rule already resolves.
#![cfg(test)]

mod support;
#[path = "fetch_min_bytes/wire.rs"]
mod wire;

use {
    crate::{
        support::InProcess,
        wire::{FetchFacts, SeededTopic},
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::time::Duration,
};

#[derive(Default)]
struct FetchFloor {
    broker: Option<InProcess>,
    topic: Option<SeededTopic>,
    observed: Option<(Duration, FetchFacts)>,
}

#[fixture]
fn floor() -> FetchFloor {
    // Start this scenario with fresh inputs and observations.

    FetchFloor {
        broker: None,
        topic: None,
        observed: None,
    }
}

#[given("a broker topic {topic} containing the first small record")]
async fn seeded(floor: &mut FetchFloor, topic: String) {
    let broker = support::start().await;
    floor.topic = Some(SeededTopic::create(&broker.broker.listen_addr().to_string(), &topic).await);
    floor.broker = Some(broker);
}

#[when(
    "Fetch requests at least {minimum} bytes for {wait} ms while a second small record arrives after {delay} ms"
)]
async fn fetch(floor: &mut FetchFloor, minimum: i32, wait: i32, delay: u64) {
    floor.observed = Some(
        floor
            .topic
            .as_ref()
            .unwrap()
            .fetch(minimum, wait, delay)
            .await,
    );
}

#[then(
    "the answer waits at least {minimum_wait} ms and returns error {error}, watermark {watermark}, and batch offsets {offsets}"
)]
fn answer(floor: &FetchFloor, minimum_wait: u64, error: i16, watermark: i64, offsets: String) {
    let (held, facts) = floor.observed.as_ref().unwrap();
    assert!(*held >= Duration::from_millis(minimum_wait));
    assert!(
        *facts
            == FetchFacts {
                error_code: error,
                high_watermark: watermark,
                base_offsets: offsets
                    .split(',')
                    .map(|value| value.parse().unwrap())
                    .collect(),
            }
    );
}

#[scenario(
    path = "tests/features/migrated/tests/fetch_min_bytes.feature",
    name = "a fetch below its min bytes is held until max wait and then answers"
)]
#[tokio::test]
async fn a_fetch_below_its_min_bytes_is_held_until_max_wait_and_then_answers(floor: FetchFloor) {
    drop(floor.topic);
    floor.broker.unwrap().broker.shutdown().await;
}
