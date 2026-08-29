//! KFC-1 deliver-at-time visibility, end to end against an in-process broker.
//!
//! Every case here drives the real Kafka wire path — `CreateTopics`,
//! `Produce`, `Fetch`, `ListOffsets` — against a live broker, and every case
//! runs twice: once on a topic with `delivery.mode=scheduled` and once on a
//! topic with `delivery.mode=immediate`. The immediate half is the control. It
//! is what shows that a scheduled topic's behaviour is the configuration and
//! not the code path every topic now takes.
//!
//! # Real time, not a mock clock
//!
//! KFC-1's test plan asks for a mock clock, and the delivery unit tests have
//! one. An integration test cannot reach it. `DeliveryHandles::now_ms` reads
//! the clock its handles were built with, `DeliveryHandles::new` in the
//! partition-spawn path builds them on the system clock, and `BrokerConfig`
//! carries no seam to change that. So these cases run on wall-clock time.
//!
//! That is only honest if no assertion is a race, so none of them is one. A
//! record that has to activate mid-test is stamped `ACTIVATION_DELAY_MS`
//! ahead, and the read that must find it still pending is taken immediately
//! after the produce and then *checked against the clock*: the case fails if
//! that read finished after the delivery time, rather than passing on a read
//! that proved nothing. A record that has to stay pending for a whole case is
//! stamped `PENDING_HORIZON_MS` ahead, which no test run approaches. And
//! nothing waits on a fixed sleep: a case that expects a record to appear polls
//! for it and reports the clock reading of the poll that found it, so "never
//! early" is an assertion on that reading and not on how long the test slept.
//!
//! # Layout
//!
//! `dat_fixtures` carries the two delivery modes, the timing constants, and the
//! `Visible` snapshot the cases compare; `dat_wire` carries the requests that
//! produce one. `dat_activation` covers what a pending record does before its
//! delivery time, `dat_watermark` covers where the watermark sits while it is
//! pending, and `dat_restart` covers the watermark a reopened log rederives.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `deliver_at_time/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "deliver_at_time/dat_activation.rs"]
mod dat_activation;
#[path = "deliver_at_time/dat_fixtures.rs"]
mod dat_fixtures;
#[path = "deliver_at_time/dat_restart.rs"]
mod dat_restart;
#[path = "deliver_at_time/dat_watermark.rs"]
mod dat_watermark;
#[path = "deliver_at_time/dat_wire.rs"]
mod dat_wire;
