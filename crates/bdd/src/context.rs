//! A scenario context: the fixture struct that holds one scenario's state.
//!
//! `#[derive(krabka_bdd::ScenarioContext)]` is the Shareable Scenario Context
//! pattern. Steps that cooperate on one scenario share one context struct,
//! which rstest builds fresh for each scenario and hands every step by name.
//! A field marked `#[introduced_by("Given ...")]` is a [`Given`] slot; the
//! derive gives it three methods:
//!
//! - `the_<field>()` and `the_<field>_mut()` read the slot, and panic, through
//!   [`Given`], naming the field and the step that introduces it when no step
//!   has set it;
//! - `set_<field>(value)` stores a value.
//!
//! The derive also implements `Default`, which builds each slot as
//! `Given::new("<field>", "<step>")` and every other field, such as a
//! [`Ref`](crate::Ref) registry, a driver or a [`StubClock`](crate::StubClock),
//! with that field's own `Default`. A `Given` field without
//! `#[introduced_by]` is a compile error. The step must start with its
//! keyword: `Given`, `When` or `Then`.
//!
//! The scenario's `#[rstest::fixture]` returns the default. Write its body as
//! the struct update `Ctx { ..Ctx::default() }` over several lines, as in the
//! example: rstest wraps a fixture's body in a second block, and rustc's
//! `unused_braces` lint flags that block when the body is a one-line
//! expression such as `Ctx::default()`.
//!
//! # One context per scenario
//!
//! Every scenario gets a fresh context, so scenarios do not depend on their
//! order and can run in parallel. Do not hold scenario state in a `static`,
//! a `thread_local!`, a `OnceLock` or a lazy global: libtest runs scenarios on
//! a thread pool in any order, and state that one scenario leaves behind
//! changes the next one's outcome. Anything a scenario needs lives in its
//! context, or in something the context owns.
//!
//! # Examples
//!
//! ```
//! use {
//!     assert2::assert,
//!     krabka_bdd::{Given, ScenarioContext},
//! };
//!
//! #[derive(ScenarioContext)]
//! struct TopicScenario {
//!     #[introduced_by("Given a topic with {n} partitions")]
//!     partitions: Given<u32>,
//! }
//!
//! // The fixture that `scenarios!(.., fixtures = [topic_scenario: TopicScenario])` names.
//! #[rstest::fixture]
//! fn topic_scenario() -> TopicScenario {
//!     TopicScenario {
//!         ..TopicScenario::default()
//!     }
//! }
//!
//! let mut scenario = TopicScenario::default();
//! scenario.set_partitions(3);
//! *scenario.the_partitions_mut() += 1;
//! assert!(*scenario.the_partitions() == 4);
//! ```

use crate::Given;

/// The [`Given`] slot behind a `ScenarioContext` field, which names the
/// slot's value type for the generated accessors.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `krabka_bdd::Given<T>` slot",
    label = "a field with `#[introduced_by]` must be a `Given<T>`",
    note = "remove `#[introduced_by]` from a field that is not scenario state set by a step"
)]
pub trait ContextSlot {
    /// The value the slot holds.
    type Value;

    /// The slot.
    fn slot(&self) -> &Given<Self::Value>;

    /// The slot, mutably.
    fn slot_mut(&mut self) -> &mut Given<Self::Value>;
}

impl<T> ContextSlot for Given<T> {
    type Value = T;

    fn slot(&self) -> &Self { self }

    fn slot_mut(&mut self) -> &mut Self { self }
}
