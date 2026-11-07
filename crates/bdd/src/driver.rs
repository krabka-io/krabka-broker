//! Test Driver and Test Action: the layer between a step and the system.
//!
//! # Three layers
//!
//! A scenario's automation has three layers, and each holds one kind of
//! decision:
//!
//! 1. **Step definitions** hold testing decisions only: which value the
//!    scenario introduces, which action it takes, and which outcome it
//!    checks. A step reads like its Gherkin line and does not know how a
//!    topic is created.
//! 2. **A driver** holds what call to make, named in domain terms:
//!    `create_topic`, `delete_topic`, `fetch_from`. It is the one place that
//!    knows which request, which API or which in-memory structure stands
//!    behind a domain action, so a change of mechanism changes the driver and
//!    no step. A driver implements [`Driver`]; each thing a step can do to it
//!    is a small [`Action`] value.
//! 3. **Infrastructure** holds the plumbing: connections, runtimes,
//!    containers, ports, clients and the stub dependencies such as
//!    [`StubClock`](crate::StubClock). The driver owns it, or borrows it from
//!    the scenario context, and steps never touch it.
//!
//! [`act`] runs an action and panics if it fails, the destructive form most
//! steps want; [`attempt`] runs one and returns an [`Attempt`] for an
//! "attempts to" step. Both run the action inside a `bdd.action` tracing span
//! that carries the action's `name`, its `elapsed_ms` and its `outcome`
//! (`succeeded` or `failed`), so a slow or failing scenario shows which action
//! took the time.
//!
//! # Defaults: Domain Default, Default Instance and Object Mother
//!
//! A step overrides only what the scenario's rule needs; everything else
//! comes from a valid default. Keep those defaults in one place per domain
//! area, as `bon` builders:
//!
//! - One `defaults` module per domain area in a crate's test support, such as
//!   `test_support::defaults::topics`.
//! - It exposes a Default Instance as a builder with valid domain defaults:
//!   `fn a_topic() -> TopicBuilder`, so a step writes
//!   `a_topic().partitions(3).call()` and states only the partition count.
//!   The defaults are Domain Defaults: values a Kafka operator would accept
//!   without comment, such as one partition and replication factor 1, not
//!   zeros.
//! - Named Object Mothers return a builder preset for a recurring kind of
//!   entity, such as `fn a_compacted_topic() -> TopicBuilder`, so a scenario
//!   about compaction does not restate the cleanup policy.
//! - A step starts from a default or a mother, then sets the fields its rule
//!   is about. A Validation Tester starts from the same default, applies the
//!   scenario's overrides with `#[derive(krabka_bdd::Overrides)]`, and runs
//!   the validation, so each invalid example differs from a valid one only in
//!   the values it names.
//!
//! ```ignore
//! pub mod defaults {
//!     use bon::builder;
//!
//!     #[builder]
//!     pub fn topic(
//!         #[builder(default = "orders".to_owned())] name: String,
//!         #[builder(default = 1)] partitions: u32,
//!         #[builder(default = 1)] replication_factor: u16,
//!         #[builder(default)] compacted: bool,
//!     ) -> Topic {
//!         Topic { name, partitions, replication_factor, compacted }
//!     }
//!
//!     /// A Default Instance: valid in every field.
//!     pub fn a_topic() -> TopicBuilder { topic() }
//!
//!     /// An Object Mother for scenarios about compaction.
//!     pub fn a_compacted_topic() -> TopicBuilder<topic_builder::SetCompacted> {
//!         topic().compacted(true)
//!     }
//! }
//! ```

use {
    crate::Attempt,
    std::{fmt::Display, future::Future, time::Instant},
    tracing::{Instrument, field},
};

/// The span name every action runs in.
pub const ACTION_SPAN: &str = "bdd.action";

/// What a step drives: the domain-named calls a scenario can make.
///
/// The trait is a marker for the middle layer. A driver names its calls in
/// domain terms and hides the infrastructure behind them; see the [module
/// docs](self).
pub trait Driver {
    /// The error the driver's calls fail with.
    type Error;
}

/// One thing a step does to a driver `D`, such as creating a topic.
///
/// An action is a value: its fields are the call's arguments, so a step builds
/// it from the scenario's values and hands it to [`act`] or [`attempt`].
pub trait Action<D: ?Sized> {
    /// What the action returns when it succeeds.
    type Output;
    /// What the action fails with.
    type Error;

    /// The action's name in domain terms, such as `create topic`, for the
    /// tracing span and the panic message.
    fn name(&self) -> &'static str;

    /// Runs the action against `driver`.
    fn run(self, driver: &mut D) -> impl Future<Output = Result<Self::Output, Self::Error>>;
}

/// Runs `action` against `driver` and returns its output, panicking if it
/// fails: the destructive form for a step whose action must succeed.
///
/// The action runs inside a [`ACTION_SPAN`] span; see the [module
/// docs](self).
///
/// # Panics
///
/// Panics with ``the action `create topic` failed: <error>`` when the
/// action fails, naming the action and the error's `Display`.
///
/// # Examples
///
/// ```
/// use {
///     assert2::assert,
///     krabka_bdd::{Action, Driver, act},
/// };
///
/// #[derive(Default)]
/// struct Counter(u32);
///
/// impl Driver for Counter {
///     type Error = String;
/// }
///
/// struct Increment;
///
/// impl Action<Counter> for Increment {
///     type Error = String;
///     type Output = u32;
///
///     fn name(&self) -> &'static str { "increment" }
///
///     async fn run(self, counter: &mut Counter) -> Result<u32, String> {
///         counter.0 += 1;
///         Ok(counter.0)
///     }
/// }
///
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let mut counter = Counter::default();
/// assert!(act(&mut counter, Increment).await == 1);
/// # });
/// ```
pub async fn act<D, A>(driver: &mut D, action: A) -> A::Output
where
    D: Driver + ?Sized,
    A: Action<D>,
    A::Error: Display,
{
    let name = action.name();
    match run(driver, action).await {
        Ok(output) => output,
        Err(error) => panic!("the action `{name}` failed: {error}"),
    }
}

/// Runs `action` against `driver` and records its result, for a step that
/// "attempts to" do something whose failure a later step checks.
///
/// The action runs inside a [`ACTION_SPAN`] span, as with [`act`].
pub async fn attempt<D, A>(driver: &mut D, action: A) -> Attempt<A::Output, A::Error>
where
    D: Driver + ?Sized,
    A: Action<D>,
{
    Attempt::from_result(run(driver, action).await)
}

/// Runs `action` inside its span and records the elapsed time and outcome on
/// the span when it finishes.
async fn run<D, A>(driver: &mut D, action: A) -> Result<A::Output, A::Error>
where
    D: ?Sized,
    A: Action<D>,
{
    let span = tracing::info_span!(
        "bdd.action",
        name = action.name(),
        elapsed_ms = field::Empty,
        outcome = field::Empty,
    );
    let started = Instant::now();
    let result = action.run(driver).instrument(span.clone()).await;
    span.record("elapsed_ms", started.elapsed().as_secs_f64() * 1000.0);
    span.record(
        "outcome",
        if result.is_ok() {
            "succeeded"
        } else {
            "failed"
        },
    );
    result
}
