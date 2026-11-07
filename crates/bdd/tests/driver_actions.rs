#![cfg(test)]
//! [`Driver`] and [`Action`] over a tiny in-memory topic registry: the steps
//! make testing decisions, the actions name the calls, and `act` and
//! `attempt` run each action in a traced `bdd.action` span.
//!
//! The tests at the bottom install a capturing `tracing` subscriber and check
//! that span: entered, named after the action, with its elapsed time and its
//! outcome recorded.

use {
    assert2::assert,
    futures_util::FutureExt,
    krabka_bdd::{ACTION_SPAN, Action, Attempt, Driver, ScenarioContext, act, attempt},
    rstest::rstest,
    rstest_bdd::panic_message,
    std::{
        collections::{BTreeMap, btree_map::Entry},
        fmt,
        future::{Future, ready},
        panic::AssertUnwindSafe,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    },
    tokio::time::sleep,
    tracing::{
        Event, Metadata, Subscriber,
        field::{Field, Visit},
        instrument::WithSubscriber,
        span::{Attributes, Id, Record},
    },
};

// ---- Infrastructure and driver ----------------------------------------------

/// The registry's topics: name to partition count. The infrastructure layer.
#[derive(Debug, Default)]
struct TopicRegistry {
    topics: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RegistryError {
    TopicAlreadyExists(String),
    UnknownTopic(String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TopicAlreadyExists(name) => write!(formatter, "the topic {name} already exists"),
            Self::UnknownTopic(name) => write!(formatter, "the topic {name} does not exist"),
        }
    }
}

impl Driver for TopicRegistry {
    type Error = RegistryError;
}

/// Creates a topic. The driver layer: what call to make, in domain terms.
struct CreateTopic {
    name: String,
    partitions: u32,
}

impl Action<TopicRegistry> for CreateTopic {
    type Error = RegistryError;
    type Output = ();

    fn name(&self) -> &'static str { "create topic" }

    fn run(self, registry: &mut TopicRegistry) -> impl Future<Output = Result<(), RegistryError>> {
        let result = match registry.topics.entry(self.name) {
            Entry::Occupied(topic) => Err(RegistryError::TopicAlreadyExists(topic.key().clone())),
            Entry::Vacant(topic) => {
                topic.insert(self.partitions);
                Ok(())
            }
        };
        ready(result)
    }
}

/// Deletes a topic and returns the partition count it had.
struct DeleteTopic {
    name: String,
}

impl Action<TopicRegistry> for DeleteTopic {
    type Error = RegistryError;
    type Output = u32;

    fn name(&self) -> &'static str { "delete topic" }

    fn run(self, registry: &mut TopicRegistry) -> impl Future<Output = Result<u32, RegistryError>> {
        ready(
            registry
                .topics
                .remove(&self.name)
                .ok_or(RegistryError::UnknownTopic(self.name)),
        )
    }
}

/// Waits before it succeeds, so its span has a measurable elapsed time.
struct SlowAction(Duration);

impl Action<TopicRegistry> for SlowAction {
    type Error = RegistryError;
    type Output = ();

    fn name(&self) -> &'static str { "wait" }

    async fn run(self, _: &mut TopicRegistry) -> Result<(), RegistryError> {
        sleep(self.0).await;
        Ok(())
    }
}

// ---- Step definitions: testing decisions only -------------------------------

type Deletion = Attempt<u32, RegistryError>;

#[derive(ScenarioContext)]
struct RegistryScenario {
    registry: TopicRegistry,
}

#[rstest::fixture]
fn registry_scenario() -> RegistryScenario {
    RegistryScenario {
        ..RegistryScenario::default()
    }
}

#[rstest::fixture]
fn deletion() -> Deletion {
    let deletion: Deletion = Attempt::NotExecuted;
    deletion
}

#[krabka_bdd::given("a topic named {name} has been created")]
async fn a_topic_has_been_created(registry_scenario: &mut RegistryScenario, name: String) {
    let action = CreateTopic {
        name,
        partitions: 1,
    };
    act(&mut registry_scenario.registry, action).await;
}

#[krabka_bdd::when("an operator creates the topic {name} with {partitions} partitions")]
async fn an_operator_creates(
    registry_scenario: &mut RegistryScenario,
    name: String,
    partitions: u32,
) {
    act(&mut registry_scenario.registry, CreateTopic {
        name,
        partitions,
    })
    .await;
}

#[krabka_bdd::when("an operator deletes the topic {name}")]
async fn an_operator_deletes(registry_scenario: &mut RegistryScenario, name: String) {
    act(&mut registry_scenario.registry, DeleteTopic { name }).await;
}

#[krabka_bdd::when("an operator attempts to delete the topic {name}")]
async fn an_operator_attempts_to_delete(
    registry_scenario: &mut RegistryScenario,
    name: String,
) -> Attempt<u32, RegistryError> {
    attempt(&mut registry_scenario.registry, DeleteTopic { name }).await
}

#[krabka_bdd::then("the registry lists the topic {name} with {partitions} partitions")]
fn the_registry_lists(registry_scenario: &RegistryScenario, name: String, partitions: u32) {
    assert!(registry_scenario.registry.topics == BTreeMap::from([(name, partitions)]));
}

#[krabka_bdd::then("the registry lists no topics")]
fn the_registry_lists_no_topics(registry_scenario: &RegistryScenario) {
    assert!(registry_scenario.registry.topics.is_empty());
}

#[krabka_bdd::then("the deletion fails because the topic {name} does not exist")]
fn the_deletion_fails(deletion: &Deletion, name: String) {
    assert!(*deletion == Attempt::Failed(RegistryError::UnknownTopic(name)));
}

krabka_bdd::scenarios!(
    "tests/features/driver_actions",
    fixtures = [registry_scenario: RegistryScenario, deletion: Deletion],
);

// ---- The action span --------------------------------------------------------

/// What a span recorded: its name, its fields and whether it was entered.
#[derive(Debug, Clone, Default, PartialEq)]
struct CapturedSpan {
    span: &'static str,
    fields: BTreeMap<String, Value>,
    entered: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Text(String),
    Number(f64),
}

/// A subscriber that keeps every span it sees.
#[derive(Clone, Default)]
struct Capture {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
    next: Arc<AtomicU64>,
}

impl Capture {
    fn spans(&self) -> Vec<CapturedSpan> { self.spans.lock().unwrap().clone() }

    fn with_span(&self, id: &Id, update: impl FnOnce(&mut CapturedSpan)) {
        let index = usize::try_from(id.into_u64() - 1).unwrap();
        update(&mut self.spans.lock().unwrap()[index]);
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, Value>);

impl Visit for Fields<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_owned(), Value::Number(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0
            .insert(field.name().to_owned(), Value::Text(value.to_owned()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), Value::Text(format!("{value:?}")));
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool { true }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut span = CapturedSpan {
            span: attributes.metadata().name(),
            ..CapturedSpan::default()
        };
        attributes.record(&mut Fields(&mut span.fields));
        self.spans.lock().unwrap().push(span);
        Id::from_u64(self.next.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn record(&self, id: &Id, values: &Record<'_>) {
        self.with_span(id, |span| values.record(&mut Fields(&mut span.fields)));
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, id: &Id) { self.with_span(id, |span| span.entered = true); }

    fn exit(&self, _: &Id) {}
}

/// `span` with its elapsed time taken out, and that time.
fn without_elapsed(mut span: CapturedSpan) -> (CapturedSpan, f64) {
    let Some(Value::Number(elapsed_ms)) = span.fields.remove("elapsed_ms") else {
        panic!("the span recorded no elapsed_ms: {span:?}");
    };
    (span, elapsed_ms)
}

fn action_span(name: &str, outcome: &str) -> CapturedSpan {
    CapturedSpan {
        span: ACTION_SPAN,
        fields: BTreeMap::from([
            ("name".to_owned(), Value::Text(name.to_owned())),
            ("outcome".to_owned(), Value::Text(outcome.to_owned())),
        ]),
        entered: true,
    }
}

fn registry_with_orders() -> TopicRegistry {
    TopicRegistry {
        topics: BTreeMap::from([("orders".to_owned(), 3)]),
    }
}

#[rstest]
#[case::act_create(true, "create topic", "succeeded")]
#[case::attempt_delete_unknown(false, "delete topic", "failed")]
#[tokio::test]
async fn an_action_runs_in_a_span_named_after_it(
    #[case] create: bool,
    #[case] name: &str,
    #[case] outcome: &str,
) {
    let capture = Capture::default();
    let mut registry = registry_with_orders();
    let run = async {
        if create {
            let action = CreateTopic {
                name: "payments".to_owned(),
                partitions: 1,
            };
            act(&mut registry, action).await;
        } else {
            let deleted = attempt(&mut registry, DeleteTopic {
                name: "refunds".to_owned(),
            })
            .await;
            assert!(deleted == Attempt::Failed(RegistryError::UnknownTopic("refunds".to_owned())));
        }
    };
    run.with_subscriber(capture.clone()).await;

    let spans: Vec<_> = capture.spans().into_iter().map(without_elapsed).collect();
    assert!(spans.len() == 1);
    let (span, elapsed_ms) = spans[0].clone();
    assert!(span == action_span(name, outcome));
    assert!(elapsed_ms >= 0.0);
}

#[tokio::test]
async fn the_span_records_the_time_the_action_took() {
    let capture = Capture::default();
    let mut registry = TopicRegistry::default();
    act(&mut registry, SlowAction(Duration::from_millis(20)))
        .with_subscriber(capture.clone())
        .await;

    let (span, elapsed_ms) = without_elapsed(capture.spans().remove(0));
    assert!(span == action_span("wait", "succeeded"));
    assert!(elapsed_ms >= 20.0);
}

#[tokio::test]
async fn a_failing_act_panics_naming_the_action_and_the_error() {
    let mut registry = TopicRegistry::default();
    let delete = act(&mut registry, DeleteTopic {
        name: "payments".to_owned(),
    });
    let payload = AssertUnwindSafe(delete).catch_unwind().await.unwrap_err();
    assert!(
        panic_message(payload.as_ref())
            == "the action `delete topic` failed: the topic payments does not exist"
    );
}

#[rstest]
#[case::delete_known("orders", Attempt::Succeeded(3))]
#[case::delete_unknown("payments", Attempt::Failed(RegistryError::UnknownTopic("payments".to_owned())))]
#[tokio::test]
async fn attempt_records_the_result(#[case] name: &str, #[case] expected: Deletion) {
    let mut registry = registry_with_orders();
    let deleted = attempt(&mut registry, DeleteTopic {
        name: name.to_owned(),
    })
    .await;
    assert!(deleted == expected);
}
