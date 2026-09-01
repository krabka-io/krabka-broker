//! Behavior of the live log filter: what it lists, what it retargets, and
//! what a retarget does to the events a subscriber actually sees.

use std::sync::{Arc, Mutex};

use assert2::{assert, check};
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt as _},
};

use super::{LogLevel, LogLevelController, ROOT_LOGGER};

/// The `target: level` of every event a subscriber let through.
type Captured = Arc<Mutex<Vec<String>>>;

/// A layer that records the events its filter admits.
struct CaptureLayer(Captured);

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _cx: Context<'_, S>) {
        let meta = event.metadata();
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!("{}:{}", meta.target(), meta.level()));
    }
}

/// A dispatcher that captures through `controller`'s filter.
///
/// The `Dispatch` registers itself with `tracing` when it is built and stays
/// registered while it is alive, so a rebuild of the interest cache consults
/// it even between the `with_default` calls that make it current.
fn capturing_dispatch(controller: &LogLevelController) -> (tracing::Dispatch, Captured) {
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(CaptureLayer(Arc::clone(&captured)).with_filter(controller.filter()));
    (tracing::Dispatch::new(subscriber), captured)
}

/// Everything the capture holds, drained.
fn drain(captured: &Captured) -> Vec<String> {
    std::mem::take(
        &mut *captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

#[test]
fn kafka_level_names_round_trip() {
    for level in [
        LogLevel::Fatal,
        LogLevel::Error,
        LogLevel::Warn,
        LogLevel::Info,
        LogLevel::Debug,
        LogLevel::Trace,
    ] {
        check!(LogLevel::from_kafka_name(level.kafka_name()) == Some(level));
    }
    // Kafka matches its value against a set of uppercase names, so anything
    // else is not a level.
    check!(LogLevel::from_kafka_name("info") == None);
    check!(LogLevel::from_kafka_name("OFF") == None);
    check!(LogLevel::from_kafka_name("") == None);
}

#[test]
fn directives_in_the_spec_seed_the_logger_list() {
    let (controller, _filter) = LogLevelController::new("warn,krabka_broker=info,krabka_log=debug");

    let loggers = controller.loggers();
    assert!(loggers.get(ROOT_LOGGER) == Some(&LogLevel::Warn));
    assert!(loggers.get("krabka_broker") == Some(&LogLevel::Info));
    assert!(loggers.get("krabka_log") == Some(&LogLevel::Debug));
    // A target no directive names and no callsite has registered under is
    // not a logger yet.
    check!(controller.contains("never_seen_target") == false);
    check!(controller.level("never_seen_target") == None);
}

#[test]
fn a_spec_with_no_bare_level_holds_the_root_at_fatal() {
    // `EnvFilter` disables every target its directives do not name, and the
    // Kafka name for a level nothing krabka emits reaches is FATAL.
    let (controller, _filter) = LogLevelController::new("krabka_broker=info");
    assert!(controller.level(ROOT_LOGGER) == Some(LogLevel::Fatal));
}

#[test]
fn a_target_level_covers_the_targets_below_it() {
    let (controller, filter) = LogLevelController::new("info");
    let (dispatch, _captured) = capturing_dispatch(&controller);
    drop(filter);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::info!(target: "seed_parent::child", "register the callsite");
    });
    assert!(controller.level("seed_parent::child") == Some(LogLevel::Info));

    controller.set_level("seed_parent", LogLevel::Trace);
    assert!(controller.level("seed_parent::child") == Some(LogLevel::Trace));
    assert!(controller.level(ROOT_LOGGER) == Some(LogLevel::Info));
}

#[test]
fn raising_a_level_enables_a_callsite_the_old_filter_had_disabled() {
    let (controller, _filter) = LogLevelController::new("info");
    let (dispatch, captured) = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "raise_target", "before");
    });
    assert!(drain(&captured) == Vec::<String>::new());

    controller.set_level("raise_target", LogLevel::Debug);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "raise_target", "after");
    });
    assert!(drain(&captured) == vec!["raise_target:DEBUG".to_string()]);

    // The describe surface reports the same level the events prove.
    assert!(controller.level("raise_target") == Some(LogLevel::Debug));
}

#[test]
fn fatal_silences_a_target_and_reads_back_as_fatal() {
    let (controller, _filter) = LogLevelController::new("trace");
    let (dispatch, captured) = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::error!(target: "silence_target", "before");
    });
    assert!(drain(&captured) == vec!["silence_target:ERROR".to_string()]);

    controller.set_level("silence_target", LogLevel::Fatal);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::error!(target: "silence_target", "after");
    });
    assert!(drain(&captured) == Vec::<String>::new());
    assert!(controller.level("silence_target") == Some(LogLevel::Fatal));
}

#[test]
fn clearing_a_level_puts_a_target_back_on_the_root() {
    let (controller, _filter) = LogLevelController::new("info,clear_target=debug");
    let (dispatch, captured) = capturing_dispatch(&controller);

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "clear_target", "before");
    });
    assert!(drain(&captured) == vec!["clear_target:DEBUG".to_string()]);

    assert!(controller.clear_level("clear_target"));

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!(target: "clear_target", "after");
    });
    assert!(drain(&captured) == Vec::<String>::new());
    // The target is still a logger — a callsite has registered under it —
    // and it now reads the root level.
    assert!(controller.level("clear_target") == Some(LogLevel::Info));
    // Clearing it twice reports that there was nothing left to clear.
    check!(controller.clear_level("clear_target") == false);
}

#[test]
fn the_root_logger_has_no_level_of_its_own_to_clear() {
    let (controller, _filter) = LogLevelController::new("info");
    check!(controller.clear_level(ROOT_LOGGER) == false);
    check!(controller.level(ROOT_LOGGER) == Some(LogLevel::Info));

    controller.set_level(ROOT_LOGGER, LogLevel::Trace);
    check!(controller.level(ROOT_LOGGER) == Some(LogLevel::Trace));
}

#[test]
fn a_span_directive_survives_a_level_change() {
    // `[span]=level` is not a logger. It is carried verbatim, so a change to
    // an unrelated level must not drop it.
    let (controller, _filter) = LogLevelController::new("off,[carried_span]=debug");
    let (dispatch, captured) = capturing_dispatch(&controller);

    check!(controller.contains("[carried_span]=debug") == false);
    check!(controller.loggers().keys().all(|name| !name.contains('[')));

    controller.set_level("unrelated_target", LogLevel::Warn);

    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("carried_span");
        let _entered = span.enter();
        tracing::debug!(target: "inside_span_target", "still on");
    });
    assert!(drain(&captured).contains(&"inside_span_target:DEBUG".to_string()));
}
