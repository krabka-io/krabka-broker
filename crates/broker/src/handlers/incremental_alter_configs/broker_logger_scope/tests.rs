//! What a `BROKER_LOGGER` alter accepts, what it refuses, and what it does to
//! the live filter.

use {
    super::handle_broker_logger_scoped,
    crate::{codes, config::DEFAULT_LOG_FILTER},
    assert2::{assert, check},
    krabka_protocol::owned::{
        incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
    krabka_telemetry::{LogLevel, LogLevelController, LogLevelFilter},
};

/// This node's id in every case below.
const NODE_ID: i32 = 3;

/// One `AlterableConfig`, spelled the way the wire carries it.
fn config(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
    AlterableConfig {
        name: name.to_owned(),
        config_operation: operation,
        value: value.map(str::to_owned),
        ..Default::default()
    }
}

/// The response of one alter beside the controller it changed, so a test can
/// read the levels back.
struct Applied {
    response: AlterConfigsResourceResponse,
    levels: LogLevelController,
}

/// Apply `configs` to a fresh controller seeded with `spec` and return the
/// response beside the controller, so a test can read the levels back.
fn apply(
    spec: &str,
    resource_name: &str,
    configs: Vec<AlterableConfig>,
    validate_only: bool,
) -> Applied {
    let (levels, _filter) = LogLevelController::new(spec);
    let resource = AlterConfigsResource {
        resource_type: 8,
        resource_name: resource_name.to_owned(),
        configs,
        ..Default::default()
    };
    let mut out = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    };
    handle_broker_logger_scoped(&resource, NODE_ID, &levels, validate_only, &mut out);
    Applied {
        response: out,
        levels,
    }
}

#[test]
fn a_set_retargets_the_named_logger() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "info,krabka_broker=info",
        "3",
        vec![config("krabka_broker", 0, Some("DEBUG"))],
        false,
    );

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("krabka_broker") == Some(LogLevel::Debug));
}

#[test]
fn a_delete_puts_the_logger_back_on_the_root_level() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "warn,krabka_broker=trace",
        "3",
        vec![config("krabka_broker", 1, None)],
        false,
    );

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("krabka_broker") == Some(LogLevel::Warn));
}

#[test]
fn a_set_of_the_root_logger_moves_every_unpinned_target() {
    let Applied {
        response: out,
        levels,
    } = apply("info", "3", vec![config("root", 0, Some("TRACE"))], false);

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("root") == Some(LogLevel::Trace));
}

#[test]
fn validate_only_reports_the_verdict_and_changes_nothing() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "info,krabka_broker=info",
        "3",
        vec![config("krabka_broker", 0, Some("TRACE"))],
        true,
    );

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("krabka_broker") == Some(LogLevel::Info));
}

#[test]
fn one_bad_config_leaves_the_whole_resource_unapplied() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "info,krabka_broker=info,krabka_log=info",
        "3",
        vec![
            config("krabka_broker", 0, Some("DEBUG")),
            config("krabka_log", 0, Some("VERBOSE")),
        ],
        false,
    );

    assert!(out.error_code == codes::INVALID_CONFIG);
    assert!(levels.level("krabka_broker") == Some(LogLevel::Info));
}

#[derive(Default)]
struct LoggerAlterationScenario {
    levels: Option<(LogLevelController, LogLevelFilter)>,
    resource: Option<AlterConfigsResource>,
    response: Option<AlterConfigsResourceResponse>,
}
#[rstest::fixture]
fn logger_alteration() -> LoggerAlterationScenario {
    let levels = None;
    LoggerAlterationScenario {
        levels,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("node 3 has root and krabka_broker loggers at INFO")]
fn configured_broker_loggers(logger_alteration: &mut LoggerAlterationScenario) {
    logger_alteration.levels = Some(LogLevelController::new("info,krabka_broker=info"));
}
#[rstest_bdd_macros::given(
    "the logger alteration names node {node} logger {logger} operation {operation} value {value}"
)]
fn requested_logger_alteration(
    logger_alteration: &mut LoggerAlterationScenario,
    node: String,
    logger: String,
    operation: i8,
    value: String,
) {
    logger_alteration.resource = Some(AlterConfigsResource {
        resource_type: 8,
        resource_name: node,
        configs: vec![config(
            &logger,
            operation,
            if value == "null" { None } else { Some(&value) },
        )],
        ..Default::default()
    });
}
#[rstest_bdd_macros::when("the live logger controller applies the alteration")]
fn apply_refused_logger_alteration(logger_alteration: &mut LoggerAlterationScenario) {
    let resource = logger_alteration.resource.as_ref().unwrap();
    let mut response = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    };
    handle_broker_logger_scoped(
        resource,
        NODE_ID,
        &logger_alteration.levels.as_ref().unwrap().0,
        false,
        &mut response,
    );
    logger_alteration.response = Some(response);
}
#[rstest_bdd_macros::then(
    "the logger alteration returns code {code} message {message} and retains INFO"
)]
fn check_refused_logger_alteration(
    logger_alteration: &LoggerAlterationScenario,
    code: i16,
    message: String,
) {
    let response = logger_alteration.response.as_ref().unwrap();
    check!(response.error_code == code);
    check!(response.error_message.as_deref() == Some(message.as_str()));
    check!(
        logger_alteration
            .levels
            .as_ref()
            .unwrap()
            .0
            .level("krabka_broker")
            == Some(LogLevel::Info)
    );
}
rstest_bdd_macros::scenarios!("tests/features/handler_broker_logger_refusals",fixtures=[logger_alteration:LoggerAlterationScenario]);

#[test]
fn the_default_filter_names_the_loggers_an_operator_reaches_for() {
    // A `tracing` callsite registers under its module path, never under the
    // bare crate root, so the crate-root names are loggers only because the
    // starting spec seeds them. If that seed stops naming them, the headline
    // `--add-config krabka_broker=DEBUG` is refused on a real broker.
    for target in ["krabka_broker", "krabka_log"] {
        let Applied {
            response: out,
            levels,
        } = apply(
            DEFAULT_LOG_FILTER,
            "3",
            vec![config(target, 0, Some("DEBUG"))],
            false,
        );
        check!(out.error_code == codes::NONE, "{target}: {out:?}");
        check!(levels.level(target) == Some(LogLevel::Debug), "{target}");
    }
}

#[test]
fn a_lowercase_level_is_not_a_level() {
    // Kafka tests the value against a set of uppercase names, so `debug` is
    // rejected there and must be rejected here.
    let Applied { response: out, .. } = apply(
        "info,krabka_broker=info",
        "3",
        vec![config("krabka_broker", 0, Some("debug"))],
        false,
    );
    assert!(out.error_code == codes::INVALID_CONFIG);
}
