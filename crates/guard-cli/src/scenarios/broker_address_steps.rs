//! The steps that name the broker on the command line or in the operator's
//! environment.

use {
    super::{driver, world::GuardScenario},
    assert2::assert,
    krabka_cli_connection::BOOTSTRAP_SERVER_ENV,
    rstest_bdd_macros::{given, when},
};

/// A bootstrap address where no broker listens: nothing binds port 1.
const UNREACHABLE_BOOTSTRAP: &str = "127.0.0.1:1";

/// `freeze list` reads the registry, so it needs nothing in place first.
const LIST_THE_FREEZES: [&str; 3] = ["krabka-guard", "freeze", "list"];

/// Run `command_line` in the scenario's environment and record the outcome.
async fn record_run(guard: &mut GuardScenario, command_line: &[&str]) {
    let run = driver::run_in(command_line, &guard.environment).await;
    guard.record(run);
}

#[given("KRABKA_BOOTSTRAP_SERVER names the broker")]
async fn environment_names_the_broker(guard: &mut GuardScenario) {
    let bootstrap = guard.plaintext_broker().await.bootstrap();
    guard
        .environment
        .insert(BOOTSTRAP_SERVER_ENV.to_owned(), bootstrap);
}

#[given("KRABKA_BOOTSTRAP_SERVER names an address where no broker listens")]
fn environment_names_nothing(guard: &mut GuardScenario) {
    guard.environment.insert(
        BOOTSTRAP_SERVER_ENV.to_owned(),
        UNREACHABLE_BOOTSTRAP.to_owned(),
    );
}

#[given("KRABKA_BOOTSTRAP_SERVER is not set")]
fn environment_unset(guard: &GuardScenario) {
    assert!(!guard.environment.contains_key(BOOTSTRAP_SERVER_ENV));
}

#[when("an operator lists the freezes without --bootstrap-server")]
async fn list_without_flag(guard: &mut GuardScenario) {
    record_run(guard, &LIST_THE_FREEZES).await;
}

#[when("an operator tries to list the freezes without --bootstrap-server")]
async fn try_list_without_flag(guard: &mut GuardScenario) {
    record_run(guard, &LIST_THE_FREEZES).await;
}

#[when("an operator lists the freezes with --bootstrap-server naming the broker")]
async fn list_with_flag(guard: &mut GuardScenario) {
    let bootstrap = guard.plaintext_broker().await.bootstrap();
    let [program, freeze, list] = LIST_THE_FREEZES;
    record_run(guard, &[
        program,
        "--bootstrap-server",
        &bootstrap,
        freeze,
        list,
    ])
    .await;
}
