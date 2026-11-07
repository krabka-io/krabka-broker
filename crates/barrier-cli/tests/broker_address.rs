#![cfg(test)]
//! Operators name the broker with `--bootstrap-server`, or leave the flag out
//! and export `KRABKA_BOOTSTRAP_SERVER` instead.
//!
//! The scenarios are `tests/features/broker_address/broker_address.feature`.
//! The tool runs in process with the scenario's own environment, because
//! setting a variable in this process is `unsafe` in edition 2024.

mod steps;
mod support;

use {
    assert2::check,
    rstest_bdd_macros::{given, then, when},
    std::collections::BTreeMap,
    steps::{BarrierScenario, barrier},
    support::{Environment, Transcript, run_line},
};

/// The variable the tool reads when `--bootstrap-server` is left out.
const BOOTSTRAP_SERVER: &str = "KRABKA_BOOTSTRAP_SERVER";

/// A bootstrap address where no broker listens: nothing binds port 1.
const UNREACHABLE_BOOTSTRAP: &str = "127.0.0.1:1";

/// `describe` with no group names every group, so it needs nothing defined.
const DESCRIBE_EVERY_GROUP: [&str; 2] = ["krabka-barrier", "describe"];

/// The variables the operator's shell exports. Only this suite needs them,
/// so they are a fixture of their own rather than a field of every scenario.
#[rstest::fixture]
fn shell() -> Environment {
    Environment {
        variables: BTreeMap::new(),
    }
}

/// Describe every group with `line` as the command line, in `shell`, and keep
/// what the tool printed.
async fn describe_every_group(
    barrier: &mut BarrierScenario,
    shell: &Environment,
    line: Vec<String>,
) {
    let transcript = run_line(line, shell).await;
    barrier.transcript.set(transcript);
}

#[given("KRABKA_BOOTSTRAP_SERVER names the broker")]
async fn environment_names_the_broker(barrier: &mut BarrierScenario, shell: &mut Environment) {
    let bootstrap = barrier.cluster.get().await.bootstrap().to_owned();
    shell.export(BOOTSTRAP_SERVER, &bootstrap);
}

#[given("KRABKA_BOOTSTRAP_SERVER names an address where no broker listens")]
fn environment_names_nothing(shell: &mut Environment) {
    shell.export(BOOTSTRAP_SERVER, UNREACHABLE_BOOTSTRAP);
}

#[given("KRABKA_BOOTSTRAP_SERVER is not set")]
fn environment_unset(shell: &Environment) {
    check!(shell.get(BOOTSTRAP_SERVER) == None);
}

#[when("an operator describes every barrier group without --bootstrap-server")]
async fn describe_without_flag(barrier: &mut BarrierScenario, shell: &Environment) {
    describe_every_group(
        barrier,
        shell,
        DESCRIBE_EVERY_GROUP.map(str::to_owned).to_vec(),
    )
    .await;
}

#[when("an operator tries to describe every barrier group without --bootstrap-server")]
async fn try_describe_without_flag(barrier: &mut BarrierScenario, shell: &Environment) {
    describe_every_group(
        barrier,
        shell,
        DESCRIBE_EVERY_GROUP.map(str::to_owned).to_vec(),
    )
    .await;
}

#[when("an operator describes every barrier group with --bootstrap-server naming the broker")]
async fn describe_with_flag(barrier: &mut BarrierScenario, shell: &Environment) {
    let bootstrap = barrier.cluster.get().await.bootstrap().to_owned();
    let [program, describe] = DESCRIBE_EVERY_GROUP.map(str::to_owned);
    describe_every_group(barrier, shell, vec![
        program,
        "--bootstrap-server".to_owned(),
        bootstrap,
        describe,
    ])
    .await;
}

/// A broker with no groups answers `describe` with nothing to print, so a
/// clean exit with nothing on either stream is the broker's answer.
#[then("the tool reaches the broker")]
fn reached(barrier: &BarrierScenario) {
    check!(
        *barrier.transcript.get()
            == Transcript {
                exit: 0,
                stdout: String::new(),
                stderr: String::new(),
                command_line_refused: false,
            }
    );
}

krabka_bdd::scenarios!(
    "tests/features/broker_address",
    fixtures = [barrier: BarrierScenario, shell: Environment],
    runtime = "tokio-multi-thread",
    workers = 4,
);
