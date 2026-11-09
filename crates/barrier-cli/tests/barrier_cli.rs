//! The tool driven end to end against a real broker.
//!
//! Every case calls [`krabka_barrier::run_from_args`] in process rather than
//! spawning the binary. A subprocess needs a Cargo working tree to build from
//! and a Bazel test sandbox has none, which is the same reason `krabka-format`
//! is a library as well as a binary.
//!
//! What this covers that the scenarios under `tests/features` do not: one
//! operator loop through every subcommand against one broker, each exiting 0.
#![cfg(test)]

mod support;

use {assert2::check, support::Cluster};

const GROUP: &str = "orders-cut";
const TOPIC: &str = "orders";

/// Run the tool, returning its exit code.
async fn cli(bootstrap: &str, args: &[&str]) -> i32 {
    let mut line = vec!["krabka-barrier", "--bootstrap-server", bootstrap];
    line.extend_from_slice(args);
    krabka_barrier::run_from_args(line).await
}

/// The whole operator loop: define a group, cut, read the cut back, and prove
/// the markers behind it are in the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_operator_loop_runs_end_to_end() {
    let cluster = Cluster::start().await;
    let bootstrap = cluster.bootstrap();
    cluster.create_topic(TOPIC, 3).await;

    check!(
        cli(bootstrap, &[
            "define",
            "--group",
            GROUP,
            "--topic",
            TOPIC,
            "--retained-cuts",
            "4"
        ],)
        .await
            == 0,
        "define"
    );
    check!(
        cli(bootstrap, &["describe", "--group", GROUP]).await == 0,
        "describe"
    );
    check!(
        cli(bootstrap, &["trigger", "--group", GROUP]).await == 0,
        "trigger"
    );
    check!(
        cli(bootstrap, &["list", "--group", GROUP]).await == 0,
        "list"
    );

    // Epoch 1 is the first cut a group takes: the broker spells "never
    // injected" as 0, so a real epoch is always positive. `verify` reads the
    // log at each offset the cut names, which makes a zero exit here the
    // strongest statement the tool makes -- the cut is not merely published,
    // it is backed.
    check!(
        cli(bootstrap, &["verify", "--group", GROUP, "--epoch", "1"]).await == 0,
        "verify"
    );

    check!(
        cli(bootstrap, &["delete", "--group", GROUP]).await == 0,
        "delete"
    );
}
