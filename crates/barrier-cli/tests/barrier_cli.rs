//! The tool driven end to end against a real broker.
//!
//! Every case calls [`krabka_barrier::run_from_args`] in process rather than
//! spawning the binary. A subprocess needs a Cargo working tree to build from
//! and a Bazel test sandbox has none, which is the same reason `krabka-format`
//! is a library as well as a binary.
//!
//! What these cover that the unit tests cannot: that each subcommand's request
//! actually reaches a broker that answers it, and that the exit code reports
//! what happened.
#![cfg(test)]

mod support;

use {
    assert2::{assert, check},
    support::Cluster,
};

/// The exit code for a request the broker refused.
const REFUSED: i32 = 1;
/// The exit code for a cut the log does not back.
const MISMATCH: i32 = 3;

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

/// A group nobody defined is refused, not reported as an empty success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_group_is_refused() {
    let cluster = Cluster::start().await;
    let bootstrap = cluster.bootstrap();

    check!(
        cli(bootstrap, &["trigger", "--group", "nope"]).await == REFUSED,
        "trigger"
    );
    check!(
        cli(bootstrap, &["list", "--group", "nope"]).await == REFUSED,
        "list"
    );
    check!(
        cli(bootstrap, &["verify", "--group", "nope", "--epoch", "0"]).await == REFUSED,
        "verify"
    );
}

/// An epoch the group never took cannot be verified. This is the case that
/// separates "no cut here" from "a cut whose markers are missing": the first is
/// a refusal, the second a mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_epoch_with_no_cut_is_refused_rather_than_mismatched() {
    let cluster = Cluster::start().await;
    let bootstrap = cluster.bootstrap();
    cluster.create_topic(TOPIC, 1).await;
    assert!(cli(bootstrap, &["define", "--group", GROUP, "--topic", TOPIC]).await == 0);
    assert!(cli(bootstrap, &["trigger", "--group", GROUP]).await == 0);

    check!(
        cli(bootstrap, &["verify", "--group", GROUP, "--epoch", "1"]).await == 0,
        "the epoch that exists verifies"
    );
    check!(
        cli(bootstrap, &["verify", "--group", GROUP, "--epoch", "99"]).await == REFUSED,
        "an epoch with no cut is a refusal, not a mismatch"
    );
    check!(
        cli(bootstrap, &["verify", "--group", GROUP, "--epoch", "99"]).await != MISMATCH,
        "an epoch with no cut must not read as a cut the log contradicts"
    );
}

/// A group may name a topic that does not exist yet.
///
/// The coordinator freezes a group's target set from the metadata image at
/// each injection, not at definition, so a topic created later is picked up by
/// the next epoch. Refusing here would make a group undefinable before its
/// topics exist, and would buy nothing: the name still has to be re-resolved
/// every epoch because a topic can be deleted after the fact.
///
/// The cut over such a group names no partition for the absent topic, which is
/// how a reader sees the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_may_name_a_topic_that_does_not_exist_yet() {
    let cluster = Cluster::start().await;
    let bootstrap = cluster.bootstrap();

    check!(
        cli(bootstrap, &[
            "define",
            "--group",
            GROUP,
            "--topic",
            "not-created-yet"
        ],)
        .await
            == 0,
        "define accepts an unresolved topic name"
    );
    check!(
        cli(bootstrap, &["trigger", "--group", GROUP]).await == 0,
        "a cut over it still succeeds"
    );
}
