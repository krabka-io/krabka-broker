//! The freeze registry lives in the metadata log, so a restart cannot thaw a
//! cluster silently.
//!
//! A registry that lived anywhere else would pass every other case in this
//! suite and fail this one in the worst possible way: the topic would start
//! accepting writes during the incident the freeze was declared for.

use {
    crate::{
        control_plane::wait_for_registry_len,
        support,
        wire::{
            CONTROL, accepted, frozen_topic_with_record, produce_outcome, refused,
            wait_controlled_partitions,
        },
    },
    assert2::check,
    krabka_protocol::krabka::freeze::PATTERN_TYPE_LITERAL,
};

#[cfg(test)]
#[rstest_bdd_macros::then("topic freeze durability: a freeze survives a controller restart")]
async fn bdd_verify_a_freeze_survives_a_controller_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        let (_frozen, _control) =
            frozen_topic_with_record(&broker, &client, "orders", "cutover").await;
        broker.shutdown().await;
    }

    let (broker, client) = support::start_with_dir(dir.path()).await;
    wait_controlled_partitions(&broker, "orders").await;

    let entries = wait_for_registry_len(&client, 1).await;
    check!(entries[0].scope == "orders");
    check!(entries[0].pattern_type == PATTERN_TYPE_LITERAL);

    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );
    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));

    broker.shutdown().await;
}

/// A freeze survives a controller restart.
///
/// The registry lives in the metadata log rather than in memory precisely so
/// that a restart cannot thaw a cluster silently. This case restarts the only
/// controller in the cluster and asserts the refusal is still there. A registry
/// that lived anywhere else would pass every other case in this suite and fail
/// this one, in the worst possible way: the topic would start accepting writes
/// during the incident that the freeze was declared for.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/topic_freeze/durability.feature",
    name = "a freeze survives a controller restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_freeze_survives_a_controller_restart() {}
