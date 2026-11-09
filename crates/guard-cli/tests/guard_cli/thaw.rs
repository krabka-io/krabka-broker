//! Clearing a freeze, which is the dangerous direction and the one the broker
//! guards hardest.
//!
//! The case drives `freeze clear` with signatures the broker will not act on and
//! asserts that each reaches the operator as the signature exit code, not as a
//! plain refusal. The missing-approval refusals are scenarios in
//! `tests/features/thawing`.

use {
    crate::support::{BAD_SIGNATURE, KEY_ID, PRINCIPAL, TOPIC, cli, frozen_cluster_with_stranger},
    assert2::check,
};

/// Every one of these is a signature the broker will not act on, and each has
/// to reach the operator as the signature exit code rather than as a plain
/// refusal, so a runbook sends them to their key material.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signature_the_broker_refuses_reports_a_signature_failure() {
    let (_broker, _dir, bootstrap, key, stranger) = frozen_cluster_with_stranger().await;

    let proposal = uuid::Uuid::new_v4().to_string();
    let clear = vec!["freeze", "clear", "--topic", TOPIC, "--proposal", &proposal];

    // A thaw with no signature at all. The broker needs one whatever
    // `freeze.require_signature` says, because a thaw is the dangerous
    // direction.
    check!(
        cli(&bootstrap, &clear).await == BAD_SIGNATURE,
        "an unsigned thaw"
    );

    // A key id the broker's trust set does not hold.
    let mut unknown_key = clear.clone();
    unknown_key.extend_from_slice(&[
        "--sign-with",
        stranger.pkcs8.to_str().expect("utf-8 path"),
        "--key-id",
        "mallory-yubi",
        "--principal",
        PRINCIPAL,
    ]);
    check!(
        cli(&bootstrap, &unknown_key).await == BAD_SIGNATURE,
        "an unknown key id"
    );

    // A record that claims an author the signing key does not speak for.
    let mut wrong_author = clear.clone();
    wrong_author.extend_from_slice(&[
        "--sign-with",
        key.pkcs8.to_str().expect("utf-8 path"),
        "--key-id",
        KEY_ID,
        "--principal",
        "User:mallory",
    ]);
    check!(
        cli(&bootstrap, &wrong_author).await == BAD_SIGNATURE,
        "an author the key does not speak for"
    );
}
