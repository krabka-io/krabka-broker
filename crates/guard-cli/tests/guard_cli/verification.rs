//! Proving the registry from this machine with `freeze list
//! --verify-signatures`.
//!
//! The claim the flag makes is that the local key material proved the entry, so
//! a trust set that cannot prove one has to report that rather than pass it,
//! and the two ways it can fail carry different exit codes.

use assert2::{assert, check};

use crate::support::{BAD_SIGNATURE, PRINCIPAL, TOPIC, cli, cluster, mint_key, signed_as};

/// `freeze list --verify-signatures` says the registry is authentic from this
/// machine, so a trust set that cannot prove an entry has to say so rather than
/// pass the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registry_the_local_keys_cannot_prove_does_not_pass() {
    let (_broker, dir, bootstrap, key) = cluster().await;
    let stranger = mint_key(dir.path(), "mallory-yubi", "User:mallory");
    let signing = signed_as(&key, PRINCIPAL);

    let mut set = vec!["freeze", "set", "--topic", TOPIC, "--reason", "DR cutover"];
    set.extend_from_slice(&signing);
    assert!(cli(&bootstrap, &set).await == 0);

    // The stranger's trust file names another key id, so the entry cannot be
    // checked here at all. That is the mismatch code, not the signature code:
    // the tool could not check, rather than checked and found it wrong.
    check!(
        cli(
            &bootstrap,
            &[
                "freeze",
                "list",
                "--verify-signatures",
                "--operator-keys",
                stranger.trust_file.to_str().expect("utf-8 path"),
            ],
        )
        .await
            == krabka_guard::EXIT_MISMATCH
    );

    // A trust file that is not there stops the verify before it starts.
    check!(
        cli(
            &bootstrap,
            &[
                "freeze",
                "list",
                "--verify-signatures",
                "--operator-keys",
                "/nonexistent/keys.toml",
            ],
        )
        .await
            == BAD_SIGNATURE
    );
}
