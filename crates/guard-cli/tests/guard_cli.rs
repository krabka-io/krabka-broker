//! The tool driven end to end against a real broker.
//!
//! Every case calls [`crabka_guard::run_from_args`] in process rather than
//! spawning the binary. A subprocess needs a Cargo working tree to build from
//! and a Bazel test sandbox has none, which is the same reason `crabka-barrier`
//! and `crabka-format` are libraries as well as binaries.
//!
//! What these cover that the unit tests cannot: that each subcommand's request
//! reaches a broker that answers it, that a signature this machine makes
//! verifies inside that broker, and that the exit code reports what happened.
//!
//! # The principal
//!
//! A plaintext listener authenticates every connection as `ANONYMOUS`, and both
//! the freeze path and the break-glass path name that connection
//! `User:ANONYMOUS`. One `[[operator_keys]]` entry therefore serves both, which
//! is what the shared trust set is for.
//!
//! A two-person rule cannot be completed over one such listener, because the
//! proposer and the approver are then the same name. The refusal that says so
//! is asserted below; the completion belongs to the broker's own suite, which
//! can mint two principals.

use std::path::{Path, PathBuf};

use assert2::{assert, check};
use crabka_broker::{
    Broker, BrokerConfig, BrokerHandle,
    operator_keys::{OperatorKeyEntry, OperatorKeys},
};
use ring::signature::{Ed25519KeyPair, KeyPair as _};

/// The exit code for a request the broker refused.
const REFUSED: i32 = 1;
/// The exit code for a transport failure, where nothing is known about the
/// request's outcome.
const UNREACHABLE: i32 = 2;
/// The exit code for an action that needs an approval which does not exist.
const NO_APPROVAL: i32 = 4;
/// The exit code for a signature that did not verify.
const BAD_SIGNATURE: i32 = 5;

/// The principal a plaintext listener authenticates, in the Kafka form both the
/// freeze path and the break-glass path use. It binds the operator key and it
/// is the one entry in `break_glass.approvers`.
const PRINCIPAL: &str = "User:ANONYMOUS";
/// The operator key id the cases sign under.
const KEY_ID: &str = "alice-yubi";

const TOPIC: &str = "orders";
const PREFIX: &str = "tenant-a.";

/// One operator key, on disk, in the three forms the tool and the broker need.
struct Key {
    /// The PKCS#8 private key that `--sign-with` reads. It never leaves this
    /// machine, and the broker never sees it.
    pkcs8: PathBuf,
    /// The raw 32-byte public key that `[[operator_keys]]` names.
    public: PathBuf,
    /// The TOML file that `--operator-keys` reads, in the shape `broker.toml`
    /// writes it.
    trust_file: PathBuf,
}

/// Mint one operator key pair under `dir`, bound to `principal`.
fn mint_key(dir: &Path, key_id: &str, principal: &str) -> Key {
    let rng = ring::rand::SystemRandom::new();
    let der = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
    let pair = Ed25519KeyPair::from_pkcs8(der.as_ref()).expect("parse pkcs8");

    let pkcs8 = dir.join(format!("{key_id}.pk8"));
    std::fs::write(&pkcs8, der.as_ref()).expect("write private key");
    let public = dir.join(format!("{key_id}.pub"));
    std::fs::write(&public, pair.public_key().as_ref()).expect("write public key");
    let trust_file = dir.join(format!("{key_id}.toml"));
    std::fs::write(
        &trust_file,
        format!(
            "[[operator_keys]]\nkey_id = \"{key_id}\"\nprincipal = \"{principal}\"\n\
             public_key_path = \"{}\"\n",
            public.display()
        ),
    )
    .expect("write trust file");
    Key {
        pkcs8,
        public,
        trust_file,
    }
}

/// Boot a single-node broker that trusts `key` and lists this connection's
/// principal as its one break-glass approver.
///
/// The `TempDir` is returned so the log directory outlives the broker.
async fn broker(dir: tempfile::TempDir, key: &Key) -> (BrokerHandle, tempfile::TempDir, String) {
    let mut config = BrokerConfig::for_tests(dir.path().join("data"));
    config.operator_keys = OperatorKeys::load(&[OperatorKeyEntry {
        key_id: KEY_ID.to_owned(),
        principal: PRINCIPAL.to_owned(),
        public_key_path: key.public.clone(),
    }])
    .expect("load the operator trust set");
    config.break_glass.approvers = vec![PRINCIPAL.to_owned()];
    let handle = Broker::start(config).await.expect("broker starts");
    let bootstrap = handle.listen_addr().to_string();
    (handle, dir, bootstrap)
}

/// A broker, a key, and the directory both live in.
async fn cluster() -> (BrokerHandle, tempfile::TempDir, String, Key) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let key = mint_key(dir.path(), KEY_ID, PRINCIPAL);
    let (handle, dir, bootstrap) = broker(dir, &key).await;
    (handle, dir, bootstrap, key)
}

/// Run the tool, returning its exit code.
async fn cli(bootstrap: &str, args: &[&str]) -> i32 {
    let mut line = vec!["crabka-guard", "--bootstrap-server", bootstrap];
    line.extend_from_slice(args);
    crabka_guard::run_from_args(line).await
}

/// The signing flags a freeze command takes, as a borrowed argument list.
fn signed_as<'a>(key: &'a Key, principal: &'a str) -> [&'a str; 6] {
    [
        "--sign-with",
        key.pkcs8.to_str().expect("utf-8 path"),
        "--key-id",
        KEY_ID,
        "--principal",
        principal,
    ]
}

/// The id of the one proposal the cluster holds.
///
/// The tool prints the id on stdout, which an in-process case cannot read, so
/// this asks the broker directly. It exercises no tool code, only the setup a
/// later `approve` or `withdraw` case needs.
async fn only_proposal(bootstrap: &str) -> uuid::Uuid {
    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .client_id("guard-cli-test")
        .build()
        .await
        .expect("client connects");
    let response = client
        .send(crabka_protocol::krabka::break_glass::DescribeBreakGlassRequest::default())
        .await
        .expect("describe break-glass");
    let stored = response.proposals.first().expect("the cluster holds one");
    uuid::Uuid::from_bytes(stored.proposal_id.0)
}

/// The whole freeze loop: sign a freeze here, send it, read the registry back,
/// and prove the entry against the operator public key on this machine.
///
/// The verify is the strongest statement the tool makes. A zero exit says that
/// the signature the broker stored was made by this key, over this scope, in
/// this cluster, and that no broker was trusted to say so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_freeze_loop_runs_end_to_end() {
    let (_broker, _dir, bootstrap, key) = cluster().await;
    let signing = signed_as(&key, PRINCIPAL);

    let mut set = vec!["freeze", "set", "--topic", TOPIC, "--reason", "DR cutover"];
    set.extend_from_slice(&signing);
    check!(cli(&bootstrap, &set).await == 0, "a signed freeze");

    check!(cli(&bootstrap, &["freeze", "list"]).await == 0, "list");
    check!(
        cli(&bootstrap, &["freeze", "list", "--scope", TOPIC]).await == 0,
        "list one scope"
    );
    check!(
        cli(
            &bootstrap,
            &[
                "freeze",
                "list",
                "--verify-signatures",
                "--operator-keys",
                key.trust_file.to_str().expect("utf-8 path"),
            ],
        )
        .await
            == 0,
        "the entry verifies against the local operator key"
    );

    // A prefix freeze needs no signature, because a freeze is the safe
    // direction and an incident can start on a cluster with no key material.
    check!(
        cli(
            &bootstrap,
            &[
                "freeze",
                "set",
                "--prefix",
                PREFIX,
                "--reason",
                "tenant offboarding",
            ],
        )
        .await
            == 0,
        "an unsigned prefix freeze"
    );
    // The registry now holds a proved entry and an attested one. An unsigned
    // entry is reported as unsigned rather than counted as a failure, because
    // that mixture is what `freeze.require_signature = false` allows.
    check!(
        cli(
            &bootstrap,
            &[
                "freeze",
                "list",
                "--verify-signatures",
                "--operator-keys",
                key.trust_file.to_str().expect("utf-8 path"),
            ],
        )
        .await
            == 0,
        "a mixed registry still verifies"
    );
}

/// A thaw is the dangerous direction. A signed one that names no approved
/// proposal is refused with the code a runbook branches on, and not with a
/// generic refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thaw_with_no_approval_reports_that_an_approval_is_missing() {
    let (_broker, _dir, bootstrap, key) = cluster().await;
    let signing = signed_as(&key, PRINCIPAL);

    let mut set = vec!["freeze", "set", "--topic", TOPIC, "--reason", "DR cutover"];
    set.extend_from_slice(&signing);
    assert!(cli(&bootstrap, &set).await == 0);

    let nobody_approved = uuid::Uuid::new_v4().to_string();
    let mut clear = vec![
        "freeze",
        "clear",
        "--topic",
        TOPIC,
        "--proposal",
        &nobody_approved,
    ];
    clear.extend_from_slice(&signing);
    check!(cli(&bootstrap, &clear).await == NO_APPROVAL);

    // The freeze is still there, which is the point of the refusal.
    check!(cli(&bootstrap, &["freeze", "list", "--scope", TOPIC]).await == 0);
}

/// Every one of these is a signature the broker will not act on, and each has
/// to reach the operator as the signature exit code rather than as a plain
/// refusal, so a runbook sends them to their key material.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signature_the_broker_refuses_reports_a_signature_failure() {
    let (_broker, dir, bootstrap, key) = cluster().await;
    let stranger = mint_key(dir.path(), "mallory-yubi", "User:mallory");
    let signing = signed_as(&key, PRINCIPAL);

    let mut set = vec!["freeze", "set", "--topic", TOPIC, "--reason", "DR cutover"];
    set.extend_from_slice(&signing);
    assert!(cli(&bootstrap, &set).await == 0);

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
            == crabka_guard::EXIT_MISMATCH
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

/// A scope that reaches an internal topic would take the cluster down, so the
/// broker refuses it. It is an ordinary refusal, and not one of the two codes a
/// runbook branches on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_that_reaches_an_internal_topic_is_an_ordinary_refusal() {
    let (_broker, _dir, bootstrap, _key) = cluster().await;

    check!(
        cli(
            &bootstrap,
            &["freeze", "set", "--prefix", "__", "--reason", "no"],
        )
        .await
            == REFUSED
    );
}

/// The break-glass loop: open a proposal, read it back, fail to approve it
/// alone, and withdraw it.
///
/// The self-approval refusal is the two-person rule working. One principal
/// cannot be both people, and the broker says so rather than counting the
/// proposer's own approval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_break_glass_loop_runs_end_to_end() {
    let (_broker, _dir, bootstrap, key) = cluster().await;

    check!(
        cli(
            &bootstrap,
            &[
                "break-glass",
                "propose",
                "--action",
                "delete-topic",
                "--target",
                "doomed",
                "--reason",
                "the topic holds test data only",
                "--ttl",
                "30m",
            ],
        )
        .await
            == 0,
        "propose"
    );
    check!(cli(&bootstrap, &["break-glass", "list"]).await == 0, "list");
    check!(
        cli(&bootstrap, &["break-glass", "list", "--pending"]).await == 0,
        "list pending"
    );

    let proposal = only_proposal(&bootstrap).await.to_string();
    check!(
        cli(
            &bootstrap,
            &["break-glass", "approve", "--proposal", &proposal],
        )
        .await
            == REFUSED,
        "the proposer cannot also approve"
    );
    // The same refusal holds with a signature, because the distinct-principal
    // rule is checked before the signature is worth anything.
    check!(
        cli(
            &bootstrap,
            &[
                "break-glass",
                "approve",
                "--proposal",
                &proposal,
                "--sign-with",
                key.pkcs8.to_str().expect("utf-8 path"),
                "--key-id",
                KEY_ID,
            ],
        )
        .await
            == REFUSED,
        "a signed self-approval is still a self-approval"
    );

    check!(
        cli(
            &bootstrap,
            &["break-glass", "withdraw", "--proposal", &proposal],
        )
        .await
            == 0,
        "the proposer may withdraw"
    );
    // A withdrawn proposal is spent. Nothing can approve it afterwards, which
    // is what makes a withdraw worth having.
    check!(
        cli(
            &bootstrap,
            &["break-glass", "approve", "--proposal", &proposal],
        )
        .await
            == REFUSED,
        "a withdrawn proposal cannot be approved"
    );
}

/// A proposal nobody opened is refused, and not reported as an empty success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proposal_that_does_not_exist_is_refused() {
    let (_broker, _dir, bootstrap, _key) = cluster().await;
    let absent = uuid::Uuid::new_v4().to_string();

    check!(
        cli(
            &bootstrap,
            &["break-glass", "approve", "--proposal", &absent],
        )
        .await
            == REFUSED,
        "approve"
    );
    check!(
        cli(
            &bootstrap,
            &["break-glass", "withdraw", "--proposal", &absent],
        )
        .await
            == REFUSED,
        "withdraw"
    );
    check!(
        cli(&bootstrap, &["break-glass", "list", "--proposal", &absent],).await == REFUSED,
        "a read that names one absent proposal is a refusal, not an empty list"
    );
    check!(
        cli(&bootstrap, &["break-glass", "list"]).await == 0,
        "a read of the whole empty registry is an empty success"
    );
}

/// A broker that cannot be reached is not a refusal. Nothing is known about the
/// outcome, and the exit code has to say so, because a runbook that read this
/// as a refusal would assume the freeze did not land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_bootstrap_reports_that_nothing_is_known() {
    // Port 1 is reserved and nothing listens on it.
    let nowhere = "127.0.0.1:1";

    check!(
        cli(
            nowhere,
            &["freeze", "set", "--topic", TOPIC, "--reason", "DR cutover"],
        )
        .await
            == UNREACHABLE,
        "freeze set"
    );
    check!(
        cli(nowhere, &["freeze", "list"]).await == UNREACHABLE,
        "freeze list"
    );
    check!(
        cli(
            nowhere,
            &[
                "break-glass",
                "propose",
                "--action",
                "delete-topic",
                "--target",
                "doomed",
                "--reason",
                "no",
            ],
        )
        .await
            == UNREACHABLE,
        "break-glass propose"
    );
    check!(
        cli(nowhere, &["break-glass", "list"]).await == UNREACHABLE,
        "break-glass list"
    );
}
