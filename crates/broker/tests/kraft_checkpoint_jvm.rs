//! Docker-gated: the JVM `kafka-dump-log --cluster-metadata-decoder` parses a
//! Krabka-built `bootstrap.checkpoint` cleanly.
//!
//! ```text
//! cargo test -p krabka-broker --test kraft_checkpoint_jvm -- --ignored --nocapture
//! ```
#![cfg(test)]

use {
    assert2::assert, bytes::Bytes,
    krabka_protocol::records::metadata::checkpoint::build_bootstrap_checkpoint,
    std::process::Output,
};

const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";

krabka_macros::jvm_checkpoint_dump_fixture!(dump_checkpoint);

#[derive(Default)]
struct JvmCheckpoint {
    bytes: Bytes,
    output: Option<Output>,
    directory_guard: Option<tempfile::TempDir>,
}
#[rstest::fixture]
fn jvm_checkpoint() -> JvmCheckpoint {
    // Each JVM decoder sees a fresh bootstrap artifact.
    JvmCheckpoint::default()
}
#[rstest_bdd_macros::given("a bootstrap checkpoint with feature levels {levels}")]
fn bootstrap_feature_levels(jvm_checkpoint: &mut JvmCheckpoint, levels: String) {
    let levels: Vec<(String, i16)> = serde_json::from_str(&levels).expect("feature levels");
    let borrowed = levels
        .iter()
        .map(|(name, level)| (name.as_str(), *level))
        .collect::<Vec<_>>();
    jvm_checkpoint.bytes = build_bootstrap_checkpoint(&borrowed);
    jvm_checkpoint.directory_guard = Some(tempfile::tempdir().expect("checkpoint directory"));
}
#[rstest_bdd_macros::when(
    "Kafka 4.0.0 dumps the bootstrap.checkpoint with its cluster metadata decoder"
)]
fn dump_jvm_checkpoint(jvm_checkpoint: &mut JvmCheckpoint) {
    jvm_checkpoint.output = Some(dump_checkpoint(
        jvm_checkpoint
            .directory_guard
            .as_ref()
            .expect("checkpoint directory")
            .path(),
        "bootstrap.checkpoint",
        &jvm_checkpoint.bytes,
        KAFKA_IMAGE,
    ));
}
#[rstest_bdd_macros::then("the JVM decoder succeeds with output fragments {fragments}")]
fn decoded_jvm_checkpoint(jvm_checkpoint: &JvmCheckpoint, fragments: String) {
    let fragments: Vec<(String, bool)> =
        serde_json::from_str(&fragments).expect("expected decoder output");
    let output = jvm_checkpoint
        .output
        .as_ref()
        .expect("JVM decoder executed");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!("{text}");
    assert!(
        output.status.success(),
        "kafka-dump-log must succeed: {text}"
    );
    for (fragment, present) in fragments {
        assert!(
            text.contains(&fragment) == present,
            "decoder fragment {fragment}: {text}"
        );
    }
}
#[ignore = "requires Docker"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_checkpoint_jvm.feature",
    name = "JVM Kafka reads a native bootstrap checkpoint"
)]
fn jvm_dump_log_parses_krabka_bootstrap_checkpoint(jvm_checkpoint: JvmCheckpoint) {
    let _ = jvm_checkpoint;
}
