//! The audit trail a delegation-token mutation leaves behind.
//!
//! `CreateDelegationToken`, `RenewDelegationToken` and `ExpireDelegationToken`
//! each have to reach the audit topic as an OCSF `AdminOperation` record that
//! names the token they changed. Two of those three are only auditable at all
//! because the broker resolves the token id from the metadata image *before*
//! the mutation: renew and expire address a token by HMAC, neither response
//! echoes an id back, and an expire removes the record the lookup would read.
//!
//! The record names the id and never the HMAC. The HMAC is the token's
//! password equivalent, and the audit topic is a place an auditor reads.
//!
//! These cases need the `SASL_PLAINTEXT` fixture rather than the in-process
//! one the rest of the audit suite uses: the four delegation-token apis are
//! gated on `delegation_token_secret_key`, and they authorize against the
//! connection's own principal rather than against a request context.

use {
    crate::{
        cluster::{TokenWorkflow, tokens},
        rpc::send_renew_delegation_token,
        support,
    },
    assert2::check,
    base64::{Engine as _, engine::general_purpose},
    krabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest,
};
/// The OCSF class for an API Activity record.
const API_ACTIVITY: i64 = 6003;
/// The OCSF `status_id` for a success.
const STATUS_SUCCESS: i64 = 1;

/// Does this record report `operation` succeeding on `token_id`, by `actor`?
fn names_token(record: &serde_json::Value, operation: &str, token_id: &str, actor: &str) -> bool {
    record["class_uid"] == API_ACTIVITY
        && record["api"]["operation"] == operation
        && record["status_id"] == STATUS_SUCCESS
        && record["actor"]["user"]["name"] == actor
        && record["resources"][0]["type"] == "DelegationToken"
        && record["resources"][0]["name"] == token_id
}

/// How many records name `operation`, whatever their outcome.
fn records_for(records: &[serde_json::Value], operation: &str) -> usize {
    records
        .iter()
        .filter(|j| j["class_uid"] == API_ACTIVITY && j["api"]["operation"] == operation)
        .count()
}

/// The token secret in the two textual encodings an audit record could leak it
/// in: base64, which is how a token holder types it as a SCRAM password, and
/// lowercase hex, which is how the audit chain renders raw bytes.
fn secret_spellings(hmac: &[u8]) -> Vec<String> {
    let hex = hmac.iter().fold(String::new(), |mut out, b| {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
        out
    });
    vec![general_purpose::STANDARD.encode(hmac), hex]
}

#[rstest_bdd_macros::when("alice renews an unknown HMAC of 32 bytes 0x5a for 3600000 milliseconds")]
async fn renew_missing_token(tokens: &mut TokenWorkflow) {
    tokens.renewed = Some(
        send_renew_delegation_token(
            tokens
                .sessions
                .get_mut("alice")
                .expect("alice authenticated"),
            300,
            &RenewDelegationTokenRequest {
                hmac: bytes::Bytes::from_static(&[0x5a; 32]),
                renew_period_ms: 3_600_000,
                ..Default::default()
            },
        )
        .await
        .expect("RenewDelegationToken unknown HMAC"),
    );
}
#[rstest_bdd_macros::then("renewal against the unknown HMAC fails")]
fn missing_token_renewal_refused(tokens: &TokenWorkflow) {
    check!(
        tokens
            .renewed
            .as_ref()
            .expect("unknown-HMAC renewal attempted")
            .error_code
            != 0
    );
}
#[rstest_bdd_macros::then(
    "the audit topic names token {alias} and alice in all three mutations audits one renewal and leaks no HMAC"
)]
async fn token_mutations_audited(tokens: &TokenWorkflow, alias: String) {
    let created = tokens.tokens.get(&alias).expect("named token minted");
    let client = support::sasl_client(&tokens.address().to_string(), "alice", "wonderland").await;
    for operation in [
        "CreateDelegationToken",
        "RenewDelegationToken",
        "ExpireDelegationToken",
    ] {
        support::wait_for_audit_record(&client, operation, |row| {
            names_token(row, operation, &created.token_id, "alice")
        })
        .await;
    }
    let records = support::consume_audit_records(&client).await;
    check!(
        records_for(&records, "RenewDelegationToken") == 1,
        "a renew that failed must not be audited"
    );
    let dump = serde_json::to_string(&records).expect("audit records serialize");
    for secret in secret_spellings(&created.hmac) {
        check!(
            !dump.contains(&secret),
            "token HMAC leaked into audit topic"
        );
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/delegation_tokens/audit.feature",
    name = "delegation token mutations reach the audit topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegation_token_mutations_reach_the_audit_topic(tokens: TokenWorkflow) {
    tokens.shutdown().await;
}
