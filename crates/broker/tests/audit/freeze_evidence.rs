//! The audit log as evidence in its own right.
//!
//! KFC-9's load-bearing claim is that the audit log *alone* answers who did
//! what. An auditor holding this topic and the operator public keys has to be
//! able to say who froze a topic, who approved the thaw, and which approval
//! authorized which transition — with no metadata image to read and no broker
//! to take anybody's word for. The unit tests cannot reach that claim: they
//! observe the event on its way into the log rather than on its way back out
//! of it.
//!
//! Each case here drives the whole workflow in [`crate::freeze_workflow`] over
//! the wire and then reads the answer back off `__krabka_audit`.

use {
    crate::{
        freeze_signing::{FreezeBytes, freeze_signing_bytes, pattern_type_byte},
        freeze_workflow::{ALICE, ALICE_KEY_ID, BOB, CAROL, run_thaw_workflow},
        support,
    },
    assert2::{assert, check},
    krabka_broker::coordinator::AUDIT_TOPIC,
    krabka_log::name,
    ring::{signature, signature::UnparsedPublicKey},
    std::fs,
};

/// The `PrivilegedAction` records whose `api.operation` is `operation`.
///
/// The OCSF body spells that field `"<action>.<phase>"`, so one string selects
/// both the transition and the step of the workflow.
fn privileged<'a>(records: &'a [serde_json::Value], operation: &str) -> Vec<&'a serde_json::Value> {
    records
        .iter()
        .filter(|j| j["class_uid"] == 6003 && j["api"]["operation"] == operation)
        .collect()
}

/// The one `PrivilegedAction` record for `operation`.
fn one_privileged<'a>(records: &'a [serde_json::Value], operation: &str) -> &'a serde_json::Value {
    let rows = privileged(records, operation);
    assert!(
        rows.len() == 1,
        "expected exactly one {operation} record, got {}",
        rows.len()
    );
    rows[0]
}

use {
    crate::freeze_workflow::ThawWorkflow, krabka_audit::VerifyReport, ring::error::Unspecified,
    serde_json::Value, tempfile::TempDir,
};
struct FreezeEvidence {
    workflow: Option<ThawWorkflow>,
    records: Option<Vec<Value>>,
    sequences: Option<Vec<u64>>,
    directory: Option<TempDir>,
    offline: Option<VerifyReport>,
    signature: Option<Result<(), Unspecified>>,
    replayed_as_thaw: Option<Result<(), Unspecified>>,
}
struct FreezeEvent<'a> {
    workflow: &'a ThawWorkflow,
    record: &'a Value,
    action: &'a Value,
}
fn freeze_event(evidence: &FreezeEvidence) -> FreezeEvent<'_> {
    let workflow = evidence.workflow.as_ref().expect("incident completed");
    let records = evidence.records.as_ref().expect("audit records fetched");
    let record = one_privileged(records, "set_topic_freeze.applied");
    FreezeEvent {
        workflow,
        record,
        action: &record["privileged_action"],
    }
}
#[rstest::fixture]
fn evidence() -> FreezeEvidence {
    // Start this scenario with fresh inputs and observations.

    FreezeEvidence {
        workflow: None,
        records: None,
        sequences: None,
        directory: None,
        offline: None,
        signature: None,
        replayed_as_thaw: None,
    }
}
#[rstest_bdd_macros::given(
    "alice froze orders and bob and carol approved its thaw while a second proposal was withdrawn"
)]
async fn completed_operator_incident(evidence: &mut FreezeEvidence) {
    evidence.workflow = Some(run_thaw_workflow().await);
}
#[rstest_bdd_macros::when("the auditor fetches audit records through operation {operation}")]
async fn fetch_incident_evidence(evidence: &mut FreezeEvidence, operation: String) {
    let client = &evidence
        .workflow
        .as_ref()
        .expect("incident completed")
        .alice;
    evidence.records = Some(
        support::wait_for_audit_record(client, &operation, |row| {
            row["api"]["operation"] == operation
        })
        .await,
    );
}
#[rstest_bdd_macros::then(
    "every freeze proposal approval thaw and withdrawal phase has successful audit evidence"
)]
fn incident_phase_evidence(evidence: &FreezeEvidence) {
    let records = evidence.records.as_ref().expect("audit records fetched");
    // One row per phase the workflow produced. `status_id` is 1 for a success,
    // and every phase here succeeded: a refusal would be 2.
    for (label, operation) in [
        ("a freeze", "set_topic_freeze.applied"),
        ("a proposal", "thaw_topic_freeze.proposed"),
        ("an approval", "thaw_topic_freeze.approved"),
        ("a thaw", "thaw_topic_freeze.applied"),
        ("a consumed proposal", "thaw_topic_freeze.consumed"),
    ] {
        let rows = privileged(records, operation);
        check!(
            !rows.is_empty(),
            "case {label}: no {operation} on the topic"
        );
        check!(
            rows.iter().all(|j| j["status_id"] == 1),
            "case {label}: {operation} did not record a success"
        );
    }
}
#[rstest_bdd_macros::when("the auditor reads the chained sequence numbers")]
async fn read_incident_sequences(evidence: &mut FreezeEvidence) {
    evidence.sequences = Some(
        support::audit_record_seqs(
            &evidence
                .workflow
                .as_ref()
                .expect("incident completed")
                .alice,
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "at least five audit sequence numbers form one contiguous duplicate free chain"
)]
fn incident_sequence_chain(evidence: &FreezeEvidence) {
    let seqs = evidence.sequences.as_ref().expect("audit sequences read");
    check!(
        seqs.len() >= 5,
        "five phases wrote fewer than five chained records: {seqs:?}"
    );
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    check!(sorted.len() == seqs.len(), "a seq repeats: {seqs:?}");
    let contiguous: Vec<u64> =
        (0..u64::try_from(seqs.len()).expect("a test-sized chain")).collect();
    check!(sorted == contiguous, "the chain has a hole: {seqs:?}");
}
#[rstest_bdd_macros::when(
    "the broker shuts down and the offline auditor verifies its durable audit partition"
)]
async fn verify_durable_incident(evidence: &mut FreezeEvidence) {
    let workflow = evidence.workflow.take().expect("incident completed");
    let (directory, log_dir) = workflow.cluster.into_durable_log().await;
    drop(workflow.alice);
    let partition = name::partition_dir(&log_dir, AUDIT_TOPIC, 0);
    evidence.offline = Some(
        krabka_audit::verify_partition_dir(&partition, &krabka_audit::TrustedKeys::default())
            .expect("read the audit partition off disk"),
    );
    evidence.directory = Some(directory);
}
#[rstest_bdd_macros::then(
    "the offline hash chain is valid and includes every record fetched online"
)]
fn durable_incident_verified(evidence: &FreezeEvidence) {
    let report = evidence.offline.as_ref().expect("offline audit completed");
    let seqs = evidence.sequences.as_ref().expect("audit sequences read");
    check!(
        report.ok,
        "the audit hash chain broke: {:?}",
        report.first_break
    );
    check!(
        report.records.0 >= u64::try_from(seqs.len()).expect("a test-sized chain"),
        "the offline reader saw fewer records than the fetch did"
    );
}
#[rstest_bdd_macros::then(
    "the thaw proposal id joins exactly alice proposal and bob and carol approvals with ordered counterparties"
)]
fn incident_authorization_join(evidence: &FreezeEvidence) {
    let records = evidence.records.as_ref().expect("audit records fetched");
    let w = evidence.workflow.as_ref().expect("incident completed");
    // Start where an auditor starts: the row that says a thaw happened.
    let thaw = one_privileged(records, "thaw_topic_freeze.applied");
    let proposal = thaw["privileged_action"]["proposal_id"]
        .as_str()
        .expect("the transition names the proposal it spent")
        .to_owned();
    check!(proposal == w.proposal_id.to_string());

    // Follow that id into the approvals. Two proposals exist on this cluster,
    // so a join that matched on anything looser would pull in the wrong rows.
    let mut approvers: Vec<&str> = privileged(records, "thaw_topic_freeze.approved")
        .into_iter()
        .filter(|j| j["privileged_action"]["proposal_id"] == serde_json::json!(proposal))
        .filter_map(|j| j["actor"]["user"]["name"].as_str())
        .collect();
    approvers.sort_unstable();
    check!(
        approvers == vec![BOB, CAROL],
        "the proposal id did not reach both approvers"
    );

    // And into the proposal, which names the person who asked for the thaw.
    let target_prop_json = serde_json::json!(proposal);
    let proposed: Vec<&str> = privileged(records, "thaw_topic_freeze.proposed")
        .iter()
        .filter(|row| row["privileged_action"]["proposal_id"] == target_prop_json)
        .filter_map(|row| row["actor"]["user"]["name"].as_str())
        .collect();
    check!(proposed == vec![ALICE]);

    // The approval row itself names both people. `counterparties` is the
    // approval list as it stood after this approval landed, so carol's row
    // carries bob as well as carol; the whole array is compared rather than a
    // membership test, so a lost or reordered name fails here.
    let carols = privileged(records, "thaw_topic_freeze.approved")
        .into_iter()
        .find(|j| j["actor"]["user"]["name"] == serde_json::json!(CAROL))
        .expect("carol's approval reached the audit topic");
    check!(
        carols["privileged_action"]["counterparties"]
            == serde_json::json!([
                { "name": BOB, "type": "" },
                { "name": CAROL, "type": "" },
            ]),
        "carol's approval does not name bob"
    );
}
#[rstest_bdd_macros::when(
    "the auditor rebuilds the signed bytes from the freeze event and verifies both freeze and thaw directions"
)]
fn verify_incident_signature(evidence: &mut FreezeEvidence) {
    let FreezeEvent {
        workflow: w,
        record: freeze,
        action,
    } = freeze_event(evidence);
    let (pattern, scope) = action["target"]
        .as_str()
        .expect("event scope")
        .split_once(':')
        .expect("pattern and scope");
    let signed = FreezeBytes {
        cluster_id: &w.cluster_id,
        pattern_type: pattern_type_byte(pattern),
        scope,
        frozen: true,
        reason: freeze["status_detail"].as_str().expect("operator reason"),
        set_by: freeze["actor"]["user"]["name"]
            .as_str()
            .expect("event actor"),
        set_at_ms: action["signed_at_ms"].as_i64().expect("signed timestamp"),
        proposal_id: uuid::Uuid::parse_str(action["proposal_id"].as_str().expect("proposal id"))
            .expect("uuid"),
    };
    let signature = hex::decode(action["signature"].as_str().expect("raw signature"))
        .expect("the signature is lowercase hex");
    let public = fs::read(&w.cluster.alice_key.public_path).expect("the operator public key");
    let key = UnparsedPublicKey::new(&signature::ED25519, &public);
    let verified = key.verify(&freeze_signing_bytes(&signed), &signature);
    let as_thaw = FreezeBytes {
        frozen: false,
        ..signed
    };
    let replayed = key.verify(&freeze_signing_bytes(&as_thaw), &signature);
    evidence.signature = Some(verified);
    evidence.replayed_as_thaw = Some(replayed);
}
#[rstest_bdd_macros::then(
    "the event preserves alice key signature timestamp and principal and verifies only as a freeze"
)]
fn signed_incident_evidence(evidence: &FreezeEvidence) {
    let FreezeEvent {
        workflow: w,
        record: freeze,
        action,
    } = freeze_event(evidence);
    let key_id = action["key_id"].as_str().expect("the event names the key");
    let set_at_ms = action["signed_at_ms"].as_i64().expect("signed timestamp");
    check!(key_id == ALICE_KEY_ID);
    check!(action["signature_verified"] == true);
    check!(set_at_ms == w.freeze_set_at_ms);
    check!(freeze["time"].as_i64() != Some(set_at_ms));
    check!(
        evidence
            .signature
            .as_ref()
            .expect("freeze signature checked")
            .is_ok(),
        "the freeze on the audit topic does not verify under alice's key"
    );
    check!(
        evidence
            .replayed_as_thaw
            .as_ref()
            .expect("thaw replay checked")
            .is_err(),
        "a freeze signature verified as a thaw"
    );
    let set_by = freeze["actor"]["user"]["name"]
        .as_str()
        .expect("event actor");
    let records = evidence.records.as_ref().expect("audit records fetched");
    let proposed = privileged(records, "thaw_topic_freeze.proposed");
    check!(set_by == ALICE);
    check!(
        proposed
            .iter()
            .any(|row| row["actor"]["user"]["name"] == serde_json::json!(set_by)),
        "the freeze and the proposal spell their author differently"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/freeze_evidence.feature",
    name = "every kfc9 phase writes one chained audit record"
)]
#[tokio::test]
async fn every_kfc9_phase_writes_one_chained_audit_record(evidence: FreezeEvidence) {
    drop(evidence);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/freeze_evidence.feature",
    name = "the proposal id joins each approval to the transition it authorized"
)]
#[tokio::test]
async fn the_proposal_id_joins_each_approval_to_the_transition_it_authorized(
    evidence: FreezeEvidence,
) {
    evidence
        .workflow
        .expect("incident completed")
        .cluster
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/freeze_evidence.feature",
    name = "a signed freeze reverifies from the audit topic with no metadata image"
)]
#[tokio::test]
async fn a_signed_freeze_reverifies_from_the_audit_topic_with_no_metadata_image(
    evidence: FreezeEvidence,
) {
    evidence
        .workflow
        .expect("incident completed")
        .cluster
        .broker
        .shutdown()
        .await;
}
