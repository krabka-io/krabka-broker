//! `SetTopicFreeze`, api key 1015.
//!
//! One request freezes a scope, or removes the freeze on one. A scope is a
//! literal topic name or a topic-name prefix, and `pattern_type` says which.
//! The controller writes the outcome to the metadata log, so a freeze holds
//! after a restart and after a leader change.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On a deny the
//! response carries `CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! # A freeze takes one command, and a thaw takes two people
//!
//! A freeze needs no break-glass proposal. An operator must reach it in one
//! command during an incident, on a cluster where nobody installed key material
//! yet, and freezing is the safe direction.
//!
//! A thaw needs an approved proposal, and it needs a signature whatever
//! `freeze.require_signature` says. That asymmetry is the feature. Without it a
//! freeze is only as strong as the one credential that set it, which is the
//! credential an attacker already holds when the incident is a compromise.
//!
//! # The consume and the thaw commit together
//!
//! A thaw prepends the consumed proposal record to its own record and submits
//! one `submit_change` call. That single raft append is what stops one approval
//! from authorizing two thaws across a crash.
//!
//! # The timestamp
//!
//! A signed record keeps the `set_at_ms` that the operator signed, because the
//! broker checks that value against the skew window and against the entry it
//! replaces. An unsigned record takes this broker's clock instead. An unsigned
//! freeze is the broker's attestation rather than the operator's proof, so the
//! broker's clock is the honest stamp, and a client then cannot park an entry
//! in the far future where no later record replaces it.

use bytes::Bytes;
use crabka_audit::{AuditOutcome, AuditResource, PrivilegedPhase};
use crabka_metadata::{
    BreakGlassAction, MetadataImage, MetadataRecord, PatternType, TopicFreezeRecord,
};
use crabka_protocol::{
    Decode,
    krabka::freeze::{SetTopicFreezeRequest, SetTopicFreezeResponse},
};
use uuid::Uuid;

use crate::{
    break_glass::gate,
    broker::Broker,
    codes,
    config::BrokerConfig,
    error::BrokerError,
    freeze::{
        freeze_target,
        handlers::{FreezeAudit, audit_freeze, pattern_type_concrete},
        scope_covers_internal_topic,
        signing::{FreezeSignatureCheck, verify_freeze_signature},
    },
    handlers::{RequestContext, audit_admin, cluster_alter_denied, encode_response},
    time_util::now_ms,
};

/// The audit action name of a freeze.
const FREEZE_ACTION: &str = "set_topic_freeze";

/// The audit action name of a thaw. It is the break-glass action's own name, so
/// the audit event and the `break_glass_*` metric labels read alike.
const THAW_ACTION: &str = "thaw_topic_freeze";

#[tracing::instrument(
    name = "handle_set_topic_freeze",
    level = "info",
    skip_all,
    fields(api = "SetTopicFreeze"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = SetTopicFreezeRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let env = FreezeEnv {
        config: &broker.config,
        image: &image,
        ctx,
    };
    let outcome = match prepare(&env, &req) {
        Err(refusal) => Err(refusal),
        Ok(accepted) => submit(broker, accepted).await,
    };
    let response = respond(broker, ctx, &req, &outcome);
    encode_response(&response, version)
}

/// Everything the checks read, and nothing they write.
///
/// The checks take this rather than the whole broker, so each rule is a pure
/// function of the configuration, the metadata image, and the connection.
struct FreezeEnv<'a> {
    config: &'a BrokerConfig,
    image: &'a MetadataImage,
    ctx: &'a RequestContext<'a>,
}

/// A request the broker did not accept, and the text that says why.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Refusal {
    code: i16,
    message: String,
    /// Whether a signature was present and verified before the refusal. It is
    /// `false` for every refusal that a signature check itself produced.
    signature_verified: bool,
}

impl Refusal {
    fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            signature_verified: false,
        }
    }
}

/// A request the broker accepted, with everything the raft append needs.
struct Accepted {
    /// The registry record the append writes.
    record: TopicFreezeRecord,
    /// The break-glass proposal the append spends, on a thaw.
    consumed_proposal: Option<MetadataRecord>,
    /// Whether the broker verified a signature on the record.
    signature_verified: bool,
}

/// Check the request against every rule, and build the records it becomes.
///
/// # Errors
///
/// Returns the [`Refusal`] of the first rule that fails, in this order: the
/// cluster `Alter` gate, the shape of the pattern type, the scope, the registry
/// ceiling, the signature, and then the break-glass approval that a thaw needs.
fn prepare(env: &FreezeEnv<'_>, req: &SetTopicFreezeRequest) -> Result<Accepted, Refusal> {
    if cluster_alter_denied(env.config.authorizer.as_ref(), env.image, env.ctx) {
        return Err(Refusal::new(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "set-topic-freeze denied",
        ));
    }
    let pattern_type = pattern_type_concrete(req.pattern_type).ok_or_else(|| {
        Refusal::new(
            codes::INVALID_REQUEST,
            format!(
                "pattern_type {} names no freeze scope kind; 3 is literal and 4 is prefixed",
                req.pattern_type
            ),
        )
    })?;
    check_scope(pattern_type, &req.scope)?;

    let replaces = live_entry(env.image, pattern_type, &req.scope).cloned();
    if req.frozen && replaces.is_none() {
        check_limit(env.image, env.config.freeze.max_entries)?;
    }

    let record = record_of(req, pattern_type, &env.ctx.principal.name);
    let signature_verified = check_signature(env, &record, replaces.as_ref())?;
    let consumed_proposal = check_approval(env, &record)?;

    Ok(Accepted {
        record,
        consumed_proposal,
        signature_verified,
    })
}

/// Refuse a scope that no freeze may cover.
fn check_scope(pattern_type: PatternType, scope: &str) -> Result<(), Refusal> {
    if scope.is_empty() {
        return Err(Refusal::new(
            codes::FREEZE_SCOPE_INVALID,
            "a freeze scope is empty",
        ));
    }
    if scope_covers_internal_topic(pattern_type, scope) {
        return Err(Refusal::new(
            codes::FREEZE_SCOPE_INVALID,
            format!("freeze scope {scope:?} reaches an internal topic, which is never freezable"),
        ));
    }
    Ok(())
}

/// Refuse a freeze that would take the registry past `freeze.max_entries`.
///
/// The ceiling bounds the reverse walk that a prefix-scoped resolve does, and
/// that resolve is one hop from the produce path.
fn check_limit(image: &MetadataImage, max_entries: usize) -> Result<(), Refusal> {
    let live = image.topic_freezes().count();
    if live >= max_entries {
        return Err(Refusal::new(
            codes::FREEZE_LIMIT_EXCEEDED,
            format!("the freeze registry already holds its ceiling of {max_entries} entries"),
        ));
    }
    Ok(())
}

/// Check the signature rules, and report whether a signature verified.
///
/// A thaw needs a signature whatever `freeze.require_signature` says, because a
/// thaw is the dangerous direction. A freeze needs one when
/// `freeze.require_signature` is on. A signature that the request carries is
/// verified whether or not it was needed, so a broken one never passes as an
/// unsigned request.
fn check_signature(
    env: &FreezeEnv<'_>,
    record: &TopicFreezeRecord,
    replaces: Option<&TopicFreezeRecord>,
) -> Result<bool, Refusal> {
    if !is_signed(&record.key_id, &record.signature) {
        if !record.frozen {
            return Err(Refusal::new(
                codes::OPERATOR_SIGNATURE_REQUIRED,
                "a thaw needs a detached operator signature",
            ));
        }
        if env.config.freeze.require_signature {
            return Err(Refusal::new(
                codes::OPERATOR_SIGNATURE_REQUIRED,
                "freeze.require_signature is on, so a freeze needs a detached operator signature",
            ));
        }
        return Ok(false);
    }

    let cluster_id = env.image.cluster_id().to_string();
    let check = FreezeSignatureCheck {
        keys: &env.config.operator_keys,
        cluster_id: &cluster_id,
        connection_principal: &env.ctx.principal.name,
        max_skew: env.config.freeze.signature_max_skew,
        now_ms: now_ms(),
        replaces,
    };
    verify_freeze_signature(&check, record).map_err(|refusal| {
        let (code, message) = refusal.wire();
        Refusal::new(code, message)
    })?;
    Ok(true)
}

/// Find the break-glass approval that a thaw needs, and stamp it consumed.
///
/// A freeze needs none and returns `None`. A thaw names its proposal on the
/// request, which is a field a krabka-private message can carry and a Kafka
/// message cannot. The gate then finds the approval, and the caller puts the
/// record the gate returns in the same raft append as the thaw.
fn check_approval(
    env: &FreezeEnv<'_>,
    record: &TopicFreezeRecord,
) -> Result<Option<MetadataRecord>, Refusal> {
    if record.frozen {
        return Ok(None);
    }
    if record.proposal_id.is_nil() {
        return Err(Refusal::new(
            codes::BREAK_GLASS_APPROVAL_REQUIRED,
            "a thaw names the break-glass proposal that approved it",
        ));
    }
    let target = freeze_target(record.pattern_type, &record.scope);
    let consumed = gate::authorize(
        env.image,
        &env.config.break_glass,
        BreakGlassAction::ThawTopicFreeze,
        &target,
        now_ms(),
    )
    .map_err(|denial| Refusal::new(codes::BREAK_GLASS_APPROVAL_REQUIRED, denial.to_string()))?;
    if consumed_proposal_id(&consumed) != Some(record.proposal_id) {
        return Err(Refusal::new(
            codes::BREAK_GLASS_APPROVAL_REQUIRED,
            format!(
                "no approved proposal {} covers a thaw of {target}",
                record.proposal_id
            ),
        ));
    }
    Ok(Some(consumed))
}

/// Whether the request carries a signature at all.
///
/// A half-filled pair is signed for this test, so a request that names a key
/// and no signature reaches the verifier and is refused there rather than
/// passing as an unsigned request.
fn is_signed(key_id: &str, signature: &[u8]) -> bool {
    !key_id.is_empty() || !signature.is_empty()
}

/// The proposal that a consumed record names.
fn consumed_proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}

/// The registry record that the request becomes.
///
/// `set_by` is the principal the broker authenticated on the connection, and
/// never a field of the request. A client cannot claim another author, and the
/// signature covers the name the broker fills in.
fn record_of(
    req: &SetTopicFreezeRequest,
    pattern_type: PatternType,
    set_by: &str,
) -> TopicFreezeRecord {
    TopicFreezeRecord {
        scope: req.scope.clone(),
        pattern_type,
        frozen: req.frozen,
        reason: req.reason.clone(),
        set_by: set_by.to_owned(),
        set_at_ms: if is_signed(&req.key_id, &req.signature) {
            req.set_at_ms
        } else {
            now_ms()
        },
        proposal_id: Uuid::from_bytes(req.proposal_id.0),
        key_id: req.key_id.clone(),
        signature: req.signature.clone(),
    }
}

/// The live registry entry at exactly this scope and pattern type.
///
/// This is an exact key lookup and not a topic resolve: a freeze on
/// `prefixed:tenant-a.` replaces the prefixed entry alone, and never the
/// literal entry of the same name.
fn live_entry<'a>(
    image: &'a MetadataImage,
    pattern_type: PatternType,
    scope: &str,
) -> Option<&'a TopicFreezeRecord> {
    image
        .topic_freezes()
        .find(|entry| entry.pattern_type == pattern_type && entry.scope == scope)
}

/// Write the accepted records in one raft append.
///
/// The consumed proposal goes first, so the approval and the thaw it authorized
/// commit together.
async fn submit(broker: &Broker, accepted: Accepted) -> Result<Accepted, Refusal> {
    let mut records = Vec::with_capacity(2);
    if let Some(proposal) = accepted.consumed_proposal.clone() {
        records.push(proposal);
    }
    records.push(MetadataRecord::V1TopicFreeze(accepted.record.clone()));

    match broker.controller.submit_change(records).await {
        Ok(_) => Ok(accepted),
        Err(error) => {
            tracing::warn!(%error, "set-topic-freeze submit failed");
            Err(Refusal {
                code: codes::COORDINATOR_NOT_AVAILABLE,
                message: format!("submit failed: {error}"),
                signature_verified: accepted.signature_verified,
            })
        }
    }
}

/// Audit the outcome and build the response.
///
/// Every outcome reaches the audit log, and a refusal reaches it as surely as a
/// success. A success also emits the ordinary administrative event, so a SIEM
/// rule that already reads those sees a freeze and a thaw.
fn respond(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    req: &SetTopicFreezeRequest,
    outcome: &Result<Accepted, Refusal>,
) -> SetTopicFreezeResponse {
    let target = pattern_type_concrete(req.pattern_type)
        .map_or_else(|| req.scope.clone(), |kind| freeze_target(kind, &req.scope));
    let proposal_id = Uuid::from_bytes(req.proposal_id.0);
    let (code, message, signature_verified, reason) = match outcome {
        Ok(accepted) => (
            codes::NONE,
            None,
            accepted.signature_verified,
            req.reason.clone(),
        ),
        Err(refusal) => (
            refusal.code,
            Some(refusal.message.clone()),
            refusal.signature_verified,
            refusal.message.clone(),
        ),
    };
    let succeeded = code == codes::NONE;
    let audit = FreezeAudit {
        action: if req.frozen {
            FREEZE_ACTION
        } else {
            THAW_ACTION
        },
        target: target.clone(),
        proposal_id,
        key_id: &req.key_id,
        signature: &req.signature,
        signature_verified,
        reason,
    };
    audit_freeze(
        broker.audit_log.as_ref(),
        ctx,
        if succeeded {
            AuditOutcome::Success
        } else {
            AuditOutcome::Failure
        },
        if succeeded {
            PrivilegedPhase::Applied
        } else {
            PrivilegedPhase::Refused
        },
        &audit,
        &broker.config.break_glass.approvers,
    );
    if succeeded {
        audit_admin(
            broker.audit_log.as_ref(),
            ctx,
            "SetTopicFreeze",
            AuditOutcome::Success,
            admin_resources(&target, req.frozen, proposal_id),
        );
    }

    SetTopicFreezeResponse {
        throttle_time_ms: 0,
        error_code: code,
        error_message: message,
        ..SetTopicFreezeResponse::default()
    }
}

/// The resources that the ordinary administrative event names.
///
/// A thaw names the proposal it spent as well as the scope, so a rule that
/// reads the administrative events joins the approval to the transition on that
/// id.
fn admin_resources(target: &str, frozen: bool, proposal_id: Uuid) -> Vec<AuditResource> {
    let mut resources = vec![AuditResource {
        resource_type: "TopicFreeze".to_owned(),
        name: target.to_owned(),
    }];
    if !frozen {
        resources.push(AuditResource {
            resource_type: "break-glass-proposal".to_owned(),
            name: proposal_id.to_string(),
        });
    }
    resources
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf};

    use assert2::{check, let_assert};
    use crabka_metadata::{BreakGlassApproval, BreakGlassProposalRecord};
    use crabka_protocol::{
        krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
        primitives::uuid::Uuid as ProtocolUuid,
    };
    use crabka_security::{AuthMethod, Principal};
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{BreakGlassConfig, FreezeConfig},
        freeze::signing::freeze_signing_bytes,
        operator_keys::{OperatorKeyEntry, OperatorKeys},
    };

    const CLUSTER: Uuid = Uuid::from_u128(0x5150);
    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    const ALICE: &str = "User:alice";
    const ALICE_KEY: &str = "alice-yubi";

    fn image(entries: &[(&str, PatternType)]) -> MetadataImage {
        let mut image = MetadataImage::new(CLUSTER);
        for (scope, pattern_type) in entries {
            image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
                scope: (*scope).to_owned(),
                pattern_type: *pattern_type,
                frozen: true,
                reason: "DR cutover".to_owned(),
                set_by: ALICE.to_owned(),
                set_at_ms: 1_770_000_000_000,
                proposal_id: Uuid::nil(),
                key_id: String::new(),
                signature: Vec::new(),
            }));
        }
        image
    }

    fn principal() -> Principal {
        Principal {
            name: ALICE.to_owned(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "10.0.0.1:51120".parse().expect("peer address")
    }

    fn context<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
        RequestContext::new(
            principal,
            peer,
            "crabka-guard",
            "conn-1",
            false,
            "PLAINTEXT",
        )
    }

    // A broker configuration with alice's operator key loaded.
    fn config_with_alice(dir: &TempDir) -> (BrokerConfig, Ed25519KeyPair) {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse pkcs8");
        let path: PathBuf = dir.path().join("alice.pub");
        std::fs::write(&path, pair.public_key().as_ref()).expect("write public key");
        let keys = OperatorKeys::load(&[OperatorKeyEntry {
            key_id: ALICE_KEY.to_owned(),
            principal: ALICE.to_owned(),
            public_key_path: path,
        }])
        .expect("load trust set");
        let config = BrokerConfig {
            operator_keys: keys,
            ..BrokerConfig::default()
        };
        (config, pair)
    }

    fn freeze_request(scope: &str, pattern_type: i8) -> SetTopicFreezeRequest {
        SetTopicFreezeRequest {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: "DR cutover".to_owned(),
            ..SetTopicFreezeRequest::default()
        }
    }

    // `record` signed by `pair` for the test cluster.
    fn sign(pair: &Ed25519KeyPair, record: &TopicFreezeRecord) -> Vec<u8> {
        let bytes = freeze_signing_bytes(&CLUSTER.to_string(), record);
        pair.sign(&bytes).as_ref().to_vec()
    }

    #[test]
    fn an_unusable_scope_is_refused_as_an_invalid_scope() {
        for (label, pattern_type, scope, expected) in [
            ("a topic name", PatternType::Literal, "orders", None),
            (
                "a namespace prefix",
                PatternType::Prefixed,
                "tenant-a.",
                None,
            ),
            (
                "an empty literal scope",
                PatternType::Literal,
                "",
                Some(codes::FREEZE_SCOPE_INVALID),
            ),
            (
                "an empty prefix scope",
                PatternType::Prefixed,
                "",
                Some(codes::FREEZE_SCOPE_INVALID),
            ),
            (
                "a literal internal topic",
                PatternType::Literal,
                "__consumer_offsets",
                Some(codes::FREEZE_SCOPE_INVALID),
            ),
            (
                "a prefix that reaches the internal namespace",
                PatternType::Prefixed,
                "_",
                Some(codes::FREEZE_SCOPE_INVALID),
            ),
            (
                "a prefix inside the internal namespace",
                PatternType::Prefixed,
                "__con",
                Some(codes::FREEZE_SCOPE_INVALID),
            ),
        ] {
            let outcome = check_scope(pattern_type, scope);
            check!(outcome.err().map(|r| r.code) == expected, "{label}");
        }
    }

    #[test]
    fn the_registry_ceiling_refuses_one_entry_past_it() {
        let image = image(&[
            ("orders", PatternType::Literal),
            ("payments", PatternType::Literal),
        ]);

        for (label, max_entries, expected) in [
            ("room for another entry", 3, None),
            (
                "the registry is full",
                2,
                Some(codes::FREEZE_LIMIT_EXCEEDED),
            ),
            (
                "the registry is over a lowered ceiling",
                1,
                Some(codes::FREEZE_LIMIT_EXCEEDED),
            ),
        ] {
            let outcome = check_limit(&image, max_entries);
            check!(outcome.err().map(|r| r.code) == expected, "{label}");
        }
    }

    #[test]
    fn a_freeze_that_replaces_a_live_entry_does_not_meet_the_ceiling() {
        let dir = TempDir::new().expect("tempdir");
        let (config, _) = config_with_alice(&dir);
        let config = BrokerConfig {
            freeze: FreezeConfig {
                max_entries: 1,
                ..FreezeConfig::default()
            },
            ..config
        };
        let image = image(&[("orders", PatternType::Literal)]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };

        for (label, scope, expected) in [
            ("the same scope replaces its entry", "orders", None),
            (
                "a second scope meets the ceiling",
                "payments",
                Some(codes::FREEZE_LIMIT_EXCEEDED),
            ),
        ] {
            let outcome = prepare(&env, &freeze_request(scope, PATTERN_TYPE_LITERAL));
            check!(outcome.err().map(|r| r.code) == expected, "{label}");
        }
    }

    #[test]
    fn a_live_entry_is_found_by_its_scope_and_its_pattern_type_together() {
        let image = image(&[
            ("orders", PatternType::Literal),
            ("orders", PatternType::Prefixed),
            ("tenant-a.", PatternType::Prefixed),
        ]);

        for (label, pattern_type, scope, expected) in [
            (
                "a literal entry",
                PatternType::Literal,
                "orders",
                Some(PatternType::Literal),
            ),
            (
                "a prefixed entry of the same name",
                PatternType::Prefixed,
                "orders",
                Some(PatternType::Prefixed),
            ),
            (
                "a prefixed entry",
                PatternType::Prefixed,
                "tenant-a.",
                Some(PatternType::Prefixed),
            ),
            (
                "a literal entry that the prefix would cover",
                PatternType::Literal,
                "tenant-a.",
                None,
            ),
            (
                "a scope no entry carries",
                PatternType::Literal,
                "absent",
                None,
            ),
            (
                "an exact lookup never matches a longer name",
                PatternType::Prefixed,
                "tenant-a.orders",
                None,
            ),
        ] {
            check!(
                live_entry(&image, pattern_type, scope).map(|entry| entry.pattern_type) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn the_record_takes_the_authenticated_principal_as_its_author() {
        let req = SetTopicFreezeRequest {
            scope: "orders".to_owned(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: true,
            reason: "DR cutover".to_owned(),
            proposal_id: ProtocolUuid(PROPOSAL.into_bytes()),
            set_at_ms: 1_770_000_000_000,
            key_id: ALICE_KEY.to_owned(),
            signature: vec![0xAB; 64],
            ..SetTopicFreezeRequest::default()
        };

        let expected = TopicFreezeRecord {
            scope: "orders".to_owned(),
            pattern_type: PatternType::Literal,
            frozen: true,
            reason: "DR cutover".to_owned(),
            set_by: ALICE.to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: PROPOSAL,
            key_id: ALICE_KEY.to_owned(),
            signature: vec![0xAB; 64],
        };
        check!(record_of(&req, PatternType::Literal, ALICE) == expected);
    }

    #[test]
    fn an_unsigned_record_takes_the_brokers_clock() {
        let req = SetTopicFreezeRequest {
            // A client that sends a timestamp in the far future cannot park an
            // entry where no later record replaces it.
            set_at_ms: i64::MAX,
            ..freeze_request("orders", PATTERN_TYPE_LITERAL)
        };

        let before = now_ms();
        let record = record_of(&req, PatternType::Literal, ALICE);

        check!(record.set_at_ms >= before);
        check!(record.set_at_ms < i64::MAX);
        check!(record.key_id.is_empty());
        check!(record.signature.is_empty());
    }

    #[test]
    fn a_signature_is_named_by_either_half_of_the_pair() {
        for (label, key_id, signature, expected) in [
            ("neither half", "", Vec::new(), false),
            ("both halves", ALICE_KEY, vec![0xAB; 64], true),
            ("a key with no signature", ALICE_KEY, Vec::new(), true),
            ("a signature with no key", "", vec![0xAB; 64], true),
        ] {
            check!(is_signed(key_id, &signature) == expected, "{label}");
        }
    }

    #[test]
    fn an_unsigned_freeze_is_accepted_by_default_and_refused_under_require_signature() {
        let dir = TempDir::new().expect("tempdir");
        let (base, _) = config_with_alice(&dir);
        let image = image(&[]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);

        for (label, require_signature, expected) in [
            ("the default accepts an unsigned freeze", false, None),
            (
                "require_signature refuses one",
                true,
                Some(codes::OPERATOR_SIGNATURE_REQUIRED),
            ),
        ] {
            let config = BrokerConfig {
                freeze: FreezeConfig {
                    require_signature,
                    ..FreezeConfig::default()
                },
                ..base.clone()
            };
            let env = FreezeEnv {
                config: &config,
                image: &image,
                ctx: &ctx,
            };
            let record = record_of(
                &freeze_request("orders", PATTERN_TYPE_LITERAL),
                PatternType::Literal,
                ALICE,
            );
            let outcome = check_signature(&env, &record, None);
            check!(
                outcome.as_ref().err().map(|r| r.code) == expected,
                "{label}"
            );
            if expected.is_none() {
                check!(outcome == Ok(false), "{label}");
            }
        }
    }

    #[test]
    fn an_unsigned_thaw_is_refused_whatever_require_signature_says() {
        let dir = TempDir::new().expect("tempdir");
        let (base, _) = config_with_alice(&dir);
        let image = image(&[("orders", PatternType::Literal)]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);

        for require_signature in [false, true] {
            let config = BrokerConfig {
                freeze: FreezeConfig {
                    require_signature,
                    ..FreezeConfig::default()
                },
                ..base.clone()
            };
            let env = FreezeEnv {
                config: &config,
                image: &image,
                ctx: &ctx,
            };
            let record = record_of(
                &SetTopicFreezeRequest {
                    frozen: false,
                    proposal_id: ProtocolUuid(PROPOSAL.into_bytes()),
                    ..freeze_request("orders", PATTERN_TYPE_LITERAL)
                },
                PatternType::Literal,
                ALICE,
            );
            let outcome = check_signature(&env, &record, None);
            check!(
                outcome.err().map(|r| r.code) == Some(codes::OPERATOR_SIGNATURE_REQUIRED),
                "require_signature = {require_signature}"
            );
        }
    }

    #[test]
    fn a_signed_freeze_verifies_and_a_tampered_one_answers_one_code() {
        let dir = TempDir::new().expect("tempdir");
        let (config, alice) = config_with_alice(&dir);
        let image = image(&[]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let good = TopicFreezeRecord {
            key_id: ALICE_KEY.to_owned(),
            set_at_ms: now_ms(),
            ..record_of(
                &freeze_request("orders", PATTERN_TYPE_LITERAL),
                PatternType::Literal,
                ALICE,
            )
        };
        let signature = sign(&alice, &good);

        for (label, record, expected) in [
            (
                "the record the operator signed",
                TopicFreezeRecord {
                    signature: signature.clone(),
                    ..good.clone()
                },
                None,
            ),
            (
                "the same signature presented as the thaw",
                TopicFreezeRecord {
                    frozen: false,
                    signature: signature.clone(),
                    ..good.clone()
                },
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
            (
                "another scope under the same signature",
                TopicFreezeRecord {
                    scope: "payments".to_owned(),
                    signature: signature.clone(),
                    ..good.clone()
                },
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
            (
                "a key_id no trust set carries",
                TopicFreezeRecord {
                    key_id: "carol-yubi".to_owned(),
                    signature: signature.clone(),
                    ..good.clone()
                },
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
        ] {
            let outcome = check_signature(&env, &record, None);
            check!(
                outcome.as_ref().err().map(|r| r.code) == expected,
                "{label}"
            );
            if expected.is_none() {
                check!(outcome == Ok(true), "{label}");
            }
        }
    }

    #[test]
    fn a_thaw_with_no_proposal_needs_a_break_glass_approval() {
        let dir = TempDir::new().expect("tempdir");
        let (config, _) = config_with_alice(&dir);
        let image = image(&[("orders", PatternType::Literal)]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let thaw = TopicFreezeRecord {
            frozen: false,
            proposal_id: Uuid::nil(),
            ..record_of(
                &freeze_request("orders", PATTERN_TYPE_LITERAL),
                PatternType::Literal,
                ALICE,
            )
        };

        let outcome = check_approval(&env, &thaw);

        check!(outcome.err().map(|r| r.code) == Some(codes::BREAK_GLASS_APPROVAL_REQUIRED));
    }

    #[test]
    fn a_freeze_needs_no_proposal() {
        let dir = TempDir::new().expect("tempdir");
        let (config, _) = config_with_alice(&dir);
        let image = image(&[]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let record = record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        );

        check!(check_approval(&env, &record) == Ok(None));
    }

    #[test]
    fn a_thaw_spends_the_approved_proposal_that_covers_its_scope() {
        let dir = TempDir::new().expect("tempdir");
        let (base, _) = config_with_alice(&dir);
        let config = BrokerConfig {
            break_glass: BreakGlassConfig {
                approvers: ["User:alice", "User:bob", "User:carol"]
                    .map(str::to_owned)
                    .to_vec(),
                ..BreakGlassConfig::default()
            },
            ..base
        };
        let mut image = image(&[("orders", PatternType::Literal)]);
        image.apply(&MetadataRecord::V1BreakGlassProposal(approved_thaw(
            "literal:orders",
        )));
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let thaw = TopicFreezeRecord {
            frozen: false,
            proposal_id: PROPOSAL,
            ..record_of(
                &freeze_request("orders", PATTERN_TYPE_LITERAL),
                PatternType::Literal,
                ALICE,
            )
        };

        let_assert!(Ok(Some(consumed)) = check_approval(&env, &thaw));

        check!(consumed_proposal_id(&consumed) == Some(PROPOSAL));
        let_assert!(MetadataRecord::V1BreakGlassProposal(proposal) = &consumed);
        check!(proposal.consumed_at_ms > 0);
    }

    #[test]
    fn a_thaw_that_names_another_proposal_is_refused() {
        let dir = TempDir::new().expect("tempdir");
        let (base, _) = config_with_alice(&dir);
        let config = BrokerConfig {
            break_glass: BreakGlassConfig {
                approvers: ["User:alice", "User:bob", "User:carol"]
                    .map(str::to_owned)
                    .to_vec(),
                ..BreakGlassConfig::default()
            },
            ..base
        };
        let mut image = image(&[("orders", PatternType::Literal)]);
        image.apply(&MetadataRecord::V1BreakGlassProposal(approved_thaw(
            "literal:orders",
        )));
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let thaw = TopicFreezeRecord {
            frozen: false,
            proposal_id: Uuid::from_u128(0xDEAD),
            ..record_of(
                &freeze_request("orders", PATTERN_TYPE_LITERAL),
                PatternType::Literal,
                ALICE,
            )
        };

        let outcome = check_approval(&env, &thaw);

        check!(outcome.err().map(|r| r.code) == Some(codes::BREAK_GLASS_APPROVAL_REQUIRED));
    }

    #[test]
    fn a_proposal_for_one_scope_does_not_thaw_another() {
        let dir = TempDir::new().expect("tempdir");
        let (base, _) = config_with_alice(&dir);
        let config = BrokerConfig {
            break_glass: BreakGlassConfig {
                approvers: ["User:alice", "User:bob", "User:carol"]
                    .map(str::to_owned)
                    .to_vec(),
                ..BreakGlassConfig::default()
            },
            ..base
        };
        let mut image = image(&[
            ("orders", PatternType::Literal),
            ("orders", PatternType::Prefixed),
        ]);
        image.apply(&MetadataRecord::V1BreakGlassProposal(approved_thaw(
            "literal:orders",
        )));
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };

        for (label, pattern_type, expected) in [
            ("the scope the proposal names", PatternType::Literal, None),
            (
                "the same name under the other pattern type",
                PatternType::Prefixed,
                Some(codes::BREAK_GLASS_APPROVAL_REQUIRED),
            ),
        ] {
            let thaw = TopicFreezeRecord {
                frozen: false,
                proposal_id: PROPOSAL,
                pattern_type,
                ..record_of(
                    &freeze_request("orders", PATTERN_TYPE_LITERAL),
                    pattern_type,
                    ALICE,
                )
            };
            check!(
                check_approval(&env, &thaw).err().map(|r| r.code) == expected,
                "{label}"
            );
        }
    }

    // A proposal that two distinct principals approved, on `target`.
    fn approved_thaw(target: &str) -> BreakGlassProposalRecord {
        BreakGlassProposalRecord {
            proposal_id: PROPOSAL,
            action: BreakGlassAction::ThawTopicFreeze,
            target: target.to_owned(),
            proposer: ALICE.to_owned(),
            reason: "restore finished".to_owned(),
            created_at_ms: 1,
            expires_at_ms: i64::MAX,
            approvals: vec![
                BreakGlassApproval {
                    principal: "User:bob".to_owned(),
                    approved_at_ms: 2,
                    key_id: "bob-yubi".to_owned(),
                    signature: vec![0xBB; 64],
                },
                BreakGlassApproval {
                    principal: "User:carol".to_owned(),
                    approved_at_ms: 3,
                    key_id: "carol-yubi".to_owned(),
                    signature: vec![0xCC; 64],
                },
            ],
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    #[test]
    fn a_pattern_type_byte_that_names_no_scope_kind_is_an_invalid_request() {
        let dir = TempDir::new().expect("tempdir");
        let (config, _) = config_with_alice(&dir);
        let image = image(&[]);
        let principal = principal();
        let peer = peer();
        let ctx = context(&principal, &peer);
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };

        for (label, byte, expected) in [
            ("literal", PATTERN_TYPE_LITERAL, None),
            ("prefixed", PATTERN_TYPE_PREFIXED, None),
            ("any", 1_i8, Some(codes::INVALID_REQUEST)),
            ("unknown", 0, Some(codes::INVALID_REQUEST)),
            ("match", 2, Some(codes::INVALID_REQUEST)),
            ("a byte no build knows", 9, Some(codes::INVALID_REQUEST)),
        ] {
            let outcome = prepare(&env, &freeze_request("orders", byte));
            check!(outcome.err().map(|r| r.code) == expected, "{label}");
        }
    }

    #[test]
    fn a_thaw_names_the_break_glass_proposal_and_the_scope_in_its_audit_resources() {
        let expected = vec![
            AuditResource {
                resource_type: "TopicFreeze".to_owned(),
                name: "literal:orders".to_owned(),
            },
            AuditResource {
                resource_type: "break-glass-proposal".to_owned(),
                name: PROPOSAL.to_string(),
            },
        ];
        check!(admin_resources("literal:orders", false, PROPOSAL) == expected);
    }

    #[test]
    fn a_freeze_names_only_the_scope_in_its_audit_resources() {
        let expected = vec![AuditResource {
            resource_type: "TopicFreeze".to_owned(),
            name: "prefixed:tenant-a.".to_owned(),
        }];
        check!(admin_resources("prefixed:tenant-a.", true, Uuid::nil()) == expected);
    }
}
