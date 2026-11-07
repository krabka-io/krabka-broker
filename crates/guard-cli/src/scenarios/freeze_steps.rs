//! The steps that set, lift and sign a freeze, and the registry checks that
//! follow them.

use {
    super::{
        common_steps::freeze_succeeds,
        driver::{self, Signing},
        params::{Attempt, KeySource, Lifetime, Operator, Outcome, Scope},
        world::GuardScenario,
    },
    crate::signing::{freeze_signing_bytes, freeze_signing_input},
    assert2::assert,
    krabka_protocol::krabka::freeze::DescribedTopicFreeze,
    ring::signature::{ED25519, UnparsedPublicKey},
    rstest_bdd_macros::{given, then, when},
    std::fs,
};

/// A bootstrap address where no broker listens: nothing binds port 1.
const UNREACHABLE_BOOTSTRAP: &str = "127.0.0.1:1";

/// The `--target` a thaw proposal names for `scope`, with its pattern type.
fn thaw_target(scope: &Scope) -> String {
    match scope {
        Scope::Topic(name) => format!("literal:{name}"),
        Scope::Prefix(prefix) => format!("prefixed:{prefix}"),
    }
}

/// `operator` freezes `scope`, and the outcome is recorded.
async fn record_freeze(
    guard: &mut GuardScenario,
    operator: Operator,
    scope: &Scope,
    signing: Signing,
) {
    let cluster = guard.cluster().await;
    let outcome = cluster.freeze(operator, scope, signing).await;
    guard.outcome.set(outcome);
}

/// `operator` thaws `scope` with the scenario's proposal, and the outcome is
/// recorded.
async fn record_thaw(
    guard: &mut GuardScenario,
    operator: Operator,
    scope: &Scope,
    signing: Signing,
) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster().await;
    let reported = cluster.thaw(operator, scope, proposal, signing).await;
    guard.outcome.set(reported.outcome);
    guard.stderr = reported.stderr;
}

/// Thaws carry the acting operator's own signature.
fn own_signature(operator: Operator) -> Signing { Signing::With(KeySource::OperatorKey(operator)) }

/// The one registry entry on `scope`.
async fn registry_entry(guard: &mut GuardScenario, scope: &Scope) -> DescribedTopicFreeze {
    let cluster = guard.cluster().await;
    cluster
        .registry()
        .await
        .into_iter()
        .find(|entry| entry.pattern_type == scope.pattern_type() && entry.scope == scope.name())
        .expect("the registry holds a freeze on the scope")
}

#[when("{operator} freezes {scope}")]
async fn freezes(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    record_freeze(guard, operator, &scope, Signing::Unsigned).await;
}

/// With no broker at the bootstrap address, the shipped entry point runs, so
/// the connect step decides the outcome.
#[when("{operator} tries to freeze {scope}")]
async fn tries_to_freeze(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    if guard.unreachable {
        let args = [
            "freeze",
            "set",
            scope.flag(),
            scope.name(),
            "--reason",
            driver::REASON,
        ];
        let outcome = driver::run_binary(UNREACHABLE_BOOTSTRAP, &args).await;
        guard.outcome.set(outcome);
    } else {
        record_freeze(guard, operator, &scope, Signing::Unsigned).await;
    }
}

/// A signed freeze, whether the scenario expects it to land or to be refused:
/// `signs` and `tries to sign` record the outcome the same way.
#[when("{operator} {attempt} a freeze of {scope} with {key}")]
async fn signs_freeze(
    guard: &mut GuardScenario,
    operator: Operator,
    _attempt: Attempt,
    scope: Scope,
    key: KeySource,
) {
    record_freeze(guard, operator, &scope, Signing::With(key)).await;
}

#[when("{operator} thaws {scope} with that proposal")]
async fn thaws(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    record_thaw(guard, operator, &scope, own_signature(operator)).await;
}

#[when("{operator} tries to thaw {scope} with that proposal")]
async fn tries_to_thaw(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    record_thaw(guard, operator, &scope, own_signature(operator)).await;
}

#[when("{operator} tries to thaw {scope} with the same proposal")]
async fn tries_to_thaw_again(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    record_thaw(guard, operator, &scope, own_signature(operator)).await;
}

#[when("{operator} tries to thaw {scope} with that proposal without signing")]
async fn tries_to_thaw_unsigned(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    record_thaw(guard, operator, &scope, Signing::Unsigned).await;
}

/// The operator freezes the scope, opens a thaw proposal for it, the two
/// other operators approve it, and the operator spends it on a signed thaw.
#[given("{operator} has lifted a freeze on {scope} with an approved proposal")]
async fn has_lifted(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    let cluster = guard.cluster().await;
    let proposal = async {
        assert!(cluster.freeze(operator, &scope, Signing::Unsigned).await == Outcome::Success);
        let (outcome, proposal) = cluster
            .propose(
                operator,
                "thaw-topic-freeze",
                &thaw_target(&scope),
                &Lifetime::Default,
            )
            .await;
        assert!(outcome == Outcome::Success);
        let proposal = proposal.expect("the propose opened a proposal");
        for approver in Operator::ALL
            .into_iter()
            .filter(|approver| *approver != operator)
        {
            assert!(cluster.approve(approver, proposal, None).await == Outcome::Success);
        }
        let thawed = cluster
            .thaw(operator, &scope, proposal, own_signature(operator))
            .await;
        assert!(thawed.outcome == Outcome::Success);
        proposal
    }
    .await;
    guard.proposal.set(proposal);
}

#[given("{operator} has refrozen {scope}")]
async fn has_refrozen(guard: &mut GuardScenario, operator: Operator, scope: Scope) {
    freeze_succeeds(guard, operator, &scope).await;
}

/// The time an entry was set is not knowable, so it is zeroed before the
/// whole registry is compared.
#[then("the registry holds an unsigned freeze on {scope} set by {operator}")]
async fn registry_holds_unsigned_freeze(
    guard: &mut GuardScenario,
    scope: Scope,
    operator: Operator,
) {
    let cluster = guard.cluster().await;
    let registry: Vec<_> = cluster
        .registry()
        .await
        .into_iter()
        .map(|entry| DescribedTopicFreeze {
            set_at_ms: 0,
            ..entry
        })
        .collect();
    let expected = vec![DescribedTopicFreeze {
        scope: scope.name().to_owned(),
        pattern_type: scope.pattern_type(),
        reason: driver::REASON.to_owned(),
        set_by: operator.principal(),
        ..DescribedTopicFreeze::default()
    }];
    assert!(registry == expected);
}

/// The broker's refusal and the tool's own words both name the scope, so an
/// operator reads that they asked for the wrong scope, not that a broker failed.
#[then("the tool explains that the registry holds no freeze on {scope}")]
fn tool_explains_no_freeze(guard: &GuardScenario, scope: Scope) {
    let target = thaw_target(&scope);
    let expected = format!(
        "error 42 (the registry holds no freeze at this exact scope, so there is nothing to \
         thaw): {target} has no live freeze entry, so there is nothing to thaw"
    );
    assert!(guard.stderr.as_deref() == Some(expected.as_str()));
}

#[then("the registry still holds the freeze on {scope}")]
async fn registry_still_holds_freeze(guard: &mut GuardScenario, scope: Scope) {
    let cluster = guard.cluster().await;
    let scopes: Vec<_> = cluster
        .registry()
        .await
        .into_iter()
        .map(|entry| (entry.pattern_type, entry.scope))
        .collect();
    assert!(scopes == vec![(scope.pattern_type(), scope.name().to_owned())]);
}

#[then(
    "the freeze on {scope} names {owner}'s key and carries a signature that {verifier}'s public \
     key verifies"
)]
async fn freeze_carries_verified_signature(
    guard: &mut GuardScenario,
    scope: Scope,
    owner: Operator,
    verifier: Operator,
) {
    let entry = registry_entry(guard, &scope).await;
    let cluster = guard.cluster().await;
    let cluster_id = cluster.cluster_id().await;
    let public = fs::read(&cluster.key(verifier).public).expect("read the public key");
    let message = freeze_signing_bytes(
        &freeze_signing_input()
            .cluster_id(&cluster_id)
            .entry(&entry)
            .frozen(true)
            .call(),
    );
    assert!(entry.key_id == owner.key_id());
    let verified = UnparsedPublicKey::new(&ED25519, &public).verify(&message, &entry.signature);
    assert!(verified.is_ok(), "the stored signature does not verify");
}

/// No 16-byte window of the private key file occurs in any byte the
/// registry stores, so the signature is the only signing material that left
/// the operator's machine.
#[then("no part of {operator}'s private key reached the broker")]
async fn private_key_stayed_local(guard: &mut GuardScenario, operator: Operator) {
    let cluster = guard.cluster().await;
    let private = fs::read(&cluster.key(operator).pkcs8).expect("read the private key");
    let stored: Vec<u8> = cluster
        .registry()
        .await
        .into_iter()
        .flat_map(|entry| {
            [
                entry.scope.into_bytes(),
                entry.reason.into_bytes(),
                entry.set_by.into_bytes(),
                entry.key_id.into_bytes(),
                entry.signature,
            ]
        })
        .flatten()
        .collect();
    assert!(!stored.is_empty(), "the registry stores nothing to inspect");
    let leaked = private
        .windows(16)
        .any(|window| stored.windows(16).any(|candidate| candidate == window));
    assert!(!leaked, "the registry stores part of the private key");
}
