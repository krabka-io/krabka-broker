//! The steps that set, lift and sign a freeze, and the registry checks that
//! follow them.

use {
    super::{
        driver::{self, Signing},
        params::{KeySource, Lifetime, Operator, Outcome, Scope},
        world::GuardScenario,
    },
    crate::signing::{freeze_signing_bytes, freeze_signing_input},
    assert2::assert,
    krabka_bdd::TokioMultiThreadContext,
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
fn record_freeze(
    guard: &mut GuardScenario,
    runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: &Scope,
    signing: Signing,
) {
    let cluster = guard.cluster(runtime);
    let outcome = runtime.block_on(cluster.freeze(operator, scope, signing));
    guard.outcome.set(outcome);
}

/// `operator` thaws `scope` with the scenario's proposal, and the outcome is
/// recorded.
fn record_thaw(
    guard: &mut GuardScenario,
    runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: &Scope,
    signing: Signing,
) {
    let proposal = *guard.proposal.get();
    let cluster = guard.cluster(runtime);
    let outcome = runtime.block_on(cluster.thaw(operator, scope, proposal, signing));
    guard.outcome.set(outcome);
}

/// Thaws carry the acting operator's own signature.
fn own_signature(operator: Operator) -> Signing { Signing::With(KeySource::OperatorKey(operator)) }

/// The one registry entry on `scope`.
fn registry_entry(
    guard: &mut GuardScenario,
    runtime: &TokioMultiThreadContext,
    scope: &Scope,
) -> DescribedTopicFreeze {
    let cluster = guard.cluster(runtime);
    runtime
        .block_on(cluster.registry())
        .into_iter()
        .find(|entry| entry.pattern_type == scope.pattern_type() && entry.scope == scope.name())
        .expect("the registry holds a freeze on the scope")
}

#[when("{operator} freezes {scope}")]
fn freezes(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    record_freeze(guard, runtime, operator, &scope, Signing::Unsigned);
}

/// With no broker at the bootstrap address, the shipped entry point runs, so
/// the connect step decides the outcome.
#[when("{operator} tries to freeze {scope}")]
fn tries_to_freeze(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    if guard.unreachable {
        let args = [
            "freeze",
            "set",
            scope.flag(),
            scope.name(),
            "--reason",
            driver::REASON,
        ];
        let outcome = runtime.block_on(driver::run_binary(UNREACHABLE_BOOTSTRAP, &args));
        guard.outcome.set(outcome);
    } else {
        record_freeze(guard, runtime, operator, &scope, Signing::Unsigned);
    }
}

#[when("{operator} signs a freeze of {scope} with {key}")]
fn signs_freeze(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
    key: KeySource,
) {
    record_freeze(guard, runtime, operator, &scope, Signing::With(key));
}

#[when("{operator} tries to sign a freeze of {scope} with {key}")]
fn tries_to_sign_freeze(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
    key: KeySource,
) {
    record_freeze(guard, runtime, operator, &scope, Signing::With(key));
}

#[when("{operator} thaws {scope} with that proposal")]
fn thaws(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    record_thaw(guard, runtime, operator, &scope, own_signature(operator));
}

#[when("{operator} tries to thaw {scope} with that proposal")]
fn tries_to_thaw(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    record_thaw(guard, runtime, operator, &scope, own_signature(operator));
}

#[when("{operator} tries to thaw {scope} with the same proposal")]
fn tries_to_thaw_again(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    record_thaw(guard, runtime, operator, &scope, own_signature(operator));
}

#[when("{operator} tries to thaw {scope} with that proposal without signing")]
fn tries_to_thaw_unsigned(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    record_thaw(guard, runtime, operator, &scope, Signing::Unsigned);
}

/// The operator freezes the scope, opens a thaw proposal for it, the two
/// other operators approve it, and the operator spends it on a signed thaw.
#[given("{operator} has lifted a freeze on {scope} with an approved proposal")]
fn has_lifted(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    let cluster = guard.cluster(runtime);
    let proposal = runtime.block_on(async {
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
        assert!(thawed == Outcome::Success);
        proposal
    });
    guard.proposal.set(proposal);
}

#[given("{operator} has refrozen {scope}")]
fn has_refrozen(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
    scope: Scope,
) {
    let cluster = guard.cluster(runtime);
    assert!(
        runtime.block_on(cluster.freeze(operator, &scope, Signing::Unsigned)) == Outcome::Success
    );
}

/// The time an entry was set is not knowable, so it is zeroed before the
/// whole registry is compared.
#[then("the registry holds an unsigned freeze on {scope} set by {operator}")]
fn registry_holds_unsigned_freeze(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    scope: Scope,
    operator: Operator,
) {
    let cluster = guard.cluster(runtime);
    let registry: Vec<_> = runtime
        .block_on(cluster.registry())
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

#[then("the registry still holds the freeze on {scope}")]
fn registry_still_holds_freeze(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    scope: Scope,
) {
    let cluster = guard.cluster(runtime);
    let scopes: Vec<_> = runtime
        .block_on(cluster.registry())
        .into_iter()
        .map(|entry| (entry.pattern_type, entry.scope))
        .collect();
    assert!(scopes == vec![(scope.pattern_type(), scope.name().to_owned())]);
}

#[then(
    "the freeze on {scope} names {owner}'s key and carries a signature that {verifier}'s public \
     key verifies"
)]
fn freeze_carries_verified_signature(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    scope: Scope,
    owner: Operator,
    verifier: Operator,
) {
    let entry = registry_entry(guard, runtime, &scope);
    let cluster = guard.cluster(runtime);
    let cluster_id = runtime.block_on(cluster.cluster_id());
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
fn private_key_stayed_local(
    guard: &mut GuardScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    operator: Operator,
) {
    let cluster = guard.cluster(runtime);
    let private = fs::read(&cluster.key(operator).pkcs8).expect("read the private key");
    let stored: Vec<u8> = runtime
        .block_on(cluster.registry())
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
