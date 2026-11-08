//! The steps of registry verification and of the exit-code contract: what the
//! registry holds, which keys the verifying machine has, and how each broker
//! answer reaches a runbook.

use {
    super::{
        driver::Signing,
        params::{BrokerAnswer, KeySource, LocalKeys, Operator, Outcome, RegistryContents, Scope},
        world::GuardScenario,
    },
    crate::{code_name, exit_for_code},
    assert2::assert,
    rstest_bdd_macros::{given, then, when},
};

/// The freezes that make up `entries`, each as who sets it, on what, and how.
fn freezes_in(entries: RegistryContents) -> Vec<(Operator, &'static str, Signing)> {
    let signed = |operator| Signing::With(KeySource::OperatorKey(operator));
    let alice_signed = (
        Operator::Alice,
        "the payments topic",
        signed(Operator::Alice),
    );
    match entries {
        RegistryContents::AliceSigned => vec![alice_signed],
        RegistryContents::AliceSignedAndUnsigned => {
            vec![
                alice_signed,
                (Operator::Bob, "the invoices topic", Signing::Unsigned),
            ]
        }
        RegistryContents::Unsigned => {
            vec![(Operator::Alice, "the payments topic", Signing::Unsigned)]
        }
        RegistryContents::AliceAndCarolSigned => {
            vec![
                alice_signed,
                (
                    Operator::Carol,
                    "the invoices topic",
                    signed(Operator::Carol),
                ),
            ]
        }
    }
}

#[given("the registry holds {entries}")]
async fn registry_holds(guard: &mut GuardScenario, entries: RegistryContents) {
    let cluster = guard.cluster().await;
    for (operator, scope, signing) in freezes_in(entries) {
        let scope: Scope = scope.parse().expect("a scope the driver names");
        assert!(cluster.freeze(operator, &scope, signing).await.outcome == Outcome::Success);
    }
}

#[given("the operator's machine holds {keys}")]
async fn machine_holds(guard: &mut GuardScenario, keys: LocalKeys) {
    let trust_file = guard.cluster().await.trust_file(keys);
    guard.trust_file.set(trust_file);
}

#[given("no broker listens at the bootstrap address")]
fn no_broker_listens(guard: &mut GuardScenario) { guard.unreachable = true; }

#[when("the operator verifies the registry")]
async fn operator_verifies(guard: &mut GuardScenario) {
    let trust_file = guard.trust_file.get().clone();
    let cluster = guard.cluster().await;
    let run = cluster.verify(Operator::Alice, &trust_file).await;
    guard.record(run);
}

#[when("the broker answers a request with {answer}")]
fn broker_answers(guard: &mut GuardScenario, answer: BrokerAnswer) { guard.answer.set(answer); }

#[then("the tool exits with code {code}")]
fn tool_exits_with(guard: &GuardScenario, code: i32) {
    assert!(exit_for_code(guard.answer.get().code()) == code);
}

#[then("the operator reads {explanation}")]
fn operator_reads(guard: &GuardScenario, explanation: String) {
    assert!(code_name(guard.answer.get().code()) == explanation);
}
