//! `#[derive(StepParam)]` as a step definition uses it: round trips over every
//! word, aliases, the unknown-word message, and a real rstest-bdd placeholder.

#![cfg(test)]

use {
    assert2::assert,
    krabka_bdd::{StepParam, StepParamError},
    rstest::rstest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, StepParam)]
enum CleanupPolicy {
    Delete,
    Compact,
    #[step_param("compact,delete")]
    CompactAndDelete,
    #[step_param("none", "off")]
    NoCleanup,
    LogCompaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, StepParam)]
enum Protocol {
    HTTPServer,
    V2Topic,
}

#[rstest]
#[case::delete(CleanupPolicy::Delete, "delete")]
#[case::compact(CleanupPolicy::Compact, "compact")]
#[case::explicit_word(CleanupPolicy::CompactAndDelete, "compact,delete")]
#[case::primary_word(CleanupPolicy::NoCleanup, "none")]
#[case::kebab_case(CleanupPolicy::LogCompaction, "log-compaction")]
fn cleanup_policy_round_trips(#[case] policy: CleanupPolicy, #[case] word: &str) {
    assert!(word.parse::<CleanupPolicy>() == Ok(policy));
    assert!(policy.to_string() == word);
}

#[rstest]
#[case::acronym(Protocol::HTTPServer, "http-server")]
#[case::digit(Protocol::V2Topic, "v2-topic")]
fn protocol_round_trips(#[case] protocol: Protocol, #[case] word: &str) {
    assert!(word.parse::<Protocol>() == Ok(protocol));
    assert!(protocol.to_string() == word);
}

#[test]
fn an_alias_parses_but_display_writes_the_primary_word() {
    assert!("off".parse::<CleanupPolicy>() == Ok(CleanupPolicy::NoCleanup));
    assert!(CleanupPolicy::NoCleanup.to_string() == "none");
}

#[test]
fn display_honours_width_and_alignment() {
    assert!(format!("[{:>8}]", CleanupPolicy::Delete) == "[  delete]");
}

#[rstest]
#[case::typo("compcat")]
#[case::wrong_case("Delete")]
#[case::variant_name("CompactAndDelete")]
#[case::empty("")]
fn an_unknown_word_lists_every_accepted_word(#[case] word: &str) {
    const ACCEPTED: &[&str] = &[
        "delete",
        "compact",
        "compact,delete",
        "none",
        "off",
        "log-compaction",
    ];
    assert!(
        word.parse::<CleanupPolicy>() == Err(StepParamError::new("CleanupPolicy", word, ACCEPTED))
    );
}

#[test]
fn the_unknown_word_message_reads_as_a_fix() {
    let error = "compcat".parse::<CleanupPolicy>().unwrap_err();
    assert!(
        error.to_string()
            == "unknown CleanupPolicy `compcat`: expected one of `delete`, `compact`, \
                `compact,delete`, `none`, `off`, `log-compaction`"
    );
}

struct PolicyScenario {
    policy: Option<CleanupPolicy>,
    text: Option<String>,
}

#[rstest::fixture]
fn policy_scenario() -> PolicyScenario {
    PolicyScenario {
        policy: None,
        text: None,
    }
}

#[rstest_bdd_macros::given("a log has cleanup policy {policy}")]
fn given_policy(policy_scenario: &mut PolicyScenario, policy: CleanupPolicy) {
    policy_scenario.policy = Some(policy);
}

#[rstest_bdd_macros::when("the cleanup policy is written back as step text")]
fn write_back_policy(policy_scenario: &mut PolicyScenario) {
    let policy = policy_scenario.policy.expect("cleanup policy given");
    policy_scenario.text = Some(policy.to_string());
}

#[rstest_bdd_macros::then("the step text is {text}")]
fn step_text_is(policy_scenario: &PolicyScenario, text: String) {
    assert!(policy_scenario.text.as_deref() == Some(text.as_str()));
}

rstest_bdd_macros::scenarios!(
    "tests/features/step_param",
    fixtures = [policy_scenario: PolicyScenario]
);
