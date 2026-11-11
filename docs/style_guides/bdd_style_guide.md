# BDD Observation Guide

This guide explains how scenario code retains and checks observations. The [Gherkin style guide](gherkin_style_guide.md) is authoritative for formulation, BRIEF, rules, outlines, tags, journeys, step definitions, and the `krabka-bdd` wrapper. Follow that guide when adding or migrating a feature. Keep property tests, model checks, and compiled macro consumers where they prove a different rule.

The examples below use `krabka_bdd::scenarios!`, `Given<T>` state slots, and `Attempt<T, E>` results. They supplement the wrapper conventions rather than define a second scenario style.

## Give each phase one job

| Phase | Responsibility | Example |
| :--- | :--- | :--- |
| Given | Build the state and record the inputs. | Three voters have committed a topic with a specified ID. |
| When | Apply the operation and store its result. | A host crashes, a request arrives, or the test advances its clock. |
| Then | Observe the state and compare it with the specified result. | The surviving voters agree on one leader. |

Use a fresh typed fixture for each scenario. Store inputs separately from observations in `krabka_bdd::Given<T>` slots. A negative-path When returns `Attempt<T, E>` for a later Then: `NotExecuted` means the operation did not run, `Succeeded(value)` records success, and `Failed(error)` retains the typed refusal.

Keep behavior assertions in Then steps. A setup helper can fail if it cannot create a temporary directory or start a required server. Do not treat a setup failure as the expected product refusal. Use `Attempt` for an expected refusal instead of unwrapping the operation or converting its error to a string. Unexpected errors from ordinary successful actions still fail the scenario.

Do not move a complete test body into a Then step. Do not call the operation again in Then to obtain a result that When did not record. A second request, retry, commit, clock advance, crash, or repair changes the system. Keep prerequisites in Given context and give a second causal action its own illustrative scenario. Follow the Gherkin guide for journey tests.

## Record typed results

This example uses the formatter's public UUID parser. The Examples rows supply the input and expected byte count. The fixture retains the actual typed error until Then checks it.

```gherkin
Feature: Kafka UUID refusal context

  Rule: A decoded UUID contains sixteen bytes

    Scenario Outline: A short base64 UUID reports its decoded length
      Given Kafka UUID text <input> has been supplied
      When an operator attempts to parse the UUID text
      Then the UUID refusal reports <decoded_len> decoded bytes

      Examples:
        | case      | input | decoded_len |
        | one-byte  | AA    | 1           |
        | two-bytes | AAA   | 2           |
```

```rust
use {
    krabka_bdd::{Attempt, Given, given, then, when},
    krabka_format::{ClusterId, KafkaUuidError},
    rstest::fixture,
};

struct UuidScenario {
    input: Given<String>,
}

#[fixture]
fn codec() -> UuidScenario {
    UuidScenario {
        input: Given::new("input", "Given Kafka UUID text <input> has been supplied"),
    }
}

#[fixture]
fn parsed() -> Attempt<ClusterId, KafkaUuidError> {
    Attempt::NotExecuted
}

#[given("Kafka UUID text {input} has been supplied")]
fn uuid_text(codec: &mut UuidScenario, input: String) {
    codec.input.set(input);
}

#[when("an operator attempts to parse the UUID text")]
fn parse_uuid(codec: &UuidScenario) -> Attempt<ClusterId, KafkaUuidError> {
    Attempt::of(|| codec.input.get().parse())
}

#[then("the UUID refusal reports {decoded_len} decoded bytes")]
fn wrong_length(
    codec: &UuidScenario,
    parsed: &Attempt<ClusterId, KafkaUuidError>,
    decoded_len: usize,
) {
    let expected = Attempt::Failed(KafkaUuidError::WrongLength {
        input: codec.input.get().clone(),
        decoded_len,
    });
    assert2::assert!(*parsed == expected);
}

krabka_bdd::scenarios!(
    "tests/features/uuid_refusals",
    fixtures = [codec: UuidScenario, parsed: Attempt<ClusterId, KafkaUuidError>],
);
```

For domain errors, compare the variant and its payload. For an API response, compare the whole expected struct where the type supports equality. Check error text when the text itself is part of the product contract.

## Derive expectations from the specification

Use Examples for real data variations of one rule, as the Gherkin guide specifies. Put the reason for each row in the first `case` column, then the relevant inputs and expected results. The case name does not select a hidden Rust test body.

Derive the expected identity, epoch, offset, and ordering from Given inputs or explicit Then parameters. Do not compare only a count when the rule names the records that survive. Do not hard-code the same node or topic ID in an unrelated assertion helper.

Use data tables for structured input or expected records. Give each column a domain name and unit, such as `node id`, `leader epoch`, or `timeout ms`. Use `#[derive(krabka_bdd::TableRow)]` and `krabka_bdd::table::rows` for strict typed input. Use `krabka_bdd::assert_table!` to compare the specified columns, including missing or extra rows. Compare complete structs when the rule specifies the whole record. Preserve order when the rule defines it.

An expected result should be independent of the operation under test. Do not call the same decision function to build both the actual value and its expected value. Use a literal expected struct, a specified rule, a golden fixture, or an independent reference implementation.

Test runtime behavior through public APIs or a narrow internal seam. Macro consumers should compile and exercise the generated API. A token string that contains a trait name does not prove that the generated implementation works.

## Observe asynchronous results without changing them

A Then step can await a watch notification or poll a read-only snapshot. Give the observation a deadline and report the state on failure. A polling helper should only read the state and yield between reads. It should not resend the request, advance the clock, or repair the network.

When a clock or network change triggers the outcome, make it the scenario's explicit When. Keep prior changes in Given context. Use the clock seam that the production code already accepts. Do not replace an observation with an arbitrary wall-clock sleep.

Distinguish an operation timeout from a domain refusal. `tokio::time::timeout` produces an outer `Result`; a request can produce an inner `Result`. `Err(elapsed)` means the operation did not finish. `Ok(Err(RaftError::NotLeader { .. }))` means it answered with a specific refusal. Do not accept both through a single `is_err()` check.

For a request that remains pending, check the forbidden state change during a bounded observation. Give a domain refusal its own deterministic example. Keep any eventual settlement check tied to its specified recovery trigger. Retain the typed outcome instead of treating every error as a valid refusal.

## State the limits of simulation

Record the seed and bound virtual time in simulation tests. Keep stable identities in Given steps when identity matters to the rule. Describe which resources the simulator controls, including network delivery, task scheduling, timers, and storage.

A network seed alone does not seed every runtime decision or the real filesystem. Turmoil can seed Tokio decisions when compiled with `tokio_unstable`. Real temporary directories do not simulate torn writes or disk failure. See the [Turmoil test instructions](../../CONTRIBUTING.md#turmoil-kraft-tests) for this repository's replay command and scope.

## Preserve test execution

Use `krabka_bdd::scenarios!` for new and migrated features. Its runner awaits async steps on one runtime for the whole scenario. Select the runtime flavor explicitly when the scenario needs multiple worker threads or a paused Tokio clock. Synchronous steps can drive a fixture-owned runtime without nesting `block_on` inside an async step.

Preserve the original Tokio runtime flavor and worker count. Preserve platform conditions, feature gates, ignored external-service tests, and required test data. Use the wrapper's World or after hooks for cleanup that needs to run on failure. Keep shutdown and directory lifetimes intact. A BDD change should not silently enable a Docker test or omit a previous assertion.

Add feature files and the crate's `Cargo.toml` to Bazel `compile_data`. When adding a feature to a directory the macro reads, touch the Rust file that binds it so Cargo sees the new input. Run the affected scenarios through Cargo or nextest and the relevant Bazel targets. Check feature lint findings against the existing Gherkin baseline without adding new exemptions. Keep model-check bounds and verification inventories when a model changes. Follow [CONTRIBUTING](../../CONTRIBUTING.md) for the validation commands.

## Review a scenario

- Can a reviewer identify the inputs, operation, and expected result from the feature alone?
- Does When retain the operation's typed result for Then?
- Does Then observe the result without repeating or changing the operation?
- Do negative cases distinguish the specified refusal from timeout, setup failure, and unrelated errors?
- Do assertions check the specified identities, values, and order through an independent expected result?
- Does the scenario keep the original runtime, cleanup, platform, and external-service requirements?
