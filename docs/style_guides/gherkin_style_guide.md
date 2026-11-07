# Gherkin Style Guide

This guide defines how Krabka writes Gherkin `.feature` files and the step definitions behind them. It follows the practices of *Effective Behavior-Driven Development* (Nagy and Rose): discovery, then formulation, then automation, one small increment at a time. The other guides still apply. Step-definition code follows the [code style guide](code_style_guide.md), and the text of a feature file follows the [prose style guide](prose_style_guide.md) where this guide is silent.

## What a scenario is for

A scenario is documentation first and a test second. It illustrates one business rule with one concrete example, in words a Kafka operator or a client author can read. Automation is how the documentation stays true: a failing scenario means the behavior is not built yet, the code has a defect, or the documentation is out of date. Triage it as one of those three.

For Krabka the "business" is the Kafka contract. The domain words are the ones in the KIPs, the Kafka docs and the JVM admin tools: topic, partition, leader epoch, log start offset, high watermark, consumer group, producer epoch, quota, ACL. A scenario that a Kafka committer could not read without opening the Rust code is not finished.

## The BDD loop

1. **Discover.** Before writing code, list the rules for the change and one or more concrete examples for each. Per rule, find a typical example, a counter-example, and an example that sits exactly on each boundary. Write down open questions with an owner rather than guessing a rule. If Kafka's behavior is the question, check the latest released cp-kafka image, as [`CLAUDE.md`](../../CLAUDE.md) says.
2. **Formulate.** Turn each agreed example into a scenario that follows BRIEF, below. Get it reviewed as a specification, separately from the code if you can.
3. **Automate, test first.** Write the step definitions and run the scenario. It must fail, and fail for the right reason, before the production code makes it pass. For behavior that already exists, show that the scenario can fail. Break the behavior on purpose, see the scenario go red with a clear message, then undo your own edit directly.

Scenarios written after the fact for existing code are knowledge recovery, not BDD. They are still worth writing for an area that is about to change, for core behavior, and for code that changes often. Do not try to cover a whole crate.

## BRIEF

Every illustrative scenario follows the six BRIEF principles.

- **Business language.** Use domain words only. Do not write Rust type names, function names, module paths, struct fields, file names or byte offsets in a step unless the domain itself uses them, as Kafka does for `__consumer_offsets` or `log.retention.ms`.
- **Real data where it matters.** For each value, ask "would the outcome change if this value changed?" If yes, keep the real value. If no, replace it with a descriptive phrase or drop it. Readers assume every number matters and search for why.
- **Intention revealing.** Steps say what the actor wants to achieve, not how the code does it. Write "a producer appends a batch past the segment size", not "call `Log::append` with 1025 bytes".
- **Essential.** Delete every step, value and assertion that does not illustrate this scenario's rule. Do not restate defaults. A scenario with no `Given` is fine.
- **Focused.** One rule per scenario. Group the scenarios for a rule under a `Rule:` heading. See [Rule blocks](#rule-blocks) for the current tool gap.
- **Brief.** Aim for five steps or fewer.

## Phrasing

- **Exactly one `When`.** It is the action that directly causes the outcome. Other actions become `Given` context. If two actions look equally causal, find out which one the code actually reacts to.
- **Write it upwards.** Write the `Then` first, then the `When` that causes it, then only the `Given` steps that cause-and-effect needs.
- **Tense.** `Given` describes a state, in the past tense or the passive: "a partition has been truncated to offset 40". `When` is in the present: "a follower fetches from offset 42". `Then` states the outcome as a confident present-tense fact: "the leader returns OFFSET_OUT_OF_RANGE". Krabka does not write "should" in a `Then`. The prose guide reserves `should` for advice that nothing enforces, and an automated `Then` is enforced.
- **No "or" inside a step.** It makes the example non-deterministic. Write two scenarios.
- **Every "and" inside a step is a smell.** In a `Given`, check that each part is essential. In a `When`, part of it is usually context, or the step is missing an abstraction. In a `Then`, it is usually two rules, or an unnamed combined outcome.
- **Third person.** Name the actor by role: "a consumer", "the controller", "an operator", "a follower broker". Never write "I". Use "they" only when there is one actor.
- **Introduce, then refer.** Use "a" or "an" to introduce an entity and "the" to refer back to it: "Given a topic with three partitions ... Then the topic has a leader for every partition".
- **No quotation marks** around values. They hurt reading. A step definition can parse an unquoted value.
- **No UI or mechanism words.** In Krabka this means no "call", "invoke", "construct", "instantiate", "mock", "struct" or "field". Describe the request, the record or the state.

## Names

- A scenario name completes the sentence "The one where ...", in domain terms: "Follower fetch past the high watermark is refused". Do not write "Test 3", "Happy path" or a ticket number.
- A `Feature:` name describes a capability: "Log segments roll on size and age". Do not name a feature after a user story ("As an operator I want ...") or a ticket.
- Rename a scenario when cleanup changes what it illustrates.

## Feature files and folders

- Organize feature files by stable capability, never one file per story, PR or ticket. Add a scenario to an existing feature before you create a new one.
- Feature files live in the crate that owns the behavior, under `tests/features/<capability>/<capability>.feature`. A capability directory can hold more than one file.
- A crate with features adds `glob(["tests/features/**/*.feature"])` to the `compile_data` of its `crate_tests` call in `BUILD.bazel`, so the Bazel test sandbox can read them. A published crate also lists `tests/features/**/*.feature` in its `include`.
- Document a shared behavior once and refer to it. Gherkin has no links, so list a dependency in the feature description by name and relative path.

## Rule blocks

Group the scenarios that illustrate one rule under a `Rule:` heading, and state the rule as a sentence. Scenarios that belong to no rule come before the first `Rule:`.

```gherkin
Feature: Producers are fenced by epoch

  Rule: A producer with an older epoch is fenced

    Scenario: Append from a stale producer epoch is refused
      Given a transactional producer has been bumped to epoch 5
      When it appends a batch with epoch 4
      Then the append fails with INVALID_PRODUCER_EPOCH

  Rule: A new epoch resets the sequence

    Scenario: First batch of a new epoch starts at sequence zero
      Given a producer has appended up to sequence 7 in epoch 2
      When it appends sequence 0 in epoch 3
      Then the append is accepted
```

> **Tool gap.** `rstest_bdd_macros::scenarios!` and `#[scenario]` in rstest-bdd 0.6 do not run scenarios that sit under a `Rule:`. They skip them without an error, so a red scenario under a `Rule:` reports nothing. Until Krabka's BDD macro layer supports `Rule:`, do not use `Rule:` in a feature that those macros run. State the rule in the feature description instead, and keep one rule per feature file where you can.

## Scenario outlines and tables

- Use a `Scenario Outline` only for real data variations: calculations, boundary sets, protocol versions. Start with plain scenarios, and compress them into an outline only if the outline is easier to read.
- Make the first column a description of why the row exists: `| case | ... |`. A column of row numbers is not a description.
- Every row must teach something different. Remove rows that are equivalent to another row. If rows illustrate different rules, such as accepted and rejected inputs, split them into two outlines with different `Then` steps.
- Keep a data table small enough to read on one screen, with aligned pipes.

## Background

Do not add a `Background`. It hides context from the reader, and an edit to it silently changes every scenario in the file. Prefer readable repetition. If a shared `Given` appears in many scenarios, first ask whether it is essential at all. Often the right fix is to delete it, because it restates a default. Do not add a `Background` to an existing file, and remove one when you already edit the file for another reason.

## Tags

A tag is an index entry that a reader can understand. Every tag must earn its place.

| Tag | Meaning |
| :--- | :--- |
| `@docker` | The scenario needs a container, such as a cp-kafka oracle. The crate's `crate_tests` call runs it under `--config=docker`. |
| `@journey` | An end-to-end flow across many rules. See [Journeys](#journeys). |
| `@formulated` | Reviewed and agreed, automation not started. Excluded from the gating run. |
| `@inprogress` | Automation or production code is incomplete. Excluded from the gating run. |

- Do not add `@done`. The absence of a lifecycle tag means done.
- Do not add tags that only the automation understands, such as `@use_mock_store`. Do not add ticket or story IDs.
- Add a new tag to this table, with its meaning, in the same change that first uses it.

## Journeys

A journey scenario shows how an actor reaches a goal across many rules, for example "a consumer group rebalances after a member leaves and resumes from committed offsets". Journeys are not BRIEF or focused, and their assertions are loose.

- Tag them `@journey`, keep them in their own `journeys/` capability directory, and write their steps with `*`.
- Never use `*` in an illustrative scenario.
- Keep the set small. Automate a journey only when the illustrative scenarios for every feature it touches are automated.

## Step definitions

Krabka runs scenarios with [rstest-bdd](https://docs.rs/rstest-bdd). The house shape is:

- Step definitions live in a `#[cfg(test)]` module of the crate that owns the behavior, or in a `tests/` support module for public behavior. The [code style guide](code_style_guide.md#tests) decides which.
- Scenario state is a scenario-scoped `#[rstest::fixture]` struct. Do not share mutable state through statics.
- A step definition makes testing decisions only. Put "how to drive the system" in a small driver or helper type named in domain terms, and put setup shortcuts there too. Seed state directly instead of replaying a long flow to reach it.
- Use the domain's types for parameters. Parse a value into its enum or newtype once, in one place, not by matching strings inside each step.
- A negative-path step says "attempts to" or "tries to". It captures the result in the scenario state instead of panicking, and a `Then` step checks it.
- Assertions are atomic. Compare the whole expected value with `assert2::assert!`, so one failure reports every actual value. See [`CLAUDE.md`](../../CLAUDE.md).
- Group step definitions by domain area, not by feature file, and reuse a step only when its wording fits the new scenario.

### rstest-bdd gaps to know

- Scenarios under `Rule:` do not run. See [Rule blocks](#rule-blocks).
- Cargo does not see a new `.feature` file in a directory that `scenarios!` reads until the Rust file that calls the macro changes. After you add a feature file, touch that file, or the new scenarios do not exist. Bazel is not affected, because the `compile_data` glob is a build input.
- Outline rows get test names `case_1`, `case_2` and so on, not the description column. Use the description column anyway, because it is for the reader.

## Review checklist

- [ ] The name is intention revealing and completes "The one where ...".
- [ ] It illustrates exactly one rule, and that rule is written down.
- [ ] Five steps or fewer.
- [ ] Exactly one `When`, and it is the true trigger of the outcome.
- [ ] Every `Given` is essential. None narrates setup or restates a default.
- [ ] Domain words only. No Rust names or mechanism words.
- [ ] Real values only where they change the outcome.
- [ ] No "or" in a step, and every "and" is justified.
- [ ] Third person, no "I", no quotation marks.
- [ ] Outlines have a leading description column and no redundant rows.
- [ ] No `Background`. Tags are in the table above.
- [ ] The scenario was seen to fail before it passed.

## Applying these conventions

The same rule applies here as in the [code style guide](code_style_guide.md#applying-these-conventions). Krabka has hundreds of feature files written before this guide. Do not make style-only sweeps across them. Bring a feature file into line when you already change its behavior or its scenarios, and keep the tidy-up proportionate to the change.
