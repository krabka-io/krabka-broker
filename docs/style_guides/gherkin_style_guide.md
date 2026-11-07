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
- **Focused.** One rule per scenario. Group the scenarios for a rule under a `Rule:` heading. See [Rule blocks](#rule-blocks).
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

Group the scenarios that illustrate one rule under a `Rule:` heading, and state the rule as a sentence. Once a file has a `Rule:`, every scenario in it sits under one, except a `@journey`. `krabka_bdd::scenarios!` rejects a scenario outside a rule in such a file (`scenario-outside-rule`).

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

Scenarios under a `Rule:` run like any other. Their steps run in the order feature `Background`, rule `Background`, scenario steps; they inherit the rule's tags; and their test names include the rule: `{feature}_{rule}_{scenario}`. `#[scenario(name = ..)]` finds them too.

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
- `krabka_bdd::scenarios!` rejects every tag that is not in this table (`unknown-tag`). Its allowed set is the `ALLOWED_TAGS` constant in `crates/bdd-macros/src/lint.rs`; change both together.
- Do not add tags that only the automation understands, such as `@use_mock_store`. Do not add ticket or story IDs.
- Add a new tag to this table, with its meaning, in the same change that first uses it.

## Journeys

A journey scenario shows how an actor reaches a goal across many rules, for example "a consumer group rebalances after a member leaves and resumes from committed offsets". Journeys are not BRIEF or focused, and their assertions are loose.

- Tag them `@journey`, keep them in their own `journeys/` capability directory, and write their steps with `*`.
- Never use `*` in an illustrative scenario.
- Keep the set small. Automate a journey only when the illustrative scenarios for every feature it touches are automated.

## Step definitions

Krabka runs scenarios with [rstest-bdd](https://docs.rs/rstest-bdd), through [krabka-bdd](#krabka-bdd) for new and migrated features. The house shape is:

- Step definitions live in a `#[cfg(test)]` module of the crate that owns the behavior, or in a `tests/` support module for public behavior. The [code style guide](code_style_guide.md#tests) decides which.
- Scenario state is a scenario-scoped `#[rstest::fixture]` struct. Do not share mutable state through statics. Hold each piece that a step fills in a `krabka_bdd::Given<T>`, not an `Option<T>` and an `expect`.
- A step definition makes testing decisions only. Put "how to drive the system" in a small driver or helper type named in domain terms, and put setup shortcuts there too. Seed state directly instead of replaying a long flow to reach it.
- Use the domain's types for parameters. Parse a value into its enum or newtype once, in one place, not by matching strings inside each step. For an enum, derive `krabka_bdd::StepParam`.
- A negative-path step says "attempts to" or "tries to". It captures the result in the scenario state instead of panicking, and a `Then` step checks it.
- Assertions are atomic. Compare the whole expected value with `assert2::assert!`, so one failure reports every actual value. See [`CLAUDE.md`](../../CLAUDE.md).
- Group step definitions by domain area, not by feature file, and reuse a step only when its wording fits the new scenario.

### rstest-bdd gaps to know

The fork the root `Cargo.toml` pins fixes most of upstream's silent failures. Its `KRABKA_PATCHES.md`, on branch `deps/rstest-bdd-0.6-fixture-guards`, lists each patch. What it changes for a scenario author:

- Outline rows are named after their first column: `case_1_<case>`, with the `Examples:` name as a prefix when it has one. A value that starts with a digit gets `v_`, and an empty value becomes `row`.
- A row whose cells are all empty is a real case.
- An `Examples` block's tags apply to its rows, `@allow_skipped` included.
- A `scenarios!` call that finds no scenarios is a compile error.
- An unknown `<placeholder>` stays as literal text. `krabka_bdd::scenarios!` does not lint for it yet, so check outlines by eye.
- A `*` step after a `When` or `Then` continues that keyword.
- A failing step's panic names the feature file and line, the rule, the scenario, the outline row and the step as written.
- `ignore_tags = "<tag expression>"` marks matching scenarios `#[ignore]` instead of dropping them, so `--ignored` still runs them. Tags may contain `:` and `.`.
- `expect_failure_tags = "<tag expression>"` runs matching scenarios as expected failures. The test passes while a step fails, and fails with "scenario tagged ... now passes: remove the tag" once every step passes, so a scenario cannot stay `@inprogress` after the work is done. A tag set that matches both `ignore_tags` and `expect_failure_tags` is a compile error.
- Placeholder values bind to a step function's parameters by name, so the parameter order does not matter. Position is the fallback only where names cannot match, such as a `StepArgs` struct.

What is still open:

- Cargo does not see a new `.feature` file in a directory that `scenarios!` reads until the Rust file that calls the macro changes. After you add a feature file, touch that file, or the new scenarios do not exist. Edits to an existing file are tracked. Bazel is not affected, because the `compile_data` glob is a build input.
- When several step definitions match a step, rstest-bdd runs the most specific one and breaks ties arbitrarily. `krabka_bdd::scenarios!` turns that into a failing test.

## krabka-bdd

`crates/bdd` (`krabka-bdd`) is Krabka's layer over rstest-bdd. Use it for every new feature and for a feature you migrate. It is a dev-dependency; a published crate declares it with a `path` and no `version`. Its rustdoc lists every argument.

### `scenarios!`

```rust
krabka_bdd::scenarios!("tests/features/log_policy", fixtures = [log_policy: LogPolicyScenario]);
```

It takes the arguments of `rstest_bdd_macros::scenarios!`. Before it expands, it lints every `.feature` file under the directory against this guide. Each finding is a compile error of the form `path:line:col: rule-id: message. Fix: hint`. A clean directory expands to rstest-bdd's `scenarios!` and to one more test, `<dir>_steps::every_step_matches_one_definition`. That test resolves every step of every scenario the call runs, with outline rows substituted, against the step definitions in the test binary. It fails on a step that matches no definition and on a step that matches more than one, and lists the `file:line` of each match. It leaves out the scenarios that `ignore_tags` names, by default `@formulated or @inprogress`, and rstest-bdd marks the same scenarios `#[ignore]`, so the gating run skips them and `cargo test -- --ignored` runs them.

| Rule id | Flags | Fix |
| :--- | :--- | :--- |
| `parse` | A file that gherkin cannot parse. | Correct the syntax at the reported position. |
| `no-background` | A `Background`. | Move each essential step into the scenarios that need it, and delete the rest. |
| `one-when` | Zero or several `When` steps, counting `And` and `But` after a `When`. | Move the extra actions into `Given` context, or split the scenario. |
| `max-steps` | More than five steps. | Delete the steps that do not illustrate the rule, or split the scenario. |
| `star-step` | A `*` step outside `@journey`. | Use `Given`, `When` or `Then`. |
| `journey-location` | A `@journey` outside a `journeys/` directory. | Move it into a feature under `journeys/`. |
| `first-person` | "I", "me" or "my" in a step or a scenario name. | Name the actor by role. |
| `or-in-step` | The word "or" in a step. | Write one scenario per alternative. |
| `quote-in-step` | A `"` in step text. Doc strings and tables are fine. | Drop the quotation marks. |
| `mechanism-word` | call, calls, invoke, invokes, instantiate, construct, mock or struct in a step. | Describe the request, the record or the state. |
| `unknown-tag` | A tag that is not in the [tag table](#tags), `@done` included. | Use a tag from the table, or add a row to the table in the same change. |
| `user-story-feature` | A `Feature:` name that starts with "As a" or "As an". | Name the capability. |
| `placeholder-name` | A scenario named like "Test 3", "TC_12", "Scenario 2" or "Happy path". | Name it so it completes "The one where ...". |
| `case-column` | An `Examples` table whose first column is not `case`. | Make the first column `case`, saying why each row exists. |
| `duplicate-example-row` | Two identical rows across one outline's `Examples`. | Delete the repeated row. |
| `ragged-table` | Table rows of different widths. gherkin truncates a long row without a word, so the lint counts the raw `\|` characters. | Give every row the same number of cells. |
| `duplicate-scenario-name` | Two scenarios with one name in a file. | Rename one so each says what it illustrates. |
| `scenario-outside-rule` | A scenario outside any `Rule:` in a file that has one, unless it is a `@journey`. | Move it under the rule it illustrates. |

`one-when`, `max-steps`, `star-step` and `scenario-outside-rule` exempt `@journey` scenarios; no other rule exempts anything. A tag on the feature or the rule counts for every scenario in it.

### Multi-thread runtime

`runtime = "tokio-multi-thread"`, with an optional `workers = N`, runs each scenario on `krabka_bdd::TokioMultiThreadHarness`. The harness owns a multi-thread Tokio runtime for the whole scenario, so a task a `Given` spawns keeps running on a worker thread until the `Then`. rstest-bdd's own Tokio harness is current-thread, where a spawned task only moves while a step awaits. A step reaches the runtime as `#[from(rstest_bdd_harness_context)] runtime: &krabka_bdd::TokioMultiThreadContext`, and uses `runtime.spawn` and `runtime.block_on`. Write the steps as synchronous functions: under any harness, rstest-bdd polls an `async fn` step once and fails it if that poll returns `Pending`. The calling crate lists `rstest-bdd-harness` as a dev-dependency. `crates/bdd/tests/multi_thread.rs` is the example.

### `StepParam` and `Given<T>`

`#[derive(krabka_bdd::StepParam)]` on an enum of unit variants generates `FromStr` and `Display` over its step words. A variant's word is its name in kebab-case, or the literals of `#[step_param("word", "alias")]`, the first of which `Display` writes. An unknown word fails with every accepted word listed. On a production type that only scenarios parse, derive it under `#[cfg_attr(test, derive(krabka_bdd::StepParam))]`, as `CleanupPolicy` in `crates/log/src/config.rs` does.

`krabka_bdd::Given<T>` is one slot of scenario state. `Given::new("policy", "Given a log has cleanup policy <policy>")` names the field and the step that fills it, keyword included. `get`, `get_mut`, `take` and `into_inner` on an unset slot panic with both names, so a scenario that lost its `Given` says which one.

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
