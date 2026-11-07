# Prose Style Guide

This guide defines how to write prose in Krabka. It applies to every guide in this directory, and it covers the wording questions that the others leave open.

Krabka prose is plain, direct technical English. It borrows a few ideas from ASD-STE100 Simplified Technical English: one word for one meaning, the actor in front of the verb, and short sentences where a long one would be hard to follow. These are aims for clarity, not a controlled vocabulary. A reviewer should ask "is this clear and accurate?", not "does this pass a word list?".

## Scope

These guidelines apply to all prose that Krabka writes:

- Rustdoc comments (`///` and `//!`) and ordinary `//` comments
- Crate `README.md` files, design docs, and coverage reports
- Commit messages, pull request titles, and pull request bodies
- Every document under `docs/`
- Gherkin `.feature` files, which follow the [Gherkin style guide](gherkin_style_guide.md) for their structure

They do **not** apply to:

- Rust code, attributes, and the contents of doctest fences
- Identifiers of any kind: type names, function names, crate names, feature flags, CLI flags, environment variables, config keys, and file paths
- Protocol vocabulary: Kafka API and record-field names, `KIP-###` references, error codes, PostgreSQL keywords, `pg_*` names, and `SQLSTATE` codes
- Quoted output, captured logs, and table data
- Text copied from an external specification

Write technical names as they are: `ApiVersions`, `RowDescription`, `producer_byte_rate`.

## Words

- **Use one term for one concept, everywhere.** If the code says `log start offset`, the docs, the comments and the scenarios say `log start offset` too, not "earliest offset" in one place and "LSO" in another. This is the one word rule that matters, because a second name for the same thing makes a reader wonder whether it is a second thing.
- Prefer the plain word when it says the same thing: "use" over "leverage", "start" over "commence", "before" over "prior to". This is a preference, not a ban. Use the precise word when the plain one loses meaning.
- Avoid slang and metaphors that a reader whose first language is not English could misread, such as "blast radius", "front door", and "footgun". Domain terms that the Kafka community uses are fine.

## `must` and `should`

This rule matters more than any other in this guide, because it changes what an API promises. It is not relaxed.

**Write `must` only when the code enforces the rule.** A rule is enforced when breaking it produces an observable failure from the code you are documenting: an `Err` return, a `panic!`, an `assert!`, an `expect`, or a protocol-level rejection.

**Write `should` for a recommendation.** Advice to a caller, to an operator, to an external client, or to a person reading a debugging note is a recommendation. Krabka cannot make a foreign Kafka client retry, and it cannot make a caller's runtime back off.

Do not write `shall`. Keep `must not` and `do not` for prohibitions.

A wording edit must never convert a `should` into a `must`. That is a change of contract, not a change of wording.

```rust
/// Panics if `size` is under one millisecond.        // must-class: an assert enforces it
pub fn of_size(size: Time) -> Self {
    assert!(size >= MIN_RESOLUTION, "window size must be >= 1ms");
```

```rust
/// Returns `None` when the source is momentarily caught up. The runtime should
/// back off and poll again.                          // should-class: nothing enforces it
```

## Sentences and paragraphs

- Lead with the main point. Put qualifiers and history after it.
- Keep sentences short enough to read in one pass. As a rough guide, a sentence over about 30 words usually reads better as two. There is no hard limit.
- Keep one topic per paragraph, with the topic sentence first.
- Do not drop articles (`a`, `an`, `the`) to save space. Telegraphic prose is harder to read, not easier.

## Voice

- Prefer the active voice and name the actor: "the broker rejects the request", not "the request is rejected". Use the passive when the actor is unknown or does not matter.
- Use the imperative for instructions.

## Punctuation

- Do not put essential information in parentheses. A reader skims past them.
- Do not use `->`, `=>`, `+`, `/`, or `iff` as words in prose. Write them out.

## Safety and rules

Keep every warning, prohibition, and absolute exactly as forceful as it is. Words such as `never`, `must`, `all`, `only`, and `do not` define rules. Do not soften one, do not narrow its scope, and do not drop a negation when you rewrite a sentence.

## Rustdoc specifics

The [rustdoc guide](rustdoc_style_guide.md) defines structure. These rules govern its prose:

- The first line is the rustdoc summary. Keep it to one short sentence.
- Never rename a heading in a Markdown file. Other files link to it by anchor.
- **`# Errors` and `# Panics` must state the real conditions for the item they sit on.** Do not paste a generic sentence across a crate. A prose audit of this workspace found eight such boilerplate strings, and many were false: a `# Panics` body that named a mutex on a file with no mutex, a `# Errors` body that named Kubernetes in a crate with no Kubernetes dependency, and encode functions documented as failing on truncated input. A section that does not describe the item is worse than no section, because a reader trusts it.
- If you cannot state the real condition, delete the section. Do not leave a placeholder.

## Applying these conventions

The same rule applies here as in the [code style guide](code_style_guide.md#applying-these-conventions). This guide is not a mandate to rewrite prose you are not otherwise touching. Bring a file into line when you already edit it, and keep the tidy-up proportionate to the change.
