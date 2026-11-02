# Native Streams assignor qualification

Baseline: released Apache Kafka 4.4.0. The native embedding API is intentionally
not a JVM class loader. Released-JVM custom-plugin interoperability remains a
separate qualification lane; this artifact alone does not close #1971.

## Released contract

- [Registry order and validation](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java): first implementation is the default, names identify registered implementations and ambiguous registration is rejected.
- [Group selection and fallback](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java): `streamsGroupTaskAssignor` chooses a registered override and falls back to the broker default when a persisted override is unavailable.
- KIP-1357 is describe-name reporting, not proof of custom implementation execution.

## Executed checks

- `cargo test -p krabka-broker --lib task_assignor_registry`: **7 passed**, including distinct assignment placement, selection, cooperative reassignment and retaining the previous target on failure.
- `cargo test -p krabka-broker --lib native_registry_stateright`: **passed**, all **36 unique states** (3 implementations including one failure, 4 member sets, 3 task universes). Every transition runs the production registry, override validation and output validator. It requires exact selected placement, or rejection for unknown members/failing implementations.
- `cargo test -p krabka-verified --lib streams_assignment`: **1 passed**, including exact replica-limit and i32-boundary cases.
- `cargo clippy -p krabka-broker --all-targets -- -D warnings`: **passed**.
- `aspect check-cpd`: **passed**, zero clones.

The finite registry model is exhaustive within its configuration domain; it is
not a distributed coordinator or replicated transaction model. Existing actor
regressions supply the cooperative handoff/previous-target retention evidence.

## Formal boundary

The production output validator calls
`krabka_verified::streams_assignment::next_standby_count`. Its precondition-free
contract admits exactly the next count when `0 <= current < maximum`, rejects
negative or full counters, and guarantees exact increment without i32 overflow.
Identity, task-universe and coordinator ownership observations are host inputs,
not conclusions of this arithmetic proof. Annotation presence alone is not
proof discharge: the repository requires matching Creusot 0.14/Why3 tooling.


Negative controls used isolated copies of the actual production kernels with
unchanged contracts: returning NONE (-1) instead of UNCERTAIN (-2) after a
failed store, and admitting a standby when `current == maximum`. Matching
Creusot 0.14 generated both faulty bodies and Why3find rejected **both mutated
verification conditions** (exit failure: 2 unproved files); the three unmodified
production conditions prove. These controls demonstrate kernel-contract
sensitivity, not completeness of the bounded actor model or Jepsen coverage.


Formal discharge: **1/1 VC proved**, `vc_next_standby_count`, Alt-Ergo0.007s,
using `ghcr.io/krabka-io/creusot:v0.14.0-4b4c83aaef59d968` (digest
`sha256:992bd1a7a51c58e188dc2b31a6eef36be106217dcf24450f9a4a52cfdeee561b`).
Artifacts: `verif/krabka_verified_rlib/streams_assignment/`. Generation used
`cargo creusot --package krabka-verified --output-dir target/streams-verif
--dry-run-why3find -- --ignore-rust-version`, then direct Why3find `prove -j 2
--summary --no-autodetect-provers` on the generated `.coma`, with
`DUNE_DIR_LOCATIONS=why3find:lib:/root/.local/share/creusot/share/why3find` and
`WHY3CONFIG=/root/.local/share/creusot/creusot_why3.conf`, exactly as the Cargo
Creusot dry-run specifies. No empty-selection or annotation-only run is counted.


The negative control changes only the production body comparison from
`current >= maximum` to `current > maximum`. With the original contract,
Why3find reports `Goal Coma.vc_next_standby_count: ✘ (2/4)` and exits nonzero;
the original production body proves in one VC. The companion failed-store
control reports `Goal Coma.vc_finish_store: ✘ (3/5)`. These are source-body
mutations, not removed preconditions or deliberately weakened specifications.
