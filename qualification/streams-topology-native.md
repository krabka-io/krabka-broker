# Native Streams topology lifecycle qualification

Baseline: released Apache Kafka 4.4.0, not trunk. This artifact does not close
#1972 or the parent qualification effort without the released-broker differential
lane. It does not claim arbitrary JVM plugin compatibility.

## Released contract

- [Solicitation and backend outcomes](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/streams/StreamsGroupTopologyDescriptionManager.java): version-gated solicitation, stored/failed epoch suppression, per-epoch backoff, permanent versus transient failures, get-error versus missing data.
- [Cleanup orchestration](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java): `runOneStreamsTopologyCleanupCycle` requires empty membership and expired offsets, persists UNCERTAIN before deleting, retries failed deletion and clears to NONE only after success.
- [Durable epoch barriers](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java): `markStoredDescriptionTopologyEpochUncertain` rechecks emptiness before plugin deletion. The native actor retains that membership turn throughout the operation.

## Implementation and regression evidence

| Requirement | Production path | Regression |
|---|---|---|
| No configured plugin | Handler/config gating | Existing no-plugin heartbeat, update and describe tests |
| Durable UNCERTAIN before storage | `streams/actor/description.rs` | `plugin_sees_durable_uncertain_before_external_side_effect` |
| Transient/permanent failure and backoff | Same | `native_store_failures_leave_durable_uncertainty_and_permanent_epoch` |
| Missing versus failed get | Same | `native_get_failure_is_error_not_missing_and_missing_is_not_error` |
| Movement and epoch change | Shared backend plus restored Streams state | `shared_native_backend_survives_coordinator_movement_and_epoch_change` |
| Process-backend reopening | Versioned filesystem backend plus new coordinator | `filesystem_description_survives_coordinator_restart` |
| Delete/recreate and failed cleanup | Explicit deletion and retry | `native_delete_failure_preserves_group_and_success_allows_clean_recreation` |
| Natural cleanup | Retention sweep -> Streams actor -> offsets home -> plugin -> Streams tombstones | `natural_cleanup_waits_for_offsets_and_retries_current_epoch_delete` |
| Pending transaction protection and progress after release | Offset-home unresolved transactional keys | `unresolved_transaction_protects_topology_until_its_reservation_is_released` |
| Persisted format | Version-1 envelope; reject unknown versions | `version_one_golden_bytes_remain_readable_and_writers_are_versioned` |

`cargo test -p krabka-broker --lib streams`: **388 passed**, including the
2,801-state Stateright schedule model. The latest retired-coordinator extension
was separately replayed and passed (1.35s). The subsequently added pending-
transaction-reservation protection/release regression also passes (1/1).
The scoped verified-kernel tests pass 2/2. Full
`cargo clippy --workspace --all-targets -- -D warnings` passed (9m47s), and
`aspect check-cpd` passed with zero clones.

## Model boundary

`streams/actor/description/tests/model.rs` exhausts all 2,801 histories through
four operations over seven actions. Every transition replays real actors, real
offset expiry, the backend SPI, failure injection and reconstructed coordinator
state. Properties require live-member/fresh-offset safety, no deletion success
under failed backend deletion, and progress at each healthy eligible sweep.
The bounded search is not an unbounded fairness proof. The offset is seeded as
committed; it does not model replicated transaction-marker recovery, network
partitions, filesystem power loss or the Jepsen replication/transaction checks.

## Formal boundary

`krabka_verified::streams_cleanup` is called by production cleanup admission and
store-result processing. `may_reclaim` requires empty membership, initialized
state and expired offsets. `finish_store` specifies exact stored/failed epochs
for success, transient failure and permanent rejection, without preconditions.
The host must supply coherent observations and durably persist the UNCERTAIN
barrier before external IO. Neither pure kernel proves ownership fencing,
backend availability, write atomicity or scheduling fairness.

Creusot/Why3 discharge is tracked independently from annotations; the local
0.13 executable cannot check repository creusot-std 0.14. The matching 0.14 CI
image is `ghcr.io/krabka-io/creusot:v0.14.0-4b4c83aaef59d968`, digest
`sha256:992bd1a7a51c58e188dc2b31a6eef36be106217dcf24450f9a4a52cfdeee561b`.


Formal discharge completed with the matching image: **2/2 verification
conditions proved** by Alt-Ergo (`vc_finish_store`, `vc_may_reclaim`), with no
preconditions. Generated artifacts and prover receipts are under
`verif/krabka_verified_rlib/streams_cleanup/`.

The exact two-phase invocation uses `cargo creusot --package krabka-verified
--output-dir target/streams-verif --dry-run-why3find -- --ignore-rust-version`
to generate the real production crate, then `why3find prove -j 2 --summary
--no-autodetect-provers` on the two generated `.coma` files. For direct Why3find
inside the pinned image, preserve Cargo Creusot's environment:
`DUNE_DIR_LOCATIONS=why3find:lib:/root/.local/share/creusot/share/why3find` and
`WHY3CONFIG=/root/.local/share/creusot/creusot_why3.conf`. A failed compile, empty
file selection or an unproved condition is not a proof pass.


Negative controls used isolated copies of the actual production kernels with
unchanged contracts: returning NONE (-1) instead of UNCERTAIN (-2) after a
failed store, and admitting a standby when `current == maximum`. Matching
Creusot 0.14 generated both faulty bodies and Why3find rejected **both mutated
verification conditions** (exit failure: 2 unproved files); the three unmodified
production conditions prove. These controls demonstrate kernel-contract
sensitivity, not completeness of the bounded actor model or Jepsen coverage.


The failed-store control changes only the runtime body from
`if succeeded { epoch } else { -2 }` to `if succeeded { epoch } else { -1 }`.
The unchanged contract requires UNCERTAIN on failure. Why3find reports
`Goal Coma.vc_finish_store: ✘ (3/5)`; the companion replica-limit control
reports `Goal Coma.vc_next_standby_count: ✘ (2/4)`, followed by
`Error: 2 unproved files`. The control project is isolated from production
worktrees, and no mutated body is committed.

## Explicit deletion and conversion ownership follow-up

Explicit DeleteGroups and cold Streams-to-classic conversion now execute the
membership check, backend cleanup, durable metadata append and registry retirement
inside a single Streams actor turn. A successful transition stops the old actor;
queued requests holding its mailbox cannot perform subsequent writes. A heartbeat
whose wire lookup raced a completed classic conversion also rechecks ownership
inside its actor before mutating Streams state.

For explicit deletion, the offset-home actor appends committed/unacknowledged and
reserved transactional offset-key tombstones in the same batch as the Streams
metadata tombstones, then stops. Registry and cached replay seeds are removed only
after the durable append succeeds. Conversion preserves offsets; its existing
nonempty-member-id no-record removal semantics remain unchanged.

Implementation regressions cover committed and reserved keys in the deletion
batch, old-handle retirement, a join deliberately queued while the native backend
is blocked (both delete and conversion), stale post-conversion lookup, and a
bootstrap log replay consuming the production deletion append after a committed
fixture. These do not establish replicated transaction-marker recovery or Jepsen
qualification. Broker-wide lifetime of the reference in-memory store and
solicitation backoff remains a separate limitation.

Executed follow-up verification:

- `CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p krabka-broker --lib streams`: **393 passed**, 0 failed, including the new blocked-backend race and bootstrap replay regressions.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings`: **passed**.
- `aspect check-cpd`: **passed**, zero clones.
- `aspect generate-kip-matrix`: **passed**, no status graduation.
- Nightly Rustfmt checks of changed Rust files and `git diff --check`: **passed**.

The debug-profile environment settings are local execution settings only. An
initial all-target build exhausted local disk; the successful checks above ran
after clearing this worktree's generated build cache, without changing product
configuration or repository gates.
