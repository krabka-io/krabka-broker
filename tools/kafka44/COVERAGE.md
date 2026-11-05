# Released Kafka 4.4 qualification ledger

This ledger maps acceptance obligations to retained evidence, not feature inventory.
The [bounded-suite acceptance](../../qualification/kafka44-acceptance-20261105.json)
sets `qualification_pass=true` for issue1969's reproducible released4.4 suite
across its explicitly pinned candidate revisions and scenarios. It does not
relabel historical runs as latest-head execution, prove every schedule or
activate production defaults. Historical `qualification_pass=false` receipts
below remain unchanged; later evidence supersedes their pending labels.
Evidence baseline: `e201a1dfafd26f4ddae193a3cd1f0b340c819aa6`.

The former Jepsen recovery blocker is resolved by the production leaderless
reconciliation fix, its valid five-node regression, and the subsequent full
[Nov4 scheduled nightly](../../qualification/kafka44-nightly-jepsen-20261104.json):
36/36 valid, including assign-mixed and transactional/client-crash workloads.
The historical counterexample is retained. Subsequent stochastic checks are not
an exact replay or arbitrary-schedule proof. Issue1970's changed default policy
still requires its own fresh/upgrade/mixed/recovery/finalization checks.

## Current bounded evidence index (2026-11-05)

The detailed matrix below began at the stated baseline and retains historical
obligations. Later receipts supersede its old “pending”/“negative only” labels;
those labels are not a current claim that executed scenarios never ran.

| Surface | Executed evidence | Still not discharged by these receipts |
| --- | --- | --- |
| DLQ | Eight size/header/payload boundaries plus restart; historical three-node ARCHIVING crash/destination failover and lost-confirmation duplicate witnesses | Coupled share-state-owner/DLQ-follower loss now passes with the measured watermark fix; single-node denied inter-broker destination WRITE measured (zero destination records, source lag0); terminal denial follows released archive-without-DLQ policy; RF3 denial remains unmeasured; minimum measured1→2 |
| Delayed uploads | Age/byte/dynamic/retention-derived delay and failure checks; pending-delay crash, natural eviction and48 exact remote reads | Inside-copy payload-close interruption and recovery now pass; RF3 leader-loss/new-leader remote reads pass; arbitrary cuts/power loss are not claimed |
| Routing | Six identity cases plus auth/preauth;40 explicit two-node before/after crash/restart assertions | Explicit same-endpoint cluster replacement and pipelined mutation behavior now measured below; automatic client-cache invalidation remains outside these fixtures |
| Controller removal | Positive register/remove/retry, voter rejection, metadata32/33 boundary and same-data crash durability; native forwarding regression | Further released/native forwarding/failure/authorization-race permutations |
| Topic-ID transactions | Modern16/legacy15 positive cases, pending single-node crash and RF3 coordinator/offsets-owner movement with commit/abort | Deterministic mid-marker cut and power-loss durability are not claimed |
| Streams | Positive lifecycle9; durable backend13 strict set/get/delete failure/repair cases and exact-source restart, with measured legacy DeleteGroups correction | RF3 shared-backend owner movement passes; additional authorization/concurrent-write permutations unmeasured |

All candidate identities are pinned in their respective receipts, not silently
relabelled final-head runs. This index is not a production/default graduation.
`qualification_pass=false`; #1969 remains open until its acceptance matrix is
complete or remaining failures are explicitly resolved as linked blockers.

## Six-surface matrix

| Surface and invariants | Production-linked model coverage | Implementation regressions to execute | Released 4.4 oracle execution | Fault / recovery / multi-broker obligations | Actual execution receipt |
| --- | --- | --- | --- | --- | --- |
| Share DLQ: successful writes precede source archival; terminal failures including destination authorization denial follow released archive-without-DLQ outcome; oversized records follow released outcome; configuration is validated and discoverable | Share membership/partition models exist, but are not by themselves DLQ fan-out durability models. DLQ bounded recovery/admission model and production proof are retained in `qualification/kafka44-dlq/README.md`; they do not prove arbitrary transport schedules. | `crates/broker/src/share_partition/state_model.rs`; DLQ PRs #1976/#1979 and their behavioral regressions; verify code paths, not only standalone transition abstractions | Paired released/candidate payload, provenance-header and size-boundary assertions plus clean same-data restart pass; see `kafka44-dlq-repeatable-20261104.json`. | Lost destination acknowledgement, source acknowledgement failure, partial append, restart, coordinator move, destination leader move, denied destination, feature level 1 vs 2 | 16 strict assertions per endpoint on immutable candidate7c4e3aa7. Historical three-node ARCHIVING/source-crash plus destination-leader-failover and lost-confirmation duplicate witnesses exist in `qualification/kafka44-dlq/`; coupled state-owner/follower loss passes in the corrected receipt below; denied destination single-node released/native parity is retained in `qualification/kafka44-dlq-denied/receipt.json`, with zero destination records and lag0; RF3 denial remains open; share1→2 differential is retained below |
| Delayed tiered uploads: no upload before either configured age/byte threshold; remote coverage never skips a hole; leadership loss withdraws unfinished copies; local retention never removes uncovered data | Existing remote-retention model covers retention, not delayed-upload scheduling. New production-linked `copy_delay_model.rs` passed: 540 unique states, 3358 generated, depth 12, two invariants and five witnesses; no remote IO/recovery model. | `crates/broker/src/remote_log_manager/copy.rs` (`CopyDelay::allows`, `copy_eligible_delayed`, leadership fencing and hole-fill tests); `crates/broker/tests/tiered_storage_multi_broker.rs` | Released RSM/enabled local-archive delay and dynamic-zero checks pass; pending-delay crash, natural eviction and48 exact remote consumer records now pass on both endpoints (receipt below) | Zero/-1/positive limits, effective local-retention derivation, clock skew, dynamic override, failed upload/retry, restart during STARTED, leader movement with misaligned segments, remote-read after local eviction | 46 actual copy-path regressions passed, including model; actual released/candidate copy-only smoke receipt below. Remote recovery/read receipt: `kafka44-copy-recovery-remote-read-20261104.json`; inside-copy payload-close interruption now passes on released4.4 and retained native8eab7ff8 (see `qualification/kafka44-copy-interruption/receipt.json`); other upload-interruption cuts remain unmeasured; RF3 new-leader remote reads pass (receipt below). |
| Misrouting detection: ApiVersions v5's node identity pair is validated together; correct identity accepts; stale/wrong routing returns REBOOTSTRAP_REQUIRED; clients must honor the response | No dedicated routing model receipt; generic replication/failover models do not establish handshake identity validation | `crates/broker/src/handlers/api_versions.rs` and `api_versions/tests.rs`, controller `crates/raft/src/server/api_versions.rs` | Discovery v5 mismatch recorded; six identity-pair behavioral cases executed | Broker and controller listeners, absent/partial/present pair, stale incarnation after restart, two endpoints with wrong routing | `qualification/kafka44-routing-e201a1d.json`: enabled matches all six error maps; defaults reject v5. Authenticated/preauth cases are retained in the authorization continuation; explicit two-node before/after same-data crash restart now passes40 assertions across both endpoints (receipt below) |
| Controller unregistration: authorization precedes active-controller/voter/feature/id validation; current voter cannot be removed; successful removal durable and correctly forwarded | No dedicated controller-unregistration model receipt; generic KRaft election safety does not prove registration deletion | `crates/broker/src/handlers/unregister_controller/tests.rs`; `crates/broker/tests/unregister_controller_forward.rs` | Five-scenario fixture includes unknown-id on broker and controller. Enabled raw responses match released oracle; default disconnects. | Nonvoter positive removal, ALTER denial, inactive controller, minimum metadata boundary, metadata commit failure, retry, restart, remote forwarding | `qualification/kafka44-protocol-negative-e201a1d.json`; initial and post-restart negative results only |
| Topic-ID transactional offset commits: UUID identifies the correct topic; missing group and stale member errors match; offsets visible only after commit; no cross-topic resurrection | Generic transaction decision/EOS/completion models do not model topic UUID lookup, delete/recreate, or v6 group-generation validation. Topic-incarnation kernel proof and 210-state production-linked deletion model executed; pending-name, real-log recovery and lifecycle races remain separate obligations. | `crates/broker/tests/transactions/txn_offset_commit_topic_ids.rs`; `crates/broker/src/txn/handlers/txn_offset_commit/{existence,verification,ordering_tests}.rs`; materialization tests | Unknown UUID v6 returns UNKNOWN_TOPIC_ID in both oracle and enabled candidate, including after restart; default disconnects. Positive commit/abort: enabled matches all 16 v6 and 15 legacy released-codec cases; see continuation below | Delete/recreate same topic name/new UUID, unknown UUID, missing group, stale epoch, authorization, transaction recovery/coordinator movement, offsets-topic leader failure | Commit/abort offset-state comparison plus oracle clean restart; known transaction-completion counterexamples below still block broad EOS claims |
| Streams topology APIs: selected assignor output is valid/cooperative; plugin updates/deletes obey durable epoch ordering; uncertain failure retries; no metadata loss across coordinator movement | Existing `streams_group_model.rs` covers reconciliation only; assignor and topology lifecycle models pending integration from #1971/#1972 | #1977/#1978 and production paths under `coordinator/unified/streams`; no JVM plugin-loading parity inferred from native SPI | No-plugin topology update and invalid v1 group responses match oracle under enabled candidate initially/after restart; default disconnects | Positive plugin persistence, transient/permanent failures, deletion failure/retry, assignment failure retention, new leader/restart, authorization, shared storage across brokers | Negative fixture only; full plugin/assignor differential coverage pending |

## Creusot proof obligations and boundaries

Every row also requires discharged proof obligations where the production decision can be isolated. A contract annotation, an assumed result, or a helper that production never calls is not proof of the implementation. Reports must state verifier version, commands, exact revision, obligation counts, failures and the remaining IO/scheduling assumptions.

| Surface | Proof target / existing receipt | Current discharge status and boundary |
| --- | --- | --- |
| Share DLQ | Production destination admission and source-ack-after-success decision, coordinated with #1973/#1974 | Dedicated production helper proofs pending; persistence, destination RPC and source acknowledgement IO remain runtime obligations |
| Delayed tiered uploads | `CopyDelay::allows`, ordered eligible-prefix selection and remote-coverage-before-retention | New `remote_copy_allowed` is called by production: three freshly discharged Alt-Ergo obligations, no preconditions; clocks, local log/export snapshots, ownership fencing and remote durability remain host obligations |
| Misrouting | Production `krabka_verified::api_versions::{request_valid,admission_error}` on both listeners | `qualification/kafka44-routing-kernel-checks.json`: six named unconditional obligations across four files, pinned Creusot0.14.0; 41 broker/86 controller/one kernel tests and workspace Clippy pass. Identity parsing/equality, socket routing, captured metadata and IO remain host obligations; not a pipeline fence. |
| Controller unregistration | Ordered admission (authorization, controller, current voter, minimum feature, registered id) and deletion-record effect | No dedicated receipt; authorizer, captured metadata image, raft submission/durability and races after capture remain host obligations |
| Topic-ID transactional offsets | Topic-ID existence/error selection, identity isolation across topic recreation and transaction visibility | Generic transaction proofs cannot establish UUID resolution or runtime replay; no dedicated v6 receipt yet |
| Streams | Native assignor output admission and topology lifecycle/epoch transitions, coordinated with #1971/#1972 | Dedicated proofs pending; custom callback behavior, actual shared storage, retries and metadata-log durability remain host obligations |
| Shared feature/default boundary | `crates/verified/src/features/feature_update_decision.rs`; checked-in `verif/krabka_verified_rlib/features/feature_update_decision/feature_update_decision/proof.json` has **4 named obligations** (three elimination + main decision) | Existing receipt replayed successfully in the fresh 616-file Creusot 0.14.0 package run; no new default ceiling is proved. It proves the pure admission decision, not correctness of local/reported feature ranges, dependency tables, captured image, feature registration, controller commit or restart behavior. No new defaults are justified by this receipt. |

Proof CI is `.github/workflows/ci.yml` job `proofs`, selected by `select.outputs.proofs`. It builds/reuses the pinned **Creusot 0.14.0** image and runs `tools/creusot/prove.sh`, currently `cargo creusot --package krabka-verified --why3find-arg=-j --why3find-arg=2 -- --ignore-rust-version`. `aspect check-creusot-packages` checks package registration; it does not discharge obligations. The host's old 0.13.0 cargo-creusot rejects the current library, so this lane pulled the published pinned 0.14.0 image at digest `sha256:992bd1a7a51c58e188dc2b31a6eef36be106217dcf24450f9a4a52cfdeee561b` and executed the actual verifier there. The full package passed (616 files), including the three new delayed-copy obligations. An isolated always-eligible mutation of the same unconditional contract failed its main proof as expected. Ten runtime kernel boundary tests passed, including timestamp/byte extremes. None of this proves host IO or six-feature runtime parity.

## What CI actually runs

`.github/workflows/ci.yml` builds all targets and tests `//crates/...` with explicit Docker and timing-sensitive filters. Broker library Stateright modules are part of the crate's unit target, whose `cpu:4` reservation is in `crates/broker/BUILD.bazel`. This establishes registration, **not execution of a particular changed model**: Bazel can return cached results. Record the exact target, model runner output (visited states, properties, witnesses and guard status), head commit and CI run URL for every new model. Docker-backed tests require the appropriate separate lane; their existence in the tree is not a receipt.

The path-filtered pull-request/manual `.github/workflows/kafka44-evidence.yml` runs discovery plus eleven released-codec scenarios on the oracle. It does not provision candidates, inject faults, or run the full six-feature matrix. Candidate differential runs must retain their exact revision/configuration and failures separately.

Local harness verification on 2026-11-02: `aspect axl-tests` reports **560 passed across 30 suites**, including four Kafka44 parser/input tests. `aspect check-scripts` passes. These verify task machinery, not broker parity.

## Transaction-completion counterexamples are not passes

Historical baseline `crates/broker/src/txn/completion_model.rs` documented and executed `check_model_with_known_failures`. The test succeeds when the declared safety/liveness counterexamples are found at their pinned depths. It must not count as successful qualification of those properties:

| Counterexample | Production linkage / declared trace | Required resolution |
| --- | --- | --- |
| Uncommitted registration is replayed, acknowledged on retry, then truncated on a second move; produced data remains open | `a_load_of_an_uncommitted_record_loses_the_transaction`; CONVERGED depth 10; coordinator replay reads past high watermark | Implementation fix plus passing invariant exploration, witness showing the crash window was reached, and a runtime failover regression |
| Uncommitted PrepareAbort is replayed; abort marker written on one data partition; second move truncates prepare; surviving Ongoing transaction commits elsewhere | Same model, ONE_OUTCOME depth 16 | No split transaction outcome under state-partition movement; runtime read_committed/offset recovery |
| Unexpected/fenced marker response causes durable PrepareCommit to be abandoned, leaving LSO stuck | `a_refused_marker_strands_a_prepared_commit`; CONVERGED depth 7 with refusals | Resolve legal-reachability and retry/recovery contract against release; retain a negative control rather than removing the fault to obtain green results |

After merging main d6b952c80, the first two historical counterexamples are addressed by waiting for committed replay before publication. The current production-linked `a_load_publishes_only_committed_records` invariant exploration passes locally; the refused-marker test remains an explicit known-failure/illegal-leader-state boundary, not a broad transaction liveness pass. These three historical model counterexamples are distinct from the legacy Jepsen manifest's categories (duplicate idempotent retry, leader epoch stuck after reelection, hanging transaction/LSO). Do not conflate old expected-failure labels with current reproduction.

## Current distributed-test release blocker

Parent audit of actual nightly [41302675028](https://github.com/krabka-io/krabka-broker/actions/runs/41302675028) reports **36 tests: 25 passed, 10 fixed, 1 regression, 0 skipped**. The regression is `assign-mixed` under partition/kill/pause faults, invalid due to workload `unseen`. Private downloaded report: `/home/matt/.openclaw/workspace/coordinator/.private/kafka44-jepsen-audit`. This lane independently parsed `results.json` and confirmed the `assign-mixed` row: status `invalid`, detail `invalid checkers: workload`, bad/error types `[unseen]`, store timestamp `2026-11-01T12:07:17.891410344Z`. The individual operation history has not yet been analyzed here. Investigate the history and establish whether the witness is lost records, checker/workload accounting, or another defect. Expected-failure annotations do not waive it and a clean bounded model does not resolve it.

## Actual released-oracle CI receipt

The new path-filtered oracle workflow actually passed at `a5d402a9c68983fa0bfd105a3cc5165fe5bc2b4a`: [run 41375711882](https://github.com/krabka-io/krabka-broker/actions/runs/41375711882). This lane downloaded and inspected its artifact, independently confirming client `4.4.0`, oracle protocol exit 0 and all eleven named results (including broker/controller routing asymmetry). The retained snapshot is `qualification/kafka44-oracle-ci-a5d402a.json`; its status is `collected-not-qualified`, and `qualification_pass` remains false. No candidate, fault or multi-broker execution is inferred from this successful oracle-only CI run.

The delayed-copy kernel execution receipt is `qualification/kafka44-copy-kernel-checks.json`, tied to source revision `2ee48444c2dee6d7128ebba8a4462b56961a49a2`. Workspace all-targets Clippy, 46 copy-path regressions, ten kernel boundary tests, the 540-state model, three new Creusot obligations and the 616-file package replay passed. The always-eligible negative control failed as intended; this is not an allowed production failure.

## Controller continuation

The controller model drives actual metadata admission and `MetadataImage` record application across registration, removal/retry, voter and minimum-feature changes. It passed **56 unique states, 189 generated, depth10** (guard20), with three invariants and three reachability witnesses; the reachable count was independently enumerated from the finite transition graph. Eleven actual controller handler regressions passed, including the model. A fresh pinned Creusot0.14 package run passed **618 files**, including the new decision contract (one bundled main obligation plus enum clone receipt); no preconditions or trusted helper were added. Workspace/all-targets Clippy passed in3m45s.

`qualification/kafka44-controller-lifecycle-e201a1d.json` retains **17** released-codec cases, extending the prior eleven with actual nonvoter controller registration, successful removal, repeated-removal error, reregistration, broker-listener removal and current-voter rejection. Enabled baseline matches every oracle error map; defaults retain explicit gate failures. `qualification/kafka44-controller-boundary-e201a1d.json` executes both the released oracle and enabled baseline at **4.4-IV1 (32)**: unregistration returns UNSUPPORTED_VERSION, while a current voter returns INVALID_REQUEST first. At **4.4-IV2 (33)** the lifecycle succeeds. This does not qualify authorization, separate controller forwarding, failed metadata commits, or restart of successful deletion.

The actual Jepsen history has now been expanded: all ten unseen messages were successfully acknowledged, and their affected keys have no leader on all five nodes after heal. Every affected partition's three replica logs are byte-identical; released Kafka4.4 DumpLogSegments validates retained missing key5 values. Thus this is concrete availability/re-election failure with persisted acknowledged data, not evidence to dismiss the checker or a proven lost-record case. Private complete diagnosis and source leads: `.private/kafka44-jepsen-audit/DIAGNOSIS.md`. Root cause/reproduction/fix remains open.

## Authorization continuation

Ten additional actual released-codec cases passed against the enabled baseline: preauth routing bypass; authenticated routing rejection; Cluster/Group/TransactionalId denials; explicit Cluster Alter and Group Read allows; Group denial after TransactionalId allow; unknown UUID after both preamble ACLs allow. Four synthetic SASL principals and five explicitly recorded ACL bindings are used. Reference/candidate raw fields remain in `qualification/kafka44-authorization-e201a1d.json`; only error maps are compared automatically. Kafka's Cluster denial message embeds ephemeral JVM session/connection details, so raw response byte equality is not claimed. The reference authorizer variant remains separate from default/no-authorizer discovery. PR CI now runs both oracle variants. Full transactions, further ACL permutations, remote forwarding and fault recovery are not established by these results.

## Positive transaction and restart evidence (2026-11-02)

`TransactionScenarios.java` explicitly encodes TxnOffsetCommit v6 and legacy v5, follows returned group/transaction coordinator endpoints, checks one-row UUID echo, and asserts offset-state changes through released Admin OffsetFetch. It retries only coordinator-loading/unavailable/not-coordinator errors; every response and visibility observation is retained. It executes 16 modern and 15 legacy cases: known-topic missing-group v6/v5 error distinction, initial pending-offset invisibility, commit publication, pending-abort retaining the prior offset, and abort not publishing its replacement. EndTxn success is not treated as synchronous publication: the released oracle initially returned null and then 7, so visibility has a bounded 6-second observation window. Unexpected errors and missing rows fail.

`qualification/kafka44-transactions-e201a1d.json` compares baseline e201a1d with pinned released 4.4.0. Enabled matches all 16 modern and 15 legacy cases. Default matches all 15 legacy cases but rejects the v6 request (EOF); the collector correctly exits 1, not a qualification pass. The comparison retains its original fixture source digest; a fresh Java 17 compilation of the current fixture produced a byte-identical class to that retained comparison run (digest and paths in `fixture_source_review`). The later oracle-restart source digest matches the current fixture. Both candidates use isolated single-node RF/min-ISR=1 effective configurations whose hashes are retained. Those are fixture prerequisites, not production-default graduation.

The oracle-only transaction CI variant also restarts its own isolated container and checks both test groups: unchanged topic UUID, committed offset 7 survives, aborted 42 does not appear. External candidate endpoints never authorize automatic lifecycle operations; candidate recovery is performed only by the owner against exact owned processes/configuration/data. This is a clean restart, not a crash, multi-broker failover or pending-marker recovery qualification.

## Topic-incarnation deletion proof/model boundary

`deleted_topic_offset_matches` is used by the actual coordinator topic-deletion handler. Its unconditional contract proves that unidentified legacy offsets are selected and identified offsets match exactly the deleted UUID. A fresh pinned Creusot 0.14 replay passed **619 files**, including **two discharged obligations** (main contract and Option elimination). It does not assume that all identified offsets belong to the current topic. Name matching, metadata notifications, actor ordering, pending transactional names, append/replay and durability remain host obligations.

The new stateful model drives the real async deletion handler, tombstone encoding and fake offsets-log append across legacy/old/new commits, pending names, delayed incarnation deletion and append failure. Pending reservations are deliberately name-bound: they may select a key even when its committed offset belongs to a different incarnation, so the model does not falsely claim UUID protection on that path. Real storage, high-watermark delay, raft and restart are outside this model.

## Historical Jepsen recovery blocker (superseded by acceptance above)

The actual nightly [41302675028](https://github.com/krabka-io/krabka-broker/actions/runs/41302675028) assign-mixed partition/kill/pause report has ten acknowledged unseen values. Private audit `../kafka44-jepsen-audit/DIAGNOSIS.md` maps their exact history indices and offsets. All three replica logs for each affected partition are byte-identical and retain the acknowledged missing values (released 4.4 DumpLogSegments independently decodes t2-1). After final heal/resume/all-running, all five nodes report those partitions with leader=nil, and final reads remain empty. This establishes a recovery/availability blocker, not demonstrated data loss; log bytes alone do not establish high-watermark recovery. Dead-leader sweep/unfence notification/retry paths are investigation leads, not a proved root cause or fix. Expected-failure model annotations and 10 “fixed” manifest statuses are not substituted for passing this actual history.

## Topic-deletion retry after disk recovery

The production-linked topic-deletion Stateright retry passed on 2026-11-02: **210 unique states, 1681 generated states, maximum depth 6**, with all three invariants and four reachability witnesses satisfied. Command: `cargo test -p krabka-broker --lib topic_deletion_interleaves_incarnations_pending_offsets_and_append_failures -- --nocapture`. Final execution log after removing lint suppressions: `../qualification-topic-model-final.log` (same 210/1681/depth6 result). The earlier independent enumeration counted 1680 edges and depth5; Stateright reports the initial state and its depth convention as well. The earlier ENOSPC run was not a model result. Workspace `cargo clippy --workspace --all-targets -- -D warnings` passed after fixing the new kernel/model lint findings (2m14s final run; private log `../kafka44-1969-clippy-final.log`). Nightly formatting of the kernel/model and `git diff --check` passed. The manifest pins source hashes for the checked kernel/model. A final pinned Creusot replay again proved all 619 files after removing lint suppressions (`../qualification-creusot-final.log`). Final workspace Clippy initially exhausted disk space in integration-test incremental caches, then passed with `CARGO_INCREMENTAL=0` (`../qualification-clippy-retry.log`); only this invocation’s failed working caches and its downloaded proof image were removed. Creusot619-file replay and oracle/candidate transaction/restart results remain separate evidence. Full qualification remains incomplete because the Jepsen recovery blocker above is unresolved.

## Leaderless retry and fresh five-node check

The liveness ticker now reconciles leaderless partitions even when no dead broker remains to drive a sweep. It uses active replicas and a metadata-image guarded commit; fenced brokers and followers cannot initiate these elections, and intervening metadata invalidates the batch. Seven ticker regressions and thirteen leaderless-path tests pass in the retained compiled test binary (built after the changed sources). A fresh Cargo test rebuild was stopped when free disk fell below 3 GB; no successful fresh rebuild is claimed.

A fresh five-node Jepsen 0.3.15 `assign-mixed` run using released Kafka client 4.4.0, 120 seconds, concurrency 10 and partition/kill/pause faults returned **valid=true**: 4648 operations, 4529 ok, 119 info, zero fail. The binary digest, source hashes, exact manifest, raw-history hashes and retained private paths are in `qualification/kafka44-leaderless-recovery.json`. JVM headless mode and installed gnuplot permit actual checker completion; the earlier missing-gnuplot unknown result is not a pass. This single stochastic run does not replay or erase the historical nightly counterexample, establish transactional recovery, or qualify the entire feature set. Full qualification remains incomplete and defaults remain unchanged.

## Enabled candidate transaction clean restart (2026-11-02 21:38 UTC)

`qualification/kafka44-transactions-candidate-restart-7bb7dd1.json` records a fresh execution of all 16 modern v6 and 15 legacy scenarios against the retained runtime binary (SHA `fea75051c3f895c6b954af9bfe4685042077586255fa8291cc37775285059d4e`, identical to the leaderless-recovery manifest). The exact owned candidate exited 0 after SIGTERM and restarted with the same data/configuration. Released 4.4 Java codecs then verified both topic UUIDs unchanged and both groups still at committed offset 7, not aborted 42: four recovery assertions passed. Raw fixture outputs and SHA-256 hashes are retained in the receipt. This closes clean-restart evidence for this isolated enabled RF1/minISR1 fixture only; crash/pending-marker recovery, multi-broker failover, default graduation and full six-surface qualification remain unproved.

## Released delayed-copy configuration boundaries

`qualification/kafka44-delayed-copy-config.json` retains thirteen matching released 4.4/enabled-candidate results: topic defaults `remote.copy.lag.ms=0` / `remote.copy.lag.bytes=-1`; both keys reject -2 and nonnumeric strings with INVALID_CONFIG and accept -1, 0 and 1000. Validate-only requests leave configuration unchanged. The actual `aspect kafka44 --protocol-scenarios` oracle collector now compiles and executes this fixture, rejects missing rows/errors, and compares paired candidates when supplied. A fresh oracle-only task passed locally with all thirteen rows. The receipt preserves hashes and raw responses, including diagnostic messages. This establishes configuration boundaries, not delayed copying, remote durability, retention, failure/retry or multi-broker parity.

## Actual released/candidate delayed-copy smoke

`qualification/kafka44-delayed-copy-runtime.json` retains a passing `aspect kafka44-remote` execution against prepared isolated released4.4/enabled candidates. Both use 500ms copier ticks and fresh topics with24 acknowledged64KiB records,1MiB segments,60s age/100MB byte thresholds: no new archive files appear during the3-second observation; changing topic lag.ms to0 produces archive files. Released Kafka invokes the recording RSM SPI and candidate invokes its actual local backend plus durable topic-backed RLMM. The receipt records setup limitations, retained binary/source identity, raw outputs, configuration and archive hashes. The initial failed native self-bootstrap setup and parser-only failed task are explicitly retained, not called passes.

This is a copy-only smoke: a short no-early-copy observation and a dynamic-zero positive control. It does not establish full threshold timing, byte-trigger crossing, remote reads, failed uploads, retention safety, leadership fencing, crash/pending-copy recovery, multibroker parity or current combined Streams/DLQ heads. The recording backend is never a production storage implementation. Full qualification remains false.

After merging current main, an actual fresh Cargo rebuild/test of `leader_election` passed113 tests (8842filtered), including ticker and leaderless regressions; private log `../qualification-leader-election-main-fresh.log`. This supersedes the old stopped-rebuild caveat for source parent `ff09ae16e`, not the retained runtime binary used by external fixtures.

## Successful controller removal survives clean restart

`qualification/kafka44-controller-removal-restart.json` records matching released/enabled register/remove/repeat cases and CONTROLLER_ID_NOT_REGISTERED after clean restart of the identical data. The existing `aspect kafka44 --protocol-scenarios --transaction-scenarios` oracle variant now compiles this strict-error-code fixture, prepares successful removal before its owned restart, and verifies the removal after readiness; a full local collector run passes (16.9s). This closes successful-deletion clean-restart evidence only. Separate-controller forwarding, failed metadata commits, further authorization races and final integrated binary qualification remain open.

## Further copy thresholds and native multibroker IO

`qualification/kafka44-delayed-copy-threshold-smokes.json` retains a successful45.6s `aspect kafka44-remote --mode all` invocation: all8 released/enabled executions pass. In addition to dynamic0 lag, the released client produces a natural6000ms age case, a1048576-newer-byte crossing case (another24acknowledged64KiB records), and lag.ms=-1 deriving6000ms local retention. Each first observes no new archive files for3s then requires files after its trigger. These bounded smokes strengthen real IO coverage without claiming exact full threshold timing, byte-for-byte data integrity or fault recovery.

Fresh current-source native multibroker integration also passes:7tiered-storage cases in45.79s (compressed/earliest reads, leader restart, raised follower floor, killed leader with survivor remote reads, misaligned segment coverage, reassignment and RLMMcatch-up); separate controller forwarding1case in4.64s. The updated current-head receipt records command/log hash. These exercise actual native brokers/logs/backends, not released-oracle replication parity. Full qualification and final combined binary remain separate.

## Final scoped copy/failure/data receipt

Internal QA found the first smoke counted global archive deltas: another old topic's background copy could satisfy a positive stage. Those historical receipts are explicitly marked **not accepted as final** and retained as provenance. The replacement `qualification/kafka44-delayed-copy-scoped-fault-data.json` passes all10released/enabled executions in56.0s with exact current-topic/partition scoping. A physical filesystem regression includes unrelated/global/prefix-decoy files and rejects them. The recording RSM uses per-topic paths.

Five scenarios run on each endpoint: dynamic0, natural6s age,1MiB newer-byte crossing, -1 derived6s local retention, and actual upload access failure/retry. The failure stage denies writes to the owned archive for3s while local producer acknowledgements succeed, restores original permissions through Java finally and task fallback, then requires that topic's upload. Fresh-topic broker log witnesses include released AccessDeniedException and native permission-denied segment-copy warnings, so the negative stage is nonvacuous.

For **every** case, released4.4 FileRecords decodes the actual uploaded prefix:15records, valid batch CRCs, contiguous offsets0..14 and exact acknowledged one-byte keys/64KiB zero values. A private copied-segment mutation rejects a byte100flip with CorruptRecordException while its unmodified control passes; the original archive is untouched. These establish prefix contents and the tested IO retry, not fsync durability, full-produced-record coverage, remote consumer reads, metadata-write failure, mid-copy crash, leader movement or final integrated binary qualification.

## Exact integrated implementation candidate (2026-11-03)

[`qualification/kafka44-integrated-20261103.json`](../../qualification/kafka44-integrated-20261103.json) pins actual source `679706fcaf1b51783a302cdd562c087561db4381`, binary SHA-256 `a0045c991818fdd6dcd06bdc25099813e91fd281eafc520522f23439b16c1a43`, all four implementation heads and full broker/verified/raft Git tree identities. This is the first execution here of the combined DLQ configuration/payload and Streams assignor/lifecycle implementations; earlier receipts remain historical and are not relabeled.

The fresh build passed in 1m09s. Released-image JVM/codecs match enabled candidate error maps for all 17 protocol, 13 delayed-copy configuration, 10 authorization, 16 modern transaction and 15 legacy transaction scenarios. Independent direct executions also all exit zero. After owned SIGTERM and restart of the identical binary/configuration/data, both enabled transaction groups retain their topic UUID and committed offset7, excluding aborted42; default legacy recovery passes too. Successful enabled nonvoter controller removal remains removed after restart. This is clean single-node RF1/minISR1 recovery, not crash or coordinator movement.

All ten scoped released/enabled copy executions pass in56.9s: dynamic-zero, age, newer bytes, derived retention age and actual permission-failure retry, each with released-codec uploaded-prefix CRC/record checks. This does not prove full durability or every acknowledged record's eventual archive coverage.

The first paired collection retains an enabled startup/readiness failure (health listener collision); corrected ready discovery is separately retained and supersedes only that failed discovery. The enabled behavioral rows were collected after startup and pass; no failed response is erased. Ready discovery confirms explicit API differences, including InitProducerIdv6 and additional controller/internal API rows. Defaults retain16 API-range differences and gated behavior/configuration differences.

Therefore `qualification_pass=false`, defaults remain unchanged, and #1969 stays open. The evidence-machinery PR is bounded progress, not full issue1969 completion. Full integrated positive Streams/native storage and DLQ fault/movement behavior, further minimum-level/authorization cases, pending-marker/crash recovery, and the broader release qualification remain separate obligations.


## Repeatable positive Streams lifecycle (2026-11-04)

The durable `StreamsPositive.java` fixture and `--streams-scenarios` lane retain nine strict positive lifecycle assertions on the released reference in-memory plugin. Historical paired receipt: [`kafka44-streams-positive-historical-20261104.json`](../../qualification/kafka44-streams-positive-historical-20261104.json), 9/9 per endpoint. This strengthens earlier negative-only protocol checks but pins historical binary679706f, not final source. Full storage/fault/movement and default qualification remain open.

## Abrupt completed-transaction recovery (2026-11-04)

`qualification/kafka44-transactions-crash-20261104.json` pins freshly built source f392992a4 and actual paired released/candidate SIGKILL recovery: both modern and legacy groups retain exact topic UUIDs and committed offset7, excluding aborted42 (four assertions per endpoint). The private oracle launcher initially refused to reformat existing data; that failed attempt is preserved. Recovery succeeds after reopening the exact same oracle data without formatting.

The durable `--oracle-crash-recovery` mode separately passes in26.0s on the task-owned pinned oracle; CI now runs clean and SIGKILL variants independently. It checks kill/start success and both transaction identities plus durable controller removal. It never restarts arbitrary candidates. Fresh native tests also pass23 transaction cases (17.48s), seven tiered multi-broker cases (24.59s), controller forwarding (4.66s), and12 feature-boundary cases. Native transaction cases include pending markers, unreplicated decision crash/retry, coordinator failover and timeout-abort crash variants. These are native regressions, not a released multi-node differential.

This receipt predates final DLQ integration. It is completed-transaction RF1 process-crash evidence, not pending-marker released parity, power-loss/fsync durability, a complete upgrade matrix or final-head qualification. `qualification_pass` remains false.

## Released-data upgrade, mixed builds and rollback (2026-11-04)

`qualification/kafka44-upgrade-mixed-20261104.json` records actual released v1.1.5 image/candidate f392 runtime evidence, pinning accepted executable ba71f5 (also directly observed from the running corrected fixtures). `aspect kafka44-upgrade` passes5.0s: released formatter/broker seed data and group offset1, candidate preserves UUID/features/offset and reads old data then writes a second record, released rollback reads both and writes a third. Exact keys/values/offsets are checked; metadata33 is refused without unstable gates and finalized levels remain unchanged.

A separate retained three-node RF3/minISR2 run starts two released v1.1.5 nodes and one enabled candidate, then replaces each old node with the candidate. Metadata remains30 throughout replacement, and the same UUID/exact RF3 record survives all six observation stages. With a new candidate active controller and old broker2 still registered, metadata33 update rejects specifically because **broker2 only supports7-30**, not merely the local controller cap. Only after all three nodes are candidates does explicit metadata33 finalization succeed. Local node2 API discovery confirms33; after clean restart it still reports33 and the original record remains readable. Attempting the released binary under its default cap30 on that persisted metadata exits1 with the fatal feature-level guard; restoring the enabled candidate succeeds. This is configured support-policy rejection, not proof the previous binary lacks all experimental33 readers.

Published image, exact commands/configs/raw outcomes are pinned. Initial mixed setup omitted explicit three-voter runtime configuration; that failed setup is retained and not counted. Public `MixedFeatureProbe.java` provides strict feature/data assertions; retained private provisioning records document the separate orchestration. These are clean rolling replacement and feature-guard evidence, not power-loss recovery, share2 finalization, remote migration, final DLQ source or full Kafka4.4 default graduation. `qualification_pass` remains false.

Provenance correction: the earlier transaction crash receipt initially reused the first failed-start a004 binary capture. Accepted corrective starts used ba71f5 after the Cargo-test relink; surviving default/enabled `/proc/PID/exe` hashes confirm it. The corrected receipt retains the failed-start capture separately instead of labeling it accepted-runtime identity.

## Final combined implementation after DLQ landing (2026-11-04)

`qualification/kafka44-final-integrated-20261104.json` pins actual main DLQ merge35f29 plus this PR's production-linked fixes at implementation d4890ecd, immutable production binary SHA7c4e3aa7fd46b8c32a0ea076c30cd095c6476e070bd57d37a54292fcbcf78c5c. Copying occurs only after all native-test linking and the final production build; both running process executables are independently hashed. Broker/verified/raft tree identities and exact configs are retained. The native check revision95ea has an empty crates/Cargo diff against d489.

Fresh released/enabled wire fixtures match **80cases per endpoint**:17protocol,13configuration,10authorization,16modern transaction,15legacy transaction and9positive Streams lifecycle assertions. All nine ready discovery commands pass; raw default gating and extra enabled API-range differences remain explicit, so the full collector intentionally exits1. Its first fresh oracle exposed a loading race (NOT_COORDINATOR before offset partition initialization). That failed comparison is retained. The fixture now explicitly resolves ownership and retains bounded coordinator-loading diagnostics before comparing missing-group results; the ready rerun matches all17protocol cases.

Ten scoped released/enabled copy/failure/data executions pass57.6s, including CRC/record checks and restored archive permissions. Default legacy plus enabled modern/legacy groups preserve six UUID/committed7-not-aborted42 assertions after actual owned SIGKILL/restart; enabled controller removal remains durable after crash. Repeatable released-data upgrade/candidate/old rollback passes5.4s on the exact immutable final executable. Fresh native43integration tests and two targeted library regressions pass, including all7381cleanup actor schedules in3.43s; workspace all-targets Clippy passes4m53s, production build49.57s, AXL568/32, CPD0, scripts27, formatter and diff checks pass.

This closes the final combined-source rerun obligation for this bounded evidence PR, **not** full issue1969 qualification. `qualification_pass=false`; defaults and issue1970 remain unchanged, and broader six-surface/default/minimum/fault obligations above remain open.

## Pending transaction process-crash continuation (2026-11-04)

`PendingTransactionRecovery.java` extends the completed-transaction checks with
unfinished v5/v6 offsets. It commits baseline7, stages42, requires invisibility,
then resolves the original producer identity after an owned SIGKILL/restart. Both
post-crash commit and abort are checked, with unchanged topic UUID and exact stable
offset assertions. `aspect kafka44 --protocol-scenarios --transaction-scenarios
--oracle-crash-recovery` runs all four cases against its immutable released oracle
in the existing crash CI variant. The caller-owned candidate fixture never kills
arbitrary supplied endpoints. Execution receipt:
[`qualification/kafka44-pending-crash-20261104.json`](../../qualification/kafka44-pending-crash-20261104.json).

This closes the single-node pending-offset recovery evidence gap only. It does
not close coordinator movement, offsets-topic leader failure, mid-marker crash,
power-loss durability, or the remaining six-surface obligations. Keep #1969 open
and `qualification_pass=false` until those obligations have verifiable evidence.

## Pending transactional offsets with real coordinator movement (2026-11-04)

`qualification/kafka44-pending-movement-20261104.json` records a caller-owned
three-node RF3/minISR2 matrix on the pinned released4.4 oracle and the immutable
final candidate binary7c4e3aa7. Each endpoint executes v6 and legacyv5, commit and
abort, after killing either the transaction coordinator or the group coordinator
(the leader of that group's offsets partition). Every accepted case requires the
selected owner endpoint to change, the original topic UUID to survive, pending42
to remain invisible over committed7 after failure, then stable42 on commit or7
on abort using the original producer identity. Shared ownership is recorded when
one failure moves both roles. Exact configs, source/tree/binary identities,
wire responses and lifecycle provisioning commands are retained.

The first oracle attempt found asynchronous transaction completion returning
`CONCURRENT_TRANSACTIONS` on reinitialization after a visible baseline commit.
The fixture now permits a bounded, wire-recorded retry for InitProducerId only.
An initial candidate attempt resolved after8s, shorter than the default9s broker
failure detector, and reached the dead coordinator. It is retained as a failed
setup, not parity evidence. Accepted candidate runs wait20s for startup, election
and rejoin and still explicitly verify ownership and exact state. A separate
released Admin readiness diagnostic after the first rejoin observed100/100
internal partitions with three replicas and three ISR; it is not mislabeled as
an orchestration precondition. See the caller-owned recipe in `README.md`.

This strengthens the transaction coordinator-movement and offsets-leader-failure
rows for pending offsets, not the other five surfaces' movement obligations. It
does not inject a deterministic mid-marker cut or power failure, nor discharge
the remaining default/minimum-feature/DLQ/Streams/remote-storage obligations.
The matrix is separately provisioned, not automatically run by the single-node
oracle CI lane. `qualification_pass=false`; #1969 must remain open.

## Durable Streams SPI differential and legacy deletion correction (2026-11-04)

[`kafka44-streams-durable-20261104.json`](../../qualification/kafka44-streams-durable-20261104.json)
records actual pinned released Kafka4.4/custom Java filesystem plugin versus a
fresh native real-broker embedding binary built from `3af529aa6`. Each endpoint
passes thirteen strict transient/permanent set/get/delete failure and repair
assertions, followed by exact-source description verification after clean
same-data restart. The new repository task independently repeats paired
`faults`, `prepare` and `recover` modes, retaining raw responses and exact source,
jar, executable and configuration identities. The native side calls the real
production filesystem backend, not an independent storage simulation.

Importantly, the fresh **before-fix** current-main binary actually failed:
DeleteGroupsv2 returned134 for backend deletion failure, where released Kafka
returned−1. That failed raw execution is retained alongside the focused
version-boundary correction; v0–v2 now map to UNKNOWN_SERVER_ERROR and v3 keeps
GROUP_DELETION_FAILED. The receipt never relabels the prior privately corrected
binary as unmodified main. A first durable Java plugin serialization setup failure
is retained separately from successful corrected runs.

This is external-filesystem SPI/RF1 clean restart evidence, not multi-broker
coordinator movement, power-loss/fsync durability, arbitrary JVM plugin loading,
new minimum-level/authorization permutations, or production-default graduation.
`qualification_pass` remains false; other #1969 obligations remain open.
## Delayed pending-copy crash, natural eviction and exact remote reads (2026-11-04)

`qualification/kafka44-copy-recovery-remote-read-20261104.json` records actual
released4.4 / immutable candidate7c4e3aa7 (implementation d4890ecd) RF1 runs.
Both acknowledge offsets0..47, require no current-topic archive before a30s copy
age threshold, receive SIGKILL and reopen the exact same data/configuration.
Natural35s local retention physically removes offset-zero `.log`; the strict
reader then consumes all48 exact offsets/keys and every deterministic64KiB
payload byte. Both corrected read executions pass. Candidate `/proc/PID/exe`
is independently hashed. The released fixture uses the recording RSM SPI and
native uses its real local backend / topic-backed metadata.

Failed provisioning (static voter/standalone conflict and bind collision), the
released invalid30s-copy/1s-retention request, incomplete reader arguments, and
released fetch failure from an absent optional TRANSACTION index are retained.
The recording fixture now returns the SPI's RemoteResourceNotFoundException for
missing indexes; after that correction and restart, actual remote reading passes.
Additional owned reopenings used for corrected reads are retained, not hidden.

This proves pending-delay process-crash recovery and post-eviction consumer data
integrity on the pinned implementation, not a kill inside an upload, leader
movement, power-loss durability, production backend parity or current-main/default
graduation. `qualification_pass` remains false.

## Retained DLQ multi-node evidence reconciliation (2026-11-04)

The earlier matrix understated the already committed #1979 evidence.
[`qualification/kafka44-dlq/README.md`](../../qualification/kafka44-dlq/README.md)
and its `fault-{release,candidate}-*` raw state/timeline/verify receipts record
three-node durable ARCHIVING before source crash, with destination leader failure
and failover: both recover exactly one intact copy and lag0.
`append-{release,candidate}-*` additionally witness a readable destination append
and withheld Produce confirmation before source crash; both recover two intact
copies and lag0. These are measured duplicate/recovery schedules, not an
exactly-once or universal-fault claim. Historical candidate/image identities and
the released/native terminal-prefix state difference remain explicit. Source
crash plus destination failover does not independently establish share
coordinator movement. Authorization and minimum-feature permutations remain open.

## Explicit two-node routing across restart (2026-11-04)

[`kafka44-routing-endpoints-20261104.json`](../../qualification/kafka44-routing-endpoints-20261104.json)
retains40 strict assertions across released4.4 and candidate7c4e3aa7: correct,
swapped-node, wrong-cluster, partial and absent identities on two explicit
endpoints, before and after owned node1 SIGKILL/same-data restart. Both clusters
preserve identity. ApiVersions5 carries no incarnation field: correct persisted
node identity remains valid after restart; swapped endpoints remain rejected.
The candidate predates the final Streams fix. Two-voter handshakes with both
nodes available are not failover-availability, cluster-replacement, pipelined
request or minimum-feature qualification. See
[`ROUTING-ENDPOINTS.md`](ROUTING-ENDPOINTS.md) for exact commands and boundaries.

## DLQ immediate-below/at-minimum feature levels (2026-11-04)

[`kafka44-dlq-minimum-20261104.json`](../../qualification/kafka44-dlq-minimum-20261104.json)
retains fresh isolated released/enabled share.version1 rejection with zero DLQ
records/lag0, followed by explicit upgrade2 and one exact DLQ payload/lag0.
A separate default candidate refuses feature2 and retains1. Initial provisioning
failures and corrected fresh formats are retained, not counted as passes.
Historical candidate7c4e3aa7 remains pinned; source-start-offset0 versus1 remains
explicit. The repeatable caller-owned `aspect kafka44-share-minimum` task has
strict parser negative controls; the retained receipt executes its identical
Java fixture directly, not the full AXL orchestration. No destination ACL or
coordinator-movement coverage is inferred. See [SHARE_MINIMUM.md](SHARE_MINIMUM.md).

## Streams immediate-below/at-minimum feature levels (2026-11-04)

[`kafka44-streams-minimum-20261104.json`](../../qualification/kafka44-streams-minimum-20261104.json)
records paired streams.version0 rejection of Heartbeatv1 and
TopologyDescriptionUpdatev0 with UNSUPPORTED_VERSION. Explicit upgrade1 then
passes the nine strict positive lifecycle assertions on both endpoints. Separate
fresh owned fixtures execute direct Java and the complete
`aspect kafka44-streams-minimum` task (3.2s); all owned runtimes are stopped.
The native filesystem SPI binary is pinned to source3af529aa6, not relabelled
a current-integration build or arbitrary JVM class loading. No new default, ACL,
coordinator-movement or crash claim follows. See [STREAMS_MINIMUM.md](STREAMS_MINIMUM.md).

## RF3 delayed-copy leader loss and remote reads (2026-11-04)

[`kafka44-copy-leader-movement-20261104.json`](../../qualification/kafka44-copy-leader-movement-20261104.json)
records real released/candidate RF3/ISR3 remote metadata readiness,48 acknowledged
records with no early archive, owned data-leader SIGKILL before30s eligibility,
and a distinct replacement leader. That new leader retains offset0 locally
before natural eviction, then serves all48 exact keys/offsets/64KiB payloads
through remote reads. Released leader2→3 and candidate1→2 transitions are
explicit; configuration/initial setup failures/commands/binary7c4e3aa7 remain
pinned. This is delayed pending-copy leader loss, not a cut during upload I/O.
See [COPY-MOVEMENT.md](COPY-MOVEMENT.md) for the deterministic-cut limitation.

## Streams shared-backend coordinator movement (2026-11-04)

[`kafka44-streams-movement-20261104.json`](../../qualification/kafka44-streams-movement-20261104.json)
records real RF3 original-owner SIGKILL and changed group owners: released1→2,
native2→3. Both preserve STORED topology/exact source, with independent raw
before/after topology object equality, then delete successfully on the new owner
and recreate with no stored description. Released custom plugin nodes share the
same actual filesystem mount; native nodes share the production filesystem SPI.
This bounded empty-member topology case does not establish concurrent plugin
write interruption, active-member reconciliation, fsync/power-loss or arbitrary
JVM loading. Direct phased Java executions passed; full phased AXL orchestration
is not claimed. See [STREAMS_MOVEMENT.md](STREAMS_MOVEMENT.md).

## Actual DLQ share-state owner movement discrepancy (2026-11-04)

[`kafka44-share-state-movement-20261104.json`](../../qualification/kafka44-share-state-movement-20261104.json)
records actual state owner3→1 after source acquisition but before rejection,
without killing source1/destination2. The killed broker3 is also destination
follower3 (replicas[2,3], minISR1), so this is a coupled state-owner/follower-loss
fault, not isolated state ownership. Released finishes with one readable DLQ
record/lag0/no source redelivery. Candidate7c4e3aa7 yields no source redelivery
but no consumer-readable DLQ record within15s. Offline logs contain two valid
DLQ records and terminal source state: physical data loss is not established.
Independent QA excludes committed-offset skipping and transactional isolation
as explanations; destination HW/LSO/ISR and Produce outcomes still require
capture before a causal claim. Visibility/retry/replication remains an active blocker under
[#1969](https://github.com/krabka-io/krabka-broker/issues/1969), not a pass.
See [SHARE-STATE-MOVEMENT.md](SHARE-STATE-MOVEMENT.md).

## Coupled DLQ fault: corrected runtime (2026-11-04)

The failure above is retained as a historical counterexample, not a current
unresolved result for that schedule. The corrected run is
[`kafka44-share-state-movement-fixed-20261104.json`](../../qualification/kafka44-share-state-movement-fixed-20261104.json).
Fresh released and native clusters complete the same coupled state-owner and
DLQ-follower loss with exactly one consumer-readable destination record, source
lag0 and no source redelivery. Native Fetch reaches HW1/LSO1 with389 bytes.
See [the fault procedure and limits](SHARE-STATE-MOVEMENT.md).

The final frozen source passes34 ISR-maintenance tests (including9 behavioral
controls), workspace/all-targets Clippy, formatting and zero-repeat CPD. Receipt
log hashes are independently verified. Installed ISR/leader/epoch matching and
pending-expansion controls prevent policy refresh from advancing durability
against stale authority; LEO is sampled under replica-state lock.

This repairs the measured availability failure, not arbitrary exactly-once
archival, every ACL permutation, power-loss safety or production-default
qualification. Historical failed runs remain retained; `qualification_pass=false`.

### Deterministic inside-copy payload-close interruption

[`kafka44-copy-interruption/receipt.json`](../../qualification/kafka44-copy-interruption/receipt.json)
retains actual payload-close gates on released4.4 and native LocalTieredStorage,
real filesystem copied15-record prefixes validated with released CRC/record
codecs, and actual STARTED-only remote metadata decoded before owned SIGKILL.
The released broker reopens identical image/config/data, naturally evicts the
local offset-zero segment, and returns all48 exact records from remote storage.
This covers interruption inside the copy callback after payload close, before
indexes/callback completion—not partial-byte writes or power-loss/fsync.

Both historical7c4e3aa7 and newer retained8eab7ff8 native binaries reach the same
cut. The older baseline was stopped after an initial replay-readiness warning;
no persistent failure is inferred. The newer candidate reopens the identical
executable/config/data, clears temporary first-tick readiness warnings, retries
the copy, naturally evicts local offset-zero, and returns all48 exact records.
Actual final remote metadata contains COPY_SEGMENT_FINISHED on both endpoints.
The measured executable is retained8eab7ff8, not claimed as latest documentation
head; its exact /proc/exe hashes before kill and after reopen are retained.
No default feature levels change. Other cutpoints and RF3 remain unqualified.

### Measured terminal DLQ authorization denial

[`kafka44-dlq-denied/receipt.json`](../../qualification/kafka44-dlq-denied/receipt.json)
retains a real exact-topic WRITE DENY for the inter-broker principal, a denied
produce control, successful source rejection, zero destination records and zero
source lag on released4.4 and immutable candidate7c4e3aa7. Both log terminal topic
authorization failure; native explicitly archives regardless. This released
terminal-failure policy is an exception to destination-durability-before-source-
archival wording, not a successful durable handoff. No RF3 authorization repair
or automatic destination-creation denial is established. Source startOffset is
0 released versus1 native, so only lag/outcome parity is claimed.

## Pipelined routing error is not a mutation fence (2026-11-05)

[`kafka44-routing-pipeline-20261105.json`](../../qualification/kafka44-routing-pipeline-20261105.json)
retains four strict released4.4/enabled-native wrong-node/wrong-cluster executions.
The client writes ApiVersions5 and CreateTopics7 before reading either response.
Both peers return REBOOTSTRAP_REQUIRED129 and then create the requested topic; a
separate Admin lookup confirms mutation. The former matrix wording requiring
wrong routing to reject without applying another request was stronger than the
release and is corrected: this handshake is client-driven rebootstrap, not a
connection-wide mutation fence. No incompatible native fence is added.

The candidate is the explicitly hashed retained intermediate watermark-fix
binary, not the final frozen1991 build. Cluster replacement, default graduation
and SASL/controller permutations are not inferred; qualification remains false.

## Same-endpoint replacement with a different cluster (2026-11-05)

[`kafka44-routing-replacement-20261105.json`](../../qualification/kafka44-routing-replacement-20261105.json)
retains actual sequential replacement, not a wrong-string simulation. Each
endpoint first serves one cluster; after that process stops, the same host/port
serves different persistent cluster data. Metadata12 confirms the changed
cluster ID. Released and enabled native both accept the current identity and
reject the previous cluster identity with REBOOTSTRAP_REQUIRED129: six strict
assertions across the two before/after sequences. Both generations retain node1.
The client explicitly sends its stale identity; automatic client cache
invalidation/rebootstrap behavior is not tested. Native source/binary identity
is the intermediate pinned watermark-fix build, not final1991; no default or
full qualification claim follows. Owned replacement processes are stopped.

## DLQ configuration boundaries (2026-11-05)

[`kafka44-dlq-config-20261105.json`](../../qualification/kafka44-dlq-config-20261105.json)
retains twenty strict released/enabled-native rows per peer (forty total), with
identical discovery defaults and validation error maps for the topic enable and
group copy/name settings. Validate-only requests leave all tested settings
unchanged. Reserved `__` names and nonboolean strings reject; `bad name` and
`..` remain valid group config values in the release at this stage. The
`--dlq-scenarios` task and its existing oracle CI variant now run this fixture.
This is config admission, not runtime acceptance of an invalid destination,
ACL/recovery or default graduation. Native binary identity is explicitly pinned
to the retained intermediate watermark-fix build, not final1991.

## Acceptance review and graduation handoff (2026-11-05)

The recovered continuation receipts above are additive; their historical source
and binary identities are not relabelled as fresh execution of this integration.
The [released ceilings](GRADUATION.md) are derived from pinned released bytecode,
not inferred from implemented request structs. The qualification suite has the
following concrete coverage of issue1969's six requested acceptance criteria:

| Criterion | Evidence / result |
| --- | --- |
| Immutable release artifacts and exact candidates/configuration | Oracle image/jar hashes and native revisions/executable hashes are retained in every respective JSON receipt; no moving trunk oracle. |
| API, metadata/feature, config/error/auth comparisons | Discovery/default-enabled matrices, authorization receipt, controller32→33, DLQ1→2, Streams0→1, copy and DLQ config boundaries; new released-ceilings extraction is library-only and explicitly separated. |
| Six runtime feature surfaces | DLQ payload/headers/denial, real local remote-storage uploads/reads, node+cluster routing, nonvoter controller removal, v6+legacy pending transaction commit/abort, durable topology set/get/delete all execute released codecs against actual endpoints. |
| Default/enabled/minimum, negative, recovery, relevant multibroker | Default gate failures retained; enabled/minimum cases above. DLQ coupled owner/follower, upload leader loss, routing endpoints+replacement, controller forwarding, pending transaction coordinator/offsets-owner and Streams shared-backend owner movement have bounded receipts. Inside-copy STARTED crash now has actual interrupted I/O evidence. |
| Repeatable coverage and evidence | Existing path-filtered oracle CI; native behavioral/model/proof lanes; AXL collectors and caller-owned Java/C protocols with exact reproduction commands. Not every privately provisioned RF3 scenario is claimed automated in CI. |
| Tool-only vs broker obligations | Released schema extraction, fixture compilation and parser checks never count as runtime parity; standalone models never substitute for actual broker/client execution. |

This is not yet an unconditional full released-default compatibility claim.
Enabled historical discovery includes InitProducerId6 while released4.4's
production maximum is5; [graduation](GRADUATION.md) must preserve this distinction
and reconcile advertisement, acceptance and feature policy together. Existing
production defaults deliberately remain4.3.1 until issue1970 is implemented and
verified. The historical Jepsen availability failure remains separately described
above; none of these bounded receipts disproves it. Arbitrary network schedules,
power loss and every Cartesian-product ACL/fault permutation are outside the
claimed executions, not silently reported passes. `qualification_pass=false`
remains unchanged by this evidence integration.
