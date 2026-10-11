# API, visibility, comments, and workflow review — resolutions

Reviewed and implemented on 2026-10-11 in `/workspace/krabka-broker`, branch `review/api-visibility-comments-20261011`, based on freshly fetched `origin/main` commit `143e7a2cc70cc423665d3096d9f765918e6cd49e`. The 35 findings below have implementation changes in the working tree. **Centralized targeted validation passed**: all 32 selected Bazel targets, including 4,816 broker unit tests and three ElectLeaders integration runs, plus workspace/all-target Clippy and the workflow checks recorded below. These changes are included in this pull request; validation limits are explicit below.

## Resolution table

| # | Finding | Implementation | Validation |
| --- | --- | --- | --- |
| 1 | CreatePartitions authority | Plans topic growth on the active controller image and guards submission against image/term changes; existing partition assignments are preserved. [Source](crates/broker/src/handlers/create_partitions.rs). | Broker admission/assignment regressions and admin/controller integration tests passed. Final runtime log (local execution log). |
| 2 | ISR and failover fencing | AlterPartition, automatic/offline-directory failover, under-replicated recovery and manual ElectLeaders use guarded image/term submissions; original admin operations are forwarded for controller planning, and role/epoch transitions invalidate controller term identity. [Source](crates/raft/src/kraft/controller/submit.rs). | Broker ISR, failover, under-replicated recovery and delayed-audit fencing regressions passed; Raft term-identity regressions passed; ElectLeaders integration passed all three runs with retries disabled. Final runtime log (local execution log). |
| 3 | Diskless share history | ShareFetch and dead-letter retrieval read retained WAL payloads; share initialization/repair use the logical deletion floor. [Source](crates/broker/src/handlers/share_fetch/records.rs). | Broker WAL-backed share/DLQ and logical-floor regressions passed; share-state integrations passed. Final runtime log (local execution log). |
| 4 | Index-cache publication | Unique temporary files are published atomically; concurrent fills cannot truncate a visible cache entry. [Source](crates/remote-storage/src/index_cache.rs). | Passed: 180 tests, including simultaneous cache fills (local test log) |
| 5 | RLMM partition hashing | Assignments use the discovered metadata-topic partition count, matching publication and readiness. [Source](crates/broker/src/broker/rlmm.rs). | Broker discovered-partition-count/reconciliation regressions and remote-storage-topic suite passed. Final runtime log (local execution log). |
| 6 | RLMM initial connection | Consumer connection attempts and retry delays are cancellable and retry without requiring reassignment. [Source](crates/remote-storage-topic/src/kafka_log/consumer.rs). | Passed: 99 tests, including initial connection retry/cancellation (local test log) |
| 7 | Legacy AlterConfigs authority | The original operation is forwarded to the active controller, planned against its current image, and submitted with image guards. [Source](crates/broker/src/handlers/alter_configs.rs). | Broker replacement tombstone/tiering-conflict regressions and controller admin integrations passed. Final runtime log (local execution log). |
| 8 | Share initialization fencing | Shard operation locks serialize absent-key initialization, fencing, persistence, deletion and recreation under an active leadership guard. Timeout/cancellation fences the shard as LOAD_IN_PROGRESS until a FIFO writer barrier and same-term replay generation are installed; recovery Notify wakes retries even when metadata is unchanged. [Source](crates/broker/src/share_coordinator/coordinator.rs). | Broker concurrency/fencing, abandoned-append barrier/replay and unchanged-metadata supervisor recovery regressions passed; share-state commit integration passed. Final runtime log (local execution log). |
| 9 | Share replay oversized batches | Replay reads a complete first batch beyond the load window instead of treating a bounded empty read as EOF; decoding failures leave replay failed. [Source](crates/broker/src/share_coordinator/coordinator/recovery.rs). | Broker oversized-batch replay regressions passed. Final runtime log (local execution log). |
| 10 | Fetch response byte limits | First-nonempty progress permission is tracked across local, remote, WAL and reread paths; later batches obey the remaining response budget. [Source](crates/broker/src/handlers/fetch/read_loop.rs). | Broker first-nonempty response-budget and long-poll reread regressions passed for the shared read paths. Final runtime log (local execution log). |
| 11 | Share duration cold history | Duration initialization resolves timestamps across local/archive/WAL sources with decompression limits; the committed WAL index deletion floor is authoritative for diskless history, rather than a checkpoint advanced by local flushing. [Source](crates/broker/src/share_partition/manager/cells.rs). | Broker archived duration and diskless local-flush versus WAL deletion-floor regressions passed. Final runtime log (local execution log). |
| 12 | ListOffsets WAL timestamps | Timestamp resolution combines exact WAL and local candidates using the committed WAL index deletion floor for diskless history; local-flush checkpoints do not delete WAL history. An interior floor forces a precise retained-record scan rather than relying on an earlier batch maximum. [Source](crates/broker/src/handlers/list_offsets/timestamp.rs). | Broker exact WAL/local/archive timestamp and deletion-floor regressions passed; log timestamp tests passed. Final runtime log (local execution log). |
| 13 | Legacy scheduled LATEST | ListOffsets v0 shares archive-aware LATEST resolution and timeout handling with newer versions. [Source](crates/broker/src/handlers/list_offsets/v0.rs). | Broker v0 archived scheduled-LATEST partition-resolution regression and existing v0 wire-shape tests passed. Final runtime log (local execution log). |
| 14 | Response write stalls | Pending writes use a no-progress deadline for framed, TLS and sendfile paths; successful progress renews the window and timed-out connections release queue permits. [Source](crates/broker/src/network/dispatch/write_idle.rs). | Broker portable write, TLS ciphertext progress/stall, TCP sendfile and global queue-permit recovery regressions passed. Final runtime log (local execution log). |
| 15 | Multipart backpressure | File upload waits for capacity before reading/queuing the next chunk, limiting outstanding multipart work to eight chunks and retaining abort cleanup. [Source](crates/object-store/src/ops/client.rs). | Passed: 88 tests, including multipart backpressure/cancellation and failed-part abort (local test log) |
| 16 | Restore interior corrupt gaps | An explicitly skipped predecessor permits a forward gap without resetting the restored prefix; surviving original offsets and filtered batch spans are preserved. [Source](crates/restore/src/materialize/segment.rs). | Passed: 146 unit tests (local test log); 6 corruption tests, including an interior gap (local test log) |
| 17 | RLMM epoch-list readiness | Epoch-list metadata queries pass through the same assignment/readiness gate as sibling reads. [Source](crates/remote-storage-topic/src/manager/rlmm.rs). | Passed: epoch-list readiness regression in 99 tests (local test log) |
| 18 | Backup identity collisions | Timestamp-plus-UUID IDs distinguish concurrent/repeated-clock captures; create-only artifact/manifest writes refuse replacement and legacy capture discovery remains supported; latest selection requires a committed manifest. [Source](crates/backup-cli/src/capture.rs). | Passed: 74 tests, including concurrent identical timestamps and orphan-manifest selection (local test log) |
| 19 | Remote/document read caps | Remote indexes use a configurable independent cap; backup manifests, offsets, snapshots and capture documents use independent capped reads, including object-change and streamed-size checks. [Source](crates/backup-cli/src/run.rs). | Passed: index-type caps (180 tests) (local test log); document caps (74 tests) (local test log); capped-read races/streams (88 tests) (local test log) |
| 20 | Authenticated snapshot handoff | Restore retains authenticated metadata bytes and the decoded authenticated RLMM snapshot for formatting, reconciliation and seeding, without reopening authenticated source paths. [Source](crates/restore/src/lib.rs). | Passed: changed-source authenticated-snapshot handoff regression in 146 tests (local test log) |
| 21 | RLMM mutation results | Publish waiters receive the exact local application result for their partition/offset; rejection and interrupted/shutdown waits no longer report success. [Source](crates/remote-storage-topic/src/manager/mutations.rs). | Passed: rejected mutations and interrupted waits in 99 tests (local test log) |
| 22 | Publishing token lifetime | A budget starts before OIDC authentication and bounds the authenticated upload batch to 20 minutes, with a five-second termination grace; exhaustion directs a full rerun that refreshes credentials and the pending plan. [Source](.github/workflows/publish.yml). | Passed: two YAML parses, Bash syntax and four publisher clock/upload probes (task tool output); no live registry exercise. |
| 23 | Missing expected-failure tests | Only observed FAIL is accepted as a known failure; missing, skipped, aborted and other non-PASS/non-FAIL statuses are regressions. [Source](.aspect/kafka_system_tests.axl). | Passed: 10 system-test task tests within 417 AXL tests/21 suites (local execution log). |
| 24 | Exact snippet anchors | Extraction matches complete comment markers and rejects missing, duplicate or out-of-order pairs instead of accepting identifier prefixes or marker-looking string statements. [Source](crates/docgen/src/snippets.rs). | Passed: 20 tests, including exact-anchor/unique-pair extraction (local test log); snippet synchronization changed 0 files (local execution log). |
| 25 | Formatter process exit | Library argv entrypoints use fallible Clap parsing and return help/version or argument-error exit codes instead of exiting their caller. [Source](crates/format/src/lib.rs). | Passed: embedding-process argv regression (1 test) (local test log); format smoke and seeded-record targets passed (local execution log). |
| 26 | OPA wire schema | Requests use the v1.5.1 Kafka-plugin action/resource vocabulary and the available request-context subset; complete supported payloads and endpoint decisions have regressions. [Source](crates/broker/src/authorizer/opa/wire.rs). | Broker complete OPA plugin payload, context-subset and endpoint-decision regressions passed. Final runtime log (local execution log). |
| 27 | Generic exported derives | EnumStr, PrimitiveCmp and RefinedNewtype preserve implementation/type generics and where clauses, including generated parsing functions. [Source](crates/macros/src/refined_newtype.rs). | Passed: EnumStr 6 tests (local test log); PrimitiveCmp 3 (local test log); RefinedNewtype 8 (local test log); generic consumer cases included. |
| 28 | Live clock-bound contract | Stricter uncertainty and backward clock changes apply prospectively above the released watermark; pending-range queries preserve already-visible records. Restart semantics are explicit. [Source](crates/log/src/log/delivery.rs). | Passed: prospective clock/reopen regression in 563 tests (local test log) |
| 29 | Clippy policy | The code guide prohibits scoped Clippy suppressions consistently with CLAUDE and CONTRIBUTING. [Source](docs/style_guides/code_style_guide.md). | Documentation policy cross-checked; final all-target Clippy passed with `-D warnings` (local execution log). |
| 30 | Benchmark contracts | README, CONTRIBUTING and workflow comments describe Cargo measurement, Bazel compilation/lint/runnable targets, and alternating reference/candidate regression comparisons. [Source](.github/workflows/benchmarks.yml). | Documentation traced to crate_bench and current workflow; edited workflow YAML parsed (task tool output). Benchmark execution not claimed. |
| 31 | Actual release/docs automation | Guides name tag-driven publication and the current CI generated-reference/snippet checks instead of nonexistent release-plz/weekly docs-freshness automation. [Source](docs/style_guides/rustdoc_style_guide.md). | Guide triggers cross-checked against CI/publish; docgen generated reference output (local execution log), snippets synchronized with 0 changes (local execution log). |
| 32 | Stamp helper preconditions | Ordinary Rust rustdoc states parallel-array lengths, insertion ordering/disjointness, validation helpers and potential indexing panics; proof annotations are not runtime checks. [Source](crates/verified/src/stamp.rs). | Ordinary Rust rustdoc/preconditions reviewed; 403 tests passed, including malformed stamp-range inputs (local test log). |
| 33 | Profiling wrapper contracts | Rustdoc describes bind/serve errors and invalid environment/fallback-address panics; router-conflict panics are also documented where applicable. [Source](crates/telemetry/src/profiling/server.rs). | Bind/serve, address and router panic contracts reviewed; 37 telemetry tests passed (local test log). Invalid-address panic remains documented behavior. |
| 34 | Envelope catalog | Catalog comments describe implemented client-identity forwarding to the active Krabka controller and retain the JVM-controller/mixed-quorum scope boundary. [Source](crates/broker/src/api_catalog.rs). | Catalog documentation cross-checked against client-identity forwarding; controller/bootstrap integration tests passed. Final runtime log (local execution log). |
| 35 | Replica-selector parse errors | Rustdoc describes the actual leader/rack-aware string parser and unsupported trimmed-value error. [Source](crates/broker/src/replica_selector.rs). | Documentation cross-checked against the pure trimmed-string parser; no runtime change or new test required. |

## Material contract limits

- **OPA:** The supported wire shape is pinned to opa-kafka-plugin v1.5.1. It supplies the complete single-resource action, `requestContext.principal` (`principalType: "User"` and name), and the client address. The authorization interface does not supply Kafka connection headers, listener names or security protocols; those context fields are omitted. Policies that require the omitted fields need adaptation. This is a documented subset, not a promise that every plugin policy works unchanged.
- **Publishing:** OIDC upload/index-wait/retry work has one 20-minute budget starting before authentication, plus at most five seconds to kill a timed-out command. Exhaustion fails with a resumable instruction: use GitHub **Re-run all jobs** so both authentication and the pending-crates plan are refreshed. Re-running only the failed publish job retains the old plan. Bootstrap-token publishing retains seven attempts and ten-minute rate-limit sleeps.
- **Scheduled clocks:** A live instance never retracts its released delivery prefix when uncertainty increases or the wall clock moves backward. New settings govern subsequent watermark advances and pending records. The watermark is not persisted; restart or leader change recomputes it using the current clock/bound and can produce a lower prefix than the prior instance had released. This prospective behavior does not establish monotonic visibility across restarts under changed clock assumptions.
- **Compatibility:** No persisted layout is intentionally changed. New backup names remain discoverable alongside timestamp-only names. Rust API changes follow the repository's unstable-API policy; Kafka wire and persisted 1.x compatibility remain required.

## Validation status

Execution logs cited below were retained in the review workspace (`/tmp/review-fix-*.log`, `/tmp/fix-creusot-proof-bfd.log` and `bazel-testlogs/`); they are not repository artifacts or remotely hosted CI logs. The results and qualification limits are recorded here. Source links are relative to this repository.

The final 32-target Bazel batch (local execution log) passed: 14 targets executed and 18 used valid cached results. The broker suite passed **4,816 tests with zero failures or ignored tests**; ElectLeaders passed **all three independent runs**, with automatic retries disabled. Workspace/all-target Clippy passed with `-D warnings`. Earlier failing/flaky intermediate runs led to the integration follow-ups below and are superseded by this final result.

| Check | Recorded evidence and limits |
| --- | --- |
| Targeted Bazel regressions | Passing test logs are linked per resolution above; cached and newly executed targets are identified in the final batch log (local execution log). This is targeted validation, not `bazel test //...` or a full Cargo workspace run. |
| Aspect tasks | 417 tests across 21 suites passed (local execution log), including 10 Kafka-system-test task tests. These test the harness classification, not a live JVM Kafka system-test run. |
| Duplicate-code check | 2,915 Rust files analyzed; 0 clones (local execution log). |
| Imports and formatting | Imported-path check passed (local execution log); final format check reported no files modified (local execution log). |
| Clippy | Final workspace/all-target Clippy passed with `-D warnings` (local execution log). |
| Generated documentation | Docgen built and wrote the reference tree (local execution log); both snippet synchronization passes changed 0 files (local execution log). Config reference generation passed (local execution log), and the generated `/tmp/review-config-reference-complete.md` compares byte-for-byte equal to `docs/config-reference.md`. KIP matrix generation and byte-identical regeneration passed (local execution log). |
| Proof evidence | **Prior implementation phase:** 604 proof files passed (local execution log). **This resolution phase:** no semantic proof-kernel edits were introduced; proof-package accounting (local execution log) and Creusot skip accounting (local execution log) passed. These records do not claim a new proof replay for the runtime fixes. |

No live cloud transport/object-lock qualification, live OPA deployment, JVM compatibility qualification, large concurrency stress run, or full workspace test pass is claimed. Local fake endpoints/in-process stores and targeted behavioral regressions establish the narrower evidence described above.

Lightweight checks already completed by the workflow/restore implementer: `aspect tests kafka-system-tests` passed 10 tests in one suite; `aspect check-imported-paths` passed; both edited workflow YAML files parsed; `bash -n` accepted the actual publish-step script; four probes executing that script with clock/cargo/timeout/sleep stubs passed (expired OIDC: zero upload calls; insufficient retry budget: one failed upload then stop; OIDC 429 followed by success: two calls with one 600-second sleep; bootstrap exhaustion: seven calls with six sleeps). These checks are recorded in task tool output; no separate log file was saved for them. They do not exercise live crates.io, OPA, or cloud transports.

## Integration follow-ups

Final validation exposed stale-metadata retries missing from manual ElectLeaders and DeleteTopics; both now replan the original operation within its deadline. Deletion retries retain the original topic UUID, recheck authorization and safety gates, and charge admission only once.

A cancellation cross-check also found that a timed-out deletion could later commit after the metadata watcher missed that short-lived topic generation. An owned completion task now retains guarded submission, cleanup, remote cascade initiation and audit. Cleanup holds the registry locks while checking captured partition identity and removing its original directory; a replacement runtime is preserved. Ordinary deletion audit now records each committed topic, including commits that finish after the client deadline. The late-commit and replacement-identity regressions passed in final runtime qualification.

## Historical review — original findings and evidence

The following text preserves the review as written before this implementation pass. Its “fixes needed,” proposed regressions, line numbers and validation limits describe that historical review, not unresolved current status. Current implementation links are in the resolution table above.

Reviewed on 2026-10-11 in `/workspace/krabka-broker`, branch `review/api-visibility-comments-20261011`, based on freshly fetched `origin/main` commit `143e7a2cc70cc423665d3096d9f765918e6cd49e`. This report reviews the **current working tree, including the earlier uncommitted fixes**. Findings describe remaining work; the earlier fixes are not counted again. Review source links refer to this checkout and its current line numbers.

**35 fixes needed: 6 P1, 21 P2, and 8 P3.**

Six subagents reviewed separate domains, with ownership rotated from the earlier implementation pass. The primary reviewer cross-checked findings against implementations, callers, configuration, dependency contracts, and existing tests. P1 means high-impact correctness, fencing, or persistent service failure; P2 means a functional defect requiring correction; P3 means an actionable documentation or API-contract defect.

### P1 — high-priority correctness and availability

#### 1. CreatePartitions can overwrite an existing partition from a stale image

**Evidence:** [create_partitions.rs:130](crates/broker/src/handlers/create_partitions.rs#L130), unguarded submission at [223](crates/broker/src/handlers/create_partitions.rs#L223). The pinned metadata dependency [validates only topic existence](https://github.com/krabka-io/krabka-protocol/blob/574e0e385f5d5187979049aa0578f1abf3fa123b/crates/metadata/src/image.rs#L1393) and [replaces existing partition entries](https://github.com/krabka-io/krabka-protocol/blob/574e0e385f5d5187979049aa0578f1abf3fa123b/crates/metadata/src/image.rs#L1033).

**Trigger and impact:** A serving broker still sees one partition after the controller grows the topic to two. A request to grow to three emits full records for partitions 1 and 2, replacing the already-created partition 1's assignment and resetting its epochs rather than preserving it.

**Fix needed:** Plan growth against authoritative controller state and use guarded image submission with retry/revalidation. **Regression:** Freeze a follower's one-partition view, commit growth to two, then request three through that follower; partition 1 must remain unchanged.

#### 2. AlterPartition and failover can overwrite newer leadership or ISR

**Evidence:** [alter_partition.rs:66](crates/broker/src/handlers/alter_partition.rs#L66), [unguarded append:140](crates/broker/src/handlers/alter_partition.rs#L140), [full-record construction](crates/broker/src/handlers/alter_partition/isr_update.rs#L143), and [failover snapshot/submission](crates/broker/src/leader_election/driver.rs#L217). Offline-directory failover has the same pattern at [failover.rs:34](crates/broker/src/handlers/broker_heartbeat/failover.rs#L34).

**Trigger and impact:** Admission snapshots metadata, awaits liveness, and later submits full partition records. An intervening election or ISR change makes the admission obsolete, but metadata application does not compare epochs. A stale plan can restore an old leader or ISR. Losing controller leadership can also forward an already-admitted plan. The [verified admission comment](crates/verified/src/isr/admission.rs#L44) overstates the host guarantee: its kernel checks supplied facts, whose authority is not preserved through append.

**Fix needed:** Guard all affected submissions against their planning image, controller term, and pending metadata tail; rebuild or reject stale plans with appropriate wire errors. **Regression:** Pause requests and failover plans after snapshotting; commit newer leadership/ISR and controller changes; resume and verify metadata never regresses.

#### 3. ShareFetch skips records still readable in diskless WAL objects

**Evidence:** [records.rs:126](crates/broker/src/handlers/share_fetch/records.rs#L126), [offset archival after a local miss](crates/broker/src/handlers/share_fetch/acquire.rs#L509), and [earliest initialization](crates/broker/src/share_partition/manager/cells.rs#L188).

**Trigger and impact:** WAL flushing trims unread offsets below the local log start. ShareFetch has only local and KIP-405 sources; diskless topics cannot enable KIP-405. Local `OffsetTooLow` is treated as deletion, archives available/deferred records, and advances share progress past payloads classic Fetch can still read from WAL. Fresh earliest groups skip the same history. [Dead-letter retrieval](crates/broker/src/share_partition/dlq/source.rs#L139) also omits this source and can lose the original payload after trimming.

**Fix needed:** Add a WAL-backed share/DLQ source and use the established logical deletion/retention floor for initialization and offset repair. **Regression:** Keep offsets 0–3 in WAL while trimming local storage to 4; existing and fresh share groups must acquire 0–3, including after restart, and DLQ copies must retain payloads.

#### 4. Concurrent index-cache fills can omit aborted transactions

**Evidence:** [index_cache.rs:320](crates/remote-storage/src/index_cache.rs#L320), [hit read:244](crates/remote-storage/src/index_cache.rs#L244), [production index fetch](crates/broker/src/remote_reader/blocking.rs#L54), and [transaction-index consumption](crates/broker/src/remote_reader/aborted_txns.rs#L52).

**Trigger and impact:** Two misses fetch the same immutable index concurrently. After one publishes its file, the other truncates that visible path with `std::fs::write`; a third hit can read empty or partial bytes successfully. An empty transaction index is accepted, potentially omitting aborted-transaction metadata from committed Fetch responses. The [race-safety rustdoc](crates/remote-storage/src/index_cache.rs#L212) is incorrect.

**Fix needed:** Write unique temporary files and publish atomically, coordinating eviction and bookkeeping. **Regression:** Use barriers around concurrent fills and publication; every hit must return complete index bytes and Fetch must retain the aborted-transaction entry.

#### 5. RLMM assignments hash with a different partition count than publication

**Evidence:** [broker/rlmm.rs:297](crates/broker/src/broker/rlmm.rs#L297), [discovered existing topic count](crates/remote-storage-topic/src/kafka_log/topic.rs#L65), and [publication hash](crates/remote-storage-topic/src/manager/rlmm.rs#L61).

**Trigger and impact:** An existing metadata topic's partition count differs from configured `num_partitions`, for example after a configuration change. The event log uses the discovered count, but initial and image-driven broker assignments use the configured count. Queries can miss metadata or remain unready; mutations can wait indefinitely for an event in an unconsumed partition.

**Fix needed:** Retain the discovered count and use it consistently for assignment, publication, and readiness. **Regression:** Bootstrap against a topic whose actual count differs from configuration, choose a user partition whose hashes differ, and verify assignment, catch-up, mutation completion, and lookup.

#### 6. One initial connection failure permanently kills an RLMM consumer

**Evidence:** [registered task token](crates/remote-storage-topic/src/kafka_log/consumer.rs#L58) and [connection-error exit](crates/remote-storage-topic/src/kafka_log/consumer.rs#L145).

**Trigger and impact:** After successful manager bootstrap, a per-partition consumer's initial `Client::build` fails transiently. Its task exits while its token remains registered. Re-adding the partition is a no-op, and reconciliation does not recreate unchanged assignments. Reads remain unready and mutation waiters remain blocked until an external assignment/reset repairs the subscription.

**Fix needed:** Retry initial connection in a cancellable loop or supervise dead tasks and recreate them. **Regression:** Fail the first connection attempt, then succeed without changing assignment; backlog consumption, readiness, and a published mutation must recover.

### P2 — functional API, storage, and workflow defects

#### 7. Legacy AlterConfigs still plans replacements from stale broker state

**Evidence:** [resource.rs:110](crates/broker/src/handlers/alter_configs/resource.rs#L110), [replacement tombstones](crates/broker/src/handlers/alter_configs/broker_configs.rs#L69), and [tiering-disable validation](crates/broker/src/handlers/alter_configs/topic_configs.rs#L80).

**Trigger and impact:** A lagging broker unaware of a newly committed cluster-default override accepts an empty replacement without deleting that override. Stale topic state can also bypass the explicit-delete requirement when disabling newly enabled tiered storage. The earlier IncrementalAlterConfigs fix does not cover this API.

**Fix needed:** Forward the original operation and plan against the controller's current image with guarded submission. **Regression:** Replace configs through a deliberately stale follower, including a newly added default override and a newly tiered topic.

#### 8. Concurrent first share initialization bypasses state-epoch fencing

**Evidence:** [state_machine.rs:132](crates/broker/src/share_coordinator/coordinator/state_machine.rs#L132), asynchronous persist/publication at [188](crates/broker/src/share_coordinator/coordinator/state_machine.rs#L188), and [replay fence behavior](crates/broker/src/share_coordinator/state.rs#L86).

**Trigger and impact:** Two initializations observe an absent key and both bypass a state-cell lock. An epoch-2 append followed by an epoch-1 append can both succeed with snapshot epoch 0 and install a lower fence. No valid sequential execution permits this outcome; live state may also disagree with replay.

**Fix needed:** Serialize lookup, fencing, append, and publication per logical key, including deletion/recreation. **Regression:** Barrier-controlled concurrent high/lower-epoch initialization must fence stale work and produce identical live/recovered progress and fences.

#### 9. Share recovery treats an oversized batch as successful EOF

**Evidence:** [recovery.rs:162](crates/broker/src/share_coordinator/coordinator/recovery.rs#L162) and [existing test:85](crates/broker/src/share_coordinator/coordinator/recovery/tests.rs#L85).

**Trigger and impact:** A valid batch exceeds supported `load_buffer_size`; the bounded read returns no batches despite remaining records. Replay stops successfully and marks an incomplete map active. The existing test explicitly expects a persisted snapshot to disappear with a 700-byte bound while a 4-KiB read recovers it. This contradicts the [partial-map guarantee](crates/broker/src/share_coordinator/coordinator/recovery.rs#L94).

**Fix needed:** Read at least one complete batch within an appropriate maximum, or fail replay before its boundary and keep the shard inactive. **Regression:** Oversized first and middle batches must yield complete recovery or a failed/inactive shard, never an active partial prefix.

#### 10. Fetch applies the oversized-batch exception after records already exist

**Evidence:** [read_loop.rs:123](crates/broker/src/handlers/fetch/read_loop.rs#L123), [cold-byte accounting](crates/broker/src/handlers/fetch/read_loop.rs#L359), and [current cold-budget test](crates/broker/src/handlers/fetch/read_loop.rs#L1125).

**Trigger and impact:** For a 500-byte response budget, partition A returns a fitting 100-byte batch, then B returns a 1,000-byte first batch against 400 remaining bytes. The response reaches 1,100 bytes. Kafka permits the first-batch exception only while the response has no records. Cold reads bypass the overrun flag entirely; the current test accepts a second cold batch after budget exhaustion. Comments incorrectly claim a positive budget always enforces a real cap.

**Fix needed:** Track first-nonempty permission across local, remote, WAL, and long-poll rereads. **Regression:** Small first partition followed by oversized second, with both positive and zero remaining budget; only an initially empty response may exceed its cap for progress.

#### 11. Share-group by-duration initialization ignores archived history

**Evidence:** [cells.rs:194](crates/broker/src/share_partition/manager/cells.rs#L194).

**Trigger and impact:** The first record in the requested duration is archived at offset 2 and the local log begins at 4. Initialization searches only local storage, selects 4 or returns `OFFSET_NOT_AVAILABLE`, and skips remotely readable history.

**Fix needed:** Use combined local/archive timestamp resolution with established deletion floors and decompression limits; correct the comment claiming equivalence with ListOffsets. **Regression:** Earlier archived match versus newer local match, and archived-only match.

#### 12. ListOffsets timestamp lookup ignores diskless WAL history

**Evidence:** [resolve.rs:597](crates/broker/src/handlers/list_offsets/resolve.rs#L597) and [timestamp.rs:45](crates/broker/src/handlers/list_offsets/timestamp.rs#L45).

**Trigger and impact:** Diskless topics have no KIP-405 `remote_topic_id`, so timestamp lookup scans only the trimmed local log. With WAL offset 0 at timestamp 1,000 and local offset 4 at 2,000, seeking timestamp 1,000 returns 4 or unknown although EARLIEST and Fetch still expose 0.

**Fix needed:** Add exact WAL timestamp lookup and combine its candidate with local results. **Regression:** Cold-only and earlier-cold matches, persisted deletion floor, compressed-record limits, and restart.

#### 13. Legacy ListOffsets v0 bypasses archived scheduled LATEST resolution

**Evidence:** [v0.rs:128](crates/broker/src/handlers/list_offsets/v0.rs#L128).

**Trigger and impact:** With legacy request versions enabled, archive/evict a future batch under Immediate delivery, then switch to Scheduled. v0 uses local-only LATEST and seek-to-end can skip the pending archived batch; the corrected v1+ path returns its pending offset.

**Fix needed:** Share archive-aware LATEST resolution with v0, including timeout handling. **Regression:** Run the existing archived mode-change scenario through an encoded v0 request.

#### 14. Stalled response writes retain global queue permits indefinitely

**Evidence:** [request permit lifetime](crates/broker/src/network/dispatch.rs#L430), [Fetch write](crates/broker/src/network/dispatch/fetch.rs#L54), [unbounded write wait](crates/broker/src/network/fetch_writer/drain.rs#L71), and [connection supervisor](crates/broker/src/broker/accept.rs#L153).

**Trigger and impact:** A client requests a response larger than its TCP receive window and stops reading. Idle deadlines cover handshake and request reads, but neither response writes nor an outer connection supervisor. The stalled task retains request-count and request-byte permits. Enough stalled receivers exhaust `queued.max.requests` and block unrelated traffic despite a configured idle timeout. A second reviewer independently checked the absence of an outer deadline.

**Fix needed:** Enforce a write/no-progress deadline consistently for framed, TLS, and sendfile paths; close timed-out connections and release permits. **Regression:** A nonreading receiver with a short idle/no-progress window must close and return capacity, while another client completes requests. Verify ongoing write progress does not cause premature closure.

#### 15. Multipart file uploads queue unbounded parts

**Evidence:** [client.rs:194](crates/object-store/src/ops/client.rs#L194). Pinned `object_store` 0.13.2 `WriteMultipart::write` queues tasks without backpressure; callers must use `wait_for_capacity`.

**Trigger and impact:** A slow/stalled backend lets the loop read and retain an entire archive segment in pending parts and launch unbounded PUTs. Errors are observed only at `finish`, after the input has been read. Cancellation cleanup does not bound outstanding work.

**Fix needed:** Await bounded multipart capacity before queuing more chunks, preserving abort cleanup. **Regression:** Block part completion while reading a many-chunk file; outstanding uploads and retained payloads must remain bounded, and a part failure must stop further input promptly.

#### 16. Continue-on-corrupt fails after skipping an interior restore segment

**Evidence:** [skip handling](crates/restore/src/lib.rs#L303) and [alignment rejection](crates/restore/src/materialize/segment.rs#L189).

**Trigger and impact:** Restore good offsets 0–9, skip corrupt 10–19, then process good 20–29. Alignment rejects base 20 because target LEO is 10, failing a requested continuation and leaving partial output. The alignment comment calls every gap a sequencing bug; the current regression corrupts a different partition's only segment and misses this case.

**Fix needed:** Carry an explicit skipped-range policy into materialization and preserve intended forward gaps, or consistently implement/report partition-level stopping. Never reset away the restored prefix. **Regression:** Three segments in one partition with only the middle damaged, plus compacted and filtered boundaries; surviving offsets and the skip report must agree.

#### 17. Epoch-list metadata queries bypass assignment/readiness gates

**Evidence:** [manager/rlmm.rs:141](crates/remote-storage-topic/src/manager/rlmm.rs#L141).

**Trigger and impact:** Unlike sibling queries, the public trait method directly reads the inner cache, exposing old snapshot/assignment data when unassigned and incomplete metadata during catch-up. Forwarding through `SwappableRlmm` exists; no current production broker caller of this particular method was found, so the immediate impact is the exposed API contract.

**Fix needed:** Apply `read_if_ready` and include this method in gate documentation. **Regression:** All metadata queries must agree before assignment, during catch-up, after readiness, and after removal.

#### 18. Millisecond backup identities can overwrite completed captures

**Evidence:** [capture ID](crates/backup-cli/src/run.rs#L98), [artifact publication](crates/backup-cli/src/run.rs#L483), and [unconditional overwrite](crates/backup-cli/src/archive.rs#L53).

**Trigger and impact:** Two captures to one prefix begin in the same millisecond, or the clock repeats a prior timestamp. Both use one directory and overwrite artifacts and the final manifest. Both may report success while only one survives or mixed artifacts fail integrity checks.

**Fix needed:** Atomically reserve identities and retry collisions, or introduce unique sortable identities while retaining old capture discovery. **Regression:** Force identical timestamps for concurrent different captures; require distinct immutable captures or an explicit collision error.

#### 19. Remote index and backup document reads bypass size caps

**Evidence:** [remote index read](crates/remote-storage/src/s3/fetch.rs#L79), [manifest read](crates/backup-cli/src/run.rs#L826), [verification artifacts](crates/backup-cli/src/run.rs#L526), and [group offsets](crates/backup-cli/src/run.rs#L606).

**Trigger and impact:** Oversized/corrupt objects are buffered with unrestricted `get` before cache budgets or recorded size/digest checks run. Production cold Fetch reaches the index path. The capped absolute WAL reader does not cover these calls, so advertised memory limits do not prevent excessive allocation here.

**Fix needed:** Apply independent per-artifact maximums through `get_capped`, with streamed-size enforcement; do not trust manifest-declared sizes as the only limit. **Regression:** Reject oversized advertised objects before buffering and streams exceeding advertised size while reading.

#### 20. Restore authenticates snapshot bytes different from those it consumes

**Evidence:** [authentication](crates/restore/src/lib.rs#L391), [metadata reread](crates/restore/src/materialize/format.rs#L206), and [RLMM reread](crates/restore/src/materialize/format.rs#L117).

**Trigger and impact:** A mutable local snapshot path changes after hashing but before materialization. Authentication retains a boolean, then later code rereads the path, allowing different bytes to seed a restore reported as authenticated. This requires concurrent modification of local input files.

**Fix needed:** Retain and decode the authenticated bytes, or authenticate the exact bytes consumed. **Regression:** Replace a snapshot after authentication and verify the replacement cannot seed an authenticated target.

#### 21. Topic-backed metadata mutation success does not imply application

**Evidence:** [pump rejection handling](crates/remote-storage-topic/src/manager/pump.rs#L37), [success returned after offset wait](crates/remote-storage-topic/src/manager/rlmm.rs#L71), and [SPI errors](crates/remote-storage/src/metadata_manager.rs#L41).

**Trigger and impact:** A duplicate add, unknown-segment update, or invalid transition is rejected by the inner manager. The pump logs it, advances `applied`, and wakes the publisher, which returns `Ok(())`. This violates the error/visibility contract and differs from the in-memory implementation. Channel closure also ends the wait without establishing application.

**Fix needed:** Convey local mutation outcomes to waiters and fail interrupted waits, while preserving appropriate replay handling. **Regression:** Compare topic-backed and in-memory errors for rejected mutations and verify shutdown before application cannot report success.

#### 22. Publish retries can outlive their authentication token

**Evidence:** [one authentication step](.github/workflows/publish.yml#L252) and [retry loop](.github/workflows/publish.yml#L273).

**Trigger and impact:** The trusted-publishing token lasts 30 minutes, but six permitted 429 waits can consume 60 minutes. Later attempts use the original expired token and fail authentication, potentially leaving a partially published release. The comment claiming uploads stay inside token lifetime is false for this path.

**Fix needed:** Refresh authentication after long waits or resume bounded upload batches in freshly authenticated jobs. **Regression:** Simulate three 429 waits followed by success with a fake clock/token provider; the final attempt must use refreshed credentials, preserving bootstrap-token support.

#### 23. Missing known-failure system tests produce a green result

**Evidence:** [verdict](.aspect/kafka_system_tests.axl#L160), [exit decision](.aspect/kafka_system_tests.axl#L332), and [test codifying acceptance](.aspect/kafka_system_tests_test.axl#L162).

**Trigger and impact:** An expected-failure manifest entry has no matching ducktape result due to renamed parameters or an aborted run. `NOT RUN` becomes `known failure`, so a selection containing only that entry exits successfully with no executed evidence. A practical selectable entry exists in [suite.json:179](tools/kafkatest/suite.json#L179).

**Fix needed:** Treat missing/skipped/aborted execution as failure before applying expected-failure rules; accept only an observed test failure. Update [the verdict documentation](docs/kafka-system-tests.md#L151). **Regression:** Missing entries with and without `known_failure` must fail; preserve observed FAIL and PASS classifications.

#### 24. Snippet extraction silently accepts a different anchor

**Evidence:** [snippets.rs:16](crates/docgen/src/snippets.rs#L16).

**Trigger and impact:** `contains()` makes requested `quorum` match existing `quorum_state`, and requested `foo` select a preceding `foobar` region. CI can approve wrong example text rather than reject the reference. The primary reviewer compiled the actual source as a standalone module and reproduced both cases: missing `quorum` returned `Ok("let selected = 42;")`; `foo` returned `Ok("wrong_region();")` despite a later correct marker.

**Fix needed:** Parse complete marker identifiers and compare exactly; reject malformed/ambiguous pairs. **Regression:** Missing prefix anchors, preceding longer anchors, and marker-looking string content, alongside valid extraction and idempotent synchronization.

#### 25. The in-process formatter exits its caller on argument errors

**Evidence:** [public entrypoint contract](crates/format/src/lib.rs#L75) and [Cli::parse_from](crates/format/src/lib.rs#L108). Restore uses [try_parse_from](crates/restore/src/lib.rs#L136).

**Trigger and impact:** An embedding caller passes invalid argv or help/version flags. Clap's `parse_from` exits the process, rather than returning the advertised exit code or panicking as documented. Callers cannot recover with normal error handling or `catch_unwind`.

**Fix needed:** Use fallible parsing and return the corresponding code/structured error, leaving process exit to binary entrypoints; correct rustdoc. **Regression:** Child-process harness proves control returns after invalid arguments and help/version, with the intended output and code.

#### 26. OPA serialization differs from the published compatibility contract

**Evidence:** [wire.rs:19](crates/broker/src/authorizer/opa/wire.rs#L19) and the [published opa-kafka-plugin v1.5.1 schema](https://github.com/open-policy-agent/opa-kafka-plugin/blob/v1.5.1/README.md#usage). The primary reviewer independently retrieved the release README and compared its schema with the current serializer.

**Trigger and impact:** The serializer sends `input.request`, a string principal, and title-case enum names. Published policies expect `input.action.operation` such as `READ`, `action.resourcePattern.resourceType` such as `TOPIC`, and `requestContext.principal` with `name`/`principalType`. A default-deny policy reading those fields rejects ordinary callers. An undefined decision can also fall through to configured `allow_on_error`. Comments promising compatibility and unchanged policy portability are false.

**Fix needed:** Pin the supported protocol release, match its schema/vocabulary, and supply required request context. **Regression:** A mock endpoint must decide from the published JSON fields and verify the complete payload for allowed and denied requests; retain HTTP-status/cache coverage. No live OPA instance was exercised during this pass.

#### 27. Exported derive macros discard generic parameters and where clauses

**Evidence:** [EnumStr expansion](crates/macros/src/enum_str.rs#L173), [PrimitiveCmp expansion](crates/macros/src/primitive_cmp.rs#L15), and [RefinedNewtype expansion](crates/macros/src/refined_newtype.rs#L255). The [shared helper](crates/macros/src/meta.rs#L142) already demonstrates splitting generics correctly.

**Trigger and impact:** Documented item shapes do not exclude generics, but generated implementations use a bare identifier. A generic data-bearing `Event<T>` deriving EnumStr and a const-generic `Seq<const DOMAIN: u8>(u64)` deriving PrimitiveCmp fail compilation. The primary reviewer compiled both external consumer declarations using an existing cached macro artifact; rustc returned `E0107: missing generics` for both. RefinedNewtype has the same omission in current expansion source; that third macro was not reproduced with a consumer build.

**Fix needed:** Preserve impl/type generics and where clauses across implementations and generated parsing functions. If specific generic forms are intentionally unsupported, reject them explicitly and document the restriction. **Regression:** External compile-pass cases for a bounded generic enum, const-generic primitive comparison, and refined rules using a const generic.

### P3 — public contracts and misleading comments/documentation

#### 28. Define stricter live clock-bound changes in Log::set_config

**Evidence:** [set_config](crates/log/src/log/state.rs#L413) and [cached watermark](crates/log/src/log/delivery.rs#L80).

**Contract gap:** Raising `delivery_clock_uncertainty` after visibility advances can disagree with a freshly computed deadline. Watermark documentation explicitly promises monotonic visibility while records remain, so automatic rollback would contradict that promise. This is a public API contract question, not a confirmed live broker configuration exploit.

**Fix needed:** Document prospective treatment of stricter bounds, or fence unsupported live changes, and test the chosen semantics. Avoid introducing a rollback that violates already-visible offset guarantees.

#### 29. The code style guide recommends forbidden Clippy suppressions

**Evidence:** [code_style_guide.md:60](docs/style_guides/code_style_guide.md#L60), [68](docs/style_guides/code_style_guide.md#L68), and [controlling prohibition](CLAUDE.md#L196).

**Fix needed:** Remove scoped-allow recommendations and align the guide with CLAUDE and CONTRIBUTING. **Validation:** One unambiguous policy should remain; this requires documentation review rather than a runtime test.

#### 30. Benchmark documentation contradicts the current build and comparison workflow

**Evidence:** [README.md:153](README.md#L153), [198](README.md#L198), [CONTRIBUTING.md:84](CONTRIBUTING.md#L84), and [benchmarks.yml:33](.github/workflows/benchmarks.yml#L33). The [crate_bench macro](bazel/defs.bzl#L235) exists and is invoked by BUILD files.

**Fix needed:** Describe Cargo measurement, Bazel bench compilation/runnable targets, and current alternating reference/candidate comparisons. Remove claims that targets do not exist and [regression comparison remains deferred](.github/workflows/benchmarks.yml#L72). **Validation:** Trace documented commands and gating claims to current macro/CI steps.

#### 31. Style guides name nonexistent release and documentation automation

**Evidence:** [README guide](docs/style_guides/readme_style_guide.md#L7) says publication uses release-plz; [rustdoc guide](docs/style_guides/rustdoc_style_guide.md#L228) promises a weekly `docs-freshness` job. Current workflows use tag-driven publication and nonscheduled CI checks for generated references.

**Fix needed:** Name/link actual workflows and their triggers. **Validation:** Cross-check automation claims against workflow definitions.

#### 32. Published stamp helpers hide preconditions in erased proof annotations

**Evidence:** [stamp.rs:43](crates/verified/src/stamp.rs#L43), [80](crates/verified/src/stamp.rs#L80), [109](crates/verified/src/stamp.rs#L109), and [public reexports](crates/verified/src/lib.rs#L279).

**Contract gap:** Normal Rust callers do not receive Creusot precondition checking. `covering_stamp_range_index(&[0], &[], 0)` indexes an empty end slice and panics, yet ordinary rustdoc omits equal-length requirements and panic behavior. Insertion additionally requires sorted, individually ordered, disjoint ranges. Existing broker/log callers maintain the arrays correctly; no production panic is alleged.

**Fix needed:** State concrete preconditions and `# Panics` in ordinary rustdoc; link insertion users to `stamp_ranges_valid`, or provide an explicitly checked API. **Validation:** Inspect non-Creusot rustdoc and add consumer-facing validation examples and behavioral malformed-input checks for the chosen contract.

#### 33. Profiling server wrappers document unrelated errors and omit address panics

**Evidence:** [server.rs:107](crates/telemetry/src/profiling/server.rs#L107), [119](crates/telemetry/src/profiling/server.rs#L119), and [actual address parsing](crates/telemetry/src/profiling/server.rs#L99).

**Contract gap:** Public wrappers claim query/backend errors and poisoned-state/columnar panics, but actually parse a socket address and bind/serve an admin endpoint. Invalid environment or fallback addresses panic before the advertised `io::Result` can report the configuration failure.

**Fix needed:** Document actual bind/serve errors and address-parse panics consistently, or return `InvalidInput` for invalid configuration. **Regression:** Isolated invalid environment/fallback-address cases matching the chosen behavior.

#### 34. The API catalog says no code constructs an Envelope

**Evidence:** [api_catalog.rs:68](crates/broker/src/api_catalog.rs#L68) contradicts [forward_to_controller.rs:102](crates/broker/src/handlers/forward_to_controller.rs#L102), used by DeleteTopics and IncrementalAlterConfigs.

**Fix needed:** Describe implemented forwarding accurately while retaining the actual mixed-controller-quorum scope boundary. **Validation:** Trace the catalog claim to the current constructor and callers.

#### 35. Replica selector parsing advertises impossible storage errors

**Evidence:** [replica_selector.rs:58](crates/broker/src/replica_selector.rs#L58).

**Fix needed:** Replace the copied log-I/O/corruption/offset `# Errors` text with the actual contract: parse `leader` or `rack-aware`, otherwise return the unsupported trimmed value. **Validation:** Compare rustdoc with the pure string match; no new runtime test is needed for this documentation correction.

### Review workflow, evidence, and limits

| Reviewer | Coverage |
| --- | --- |
| `storage_api` | Kafka handlers, authorization, controller admin planning, routing, errors, admission and versions |
| `auth_workflows` | Fetch/ShareFetch/ListOffsets, record and transaction visibility, scheduled delivery, retention, compaction, remote/WAL reads |
| `broker_api` | Crate exports, public Rust types and invariants, extension traits, public rustdoc and examples |
| `comments_contracts` | Object-store, remote metadata/cache, backup/restore, snapshots, size limits, retries and integrity |
| `rust_visibility` | All six GitHub workflows, Aspect/Bazel tooling, packaging, guides and snippet synchronization |
| `record_visibility` | Raft/controller fencing, failover, classic/consumer/share/streams coordinators, persistence and recovery |
| Primary reviewer | Cross-checks, network/session/TLS and queue accounting, telemetry/configuration contracts, consolidation |

The current pass used source/caller tracing, comparison with dependency contracts, inspection of existing behavioral tests, an executed snippet reproduction, a failing external macro-consumer compile, and independent comparison with the published OPA schema. Proposed regressions are **work still needed**, not tests claimed to have run. No new full build, live Kafka qualification, object-lock test, GCS transport exercise, or large concurrency stress run was performed during this review. A comprehensive scoped review does not establish that every file or interleaving is defect-free.

Earlier implementation validation remains separate evidence: 6,908 Cargo library tests, seven targeted Bazel suites, workspace/all-target Clippy, 417 Aspect tests, and the 604-file Creusot proof replay passed. Those checks were completed before this review; they do not cover every failure condition listed above, and some inspected tests currently encode incorrect behavior.

One further coordinator seam needs a controlled behavioral investigation before it becomes a confirmed finding: [ProductionOffsetsLog::append_as_leader](crates/broker/src/coordinator/unified/offsets_log.rs#L147) checks leadership before awaiting writer enqueue/ack without holding the partition transition barrier. Commit waiting fences the response, but the effect of a stale physical append on follower convergence has not been demonstrated.

Implementation should preserve Kafka wire behavior and persisted 1.x/rolling-upgrade compatibility. Fix authority checks at planning/append boundaries, preserve stored offsets rather than resetting surviving history, and keep behavioral regression tests separate from documentation-only corrections.
