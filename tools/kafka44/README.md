# Released Kafka 4.4 evidence

Issue [#1969](https://github.com/krabka-io/krabka-broker/issues/1969) requires released artifacts, not the moving `tools/kafkatest/suite.json` source revision. `aspect kafka44` starts a disposable combined-KRaft reference broker and uses the same immutable **Linux amd64** Apache Kafka 4.4.0 image for reference and candidate tools. It records raw commands, outputs, exit codes, reference configuration, client version, candidate revision, and SHA-256 hashes of candidate configuration inputs.

This is the first evidence lane, **not a qualification suite**. The task verifies the revision exists in the local Git object store. A zero exit code means collection succeeded; `qualification_pass` remains false. No output normalization silently converts API, feature, or configuration differences into parity passes. Review raw results before drawing conclusions. The collector never upgrades candidate features or changes candidate configuration.

## Run

Requires Aspect CLI, GNU timeout, Docker, Linux amd64, and permission to run containers. Oracle-only collection is useful before provisioning candidates:

```sh
aspect kafka44 --revision FULL_KRABKA_COMMIT_SHA --output evidence/kafka44-oracle
```

For candidate evidence, start two isolated brokers from the **same recorded revision**: one with default compatibility gates, and one explicitly configured with the candidate API/feature gates and intended metadata/feature levels. Supply their complete effective configuration files (retain these privately; only their checksums enter evidence):

```sh
aspect kafka44 \
  --revision FULL_KRABKA_COMMIT_SHA \
  --default-bootstrap localhost:19092 \
  --enabled-bootstrap localhost:29092 \
  --default-config /private/default.toml \
  --enabled-config /private/enabled.toml \
  --output evidence/kafka44-comparison
```

Candidate tools run with host networking; advertised broker addresses must be reachable there. Bootstrap addresses must identify the prepared candidates, not production. The output directory must not already exist. Review output for private endpoint/configuration information before attaching it publicly. The optional synthetic SASL authorization fixture is described below; general client-properties inputs are not supported.

`evidence.json` contains explicit API-range differences (endpoint identities are excluded, empty parsing fails), raw API ranges, feature levels, and broker-default configuration discovery from each requested side. Timeouts and tool failures produce a nonzero status and retained output. The owned reference container is removed on normal completion or handled errors. If the process is killed, remove only the container named by the recorded startup command.

## Remaining acceptance criteria

Keep #1969 open until released-client/protocol differential scenarios verify config validation and errors, authorization, minimum feature boundaries, default/enabled behavior, restart/recovery, and relevant multi-broker paths for all six surfaces: share DLQ, delayed tiered uploads, misrouting detection, controller unregistration, topic-ID transactional offset commits, and Streams topology APIs. Existing Rust/unit coverage and trunk system-test inventory are useful inputs, not released-4.4 execution evidence. Library-only, Connect and shell-tool features are outside broker parity unless they expose a broker obligation.

## Observed baseline

[`qualification/kafka44-oracle-e201a1d.json`](../../qualification/kafka44-oracle-e201a1d.json) records a successful local Linux amd64 reference-only collection on 2026-11-01. The released client reported `4.4.0`; all three probes and owned-container cleanup returned zero. The recorded Krabka revision identifies the source inventory under review, **not an executed candidate**: only `sides.oracle` exists. Verbose reference startup logs are omitted from this checked-in snapshot. Candidate interoperability and all listed feature scenarios remain unexecuted.

## Task verification

The collector is an Aspect task, following the repository task policy; it requires GNU `timeout` to bound Docker/tool invocations. `aspect tests kafka44` covers full revision validation, every partial candidate-input combination and API-range comparison (including empty-output refusal). `aspect axl-tests` includes these tests in the existing CI task lane. The dispatch workflow installs the pinned Aspect CLI before collecting.

On 2026-11-01 the AXL collector also completed a local reference-only run: version `4.4.0`, all three probes and owned-container cleanup returned zero. This verifies the port, not candidate interoperability.

## Executed default/enabled candidates

[`qualification/kafka44-candidates-e201a1d.json`](../../qualification/kafka44-candidates-e201a1d.json) records released-4.4 tools against the baseline broker revision `e201a1dfafd26f4ddae193a3cd1f0b340c819aa6`, built with `bazel build //:broker_bin //:format_bin`. The default standalone node was formatted at `4.3-IV0`; the enabled node at `4.4-IV2` with both unstable switches explicitly enabled in TOML. All nine discovery probes returned zero. **This is not interoperability qualification.**

Observed API-range differences:

- Default: `ApiVersions` v4 versus oracle v5; `DeleteGroups` v2 versus v3; `StreamsGroupHeartbeat`/`StreamsGroupDescribe` v0 versus v1; `TxnOffsetCommit` v5 versus v6; topology-update and controller-unregistration APIs unsupported versus oracle v0.
- Enabled: `InitProducerId` advertises v6 versus oracle v5. Every other released-tool API row matched, but that does not exercise its semantics.
- Default feature support ends at metadata `4.3-IV0` and share level 1; oracle ends at `4.4-IV2` and share level 2. Enabled supports metadata through `4.5-IV0` (beyond the oracle), finalized at `4.4-IV2`, with share level 2.

These retained mismatches block a blanket released-4.4 parity/default-graduation claim. Config discovery output is retained raw; it has not been semantically compared or used as config validation proof. No authorization, six-surface behavioral, restart or multi-broker assertion has been run. The locally retained effective configurations are identified by SHA-256; CLI controller addresses and formatting levels are recorded in the snapshot provisioning notes.

Both candidates were then stopped with SIGTERM (both exited 0) and restarted from the same persisted directories and effective configurations. All nine discovery probes again returned zero with identical API-range differences. The snapshot retains post-restart outputs. This is discovery continuity only: no six-feature state, records, failure injection or multi-broker recovery was exercised.

## Released-codec negative wire scenarios

Add `--protocol-scenarios` to compile `ProtocolScenarios.java` with a local JDK (17 or newer) against jars copied from the pinned reference image, then execute it with the image's JVM and jars. No moving Maven dependencies or candidate protocol implementation is used. The fixture hash, compile commands, raw responses and named error-map results are retained. With candidates, also pass `--default-controller HOST:PORT --enabled-controller HOST:PORT`.

The seventeen scenarios exercise six controller lifecycle cases, six ApiVersions v5 routing-identity cases plus unknown controller unregistration through controller and broker listeners (the broker may forward), topology update without a configured plugin, an invalid Streams v1 group, and TxnOffsetCommit v6 with an unknown topic UUID. They deliberately do **not** claim positive transaction, registration-deletion, plugin lifecycle, authorization, tiered storage, authenticated/pre-authentication misrouting, recovery or multi-broker qualification. These are negative probes, not substitutes for those obligations. Transport errors and missing result rows cannot become parity passes. Candidate error-map differences make the task fail while retaining evidence; complete raw responses remain available for message/field review. Error-map equality alone is not full response equality.

For an oracle-only smoke check:

```sh
aspect kafka44 --revision FULL_KRABKA_COMMIT_SHA --protocol-scenarios --output evidence/kafka44-protocol-oracle
```

The path-filtered pull-request and manual evidence workflow executes these scenarios alongside discovery. `qualification_pass` is still always false.

The [coverage ledger](COVERAGE.md) maps each surface to invariants, models, runtime regressions, released-oracle results, fault coverage and CI receipts. Known counterexamples and missing executions remain explicit.

`qualification/kafka44-routing-e201a1d.json` extends the initial negative snapshot with six real ApiVersions v5 identity scenarios. The released Admin client discovers the candidate cluster/node identity first. Enabled matches all eleven error maps: absent/correct broker identity accepts; partial identity is INVALID_REQUEST; wrong cluster is REBOOTSTRAP_REQUIRED on broker but accepted on controller. Defaults retain their pre-graduation unsupported-version/connection-close behavior. Full ApiVersions response rows remain raw because API/feature mismatches are independently retained. This is not authorization, pre-authentication or multi-node routing proof.

## SASL authorization and pre-authentication routing

Add `--authorization-scenarios --protocol-scenarios` for a separate oracle variant with a SASL/PLAIN listener and StandardAuthorizer. It uses four **synthetic fixture users**, not production credentials: `denied` has no ACLs; `operator` has Cluster Alter and Group Read; `writer` has TransactionalId Write only; `reader` has TransactionalId Write and Group Read. The fixture creates only those five scoped ACL bindings through the privileged plaintext test listener and waits for DescribeAcls visibility. Never point this mutating harness at production.

Ten cases cover pre-authentication identity bypass, authenticated misrouting detection, denied Cluster/Group/TransactionalId operations, explicitly authorized controller/topology paths, Group denial after TransactionalId permission, and unknown UUID after both preamble permissions. With paired candidates also supply `--default-auth-bootstrap HOST:PORT --enabled-auth-bootstrap HOST:PORT`, with the synthetic users configured on their dedicated SASL listeners. Config hashes, exact grants, fixture hashes, raw fields and error-map differences are retained. Error-map equality does not claim byte equality of implementation-specific diagnostic messages (Kafka's controller authorization error embeds ephemeral connection/session identities).

`qualification/kafka44-authorization-e201a1d.json` records actual released4.4 execution: enabled baseline matches all ten error maps; default API gates retain their differences. The CI oracle workflow runs plain and authorization variants separately, so the authorizer-enabled variant is not mistaken for default configuration discovery. Positive transaction state/durability, further ACL permutations and multi-broker forwarding remain separate obligations.

## Positive topic-ID transactional commits

`--transaction-scenarios --protocol-scenarios` adds explicit released-codec v6 and legacy v5 transactions on uniquely named disposable topics/groups. It follows coordinator endpoints, verifies missing-group error precedence, offset UUID echo, commit publication, pending-offset invisibility and abort preservation. The isolated test cluster must allow internal topics (single-node fixtures use offsets/transaction-state RF=1 and transaction-state min ISR=1). Do not point this mutating fixture at production.

The oracle variant restarts only its own container, then verifies unchanged topic UUIDs and committed-not-aborted offsets for both groups. The task never restarts arbitrary candidate endpoints. To verify a caller-owned candidate after restarting its exact binary/config/data, use the retained `RECOVERY` identity with `TransactionScenarios ENDPOINT recover TOPIC GROUP TOPIC_UUID`; a missing/ambiguous identity fails collection. This is clean-restart coverage, not crash, pending-marker, multi-broker or deletion/recreation qualification.

The CI matrix runs plain, authorization and transaction variants independently. Actual snapshot results and remaining coverage holes are recorded in [COVERAGE.md](COVERAGE.md); no variant sets `qualification_pass` true.

### Delayed-copy configuration fixture

`--protocol-scenarios` also executes `ConfigScenarios.java` using only the pinned released jars: thirteen topic discovery/validate-only cases for `remote.copy.lag.ms` and `remote.copy.lag.bytes`, including -2/-1/0/1000/nonnumeric values and unchanged configuration. Candidate differences or missing cases fail collection. `qualification/kafka44-delayed-copy-config.json` records the first actual released/enabled comparison and a successful full oracle collector invocation. No remote-storage IO or upload scheduling is inferred from config acceptance.

### Copy-only delayed upload smoke

`aspect kafka44-remote` runs the retained released-client `CopyProbe.java` against two **prepared isolated** brokers and archives. It validates the oracle container's immutable image, compiles only against its exported4.4 jars, records candidate revision/config/source hashes and raw outputs, and requires both exact successful scenario rows. It neither provisions nor restarts arbitrary candidate endpoints.

Prepare the released broker with `RecordingStorage.java` compiled against the pinned image's jars and placed on `remote.log.storage.manager.class.path`; this is a copy-only recording backend, **not production storage or a remote-read test**. Configure `remote.log.storage.system.enable=true`, `remote.log.storage.manager.class.name=RecordingStorage`, `remote.log.storage.manager.impl.prefix=rsm.config.`, and `rsm.config.dir` to the mounted archive directory. Use the topic-backed remote metadata manager, RF1/minISR1 only for this single-node fixture. The retained receipt includes the complete oracle properties and dynamic internal-topic minISR override. Configure the candidate with a fresh owned data directory, enabled API gates, local `[remote_storage] storage_dir`, explicit self-bootstrap and RF1/minISR1 under `[remote_storage.kafka_metadata]`. Set both copier intervals to500ms (candidate `--remote-log-manager-interval 500ms`). Preserve all setup outputs and exact binary SHA.

```console
aspect kafka44-remote --revision <exact-runtime-sha> \
  --oracle-container <owned-pinned-oracle> --oracle-bootstrap <oracle-endpoint> \
  --oracle-archive <mounted-archive-host-path> \
  --enabled-bootstrap <owned-candidate-endpoint> --enabled-archive <local-archive> \
  --enabled-config <effective-candidate-toml> --mode all --output <new-evidence-directory>
```

The first actual task pass is retained in `qualification/kafka44-delayed-copy-runtime.json`. The probe creates a fresh topic, snapshots archive filenames, produces24 acknowledged64KiB records with1MiB segment roll, observes no new archive files for3seconds under60s/100MB thresholds, then changes lag.ms to0 and requires new files. This is a short copy/dynamic-update smoke, not proof of threshold timing or uploaded data integrity, remote reads, crash recovery, leadership fencing or multibroker parity. No clean CI result may broaden that claim.

The `--transaction-scenarios` oracle restart also runs `ControllerRecovery.java`: register a synthetic nonvoter, successfully remove it, require the repeat error, and require CONTROLLER_ID_NOT_REGISTERED after the owned clean restart. Exact error codes are checked, not merely error-map equality (some successful released responses have empty `errorCounts()`). Caller-owned candidates are restarted only by their owner; external evidence is retained in `qualification/kafka44-controller-removal-restart.json`.

`--mode all` additionally executes natural6s age,1MiB newer-byte crossing and -1 derived6s local-retention age, on both endpoints; individual modes are `dynamic-zero` (default), `age`, `bytes` and `derived-age`. Each requires its own exact scenario identity; an age pass cannot count as a byte-trigger pass. Actual8-case receipt: `qualification/kafka44-delayed-copy-threshold-smokes.json`. These remain bounded positive/negative smokes, not a full remote-storage qualification.

The current `all` mode also includes `failed-upload`: it temporarily denies writes to the **owned** archive, produces locally acknowledged records, observes3s of failed upload, restores the exact original permissions in Java finally and a task fallback, and requires retry. Every successful probe is followed by released FileRecords decoding of that topic's actual archived prefix (batchCRC, contiguous offsets, exact keys/value bytes). The preparation/setup regression verifies unrelated-topic/global files cannot satisfy a probe. Use the per-topic recording backend layout from the current source; old global-layout fixtures cannot satisfy these scoped checks. Current accepted receipt: `qualification/kafka44-delayed-copy-scoped-fault-data.json` (10executions,56.0s); earlier global-delta receipts are historical only.

## Integrated candidate receipt

The [2026-11-03 integrated receipt](../../qualification/kafka44-integrated-20261103.json) records an actual fresh binary containing all scoped Streams/DLQ implementation heads, released-codec behavioral/configuration/authorization comparisons, clean transaction/controller restart and scoped copy-failure/content checks. It also retains setup failures, default gate failures and enabled discovery differences. See the [coverage ledger](COVERAGE.md#exact-integrated-implementation-candidate-2026-11-03) for boundaries. This is bounded evidence, not full interoperability qualification or default graduation.


## Positive reference Streams lifecycle

`aspect kafka44 --protocol-scenarios --streams-scenarios --revision <full-sha> --output <new-directory>` configures the immutable released oracle with its reference in-memory topology plugin. Nine explicit assertions cover first-member solicitation, no duplicate solicitation, initial NOT_STORED, successful push, STORED with the exact source topic, leaving both members, successful group deletion, and recreated-group solicitation/empty storage. Every response remains in raw output. The optional enabled candidate must independently configure the same reference plugin. The default candidate is deliberately not passed this positive fixture; default rejection coverage remains separate. Missing rows, response errors, assertion failures or outcome differences fail the task.

The CI `streams` variant runs the released oracle fixture. It does not establish candidate behavior. `qualification/kafka44-streams-positive-historical-20261104.json` retains the initial paired 9/9 pass against the historical integrated679706f binary; its explicit executable identity must not be relabeled as the final head. Reference memory-plugin, single-node checks do not qualify durable shared storage, injected plugin failures or coordinator movement.

### Task-owned abrupt transaction recovery

`--transaction-scenarios --protocol-scenarios --oracle-crash-recovery` replaces the task-owned oracle's clean restart with an actual Docker `SIGKILL`, then starts its unchanged container/data and checks both modern/legacy topic UUIDs and committed-not-aborted offsets. Stop/start exit codes and crash mode are retained; a failed kill fails collection. It never kills candidate endpoints. CI runs clean and abrupt variants separately. This tests completed single-node RF1 transactions, not a pending-marker crash, power-loss/fsync durability or multi-broker failover.

### Previous-release upgrade and reversible rollback

`aspect kafka44-upgrade --revision FULL_BUILD_SHA --candidate-binary /absolute/prebuilt/krabka-broker --output /fresh/private/directory` provisions fresh data with immutable released Krabka v1.1.5 image `sha256:577a686c1b24a5a655ec8a2a82c877e119e3dd3f07b5fa617e91d05851714e1e`, using its formatter and broker. Pinned Kafka4.4 codecs seed a record/group offset, then verify exact UUID, finalized feature map and records through native candidate replacement and released-binary rollback. The candidate writes a second record, rollback writes a third, and both must read all preceding exact keys/values/offsets. The candidate must reject metadata33 under unchanged production gates without mutating finalized levels. The task checks candidate clean exit and released rollback shutdown, retains Docker names/data/logs/commands, and refuses an existing output directory. Only task-owned fixtures are stopped.

This is a single-node RF1 clean data compatibility regression, not a mixed-version rolling quorum or rollback after new feature finalization. Candidate revision is supplied by the caller; binary digest is recorded separately and must be tied to its build receipt. `qualification_pass` remains false.

`MixedFeatureProbe BOOTSTRAP seed-reject|reject|read|finalize EXPECTED_METADATA TOPIC` supplies released-codec three-node assertions for separately provisioned isolated rolling fixtures. `seed-reject` creates a fresh RF3 topic and exact record; subsequent stages read it. `reject` expects metadata33 to fail without changing the current level; `finalize` expects explicit33 success once all required nodes support it. `local` directly checks the supplied listener's finalized metadata, bypassing Admin controller routing; `controller` reports the active controller. The mixed receipt records actual old-broker intersection rejection, rolling replacement, explicit update, restart and rejected configured rollback; unlike `kafka44-upgrade`, provisioning for this lane is retained separately, not claimed as one-command CI orchestration.

The negative topology wire fixture explicitly resolves `kafka44-negative` coordinator ownership first. Fresh brokers may not yet have initialized their offsets partition: codes14/15/16 are retried with retained `PRECONDITION`/`RETRY` diagnostics (40attempts,250ms). Terminal errors are not normalized. This prevents startup ownership timing from being mislabeled as missing-group semantic parity; the original failed final comparison remains in the final integrated receipt.

### Pending transactional offsets across an abrupt restart

The existing `--transaction-scenarios --protocol-scenarios --oracle-crash-recovery`
lane now also seeds four **unfinished** transactions using the pinned released
v5/v6 codecs before killing its owned oracle. Each has a baseline committed offset
7 and a pending offset 42. After the unchanged container/data restart, the fixture
requires the same topic UUID, offset 7 while pending, successful resolution with
the original producer ID/epoch, and stable offset 42 on commit or 7 on abort.
The four cases are modern/legacy crossed with commit/abort. Missing seed identity,
missing assertion rows, failed SIGKILL, failed restart, or any fixture failure
fails the collector. This lane runs in the existing crash-recovery CI variant.

`PendingTransactionRecovery.java` can exercise caller-owned isolated candidates
without granting the collector permission to restart an arbitrary endpoint:
compile it together with `ProtocolScenarios.java` and `TransactionScenarios.java`
using the exported pinned oracle jars. Run
`PendingTransactionRecovery seed ENDPOINT modern|legacy`; retain the single
`PENDING` row's six fields (topic, group, UUID, transactional ID, producer ID,
producer epoch). Abruptly stop **only the broker process/container provisioned by
that test**, restart its exact binary/config/data, then run
`PendingTransactionRecovery resolve ENDPOINT TOPIC GROUP UUID TXN PID EPOCH commit|abort`.
Do not reinitialize the producer between seed and resolution. The pending
transaction timeout is five minutes; resolving an expired transaction is not the
same scenario and must fail. Source/config/binary hashes and owned lifecycle
commands belong in the execution receipt, not inferred from an endpoint.

These are single-node RF1 process-crash tests, not power-loss/fsync durability,
pending transaction marker failure, or coordinator/partition-leader movement.
Default production gates support the legacy fixture only; the v6 fixture requires
explicit API/feature enabling. No result graduates defaults or sets a full
qualification pass.

### Caller-owned multi-broker pending-offset failover

For a fresh three-broker cluster with offsets and transaction-state replication3,
minimum ISR2, run `PendingTransactionRecovery seed ENDPOINT modern 3` (or
`legacy 3`). The optional final argument sets the test topic replication factor;
it does not configure internal-topic replication, which must be set before the
cluster starts. The fixture prints `OWNERS` with group/offsets coordinator then
transaction coordinator endpoints, followed by the existing six-field `PENDING`
identity. Retain both rows.

SIGKILL just the task-owned broker matching the selected `OWNERS` endpoint. Keep
the other two nodes alive; resolve via a surviving bootstrap endpoint using the
original identity and `commit` or `abort`. The resolution prints the current
`OWNERS` too: require the selected owner to differ, rather than assuming the kill
caused an election. All UUID and exact pending/committed offset assertions still
apply. Restore the killed node with identical data/configuration, wait for
replication to recover, and use a fresh seed identity for every case. Run both
versions, both decisions, and each coordinator target separately on the immutable
released oracle and the exact candidate executable. Shared ownership can make
one kill move both roles; record that fact instead of claiming isolated roles.

`InitProducerId` alone permits a bounded retry of `CONCURRENT_TRANSACTIONS` after
the baseline commit, because marker completion can lag stable offset visibility.
The fixture retains every wire result; it does not retry that error for offset
commits or transaction resolution, nor normalize a terminal error into success.
This caller-provisioned lane is not automatically run by the single-node oracle
CI task, and process kills do not emulate power loss or a deterministic mid-marker
cut.

## Repeatable released-client DLQ boundaries

`aspect kafka44 --protocol-scenarios --dlq-scenarios --revision <full-sha> --output <fresh-directory>` reuses `qualification/kafka44-dlq/ShareBoundary.java` (not a second implementation). Eight explicit assertions check copied/headers-only destination size boundaries, intact payloads and six provenance headers. Eight recovery assertions verify zero share lag after restarting only the task-owned oracle. Duplicate, missing or misnamed scenario rows fail collection. The optional explicitly enabled candidate executes the same boundary fixture; candidate restart remains caller-owned. Raw source start offsets retain the released/native terminal-prefix difference. This is not authorization, minimum-feature, denied-destination or multibroker-fault coverage, and never changes `qualification_pass=false`.

### Isolated Streams backend faults and persistent restart

`StreamsFault.java` checks thirteen strict set/get/delete failure-and-repair
outcomes on actual brokers. `FaultTopologyPlugin.java` is a **test-only** released
Java plugin: flag files `/evidence/{set,get,delete}` select `ok`, `transient` or
`permanent`; descriptions persist as typed JSON under `/evidence/store`. Its
constrained deserialization permits only the released Streams records and Java
collections. This is an external-filesystem test plugin, not a production storage
implementation or an fsync/power-loss guarantee.

The native embedding example `streams_fault_broker` installs the production
`FileTopologyDescriptionBackend` behind the same injected-failure wrapper. Build
with `cargo build -p krabka-broker --example streams_fault_broker`, then launch
`target/debug/examples/streams_fault_broker DATA FLAGS 127.0.0.1:64664`.
DATA/FLAGS must be fresh private fixture directories. Controller uses the next
port. Stop only this owned process with SIGINT; launch the same command with
`rejoin` appended to reopen the exact data and filesystem backend.

Prepare a pinned released Kafka4.4 oracle using the same settings as the positive
Streams lane, `streams.version=1`, and
`group.streams.topology.description.plugin.class=FaultTopologyPlugin`. Compile the
plugin against its exported jars; add compiled classes and those jars to the
oracle CLASSPATH, and mount a fresh writable private directory at `/evidence`.
The native SPI is not JVM class loading and this lane makes no such claim.

On **caller-owned isolated fixtures only**, run:

```
aspect kafka44-streams-storage --oracle-container OWNED_PINNED_CONTAINER \
  --oracle-endpoint 127.0.0.1:64662 --oracle-flags /absolute/oracle/evidence \
  --native-endpoint 127.0.0.1:64664 --native-flags /absolute/native/flags \
  --libs /absolute/pinned/released/libs --output /fresh/private/fault-receipt
```

Repeat with `--mode prepare` and a fresh output directory, cleanly stop/reopen
**both exact owned data directories**, then repeat with `--mode recover` and a
third fresh output directory. Prepare retains unique group/topic identities in
the flag directories; recover requires STORED and the exact source topic after
restart. The task never stops arbitrary candidates. Pin native source revision,
executable digest, oracle configuration and lifecycle commands alongside receipts.
Missing/error/duplicate scenario rows fail collection. RF1 clean restart is not
coordinator movement, abrupt-crash or shared multi-node durability qualification.
### Delayed-copy process recovery and remote reads

`RemoteRecoveryProbe` is a split-process released-client fixture. Compile it with
`CopyProbe.java` and the pinned Kafka 4.4 libraries. On a freshly provisioned,
task-owned remote-storage broker, run:

```
java -cp "$CLASSES:$KAFKA_LIBS/*" RemoteRecoveryProbe seed "$BOOTSTRAP" "$TOPIC" "$ARCHIVE"
```

This acknowledges and checks offsets 0..47 with deterministic 64KiB payloads,
requires no current-topic archive yet, and uses 30s copy lag / 35s local retention.
SIGKILL and restart the **owned** broker with exactly the same data/configuration.
Poll for the physical disappearance of its partition's offset-zero `.log` and
current-topic archive creation; do not remove local files manually. Then run:

```
java -cp "$CLASSES:$KAFKA_LIBS/*" RemoteRecoveryProbe read "$BOOTSTRAP" "$TOPIC" "$LOCAL_OFFSET_ZERO_LOG" "$ARCHIVE"
```

The reader requires that eviction witness, reads offset zero without reset, and
checks every offset, key and payload byte across all 48 acknowledged records.
Set the broker local-retention sweep and remote-manager intervals low for this
bounded fixture (1s and 500ms respectively). `RecordingStorage` now reports absent
optional indexes with the SPI's `RemoteResourceNotFoundException`; a generic
storage failure incorrectly makes the released remote reader retry indefinitely.
These checks cover restart while copying is **delayed**, not interruption inside
an upload, leader movement, power-loss durability or production storage backends.

### Inside-copy interruption

See [COPY-INTERRUPTION.md](COPY-INTERRUPTION.md) for process-scoped native and
released payload-close gates, actual STARTED-only metadata witnesses, owned
SIGKILL/reopen steps and exact post-eviction reads. The retained receipt records
a released4.4/retained-native8eab7ff8 paired recovery pass, plus the incomplete
earlier baseline and temporary startup readiness warnings. The gate is inactive without explicit owned fixture configuration.
