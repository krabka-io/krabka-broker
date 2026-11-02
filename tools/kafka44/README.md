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

Candidate tools run with host networking; advertised broker addresses must be reachable there. Bootstrap addresses must identify the prepared candidates, not production. The output directory must not already exist. Review output for private endpoint/configuration information before attaching it publicly. Client properties/authentication are not yet supported; authorization is explicitly unqualified.

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
