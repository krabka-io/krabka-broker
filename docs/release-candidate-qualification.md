# Release candidate qualification

This record is the contract for the next promoted release candidate. It gives
the procedure that freezes a candidate, the gates that must pass on that exact
candidate, and the proposed list of release blockers. Issue
[#1401](https://github.com/krabka-io/krabka-broker/issues/1401) owns it.

This record does not authorize a release. It does not tag, sign, publish or
promote anything. [Releasing](releasing.md) is the release procedure, and it
stays unchanged: a candidate that passes every gate here is released by that
procedure, from a `vX.Y.Z` tag on `main`.

**Status, 2026-10-18.** No candidate is frozen. `main` is at `045b10d4`. The
latest `main` commit with a green required CI run is `948e3cc8`. The latest
release is v1.0.5 (`ac9deff3`). Several gates have no current evidence, and
some gates are red. The blocker list below is a **proposal**. The maintainer
agrees the final list on #1401.

## Release and ecosystem claims

Krabka makes two separate claims, as [Releasing](releasing.md) and
[Ecosystem qualification](qualification.md) say:

- A **broker release** is a signed image and a GitHub release for a commit
  whose `main` CI run passed. v1.0.0 to v1.0.5 are broker releases.
- An **ecosystem-qualified stack** is a broker release whose frozen candidate
  passed every gate in [Candidate gates](#candidate-gates), with the sibling
  set that the candidate manifest names.

Describe a stack as ecosystem-qualified only when every gate below passed on
the frozen candidate and every agreed release blocker is closed. A partial
pass, an older run, or a run on another revision is not enough. On
2026-10-18 no 1.x release is ecosystem-qualified. The last qualified stack is
krabka-broker v0.6.1 with the sibling set in
[`ecosystem-eight-gate-baseline.json`](../qualification/ecosystem-eight-gate-baseline.json).

## Freeze a candidate

A candidate is three things that must not move while it is qualified:

1. The krabka-broker commit. It is on `main`, and its `ci.yml` push run
   passed `gate`.
2. Every sibling revision that the commit's `[patch.crates-io]` block pins.
   These revisions are compiled into the image.
3. The amd64 image digest that `delivery` recorded for the commit, in the
   `delivery-image` artifact of that run. The artifact expires after 90 days.

Do these steps in sequence:

1. Pick the commit. Its `ci.yml` push run must be `success`. Get that run's
   `delivery-image` artifact:

   ```sh
   run="$(gh api "repos/krabka-io/krabka-broker/actions/workflows/ci.yml/runs?head_sha=${COMMIT}&event=push" \
     --jq '[.workflow_runs[] | select(.conclusion == "success")][0].id')"
   artifact="$(gh api "repos/krabka-io/krabka-broker/actions/runs/${run}/artifacts" \
     --jq '.artifacts[] | select(.name == "delivery-image") | .id')"
   gh api "repos/krabka-io/krabka-broker/actions/artifacts/${artifact}/zip" >delivery-image.zip
   unzip -o delivery-image.zip
   ```

2. Freeze the commit, the pins and the digest. The task checks the digest
   against the registry, and lists each component of the manifest whose
   revision is not the pin that the image compiles in:

   ```sh
   aspect freeze-candidate --commit "${COMMIT}" \
     --delivery-digest delivery-image.digest \
     --manifest qualification/ecosystem-eight-gate-baseline.json \
     --output candidate-freeze.json
   ```

3. When the task prints a `DRIFT` line, write a reviewed candidate manifest.
   Start it with `aspect check-qualification --new-candidate`, and set every
   component to its pinned revision and to the artifacts built from that
   revision. The workflow's `broker_revision` and `broker_digest` overrides
   replace the broker component only. With drift, they describe siblings that
   the image was not built from.
4. Commit the candidate manifest and the freeze record under `qualification/`
   in one pull request. From that merge on, every gate below runs against
   these values and no others.
5. Rebuild nothing. A fix to a blocker makes a new commit, and a new commit
   is a new candidate. Start again at step 1, and run every gate again.

The dry run of these steps on `948e3cc8` is in
[`candidate-freeze-948e3cc8-dry-run.json`](../qualification/candidate-freeze-948e3cc8-dry-run.json).
It is not a candidate. It was written by the schema 1 task, which compared
every `[patch.crates-io]` pin with the manifest, so it lists all ten pinned
siblings as drift. Seven of them, `krabka-gateway` among them, are patches that
nothing in the dependency graph uses: `Cargo.lock` keeps them under
`[[patch.unused]]`, and the image does not contain them. The task now records
them as `unused_patches` and compares only the siblings that the image compiles
in. Those are `krabka-protocol`, `krabka-client-rs` and
`krabka-schema-registry`, and all three still differ from the qualified
baseline manifest. So the next qualification needs a reviewed manifest that
names those three revisions; the broker overrides alone are not correct.

## Candidate gates

Each gate must pass on the frozen candidate. "Needs" names what the run needs
that a pull-request runner does not have. The status column is the state on
`main` on 2026-10-18, and it is not candidate evidence.

| Gate | Workflow and job, or command | Evidence | Status on `main`, 2026-10-18 | Needs |
| :--- | :--- | :--- | :--- | :--- |
| Required CI | `ci.yml` job `gate`, on the candidate's push run. It needs `cpd`, `ci`, `coverage`, `supply-chain`, `cargo`, `wasm`, `checks`, `proofs`, `select`, `docker` and `kubernetes`. | The run, and its `delivery-image` artifact | Green on `948e3cc8`, [run 39058863421](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421). Red on `f2305998`, [run 39053924323](https://github.com/krabka-io/krabka-broker/actions/runs/39053924323): a nextest timeout. `045b10d4` is in progress, and two container groups failed to pull images on the remote executor. | Nothing extra |
| Creusot proofs | `ci.yml` job `proofs` | The job log | Green on `045b10d4`, [run 39165644154](https://github.com/krabka-io/krabka-broker/actions/runs/39165644154) | Docker on the runner |
| Stateright models | `ci.yml` job `ci`, the model targets of `bazel test //...`, and `kraft_model_test` and `bucket_model_test` | The job log | Green on `948e3cc8`, run 39058863421 | Nothing extra |
| Full container matrix, with the Kafka 4.3.1 JVM acceptance suites | `ci.yml` job `docker` on a `workflow_dispatch` of the candidate commit. A dispatch runs every suite (`aspect container-suites` with no `--base`). The `apache_kafka_4_3_1` suites include `jvm_acceptance_cli`, `jvm_acceptance_durability`, `jvm_acceptance_reassign`, `jvm_acceptance_sasl` and `jvm_bootstrap_controller`. | The run | Red. Nightly [run 39114726914](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914) on `948e3cc8`: `jvm_acceptance_legacy+6`, `librdkafka_conformance+6` and `minio_roundtrip` failed to build, because image pulls on the remote executor were cancelled. The other groups passed. Issue [#1493](https://github.com/krabka-io/krabka-broker/issues/1493). | Docker, remote executor |
| GSSAPI | `ci.yml` job `gssapi`, on the same dispatch | The run | Green in run 39114726914 | Docker Compose |
| Upgrade from the previous release | `ci.yml` job `upgrade`, on the same dispatch | The run | Red. Cancelled at its 90-minute limit on 2026-10-16, 10-17 and 10-18. It stops after `Records delete operation completed`. Before that it failed on 2026-10-13 and 10-14. | Docker, GHCR read |
| Four-hour soak | `ci.yml` job `soak`, on the same dispatch | The job log | Red. `a_three_broker_cluster_holds_its_resources_over_a_soak` failed on 2026-10-16, 10-17 and 10-18. On all three brokers the log directory rose by about 4.89 MB over the second half, against an allowance of 4,194,304 bytes. | Docker |
| Criterion regression lane | `ci.yml` job `bench`, on the same dispatch | The `benchmark-regression-evidence` artifact | Green in run 39114726914 | Nothing extra |
| Regression budgets (#1435) | `packaging/performance/qualify.sh budget before_after <reference>` and `qualify.sh budget pinned_kafka`, on the benchmark host. `benchmarks.yml` job `performance-budget-contract` validates the contract only. | The results record that [Performance regression budgets](operations/performance-budgets.md) defines | Not run. The contract job is green on `045b10d4`, [run 39165644181](https://github.com/krabka-io/krabka-broker/actions/runs/39165644181). No measured budget run exists, and the budgets are not calibrated ([#1395](https://github.com/krabka-io/krabka-broker/issues/1395)). | Benchmark host, Kind, Docker |
| 72-hour soak (#1393) | The `soak_contract` suite with `KRABKA_SOAK_CONTRACT=bench/soak/rf3-72h.yaml`, then `krabka-soak-check judge`. [Performance qualification](operations/performance-qualification.md#sustained-load-contract) gives the commands. | `evidence.json` and `verdict.json` | Not run. The five-minute self-check is green on `948e3cc8`. The 72-hour run has never run ([#1393](https://github.com/krabka-io/krabka-broker/issues/1393)). | A 16 CPU, 64 GiB, 500 GiB host for three days |
| Ecosystem qualification, eight gates | `qualification.yml` with `execute=true` and the reviewed candidate manifest. The gates are `installation`, `operator-lifecycle`, `cli-admin`, `observability-recovery`, `disaster-recovery`, `schema-evolution`, `registry-migration` and `snapshot-retention`. | A `qualification-<run>` prerelease, its `manifest.json`, `report.md` and `SHA256SUMS` | Not run on any 1.x commit. The last run of all eight gates was [run 36293960620](https://github.com/krabka-io/krabka-broker/actions/runs/36293960620) on 2026-09-27, for the v0.6.1 baseline. | Docker, Kind, Helm |
| Recovery (#1397) | The `disaster-recovery` gate above, plus the declared RPO and RTO, classic and diskless, TLS, SASL and ACL cases that [#1397](https://github.com/krabka-io/krabka-broker/issues/1397) plans | The gate bundle, and the #1397 record | Planned. #1397 has no evidence yet. | Docker, MinIO |
| Lifecycle (#1396) | The `operator-lifecycle` gate and the `upgrade` job above, plus the loaded rolling upgrade, certificate rotation and reassignment on published digests that [#1396](https://github.com/krabka-io/krabka-broker/issues/1396) plans | The gate bundle, and the #1396 record | Planned. #1396 has no evidence yet. | Kind, published images |
| Operator acceptance (#1399) | Independent operators run the migration and recovery plans of [#1398](https://github.com/krabka-io/krabka-broker/issues/1398) without maintainer help | The operator reports that [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) names | Planned. No operator is named and no evidence exists. | Pilot access, real hosts |
| Support documents (#1400) | The support matrix, measured envelopes and known limitations that [#1400](https://github.com/krabka-io/krabka-broker/issues/1400) plans, at the candidate commit | The documents at the tagged commit, pinned by `aspect verify-release` | Drafted in the [support matrix](operations/support-matrix.md) and the [measured envelope](operations/capacity.md#measured-envelope). No row is `candidate` yet, because no candidate is frozen. | Nothing extra |
| Provenance | `aspect verify-release --tag <tag> --delivery-digest delivery-image.digest`, after the release | `release-verification.json` | Red for every 1.x release. See [Provenance verification](#provenance-verification). | cosign 3, Docker CLI, `gh` |

Rules for the gates:

- Run every gate on the frozen commit and image digest. A gate that ran on
  another commit gives no evidence for the candidate.
- Keep every failed run. A failure is a blocker until a later run on a new
  candidate passes, or the maintainer agrees that the item is a known
  limitation.
- Re-run a failed job once only to show an infrastructure failure, such as
  the image pull failures of run 39114726914. A second failure is a real
  failure.
- Do not skip, disable or ignore a test to make a gate pass.

## Provenance verification

`release.yml` does these steps for a `vX.Y.Z` tag:

1. It gets the amd64 digest from the `delivery-image` artifact of the
   successful `main` push run, and checks it against the registry with
   `aspect check-delivery-image`.
2. It builds and pushes the arm64 manifest, and joins both children by
   digest into the `vX.Y.Z` index.
3. It signs the index with `cosign sign`, and attests the CycloneDX SBOM with
   `cosign attest --type cyclonedx` and the provenance with
   `cosign attest --type slsaprovenance`. The identity is the keyless GitHub
   OIDC identity of `release.yml` at the ref that ran it.
4. It runs `cosign verify` and `cosign verify-attestation` against the index,
   and uploads `sbom.cdx.json` to the GitHub release.

`aspect verify-release` repeats these checks from outside, against a release
that exists. It also checks the content of the attestations, and pins the
support documents at the tagged commit:

```sh
git fetch origin tag v1.0.5
aspect verify-release --tag v1.0.5 \
  --delivery-digest delivery-image.digest \
  --output release-verification
# A release that a workflow_dispatch from main re-ran:
aspect verify-release --tag v1.0.0 --identity-ref refs/heads/main
# The support documents must be the bytes an earlier record pinned:
aspect verify-release --tag vX.Y.Z --expected-support candidate/release-verification.json
```

The task fails when any of these is false:

- The index is signed by `release.yml` under the identity of the ref that ran
  it.
- The index has one amd64 child and one arm64 child.
- The provenance statement is SLSA v1, its subject is the index, and its
  predicate names the tagged commit, the tag ref, the arm64 child, and the
  amd64 child as the tested delivery image.
- The SBOM attestation's subject is the index, and its predicate is the
  release's `sbom.cdx.json` asset, compared as JSON.
- The amd64 child is the digest of the `delivery-image` artifact, when
  `--delivery-digest` is given.
- Each support document exists at the tagged commit, and matches the record
  that `--expected-support` names, when one is given.

The support documents are `README.md`, `CHANGELOG.md`, `SECURITY.md`,
`docs/persisted_formats.md`, `docs/format-divergences.md`,
`docs/KIP_MATRIX.md`, `docs/config-reference.md`, `docs/releasing.md`,
`docs/qualification.md`, and every file under `docs/operations/`. The record
gives the SHA-256 of each one. A page added under `docs/operations/` later,
such as the #1400 support matrix, is pinned with no change to the task. To
bind the documents to the delivered bytes, keep the candidate's record, and
check the release against it with `--expected-support`. Because the tag
names the commit and the provenance names the same commit and the delivered
digests, a pass ties the documents to the image bytes.

Use cosign 3. cosign 2 finds no signature on these releases, because
`sigstore/cosign-installer` v4 signs with cosign 3, which stores the
signature as an OCI referrer.

### Result for v1.0.5

The task ran on 2026-10-18 against v1.0.5 and v1.0.0. The record for v1.0.5
is [`release-verification-v1.0.5.json`](../qualification/release-verification-v1.0.5.json).

| Check | v1.0.5 |
| :--- | :--- |
| Index | `sha256:2a81d1ba38de4e8ffb032fedf67952845185158a7607ac76ba38fa162dc37e50` at commit `ac9deff3` |
| Children | amd64 `sha256:8c515d33…`, arm64 `sha256:303ced00…` |
| Signature, `release.yml@refs/tags/v1.0.5` | Pass |
| Signature under another identity (`refs/heads/main`) | Fails, as it must |
| amd64 child against the `delivery-image` artifact of [run 38516831673](https://github.com/krabka-io/krabka-broker/actions/runs/38516831673) | Pass |
| SBOM attestation against `sbom.cdx.json` (`sha256:c123ba49…`) | Pass |
| Provenance | **Fail.** Attested as `https://slsa.dev/provenance/v0.2` with an empty predicate (`{}`) |
| Support documents | 58 documents pinned |

v1.0.0, which a dispatch from `main` released, gives the same result under
`--identity-ref refs/heads/main`.

The provenance defect is in `release.yml`. It writes a SLSA v1 predicate
(`buildDefinition` and `runDetails`) but attests it with
`--type slsaprovenance`. cosign parses that type into the SLSA v0.2 schema, so
every field is dropped, and the statement carries no commit, no ref and no
image source. `cosign verify-attestation --type slsaprovenance` still passes,
because it checks the signature and the type, not the content. A local
`cosign attest-blob` test with cosign 3.0.2 shows that the type
`https://slsa.dev/provenance/v1` keeps the predicate as written. The fix is
blocker B1 below, in
[#1509](https://github.com/krabka-io/krabka-broker/pull/1509). It does not change the attestations of v1.0.0 to
v1.0.5, so those releases keep an empty provenance predicate. For them, the release run log and the
`delivery-image` artifact are the provenance evidence.

## Blocker list (proposal)

The maintainer agrees this list on #1401. Each item has one class:

- **Release blocker**: the candidate cannot be promoted, or described as
  ecosystem-qualified, until the item is fixed and the fix is verified on a
  new candidate.
- **Known limitation**: the release can ship with it, and the support
  documents (#1400) must state it.
- **Not relevant**: the item does not affect the candidate.

### Release blockers

| ID | Item | Source | Reason |
| :--- | :--- | :--- | :--- |
| B1 | The provenance attestation has an empty predicate. | This record; `release.yml` | The provenance is the link between the signed index, the commit and the tested image. Fix in [#1509](https://github.com/krabka-io/krabka-broker/pull/1509): attest with `--type https://slsa.dev/provenance/v1`, and fail the release unless the statement read back names the commit, the ref and both children. Verified when the next release passes `aspect verify-release`. |
| B2 | The four-hour soak fails: log-directory growth is over its allowance on all three brokers. | `ci.yml` `soak`, 2026-10-06 to 10-18; [#1507](https://github.com/krabka-io/krabka-broker/issues/1507) | A resource that rises without bound under steady load is the failure this lane exists to find. The growth was `soak-verify`, the offsets topic and the metadata log, which Kafka's defaults do not reclaim within four hours, and the soak now rolls the last two every minute. Rolling them exposed two leaks: compaction left the producer snapshots of the segments it merged, and the metadata log never deleted its retired segments. Fixed in [#1537](https://github.com/krabka-io/krabka-broker/pull/1537), merged 2026-10-19. The four-hour soak passed on its head `2cf5551f` ([run 39288583006](https://github.com/krabka-io/krabka-broker/actions/runs/39288583006), 14,412 s). It closes when the soak passes on the candidate. |
| B3 | The upgrade lane stops at its timeout after `DeleteRecords`. | `ci.yml` `upgrade`, 2026-10-16 to 10-18; [#1301](https://github.com/krabka-io/krabka-broker/issues/1301), [#1389](https://github.com/krabka-io/krabka-broker/issues/1389), [#1508](https://github.com/krabka-io/krabka-broker/issues/1508) | The upgrade from the previous release is a lifecycle gate, and the #1301 confirmation that #1389 needs had never passed on `main`. The hang was in `packaging/upgrade-smoke.sh`, not in the broker. The script read changelog prose about the on-disk format as a declared format break. It then ran HEAD in the foreground on a directory that HEAD opens normally, so HEAD served until the job limit. Fixed in [#1527](https://github.com/krabka-io/krabka-broker/pull/1527), merged 2026-10-19: only the `**Breaking, on-disk format.**` marker counts, and every step after the HEAD image build has a time limit. The build itself is bounded only by the job's 90 minutes. The lane passed on `main` in the nightly [run 39237543947](https://github.com/krabka-io/krabka-broker/actions/runs/39237543947) at `e0588325`, and in the dispatch [run 39288583006](https://github.com/krabka-io/krabka-broker/actions/runs/39288583006). Both upgraded v1.0.5's volume, read the record back with HEAD and found the raised log start. It closes when the lane passes on the candidate. |
| B4 | The full container matrix was not green on the latest commits. | [#1493](https://github.com/krabka-io/krabka-broker/issues/1493), run 39114726914 | Three groups failed on image pulls, so their Kafka 4.3.1 and librdkafka suites have no result. No container test failed in the eleven nightlies from 2026-10-08 to 10-19, and every red group was infrastructure. The remote executor's Docker Hub token for `ubuntu:22.04` timed out on 10-18 and in dispatch 39177946645, which [#1530](https://github.com/krabka-io/krabka-broker/pull/1530) fixed: the image now comes from `mirror.gcr.io`, and an action that cannot start remotely runs locally. `mirror.gcr.io` was missing a blob of `apache/kafka:4.3.1` on 10-10, and [#1358](https://github.com/krabka-io/krabka-broker/pull/1358) added the Docker Hub fallback for it. GitHub answered HTTP 500 for `aspect-launcher` on 10-12, and BuildBuddy was unavailable for `gssapi` on 10-19. All 13 groups passed in the nightlies of 10-13, 10-14, 10-16 and 10-17, and in [dispatch 39288583006](https://github.com/krabka-io/krabka-broker/actions/runs/39288583006) at `2cf5551f`, which has #1530. It closes when one full-matrix run is green on the candidate. |
| B5 | Ecosystem qualification has not run on a 1.x commit, and the baseline manifest does not match the pinned siblings. | [Freeze a candidate](#freeze-a-candidate) | The stack cannot be ecosystem-qualified without a run of all eight gates on a manifest that names what the image compiles in. The image compiles in three siblings, not ten: the other seven pins are unused patches, and `aspect freeze-candidate` now leaves them out of the drift. The three are `krabka-protocol` at `af4dc00a`, `krabka-client-rs` at `7f3c17b2` and `krabka-schema-registry`, against `05e3e32d`, `c4c113c7` and `904490ad` in the baseline. The schema-registry pin `565e5ed9` was a merge commit on a release branch, not on that repository's `main`, so the workflow's default-branch check would refuse it. It is now `7e5115f7` on `main`, where `crates/schema-serde` is unchanged. None of the three ships an artifact, so the reviewed manifest changes three revisions and rebuilds nothing. It closes when `qualification.yml` passes all eight gates on the frozen candidate with that manifest. |
| B6 | Fetch in the tiered remote band hangs, and the remote log metadata manager stays `NotReady`. | [#1448](https://github.com/krabka-io/krabka-broker/issues/1448), reproduced on v1.0.5; [#1542](https://github.com/krabka-io/krabka-broker/issues/1542) | A Fetch with no response is a correctness defect, and the 72-hour tiered path needs its fix. The hang is fixed in [#1538](https://github.com/krabka-io/krabka-broker/pull/1538). The remote read now serves the archived segment's own bytes, as Kafka's `RemoteLogManager.read` does, so a compressed batch is no longer framed by its uncompressed length. A Fetch while the RLMM catches up now answers the retriable `REPLICA_NOT_AVAILABLE`. The `NotReady` stall did not reproduce in 11 Docker soak runs with the real follower pause, three of them on the #1448 executor. #1542 is closed, to be reopened if `KrabkaRlmmPartitionNotReady` fires. [#1555](https://github.com/krabka-io/krabka-broker/pull/1555), merged 2026-10-20, fixes the one hazard found: a slow metadata publish held the lock the RLMM consumer needs, so it stalled catch-up on every metadata partition. #1555 also exports per-partition RLMM readiness (`krabka_broker_tiered_storage_rlmm_partition_{ready,target_offset,applied_offset}`) and adds the `KrabkaRlmmPartitionNotReady` alert, so a recurrence is visible. Verified when the soak's tiered path, with its follower pause, reads back every record on the candidate and no RLMM stays `NotReady`. |
| B7 | The 72-hour soak has never run. | [#1551](https://github.com/krabka-io/krabka-broker/issues/1551), [#1393](https://github.com/krabka-io/krabka-broker/issues/1393), PR #1473 | Sustained correctness under faults is a milestone 26 criterion. The harness is merged (#1473, and the broker fixes its trial runs found: #1485, #1487, #1491). The two fixes #1551 waited on are merged: #1553, a restarted controller keeping its leaderships (#1442), and #1555, the RLMM pump stall. A host that meets the contract's 16 CPU, 64 GiB and 500 GiB is available, and #1551 holds the run plan. No GitHub-hosted runner can take the run: a hosted job stops at six hours. The `netem` slow-follower fault had never run, because no kernel the repository tested on had `sch_netem`; this sandbox's kernel is built without it (`CONFIG_NET_SCH_NETEM` is not set). The nightly `netem` job now runs `bench/soak/self-check-netem.yaml`, the self-check with that fault, on a GitHub-hosted runner after loading the module, so the fault is proven before the long run depends on it. The run host must also load `sch_netem`, or the first slow-follower fault, at 65 minutes, fails the run. It closes when `bench/soak/rf3-72h.yaml` runs for 72 hours on the candidate and `krabka-soak-check judge` passes its evidence. |
| B8 | The regression budgets have no measured run, and are not calibrated. | [#1395](https://github.com/krabka-io/krabka-broker/issues/1395), PR #1435 | #1401 asks to repeat performance qualification on the candidate. The contract, the evaluator and `qualify.sh budget` are merged, but no part of the harness's cluster path has run. A review of it against the manifests, the image tag and the workload's arguments found nothing a run would trip on; it still needs its first execution. It needs a host with at least 16 CPUs and 32 GiB, Kind, Docker and Bazel. No GitHub-hosted runner qualifies, and this sandbox has 4 CPUs and no access to Bazel's GitHub downloads. The 72-hour soak host meets it. Run the budgets after the soak, not beside it: the soak's load would push the cells past their variance budgets. The timeouts bound one mode at about 20 hours; at earlier throughputs a mode takes a few hours. The first run calibrates: a cell whose spread exceeds its variance budget needs a contract change in a reviewed pull request, and then a new run, before anything is published. It closes when `qualify.sh budget before_after v1.0.5` and `qualify.sh budget pinned_kafka` both pass on the candidate and their records and verdicts are published. |
| B9 | The OMB max-rate RF3 rerun for `COORDINATOR_NOT_AVAILABLE` has not run. | [#1352](https://github.com/krabka-io/krabka-broker/issues/1352), [#1389](https://github.com/krabka-io/krabka-broker/issues/1389), [#1394](https://github.com/krabka-io/krabka-broker/issues/1394) | #1389 lists it as a release blocker. #1352 found an `OffsetCommit` answered `COORDINATOR_NOT_AVAILABLE` twice in `omb-local-2026-10-11` on v1.0.1, where Kafka 4.3.1 had none: the coordinator's 5 s `__consumer_offsets` append timed out with one in-sync follower a record short. Two causes found since are fixed on `main`: a busy partition starved a follower fetch session ([#1410](https://github.com/krabka-io/krabka-broker/pull/1410)), and retired segment files were unlinked under the log lock, stalling appends and follower reads for up to 0.7 s ([#1457](https://github.com/krabka-io/krabka-broker/pull/1457)). The 5 s timeout matches Kafka's `offsets.commit.timeout.ms`, and Kafka also answers a timed-out offsets append with `COORDINATOR_NOT_AVAILABLE`, so the remaining question is whether a follower still stalls that long; ext4 journal stalls on the run host (#1333) cannot be reproduced elsewhere. The rerun needs that host, an AMD EPYC 4344P with three 4 CPU / 10 GiB brokers, and the local Docker OMB setup the run was published from in krabka-io.github.io#109. `bench/omb` cannot stand in: it runs krabka at RF=1 only. It closes when the two `max-rate-1-topic-1-partition` RF3 trials run on the candidate with no failed commits, or with each failure explained by the replicator and fetch debug logs and matched by Kafka in the same run. |
| B10 | Recovery, lifecycle and operator acceptance have no evidence. | [#1397](https://github.com/krabka-io/krabka-broker/issues/1397), [#1396](https://github.com/krabka-io/krabka-broker/issues/1396), [#1398](https://github.com/krabka-io/krabka-broker/issues/1398), [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) | Each is a candidate gate. All four issues are planning only. |
| B11 | The support matrix and the known limitations are not published. | [#1400](https://github.com/krabka-io/krabka-broker/issues/1400) | #1401 depends on it, and `aspect verify-release` pins it. |
| F1 | Flake: `txn_secured_marker_fanout` loses its metadata log directory. | `main` `0d435874`, [job 116614668765](https://github.com/krabka-io/krabka-broker/actions/runs/38790841370/job/116614668765) | `a_remote_coordinator_verifies_a_produce_for_a_broker_with_only_cluster_action` panics after `the metadata log directory /tmp/.tmp… has failed: No such file or directory`. The log shows that the temporary directory was gone while the controller still used it. It can turn `gate` red. No issue or fix exists. |
| F2 | Flake: `diskless_e2e` copies a voter log file that a compaction removed. | [PR #1452 comment](https://github.com/krabka-io/krabka-broker/pull/1452#issuecomment-6209024210) | `diskless_acks_all_append_survives_the_acking_broker` panics at `voter_dir.rs` (`copy a voter log file: NotFound`). The proposed fix is to tolerate `NotFound` in `copy_dir`. It is not on `main`. |
| F3 | Flake: `produce_gate` in the coverage job. | `main` `4fdf5f44`, [job 116715925029](https://github.com/krabka-io/krabka-broker/actions/runs/38818733024/job/116715925029) | `a_batch_larger_than_segment_bytes_is_refused_even_when_the_message_cap_allows_it` fails its check of the log end offset (`produce_gate.rs:34`). No issue or fix exists. |
| F4 | Flake: `tiered_storage_multi_broker::multi_earliest::a_follower_left_with_a_raised_floor_takes_its_leaders_back` times out at 600 s. | `main` `f2305998`, run 39053924323 | It made a `main` `gate` fail. |

F1 to F4 are blockers to resolve, not documented flakes, because each one is
in a lane that `gate` requires. A fix makes the test deterministic. It does not
skip the test or add a retry.

### Known limitations

| ID | Item | Source | Reason |
| :--- | :--- | :--- | :--- |
| L1 | v1.0.0 to v1.0.5 carry an empty provenance predicate. | [Result for v1.0.5](#result-for-v105) | A signed attestation cannot be changed after release. The signature, the SBOM and the delivery digest still verify. |
| L2 | SASL edge differences from Kafka 4.3.1: idle expiry does not count handler and mute time, PLAIN compares raw bytes, SCRAM `c=` must equal the GS2 header, a wrong-length proof gets error 58, and an expired delegation token is refused. | [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | Disclosed differences in rare error paths. Each one is stricter or more lenient, and none changes a successful authentication. |
| L3 | Java regex edge differences: the compiled-size limit, the message for a repetition with no argument, and surrogate escapes. | [#1343](https://github.com/krabka-io/krabka-broker/issues/1343), #1389 | #1389 classifies them as documented edge differences. |
| L4 | An override of `offsets.commit.timeout.ms` is not read, and `OffsetCommit` maps an append error where Kafka answers `NOT_COORDINATOR`. | [#1352](https://github.com/krabka-io/krabka-broker/issues/1352), #1388 | Minor and on error paths. The maintainer should decide whether to fix them before the candidate. |
| L5 | A SIGKILL of the active controller can leave a 15-second outage unrecovered in that window (3 of 10 trials). | PR #1452; [#1442](https://github.com/krabka-io/krabka-broker/issues/1442) | #1452 shows that the window, not the broker, decides the case. State the recovery time an operator can expect. |
| L6 | A dead broker that is never unregistered leaves an offline `__krabka_audit` partition. | PR #1485 | Expected, and visible in the alerts. Document the unregister step. |
| L7 | Evidence gaps: no live harness reads `read_committed` across local eviction, or fails over an RF=3 consumer group. #1388 proposed ten follow-up issues, and none is filed. | [#1388](https://github.com/krabka-io/krabka-broker/issues/1388) | The behavior is compared with Kafka's source, not run. The maintainer should file the ten issues and decide which, if any, are blockers. |
| L8 | Performance follow-ups: the gap to the page cache (#1370), O_DIRECT segments (#1333), and the indicative numbers of #1394. | [#1370](https://github.com/krabka-io/krabka-broker/issues/1370), [#1333](https://github.com/krabka-io/krabka-broker/issues/1333), [#1394](https://github.com/krabka-io/krabka-broker/issues/1394) | Optimizations, not defects. The measured envelope in #1400 states the current limits. |
| L9 | The 200 ms and 400 ms reproduction of #621 still fails. | [#621](https://github.com/krabka-io/krabka-broker/issues/621) | A limit of the harness, as #621 records, not a broker defect. |

### Not relevant

| Item | Source | Reason |
| :--- | :--- | :--- |
| #1344 is still open. | [#1344](https://github.com/krabka-io/krabka-broker/issues/1344) | Fixed by #1346 and pinned by #1404. #1389 says it is ready to close. |
| #1442 is still open. | [#1442](https://github.com/krabka-io/krabka-broker/issues/1442) | The same defect as #1450, which #1455 fixed. The maintainer should close it as a duplicate. The open part is L5. |
| Flakes fixed on `main`: `kraft_model_test` timeouts (#1460), `cli_smoke` port race (#1454), the `multi_misaligned` copy flake (#1479). | The pull requests | Fixed. They stay fixed only if F1 to F4 runs stay clean. |
| The stale branch `fix/controller-finalized-features-epoch`. | #1389 | No effect on the candidate. |
| The soak judge abandons a recovery at twice its deadline. | PR #1470 | Behavior of the test harness, not of the broker. |
