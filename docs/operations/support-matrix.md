# Release support matrix

This page says which clients, broker builds, security modes, storage modes and
deployments have evidence, and how current that evidence is. Every row links
the run, pull request or issue comment that holds the evidence, and names the
revision that the evidence is for. A row without current evidence says so. It
does not borrow evidence from another revision.

The page is the support-matrix half of
[#1400](https://github.com/krabka-io/krabka-broker/issues/1400). Candidate
qualification and the release-blocker list are
[#1401](https://github.com/krabka-io/krabka-broker/issues/1401). The KIP-level
claims are in the [KIP matrix](../KIP_MATRIX.md), and the tool classes that
cannot work are in [Ecosystem support](ecosystem-support.md).

## How to read this page

Candidate revision: `none`

No release candidate is frozen yet. [#1401](https://github.com/krabka-io/krabka-broker/issues/1401)
freezes it, with the procedure and gates in
[Release candidate qualification](../release-candidate-qualification.md). Until then, no row can be `candidate`, and every passing result
below is `historical`: it is true of the revision that it names, and it says
nothing about a later build.

| Status | Meaning |
| :--- | :--- |
| `candidate` | Passed on the frozen candidate revision above. |
| `historical` | Passed on an earlier revision, which the row names. Rerun it on the candidate before release. |
| `failing` | The most recent run that executed the check failed at the named revision. |
| `untested` | No executed evidence. The row links the issue that owns the gap. |
| `unsupported` | krabka does not support it. The row links the decision. |

`aspect check-support-matrix` reads this page. It fails when a row has another
status, when a row has no evidence link, when an executed row names no
revision, when an `untested` row links no issue, and when a row is
`candidate` but does not name the candidate revision. CI runs it in the
`checks` job.

### Reference runs

The rows cite these runs. A nightly container suite builds the broker from the
source at its revision, so the image digest names the delivered image of that
revision. It does not prove that the suite ran those bytes.
[#1401](https://github.com/krabka-io/krabka-broker/issues/1401) binds the
candidate evidence to the delivered bytes.

| Run | Revision | Delivered image | What it executed |
| :--- | :--- | :--- | :--- |
| [Nightly 39114726914](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914), 2026-10-18 | `948e3cc8f99dfabad5bd0917f7b710abc3a07710` | `sha256:ad1149a44f899e4d30c8eddd9944444e9fddcf8bb3f0ff322ff17f1e85ba4a23`, from [push CI 39058863421](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421/job/117476230788) | The full container matrix. Three groups failed before any test ran, because their image pulls were cancelled: `jvm_acceptance_legacy+6`, `librdkafka_conformance+6` and `minio_roundtrip` ([#1493](https://github.com/krabka-io/krabka-broker/issues/1493)). The four-hour soak failed. The upgrade job stopped at its time limit. |
| [Nightly 39019310161](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161), 2026-10-17 | `dc4acebe91b704dc1eed7dcce6e687638f70617d` | `sha256:c418a9bdd17611459c3b534d0743aa20c6a81e942b37a7c810c191ef9b922a2c`, from [push CI 39017848795](https://github.com/krabka-io/krabka-broker/actions/runs/39017848795) | The last nightly in which every container group passed. The four-hour soak failed as in the first row, and the upgrade job stopped at its time limit. |
| [Push CI 39058863421](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421), 2026-10-17 | `948e3cc8f99dfabad5bd0917f7b710abc3a07710` | As the first row | `bazel test //...`, Clippy, the proofs and the sustained-load self-check on three brokers. |
| [Release 38520465048](https://github.com/krabka-io/krabka-broker/actions/runs/38520465048), `v1.0.5` | `ac9deff316d38418622b16c237c671df88f57c7a` | `sha256:8c515d335e89455525fb1ad4d9986769c9dd18e44fb259250919c6e7cb53bd84` (amd64) and `sha256:303ced00e15fcd07480e324f7c430f604dcce6b52d38a5df4ed2936743becd4a` (arm64) | Promotion of the image that CI run 38516831673 delivered. |
| [Qualification 39172925991](https://github.com/krabka-io/krabka-broker/actions/runs/39172925991), `loaded lifecycle`, 2026-10-18 | `948e3cc8f99dfabad5bd0917f7b710abc3a07710`, from `v1.0.5` | `sha256:ad1149a44f899e4d30c8eddd9944444e9fddcf8bb3f0ff322ff17f1e85ba4a23` (target, as the first row) and `sha256:2a81d1ba38de4e8ffb032fedf67952845185158a7607ac76ba38fa162dc37e50` (`v1.0.5`, the signed index) | The loaded lifecycle of [#1396](https://github.com/krabka-io/krabka-broker/issues/1396#issuecomment-6256365069) on four nodes under load: rolling upgrade, TLS rotation to a new CA, reassignment, rollback to `v1.0.5`, and a refused unsupported rollback. |
| [Qualification 39328090996](https://github.com/krabka-io/krabka-broker/actions/runs/39328090996), `disaster-recovery-drill`, 2026-10-19 | `f7d824abcb8f95e51f1e078edc8e005db09e2f2c`'s delivery image, with the drill harness of #1540's branch | `sha256:afbb6d96…655c`, as [#1540](https://github.com/krabka-io/krabka-broker/pull/1540) records it | The secured DR drill of [#1397](https://github.com/krabka-io/krabka-broker/issues/1397) under its declared load. |
| [qualification-36293960620](https://github.com/krabka-io/krabka-broker/releases/tag/qualification-36293960620), `v0.6.1` | `d7e46cfed8ecccac21f4fa3391fd4643dc03fcb6` | `sha256:d904478424431650314950e390e00107aee089464747208f573206a95c8d0c24` | All eight [ecosystem qualification](../qualification.md) gates, with krabka-operator v0.4.1. This is a 0.x build, so the 1.x on-disk contract does not cover it. |

## Broker builds

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Release candidate | `untested` | [#1401](https://github.com/krabka-io/krabka-broker/issues/1401) | none | Not frozen. |
| `main`, hermetic suites, Clippy and proofs | `historical` | [Push CI 39058863421, `build · lint · format · test · docgen`](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421/job/117472458106) | `948e3cc8` | The newest `main`, `045b10d4`, had its push CI still running when this page was written. |
| `main`, full container matrix | `historical` | [Nightly 39019310161](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161) | `dc4acebe` | Every group passed. At `948e3cc8`, three groups did not execute. See [Reference runs](#reference-runs). |
| `v1.0.5` release image | `historical` | [Release 38520465048](https://github.com/krabka-io/krabka-broker/actions/runs/38520465048); [#1473 self-check runs](https://github.com/krabka-io/krabka-broker/pull/1473) | `v1.0.5` (`ac9deff3`) | The local self-check runs in #1436 and #1473 used `:latest`, which was `v1.0.5`. They found defects that #1451, #1452, #1455, #1456, #1463, #1485 and #1487 fixed on `main` after it. #1442 and #1448 are still open. |
| `v0.6.1` with the eight-gate sibling set | `historical` | [qualification-36293960620](https://github.com/krabka-io/krabka-broker/releases/tag/qualification-36293960620) | `v0.6.1` (`d7e46cfe`) | No 1.x build has run the eight gates. |

## Clients

The Kafka image column of the [KIP matrix](../KIP_MATRIX.md) says which KIP
each image covers. The rows below say when each client last ran.

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Apache Kafka 4.3.1 Java clients and tools | `historical` | [Nightly 39114726914, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060394); [`jvm_bootstrap_controller+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060512) | `948e3cc8` | The `ApiVersions` differential, the CLI, durability, features, Connect and consumer-offsets formatter suites. The 4.3.1 SASL and reassignment suites are in the group that did not execute at `948e3cc8`; they last passed at `dc4acebe`, in [job 117369463373](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373). |
| Apache Kafka 4.1.0 Java clients | `historical` | [Nightly 39114726914, `jvm_kip320_divergence+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060359) | `948e3cc8` | Share groups and streams groups. |
| Apache Kafka 4.0.0 Java clients and tools | `historical` | [Nightly 39114726914, `jvm_bootstrap_controller+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060512); [`kraft_checkpoint_jvm`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060338) | `948e3cc8` | The KIP-848 consumer, the metadata checkpoint decoder and the mixed static quorum. |
| cp-kafka 7.8.8 (Kafka 3.8 clients) | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373) | `dc4acebe` | Tiered storage and WORM Object Lock. Did not execute at `948e3cc8`. |
| cp-kafka 7.5.0 (Kafka 3.5 clients) | `historical` | [Nightly 39114726914, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060394) | `948e3cc8` | CLI, durability, freeze and Streams. The 7.5.0 TLS, SASL and quota suites last passed at `dc4acebe`. |
| cp-kafka 7.4.0 (Kafka 3.4 clients) | `historical` | [Nightly 39114726914, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060394) | `948e3cc8` | The diskless Jepsen readback and the next-generation group suite. |
| cp-kafka 6.1.1 (Kafka 2.7 clients) | `historical` | [Nightly 39114726914, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060394) | `948e3cc8` | CLI and durability. The 6.1.1 TLS, SASL and tiered suites last passed at `dc4acebe`. |
| cp-kafka 3.1.2 (Kafka 0.10.1 clients) | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373) | `dc4acebe` | The legacy client suite. Did not execute at `948e3cc8`. |
| librdkafka 1.8.2 (kcat 1.7.1) and librdkafka 2.15.0 | `historical` | [Nightly 39019310161, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463297) | `dc4acebe` | Did not execute at `948e3cc8`. |
| sarama (`kaf` 0.2.14, protocol version 1.1) | `historical` | [Nightly 39019310161, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463297) | `dc4acebe` | Did not execute at `948e3cc8`. |
| MirrorMaker 2 from a Kafka 4.3.1 source cluster | `historical` | [Nightly 39019310161, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463297) | `dc4acebe` | Did not execute at `948e3cc8`. |
| krabka-client-rs at `29dac7a5` | `historical` | [Push CI 39058863421, `build · lint · format · test · docgen`](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421/job/117472458106) | `948e3cc8` | The pin that #1457 moved, which carries krabka-io/krabka-client-rs#186. |
| JMX-based tools | `unsupported` | [Ecosystem support](ecosystem-support.md#what-does-not-work) | none | krabka serves no JMX and loads no JVM plugin. |

## Security modes

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| PLAINTEXT | `historical` | [Nightly 39114726914, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060394) | `948e3cc8` | Most container suites run on PLAINTEXT. |
| TLS | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373) | `dc4acebe` | `jvm_acceptance_tls`: handshake and inter-broker TLS. Dynamic keystore reload is #1445. |
| mTLS | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373) | `dc4acebe` | `jvm_acceptance_tls` principal mapping, plus the in-process `mtls` suite. Java-regex limits of the mapping rules are under [Known limitations](#known-limitations). |
| SASL PLAIN | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373); [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | `dc4acebe` | The Kafka 4.3.1 client gave the same outcome against krabka and a stock 4.3.1 broker in the #1419 cases. |
| SASL SCRAM-SHA-256 and SCRAM-SHA-512 | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373); [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | `dc4acebe` | #1476 added the 4096-iteration minimum at authentication. |
| SASL OAUTHBEARER | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373); [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | `dc4acebe` | Unsecured JWS validator only. A JWKS-backed validator has no row here, because no suite runs one. |
| SASL GSSAPI | `historical` | [Nightly 39114726914, `container · gssapi`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117638077249) | `948e3cc8` | `gssapi_e2e` and the GSSAPI `auth_handlers` cases, against the repository's KDC fixture. |
| SASL_SSL with a secured inter-broker listener | `historical` | [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373) | `dc4acebe` | `jvm_acceptance_tls` `sasl_ssl_stack` and `inter_broker_sasl_ssl`. |
| Secured disaster recovery under load (TLS, SASL and ACLs; classic, tiered and diskless paths) | `failing` | [Qualification 39328090996](https://github.com/krabka-io/krabka-broker/actions/runs/39328090996); [#1397](https://github.com/krabka-io/krabka-broker/issues/1397#issuecomment-6256550349) | `f7d824ab` | The judge passed (`dr-secured: PASS`, RPO 19,994 ms against 60 s, RTO 21,015 ms against 5 min), but the job failed `authenticated-round-trip-diskless-idempotent`: a restored diskless partition refused writes ([#1524](https://github.com/krabka-io/krabka-broker/issues/1524)). [#1540](https://github.com/krabka-io/krabka-broker/pull/1540) fixes it, and no drill has run on an image with it. `v1.0.5` does not restore under load at all ([#1520](https://github.com/krabka-io/krabka-broker/issues/1520)). |

## Storage modes

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Classic logs, RF3 | `historical` | [Push CI 39058863421, `container · soak_contract`](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421/job/117472677079); [Nightly 39019310161, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463297) | `948e3cc8`; `dc4acebe` | The five-minute sustained-load self-check, and `process_kill_durability`. |
| Classic logs, `preallocate=true` (`O_DIRECT`) | `untested` | [#1370](https://github.com/krabka-io/krabka-broker/issues/1370) | none | Benchmark-host numbers exist for `main` with #1373 and #1376, but the run did not record its revision, so they cannot count. [#1438](https://github.com/krabka-io/krabka-broker/pull/1438) changed the write alignment after them. [Capacity](capacity.md#observations-without-a-recorded-build) has the numbers. |
| Tiered storage (S3-compatible, MinIO) | `historical` | [Nightly 39114726914, `jvm_tiered_storage`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060369); [Nightly 39019310161, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463373) | `948e3cc8`; `dc4acebe` | [#1448](https://github.com/krabka-io/krabka-broker/issues/1448), a remote-band Fetch with no response, is fixed by [#1538](https://github.com/krabka-io/krabka-broker/pull/1538) after these runs. |
| Tiered storage (GCS emulator) | `historical` | [Nightly 39114726914, `gcs_emulator`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060430) | `948e3cc8` | An emulator, not GCS. |
| WORM Object Lock archive | `historical` | [Nightly 39019310161, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463297) | `dc4acebe` | Did not execute at `948e3cc8`. |
| Backup capture and restore | `historical` | [Nightly 39019310161, `minio_roundtrip`](https://github.com/krabka-io/krabka-broker/actions/runs/39019310161/job/117369463411) | `dc4acebe` | Did not execute at `948e3cc8`. Measured RPO and RTO are for `v0.6.1` only. |
| Diskless WAL | `historical` | [Nightly 39114726914, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117639060394); [#1487](https://github.com/krabka-io/krabka-broker/pull/1487) | `948e3cc8` | `diskless_jepsen`, and the `diskless_e2e` batched-load cases that #1487 added. |
| Diskless WAL under sustained load and faults | `untested` | [#1551](https://github.com/krabka-io/krabka-broker/issues/1551) | none | The self-check with a diskless path ran once, locally, on `v1.0.5` with #1487's binaries overlaid. That is not a CI run of a revision. |

## Deployment configurations

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Single node, combined broker and controller | `historical` | [Push CI 39058863421, `build · lint · format · test · docgen`](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421/job/117472458106) | `948e3cc8` | The in-process suites and `cli_smoke`. |
| Three nodes, RF3, Docker | `historical` | [Push CI 39058863421, `container · soak_contract`](https://github.com/krabka-io/krabka-broker/actions/runs/39058863421/job/117472677079) | `948e3cc8` | The self-check runs for about five minutes. It is not the 72-hour soak. |
| Three nodes, RF3, four-hour soak | `historical` | [Dispatch 39288583006, `soak · four hours`](https://github.com/krabka-io/krabka-broker/actions/runs/39288583006/job/118185682689) | `2cf5551f` | Passed, on the head of [#1537](https://github.com/krabka-io/krabka-broker/pull/1537), which fixed [#1507](https://github.com/krabka-io/krabka-broker/issues/1507). Every nightly from 2026-10-06 to 2026-10-18, the last at `948e3cc8`, failed the log-directory drift bound: each broker's log directory rose about 4.9 MB in the second half, against a 4 MiB allowance, and no acknowledged record was lost. The rise came from `soak-verify`, the offsets topic and the metadata log, which Kafka's defaults do not reclaim within four hours. Rolling the last two each minute exposed two broker leaks that #1537 fixed: compaction left the producer snapshots of merged segments, and the metadata log never deleted its retired segments. |
| Three nodes, RF3, 72-hour soak | `untested` | [#1551](https://github.com/krabka-io/krabka-broker/issues/1551) | none | The contract is `bench/soak/rf3-72h.yaml`. A host is available, and #1551 holds the run plan. |
| Kubernetes StatefulSet on Kind | `historical` | [Nightly 39114726914, `kubernetes · kind`](https://github.com/krabka-io/krabka-broker/actions/runs/39114726914/job/117638077033) | `948e3cc8` | `packaging/k8s/kind-smoke.sh`: three pods, RF3 produce, rolling restart, readback. |
| krabka-operator on Kind | `historical` | [qualification-36293960620](https://github.com/krabka-io/krabka-broker/releases/tag/qualification-36293960620) | `v0.6.1` (`d7e46cfe`) | Operator v0.4.1: upgrade, disruption, scale-down and certificate rotation. No 1.x build has run it. |
| Independent operator installation | `untested` | [#1398](https://github.com/krabka-io/krabka-broker/issues/1398); [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) | none | [Pilot acceptance plans](pilot-acceptance-plans.md) define both pilots. An AI agent's dry run of the runbooks is recorded on #1399 and is not an acceptance run. |

## Upgrades and rollback

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `v1.0.5` to `main`, single-broker smoke | `historical` | [Nightly 39237543947, `upgrade · previous release`](https://github.com/krabka-io/krabka-broker/actions/runs/39237543947/job/118010181507) | `e0588325` | HEAD started on the volume `v1.0.5` wrote, read the record back and found the log start that `DeleteRecords` raised. From 2026-10-16 to 2026-10-18 the smoke hung until the job limit. It read "The on-disk format does not change" in #1438's changelog entry as a declared format break, so it ran HEAD in the foreground and waited for a refusal. HEAD served the old directory. That was a harness fault, not a broker fault. [#1508](https://github.com/krabka-io/krabka-broker/issues/1508) is fixed by #1527, and the smoke now reads only the `**Breaking, on-disk format.**` marker. |
| Rolling upgrade under load, RF3 | `historical` | [Qualification 39172925991, `loaded lifecycle`](https://github.com/krabka-io/krabka-broker/actions/runs/39172925991); [#1396](https://github.com/krabka-io/krabka-broker/issues/1396#issuecomment-6256365069) | `948e3cc8`, from `v1.0.5` | Four nodes, min ISR 2, idempotent, transactional and compacted classic paths at 40/s each. Each node upgraded in turn, then TLS rotation to a new CA and a reassignment. Leader failover took 5.3 to 8.4 s and ISR rejoin 7.8 to 18.9 s. 0 of 141,918 acknowledged sends were lost or duplicated, and all 168 committed-offset checks passed. It does not cover network partitions, tiered or diskless paths, or the operator. |
| Rollback before feature finalization | `historical` | [Qualification 39172925991, `loaded lifecycle`](https://github.com/krabka-io/krabka-broker/actions/runs/39172925991); [#1396](https://github.com/krabka-io/krabka-broker/issues/1396#issuecomment-6256365069) | `948e3cc8`, to `v1.0.5` | Every node rolled back to `v1.0.5` under the same load, with no configuration drift and the same reconciliation as the upgrade. After the candidate finalized `metadata.version` 33 on a node, `v1.0.5` exited with `only supports versions 7-30` and changed no segment, checkpoint or `quorum-state` byte. |
| 0.x data directory read by 1.x | `unsupported` | [Persisted formats](../persisted_formats.md#the-contract) | none | Reformat and restore. |

### Upgrade paths

[Deploy](deploy.md#rolling-upgrade) is the procedure. The rules come from
[Persisted formats](../persisted_formats.md).

- **1.x to 1.y.** Roll one node at a time. A 1.y broker reads everything that
  an earlier 1.x broker wrote.
- **0.x to 1.x.** This is not a roll. Run `krabka-format` on every node and
  restore the topic data from a backup.
- **A new persisted format.** The new build keeps writing the old format until
  you finalize the feature level that introduces it. Roll every node first,
  then finalize with `kafka-features`.

### Rollback restrictions

- **Before finalization**, you can roll back to any earlier 1.x build, because
  nothing on disk uses the new format.
- **After finalization**, a rollback follows Kafka's downgrade rules for that
  feature. For `metadata.version`, krabka implements the KIP-1155 snapshot
  path. A build that does not support the finalized level cannot join.
- **`krabka.version`.** Do not finalize level 1 while the cluster has a JVM
  Kafka node. A JVM node supports only level 0, and a level above 0 crashes a
  JVM standby controller. The controller refuses a level that a registered
  node does not support.
- **Tiered `DeleteRecords` during a roll.** Do not run `DeleteRecords` on a
  tiered topic while the roll is in progress, until every broker has the fix
  for [#1481](https://github.com/krabka-io/krabka-broker/issues/1481).
  [Deploy](deploy.md#rolling-upgrade) gives the reason.
- **0.x data has no promise.** A 1.x broker does not read it.

[#1396](https://github.com/krabka-io/krabka-broker/issues/1396) exercised
the first two on release images: a rollback to `v1.0.5` before finalization,
and a refused one after it. The other rules are the documented contract.

## Known limitations

These come from the evidence records of milestones 24 to 29. Each item links
its source. An item stays here until a linked change removes it.

### Open defects

- **`COORDINATOR_NOT_AVAILABLE` under max-rate RF3 load.** #1410 fixed two
  follower-fetch causes, and #1457 stopped segment deletion from stalling
  appends and follower reads. The storage-stall candidate needs a
  benchmark-host rerun.
  [#1352](https://github.com/krabka-io/krabka-broker/issues/1352) is open.

Fixed since the 2026-10-18 version of this page, and not yet rerun on a
candidate: [#1448](https://github.com/krabka-io/krabka-broker/issues/1448),
a tiered Fetch in the remote band with no response
([#1538](https://github.com/krabka-io/krabka-broker/pull/1538)), and
[#1442](https://github.com/krabka-io/krabka-broker/issues/1442), a killed
broker that restarted inside its heartbeat session and kept its partitions
([#1553](https://github.com/krabka-io/krabka-broker/pull/1553)).

### Kafka behavior differences

- **RE2J consumer-group regexes.** Rust `regex`'s compiled-size limit refuses
  some nested counts that RE2J 1.8 accepts, such as `(a{1000}){1000}`. A
  repetition with no argument is refused with Rust's message, not RE2J's. A
  surrogate escape such as `\x{D800}` is refused.
  The "Unresolved" section of
  [#1407](https://github.com/krabka-io/krabka-broker/pull/1407) lists them.
- **Java regexes** in `ssl.principal.mapping.rules`, Kerberos `auth_to_local`
  and KIP-714 `match`. For a group that only matches the empty string, such as
  `()*\1`, krabka keeps the group where Java's `GroupCurly` drops it. Java
  splits an emoji's UTF-16 pair in `replaceAll("x*")`, and a Rust `String`
  cannot hold that. [#1480](https://github.com/krabka-io/krabka-broker/pull/1480)
  and [#1482](https://github.com/krabka-io/krabka-broker/pull/1482).
- **ShareFetch quotas.** ShareFetch charges its active handler time, where the
  consumer Fetch path charges wall time. A share fetch's tiered read does not
  check the fetch quota or the remote wait. Kafka's KIP-956 throttle-time and
  quota sensors are not published.
  [#1412](https://github.com/krabka-io/krabka-broker/pull/1412) and
  [#1486](https://github.com/krabka-io/krabka-broker/pull/1486).
- **Authentication.** Handler and mute time does not count toward idle expiry.
  PLAIN compares raw bytes where Kafka decodes lossy UTF-8. A SCRAM proof of
  the wrong length gets error 58 where Kafka drops the connection.
  [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007).
- **Connection caps.** krabka closes a connection over `max.connections`.
  Kafka's acceptor waits for a slot.
  [#1391 comment](https://github.com/krabka-io/krabka-broker/issues/1391#issuecomment-6195246931).
- **Dynamic `listeners`.** A TLS listener added without a broker-wide TLS
  config is not opened. An added listener is not registered with the
  controller, as in Kafka.
  [#1474](https://github.com/krabka-io/krabka-broker/pull/1474).
- **The KIP-matrix notes** list the remaining per-KIP differences. Read the
  [KIP matrix](../KIP_MATRIX.md) for the KIP that you depend on.

### Evidence gaps

- **netem slow followers have run only in the five-minute self-check.** The
  nightly `netem` job runs `bench/soak/self-check-netem.yaml` on a
  GitHub-hosted runner, and its first run passed
  ([job 118454690131](https://github.com/krabka-io/krabka-broker/actions/runs/39369741872/job/118454690131)).
  The 72-hour run is the first to repeat the fault under sustained load.
  [#1551](https://github.com/krabka-io/krabka-broker/issues/1551).
- **Diskless under sustained load** has one local self-check and no CI or
  72-hour run. [#1393](https://github.com/krabka-io/krabka-broker/issues/1393).
- **read_committed across local eviction on a live cluster, and RF3
  consumer-group coordinator failover**, have no harness.
  [#1388 comment](https://github.com/krabka-io/krabka-broker/issues/1388#issuecomment-6186902018).
- **Performance on the benchmark host** after #1438 and #1457 has not run.
  [Capacity](capacity.md#measured-envelope) lists what is measured and where.

## Operator findings

No independent operator has installed, migrated or recovered a cluster yet.
[#1398](https://github.com/krabka-io/krabka-broker/issues/1398) defines the
two pilots in [Pilot acceptance plans](pilot-acceptance-plans.md), and they
wait on its human actions: naming the operators, access and consent.
[#1399](https://github.com/krabka-io/krabka-broker/issues/1399) records an AI
agent's dry run of the runbooks. It is not an operator finding, and every
defect it found is fixed on `main`. This section stays empty until #1399
publishes consented results.

## Evidence needed before release

1. Freeze the candidate revision and image digest
   ([#1401](https://github.com/krabka-io/krabka-broker/issues/1401)), then
   rerun every `historical` row on it.
2. One nightly in which every container group executes on the candidate. At
   `948e3cc8`, three groups did not.
3. The 72-hour RF3 soak, including the diskless and netem faults
   ([#1551](https://github.com/krabka-io/krabka-broker/issues/1551)), and a
   pass of the four-hour soak on the candidate. Since the
   [#1507](https://github.com/krabka-io/krabka-broker/issues/1507) fix, it has
   passed at `2cf5551f`.
4. A pass of `upgrade · previous release` on the candidate. It passed on
   `main` at `e0588325` after the
   [#1508](https://github.com/krabka-io/krabka-broker/issues/1508) fix. The
   loaded upgrade and rollback passed at `948e3cc8`
   ([#1396](https://github.com/krabka-io/krabka-broker/issues/1396)), and
   must pass on the candidate too.
5. A green secured disaster-recovery drill with measured RPO and RTO for a
   1.x image that has [#1540](https://github.com/krabka-io/krabka-broker/pull/1540)
   ([#1397](https://github.com/krabka-io/krabka-broker/issues/1397)), and the
   eight qualification gates for a 1.x broker.
6. Benchmark-host runs of the RF3 regression budgets and the
   `preallocate=true` workload
   ([#1395](https://github.com/krabka-io/krabka-broker/issues/1395),
   [#1370](https://github.com/krabka-io/krabka-broker/issues/1370)).
7. Operator pilots or labeled independent acceptance runs
   ([#1398](https://github.com/krabka-io/krabka-broker/issues/1398),
   [#1399](https://github.com/krabka-io/krabka-broker/issues/1399)).
8. A disposition for each open defect under
   [Known limitations](#known-limitations).

## Keeping this page current

Change a row only with a link to the run that changes it. When a run fails,
set the row to `failing` and link the failed run. Do not keep an older pass in
its place. When #1401 freezes the candidate, set the candidate revision above.
Then move each row to `candidate` only after its check passes on that
revision.
