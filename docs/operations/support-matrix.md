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
nothing about a later build. A draft manifest for `v1.0.8` waits for
maintainer review, and it does not freeze anything.

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

The rows cite these runs. This page is current as of 2026-10-25. A container
suite builds the broker from the source at its revision, so the image digest
names the delivered image of that revision. It does not prove that the suite
ran those bytes. [#1401](https://github.com/krabka-io/krabka-broker/issues/1401)
binds the candidate evidence to the delivered bytes.

| Run | Revision | Delivered image | What it executed |
| :--- | :--- | :--- | :--- |
| [Dispatch 39519220181](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181), 2026-10-21, `v1.0.8` | `3831a25de74804460f32d921b711e0cacef48deb` | `sha256:960d3ad1ae6f87d0a4bc3ff2604c05d0e1d65f4e77a0013cecff6787f0a586b4` (amd64), from [push CI 39518736364](https://github.com/krabka-io/krabka-broker/actions/runs/39518736364) | Every `ci.yml` job: the full container matrix, GSSAPI, Kind, `netem`, the four-hour soak and the upgrade from `v1.0.7`. Every check job passed. Two jobs failed: `external links`, on 404s for the `v1.0.8` tag before it existed, which passed on its re-run; and the criterion lane, which measured `broker/produce_acks/32part/acks_all` at 1.16 times the `v1.0.5` reference, against a 0.113 tolerance. |
| [Dispatch 39493788635](https://github.com/krabka-io/krabka-broker/actions/runs/39493788635), 2026-10-20, `v1.0.7` | `eb7e0c6ab22ead627997d75bcac6e532d4c41c51` | `sha256:7a6075d62cfd28064830deafe173d9522f5b43c9bb14c85a5cb4fc262f5b3fe1` (amd64), from [push CI 39493754304](https://github.com/krabka-io/krabka-broker/actions/runs/39493754304) | The same jobs. Every check job passed, and the upgrade ran from `v1.0.6`. The same two jobs failed: `external links`, and the criterion lane on `log/read/past_end_returns_empty`, at 1.79 times the reference. |
| [Release 39523147617](https://github.com/krabka-io/krabka-broker/actions/runs/39523147617), [39501119789](https://github.com/krabka-io/krabka-broker/actions/runs/39501119789) and [39474117467](https://github.com/krabka-io/krabka-broker/actions/runs/39474117467) | `v1.0.8`, `v1.0.7` and `v1.0.6` | The signed indexes in [Provenance verification](../release-candidate-qualification.md#result-for-v106-v107-and-v108) | Promotion of each delivered image. `aspect verify-release` passes for all three. |
| [Nightly 40081197462](https://github.com/krabka-io/krabka-broker/actions/runs/40081197462), 2026-10-24 | `782f5b50268679e428f2da5118735ba3f87c4dda` | None. Its push CI did not deliver an image. | The full container matrix as one `container suites` job, with 41 of 41 targets passed and `jbod_enospc` flaky. The four-hour soak, the upgrade from `v1.0.8`, GSSAPI, Kind and `netem` passed. The criterion lane failed in its harness before any comparison ran. |
| [Push CI 40160834355](https://github.com/krabka-io/krabka-broker/actions/runs/40160834355), 2026-10-25 | `315c29856` (`main`) | None. `gate` failed, so `delivery` did not run. | `coverage` failed `feature_finalization_test::validate_only_does_not_persist` and `dynamic_voters_test::remove_voter_shrinks_quorum`. The build, Clippy, proofs and container suites passed. |
| [Qualification 39375286339](https://github.com/krabka-io/krabka-broker/actions/runs/39375286339), `disaster-recovery-drill`, 2026-10-20 | `7301120105168ff7118e72bb5fc320f76cc45dbf`'s delivery image, with the drill harness at `e7d0f3c6` | `sha256:9241c4fa3bc90231a38ce0a38ec3cb20cd7d7145cd5f0be8481002ae9f59d776` | The secured DR drill of [#1397](https://github.com/krabka-io/krabka-broker/issues/1397) under its declared load. `7301120` has [#1540](https://github.com/krabka-io/krabka-broker/pull/1540). |
| [Qualification 39446231280](https://github.com/krabka-io/krabka-broker/actions/runs/39446231280), eight-gate preflight, 2026-10-20 | `f5c683d9`'s image, with the manifest at `168d7981` on a release branch. `f5c683d9` is three commits before `v1.0.6`. | `sha256:236d5745…3856` | The eight ecosystem gates and the loaded lifecycle from and to `v1.0.5`. The loaded lifecycle, `observability-recovery`, `registry-migration` and `snapshot-retention` passed. `installation`, `cli-admin` and `operator-lifecycle` failed. `disaster-recovery` and `schema-evolution` failed in the harness, so their checks did not run. |
| [qualification-36293960620](https://github.com/krabka-io/krabka-broker/releases/tag/qualification-36293960620), `v0.6.1` | `d7e46cfed8ecccac21f4fa3391fd4643dc03fcb6` | `sha256:d904478424431650314950e390e00107aee089464747208f573206a95c8d0c24` | All eight [ecosystem qualification](../qualification.md) gates, with krabka-operator v0.4.1. This is a 0.x build, so the 1.x on-disk contract does not cover it. |

## Broker builds

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Release candidate | `untested` | [#1401](https://github.com/krabka-io/krabka-broker/issues/1401) | none | Not frozen. The draft manifest [`ecosystem-v1.0.8.json`](../../qualification/ecosystem-v1.0.8.json) names `v1.0.8` and waits for maintainer review. |
| `main`, hermetic suites, Clippy and proofs | `failing` | [Push CI 40160834355, `coverage`](https://github.com/krabka-io/krabka-broker/actions/runs/40160834355/job/121074434938) | `315c2985` | `feature_finalization_test::validate_only_does_not_persist` and `dynamic_voters_test::remove_voter_shrinks_quorum` failed, so `gate` failed. The `build · lint · format · test · docgen` job passed. The last `main` push CI with a green `gate` and every container group is [39518736364](https://github.com/krabka-io/krabka-broker/actions/runs/39518736364) at `3831a25d`. |
| `main`, full container matrix | `historical` | [Nightly 40081197462, `container suites`](https://github.com/krabka-io/krabka-broker/actions/runs/40081197462/job/120835178648) | `782f5b50` | 41 of 41 targets passed. `jbod_enospc` passed on a retry. Every container group passed in the nightlies of 10-19, 10-21, 10-22 and 10-23 too. |
| `v1.0.8` release image | `historical` | [Release 39523147617](https://github.com/krabka-io/krabka-broker/actions/runs/39523147617); [dispatch 39519220181](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181) | `v1.0.8` (`3831a25d`) | Every check job of the dispatch passed. The criterion lane failed on `broker/produce_acks/32part/acks_all`; [Release candidate qualification](../release-candidate-qualification.md#candidate-gates) records it. |
| `v1.0.7` release image | `historical` | [Release 39501119789](https://github.com/krabka-io/krabka-broker/actions/runs/39501119789); [dispatch 39493788635](https://github.com/krabka-io/krabka-broker/actions/runs/39493788635) | `v1.0.7` (`eb7e0c6a`) | Every check job of the dispatch passed. The criterion lane failed on `log/read/past_end_returns_empty`. |
| `v1.0.6` release image | `historical` | [Release 39474117467](https://github.com/krabka-io/krabka-broker/actions/runs/39474117467); [push CI 39467563012](https://github.com/krabka-io/krabka-broker/actions/runs/39467563012) | `v1.0.6` (`402edff3`) | Its push CI passed `gate` and ran the `jvm_acceptance_cli` container group only. No full dispatch ran at `402edff3`. |
| `v1.0.5` release image | `historical` | [Release 38520465048](https://github.com/krabka-io/krabka-broker/actions/runs/38520465048); [#1473 self-check runs](https://github.com/krabka-io/krabka-broker/pull/1473) | `v1.0.5` (`ac9deff3`) | The local self-check runs in #1436 and #1473 used `:latest`, which was `v1.0.5`. They found defects that #1451, #1452, #1455, #1456, #1463, #1485 and #1487 fixed on `main` after it. |
| `v0.6.1` with the eight-gate sibling set | `historical` | [qualification-36293960620](https://github.com/krabka-io/krabka-broker/releases/tag/qualification-36293960620) | `v0.6.1` (`d7e46cfe`) | The last build that passed all eight gates. |
| 1.x with the eight gates | `failing` | [Qualification 39446231280](https://github.com/krabka-io/krabka-broker/actions/runs/39446231280) | `f5c683d9` | A preflight, not a candidate. Three of eight gates passed. `installation` failed on `COORDINATOR_NOT_AVAILABLE` in the Go client check, `cli-admin` on a SASL connection reset, and `operator-lifecycle` on a timeout. In `disaster-recovery`, `krabka-cli` v0.4.4 refused the `krabka.diskless` topic config before any capture. `schema-evolution` looked for a test name that its suite does not have. |

## Clients

The Kafka image column of the [KIP matrix](../KIP_MATRIX.md) says which KIP
each image covers. The rows below say when each client last ran.

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Apache Kafka 4.3.1 Java clients and tools | `historical` | [Dispatch 39519220181, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959888020); [`jvm_bootstrap_controller+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887762); [`jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | The `ApiVersions` differential, the CLI, durability, features, Connect, consumer-offsets formatter, SASL and reassignment suites. |
| Apache Kafka 4.1.0 Java clients | `historical` | [Dispatch 39519220181, `jvm_kip320_divergence+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887871) | `3831a25d` | Share groups and streams groups. `jvm_share_groups_docker_test` passed on its second attempt. |
| Apache Kafka 4.0.0 Java clients and tools | `historical` | [Dispatch 39519220181, `jvm_bootstrap_controller+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887762); [`kraft_checkpoint_jvm`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887668) | `3831a25d` | The KIP-848 consumer, the metadata checkpoint decoder and the mixed static quorum. |
| cp-kafka 7.8.8 (Kafka 3.8 clients) | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | Tiered storage and WORM Object Lock. |
| cp-kafka 7.5.0 (Kafka 3.5 clients) | `historical` | [Dispatch 39519220181, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959888020); [`jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | CLI, durability, freeze and Streams, and the TLS, SASL and quota suites. |
| cp-kafka 7.4.0 (Kafka 3.4 clients) | `historical` | [Dispatch 39519220181, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959888020) | `3831a25d` | The diskless Jepsen readback and the next-generation group suite. |
| cp-kafka 6.1.1 (Kafka 2.7 clients) | `historical` | [Dispatch 39519220181, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959888020); [`jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | CLI and durability, and the TLS, SASL and tiered suites. |
| cp-kafka 3.1.2 (Kafka 0.10.1 clients) | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | The legacy client suite. |
| librdkafka 1.8.2 (kcat 1.7.1) and librdkafka 2.15.0 | `historical` | [Dispatch 39519220181, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887711) | `3831a25d` | |
| sarama (`kaf` 0.2.14, protocol version 1.1) | `historical` | [Dispatch 39519220181, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887711) | `3831a25d` | |
| MirrorMaker 2 from a Kafka 4.3.1 source cluster | `historical` | [Dispatch 39519220181, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887711) | `3831a25d` | |
| krabka-client-rs at `ba3443d0` (0.7.1) | `historical` | [Dispatch 39519220181, `build · lint · format · test · docgen`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118958685611) | `3831a25d` | The pin that `v1.0.8` compiles in. `main` has moved to `b1320dad` since. |
| JMX-based tools | `unsupported` | [Ecosystem support](ecosystem-support.md#what-does-not-work) | none | krabka serves no JMX and loads no JVM plugin. |

## Security modes

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| PLAINTEXT | `historical` | [Dispatch 39519220181, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959888020) | `3831a25d` | Most container suites run on PLAINTEXT. |
| TLS | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | `jvm_acceptance_tls`: handshake and inter-broker TLS. Dynamic keystore reload came with [#1445](https://github.com/krabka-io/krabka-broker/pull/1445). |
| mTLS | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | `jvm_acceptance_tls` principal mapping, plus the in-process `mtls` suite. Java-regex limits of the mapping rules are under [Known limitations](#known-limitations). |
| SASL PLAIN | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875); [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | `3831a25d` | The Kafka 4.3.1 client gave the same outcome against krabka and a stock 4.3.1 broker in the #1419 cases. |
| SASL SCRAM-SHA-256 and SCRAM-SHA-512 | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875); [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | `3831a25d` | #1476 added the 4096-iteration minimum at authentication. |
| SASL OAUTHBEARER | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875); [#1390 comment](https://github.com/krabka-io/krabka-broker/issues/1390#issuecomment-6195563007) | `3831a25d` | Unsecured JWS validator only. A JWKS-backed validator has no row here, because no suite runs one. |
| SASL GSSAPI | `historical` | [Dispatch 39519220181, `container · gssapi`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118958685719) | `3831a25d` | `gssapi_e2e` and the GSSAPI `auth_handlers` cases, against the repository's KDC fixture. |
| SASL_SSL with a secured inter-broker listener | `historical` | [Dispatch 39519220181, `jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | `jvm_acceptance_tls` `sasl_ssl_stack` and `inter_broker_sasl_ssl`. |
| Secured disaster recovery under load (TLS, SASL and ACLs; classic, tiered and diskless paths) | `historical` | [Qualification 39375286339, `disaster-recovery-drill`](https://github.com/krabka-io/krabka-broker/actions/runs/39375286339/job/118475068907); [#1397](https://github.com/krabka-io/krabka-broker/issues/1397) | `7301120` | The judge passed (`dr-secured: PASS`, RPO 19,980 ms against 60 s, RTO 23,839 ms against 5 min), and all 51 checks passed, `authenticated-round-trip-diskless-idempotent` among them. `7301120` has [#1540](https://github.com/krabka-io/krabka-broker/pull/1540), which fixed [#1524](https://github.com/krabka-io/krabka-broker/issues/1524). It is a `main` image, not a release image; the drill on a release image is scheduled with the [1.1 release](https://github.com/krabka-io/krabka-broker/milestone/31) qualification. `v1.0.5` does not restore under load at all ([#1520](https://github.com/krabka-io/krabka-broker/issues/1520)). |

## Storage modes

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Classic logs, RF3 | `historical` | [Dispatch 39519220181, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887711) | `3831a25d` | The five-minute sustained-load self-check, and `process_kill_durability`. |
| Classic logs, `preallocate=true` (`O_DIRECT`) | `untested` | [#1370](https://github.com/krabka-io/krabka-broker/issues/1370) | none | Benchmark-host numbers exist for `main` with #1373 and #1376, but the run did not record its revision, so they cannot count. [#1438](https://github.com/krabka-io/krabka-broker/pull/1438) changed the write alignment after them. [Capacity](capacity.md#observations-without-a-recorded-build) has the numbers. The rerun is scheduled with the 1.1 release qualification. |
| Tiered storage (S3-compatible, MinIO) | `historical` | [Dispatch 39519220181, `jvm_tiered_storage`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887685); [`jvm_acceptance_legacy+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887875) | `3831a25d` | `3831a25d` has [#1538](https://github.com/krabka-io/krabka-broker/pull/1538), the fix for [#1448](https://github.com/krabka-io/krabka-broker/issues/1448), a remote-band Fetch with no response. |
| Tiered storage (GCS emulator) | `historical` | [Dispatch 39519220181, `gcs_emulator`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887857) | `3831a25d` | An emulator, not GCS. |
| WORM Object Lock archive | `historical` | [Dispatch 39519220181, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887711) | `3831a25d` | |
| Backup capture and restore | `historical` | [Dispatch 39519220181, `minio_roundtrip`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887856) | `3831a25d` | The 1.x RPO and RTO are from the secured DR drill at `7301120`, above. |
| Diskless WAL | `historical` | [Dispatch 39519220181, `api_versions_differential+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959888020); [#1487](https://github.com/krabka-io/krabka-broker/pull/1487) | `3831a25d` | `diskless_jepsen`, and the `diskless_e2e` batched-load cases that #1487 added. |
| Diskless WAL under sustained load and faults | `untested` | [#1551](https://github.com/krabka-io/krabka-broker/issues/1551) | none | The self-check with a diskless path ran once, locally, on `v1.0.5` with #1487's binaries overlaid. That is not a CI run of a revision. The 72-hour run is scheduled with the 1.1 release qualification. |

## Deployment configurations

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Single node, combined broker and controller | `historical` | [Push CI 39518736364, `build · lint · format · test · docgen`](https://github.com/krabka-io/krabka-broker/actions/runs/39518736364/job/118957113250) | `3831a25d` | The in-process suites and `cli_smoke`. |
| Three nodes, RF3, Docker | `historical` | [Dispatch 39519220181, `librdkafka_conformance+6`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118959887711) | `3831a25d` | The `soak_contract` self-check runs for about five minutes. It is not the 72-hour soak. |
| Three nodes, RF3, four-hour soak | `historical` | [Dispatch 39519220181, `soak · four hours`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118958685613) | `3831a25d` | Passed in 14,411 s. It also passed at `eb7e0c6a`, and in every `main` nightly from 10-21 to 10-24, the last at `782f5b50` ([job 120834946250](https://github.com/krabka-io/krabka-broker/actions/runs/40081197462/job/120834946250)). The last failure was the 10-19 nightly at `e0588325`, before [#1537](https://github.com/krabka-io/krabka-broker/pull/1537) fixed [#1507](https://github.com/krabka-io/krabka-broker/issues/1507). |
| Three nodes, RF3, 72-hour soak | `untested` | [#1551](https://github.com/krabka-io/krabka-broker/issues/1551) | none | The contract is `bench/soak/rf3-72h.yaml`. The run is scheduled with the [1.1 release](https://github.com/krabka-io/krabka-broker/milestone/31) qualification. |
| Kubernetes StatefulSet on Kind | `historical` | [Dispatch 39519220181, `kubernetes · kind`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118958685906) | `3831a25d` | `aspect kind-smoke`: three pods, mTLS RF3 produce, rolling restart, readback. |
| krabka-operator on Kind | `failing` | [Qualification 39446231280, `gate (operator-lifecycle)`](https://github.com/krabka-io/krabka-broker/actions/runs/39446231280/job/118709581934) | `f5c683d9` | Operator chart 1.0.1 at `56e1f8f2`. The upgrade, the disruption, the drain to three brokers and the rolling update completed. Then the gate timed out waiting on `kafkanodepools/brokers`. The run does not show whether the operator or the broker caused it. The last pass is `v0.6.1` with operator v0.4.1. |
| Independent operator installation | `untested` | [#1398](https://github.com/krabka-io/krabka-broker/issues/1398); [#1399](https://github.com/krabka-io/krabka-broker/issues/1399) | none | [Pilot acceptance plans](pilot-acceptance-plans.md) define both pilots. An AI agent's dry run of the runbooks is recorded on #1399 and is not an acceptance run. The pilots gate the 1.1 release. |

## Upgrades and rollback

| Combination | Status | Evidence | Tested at | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `v1.0.5` to `main`, single-broker smoke | `historical` | [Dispatch 39519220181, `upgrade · previous release`](https://github.com/krabka-io/krabka-broker/actions/runs/39519220181/job/118958685699) | `3831a25d`, from `v1.0.7` | The job now upgrades from the previous release, not from `v1.0.5`. HEAD started on the volume that the previous release wrote, read the record back and found the log start that `DeleteRecords` raised. It also passed at `eb7e0c6a` from `v1.0.6`, and in every nightly from 10-21 to 10-24 from `v1.0.8`, the last at `782f5b50` ([job 120834946248](https://github.com/krabka-io/krabka-broker/actions/runs/40081197462/job/120834946248)). |
| Rolling upgrade under load, RF3 | `historical` | [Qualification 39446231280, `loaded lifecycle`](https://github.com/krabka-io/krabka-broker/actions/runs/39446231280/job/118708771194); [#1396](https://github.com/krabka-io/krabka-broker/issues/1396) | `f5c683d9`, from `v1.0.5` | Four nodes, min ISR 2, idempotent, transactional and compacted classic paths under load. Each node upgraded in turn, then TLS rotation to a new CA and a reassignment. Leader failover took 5.3 to 8.4 s and ISR rejoin 7.1 to 18.7 s. 0 of 141,412 acknowledged sends were lost or duplicated, and all 168 committed-offset checks passed. `f5c683d9` is a release-branch commit three commits before `v1.0.6`. It does not cover network partitions, tiered or diskless paths, or the operator. The run on release images, `v1.0.6` to `v1.0.8`, is scheduled with the [1.1 release](https://github.com/krabka-io/krabka-broker/milestone/31) qualification. |
| Rollback before feature finalization | `historical` | [Qualification 39446231280, `loaded lifecycle`](https://github.com/krabka-io/krabka-broker/actions/runs/39446231280/job/118708771194); [#1396](https://github.com/krabka-io/krabka-broker/issues/1396) | `f5c683d9`, to `v1.0.5` | Every node rolled back to `v1.0.5` under the same load. After the candidate finalized `metadata.version` 33 on a node, `v1.0.5` refused it and changed nothing. The same check in the `soak_contract` self-check is intermittent: it failed in [`netem` at `f5c683d9`](https://github.com/krabka-io/krabka-broker/actions/runs/39438207211/job/118680789544) and on the first attempt of the [10-22 nightly](https://github.com/krabka-io/krabka-broker/actions/runs/39758128742/job/119760217842). In both failures, `v1.0.5` changed the metadata log, the high watermark, `leader-epoch-checkpoint` and `quorum-state` while it refused the level. It passed at `3831a25d` and in the nightlies of 10-23 and 10-24. Release blocker F5 records it. |
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
  benchmark-host rerun, which is scheduled with the 1.1 release
  qualification. [#1352](https://github.com/krabka-io/krabka-broker/issues/1352) is open.
- **A refused rollback can change bytes, intermittently.** In two
  `soak_contract` runs, `v1.0.5` changed the metadata log, the high
  watermark, `leader-epoch-checkpoint` and `quorum-state` while it refused a
  finalized `metadata.version` 33. See
  [Rollback before feature finalization](#upgrades-and-rollback). No issue is
  filed yet. Release blocker F5 records it.

Fixed since the 2026-10-18 version of this page, and passed at `v1.0.8`
(`3831a25d`) but not on a candidate: [#1448](https://github.com/krabka-io/krabka-broker/issues/1448),
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
  72-hour run. [#1551](https://github.com/krabka-io/krabka-broker/issues/1551).
- **read_committed across local eviction on a live cluster, and RF3
  consumer-group coordinator failover**, have no harness.
  [#1388 comment](https://github.com/krabka-io/krabka-broker/issues/1388#issuecomment-6186902018).
- **Performance on the benchmark host** after #1438 and #1457 has not run.
  The RF3 regression budgets ran on a 32-core cloud worker instead, at
  `fd007630`; [Performance budget results](performance-budget-results.md)
  has them.
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

The runs in items 3 to 7 are scheduled with the
[1.1 release](https://github.com/krabka-io/krabka-broker/milestone/31)
qualification.

1. Freeze the candidate revision and image digest
   ([#1401](https://github.com/krabka-io/krabka-broker/issues/1401)), then
   rerun every `historical` row on it.
2. One full-matrix run in which every container group passes on the
   candidate. It passed at `v1.0.8` (`3831a25d`) and in the nightlies up to
   `782f5b50`.
3. The 72-hour RF3 soak, including the diskless and netem faults
   ([#1551](https://github.com/krabka-io/krabka-broker/issues/1551)), and a
   pass of the four-hour soak on the candidate. The four-hour soak passed at
   `v1.0.8` and in every nightly from 10-21 to 10-24.
4. A pass of `upgrade · previous release` on the candidate. It passed at
   `v1.0.8` from `v1.0.7`. The loaded upgrade and rollback passed at
   `f5c683d9` from and to `v1.0.5`
   ([#1396](https://github.com/krabka-io/krabka-broker/issues/1396)), and
   must pass on release images and on the candidate.
5. A green secured disaster-recovery drill on a release image or the
   candidate ([#1397](https://github.com/krabka-io/krabka-broker/issues/1397)).
   It passed on the `main` image of `7301120`. All eight qualification gates
   must also pass for a 1.x broker; the preflight at `f5c683d9` passed three.
6. Runs of the RF3 regression budgets under `rf3-regression-v3` on the
   candidate, and benchmark-host runs of the `preallocate=true` workload
   ([#1395](https://github.com/krabka-io/krabka-broker/issues/1395),
   [#1370](https://github.com/krabka-io/krabka-broker/issues/1370)).
7. Operator pilots or labeled independent acceptance runs
   ([#1398](https://github.com/krabka-io/krabka-broker/issues/1398),
   [#1399](https://github.com/krabka-io/krabka-broker/issues/1399)).
8. A disposition for each open defect under
   [Known limitations](#known-limitations), and a green `main` `gate`
   again.

## Keeping this page current

Change a row only with a link to the run that changes it. When a run fails,
set the row to `failing` and link the failed run. Do not keep an older pass in
its place. When #1401 freezes the candidate, set the candidate revision above.
Then move each row to `candidate` only after its check passes on that
revision.
