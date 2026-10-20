# Roadmap

This plan covers October 14, 2026 to April 14, 2027. The goal is reliable
operation under load. It assumes one primary maintainer, with 25% of capacity
reserved for regressions and release work.

[GitHub milestones](https://github.com/krabka-io/krabka-broker/milestones)
track the work and completion criteria. Target dates are planning targets.
Review failures weekly and scope monthly. If evidence is incomplete, reduce
later feature scope to make room for the fix. Continue compatibility and
security fixes throughout the plan.

## Delivered

| Milestone | Question |
| :--- | :--- |
| M1 | Can CI catch a regression, and is the dependency policy real? |
| M2 | Does what krabka persists survive failure, and are the proved decisions proved? |
| M3 | Is the diskless WAL reachable, safe and observable? |
| M4 | Is the Kafka compatibility claim true and evidenced? |
| M5 | Can anyone run this, measure it, and know it is healthy? |
| M6 | Can someone else operate or contribute to this? |
| M7 | Formal verification: safety-critical algorithms. |
| M8 | Does the cluster survive a partition, a prune and a restart without losing metadata or records? |
| M9 | Does a stock Kafka Streams, Connect or share-consumer deployment run against krabka? |
| M10 | Can an operator secure the cluster and prove who did what? |
| M11 | Can an operator see the cluster's health, roll it, repair the quorum and keep tenants fair? |
| M12 | Is what ships the thing that was tested, and does the evidence the docs cite hold up? |
| M13 | Does an acknowledged record survive a kill, and does anything page when the broker starts failing? |
| M14 | Is the tiered archive a complete copy, and can a cluster be rebuilt from it? |
| M15 | Can the clients and tools a stranger already runs talk to krabka, and does the matrix they read say so? |
| [M16](https://github.com/krabka-io/krabka-broker/milestone/16) | Can this be deployed on Kubernetes, grown under load, and given the next build on the same disks? |
| [M17](https://github.com/krabka-io/krabka-broker/milestone/17) | Do recovery and placement preserve the semantics Kafka operators rely on? |
| [M18](https://github.com/krabka-io/krabka-broker/milestone/18) | Can throughput and operating limits be predicted from reproducible measurements? |
| [M19](https://github.com/krabka-io/krabka-broker/milestone/19) | Do the first-party clients, streams, registry and connectors work together through failure? |
| [M20](https://github.com/krabka-io/krabka-broker/milestone/20) | Can an operator install, observe and roll one tested krabka stack? |
| [M21](https://github.com/krabka-io/krabka-broker/milestone/21) | Does each new release carry fresh evidence for the bytes it ships? |
| [M22](https://github.com/krabka-io/krabka-broker/milestone/22) | Can a secured cluster recover trustworthy records after disk or cluster loss? |
| [M23](https://github.com/krabka-io/krabka-broker/milestone/23) | Do stateful applications preserve schemas and snapshots across change? |

Milestones M1 through M23 are closed on GitHub as of October 14, 2026.
Closure records completed milestone scope; it does not qualify every later
release. Current candidates need evidence for their exact artifacts.

## Next

M24 and M25 share the first month. They retain their existing compatibility
scope and add current backlog triage. Later phases reuse the existing
performance, operator lifecycle and ecosystem qualification harnesses.

| Period | Target | Milestone and focus | Completion criteria |
| :--- | :--- | :--- | :--- |
| Oct 14 to Nov 14 | 2026-11-14 | [M24: Close correctness gaps](https://github.com/krabka-io/krabka-broker/milestone/24) | Confirm current M24 findings against pinned Kafka 4.3.1. Each confirmed defect has a production-boundary regression test. Resolve or explicitly record release blockers for coordinator progress, quorum convergence, fetch limits and replication throttles. |
| Oct 14 to Nov 14 | 2026-11-14 | [M25: Close authentication and live configuration gaps](https://github.com/krabka-io/krabka-broker/milestone/25) | Confirm current M25 findings against pinned Kafka 4.3.1. Record positive and negative authentication results, observable runtime configuration changes, and network admission and idle-expiry checks. |
| Nov 14 to Dec 14 | 2026-12-14 | [M26: Prove stability under sustained load](https://github.com/krabka-io/krabka-broker/milestone/26) | Complete a 72-hour RF=3 soak with concurrent produce, consume, offset commits, compaction and tiered reads. Inject disk pressure, slow followers, process pauses and restarts. Reconcile acknowledged records and committed offsets. Declare latency and recovery budgets before execution. |
| Dec 14 to Jan 14 | 2027-01-14 | [M27: Improve measured performance](https://github.com/krabka-io/krabka-broker/milestone/27) | Investigate preallocation and RF=3 latency tails with profiles. Publish repeated before/after throughput, p99 latency, CPU and memory results for exact revisions and identical durability settings. Reject correctness regressions and changes outside predeclared performance budgets. |
| Jan 14 to Feb 14 | 2027-02-14 | [M28: Qualify upgrades and recovery](https://github.com/krabka-io/krabka-broker/milestone/28) | Extend existing qualification for loaded upgrades, supported rollback, certificate rotation and reassignment. Recover a secured cluster after source data loss, reconcile records and offsets, and report RPO/RTO for exact image digests. |
| Feb 14 to Mar 14 | 2027-03-14 | [M29: Test adoption with independent operators](https://github.com/krabka-io/krabka-broker/milestone/29) | Run two pilot workloads with different access patterns. Operators install, migrate and complete a recovery drill without maintainer intervention. Record and verify pilot fixes. If external pilots are unavailable, use independent acceptance runs and label the evidence accordingly. |
| Mar 14 to Apr 14 | 2027-04-14 | [M30: Qualify the release and publish operating limits](https://github.com/krabka-io/krabka-broker/milestone/30) | Repeat compatibility, recovery and performance checks on the release candidate. Publish a support matrix, measured capacity limits, upgrade instructions and known limitations. Bind evidence to shipped artifacts and close agreed release blockers before promotion. |

### Status on 2026-10-20

The counts come from the GitHub milestones. A closed issue records finished
scope. It does not qualify a later candidate. The
[support matrix](operations/support-matrix.md) gives the evidence for each
supported combination and the revision it ran at, and
[Release candidate qualification](release-candidate-qualification.md) lists
the release blockers.

| Milestone | Open | Closed | Done | Still open |
| :--- | ---: | ---: | :--- | :--- |
| [M24](https://github.com/krabka-io/krabka-broker/milestone/24) | 4 | 6 | The month-one baseline record at `3cf42388`, with a green `main` run. Fetch limits, replication throttles and RE2J quoting are fixed. The nightly upgrade lane passes on `main` again, at `e0588325`. | [#1344](https://github.com/krabka-io/krabka-broker/issues/1344), [#1352](https://github.com/krabka-io/krabka-broker/issues/1352), [#1388](https://github.com/krabka-io/krabka-broker/issues/1388), [#1389](https://github.com/krabka-io/krabka-broker/issues/1389). #1389 waits on the RF3 benchmark rerun for #1352. |
| [M25](https://github.com/krabka-io/krabka-broker/milestone/25) | 0 | 13 | Live broker configuration is verified, and the authentication and admission follow-ups are fixed. | None. |
| [M26](https://github.com/krabka-io/krabka-broker/milestone/26) | 1 | 17 | The sustained-load contract, its judge and its evidence harness; the KIP-835 quorum metrics; the failover, tiered-floor, remote-band Fetch and RLMM defects the soak found; the four-hour soak passes again; the `netem` fault has run. | [#1551](https://github.com/krabka-io/krabka-broker/issues/1551), the 72-hour run. A host is available. |
| [M27](https://github.com/krabka-io/krabka-broker/milestone/27) | 3 | 0 | The regression-budget machinery and the first profiling fixes are merged, measured on a VM only. | [#1370](https://github.com/krabka-io/krabka-broker/issues/1370), [#1394](https://github.com/krabka-io/krabka-broker/issues/1394), [#1395](https://github.com/krabka-io/krabka-broker/issues/1395). All three need benchmark-host runs. |
| [M28](https://github.com/krabka-io/krabka-broker/milestone/28) | 2 | 5 | The loaded upgrade and rollback passed on release images at `948e3cc8`. The secured DR drill runs under a declared load, and five restore defects it found are fixed. | [#1396](https://github.com/krabka-io/krabka-broker/issues/1396), [#1397](https://github.com/krabka-io/krabka-broker/issues/1397). No DR drill has passed end to end: the one at `f7d824ab` judged PASS and failed on [#1524](https://github.com/krabka-io/krabka-broker/issues/1524), since fixed. |
| [M29](https://github.com/krabka-io/krabka-broker/milestone/29) | 2 | 17 | Both pilot acceptance plans are written, and the doc gaps they found and the defects an agent's dry run found are fixed. | [#1398](https://github.com/krabka-io/krabka-broker/issues/1398), [#1399](https://github.com/krabka-io/krabka-broker/issues/1399). They wait on named operators. |
| [M30](https://github.com/krabka-io/krabka-broker/milestone/30) | 2 | 0 | The support matrix and the measured envelope are published with their gaps, and the matrix is current as of 2026-10-20. | [#1400](https://github.com/krabka-io/krabka-broker/issues/1400), [#1401](https://github.com/krabka-io/krabka-broker/issues/1401). No candidate is frozen. |

### Issues by milestone

- **M24:** [#621](https://github.com/krabka-io/krabka-broker/issues/621), [#1301](https://github.com/krabka-io/krabka-broker/issues/1301), [#1339](https://github.com/krabka-io/krabka-broker/issues/1339), [#1340](https://github.com/krabka-io/krabka-broker/issues/1340), [#1343](https://github.com/krabka-io/krabka-broker/issues/1343), [#1344](https://github.com/krabka-io/krabka-broker/issues/1344), [#1352](https://github.com/krabka-io/krabka-broker/issues/1352), [#1388](https://github.com/krabka-io/krabka-broker/issues/1388), [#1389](https://github.com/krabka-io/krabka-broker/issues/1389).
- **M25:** [#1342](https://github.com/krabka-io/krabka-broker/issues/1342), [#1345](https://github.com/krabka-io/krabka-broker/issues/1345), [#1390](https://github.com/krabka-io/krabka-broker/issues/1390), [#1391](https://github.com/krabka-io/krabka-broker/issues/1391), [#1421](https://github.com/krabka-io/krabka-broker/issues/1421), [#1422](https://github.com/krabka-io/krabka-broker/issues/1422), [#1423](https://github.com/krabka-io/krabka-broker/issues/1423), [#1424](https://github.com/krabka-io/krabka-broker/issues/1424), [#1426](https://github.com/krabka-io/krabka-broker/issues/1426), [#1427](https://github.com/krabka-io/krabka-broker/issues/1427), [#1428](https://github.com/krabka-io/krabka-broker/issues/1428), [#1429](https://github.com/krabka-io/krabka-broker/issues/1429), [#1430](https://github.com/krabka-io/krabka-broker/issues/1430).
- **M26:** [#1341](https://github.com/krabka-io/krabka-broker/issues/1341), [#1392](https://github.com/krabka-io/krabka-broker/issues/1392), [#1393](https://github.com/krabka-io/krabka-broker/issues/1393), [#1442](https://github.com/krabka-io/krabka-broker/issues/1442), [#1444](https://github.com/krabka-io/krabka-broker/issues/1444), [#1446](https://github.com/krabka-io/krabka-broker/issues/1446), [#1448](https://github.com/krabka-io/krabka-broker/issues/1448), [#1449](https://github.com/krabka-io/krabka-broker/issues/1449), [#1450](https://github.com/krabka-io/krabka-broker/issues/1450), [#1453](https://github.com/krabka-io/krabka-broker/issues/1453), [#1458](https://github.com/krabka-io/krabka-broker/issues/1458), [#1462](https://github.com/krabka-io/krabka-broker/issues/1462), [#1469](https://github.com/krabka-io/krabka-broker/issues/1469), [#1477](https://github.com/krabka-io/krabka-broker/issues/1477), [#1481](https://github.com/krabka-io/krabka-broker/issues/1481).
- **M27:** [#1370](https://github.com/krabka-io/krabka-broker/issues/1370), [#1394](https://github.com/krabka-io/krabka-broker/issues/1394), [#1395](https://github.com/krabka-io/krabka-broker/issues/1395).
- **M28:** [#1396](https://github.com/krabka-io/krabka-broker/issues/1396), [#1397](https://github.com/krabka-io/krabka-broker/issues/1397).
- **M29:** [#1398](https://github.com/krabka-io/krabka-broker/issues/1398), [#1399](https://github.com/krabka-io/krabka-broker/issues/1399).
- **M30:** [#1400](https://github.com/krabka-io/krabka-broker/issues/1400), [#1401](https://github.com/krabka-io/krabka-broker/issues/1401).

### Dependencies and release decisions

The sustained-load phase starts from the month-one correctness baseline.
Performance work uses the resulting failure and latency evidence. Loaded
upgrades and recovery use stable candidate artifacts. Operator trials follow
that qualification. The final release review uses all five phases.

Existing defect issues own their fixes. The new qualification issues own test
contracts, executed evidence and acceptance results. Link each confirmed gap
to its implementation issue instead of duplicating the work.

Set recovery and latency budgets before each run. Reconcile acknowledged
records and committed offsets within the declared failure contract. Record
exact revisions, image digests, commands, raw results and unresolved blockers.
Historical reports do not count as fresh candidate results.

Target two pilot workloads with different access patterns. If external pilots
are unavailable, use independently operated acceptance runs. Label the results
accordingly. Operators should complete installation and a recovery drill
without maintainer intervention before acceptance.

The release candidate needs current compatibility, recovery and performance
results. Publish its support matrix, measured operating limits, upgrade paths,
rollback restrictions and known limitations. Close agreed release blockers
before promotion. Follow the [release process](releasing.md) and keep broker
release claims separate from [ecosystem qualification](qualification.md).

## Deferred

- Direct I/O remains an investigation in
  [#1333](https://github.com/krabka-io/krabka-broker/issues/1333).
  Adopt it only if measurements justify the complexity.
- Broad new protocol features wait for measured needs or pilot requirements.
- Reuse the existing operator and qualification harnesses. A replacement
  operator or a second load framework is outside this plan.

Routine dependency maintenance continues outside the dated milestones in
[#178](https://github.com/krabka-io/krabka-broker/issues/178).

## How a milestone starts

Read the current code, tests, CI jobs and documents before accepting a gap.
Each new defect needs a source citation or a reproduction. Planned validation
issues name the existing harness, their test contract and completion criteria;
they do not assert an unverified defect.

At each monthly review, record completed evidence, unresolved blockers and
scope changes in GitHub. Update this page when milestone scope or dates change.
