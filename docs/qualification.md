# Ecosystem qualification

[`qualification/milestone-20.json`](../qualification/milestone-20.json) is the
completed historical candidate. Keep its revisions, evidence and report intact.
Schema 1 describes that historical four-gate result. Schema 2 also binds newly
executed checks to a candidate and the exact qualification adapters, and
requires the Milestone 22 disaster-recovery gate owned by issue 555.

## Run a subsequent candidate

The `ecosystem qualification` workflow accepts a repository-relative `manifest`
and an `execute` switch. With `execute=false`, it verifies the existing published
result and its hashes. With `execute=true`, it creates a fresh draft from that
component set, runs five independent jobs, and publishes a qualification release
only after every job succeeds.

Supply both `broker_revision` and `broker_digest` to qualify a newly delivered
broker using the same sibling set. The prepare job applies those overrides,
freezes the complete input, verifies revisions against each recorded default
branch, and distributes that exact input to every job. This needs no candidate
commit after the delivery image has been published. To change sibling revisions
or artifacts, provide a reviewed manifest containing that full candidate set.

Until the first M22 delivery and five-gate release exist, the historical M20
manifest remains the bootstrap seed and a run must supply the delivered M22
broker revision and digest. After that evidence is published, check in its
qualified manifest and use it as the scheduled baseline; do not invent a draft
manifest or mutable image tag to make the schedule appear green.

```sh
gh workflow run qualification.yml \
  -f manifest=qualification/milestone-20.json \
  -F execute=true \
  -f broker_revision="$DELIVERED_COMMIT" \
  -f broker_digest="$DELIVERED_IMAGE_DIGEST"
```

The broker digest must come from the successful delivery run for that commit.
The qualification workflow exercises those bytes; release promotion separately
verifies the delivery record. Do not infer a delivery digest from a mutable tag.

The matrix has `fail-fast: false`. GitHub's **Re-run failed jobs** retries only
failed gates with the original uploaded candidate; successful gates remain
available. Re-running the entire workflow creates a new candidate timestamp and
reruns every gate. The jobs require Linux amd64, Docker, and sufficient capacity
for the existing stack; installation and lifecycle also install pinned Helm and
Kind. Their timeouts bound the run rather than turning a timeout into a pass.

## What runs

| Gate | Executed boundary |
| :--- | :--- |
| Installation | The pinned public Compose and Helm recipes with candidate images and downloaded, checksum-verified charts; produce/readback and uninstall. The existing Go and Java integration suites run against the installed broker, and the pinned broker's RF=3 schema suite plus incompatible-protocol build probe rerun the M19 supporting checks. |
| Operator lifecycle | The pinned operator `packaging/kind-lifecycle.sh`, using published chart contents and the candidate broker, operator and rebalancer images; upgrade, disruption, scale-down, certificate rotation and acknowledged-record reconciliation. |
| Authenticated CLI | The pinned CLI's real `candidate_broker` test, against a disposable three-broker SASL cluster. Its one executable lookup is adapted in the isolated checkout to invoke the downloaded CLI binary instead of a rebuilt binary; the patch is retained. |
| Observability recovery | The pinned demo Compose recipe with explicit candidate broker/o11y overrides, followed by its `qualify-failover.sh`; signal queries, WAL recovery, offset reconciliation and alert firing/resolution. |
| Disaster recovery | A TLS and SASL/SCRAM-SHA-512 RF=3 cluster created from the candidate broker image, a locked MinIO archive, and the image's bundled backup, restore, and WORM verifier. The job captures offsets and configuration at an explicit boundary, destroys every source data directory, restores a fresh cluster, reconciles records at their original offsets, settings, and consumer positions, and records measured RPO/RTO. Tampered, missing, untrusted-head, invalid-certificate, invalid-credential, and denied-ACL probes must fail without a complete capture. Classic and diskless topics are both required. |

The lifecycle harness expects a local operator tag. Its private alias is checked
against the pulled candidate image ID before execution. It packages source
charts as ancillary evidence, but installation uses the verified downloaded
chart contents copied into the isolated harness tree. No user checkout is
modified. Supporting source tests remain distinguishable from installed-image
checks in the command log.

A test process exiting successfully is insufficient: adapters check named live
test results or behavioral outcomes and record the checks that actually ran.
Skipped or zero-test runs cannot produce a valid gate receipt. Historical M19
recovery/CDC evidence remains historical; this workflow does not relabel those
old runs as fresh candidate results. The monthly schedule is enabled only after
the first schema-2 M22 manifest is checked in as the immutable baseline.

## Local execution and evidence

```sh
aspect check-qualification --new-candidate qualification/candidate.json
# Edit candidate.json to name the intended immutable revisions and artifacts.
aspect check-qualification --manifest qualification/candidate.json
aspect check-qualification --manifest qualification/candidate.json \
  --gate cli-admin --output qualification-run-1
```

Every gate invocation uses a new temporary checkout. Choose a fresh `--output`
for retries; existing evidence is never silently replaced. Failed attempts keep
their command log and scratch directory for diagnosis. Successful receipts name
the gate, candidate SHA-256, adapter SHA-256, executed check count and names,
command hash, and log hash. The candidate identity covers its timestamp,
architecture, all repository revisions/default branches, and artifact
references/digests. Reordering components or adding output evidence does not
change that identity.

Run all five gates into the same output directory, then assemble:

```sh
aspect check-qualification --manifest qualification/candidate.json \
  --assemble --output qualification-run-1 \
  --publish-prefix https://github.com/krabka-io/krabka-broker/releases/download/qualification-RUN_ID
aspect check-qualification --manifest qualification-run-1/manifest.json --final
```

Assembly verifies every receipt and retained log/command hash before writing a
qualified manifest, report and per-gate bundles. It rejects another candidate's
receipt, missing gates, failed commands and zero executed checks. Local assembly
does not publish anything. The workflow uploads the assembled artifacts to a
draft `qualification-RUN_ID` release, downloads them to verify checksums, then
publishes it as a prerelease without changing `latest`. Published qualification
releases are never overwritten by retries.
