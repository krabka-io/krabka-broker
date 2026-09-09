# Ecosystem qualification

[`qualification/milestone-20.json`](../qualification/milestone-20.json) is the
one candidate set. It records the exact revision of every repository that can
change the installed stack, the artifacts and supported architecture, and the
four broker-owned acceptance results. `draft` is intentionally not a release
claim.

Update the manifest only from completed sibling runs. An image or chart entry
uses the digest of the bytes the run exercised, never a tag. Each gate evidence
entry links one retained bundle and records that bundle's SHA-256. The bundle
contains the commands, Kubernetes events, logs, acknowledged-offset ledger,
queries, recovery durations and exact image references required by its issue.

```sh
aspect check-qualification
aspect check-qualification --final
```

The first command runs in ordinary CI and checks the contract. The second
refuses a pending gate, mutable artifact, or unpublished report; the manually
dispatched `ecosystem qualification` workflow also proves every recorded Git
commit and OCI digest still resolves. A successful workflow artifact is the
qualification result consumed by release review. It does not publish sibling
artifacts or replace their own release workflows.
