# WORM manifest seal failures

**Alert:** `KrabkaWormSealFailures`,
`rate(krabka_broker_worm_manifest_seal_failures_total[5m]) > 0`.

## What it means

A segment copy into the write-once archive finished without a usable manifest.
The copy leaves the segment in `CopySegmentStarted` rather than
`CopySegmentFinished`, so the remote log metadata never publishes it and the
tiered read path cannot serve it. The manifest chain stops at the last sealed
link, so `worm-verify` can no longer attest anything written after that point.

`krabka_broker_worm_manifests_sealed_total` flat while the failure counter
climbs confirms the chain is not advancing. Both counters carry no labels; the
broker log names the partition and the backend error.

## Confirm

```
rate(krabka_broker_worm_manifest_seal_failures_total[5m])
rate(krabka_broker_worm_manifests_sealed_total[5m])
```

The second at zero while the first is positive means every copy attempt is
failing, not a subset.

## Diagnose

1. Find the backend error in the broker log next to the failing partition. A
   write rejected by the object store, a signing key the broker cannot read,
   and a chain head that does not match the previous manifest all surface
   here and all read differently.
2. A signing key rotation that only replaced the key without registering the
   new key id breaks the seal on the first copy after the rotation. Check the
   archive's key configuration against the key ids `worm-verify --key-id`
   accepts.
3. A chain-head mismatch means another writer sealed a manifest for the same
   archive prefix. Two brokers writing one prefix is a misconfiguration, not
   a transient fault.

## Fix

- Restore the object store's write path first; the copier retries the segment
  on its next tick and the chain resumes from the last sealed link.
- After a key rotation, register the new key id with the archive and re-run
  `worm-verify` with both the old and the new key pair, so the half of the
  chain on each side of the rotation verifies.
- For a chain-head mismatch, stop the second writer before letting the copier
  retry. A retry against a forked chain seals a link nothing can verify.

## Escalate

A chain that stays unsealed past the archive's retention window loses the
attestation for those segments permanently. Escalate before the window closes
rather than after.
