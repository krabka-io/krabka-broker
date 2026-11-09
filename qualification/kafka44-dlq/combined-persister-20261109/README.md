# Combined-tree persister transport-failure rerun (2026-11-09)

Fresh broker and formatter built with `cargo build --locked` from `86afb6fdd`.
Only the qualification harness changed: unique container names avoid collisions
with retained runs; offline verification accepts the observed oracle count of
one or two without weakening native replay or six replica-state assertions.

The corrected runner completed end-to-end, exit 0, on fresh private loopback
RF3/minISR2 clusters. Native recovered two intact DLQ copies; oracle recovered
one. Both reported start offset 0 and lag 0 after 15-second empty polling.
The offline verifier passed real failure-before-kill timelines and all six
final ARCHIVED/start0/deliveryCompleteCount1 replica states.

See [receipt](receipt.json), [verification](verification.txt), [runner output](live.log)
and [binary hashes](binaries.sha256). All 21 earlier extension source/evidence
hashes matched; the Java suite passed all 10 methods again ([output](java.log)).
The original build failed because four cc dependency files were absent in the
shared Cargo cache. They were restored from the Cargo.lock-checksum-verified
cached archive without replacing existing files; the locked build then passed.
Full build logs and private cluster data remain at the receipt artifact path.
Owned test processes and containers were stopped; their data was retained.

## Reproduction

Run the canonical `../persister-failure-20261108/run.py` with fresh private
`--run`, released Kafka 4.4 `--libs`, and the recorded combined broker/formatter
binaries. Then run its `verify.py` against that run directory. Copies of the
exercised scripts and generated fixture are retained here with hashes.

## Limits

This closes the stale-binary/corrected-harness end-to-end gap for this schedule.
Native connection/API negotiation failure and oracle WriteShareGroupState
request timeout are different transport cut points. A timeout can still commit:
one versus two copies is not parity or exactly-once proof. No negative state-write
response injection, disk EIO, physical power cut, isolated controller fault,
or exhaustive distributed scheduling is qualified. No live Java-plugin deployment
is exercised. Prior broker-library test counts are retained evidence, not fresh
Rust suite reruns. No push, merge, release or production change was performed.
