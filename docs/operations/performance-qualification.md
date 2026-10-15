# Performance qualification

The performance qualification compares one exact Krabka revision with Apache
Kafka 4.0.0, then measures Krabka's partition envelope. It is a workload
result, not a claim that either broker is universally faster or that the
largest passing tier is a supported limit.

Run the full protocol from a clean Linux host with at least 16 CPUs, 32 GiB of
available RAM, 40 GiB of free disk, and `fs.inotify.max_user_instances` set to
at least 512:

```sh
PERF_ARTIFACT_DIR=$PWD/performance-artifacts \
  packaging/performance/qualify.sh full
```

The harness creates and removes a dedicated four-worker Kind cluster. Set
`PERF_KEEP_CLUSTER=1` to retain it after a failure. `smoke` uses 30 and 100
partitions, one short comparison run and the same failure paths; it validates
the machinery but produces no publishable performance result.

## Fixed comparison contract

Both brokers run as three combined broker/controller pods. Each broker gets a
2 CPU limit, 2 GiB memory limit and a 100 GiB volume claim. The same Kafka 4.0.0
client image drives twelve partitions at replication factor 3,
`min.insync.replicas=2`, `acks=all`, idempotence enabled, LZ4 compression, a
65,536-byte batch and 5 ms linger. Each side gets three saturation runs and
three fixed-rate steady-state runs. Every run reports end-to-end p50/p95/p99
latency, and the producer and consumer counts must agree or the harness fails.

The output keeps exact Git and image revisions, host resources, manifests,
producer and consumer reports, end-to-end p50/p95/p99 latency, and process CPU,
RSS, file-descriptor, disk and network snapshots. Publish the directory
unchanged so a summary remains traceable to its raw inputs.

## Partition envelope contract

The full run starts a fresh four-broker Krabka cluster for each tier. It must
pass 1,000 partitions and attempts 10,000, always at replication factor 3. A
fixed-rate producer and consumer stay active while the controller leader is
deleted, its replacement is observed, the old pod becomes ready again, and up
to 1,000 partitions are reassigned across all four brokers. Each tier has a
30-minute deadline.

For each broker the evidence records total hosted replicas, metadata lag,
process RSS, file descriptors, disk and network use, and metrics scrape time,
bytes and series count. `scale/verdict.txt` names the highest passing tier;
failure of the 10,000-partition attempt is evidence, not permission to describe
1,000 as a product limit.

## Regression budget mode

`qualify.sh budget before_after <reference-revision>` and `qualify.sh budget
pinned_kafka` run the predeclared RF=3 workload matrix in interleaved rounds
and judge the candidate against fixed budgets. A correctness failure always
rejects. [Performance regression budgets](performance-budgets.md) defines the
matrix, the results record and the decision rule.

## Sustained-load contract

This qualification measures throughput and the partition envelope over runs of minutes. Correctness over days, under injected faults, is a separate contract. [`bench/soak/rf3-72h.yaml`](../../bench/soak/rf3-72h.yaml) declares the 72-hour RF=3 soak before it runs: host and disk, seed, client settings, throughput floors, p50/p99/p999 budgets for each path, recovery deadlines, and a seeded schedule of bounded pauses, restarts, slow followers and disk fills. The `soak_contract` suite of `krabka-broker` executes a contract against an isolated three-broker cluster, and `krabka-soak-check` reconciles the evidence. The [design document](../../crates/bench-driver/docs/soak-contract-design.md) gives the rules.

```sh
bazel run -c opt //packaging:image_load
KRABKA_SOAK_CONTRACT=$PWD/bench/soak/rf3-72h.yaml \
KRABKA_SOAK_EVIDENCE_DIR=$PWD/soak-evidence \
  cargo test -p krabka-broker --test soak_contract -- --ignored --nocapture
krabka-soak-check judge --contract bench/soak/rf3-72h.yaml \
  --evidence soak-evidence/evidence.json --out soak-evidence/verdict.json
```

Without `KRABKA_SOAK_CONTRACT` the suite runs `bench/soak/self-check.yaml`, a five-minute run that proves the judge reports an injected missing record and an injected missed deadline. The container matrix in CI runs it.
