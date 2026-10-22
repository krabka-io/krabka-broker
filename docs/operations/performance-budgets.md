# Performance regression budgets design

This document defines how a candidate krabka build is compared with a reference build or with pinned Apache Kafka, and when the comparison rejects the candidate.

A published number applies to one contract on one host. This design gives the contract a name, a fixed workload matrix and fixed budgets. It records every raw trial. It rejects a correctness failure in every case. It does not rank brokers, and it does not define a supported limit.

## Design Goals

- **Budgets come before numbers.** The materiality and variance budgets are in a checked-in contract file. The harness records the SHA-256 of that file before the first trial. The evaluator refuses a record whose digest is not the digest of the contract that it gets. If a budget changes after a run, that run cannot be judged against the new budget.
- **One regression engine.** The nightly criterion lane in `ci.yml` already compares four alternating reference and candidate runs. It uses medians, three times the larger median absolute deviation, and a 10% floor. The broker lane uses the same functions in `krabka-parse-benches`: `median`, `relative_mad` and `calibrated_tolerance`. There is no second statistics implementation.
- **Correctness always rejects.** A trial that loses, duplicates or fails to send a record rejects the comparison in every mode. This applies to both sides: a reference that loses data makes the comparison invalid.
- **Noise is not a verdict.** If one side's spread is larger than the variance budget, the cell is inconclusive and the comparison rejects. A change that is larger than the materiality floor but inside the observed spread is reported as noisy. It does not reject.
- **Limits of the tested contract only.** Every report starts with the contract name and the exact builds. It names the reference and the candidate by revision and image digest, not by product. A comparison with Kafka reports measured differences for this contract. It does not claim that one broker is faster in general.
- **Reproducible from the artifact.** The record holds the reference and candidate revisions, image digests, the client image digest, the host resources, the harness settings and every raw trial. The Markdown report is rendered from the record and does not add data.

## Architecture Overview

```text
packaging/performance/regression-contract.json      (checked in, digest recorded first)
        |
        v
packaging/performance/qualify.sh budget <mode> [reference]
        |   for each round: for each side (first side alternates): fresh 3-broker cluster
        |       for each cell: create topic, snapshot /proc, run BrokerPerformanceWorkload, snapshot /proc
        v
<artifact-dir>/  contract.json  contract.sha256  provenance.env  host.env  trials/round-<r>/<side>/<cell>/
        |
        v
krabka-parse-benches assemble   -> performance-record.json   (the results record)
krabka-parse-benches evaluate   -> verdict.json + performance-budget.md
```

`qualify.sh` drives a Kind cluster on the measured host. `BrokerPerformanceWorkload.java` is the client. It produces and consumes every record, and it checks that the counts agree. `krabka-parse-benches` is the Rust tool that reads the artifacts, applies the budgets and renders the report. Only the Rust tool makes decisions. The shell script only runs trials and writes files.

### Workload matrix

Every cell runs against three combined broker/controllers. Each cell gets a fresh topic with replication factor 3 and `min.insync.replicas=2`. The producer uses `acks=all`, idempotence, 65,536-byte batches and a 5 ms linger. Each broker has a 2 CPU, 2 GiB and 100 GiB claim. These are the settings of the existing [performance qualification](performance-qualification.md) and the [Kafka 4.3.1 experiment](performance-kafka-4.3.1.md), so earlier results stay comparable.

| Cell | Record | Payload | Compression | Partitions | Load | What it isolates |
| :--- | ---: | :--- | :--- | ---: | :--- | :--- |
| `1k-random-lz4-p12-saturate` | 1 KiB | random | LZ4 | 12 | 3,000,000 records, unlimited | The headline RF=3 throughput and CPU per record. |
| `100b-random-lz4-p12-saturate` | 100 B | random | LZ4 | 12 | 10,000,000 records, unlimited | Per-record overhead: framing, parsing and request handling. |
| `1k-zeros-lz4-p12-saturate` | 1 KiB | zeros | LZ4 | 12 | 3,000,000 records, unlimited | Highly compressible traffic: decompression and record validation. |
| `1k-random-none-p12-saturate` | 1 KiB | random | none | 12 | 3,000,000 records, unlimited | Uncompressed bytes: copy and replication bandwidth. |
| `100k-random-lz4-p12-saturate` | 100 KiB | random | LZ4 | 12 | 30,000 records, unlimited | Large records. |
| `1k-random-lz4-p1-saturate` | 1 KiB | random | LZ4 | 1 | 1,000,000 records, unlimited | One hot partition: one leader and one replication stream. |
| `1k-random-lz4-p12-fixed-20k` | 1 KiB | random | LZ4 | 12 | 600,000 records at 20,000/s | Latency at a rate that the cluster sustains. |
| `1k-random-lz4-p1000-fixed-5k` | 1 KiB | random | LZ4 | 1,000 | 300,000 records at 5,000/s | Many partitions with small per-partition batches. |
| `1k-random-lz4-p12-idle-burst` | 1 KiB | random | LZ4 | 12 | 120 s idle, then 500,000 records at 50,000/s | Idle to burst: a long-polling consumer and idle brokers, then a burst. |

Random payloads come from a seeded 4 MiB pool, so compression cannot shrink them. Every value carries its send timestamp and a unique sequence number. The consumer uses these to measure end-to-end latency and to detect lost and duplicate records.

### Results record

`performance-record.json` is the machine-readable result. It has `format_version: 1`. It holds these fields:

- The contract name and the contract SHA-256 that the harness recorded before the first trial.
- The mode: `before_after` or `pinned_kafka`.
- For each side, a label, the revision, the image reference and the image digest. A `before_after` reference is an earlier git revision built with the same `-c opt` image rule. A `pinned_kafka` reference is the contract's Kafka image, pinned by digest.
- The client image and its digest. The pinned Kafka 4.3.1 jars drive both sides in every mode.
- The host: CPU model, logical CPUs, memory, free disk, kernel and `CLK_TCK`.
- Every other harness setting from `provenance.env`, such as the source patch digest and the broker resource limits.
- Every raw trial: cell, round, side, position in the run, and either a measurement or a failure reason. A measurement holds the sent, consumed, duplicate and error counts, the wall time, records/s, MiB/s, p50/p95/p99 latency, broker CPU seconds and peak RSS.

CPU is the sum over every broker of the `utime + stime` change in `/proc/<pid>/stat` during the trial, divided by acknowledged records. Peak memory is the sum over every broker of `VmHWM` after the trial. `VmHWM` is the peak since the broker process started. Each side starts a fresh cluster for every round and runs the cells in contract order, so the two sides have the same memory history at every cell. Each snapshot also records the process start time from `/proc/<pid>/stat`. If a broker process restarts during a trial, its start time changes, even though the pod keeps its name. The trial then fails, because its numbers do not describe one process.

`verdict.json` holds the verdict: per cell and per metric, the two medians, the change, both spreads, the allowed change and a status. It also lists every rejection. `performance-budget.md` is the published page that the tool renders from the record and the verdict.

### Budgets and the decision rule

For each cell and each budgeted metric, take the median of each side over the interleaved rounds. The change is the candidate median relative to the reference median, with the sign set so that a positive change is worse. For throughput, a lower value is worse. For latency, CPU and memory, a higher value is worse. The spread of a side is its median absolute deviation relative to its median.

| Status | Condition | Rejects |
| :--- | :--- | :--- |
| excess variance | the spread of a side is larger than the variance budget | yes, in every mode |
| material regression | the change is larger than the materiality floor **and** larger than the spread multiplier times the larger spread | in `before_after` |
| noisy, not material | the change is larger than the floor, but not larger than the spread allows | no |
| improved | the improvement is larger than the allowed change | no |
| within budget | the change is not larger than the floor | no |

A correctness failure, a missing trial, or two consecutive rounds that ran the same side first also reject. A rejected cell reports no performance numbers.

| Metric | Materiality | Spread multiplier | Variance budget |
| :--- | ---: | ---: | ---: |
| records/s | 5% | 3 | 10% |
| p99 latency | 10% | 3 | 25% |
| CPU µs/record | 5% | 3 | 10% |
| peak RSS | 10% | 3 | 10% |

The contract runs five rounds. Throughput, CPU and memory are budgeted in every cell. p99 latency is budgeted only in the fixed-rate and idle-to-burst cells. In a saturating cell, p99 measures the producer's own queue, so it follows throughput and has no budget there. Every cell records p50, p95 and p99 latency.

## Key Design Decisions

### Extend the criterion regression lane, do not add a second one

The criterion lane already had the needed rule: compare medians, allow the larger of a floor and three times the larger relative MAD, and fail on invalid samples. The broker lane adds a direction per metric, a variance budget and correctness checks. It calls the same `calibrated_tolerance` function. A second engine could give a different verdict on the same numbers, so this design keeps one.

The median and the MAD resist one outlier. The 2026 qualification run had one steady-state Kafka trial with a 212 ms p99 among trials near 6 ms. With five rounds, one such trial moves neither the median nor the MAD much. A mean and a standard deviation would move a lot. The rejected alternative was a fixed percentage. A fixed percentage is too strict on a noisy host and too loose on a quiet host.

### Record the contract digest before the first trial

A budget that someone can edit after the run does not limit anything. The harness copies the contract into the artifact directory and records its SHA-256 before it builds or starts anything. `assemble` refuses a copy that does not match that digest. `evaluate` gets the checked-in contract and refuses a record with a different digest. To change a budget, you change the contract file in a pull request and run the comparison again. The contract name contains a version (`rf3-regression-v1`), so a report always says which budgets judged it.

### Interleave whole clusters, and alternate the first side

Host drift is the main source of systematic error. Examples are thermal state, page-cache state, and other tenants. Each round starts a fresh cluster for each side and runs every cell. Odd rounds run the reference first and even rounds run the candidate first, so drift does not always fall on the same side. The evaluator checks this order from the recorded trial positions. A fresh cluster per round also gives each side the same history at every cell. The cost is ten cluster starts for five rounds. The alternative was one long-lived cluster per side. It is cheaper, but the second side would always run on a host that the first side had changed.

### Correctness failures reject on both sides, in every mode

The workload checks that every acknowledged record is consumed once. A broker that is fast because it loses records is not faster. A failed reference trial means the comparison has no valid baseline. So both cases reject, and the report shows the failure reason from the workload's own log line. A missing trial directory also rejects, because the harness stopped and the evidence is incomplete.

### A Kafka comparison reports differences and does not gate them

Two krabka revisions with the same contract must agree within budget. That is a regression test. A krabka-to-Kafka difference is a fact about this contract on this host, and the 4.3.1 experiment shows that it moves in different directions for throughput, CPU and memory. A parity budget would be a ranking claim, so the `pinned_kafka` mode has `gate_performance: false`. It still rejects correctness failures, missing trials and excess variance, because a page with noisy numbers is not publishable. The report says "material difference (not gating)" for such a cell.

### Measure CPU and memory from `/proc`, as the qualification already does

The existing qualification snapshots `/proc/<pid>/stat` and `/proc/<pid>/status` from a probe container that shares the broker's process namespace. This design adds `VmHWM` to the same snapshot. It uses the same source for Kafka (`java`) and krabka (`krabka-broker`). The alternative was cgroup `cpu.stat` and `memory.peak`, as in the 4.3.1 experiment. Their location and fields depend on the cgroup version of each Kind node. The `/proc` fields do not, and they give the same quantities for both brokers.

### Shared definitions with the sustained-load contract

The sustained-load and failure contract (#1392) declares host resources, client settings and budgets before its run, and it reconciles acknowledged records. It can use the same definitions: the `Host` record, the contract digest rule, and the correctness rule (sent and consumed equal the expected count, with no duplicates and no producer errors). Permitted retries and transactional visibility are specific to that contract. The record format has a version marker so that a later format can add them.

## Integration

### Running a comparison

The host needs what the [performance qualification](performance-qualification.md) needs: Kind, Docker, Bazel, `jq` and a JDK, at least 16 CPUs and 32 GiB of RAM. Compare the working tree with an earlier revision:

```sh
PERF_ARTIFACT_DIR=$PWD/budget-before-after \
  packaging/performance/qualify.sh budget before_after <reference-revision>
```

Compare the working tree with the contract's pinned Kafka image:

```sh
PERF_ARTIFACT_DIR=$PWD/budget-kafka \
  packaging/performance/qualify.sh budget pinned_kafka
```

The script builds the images, runs the rounds, assembles the record and evaluates it. It exits non-zero when the verdict rejects. The artifact directory then holds `performance-record.json`, `verdict.json` and `performance-budget.md` beside the raw trials. Publish the directory unchanged. To judge an existing record again, for example on another machine:

```sh
cargo run -p krabka-parse-benches -- evaluate performance-record.json \
  --contract packaging/performance/regression-contract.json \
  --output verdict.json --markdown performance-budget.md
```

### Components

- `packaging/performance/regression-contract.json` is the contract. `krabka-parse-benches check-contract` validates it and prints its digest. The crate's tests load it, so `bazel test //...` fails on an invalid contract.
- `packaging/performance/qualify.sh` runs the trials. Its `full` and `smoke` modes are unchanged.
- `packaging/performance/krabka.yaml` is the krabka cluster that both krabka sides run on: plaintext, three combined broker/controllers, 2 CPU, 2 GiB and a 100 GiB claim each. It is the `packaging/k8s` reference deployment as it stood before that deployment moved to TLS and a mounted config file, kept here so the measured setup does not change with the reference deployment. `packaging/performance/kafka.yaml` is the Kafka cluster.
- `packaging/performance/BrokerPerformanceWorkload.java` takes an optional tenth argument, the idle seconds before the first send.
- `krabka-parse-benches` holds the `contract`, `record`, `budget` and `render` modules beside the criterion lane.
- The `benchmarks` workflow validates the contract when it or the crate changes. A dispatch with `budget` set runs both comparisons, one after the other, on a 32-core cloud worker, and keeps each mode's artifact directory for 90 days. It measures the dispatched revision, so it calibrates the contract; candidate evidence comes from `qualification-profile.yml`.

## Kafka / KIP Compliance

The workload uses the Kafka 4.3.1 client jars from the pinned image for both sides. Kafka's producer and consumer therefore define the wire behavior that is measured, including idempotent production ([KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)). The design makes no change to the broker or to the wire protocol. The Kafka reference is `apache/kafka:4.3.1`, pinned by the digest that the 4.3.1 experiment recorded. The Kafka manifest gives it the same resource limits as krabka.

## Testing

`crates/parse-benches/tests/budget_tests.rs` tests the evaluator with tables. It covers: within budget, a material regression, a regression that does not gate against Kafka, a noisy change that is not material, excess variance in every mode, lost and duplicate records on either side, a failed trial, a missing trial, broken interleaving, a budget edited after the run, and structurally invalid records and contracts. It also writes a complete artifact directory in the harness layout as a recorded sample, then assembles, evaluates and renders it through the command-line entry points.

What these tests do not prove: no comparison has run on the benchmark host. The cluster steps of `qualify.sh budget` are checked with `bash -n` only. The budget values come from earlier results, not from a calibration run of this contract. The first real run must confirm that each cell's spread is inside its variance budget on the benchmark host. If it is not, a pull request must change the contract before any result is published.
