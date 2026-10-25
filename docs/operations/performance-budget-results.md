# Performance budget results

These are the measured runs of the RF3 regression contract that
[Performance regression budgets](performance-budgets.md) defines. A result
applies to its contract on the host it names. It is not a ranking of brokers
and not a supported limit.

## Results

The current contract is `rf3-regression-v3`. No run of it has completed yet.
Until a run on a frozen release candidate passes in both modes, release
blocker B8 stays open
([release-candidate qualification](../release-candidate-qualification.md)).

One run of the previous contract, `rf3-regression-v2`, completed in the
`pinned_kafka` mode and passed. Its verdict is valid under `rf3-regression-v2`
only: `evaluate` judges a record against the contract whose digest the record
holds. `rf3-regression-v3` changed one variance budget after the
`before_after` half of the same dispatch, as [Calibration](#calibration)
records.

| Date | Mode | Reference | Candidate | Verdict | Evidence |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 2026-10-23 | `pinned_kafka` | Apache Kafka 4.3.1, `sha256:77e3df90…6532837` | `main` at `fd007630`, `sha256:7e680015…c323d8` | Pass under `rf3-regression-v2` | [Run 39865512647](performance-budget-runs/2026-10-23-pinned-kafka-run-39865512647/performance-budget.md) |

`fd007630` is the merge of #1666, which changed only the contract and the
documents. It is not a release and not a candidate. All 90 trials consumed
every record that they sent, with no duplicates and no errors, on both sides.

### Measured differences from Kafka 4.3.1

The `pinned_kafka` mode reports differences and does not gate them
(`gate_performance: false`). These are the medians over five rounds, Kafka
first and krabka second. They apply to this contract on this host only.

| Cell | records/s | p99 ms | CPU µs/record | Peak RSS MiB, three brokers |
| :--- | ---: | ---: | ---: | ---: |
| `1k-random-lz4-p12-saturate` | 105,089 and 191,793 | 1,033.7 and 954.1 | 54.7 and 18.4 | 2,855 and 140 |
| `100b-random-lz4-p12-saturate` | 867,126 and 999,877 | 77.3 and 57.1 | 6.4 and 3.4 | 2,877 and 147 |
| `1k-zeros-lz4-p12-saturate` | 711,465 and 739,670 | 410.5 and 335.1 | 3.7 and 2.3 | 2,882 and 174 |
| `1k-random-none-p12-saturate` | 161,082 and 179,255 | 1,295.0 and 1,323.7 | 26.2 and 20.2 | 2,898 and 186 |
| `100k-random-lz4-p12-saturate` | 1,492 and 1,869 | 1,380.7 and 1,236.3 | 1,978 and 1,513 | 2,905 and 192 |
| `1k-random-lz4-p1-saturate` | 43,714 and 48,535 | 764.0 and 690.4 | 44.1 and 38.5 | 2,912 and 193 |
| `1k-random-lz4-p12-fixed-20k` | 19,994 and 19,993 | 22.2 and 8.1 | 59.7 and 54.0 | 2,921 and 195 |
| `1k-random-lz4-p1000-fixed-5k` | 4,999 and 4,999 | 125.9 and 38.8 | 414.0 and 406.2 | 3,256 and 252 |
| `1k-random-lz4-p12-idle-burst` | 49,808 and 49,737 | 114.3 and 149.7 | 146.4 and 132.0 | 3,287 and 271 |

One cell has a material difference against krabka: the idle-to-burst p99 is
31% higher than Kafka's, against an allowed 12.4%. The report marks it
"material difference (not gating)". p99 has no budget in the saturating
cells, because there it measures the producer's own queue.

## RF3 numbers for #1394

[#1394](https://github.com/krabka-io/krabka-broker/issues/1394) asks whether
the targeted fixes moved the RF3 tails. The rejected `before_after` run below
compares `v1.0.5` (`ac9deff3`) with `fd007630`. `fd007630` has
[#1457](https://github.com/krabka-io/krabka-broker/pull/1457), which deletes
retired segment files outside the log lock, and
[#1410](https://github.com/krabka-io/krabka-broker/pull/1410). `v1.0.5` has
neither. It also has every other change between the two commits, so the run
cannot attribute a difference to #1457 alone.

The contract rejected this comparison, so these medians are not a budget
result. They are indicative only:

| Cell | Metric | `v1.0.5` | `fd007630` | Budget status |
| :--- | :--- | ---: | ---: | :--- |
| `1k-random-lz4-p12-saturate` | records/s | 134,101 | 163,557 | Improved |
| `1k-random-lz4-p12-saturate` | CPU µs/record | 38.7 | 23.3 | Excess variance, 11.6% candidate spread |
| `1k-random-lz4-p12-saturate` | p99 ms | 575.5 | 1,046.7 | Not budgeted; the reference spread is 54% |
| `1k-random-lz4-p1-saturate` | records/s | 29,627 | 41,407 | Improved |
| `1k-random-lz4-p1-saturate` | p99 ms | 1,292.8 | 799.3 | Not budgeted |
| `1k-random-lz4-p12-fixed-20k` | p99 ms | 16.7 | 13.3 | Within budget; spreads 33% and 25% |
| `1k-random-lz4-p1000-fixed-5k` | p99 ms | 52.9 | 48.7 | Within budget |
| `1k-random-lz4-p12-idle-burst` | p99 ms | 482.8 | 210.3 | Improved |

The host is a 32-core `gcp-ubuntu-24-04-32core` cloud worker with three
2 CPU, 2 GiB brokers on Kind. It is not the AMD EPYC 4344P OMB host of
[#1352](https://github.com/krabka-io/krabka-broker/issues/1352), and it
does not run the OMB `max-rate-1-topic-1-partition` workload. These numbers do
not close B9, #1352 or
[#1370](https://github.com/krabka-io/krabka-broker/issues/1370). Those stay
with the maintainer, because they need that host.

## Calibration

A run whose spread exceeds a variance budget changes the contract before any
result of that run is published. That rule changed the contract twice. The
runs below are the calibration evidence. They publish no result.

| Date | Contract | Mode | Reference | Candidate | Verdict | Evidence |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2026-10-22 | `rf3-regression-v1` | `before_after` | v1.0.5, `ac9deff3` | `54c132a4` | Reject: excess variance in two cells | [Run 39756166298](performance-budget-runs/2026-10-22-before-after-run-39756166298/performance-budget.md) |
| 2026-10-22 | `rf3-regression-v1` | `before_after` | v1.0.5, `ac9deff3` | `bc037c1b` | Pass | [Run 39840381315](performance-budget-runs/2026-10-22-before-after-run-39840381315/performance-budget.md) |
| 2026-10-22 | `rf3-regression-v2` | `before_after` | v1.0.5, `ac9deff3` | `fd007630` | Reject: excess variance in one cell | [Run 39865512647](performance-budget-runs/2026-10-22-before-after-run-39865512647/performance-budget.md) |

The first two candidates are `main` plus harness changes only; neither changes
the broker. `54c132a4` is `main` at `c5725ed4` with the cloud budget dispatch
(#1649). `bc037c1b` is `main` at `3269fae8` with the harness's own krabka
manifest (#1664). `fd007630` is `main` with #1666. Every one of the 90 trials
in each run consumed every record it sent, with no duplicates and no errors.

### Spreads measured

A metric's spread is the relative median absolute deviation over the five
rounds. A cell is judged on the larger of its two sides' spreads. This is the
largest spread of each budgeted metric, over every cell of each
`before_after` run:

| Metric | v1, first run | v1, second run | v2, run 39865512647 | v1 budget | v2 budget | v3 budget |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| records/s | 12.1%, `1k-random-none-p12-saturate` | 9.2%, `100k-random-lz4-p12-saturate` | 10.1%, `1k-zeros-lz4-p12-saturate` | 10% | 15% | 15% |
| p99 latency | 40.2%, `1k-random-lz4-p12-idle-burst` | 9.3%, `1k-random-lz4-p1000-fixed-5k` | 33.0%, `1k-random-lz4-p12-fixed-20k` | 25% | 50% | 50% |
| CPU µs/record | 5.8%, `1k-random-none-p12-saturate` | 3.5%, `1k-random-lz4-p12-idle-burst` | 11.6%, `1k-random-lz4-p12-saturate` | 10% | 10% | 15% |
| peak RSS | 3.4%, `1k-zeros-lz4-p12-saturate` | 3.7%, `1k-random-lz4-p1000-fixed-5k` | 7.4%, `1k-zeros-lz4-p12-saturate` | 10% | 10% | 10% |

Each new budget is about 1.25 times the largest spread measured, rounded up to
a multiple of 5%. `rf3-regression-v2` set 15% for records/s and 50% for p99.
`rf3-regression-v3` sets 15% for CPU per record, from 1.25 times 11.6%. Each
change keeps every other budget, every materiality floor, every spread
multiplier, the cells and the rounds. A wider variance budget does not loosen
the regression test of a quiet cell. It lets a noisier cell be judged instead
of rejected, and that cell's allowed change is still three times its own
spread.

### Why the v2 `before_after` run failed

The job failed in its comparison step, because `evaluate` rejected the
verdict. The cause is variance on the host. It is not a correctness failure, a
missing trial, a material regression or a harness defect:

- All 90 trials completed, and the rounds alternated the first side.
- The one rejection is `1k-random-lz4-p12-saturate` CPU per record. The
  candidate's spread was 11.6%, against a 10% budget. The reference's spread
  was 3.1%.
- The candidate's CPU per record in that cell was 32.2 and 37.0 µs in rounds
  1 and 2, and 21.4, 20.6 and 23.3 µs in rounds 3 to 5. The same image, in
  the `pinned_kafka` half on the same worker, measured 18.0 to 19.5 µs in all
  five rounds.
- Single-round outliers came on both sides in other cells. The reference's
  CPU per record rose 35% to 60% in round 4 of four saturating cells,
  and 45% to 55% in round 5 of three other cells. The candidate's rose in round 2 of
  `1k-random-none-p12-saturate`, from 22 to 49 µs.
- The candidate was better than the reference in every budgeted metric of
  every cell, or within budget. The rejected cell improved by 39.8%.

The `pinned_kafka` half of the dispatch had no rejection, so it is the
result above. Under `rf3-regression-v3`, the `before_after` comparison must
run again. A record cannot be judged against a contract with another digest.

### Contract and host

The two v1 runs recorded `rf3-regression-v1`, SHA-256
`88125f23e83026670713516c73f401d80cced33577e1a4244b33b3cac83e4d17`, before
their first trial. Both halves of run 39865512647 recorded
`rf3-regression-v2`, SHA-256
`038d48e5ff398467a7ce5f97bb239f8378e21102e7113344693c65df725eeded`.
`rf3-regression-v3` is SHA-256
`120eddd2b4bd224a65baf42b218d11e984d86390d8620750d4b690cddf3f3f01`.

Every run used the `benchmarks.yml` `performance-budget` job on a
`gcp-ubuntu-24-04-32core` worker:

- an Intel Xeon at 2.20 GHz with 32 logical CPUs and about 126 GiB of memory;
- kernel `7.0.0-1011-gcp`, Docker 29.8.2 and kind v0.33.0.

Each side of each round ran on a fresh three-node cluster. Each node is a
combined broker/controller with a 2 CPU, 2 GiB and 100 GiB claim. Topics used
RF=3 and `min.insync.replicas=2`; the producer used `acks=all`, idempotence,
64 KiB batches and a 5 ms linger. Five rounds interleaved the sides.

## Verify the evidence

Each run's directory holds the job's artifact exactly as GitHub Actions stored
it, `artifact.zip`, and byte-identical copies of the report, the verdict and
the record from inside it. From the repository root:

```sh
run=docs/operations/performance-budget-runs/2026-10-23-pinned-kafka-run-39865512647
(cd "$run" && sha256sum --check SHA256SUMS)
unzip -l "$run/artifact.zip"
```

The SHA-256 of each `artifact.zip` is the digest GitHub recorded for that
artifact: `6155ebf7…ec8ae58` for run 39756166298, `086ef8ae…8f75194` for run
39840381315, and, for run 39865512647, `73e21136…cb60f` for `before_after`
and `0d706dea…7b019` for `pinned_kafka`. The archive holds every raw trial's
workload log, resource snapshots and order, the cluster manifests, and the
host files.

`evaluate` judges a record only against the contract whose digest the record
holds. To judge one of these records again, take its contract from the
history, also from the repository root, into a scratch directory.
`92d78effd` holds `rf3-regression-v1`, and `fd007630` holds
`rf3-regression-v2`:

```sh
mkdir -p rejudged
git show fd007630:packaging/performance/regression-contract.json >rejudged/contract-v2.json
cargo run -p krabka-parse-benches -- evaluate "$run/performance-record.json" \
  --contract rejudged/contract-v2.json \
  --output rejudged/verdict.json --markdown rejudged/performance-budget.md
cmp rejudged/verdict.json "$run/verdict.json"
```
