# Performance budget results

These are the measured runs of the `rf3-regression-v1` contract that
[Performance regression budgets](performance-budgets.md) defines. Each result
applies to that contract on the host it names. It is not a ranking of brokers
and not a supported limit.

The runs below measured `main`, not a frozen release candidate. They calibrate
the contract. They do not close release blocker B8, which needs both modes to
pass on the candidate
([release-candidate qualification](../release-candidate-qualification.md)).

## Runs

| Date | Mode | Reference | Candidate | Verdict | Evidence |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 2026-10-22 | `before_after` | v1.0.5, `ac9deff3` | `54c132a4` | Reject: excess variance in two cells. No correctness failure and no material regression. | [Run 39756166298](performance-budget-runs/2026-10-22-before-after-run-39756166298/performance-budget.md) |
| 2026-10-22 | `before_after` | v1.0.5, `ac9deff3` | `bc037c1b` | **Pass** | [Run 39840381315](performance-budget-runs/2026-10-22-before-after-run-39840381315/performance-budget.md) |

Both candidates are `main` plus harness changes only; neither changes the
broker. `54c132a4` is `main` at `c5725ed4` with the cloud budget dispatch
(#1649). `bc037c1b` is `main` at `3269fae8` with the harness's own krabka
manifest (#1664).

The `pinned_kafka` comparison against Kafka 4.3.1 has not completed a run yet.
It is added here when it does.

## Contract and host

Both runs judged their record against the checked-in contract, SHA-256
`88125f23e83026670713516c73f401d80cced33577e1a4244b33b3cac83e4d17`. Each run
recorded that digest before its first trial.

Both runs used the `benchmarks.yml` `performance-budget` job on a
`gcp-ubuntu-24-04-32core` worker:

- an Intel Xeon at 2.20 GHz with 32 logical CPUs and about 126 GiB of memory;
- kernel `7.0.0-1011-gcp`, Docker 29.8.2 and kind v0.33.0.

Each side of each round ran on a fresh three-node cluster. Each node is a
combined broker/controller with a 2 CPU, 2 GiB and 100 GiB claim. Topics used
RF=3 and `min.insync.replicas=2`; the producer used `acks=all`, idempotence,
64 KiB batches and a 5 ms linger. Five rounds interleaved the sides, and every
one of the 90 trials in each run consumed every record it sent, with no
duplicates and no errors.

## Before and after: v1.0.5 against `main`

These are the medians of run 39840381315. A negative change is better. In a
saturating cell, p99 is recorded but not budgeted, because it measures the
producer's own queue.

| Cell | records/s | CPU µs/record | Peak RSS | p99 |
| :--- | ---: | ---: | ---: | ---: |
| `1k-random-lz4-p12-saturate` | 164,369 → 187,478 | −42.6% | −0.8% | +20.9%, recorded |
| `100b-random-lz4-p12-saturate` | 932,244 → 1,013,227 | −31.0% | −0.9% | −24.4%, recorded |
| `1k-zeros-lz4-p12-saturate` | 712,885 → 720,374 | −19.4% | −12.1% | −8.9%, recorded |
| `1k-random-none-p12-saturate` | 161,410 → 189,077 | −41.0% | −9.3% | +113.7%, recorded |
| `100k-random-lz4-p12-saturate` | 1,795 → 2,113 | −36.6% | −7.7% | +6.5%, recorded |
| `1k-random-lz4-p1-saturate` | 37,789 → 49,363 | −27.9% | −7.0% | −25.0%, recorded |
| `1k-random-lz4-p12-fixed-20k` | 19,993 → 19,994 | −23.5% | −6.8% | 9.3 → 7.7 ms, within budget |
| `1k-random-lz4-p1000-fixed-5k` | 4,998 → 4,999 | −12.5% | +0.2% | 37.5 → 37.5 ms, within budget |
| `1k-random-lz4-p12-idle-burst` | 49,504 → 49,728 | −0.4% | +3.5% | 1,092 → 139 ms, improved |

The `1k-random-none-p12-saturate` p99 rose from 589 ms to 1,258 ms while its
throughput rose 17%. The contract does not budget it. It is recorded here so
that a later run can show whether it holds.

## Calibration

The first run rejected two cells for spread alone:

- `1k-random-none-p12-saturate` records/s, with a candidate spread of 12.1%
  against a 10% budget;
- `1k-random-lz4-p12-idle-burst` p99, with a candidate spread of 40.2% against
  a 25% budget.

The second run, on the same contract and the same kind of host, measured the
larger side's spread in those cells at 3.0% and 6.1%, and every other cell was
within its variance budget. The first run's excess is consistent with one noisy trial, not with
budgets that are too tight, so the contract keeps its budgets. A later run that
exceeds them again changes the contract in a reviewed pull request, as the
design requires.

## Verify the evidence

Each run's directory holds the job's artifact exactly as GitHub Actions stored
it, `artifact.zip`, and byte-identical copies of the report, the verdict and
the record from inside it. From the repository root:

```sh
run=docs/operations/performance-budget-runs/2026-10-22-before-after-run-39840381315
(cd "$run" && sha256sum --check SHA256SUMS)
unzip -l "$run/artifact.zip"
```

The SHA-256 of each `artifact.zip` is the digest GitHub recorded for that
artifact: `6155ebf7…ec8ae58` for run 39756166298, and `086ef8ae…8f75194` for run
39840381315. The archive holds every raw trial's workload log, resource
snapshots and order, the cluster manifests, and the host files. To judge a
record again, also from the repository root, into a scratch directory:

```sh
mkdir -p rejudged
cargo run -p krabka-parse-benches -- evaluate "$run/performance-record.json" \
  --contract packaging/performance/regression-contract.json \
  --output rejudged/verdict.json --markdown rejudged/performance-budget.md
cmp rejudged/verdict.json "$run/verdict.json"
```
