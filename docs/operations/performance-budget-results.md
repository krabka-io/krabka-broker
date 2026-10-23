# Performance budget results

These are the measured runs of the RF3 regression contract that
[Performance regression budgets](performance-budgets.md) defines. A result
applies to its contract on the host it names. It is not a ranking of brokers
and not a supported limit.

## Results

No run of the current contract, `rf3-regression-v2`, has completed yet. Its
first `before_after` and `pinned_kafka` runs are published here when they
complete. Until a run on a frozen release candidate passes, release blocker B8
stays open ([release-candidate qualification](../release-candidate-qualification.md)).

## Calibration

The first contract, `rf3-regression-v1`, took its budgets from earlier
results rather than from a run of its own. Its first measured run had to show
that every cell's spread fits inside its variance budget, or the contract had
to change before any result was published. It did not fit, so the contract
changed to `rf3-regression-v2`. The runs below are the calibration evidence for
that change. Their verdicts were reached under `rf3-regression-v1`, and they
publish no result.

| Date | Mode | Reference | Candidate | Verdict under v1 | Evidence |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 2026-10-22 | `before_after` | v1.0.5, `ac9deff3` | `54c132a4` | Reject: excess variance in two cells | [Run 39756166298](performance-budget-runs/2026-10-22-before-after-run-39756166298/performance-budget.md) |
| 2026-10-22 | `before_after` | v1.0.5, `ac9deff3` | `bc037c1b` | Pass | [Run 39840381315](performance-budget-runs/2026-10-22-before-after-run-39840381315/performance-budget.md) |

Both candidates are `main` plus harness changes only; neither changes the
broker. `54c132a4` is `main` at `c5725ed4` with the cloud budget dispatch
(#1649). `bc037c1b` is `main` at `3269fae8` with the harness's own krabka
manifest (#1664). Every one of the 90 trials in each run consumed every record
it sent, with no duplicates and no errors.

### Spreads measured

A metric's spread is the relative median absolute deviation over the five
rounds. A cell is judged on the larger of its two sides' spreads. This is the
largest spread of each budgeted metric, over every cell of each run:

| Metric | First run | Second run | v1 budget | v2 budget |
| :--- | ---: | ---: | ---: | ---: |
| records/s | 12.1%, `1k-random-none-p12-saturate` | 9.2%, `100k-random-lz4-p12-saturate` | 10% | 15% |
| p99 latency | 40.2%, `1k-random-lz4-p12-idle-burst` | 9.3%, `1k-random-lz4-p1000-fixed-5k` | 25% | 50% |
| CPU µs/record | 5.8%, `1k-random-none-p12-saturate` | 3.5%, `1k-random-lz4-p12-idle-burst` | 10% | 10% |
| peak RSS | 3.4%, `1k-zeros-lz4-p12-saturate` | 3.7%, `1k-random-lz4-p1000-fixed-5k` | 10% | 10% |

`rf3-regression-v2` sets each budget that a run exceeded to about 1.25 times
the largest spread measured, rounded up to a multiple of 5%: 15% for records/s
and 50% for p99. It keeps the other two budgets, every materiality floor, every
spread multiplier, the cells and the rounds. A wider variance budget does not
loosen the regression test of a quiet cell. It lets a noisier cell be judged
instead of rejected, and that cell's allowed change is still three times its
own spread.

The `pinned_kafka` mode has no completed run under either contract.

### Contract and host

Both runs recorded `rf3-regression-v1`, SHA-256
`88125f23e83026670713516c73f401d80cced33577e1a4244b33b3cac83e4d17`, before
their first trial. `rf3-regression-v2` is SHA-256
`038d48e5ff398467a7ce5f97bb239f8378e21102e7113344693c65df725eeded`.

Both runs used the `benchmarks.yml` `performance-budget` job on a
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
run=docs/operations/performance-budget-runs/2026-10-22-before-after-run-39840381315
(cd "$run" && sha256sum --check SHA256SUMS)
unzip -l "$run/artifact.zip"
```

The SHA-256 of each `artifact.zip` is the digest GitHub recorded for that
artifact: `6155ebf7…ec8ae58` for run 39756166298, and `086ef8ae…8f75194` for run
39840381315. The archive holds every raw trial's workload log, resource
snapshots and order, the cluster manifests, and the host files.

`evaluate` judges a record only against the contract whose digest the record
holds. To judge one of these records again, take `rf3-regression-v1` from the
history, also from the repository root, into a scratch directory:

```sh
mkdir -p rejudged
git show 92d78effd:packaging/performance/regression-contract.json >rejudged/contract-v1.json
cargo run -p krabka-parse-benches -- evaluate "$run/performance-record.json" \
  --contract rejudged/contract-v1.json \
  --output rejudged/verdict.json --markdown rejudged/performance-budget.md
cmp rejudged/verdict.json "$run/verdict.json"
```
