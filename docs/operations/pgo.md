# Profile-guided optimization

`aspect pgo` measures what profile-guided optimization (PGO) does for
`krabka-broker`. It builds a broker from a profile of benchmark load. Then it
runs that broker and the normal release broker against the same scenarios,
and it writes a report that compares the two.

The task is an experiment. The shipped image does not use PGO.

## Requirements

- Linux. The task reads `/proc/<pid>/stat` and pins processes with `taskset`.
- At least 4 CPUs. The compare phase puts the broker on CPUs 2 and 3, and the
  driver on CPUs 0 and 1.
- `llvm-profdata` from the pinned toolchain. Install it with
  `rustup component add llvm-tools`.
- Free ports 19092 and 19093 on `127.0.0.1`.
- Disk for three release target directories. A saturating run also writes
  several GB to its log directory. The task deletes each log directory when
  its run ends.

## Run it

```
aspect pgo
```

A full run takes a long time. Three of the steps are fat-LTO release builds,
and the compare phase runs 5 rounds of two scenarios on two brokers.

The options are:

- `--work-dir DIR`: where the builds, profiles, logs and results go. The
  default is `pgo-work` beside the repository root.
- `--phase NAME`: the first phase to run. The task runs that phase and every
  phase after it. The default is `all`.
- `--trials N`: the number of compare rounds. The default is 5.
- `--dry-run`: print every command with its environment, and run nothing.

The phases run in this order:

1. `build` builds the baseline `krabka-broker`, `krabka-format` and
   `krabka-bench-driver` with the repository's `[profile.release]`. It then
   builds a broker with `RUSTFLAGS=-Cprofile-generate`.
2. `train` runs the instrumented broker under six short scenarios. They are
   copies of the files in `bench/scenarios/` with a 2s warmup and a 20s
   measured window. They cover small records, large records with lz4, small
   records with zstd, four producers and four consumers, a fixed rate, and
   `acks=all`. The task stops each broker with SIGTERM. An instrumented
   binary writes its `.profraw` file only when it exits cleanly.
3. `merge` merges the `.profraw` files into `merged.profdata`.
4. `optimize` builds the broker with `RUSTFLAGS=-Cprofile-use`.
5. `compare` runs the baseline and the PGO broker, and writes `report.md`.

If a phase fails, fix the cause and start again from that phase:

```
aspect pgo --phase merge
```

## What the compare phase measures

The evaluation set is `small-msg-saturate` as it is in `bench/scenarios/`,
and `large-msg` with a 30s measured window. Each round runs every scenario
once on each broker. The baseline goes first in odd rounds, and the PGO
broker goes first in even rounds. Each run starts from a freshly formatted
log directory.

The task records four values for each run:

- Producer throughput in messages a second: the driver's `msgs_produced`
  divided by the scenario's `duration`.
- Producer throughput in MB a second: the driver's `bytes_in` divided by the
  `duration`. A MB is 10^6 bytes.
- The p99 producer-ack latency: the driver's `producer_latency.p99`.
- Broker CPU seconds: `utime` plus `stime` from `/proc/<pid>/stat` just
  before the SIGTERM, divided by `CLK_TCK`. This count includes the broker's
  startup.

## Read the report

The task prints the path of `report.md` when it ends. The report has one
table for each scenario, and one row for each value. A row gives the
baseline median, the PGO median, the change, and the value of every trial.

The change is the PGO median minus the baseline median, as a percent of the
baseline median. The `Better` column says which direction is an improvement.
A positive change is better for throughput. A negative change is better for
latency and CPU seconds.

Compare the change with the spread of the trial values. A change that is
smaller than the spread inside one broker's trials is noise. Run more
trials with `--trials` before you trust it.

The work directory also holds the data behind the report:

- `compare/results.json` holds every trial's values. The task writes it after
  each trial.
- `compare/<trial>.json` is the driver's own output for each trial.
- `logs/<trial>.out` and `logs/<trial>.err` hold each broker's output.
- `train/<scenario>.json` is the driver's output for each training run.
