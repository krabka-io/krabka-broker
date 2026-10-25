# Profile-guided optimization

`aspect pgo` measures what profile-guided optimization (PGO) does for
`krabka-broker`, and what building it for `x86-64-v3` does. It builds the
broker in up to four variants:

- `baseline`: the normal release broker.
- `pgo`: the broker built from a profile of benchmark load.
- `v3`: the release broker built with `-Ctarget-cpu=x86-64-v3`.
- `v3-pgo`: both, from the same profile as `pgo`.

Then it runs every variant against the same scenarios, and it writes a
report that compares each one with the baseline.

The task is an experiment. The shipped image uses neither PGO nor `x86-64-v3`.

## What x86-64-v3 is

`x86-64-v3` is a microarchitecture level of the x86-64 psABI. Beyond the
`x86-64-v2` set (SSE4.2, POPCNT, CMPXCHG16B), it lets the compiler use AVX,
AVX2, BMI1, BMI2, F16C, FMA, LZCNT, MOVBE and XSAVE anywhere in the binary.
A build without `-Ctarget-cpu` assumes only the baseline x86-64 set, which
ends at SSE2.

Intel CPUs from Haswell (2013) on, and AMD CPUs from Zen 1 (2017) on, have
the level. Some later Intel Pentium, Celeron and Atom parts do not. A `v3`
binary dies with SIGILL on a CPU that lacks it, at the first instruction it
cannot run. The task reads the `flags` line of `/proc/cpuinfo` and refuses
to build or run a `v3` variant on a host that lacks any of the level's
flags. A dry run prints a warning instead.

The build host needs the level as well as the compare host. `RUSTFLAGS`
also applies to build scripts, and cargo runs them on the build host.

## Requirements

- Linux. The task reads `/proc/<pid>/stat` and pins processes with `taskset`.
- At least 4 CPUs. The compare phase puts the broker on CPUs 2 and 3, and the
  driver on CPUs 0 and 1.
- `llvm-profdata` from the pinned toolchain. Install it with
  `rustup component add llvm-tools`.
- Free ports 19092 and 19093 on `127.0.0.1`.
- An `x86-64-v3` CPU for the `v3` and `v3-pgo` variants. Leave them out of
  `--variants` on an older CPU.
- Disk for a release target directory for each variant, and one for the
  instrumented build: five with the default variants. A saturating run also
  writes several GB to its log directory. The task deletes each log
  directory when its run ends.

## Run it

```
aspect pgo
```

A full run takes a long time. Five of the steps are fat-LTO release builds,
and the compare phase runs 5 rounds of two scenarios on four brokers.

The options are:

- `--work-dir DIR`: where the builds, profiles, logs and results go. The
  default is `pgo-work` beside the repository root.
- `--phase NAME`: the first phase to run. The task runs that phase and every
  phase after it. The default is `all`.
- `--phase NAME,NAME,...`: a comma list runs exactly the phases it names, in
  pipeline order, whatever order the list gives them in.
- `--variants NAME,...`: the variants that `build` and `optimize` build and
  that `compare` runs. The default is `baseline,pgo,v3,v3-pgo`. The list
  must include `baseline`, because every change is against it.
- `--trials N`: the number of compare rounds. The default is 5. Use a
  multiple of the variant count, so that every variant goes first equally
  often.
- `--dry-run`: print every command with its environment, and run nothing.

The phases run in this order:

1. `build` builds the baseline `krabka-broker`, `krabka-format` and
   `krabka-bench-driver` with the repository's `[profile.release]`. It then
   builds a broker with `RUSTFLAGS=-Cprofile-generate`, and the `v3` broker
   with `RUSTFLAGS=-Ctarget-cpu=x86-64-v3`. It always makes the baseline and
   the instrumented broker, and makes `v3` only when `--variants` selects it.
2. `train` runs the instrumented broker under six short scenarios. They are
   copies of the files in `bench/scenarios/` with a 2s warmup and a 20s
   measured window. They cover small records, large records with lz4, small
   records with zstd, four producers and four consumers, a fixed rate, and
   `acks=all`. The task stops each broker with SIGTERM. An instrumented
   binary writes its `.profraw` file only when it exits cleanly. The task
   gives each driver run 600 seconds and retries a hung session once.
3. `merge` merges the `.profraw` files into `merged.profdata`.
4. `optimize` builds the `pgo` broker with `RUSTFLAGS=-Cprofile-use`, and
   the `v3-pgo` broker with
   `RUSTFLAGS="-Ctarget-cpu=x86-64-v3 -Cprofile-use"`. It makes only the
   ones that `--variants` selects. The instrumented broker is a generic
   build, so LLVM can find that a few functions of the `v3-pgo` build no
   longer match the profile. It warns, and it builds those functions
   without profile data.
5. `compare` runs every selected broker, and writes `report.md`.

Each variant builds into `target-<variant>` under the work directory, and the
instrumented broker into `target-instrumented`.

If a phase fails, fix the cause and start again from that phase:

```
aspect pgo --phase merge
```

To add a variant to a work directory that already has a profile, skip
`train` and `merge`. `train` rewrites the `.profraw` files, and a new merge
would rewrite `merged.profdata`, which calls for both PGO builds again:

```
aspect pgo --phase build,optimize,compare
```

Cargo finds the builds that exist up to date, so this builds only the new
variants, then compares all of them.

Before a phase runs, the task checks that the work directory holds the files
the phase reads, such as every selected variant's broker for `compare`. A
real run stops at the first missing file and names the phase that makes it.
`--dry-run` makes nothing, so it checks only the files that no phase of the
run makes, and it prints a `missing` line for each one that a real run would
stop on.

## What the compare phase measures

The evaluation set is `small-msg-saturate` as it is in `bench/scenarios/`,
and `large-msg` with a 30s measured window. Each round runs every scenario
once on each selected broker. The order rotates from round to round: round 1
runs the brokers in the order `baseline`, `pgo`, `v3`, `v3-pgo`, and each
later round starts one broker further along that list. With four variants,
round 2 runs `pgo`, `v3`, `v3-pgo`, `baseline`. Over as many rounds as there
are variants, every broker runs once in every position, so a drift over the
run falls on all of them alike. Each run starts from a freshly formatted log
directory.

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
baseline median, then a median and a change for each other selected variant,
then the value of every trial of every variant.

A change is the variant's median minus the baseline median, as a percent of
the baseline median. The `Better` column says which direction is an improvement.
A positive change is better for throughput. A negative change is better for
latency and CPU seconds.

Compare the change with the spread of the trial values. A change that is
smaller than the spread inside one broker's trials is noise. To tell how much
of `v3-pgo` comes from each half, set its change beside those of `pgo` and
`v3`. Run more
trials with `--trials` before you trust it.

The work directory also holds the data behind the report:

- `compare/results.json` holds every trial's values. The task writes it after
  each trial.
- `compare/<trial>.json` is the driver's own output for each trial.
- `logs/<trial>.out` and `logs/<trial>.err` hold each broker's output.
- `train/<scenario>.json` is the driver's output for each training run.
