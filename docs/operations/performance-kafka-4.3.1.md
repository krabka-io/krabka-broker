# Release performance against Kafka 4.3.1 — 2026-09-30

This is a local throughput, CPU, and memory experiment on one shared Linux host.
All reported comparisons use the same Kafka 4.3.1 client, broker resource limits,
payload, acknowledgments, replication contract, and number of repetitions.
Medians summarize three independent topics; the accompanying CSV retains each run.

The tables are the archived benchmark results for the exact sources and binary
hashes below. Publication rebased the changes onto broker `a5142413` and protocol
`a6684fca`, which had advanced while the benchmark was running. The broker pins
protocol commit `0536491e1c270051cc04dc24d192f5c819ba11b5` from
[krabka-protocol PR #48](https://github.com/krabka-io/krabka-protocol/pull/48).
These archived numbers do not claim a rerun of the rebased PR revision.

## Changes and evidence

The first CPU profile showed repeated first-batch CRC verification, decompression,
and record allocation through `should_roll_for_incoming`. Cache the first record's
timestamp per segment, invalidate it on rollback/truncation, and recover it lazily
on reopen. This preserves CreateTime/LogAppendTime behavior and negative timestamp
deltas. The next profile confirmed that repeated owned batch decoding disappeared.
It also identified request guards cloning/dropping every metrics handle; those
guards now borrow the metrics for their existing lifetime.

The final random-payload/LZ4 CPU profile confirms the
repeated roll-check decoding is gone. It attributes about 28% of sampled time
to the sendfile path, 5% to the new CRC backend, and 5% cumulatively to record
parsing. For zero-filled compressed traffic, decompression and record validation
remain much larger costs. CPU sample shares are qualitative attribution, not
instruction counts or a demonstrated universal throughput ceiling.

`--config=release` selects `-c opt`, thin LTO, and one codegen unit. Pilot runs of
both source fixes with ordinary opt versus thin LTO had medians of 569,003 versus
643,790 records/s, and 17.894 versus 17.495 broker CPU seconds for 10 million
1 KiB random records. The small CPU difference is inconclusive; binary size fell
from 80,446,224 to 57548696 bytes. These pilots are separate from the
matched matrix below.

The workload accepts optional compression and `zeros|random` arguments. A seeded
4 MiB random pool prevents mostly zero payloads from masquerading as incompressible
traffic. Every record retains its timestamp and unique sequence number. Existing
seven-argument invocations retain their previous LZ4/zeros behavior.

## Contract and provenance

- Baseline broker commit: `401945732d9ac37b93de5c89d5e649ece18049b0`.
- Kafka: `apache/kafka:4.3.1`, pinned as
  `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
- Native runtime: the repository's Wolfi production image, immutable local image
  `sha256:66999791b48d10e3ef53a5c3ccaf0b7d30389040138e5476392540ad43b7662e`,
  with the measured binary mounted read-only. Initial Alpine/glibc launch failures
  were archived and contributed no measurements.
- Host: AMD EPYC 4344P, 8 physical/16 logical CPUs, about 61 GiB RAM,
  RAID-backed disk, loopback networking. No TLS, authentication, tiered storage,
  compaction, forced per-append fsync, restart, or failure injection in these runs.
- Each broker/controller: two physical cores, 2 CPU quota, 2 GiB memory,
  no swap. Kafka heap: `-Xms1g -Xmx1g`, except the explicitly labeled smaller-heap check.
- RF1: one combined broker/controller on cores 2–3; client on cores 0–1 and 4–7.
  RF3: three combined broker/controllers on cores 2–3, 4–5, and 6–7;
  client on cores 0–1 plus their SMT siblings 8–9.
- Each topic: 12 partitions, RF1/minISR1 or RF3/minISR2, `acks=all`, idempotence,
  65,536-byte producer batches, 5 ms linger. Clients use OpenJDK 25.0.4.1 and
  the exact Kafka 4.3.1 jars for both brokers. Admin JVMs run outside broker cgroups.
- RF1 warm-up: 3 million records; RF3: 1 million records. Each measured topic is
  fresh, while the broker remains alive across the same ordered case sequence.
  RF1 samples: 10 million records for 1 KiB/100 B LZ4, 5 million uncompressed,
  30,000 for 100 KiB, or 600,000 at 20k/s for 30 seconds.
- CPU is the broker cgroup's `cpu.stat` usage delta, including system CPU;
  RF3 adds all three brokers. CPU per record uses acknowledged records, not replicas.
  Monitoring includes client initialization/closing, whereas workload throughput
  and latency exclude that initialization. Short unlimited cases consequently
  have a larger fixed overhead and more variance.
- Peak process RSS, anonymous cgroup memory, and working set are sampled every
  250 ms. Working set is `memory.current - inactive_file`; RSS excludes most disk
  page cache. These are observed peaks, not hard bounds. Profiling runs are excluded
  from memory comparisons because pprof allocates additional buffers.
- Every completed workload consumed exactly its acknowledged count with zero
  producer errors and zero duplicate sequences. This validates delivery in the
  experiment; it is not a crash-durability qualification.

## Single broker, RF1

| Workload | Kafka records/s | Original Krabka | Release Krabka | Kafka CPU µs/record | Original Krabka | Release Krabka |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, LZ4 | 628,804 | 471,057 | 649,112 | 1.553 | 2.359 | 1.672 |
| 100 B random, LZ4 | 2,749,274 | 2,993,714 | 3,722,611 | 0.306 | 0.379 | 0.325 |
| 1 KiB zeros, LZ4 | 2,180,883 | 1,507,289 | 2,168,170 | 0.204 | 0.579 | 0.330 |
| 1 KiB random, no compression | 558,709 | 547,849 | 540,143 | 1.343 | 2.198 | 1.995 |
| 100 KiB random, LZ4 | 5,552 | 4,629 | 6,834 | 116.330 | 221.598 | 142.004 |
| 1 KiB random, 20k records/s | 19,998 | 19,997 | 19,998 | 4.533 | 8.217 | 6.289 |

| Workload | Kafka peak RSS MiB | Krabka peak RSS MiB | Kafka working set MiB | Krabka working set MiB | Kafka p99 ms | Krabka p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, LZ4 | 677.8 | 63.6 | 742.8 | 137.3 | 325.1 | 313.7 |
| 100 B random, LZ4 | 1,152.5 | 76.1 | 1,217.9 | 149.3 | 76.0 | 4.5 |
| 1 KiB zeros, LZ4 | 1,188.5 | 120.7 | 1,260.3 | 191.5 | 559.9 | 629.7 |
| 1 KiB random, no compression | 1,199.1 | 122.8 | 1,281.4 | 200.9 | 335.7 | 367.1 |
| 100 KiB random, LZ4 | 1,215.0 | 123.2 | 1,304.8 | 201.5 | 310.4 | 370.2 |
| 1 KiB random, 20k records/s | 1,220.4 | 123.4 | 1,318.9 | 200.4 | 3.3 | 3.4 |

### Local protocol optimization

The table above uses the benchmark baseline's original protocol dependency. The
following variant also changes the sibling `krabka-protocol` checkout: use the
runtime-dispatched `crc-fast` CRC-32C backend, preserve finalized seeded CRC
semantics, and inline four existing varint readers. CRC equality was checked
against the previous independent implementation for arbitrary seeds, unaligned
buffers, and chained chunks. A standalone 64 KiB CRC microbenchmark improved from
7.38 to 63.4 GiB/s on this AVX-512 host; whole-broker gains are much smaller.
The compressed-record profile's sampled varint share fell from about 29% to 13%;
inlining changes attribution, so that percentage is not a whole-broker CPU gain.

**The measured binary used a local two-repository variant.** The protocol edits were in
`/home/matt/.codex/worktrees/782f/krabka-protocol`, branch
`codex/crc32c-performance`, based on `753a8e2a6795dad438b587910afa05b43bf63e95`.
The implementation is now published in protocol PR #48 and pinned by this
broker PR. An ordinary release build includes both repositories' optimizations.
Replaying the exact historical measured binary uses the original source patches
and both repository overrides listed in `builds.json` and below.

| Workload | Kafka records/s | Original Krabka | Release Krabka | Kafka CPU µs/record | Original Krabka | Release Krabka |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, LZ4 | 628,804 | 471,057 | 636,407 | 1.553 | 2.359 | 1.562 |
| 100 B random, LZ4 | 2,749,274 | 2,993,714 | 3,663,164 | 0.306 | 0.379 | 0.307 |
| 1 KiB zeros, LZ4 | 2,180,883 | 1,507,289 | 2,827,004 | 0.204 | 0.579 | 0.271 |
| 1 KiB random, no compression | 558,709 | 547,849 | 662,199 | 1.343 | 2.198 | 1.446 |
| 100 KiB random, LZ4 | 5,552 | 4,629 | 6,615 | 116.330 | 221.598 | 132.840 |
| 1 KiB random, 20k records/s | 19,998 | 19,997 | 19,997 | 4.533 | 8.217 | 5.932 |

| Workload | Kafka peak RSS MiB | Krabka peak RSS MiB | Kafka working set MiB | Krabka working set MiB | Kafka p99 ms | Krabka p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, LZ4 | 677.8 | 64.9 | 742.8 | 137.7 | 325.1 | 315.8 |
| 100 B random, LZ4 | 1,152.5 | 74.6 | 1,217.9 | 146.9 | 76.0 | 59.3 |
| 1 KiB zeros, LZ4 | 1,188.5 | 112.5 | 1,260.3 | 185.4 | 559.9 | 312.6 |
| 1 KiB random, no compression | 1,199.1 | 113.8 | 1,281.4 | 190.8 | 335.7 | 340.9 |
| 100 KiB random, LZ4 | 1,215.0 | 113.8 | 1,304.8 | 191.6 | 310.4 | 324.9 |
| 1 KiB random, 20k records/s | 1,220.4 | 113.9 | 1,318.9 | 190.1 | 3.3 | 3.4 |

This variant improves original Krabka's 1 KiB random throughput by about 35%
and lowers its CPU per record by about 34%. Against Kafka, the 1 KiB random
result is effectively tied within host variance. The other unlimited RF1 rows
show about 19–33% greater throughput, but CPU is still about 33% higher for
compressible records, 8% higher without compression, and 14% higher for large
records. At 20k records/s it uses about 31% more CPU than Kafka. These are
separate throughput and efficiency outcomes.

The random 100 B unlimited case is short; its headline rate is not evidence of
a sustained broker saturation ceiling. See the longer concurrent-client check.

### Interleaved large-record recheck

The CRC-only sequential matrix had a slow 100 KiB row. Three interleaved rounds
with a fresh broker per variant, 5,000 warm-up records, and 60,000 measured
100 KiB random LZ4 records did not reproduce that regression:

| Variant | Records/s | CPU µs/record | Peak RSS MiB | p99 ms |
| :--- | ---: | ---: | ---: | ---: |
| Broker-only release | 6,775 | 131.435 | 59.6 | 343.4 |
| CRC-only | 6,733 | 122.477 | 61.4 | 345.2 |
| CRC and inline readers | 7,052 | 123.825 | 62.5 | 336.6 |


CPU µs/record is lower-is-better; throughput is higher-is-better. RSS depends on
heap policy and previous workloads: Kafka's committed heap grows during the case
sequence. The first 1 KiB row has substantially lower Kafka RSS than later rows.
Highly compressible MiB/s measures logical payload bytes, not wire/disk bandwidth.

## Three brokers, RF3/minISR2

CPU and memory below are aggregate cluster values. Throughput and latency are
from the same client workload, with all replicas in the ISR before the run.

| Workload | Kafka records/s | Original Krabka | Release Krabka | Kafka CPU µs/record | Original Krabka | Release Krabka |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, unlimited | 287,992 | 313,473 | 342,679 | 6.555 | 10.335 | 7.293 |
| 1 KiB random, 20k records/s | 19,997 | 19,997 | 19,997 | 16.519 | 21.250 | 17.073 |

| Workload | Kafka peak RSS MiB | Krabka peak RSS MiB | Kafka working set MiB | Krabka working set MiB | Kafka p99 ms | Krabka p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, unlimited | 2,857.7 | 154.7 | 3,041.0 | 322.2 | 800.4 | 553.9 |
| 1 KiB random, 20k records/s | 2,889.0 | 165.4 | 3,136.0 | 362.1 | 5.3 | 4.0 |

The local protocol variant, with the same RF3 contract:

| Workload | Kafka records/s | Original Krabka | Release Krabka | Kafka CPU µs/record | Original Krabka | Release Krabka |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, unlimited | 287,992 | 313,473 | 317,700 | 6.555 | 10.335 | 7.245 |
| 1 KiB random, 20k records/s | 19,997 | 19,997 | 19,997 | 16.519 | 21.250 | 17.258 |

| Workload | Kafka peak RSS MiB | Krabka peak RSS MiB | Kafka working set MiB | Krabka working set MiB | Kafka p99 ms | Krabka p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random, unlimited | 2,857.7 | 152.8 | 3,041.0 | 328.0 | 800.4 | 761.0 |
| 1 KiB random, 20k records/s | 2,889.0 | 163.8 | 3,136.0 | 372.9 | 5.3 | 5.8 |

The broker-only release is about 19% faster than Kafka in this RF3 run, while
using about 11% more aggregate CPU per record. The local protocol variant is
about 10% faster, with similar CPU to the broker-only variant. This difference
does not establish a protocol throughput improvement for replication.

## Repeated measurements and follow-up checks

| RF1 workload | Kafka records/s range | Broker-only range | Local protocol range |
| :--- | ---: | ---: | ---: |
| 1 KiB random, LZ4 | 501,852–641,363 | 634,823–663,519 | 598,709–657,292 |
| 100 B random, LZ4 | 2,630,459–2,779,463 | 2,590,848–3,757,029 | 3,418,418–3,705,671 |
| 1 KiB zeros, LZ4 | 2,118,178–2,324,273 | 1,990,538–2,198,121 | 2,824,116–2,870,377 |
| 1 KiB random, no compression | 548,946–567,907 | 436,430–552,186 | 627,047–784,212 |
| 100 KiB random, LZ4 | 5,482–6,362 | 5,880–7,791 | 6,288–7,386 |
| 1 KiB random, 20k records/s | 19,997–19,998 | 19,997–19,998 | 19,997–19,998 |


### Linux TCP packet batching experiment

Three interleaved rounds compared the local protocol variant with and without
`TCP_CORK` around file-backed fetch responses, using a fresh broker each time.
The socket is uncorked on success and I/O errors; cancellation already requires
the caller to close a partially written response. TLS fallback and other OS
paths are unchanged. Results below determine whether this additional code stays.

| Workload | Before records/s | Cork records/s | Before CPU µs/record | Cork CPU µs/record | Before p99 ms | Cork p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random unlimited | 616,753 | 672,844 | 1.587 | 1.565 | 320.908 | 301.634 |
| 1 KiB random 20k/s | 19,997 | 19,997 | 6.060 | 5.954 | 3.397 | 3.389 |

The TCP_CORK experiment was removed: median CPU gains were only 1.4% unlimited and 1.8% at 20k/s, below the materiality threshold and host variance. Its 9% median throughput difference came with overlapping 406k–686k versus 606k–692k ranges and an isolated p99 spike. The simpler previously validated release-final binary is used for follow-up comparisons.


A final isolated record-validation check compared the existing parser's inline
hint with forced inlining. Three interleaved samples of 64 KiB batches, with
100 B/1 KiB zero values and no compression/LZ4, showed no material improvement
(median changes within about 1%). The last round had substantial host variance.
No parser logic or additional inline attribute was retained. The corrected
experiment and compiler output are retained in `record-probe/forced-results.txt`;
the earlier duplicate-attribute trial is not evidence of an optimization.


### Four concurrent producer/consumer pairs

Four independent topics each have 12 partitions (48 total). Each client writes and reads 5 million 1 KiB random, 40 million 100 B random, or 10 million 1 KiB zero records. Rate uses the total count divided by full wall time including client startup and shutdown, unlike the single-client matrix. Latency values are the **worst client quantile**, not a merged distribution; each client JSON is retained. All four JVMs share the same six client cores.

| Workload | Kafka records/s | Krabka records/s | Kafka CPU µs/record | Krabka CPU µs/record | Kafka RSS MiB | Krabka RSS MiB | Kafka p99 ms | Krabka p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random | 577,012 | 580,839 | 1.829 | 1.974 | 1203.0 | 118.6 | 417.478 | 522.597 |
| 100 B random | 2,697,986 | 4,209,653 | 0.287 | 0.233 | 1231.3 | 144.5 | 840.101 | 566.763 |
| 1 KiB zeros | 3,406,950 | 2,307,669 | 0.184 | 0.324 | 1230.9 | 188.4 | 2357.737 | 2714.179 |

| Workload | Kafka records/s range | Krabka records/s range |
| :--- | ---: | ---: |
| 1 KiB random | 551,196–654,104 | 527,346–605,226 |
| 100 B random | 2,642,047–5,031,914 | 3,821,041–4,286,916 |
| 1 KiB zeros | 3,258,908–3,523,161 | 2,303,012–2,344,034 |


### Fresh broker with Kafka 256 MiB heap

Both brokers start fresh, warm up with 3 million 1 KiB random records, then run three unlimited and three 20k/s topics. Kafka uses `-Xms256m -Xmx256m`; native broker settings remain the same. This check measures the effect of a smaller Kafka heap instead of attributing the entire memory gap to implementation.

| Workload | Kafka records/s | Krabka records/s | Kafka CPU µs/record | Krabka CPU µs/record | Kafka RSS MiB | Krabka RSS MiB | Kafka p99 ms | Krabka p99 ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB random unlimited | 489,759 | 520,989 | 2.234 | 1.876 | 412.3 | 70.7 | 351.493 | 402.750 |
| 1 KiB random 20k/s | 19,998 | 19,998 | 5.404 | 6.630 | 409.6 | 73.4 | 3.829 | 3.748 |

| Workload | Kafka records/s range | Krabka records/s range |
| :--- | ---: | ---: |
| 1 KiB random unlimited | 455,308–501,800 | 502,913–591,391 |
| 1 KiB random 20k/s | 19,997–19,998 | 19,997–19,998 |


## Interpretation and limits

This host runs other workloads, and the measurements have visible variance.
The small RF1 1 KiB throughput lead over Kafka should not be treated as a decisive
win. Some single-client tests leave broker CPU below its limit; their throughput
is an end-to-end workload rate, not a demonstrated broker saturation ceiling.
CPU efficiency, throughput, and latency can move in different directions.

These results establish local behavior for these binaries and contracts. They
are separate from the existing Kind performance qualification, partition-envelope
results, remote CI, and release readiness.

The original native container inherited a 1,024-descriptor soft limit. Completed
matrix runs stayed within it, but a later profiled random-record run after a
60-million-record profile exhausted descriptors and triggered the broker's
shutdown path. That incomplete run is excluded. All subsequent fresh-broker,
packet-batching, concurrent, and small-heap runs explicitly use
`--ulimit nofile=131072:131072` for both brokers; saved `/proc` limits establish
the setting. There was no OOM. This experiment does not qualify default-container
behavior at an accumulated multi-topic file-descriptor limit.

## Reproduction and retained artifacts

Build the current broker, including the published protocol pin, with:

```sh
bazel build --config=release --lockfile_mode=off --bes_backend= \
  //crates/broker:krabka-broker //crates/format:krabka-format
bazel run --config=release --lockfile_mode=off --bes_backend= //packaging:image_load
```

To replay the historical local variant on the archived broker source, add these
to the build (the first directory
preserves Bazel-generated BUILD files, and the second supplies its dependency
metadata; a raw source checkout alone cannot replace those generated files):

```text
--override_repository=rules_rs++crate+crates__github.com_krabka-io_krabka-protocol_753a8e2a=/home/matt/.cache/krabka-perf/782f-20260930/protocol-source-override
--override_repository=rules_rs++crate+crates__krabka-protocol-0.4.0=/home/matt/.cache/krabka-perf/782f-20260930/protocol-crate-override
```

Compile `packaging/performance/BrokerPerformanceWorkload.java` against Kafka 4.3.1
jars. Its complete argument contract is:

```text
bootstrap topic group records bytes records_per_second timeout_seconds [compression] [zeros|random]
```

The exact local runner, container commands, configs, topic descriptions, resource
time series, per-client JSON, profiles, build hashes, and validation logs are in
`/home/matt/.cache/krabka-perf/782f-20260930`. `run.py`, `matrix.py`, `rf3.py`, and
`concurrent.py` record the launch and workload commands. Paths in this experimental
runner refer to this checkout; adapt ROOT/ART and the native runtime image ID for
another host. `builds.json` records the source patch and binary SHA-256 hashes.
Disposable broker record logs were removed after count/sequence validation to
avoid disk exhaustion; configs, measurements, profiles, and binaries remain.

`performance-kafka-4.3.1.csv` contains each matched unprofiled run, including
throughput, CPU seconds, peak RSS/anonymous/working-set memory, and p50/p95/p99.
Profiles are retained in `krabka-1k-profile`, `cache-1k-profile`, and
`release-rf3-optimized-profile`.

The following hashes identify the main native binaries independently of mutable
Bazel outputs:

| Binary | SHA-256 | Bytes |
| :--- | :--- | ---: |
| `release-baseline` | `7768013ce895d53eb9931446aff6d75626c27af3648ec0dd02dda0096e645a4c` | 80,432,424 |
| `release-optimized` | `dcde23b3a2435fb6f481906017db1df4ece2ab7118d28ddd8a4a0341617a1c0d` | 57,548,696 |
| `release-crc` | `708492a9c052aea7d6df9bccb4945f43be3fe40bb6034a2647b7eecbe4c4f0cb` | 57,599,688 |
| `release-final` | `6083b08058af05b7ed63c6bf637202b9fb1d996d2b32911bf7c2a841b43c5395` | 57,882,720 |

## Validation

474 log unit tests, 4 restart tests, and all 4,620 broker unit tests passed
with `-c opt` and the local protocol overrides after removing the packet-batching
experiment. The experiment separately passed 4,621 broker unit tests.
Broker and log Bazel Clippy targets passed. All 1,137 protocol unit tests and
protocol Cargo Clippy (`--all-targets --locked -D warnings`) passed in the sibling
checkout. Thin-LTO release builds completed the measured workloads.
`git diff --check` and formatting passed. No remote CI conclusion is claimed.

Publication revalidation after the rebase and published protocol pin passed
4,634 broker unit tests, 474 log unit tests, and four restart tests through
normal Bazel resolution, without repository overrides. Both repositories passed
`cargo clippy --workspace --all-targets --locked -- -D warnings`; the rebased
protocol companion passed 1,140 unit tests. Formatting and diff checks passed.
The archived benchmark tables remain tied to their original binaries above.
