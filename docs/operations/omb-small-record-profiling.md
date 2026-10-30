# Small-record OMB profiling

This investigation targets 100-byte records with 16 partitions or three producers, and RF3 tail latency. The reference is broker commit `782f5b50268679e428f2da5118735ba3f87c4dda`. Measurements were collected on 2026-10-24. They diagnose the implementation on a constrained host; they do not reproduce the original Redpanda percentages or establish a production capacity limit.

The clearest replicated improvement is RF3 direct mode with 16 partitions: finite-burst completion rate increased 16.1%, and broker CPU per acknowledged record decreased 20.8%. Normal-mode throughput remained broadly unchanged within the observed spread. Normal-mode OMB latency was mixed, including unfavorable results retained below. These results do not establish closure of the original performance gap.

## Conditions

All processes share a four-CPU quota and 16 GiB of RAM on Linux x86-64. Each krabka broker has two Tokio workers; each Redpanda broker has two shards and 1 GiB of application memory. The Java 21 client uses `ActiveProcessorCount=2`, `Xms256m`, and `Xmx2g`. The filesystem has 32 GiB of space, so final comparisons use 64 MiB segments and fresh clusters. Compilation and tests finish before timed workloads run.

OMB is commit `5b1fa70951a323da26bd587174b58bb2c65b0b5c` with the website's common Kafka commit-coalescing patch, SHA-256 `2745718b278c5e655dd656faca7465fa7d1629834fd2a50dafa6ebdb054688ec`. It uses Kafka client 3.6.1, `acks=all`, idempotence, one in-flight request per connection, 1 ms linger, 1 MiB producer batches, no compression, and no automatic consumer commits. The same client configuration applies to both brokers. Redpanda is 26.2.2, image digest `sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96`, with write caching enabled and unsafe fsync bypass disabled. Final Redpanda runs set `storage_min_free_bytes=1073741824`, matching the harness's 1 GiB free-space floor; an earlier RF3 trial hit Redpanda's default 5 GiB reserve, returned storage errors, and was rejected. Krabka uses `min.insync.replicas=2` at RF3 and retains its existing all-ISR acknowledgement behavior. Direct mode sets `preallocate=true`.

The upstream suite's longer runs and larger host do not fit here. Local OMB trials measure one minute, with a one-minute warmup where stated. Cold trials are explicitly marked. The adaptive maximum-rate controller changes offered load during a trial, making short trials sensitive to its starting point and client pauses. Fixed-rate trials separate latency from that controller. A separate native diagnostic client checks acknowledged and consumed counts, contiguous offsets, record CRCs, and per-producer sequences. It uses round-robin partition selection, while OMB uses the Kafka Java client's partitioning. Its finite-burst completion rate includes consumer drain time and is not an OMB sustained-capacity result.

## Final measurements

The [per-trial CSV](omb-small-record-results.csv) contains 55 completed final cases: 32 native trials, 22 ordinary OMB trials, and one explicitly instrumented profiler check. All accepted trials had zero reported publish errors; every native trial consumed exactly its acknowledged count and passed its integrity checks, totaling 792,216,006 records. [Provenance](omb-small-record-provenance.json) records source revisions, binary hashes, toolchains, and limits. The final candidate binary's SHA-256 is `bf78ffafbb93b8d74451b6b98c66f4b13a76f17cb7e008e6ae85ab850fa24c59`.

### Native diagnostic client

Each ordinary cell ran baseline, candidate, candidate, baseline with 15 seconds of production followed by consumer drain. The RF3 three-producer normal-mode cell repeated that sequence, giving four trials per binary. The table reports medians; CPU is the sum of broker process user and system time per acknowledged record, excluding client CPU.

| RF / partitions / producers | Mode | Completion rate, million records/s, baseline → candidate | Change | Broker CPU, µs/record, baseline → candidate |
| --- | --- | ---: | ---: | ---: |
| 1 / 16 / 1 | Normal | 1.566 → 1.490 | −4.9% | 0.308 → 0.320 |
| 1 / 16 / 1 | Direct | 1.327 → 1.332 | +0.4% | 0.754 → 0.669 |
| 3 / 16 / 1 | Normal | 1.258 → 1.230 | −2.2% | 1.470 → 1.467 |
| 3 / 16 / 1 | Direct | 0.842 → 0.978 | +16.1% | 2.955 → 2.340 |
| 1 / 3 / 3 | Normal | 2.047 → 2.040 | −0.3% | Inconclusive¹ |
| 1 / 3 / 3 | Direct | 1.807 → 1.838 | +1.7% | 0.186 → 0.170 |
| 3 / 3 / 3 | Normal | 1.611 → 1.611 | <0.1% | 0.328 → 0.322 |

The RF3 direct 16-partition completion-rate ranges were 0.814–0.870 million/s for the baseline and 0.9774–0.9776 million/s for the candidate. CPU ranges were 2.869–3.041 and 2.323–2.356 µs/record. Both measures separated across the two trials per binary. Publish p99 improved from a median 48.9 to 43.1 ms. This is the strongest result, though two repeats do not support a population-level confidence interval.

The normal 16-partition rate ranges overlapped: RF1 baseline 1.462–1.670 versus candidate 1.455–1.524 million/s; RF3 baseline 1.218–1.298 versus candidate 1.199–1.261 million/s. The initial RF3 three-producer median was 3.4% lower for the candidate; the additional ABBA sequence brought the full-set difference below 0.1%. None of these is evidence of a normal-mode capacity improvement.

¹ RF1 normal three-producer baseline CPU ranged from 0.080 to 0.152 µs/record, versus 0.081–0.083 for the candidate. The apparent 29% median reduction is dominated by one baseline outlier and is not claimed as a gain. The native client limits this case: one candidate trial used 36.45 cgroup CPU-seconds over 17.79 wall-seconds, while the broker used approximately 2.9 CPU-seconds. RF1 three-producer end-to-end p99 also includes seconds of consumer backlog; it is not comparable to the steady fixed-rate OMB latencies below.

### Fixed-rate OMB

Every row uses one minute of warmup and one minute of measurement, without CPU profiling or syscall tracing. Rates are offered records/s. Latencies are milliseconds. Each arrow is baseline → final candidate; the normal RF1 16-partition repeat reverses the execution order. Redpanda columns refer to separate runs with the same workload and offered rate. A dash means no corresponding final Redpanda trial was run.

| Workload | Rate | Publish p99 | Publish p99.9 | End-to-end p99 | Redpanda publish p99 / p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: |
| RF1, 16 partitions, 100 B, normal | 800,000 | 3.518 → 4.974 | 8.515 → 15.393 | 27 → 27 | — |
| Same, reverse-order repeat | 800,000 | 3.863 → 6.149 | 39.261 → 12.969 | 27 → 28 | — |
| RF1, 16 partitions, 100 B, direct | 800,000 | 6.602 → 4.499 | 152.803 → 11.279 | 28 → 26 | — |
| RF3, 1 partition, 1 KiB, normal | 40,000 | 2.322 → 2.771 | 6.358 → 11.333 | 3 → 3 | 3.746 / 19.778 |
| RF3, 16 partitions, 100 B, normal | 200,000 | 4.073 → 4.762 | 8.701 → 12.385 | 486 → 489 | 4.243 / 11.910 |
| RF3, 16 partitions, 100 B, direct | 200,000 | 5.376 → 4.377 | 9.936 → 8.775 | 492 → 488 | — |
| RF3, 3 partitions / 3 producers, 100 B, normal | 350,000 | 4.380 → 4.329 | 10.011 → 14.431 | 5 → 5 | 4.989 / 26.067 |
| RF1, 3 partitions / 3 producers, 100 B, normal | 800,000 | 5.824 → 6.056 | 11.936 → 11.118 | 104 → 118 | 6.751 / 12.678 |
| RF1, 3 partitions / 3 producers, 100 B, direct | 800,000 | 7.153 → 5.025 | 13.365 → 9.816 | 118 → 113 | — |

The direct RF1 16-partition p99.9 reduction is a single matched pair, not a universal 94% tail-latency claim. Direct RF1 three-producer publish p99 improved 30% in its pair, but end-to-end p99.9 worsened from 168 to 182 ms. RF3 direct 16-partition publish tails improved modestly. The normal-mode results do not demonstrate consistent tail-latency improvement. In the RF3 16-partition case, Redpanda's end-to-end p99 was also high, at 462 ms; the approximately 500 ms tails are not unique to krabka on this setup.

The repeated normal RF1 16-partition publish-p99 regression warranted a client control. Broker Produce handling means over the whole trials were 116–122 µs, and 99.8% or more of requests completed within 1 ms for both binaries. The repeat's Java GC pause totals were 3.07 seconds for the baseline and 4.84 seconds for the candidate; the approximate measured portion accounted for 1.38 and 3.22 seconds. These observations do not by themselves prove causation or rule out scheduling outside the handler. In a supplemental candidate-then-baseline control with both Java heaps fixed at 2 GiB, publish p99 was 2.101 → 2.157 ms and p99.9 was 4.917 → 4.169 ms; end-to-end p99 was 25 ms for both. Holding the heap fixed removed most of the earlier difference. This supports client heap behavior as a substantial influence, but one control pair does not prove sole causation. The original settings and unfavorable primary results remain unchanged; the control is not counted in the 55-case primary CSV.

The final profiler check is deliberately excluded from this table. At RF3, three producers, and 350,000 records/s, collecting 30-second CPU profiles on all three brokers produced publish p99.9 of 11.678 ms and end-to-end p99.9 of 13 ms. The same final binary's unprofiled case measured 14.431 and 17 ms. The earlier candidate's instrumented publish p99.9 was 100.341 ms. This supports the profiler fix without treating instrumented trials as ordinary capacity measurements.

## Changes supported by profiles

Direct-mode appends previously always required a blocking-worker handoff, even when the direct writer had already reserved its segment and had queue space. A verbatim batch can now stay on its caller only when its storage work can be queued without waiting. Queue pressure, initialization, reservation growth, segment rolls, flush policies, timestamp stamping, transactional/control batches, and epoch-checkpoint changes keep blocking placement. The check and append hold the same log lock. A follower regression test covers a reconciled offset frontier that would otherwise hide a checkpoint rewrite.

Direct reads can now stay on their caller when the bounded tail cache contains the sparse-index floor and every following byte in the active segment. Evicted, reopened, sealed, and previously direct-written bytes retain blocking placement. Tests cover eviction, truncation, segment rolls, allocation-mode changes, and serving cached bytes after the backing file is truncated.

Large entirely inline fetch plans now use bounded vectored writes. Partial writes advance both the slice list and its first remaining byte; interruptions retry and zero-byte progress fails. Tiny responses retain the existing coalescing path, and file-backed responses retain sendfile. This threshold matters: in five alternating loopback microbenchmark rounds, unrestricted vectored writes made 16-partition, 100-byte response fragments 37% slower. For 16 partitions with 16 KiB or 64 KiB fragments, vectored writes reduced median write time by 57% and 27%, respectively. These are response-fragment sizes, not Kafka record sizes.

In a six-second, straced RF1 direct-mode run with 16 partitions and 100-byte records, the baseline made 4,018 `sendto` plus `writev` calls per million records; the development candidate with vectored fetch writes made 635, an 84% reduction. The final candidate retains that implementation. Tracing perturbs throughput, so these counts explain the mechanism and are not throughput claims.

CPU-profile report construction previously ran synchronously on a Tokio worker. The instrumented RF3 three-producer trials had publish p99.9 values of 195.5 and 100.3 ms; report completion fell in the same ten-second windows as those spikes. An unprofiled repeat on the same candidate, at the same 350,000 records/s, reached 7.73 ms despite comparable memory pressure and page reclaim. The profiler now offloads setup, report construction, encoding, compression, and normal teardown. Instrumented latency measurements are diagnostic evidence, not the final performance comparison.

## Other measurements and rejected experiments

CPU sampling identified sendfile dispatch and record timestamp scanning in normal mode, and direct-I/O wakeups in direct mode. An attempted short-varint scanner optimization was approximately 17% slower in its isolated comparison and was removed. TCP corking improved small sendfile responses but regressed large responses; traced normal-mode requests had only 3.7% of file regions at or below 16 KiB in the 16-partition case and none in the three-producer case. Corking was not retained.

A separate AB/BA experiment released clean sealed-segment pages after completed rollover flushes when cgroup memory use exceeded 87.5% of its limit. It reduced memory use, but its two publish p99.9 results were 43.08 and 10.06 ms versus 7.73 and 8.29 ms in the unprofiled controls. The corresponding p99 values also worsened. No cache-eviction policy was added. Comparable page reclaim in the controls without extreme tails showed why correlation with memory pressure alone was insufficient.

Java Flight Recorder samples also show material client work in histogram recording, atomic phasers, callbacks, and byte-buffer handling. In two two-minute direct-mode OMB trials, actual GC pause totals were 4.15 and 4.75 seconds, with maximum pauses of 19.3 and 42.4 ms. The larger 17.8 and 19.9 second GC-event durations include concurrent collection work and must not be described as stop-the-world time. Client pauses can interact with OMB's backlog-based rate controller; this does not establish a universal client throughput ceiling.

An initial RF3 fixed-rate trace at 50,000 records/s, one partition, and 1 KiB records collected 8,033 spans. Produce handling had a 0.881 ms p99, leader verbatim appends 0.140 ms, and follower verbatim appends 0.165 ms. Idle fetch long polls reached approximately 500 ms; those spans are not equivalent to record end-to-end latency. The OMB publish p99 in that diagnostic trial was 3.636 ms.

A separate syscall-duration trace covered all three brokers during a normal-mode, three-producer 350,000-record/s run. It recorded 246,186 record-log `writev` calls with p99 0.342 ms and p99.9 0.817 ms. The 201 record-log `fdatasync` calls were much longer: median 59.4 ms, p99 118.1 ms. Those calls belong to the existing background rollover flusher; they do not justify charging that entire duration to an ordinary append. The traced client's publish p99 was 16.4 ms, substantially above the untraced controls, illustrating why tracing is separate from the final comparison.

## Correctness and validation

The change preserves wire bytes, persisted formats, replication acknowledgements, flush policies, and direct-queue backpressure. Validation includes 909 storage tests, 20 fetch-writer tests, and 21 broker integration tests covering sendfile, fetch minimum bytes, blocking placement, verbatim production, replication, retries, compaction, and failover. The profiler change also passed 77 telemetry unit tests and four HTTP profiling tests, including a current-thread runtime progress test. Full workspace Clippy with warnings denied, duplicate-code checking, and imported-path checking passed. New tests exercise partial vectored writes across more than 64 slices, zero-progress failure, queue admission under contention and storage failure, cache coverage, and conservative epoch handling.

## Evidence and stopping point

The companion `krabka-performance-evidence.tar.gz` preserves raw OMB results, native-client results, exact configurations and commands, CPU profiles, folded stacks, syscall traces, JFR recordings, OTLP spans, code patches, harness sources, and successful validation logs. Its README describes rebuilding the immutable baseline and candidate and rerunning the final matrix. The repository CSV's `rate` is native completion rate for `collect` rows and mean OMB consume rate for `omb` rows. Missing CPU fields mean that the process was not sampled, including Redpanda containers; they are not zero-CPU measurements.

Rejected runs remain identified in the evidence: an adaptive RF3 trial exhausted local capacity, an early Redpanda trial hit its default storage reserve, and the first RF3 1 KiB Redpanda control reached the harness's disk guard before completing. The last control was rerun unchanged after removing completed build caches. Startup failures are also retained separately. None contributes a performance result.

The retained changes address measured direct-mode costs and profiler interference. Further experiments with the record scanner, socket corking, and cache eviction produced regressions or no useful improvement, so they were removed. Normal-mode measurements on this host increasingly reflect client work, batching, and run-to-run variation. That is the observed point of diminishing returns for this investigation; stronger normal-mode capacity or Redpanda-gap claims require longer runs with separately provisioned load generators.

## Idle workers and the low-load `acks=all` round trip

This section records a design decision from 2026-10-29. Krabka's runtime workers sleep when they have no work, and krabka does not trade idle CPU for latency. This section gives the measurement and the reason.

The v1.1.3 three-broker matrix ([site run 40844196341](https://github.com/krabka-io/krabka-io.github.io/actions/runs/40844196341)) had one fixed-rate cell where Redpanda's publish p99 was lower. The cell has one partition, 100-byte records, RF3, and 50,000 records/s. Krabka's publish p99 was 2.64 ms. Redpanda's was 2.39 ms, and Kafka's was 3.73 ms. End-to-end p99 was 3 ms for krabka and Redpanda. In the same matrix, krabka had the highest throughput in all 13 maximum-rate cells. It had the lowest publish p99 in the other 12 fixed-rate and backlog cells.

A `perf trace` of the leader showed that much of the produce's remote phase is one runtime worker waking another. Each wake is a futex or an eventfd write that brings an idle thread back onto a CPU. Redpanda's shards poll their queues and do not sleep, so they do not pay this cost. Three local trials measured it on one host:

| Condition | Krabka publish p50 | Krabka publish p99 |
| --- | ---: | ---: |
| Default idle states | 0.849 ms | 1.523 ms |
| `/dev/cpu_dma_latency` held at 20 µs | 0.851 ms | 1.517 ms |
| `/dev/cpu_dma_latency` held at 0, no idle states | 0.814 ms | 1.466 ms |

The depth of the idle state did not matter; the cost is that a worker sleeps at all, and it is about 35 µs at p50 and 55 µs at p99. That is not the whole gap. A run of the site's runner with `/dev/cpu_dma_latency` held at 0 for both brokers, in one session on the same host, measured krabka at 0.815 to 0.816 ms at p50 and 1.465 to 1.466 ms at p99, and Redpanda at 0.736 to 0.746 ms and 1.340 to 1.355 ms. Removing idle states made both brokers faster and left krabka about 75 µs behind at p50.

The rest of the gap is CPU work on the leader. A packet capture on the bridge timed each produce's hops: Redpanda sends to its followers sooner after the produce arrives, and answers the producer after the first follower replies, where krabka keeps Kafka's `acks=all` and waits for every in-sync replica. Frame-pointer profiles of the leader put about 40% of its CPU in follower fetches. A replica fetcher's session holds every partition the follower replicates from this leader, which on three brokers is about 17, most of them idle `__consumer_offsets` partitions, and the leader checks, reads and records each of them on every fetch.

Redpanda's figures vary between runs on the same host. A later run of the site's runner put both brokers on the same host in one session, with krabka built from the change described below. Each broker ran two 5-minute rounds:

| Broker | Publish p50 | p95 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| Krabka, round 1 | 0.851 ms | 1.454 ms | 1.522 ms | 1.608 ms |
| Krabka, round 2 | 0.854 ms | 1.461 ms | 1.524 ms | 1.605 ms |
| Redpanda, round 1 | 0.769 ms | 1.329 ms | 1.400 ms | 1.481 ms |
| Redpanda, round 2 | 0.755 ms | 1.312 ms | 1.376 ms | 1.460 ms |

In that run Redpanda's publish latency was about 90 µs lower at p50 and about 130 µs lower at p99. End-to-end latency was 1 ms at p50 and 2 ms at p99 for both brokers in every round. The benchmark reports end-to-end latency in whole milliseconds.

Krabka removes the wakes that have no work behind them. A parked follower fetch now wakes only on an append or a leader change. A parked consumer fetch wakes only when the high watermark or the delivery watermark moves. Before, both re-read the partition on every `acks=all` produce and found nothing. Four alternating trials measured p50 0.844 to 0.845 ms against 0.852 to 0.860 ms, and p99 1.508 to 1.509 ms against 1.523 to 1.534 ms.

Krabka does not buy back the wake cost by polling. A worker that spins before it sleeps, or a socket that busy-polls, holds a CPU at 100% while the broker has no work. On a shared host or a cloud instance, that CPU is paid for whether or not records arrive. It is also unavailable to other processes, which includes the clients on a shared host. Krabka's workers use no CPU when idle. At 50,000 records/s the leader used about 0.47 CPU, and each follower used about 0.06 CPU. A polling design pays its idle cost at every load. Its saving appears only when the load is low enough for the workers to sleep. At higher loads the workers sleep less, and the matrix shows krabka ahead in every other cell. That is the better trade for krabka's users, and krabka keeps it.
