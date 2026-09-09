# Broker performance results

This result applies to the workload and host below. It is neither a universal
broker ranking nor a supported-limit claim. The full raw artifact directory is
produced by the [qualification harness](performance-qualification.md).

## Provenance and contract

The run used Krabka commit `106ae27b121c8317ba86c10e462fa8c9baaad3b1`
plus a source patch whose SHA-256 is
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
The Krabka image id was
`sha256:d46923ca9c40895650975aea38c535758cb5958ad6bcc3cc21a79aa4d0c1f624`;
the pinned `apache/kafka:4.0.0` image id was
`sha256:3f7b939115cd4872e9cee9369d80bd69712fde55f9902f46d793f64848dedc75`.

The host had 16 logical CPUs (AMD EPYC 4344P), 65.9 GB RAM and 361.9 GB
available disk. Each of three broker/controller pods had a 2 CPU, 2 GiB and
100 GiB claim. Both sides used 12 partitions, RF 3, min ISR 2, `acks=all`,
idempotence, LZ4, a 65,536-byte batch and 5 ms linger with 1,024-byte records.

Reproduce the complete run with:

```sh
PERF_ARTIFACT_DIR=$PWD/performance-artifacts \
  packaging/performance/qualify.sh full
```

## Kafka comparison

Every row is a raw run result. Saturation sent 100,000 records; steady state
sent 300,000 at a requested 5,000 records/s. Every run consumed exactly what
it sent with zero producer errors and zero duplicate sequences.

| Broker | Shape | Run | records/s | MiB/s | p50 ms | p95 ms | p99 ms |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| Kafka | saturation | 1 | 171,307.957 | 167.293 | 252.705 | 338.327 | 378.705 |
| Kafka | saturation | 2 | 344,926.749 | 336.843 | 10.567 | 20.193 | 26.417 |
| Kafka | saturation | 3 | 300,566.675 | 293.522 | 38.088 | 60.924 | 80.089 |
| Krabka | saturation | 1 | 185,568.285 | 181.219 | 169.953 | 252.837 | 315.207 |
| Krabka | saturation | 2 | 172,020.136 | 167.988 | 179.715 | 265.506 | 293.146 |
| Krabka | saturation | 3 | 169,372.170 | 165.403 | 183.986 | 299.008 | 311.533 |
| Kafka | steady | 1 | 4,999.505 | 4.882 | 3.244 | 5.790 | 6.465 |
| Kafka | steady | 2 | 4,999.438 | 4.882 | 3.317 | 6.400 | 212.183 |
| Kafka | steady | 3 | 4,999.441 | 4.882 | 3.110 | 5.623 | 5.968 |
| Krabka | steady | 1 | 4,999.266 | 4.882 | 4.582 | 7.193 | 7.739 |
| Krabka | steady | 2 | 4,999.391 | 4.882 | 4.674 | 7.203 | 7.864 |
| Krabka | steady | 3 | 4,999.409 | 4.882 | 4.719 | 7.121 | 7.790 |

The saturation medians were 300,567 records/s and 80.089 ms p99 for Kafka,
and 172,020 records/s and 311.533 ms p99 for Krabka, a 1.75x throughput gap.
The first saturation run was 171,308 records/s for Kafka and 185,568 records/s
for Krabka, so the gap is concentrated in Kafka's warm runs. At the fixed rate,
both held 4,999 records/s; median p99 was 6.465 ms for Kafka and 7.790 ms for
Krabka.

Across the three steady runs, the three broker processes consumed 1,234--3,273
CPU ticks per Kafka run and 3,705--4,091 per Krabka run. Peak per-process RSS
was 945 MiB for Kafka and 92 MiB for Krabka. Aggregate volume growth was
26.3 MB on both brokers. Kafka received 43.2--43.6 MB and transmitted
43.6--44.0 MB; Krabka received 47.8--48.2 MB and transmitted 48.3--48.7 MB.
The raw before/after snapshots also retain per-process file descriptors and
every individual run.

## Partition envelope

Each tier used RF 3 and 600,000 acknowledged records at 1,000 records/s while
deleting the active controller and moving 1,000 partitions to a fourth broker.
Both tiers passed their predeclared 30-minute deadline.

| User partitions | User replicas | User replicas per broker after move | Failover | Restart ready | Reassignment ready | p50 / p95 / p99 | Result |
| ---: | ---: | :--- | ---: | ---: | ---: | :--- | :--- |
| 1,000 | 3,000 | 667 / 667 / 666 / 1,000 | 22 s | 24 s | 0 s | 22.680 / 296.989 / 10,084.009 ms | pass |
| 10,000 | 30,000 | 9,667 / 9,667 / 9,666 / 1,000 | 23 s | 26 s | 2 s | 430.922 / 3,487.868 / 12,756.453 ms | pass |

Both tiers consumed 600,000 of 600,000 records with zero errors and duplicates.
All four brokers ended with metadata lag 0. At 10,000 partitions, the largest
broker metrics body was 2,376,748 bytes with 27,249 series and the slowest
scrape took 1.121 seconds. Peak per-process RSS was 890,356 KiB, peak file
descriptor count was 30,178, and peak volume use was 64,039,262 bytes.
`scale/verdict.txt` therefore records `highest_passing_tier=10000`; this is the
highest tested tier, not the product's maximum.
