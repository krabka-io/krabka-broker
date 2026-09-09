# Broker performance results

This result applies to the workload and host below. It is neither a universal
broker ranking nor a supported-limit claim. The full raw artifact directory is
produced by the [qualification harness](performance-qualification.md).

## Provenance and contract

The run used Krabka commit `b04c22d5b422bf43f737de55de6567a0011b2c86`
plus a source patch whose SHA-256 is
`b8ff4efd6354d887d44e1334619ce4fd22cb74b516dec4df631370879eb43201`.
The Krabka image id was
`sha256:5f1161dcd9431ee75da68998adbcd5fb4941563f087a210e79e36cca80b1eb6d`;
the pinned `apache/kafka:4.0.0` image id was
`sha256:3f7b939115cd4872e9cee9369d80bd69712fde55f9902f46d793f64848dedc75`.

The host had 16 logical CPUs (AMD EPYC 4344P), 65.9 GB RAM and 547.7 GB
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
| Kafka | saturation | 1 | 160,476.415 | 156.715 | 298.509 | 368.587 | 389.876 |
| Kafka | saturation | 2 | 337,283.015 | 329.378 | 6.924 | 20.588 | 29.543 |
| Kafka | saturation | 3 | 382,837.328 | 373.865 | 19.360 | 29.813 | 35.510 |
| Krabka | saturation | 1 | 78,931.278 | 77.081 | 565.825 | 881.522 | 941.920 |
| Krabka | saturation | 2 | 76,417.276 | 74.626 | 588.172 | 896.159 | 964.327 |
| Krabka | saturation | 3 | 76,419.269 | 74.628 | 632.635 | 909.588 | 970.326 |
| Kafka | steady | 1 | 4,999.361 | 4.882 | 3.283 | 5.838 | 6.464 |
| Kafka | steady | 2 | 4,999.532 | 4.882 | 3.066 | 5.576 | 5.960 |
| Kafka | steady | 3 | 4,999.545 | 4.882 | 3.094 | 5.608 | 5.949 |
| Krabka | steady | 1 | 4,999.312 | 4.882 | 5.323 | 7.852 | 8.492 |
| Krabka | steady | 2 | 4,999.300 | 4.882 | 5.406 | 7.923 | 8.612 |
| Krabka | steady | 3 | 4,999.288 | 4.882 | 5.596 | 8.172 | 8.827 |

The saturation medians were 337,283 records/s and 35.510 ms p99 for Kafka,
and 76,419 records/s and 964.327 ms p99 for Krabka. At the fixed rate, both
held 4,999 records/s; median p99 was 5.960 ms for Kafka and 8.612 ms for
Krabka.

Across the three steady runs, the three broker processes consumed 1,141--3,320
CPU ticks per Kafka run and 5,447--5,677 per Krabka run. Peak per-process RSS
was 952 MiB for Kafka and 104 MiB for Krabka. Aggregate volume growth was
26.3--26.4 MB and network receive/transmit growth was 43.3--44.2 MB for Kafka;
Krabka recorded 26.8--26.9 MB and 47.9--49.1 MB. The raw before/after snapshots
also retain per-process file descriptors and every individual run.

## Partition envelope

Each tier used RF 3 and 600,000 acknowledged records at 1,000 records/s while
deleting the active controller and moving 1,000 partitions to a fourth broker.
Both tiers passed their predeclared 30-minute deadline.

| User partitions | User replicas | User replicas per broker after move | Failover | Restart ready | Reassignment ready | p50 / p95 / p99 | Result |
| ---: | ---: | :--- | ---: | ---: | ---: | :--- | :--- |
| 1,000 | 3,000 | 667 / 667 / 666 / 1,000 | 22 s | 25 s | 1 s | 11.959 / 81.426 / 10,368.438 ms | pass |
| 10,000 | 30,000 | 9,667 / 9,667 / 9,666 / 1,000 | 24 s | 26 s | 1 s | 425.212 / 1,075.863 / 12,273.299 ms | pass |

Both tiers consumed 600,000 of 600,000 records with zero errors and duplicates.
All four brokers ended with metadata lag 0. At 10,000 partitions, the largest
broker metrics body was 2,340,650 bytes with 27,132 series and the slowest
scrape took 1.124 seconds. Peak per-process RSS was 490,320 KiB, peak file
descriptor count was 30,177, and peak volume use was 63,953,511 bytes.
`scale/verdict.txt` therefore records `highest_passing_tier=10000`; this is the
highest tested tier, not the product's maximum.
