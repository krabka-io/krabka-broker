# Performance budget: `rf3-regression-v2`

These results compare one candidate build with the pinned Apache Kafka image under the `rf3-regression-v2` contract on the host below. They are measured limits of that contract, not a ranking of brokers and not a supported limit.

**Verdict: pass.**

## Provenance

| Side | Label | Revision | Image | Digest |
| :--- | :--- | :--- | :--- | :--- |
| reference | kafka | `4.3.1` | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| candidate | krabka | `fd007630813bd8c75fac51fb154177cd22a5e71f` | `docker.io/krabka-io/krabka-broker:budget-candidate` | `sha256:7e6800159a55991ab5762e377725beec90f7ff8d9c618aba52a31f15e2c323d8` |

Client: `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`). Contract digest: `038d48e5ff398467a7ce5f97bb239f8378e21102e7113344693c65df725eeded`. Started 2026-10-23T01:33:09Z.

Host: Intel(R) Xeon(R) CPU @ 2.20GHz, 32 logical CPUs, 135056584704 bytes of memory, 1216255946752 bytes of free disk, kernel 7.0.0-1011-gcp.

Cluster: 3 combined broker/controllers, replication factor 3, `min.insync.replicas=2`, `acks=all`, idempotence, 65,536-byte batches, 5 ms linger. 5 interleaved rounds; each round runs every cell once per side.

Settings: `acks=all` `batch_size=65536` `broker_cpu_limit=2` `broker_disk_request=100Gi` `broker_memory_limit=2Gi` `enable_idempotence=true` `krabka_compilation_mode=opt` `linger_ms=5` `source_patch_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Budgets

Declared before the run. A regression is material when the median gets worse by more than the materiality floor and by more than the multiplier times the larger side's relative median absolute deviation. A side whose spread exceeds the variance budget makes the cell inconclusive.

| Metric | Materiality | Spread multiplier | Variance budget |
| :--- | ---: | ---: | ---: |
| records/s | 5.0% | 3.0 | 15.0% |
| p99 ms | 10.0% | 3.0 | 50.0% |
| CPU µs/record | 5.0% | 3.0 | 10.0% |
| peak RSS MiB | 10.0% | 3.0 | 10.0% |

## Results

Medians over the interleaved rounds. A change is relative to the reference median; positive is worse.

| Cell | Metric | Reference | Candidate | Change | Spread (ref / cand) | Allowed | Status |
| :--- | :--- | ---: | ---: | ---: | :--- | ---: | :--- |
| `1k-random-lz4-p12-saturate` | records/s | 105089 | 191793 | -82.5% | 1.2% / 7.4% | +22.2% | improved |
| `1k-random-lz4-p12-saturate` | p50 ms | 238.759 | 116.187 | -51.3% | 2.9% / 1.2% | — | recorded |
| `1k-random-lz4-p12-saturate` | p95 ms | 520.815 | 261.706 | -49.8% | 6.3% / 16.3% | — | recorded |
| `1k-random-lz4-p12-saturate` | p99 ms | 1033.665 | 954.133 | -7.7% | 17.0% / 8.3% | — | recorded |
| `1k-random-lz4-p12-saturate` | CPU µs/record | 54.727 | 18.380 | -66.4% | 1.9% / 1.9% | +5.7% | improved |
| `1k-random-lz4-p12-saturate` | peak RSS MiB | 2855.3 | 140.2 | -95.1% | 0.2% / 2.3% | +10.0% | improved |
| `100b-random-lz4-p12-saturate` | records/s | 867126 | 999877 | -15.3% | 1.5% / 3.7% | +11.0% | improved |
| `100b-random-lz4-p12-saturate` | p50 ms | 16.518 | 5.835 | -64.7% | 2.8% / 0.7% | — | recorded |
| `100b-random-lz4-p12-saturate` | p95 ms | 48.799 | 20.512 | -58.0% | 2.0% / 13.4% | — | recorded |
| `100b-random-lz4-p12-saturate` | p99 ms | 77.265 | 57.093 | -26.1% | 2.1% / 29.9% | — | recorded |
| `100b-random-lz4-p12-saturate` | CPU µs/record | 6.398 | 3.445 | -46.2% | 1.1% / 0.7% | +5.0% | improved |
| `100b-random-lz4-p12-saturate` | peak RSS MiB | 2877.2 | 146.9 | -94.9% | 0.2% / 0.5% | +10.0% | improved |
| `1k-zeros-lz4-p12-saturate` | records/s | 711465 | 739670 | -4.0% | 1.7% / 1.4% | +5.2% | within budget |
| `1k-zeros-lz4-p12-saturate` | p50 ms | 183.625 | 154.276 | -16.0% | 4.3% / 38.7% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p95 ms | 366.063 | 296.167 | -19.1% | 6.9% / 28.8% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p99 ms | 410.451 | 335.117 | -18.4% | 21.5% / 21.1% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | CPU µs/record | 3.733 | 2.270 | -39.2% | 6.5% / 2.9% | +19.6% | improved |
| `1k-zeros-lz4-p12-saturate` | peak RSS MiB | 2882.0 | 174.1 | -94.0% | 0.1% / 1.6% | +10.0% | improved |
| `1k-random-none-p12-saturate` | records/s | 161082 | 179255 | -11.3% | 6.6% / 4.3% | +19.9% | within budget |
| `1k-random-none-p12-saturate` | p50 ms | 155.109 | 133.927 | -13.7% | 1.0% / 1.5% | — | recorded |
| `1k-random-none-p12-saturate` | p95 ms | 445.445 | 264.605 | -40.6% | 31.3% / 11.4% | — | recorded |
| `1k-random-none-p12-saturate` | p99 ms | 1295.011 | 1323.724 | +2.2% | 12.3% / 5.0% | — | recorded |
| `1k-random-none-p12-saturate` | CPU µs/record | 26.220 | 20.233 | -22.8% | 1.9% / 0.4% | +5.8% | improved |
| `1k-random-none-p12-saturate` | peak RSS MiB | 2898.3 | 185.7 | -93.6% | 0.1% / 4.2% | +12.7% | improved |
| `100k-random-lz4-p12-saturate` | records/s | 1492 | 1869 | -25.2% | 3.4% / 5.0% | +14.9% | improved |
| `100k-random-lz4-p12-saturate` | p50 ms | 118.773 | 117.949 | -0.7% | 0.6% / 2.4% | — | recorded |
| `100k-random-lz4-p12-saturate` | p95 ms | 1153.415 | 607.865 | -47.3% | 0.8% / 48.6% | — | recorded |
| `100k-random-lz4-p12-saturate` | p99 ms | 1380.744 | 1236.300 | -10.5% | 8.3% / 8.3% | — | recorded |
| `100k-random-lz4-p12-saturate` | CPU µs/record | 1978.000 | 1513.000 | -23.5% | 2.6% / 1.0% | +7.8% | improved |
| `100k-random-lz4-p12-saturate` | peak RSS MiB | 2904.6 | 192.0 | -93.4% | 0.2% / 4.1% | +12.4% | improved |
| `1k-random-lz4-p1-saturate` | records/s | 43714 | 48535 | -11.0% | 0.3% / 0.3% | +5.0% | improved |
| `1k-random-lz4-p1-saturate` | p50 ms | 675.530 | 613.088 | -9.2% | 0.5% / 0.6% | — | recorded |
| `1k-random-lz4-p1-saturate` | p95 ms | 749.007 | 648.965 | -13.4% | 1.7% / 1.9% | — | recorded |
| `1k-random-lz4-p1-saturate` | p99 ms | 763.974 | 690.419 | -9.6% | 0.7% / 4.7% | — | recorded |
| `1k-random-lz4-p1-saturate` | CPU µs/record | 44.050 | 38.520 | -12.6% | 0.4% / 0.5% | +5.0% | improved |
| `1k-random-lz4-p1-saturate` | peak RSS MiB | 2911.7 | 193.0 | -93.4% | 0.2% / 4.2% | +12.7% | improved |
| `1k-random-lz4-p12-fixed-20k` | records/s | 19994 | 19993 | +0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p12-fixed-20k` | p50 ms | 3.986 | 3.988 | +0.1% | 0.2% / 0.9% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p95 ms | 5.712 | 5.596 | -2.0% | 0.2% / 0.7% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p99 ms | 22.199 | 8.092 | -63.5% | 6.1% / 1.6% | +18.3% | improved |
| `1k-random-lz4-p12-fixed-20k` | CPU µs/record | 59.683 | 53.950 | -9.6% | 0.5% / 0.9% | +5.0% | improved |
| `1k-random-lz4-p12-fixed-20k` | peak RSS MiB | 2920.8 | 195.3 | -93.3% | 0.1% / 4.7% | +14.0% | improved |
| `1k-random-lz4-p1000-fixed-5k` | records/s | 4999 | 4999 | +0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | p50 ms | 9.776 | 10.413 | +6.5% | 0.5% / 2.0% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p95 ms | 50.766 | 17.280 | -66.0% | 5.6% / 6.7% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p99 ms | 125.886 | 38.846 | -69.1% | 3.9% / 30.2% | +90.6% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | CPU µs/record | 414.033 | 406.233 | -1.9% | 0.6% / 1.9% | +5.6% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | peak RSS MiB | 3256.1 | 252.4 | -92.2% | 0.2% / 1.5% | +10.0% | improved |
| `1k-random-lz4-p12-idle-burst` | records/s | 49808 | 49737 | +0.1% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p12-idle-burst` | p50 ms | 29.440 | 50.436 | +71.3% | 0.3% / 6.7% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p95 ms | 88.018 | 107.584 | +22.2% | 2.7% / 6.3% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p99 ms | 114.263 | 149.699 | +31.0% | 3.0% / 4.1% | +12.4% | material difference (not gating) |
| `1k-random-lz4-p12-idle-burst` | CPU µs/record | 146.360 | 132.040 | -9.8% | 1.4% / 0.3% | +5.0% | improved |
| `1k-random-lz4-p12-idle-burst` | peak RSS MiB | 3287.4 | 271.0 | -91.8% | 0.1% / 1.4% | +10.0% | improved |

## Raw trials

| Order | Cell | Round | Side | records/s | p50 ms | p95 ms | p99 ms | CPU µs/record | peak RSS MiB | Outcome |
| ---: | :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 1 | `1k-random-lz4-p12-saturate` | 1 | reference | 94066 | 261.857 | 609.849 | 1209.603 | 59.040 | 2863.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 2 | `100b-random-lz4-p12-saturate` | 1 | reference | 852437 | 16.202 | 48.970 | 77.265 | 6.471 | 2877.2 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 3 | `1k-zeros-lz4-p12-saturate` | 1 | reference | 689216 | 181.036 | 366.063 | 405.063 | 3.967 | 2880.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 4 | `1k-random-none-p12-saturate` | 1 | reference | 148852 | 157.765 | 445.445 | 1500.995 | 26.730 | 2894.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 5 | `100k-random-lz4-p12-saturate` | 1 | reference | 1417 | 131.346 | 1198.323 | 1380.744 | 2160.333 | 2920.6 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 6 | `1k-random-lz4-p1-saturate` | 1 | reference | 39364 | 750.272 | 841.854 | 881.218 | 48.680 | 2920.6 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 7 | `1k-random-lz4-p12-fixed-20k` | 1 | reference | 19994 | 4.216 | 5.958 | 22.199 | 63.883 | 2926.1 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 8 | `1k-random-lz4-p1000-fixed-5k` | 1 | reference | 4999 | 10.404 | 53.753 | 130.750 | 435.667 | 3256.1 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 9 | `1k-random-lz4-p12-idle-burst` | 1 | reference | 49681 | 29.440 | 88.489 | 116.000 | 148.860 | 3292.5 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 10 | `1k-random-lz4-p12-saturate` | 1 | candidate | 188077 | 125.457 | 346.228 | 954.133 | 19.483 | 146.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 11 | `100b-random-lz4-p12-saturate` | 1 | candidate | 999877 | 6.850 | 20.512 | 40.014 | 3.654 | 147.6 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 12 | `1k-zeros-lz4-p12-saturate` | 1 | candidate | 739670 | 120.076 | 232.396 | 286.168 | 2.390 | 171.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 13 | `1k-random-none-p12-saturate` | 1 | candidate | 179255 | 147.573 | 264.605 | 1369.276 | 21.323 | 177.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 14 | `100k-random-lz4-p12-saturate` | 1 | candidate | 2034 | 122.333 | 283.164 | 1096.065 | 1609.000 | 182.5 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 15 | `1k-random-lz4-p1-saturate` | 1 | candidate | 45833 | 642.600 | 693.057 | 731.630 | 40.590 | 183.1 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 16 | `1k-random-lz4-p12-fixed-20k` | 1 | candidate | 19993 | 4.217 | 5.877 | 9.287 | 58.100 | 185.4 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 17 | `1k-random-lz4-p1000-fixed-5k` | 1 | candidate | 4997 | 11.378 | 19.897 | 50.575 | 426.200 | 252.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 18 | `1k-random-lz4-p12-idle-burst` | 1 | candidate | 49747 | 55.711 | 139.307 | 170.097 | 133.560 | 271.0 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 19 | `1k-random-lz4-p12-saturate` | 2 | candidate | 174411 | 124.565 | 382.667 | 1157.285 | 19.437 | 140.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 20 | `100b-random-lz4-p12-saturate` | 2 | candidate | 961963 | 5.639 | 17.246 | 57.093 | 3.657 | 143.4 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 21 | `1k-zeros-lz4-p12-saturate` | 2 | candidate | 645184 | 281.167 | 625.271 | 832.659 | 2.383 | 174.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 22 | `1k-random-none-p12-saturate` | 2 | candidate | 194811 | 144.770 | 241.463 | 971.580 | 21.357 | 185.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 23 | `100k-random-lz4-p12-saturate` | 2 | candidate | 1912 | 120.419 | 312.266 | 1236.300 | 1591.667 | 192.0 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 24 | `1k-random-lz4-p1-saturate` | 2 | candidate | 46381 | 635.744 | 692.019 | 722.274 | 40.170 | 193.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 25 | `1k-random-lz4-p12-fixed-20k` | 2 | candidate | 19993 | 4.293 | 5.953 | 8.168 | 59.583 | 195.3 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 26 | `1k-random-lz4-p1000-fixed-5k` | 2 | candidate | 4999 | 11.404 | 20.392 | 66.727 | 424.167 | 259.3 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 27 | `1k-random-lz4-p12-idle-burst` | 2 | candidate | 49724 | 59.775 | 119.223 | 155.889 | 132.540 | 274.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 28 | `1k-random-lz4-p12-saturate` | 2 | reference | 106362 | 245.567 | 507.408 | 792.306 | 55.773 | 2855.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 29 | `100b-random-lz4-p12-saturate` | 2 | reference | 853949 | 18.596 | 49.756 | 78.870 | 6.403 | 2877.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 30 | `1k-zeros-lz4-p12-saturate` | 2 | reference | 723736 | 93.514 | 199.583 | 252.113 | 3.733 | 2882.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 31 | `1k-random-none-p12-saturate` | 2 | reference | 162706 | 155.109 | 571.795 | 1135.927 | 26.220 | 2899.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 32 | `100k-random-lz4-p12-saturate` | 2 | reference | 1492 | 118.773 | 1153.415 | 1706.126 | 1926.667 | 2899.4 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 33 | `1k-random-lz4-p1-saturate` | 2 | reference | 44452 | 661.070 | 758.870 | 771.124 | 43.610 | 2907.1 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 34 | `1k-random-lz4-p12-fixed-20k` | 2 | reference | 19994 | 4.000 | 5.712 | 20.845 | 59.400 | 2919.2 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 35 | `1k-random-lz4-p1000-fixed-5k` | 2 | reference | 4999 | 9.705 | 51.619 | 125.886 | 404.600 | 3265.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 36 | `1k-random-lz4-p12-idle-burst` | 2 | reference | 49808 | 29.472 | 88.018 | 110.798 | 146.360 | 3285.4 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 37 | `1k-random-lz4-p12-saturate` | 3 | reference | 105089 | 238.759 | 520.815 | 1143.429 | 54.727 | 2858.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 38 | `100b-random-lz4-p12-saturate` | 3 | reference | 867126 | 16.058 | 41.677 | 83.899 | 6.324 | 2882.9 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 39 | `1k-zeros-lz4-p12-saturate` | 3 | reference | 680448 | 183.625 | 391.405 | 508.062 | 3.490 | 2884.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 40 | `1k-random-none-p12-saturate` | 3 | reference | 171772 | 149.818 | 306.199 | 1143.424 | 25.113 | 2893.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 41 | `100k-random-lz4-p12-saturate` | 3 | reference | 1546 | 114.093 | 1148.141 | 1265.517 | 1978.000 | 2909.0 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 42 | `1k-random-lz4-p1-saturate` | 3 | reference | 43714 | 678.838 | 736.313 | 759.073 | 44.230 | 2920.3 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 43 | `1k-random-lz4-p12-fixed-20k` | 3 | reference | 19994 | 3.986 | 5.683 | 24.962 | 59.583 | 2924.6 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 44 | `1k-random-lz4-p1000-fixed-5k` | 3 | reference | 4999 | 9.823 | 50.766 | 117.975 | 414.033 | 3253.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 45 | `1k-random-lz4-p12-idle-burst` | 3 | reference | 49787 | 28.147 | 80.129 | 97.467 | 148.480 | 3287.4 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 46 | `1k-random-lz4-p12-saturate` | 3 | candidate | 207361 | 115.436 | 218.967 | 650.742 | 18.307 | 143.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 47 | `100b-random-lz4-p12-saturate` | 3 | candidate | 975995 | 5.792 | 170.211 | 588.910 | 3.421 | 146.9 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 48 | `1k-zeros-lz4-p12-saturate` | 3 | candidate | 750219 | 154.276 | 296.167 | 335.117 | 2.240 | 201.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 49 | `1k-random-none-p12-saturate` | 3 | candidate | 178412 | 131.863 | 569.375 | 1323.724 | 20.153 | 204.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 50 | `100k-random-lz4-p12-saturate` | 3 | candidate | 1869 | 111.118 | 607.865 | 1556.334 | 1498.333 | 208.2 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 51 | `1k-random-lz4-p1-saturate` | 3 | candidate | 48555 | 609.522 | 648.965 | 690.419 | 38.520 | 209.9 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 52 | `1k-random-lz4-p12-fixed-20k` | 3 | candidate | 19993 | 3.988 | 5.596 | 7.963 | 53.950 | 211.0 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 53 | `1k-random-lz4-p1000-fixed-5k` | 3 | candidate | 4999 | 10.201 | 16.115 | 25.564 | 398.600 | 268.7 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 54 | `1k-random-lz4-p12-idle-burst` | 3 | candidate | 49737 | 47.040 | 102.019 | 149.699 | 131.980 | 283.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 55 | `1k-random-lz4-p12-saturate` | 4 | candidate | 191793 | 116.187 | 261.706 | 875.387 | 18.380 | 136.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 56 | `100b-random-lz4-p12-saturate` | 4 | candidate | 1039520 | 5.842 | 17.755 | 34.806 | 3.436 | 142.7 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 57 | `1k-zeros-lz4-p12-saturate` | 4 | candidate | 753834 | 63.596 | 167.815 | 264.365 | 2.270 | 189.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 58 | `1k-random-none-p12-saturate` | 4 | candidate | 195786 | 133.503 | 234.433 | 1254.625 | 20.177 | 191.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 59 | `100k-random-lz4-p12-saturate` | 4 | candidate | 1753 | 117.949 | 750.262 | 1339.482 | 1513.000 | 194.3 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 60 | `1k-random-lz4-p1-saturate` | 4 | candidate | 48535 | 613.088 | 648.004 | 658.263 | 38.340 | 195.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 61 | `1k-random-lz4-p12-fixed-20k` | 4 | candidate | 19993 | 3.966 | 5.556 | 8.092 | 53.483 | 196.3 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 62 | `1k-random-lz4-p1000-fixed-5k` | 4 | candidate | 4999 | 10.413 | 17.280 | 38.846 | 406.233 | 252.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 63 | `1k-random-lz4-p12-idle-burst` | 4 | candidate | 49731 | 50.436 | 100.829 | 148.247 | 132.040 | 269.2 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 64 | `1k-random-lz4-p12-saturate` | 4 | reference | 104370 | 226.552 | 684.472 | 1033.665 | 53.163 | 2847.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 65 | `100b-random-lz4-p12-saturate` | 4 | reference | 884679 | 18.362 | 44.034 | 66.371 | 6.274 | 2870.3 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 66 | `1k-zeros-lz4-p12-saturate` | 4 | reference | 718312 | 191.512 | 344.472 | 410.451 | 3.980 | 2878.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 67 | `1k-random-none-p12-saturate` | 4 | reference | 136846 | 156.717 | 1046.752 | 2179.881 | 25.643 | 2903.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 68 | `100k-random-lz4-p12-saturate` | 4 | reference | 1466 | 119.480 | 1099.925 | 2194.838 | 1988.667 | 2904.6 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 69 | `1k-random-lz4-p1-saturate` | 4 | reference | 43596 | 672.102 | 749.007 | 763.974 | 44.050 | 2908.3 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 70 | `1k-random-lz4-p12-fixed-20k` | 4 | reference | 19993 | 3.980 | 5.725 | 26.624 | 60.217 | 2920.8 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 71 | `1k-random-lz4-p1000-fixed-5k` | 4 | reference | 4999 | 9.762 | 46.732 | 111.624 | 416.633 | 3267.5 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 72 | `1k-random-lz4-p12-idle-burst` | 4 | reference | 49874 | 29.346 | 85.674 | 114.263 | 145.320 | 3281.5 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 73 | `1k-random-lz4-p12-saturate` | 5 | reference | 109280 | 234.215 | 487.870 | 795.408 | 54.563 | 2850.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 74 | `100b-random-lz4-p12-saturate` | 5 | reference | 868498 | 16.518 | 48.799 | 76.125 | 6.398 | 2887.9 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 75 | `1k-zeros-lz4-p12-saturate` | 5 | reference | 711465 | 212.136 | 432.713 | 498.653 | 3.317 | 2887.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 76 | `1k-random-none-p12-saturate` | 5 | reference | 161082 | 154.889 | 285.552 | 1295.011 | 26.297 | 2898.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 77 | `100k-random-lz4-p12-saturate` | 5 | reference | 1543 | 118.070 | 1162.217 | 1357.687 | 1821.667 | 2900.2 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 78 | `1k-random-lz4-p1-saturate` | 5 | reference | 43737 | 675.530 | 736.443 | 758.838 | 44.020 | 2911.7 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 79 | `1k-random-lz4-p12-fixed-20k` | 5 | reference | 19994 | 3.983 | 5.701 | 21.354 | 59.683 | 2913.3 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 80 | `1k-random-lz4-p1000-fixed-5k` | 5 | reference | 4999 | 9.776 | 47.936 | 126.085 | 412.400 | 3248.3 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 81 | `1k-random-lz4-p12-idle-burst` | 5 | reference | 49829 | 29.834 | 94.773 | 125.971 | 143.200 | 3291.9 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 82 | `1k-random-lz4-p12-saturate` | 5 | candidate | 205993 | 114.746 | 250.867 | 1031.988 | 18.030 | 137.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 83 | `100b-random-lz4-p12-saturate` | 5 | candidate | 1036459 | 5.835 | 21.901 | 62.144 | 3.445 | 147.6 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 84 | `1k-zeros-lz4-p12-saturate` | 5 | candidate | 730028 | 213.942 | 381.562 | 430.601 | 2.203 | 171.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 85 | `1k-random-none-p12-saturate` | 5 | candidate | 171625 | 133.927 | 496.379 | 1389.927 | 20.233 | 177.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 86 | `100k-random-lz4-p12-saturate` | 5 | candidate | 1776 | 115.088 | 1086.952 | 1162.461 | 1509.000 | 184.1 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 87 | `1k-random-lz4-p1-saturate` | 5 | candidate | 48661 | 610.197 | 636.462 | 651.805 | 38.520 | 184.8 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 88 | `1k-random-lz4-p12-fixed-20k` | 5 | candidate | 19994 | 3.954 | 5.555 | 7.655 | 53.450 | 186.2 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 89 | `1k-random-lz4-p1000-fixed-5k` | 5 | candidate | 4999 | 10.268 | 16.632 | 34.726 | 402.767 | 248.7 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 90 | `1k-random-lz4-p12-idle-burst` | 5 | candidate | 49746 | 49.588 | 107.584 | 142.384 | 131.620 | 261.3 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
