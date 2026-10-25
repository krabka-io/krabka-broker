# Performance budget: `rf3-regression-v2`

These results compare one candidate build with an earlier krabka revision under the `rf3-regression-v2` contract on the host below. They are measured limits of that contract, not a ranking of brokers and not a supported limit.

**Verdict: reject.**

- `1k-random-lz4-p12-saturate` CPU µs/record spread is +11.6%; the variance budget is +10.0%

## Provenance

| Side | Label | Revision | Image | Digest |
| :--- | :--- | :--- | :--- | :--- |
| reference | krabka | `ac9deff316d38418622b16c237c671df88f57c7a` | `docker.io/krabka-io/krabka-broker:budget-reference` | `sha256:3577489db455977c29025ef610541376c4d9e6873245e533e2ebccfb319be015` |
| candidate | krabka | `fd007630813bd8c75fac51fb154177cd22a5e71f` | `docker.io/krabka-io/krabka-broker:budget-candidate` | `sha256:7e6800159a55991ab5762e377725beec90f7ff8d9c618aba52a31f15e2c323d8` |

Client: `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`). Contract digest: `038d48e5ff398467a7ce5f97bb239f8378e21102e7113344693c65df725eeded`. Started 2026-10-22T23:10:07Z.

Host: Intel(R) Xeon(R) CPU @ 2.20GHz, 32 logical CPUs, 135056580608 bytes of memory, 1211085320192 bytes of free disk, kernel 7.0.0-1011-gcp.

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
| `1k-random-lz4-p12-saturate` | records/s | 134101 | 163557 | -22.0% | 5.5% / 6.7% | +20.0% | improved |
| `1k-random-lz4-p12-saturate` | p50 ms | 193.538 | 144.359 | -25.4% | 2.7% / 7.5% | — | recorded |
| `1k-random-lz4-p12-saturate` | p95 ms | 295.214 | 340.857 | +15.5% | 10.1% / 7.7% | — | recorded |
| `1k-random-lz4-p12-saturate` | p99 ms | 575.516 | 1046.673 | +81.9% | 53.6% / 3.5% | — | recorded |
| `1k-random-lz4-p12-saturate` | CPU µs/record | 38.690 | 23.273 | -39.8% | 3.1% / 11.6% | +34.8% | **excess variance** |
| `1k-random-lz4-p12-saturate` | peak RSS MiB | 142.6 | 140.1 | -1.8% | 0.2% / 2.5% | +10.0% | within budget |
| `100b-random-lz4-p12-saturate` | records/s | 862724 | 950405 | -10.2% | 2.4% / 2.9% | +8.7% | improved |
| `100b-random-lz4-p12-saturate` | p50 ms | 12.952 | 8.224 | -36.5% | 1.9% / 6.1% | — | recorded |
| `100b-random-lz4-p12-saturate` | p95 ms | 25.985 | 34.008 | +30.9% | 9.0% / 10.6% | — | recorded |
| `100b-random-lz4-p12-saturate` | p99 ms | 55.151 | 66.615 | +20.8% | 30.1% / 15.6% | — | recorded |
| `100b-random-lz4-p12-saturate` | CPU µs/record | 5.694 | 3.825 | -32.8% | 2.1% / 4.3% | +12.8% | improved |
| `100b-random-lz4-p12-saturate` | peak RSS MiB | 146.9 | 147.1 | +0.1% | 0.2% / 1.0% | +10.0% | within budget |
| `1k-zeros-lz4-p12-saturate` | records/s | 620229 | 679415 | -9.5% | 10.1% / 3.2% | +30.4% | within budget |
| `1k-zeros-lz4-p12-saturate` | p50 ms | 260.994 | 253.589 | -2.8% | 15.0% / 26.2% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p95 ms | 813.776 | 457.751 | -43.7% | 47.1% / 60.0% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p99 ms | 904.930 | 535.526 | -40.8% | 43.9% / 53.0% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | CPU µs/record | 3.263 | 2.603 | -20.2% | 2.2% / 3.2% | +9.6% | improved |
| `1k-zeros-lz4-p12-saturate` | peak RSS MiB | 191.2 | 185.4 | -3.0% | 1.0% / 7.4% | +22.1% | within budget |
| `1k-random-none-p12-saturate` | records/s | 138212 | 163758 | -18.5% | 2.5% / 1.7% | +7.5% | improved |
| `1k-random-none-p12-saturate` | p50 ms | 225.505 | 153.358 | -32.0% | 6.0% / 0.8% | — | recorded |
| `1k-random-none-p12-saturate` | p95 ms | 284.420 | 291.952 | +2.6% | 4.3% / 7.5% | — | recorded |
| `1k-random-none-p12-saturate` | p99 ms | 994.988 | 1234.584 | +24.1% | 27.3% / 28.6% | — | recorded |
| `1k-random-none-p12-saturate` | CPU µs/record | 41.087 | 22.900 | -44.3% | 5.5% / 2.6% | +16.4% | improved |
| `1k-random-none-p12-saturate` | peak RSS MiB | 192.8 | 191.3 | -0.8% | 1.1% / 6.2% | +18.7% | within budget |
| `100k-random-lz4-p12-saturate` | records/s | 1474 | 1729 | -17.3% | 8.5% / 1.2% | +25.5% | within budget |
| `100k-random-lz4-p12-saturate` | p50 ms | 180.498 | 133.763 | -25.9% | 4.5% / 1.7% | — | recorded |
| `100k-random-lz4-p12-saturate` | p95 ms | 346.800 | 558.839 | +61.1% | 15.5% / 8.4% | — | recorded |
| `100k-random-lz4-p12-saturate` | p99 ms | 1173.682 | 1348.457 | +14.9% | 8.9% / 1.5% | — | recorded |
| `100k-random-lz4-p12-saturate` | CPU µs/record | 3055.333 | 1785.000 | -41.6% | 4.4% / 1.8% | +13.3% | improved |
| `100k-random-lz4-p12-saturate` | peak RSS MiB | 202.3 | 199.6 | -1.3% | 1.9% / 4.0% | +12.1% | within budget |
| `1k-random-lz4-p1-saturate` | records/s | 29627 | 41407 | -39.8% | 3.9% / 2.5% | +11.6% | improved |
| `1k-random-lz4-p1-saturate` | p50 ms | 995.282 | 709.748 | -28.7% | 1.8% / 1.7% | — | recorded |
| `1k-random-lz4-p1-saturate` | p95 ms | 1128.751 | 772.083 | -31.6% | 4.4% / 2.9% | — | recorded |
| `1k-random-lz4-p1-saturate` | p99 ms | 1292.780 | 799.254 | -38.2% | 4.7% / 4.3% | — | recorded |
| `1k-random-lz4-p1-saturate` | CPU µs/record | 64.430 | 44.730 | -30.6% | 4.3% / 2.3% | +12.9% | improved |
| `1k-random-lz4-p1-saturate` | peak RSS MiB | 202.1 | 200.8 | -0.7% | 1.9% / 2.4% | +10.0% | within budget |
| `1k-random-lz4-p12-fixed-20k` | records/s | 19993 | 19993 | +0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p12-fixed-20k` | p50 ms | 4.923 | 4.429 | -10.0% | 1.4% / 0.9% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p95 ms | 6.922 | 6.258 | -9.6% | 3.5% / 0.4% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p99 ms | 16.714 | 13.320 | -20.3% | 33.0% / 25.3% | +98.9% | within budget |
| `1k-random-lz4-p12-fixed-20k` | CPU µs/record | 81.317 | 62.867 | -22.7% | 1.8% / 0.6% | +5.4% | improved |
| `1k-random-lz4-p12-fixed-20k` | peak RSS MiB | 202.6 | 206.5 | +1.9% | 2.3% / 3.5% | +10.4% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | records/s | 4998 | 4998 | -0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | p50 ms | 13.722 | 12.090 | -11.9% | 1.9% / 0.7% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p95 ms | 25.172 | 21.844 | -13.2% | 6.4% / 3.7% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p99 ms | 52.945 | 48.698 | -8.0% | 23.4% / 15.3% | +70.1% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | CPU µs/record | 508.167 | 439.933 | -13.4% | 1.7% / 0.5% | +5.2% | improved |
| `1k-random-lz4-p1000-fixed-5k` | peak RSS MiB | 263.0 | 272.6 | +3.7% | 0.9% / 3.1% | +10.0% | within budget |
| `1k-random-lz4-p12-idle-burst` | records/s | 48404 | 49685 | -2.6% | 2.0% / 0.0% | +6.1% | within budget |
| `1k-random-lz4-p12-idle-burst` | p50 ms | 378.189 | 62.681 | -83.4% | 31.2% / 5.1% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p95 ms | 452.320 | 171.964 | -62.0% | 14.5% / 8.8% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p99 ms | 482.793 | 210.340 | -56.4% | 14.9% / 4.3% | +44.6% | improved |
| `1k-random-lz4-p12-idle-burst` | CPU µs/record | 149.600 | 138.080 | -7.7% | 4.2% / 1.4% | +12.5% | within budget |
| `1k-random-lz4-p12-idle-burst` | peak RSS MiB | 268.1 | 291.1 | +8.6% | 1.7% / 1.7% | +10.0% | within budget |

## Raw trials

| Order | Cell | Round | Side | records/s | p50 ms | p95 ms | p99 ms | CPU µs/record | peak RSS MiB | Outcome |
| ---: | :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 1 | `1k-random-lz4-p12-saturate` | 1 | reference | 126364 | 188.595 | 587.041 | 1253.237 | 37.687 | 142.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 2 | `100b-random-lz4-p12-saturate` | 1 | reference | 864447 | 12.710 | 24.986 | 79.164 | 5.573 | 146.7 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 3 | `1k-zeros-lz4-p12-saturate` | 1 | reference | 677560 | 221.739 | 430.774 | 507.769 | 3.190 | 190.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 4 | `1k-random-none-p12-saturate` | 1 | reference | 141203 | 211.921 | 275.334 | 1066.232 | 38.837 | 191.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 5 | `100k-random-lz4-p12-saturate` | 1 | reference | 1465 | 166.128 | 791.839 | 1173.682 | 2823.000 | 199.4 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 6 | `1k-random-lz4-p1-saturate` | 1 | reference | 31527 | 932.362 | 1018.944 | 1232.665 | 60.950 | 198.9 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 7 | `1k-random-lz4-p12-fixed-20k` | 1 | reference | 19993 | 4.855 | 6.680 | 11.851 | 78.500 | 199.4 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 8 | `1k-random-lz4-p1000-fixed-5k` | 1 | reference | 4996 | 13.163 | 23.569 | 40.571 | 495.300 | 262.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 9 | `1k-random-lz4-p12-idle-burst` | 1 | reference | 32915 | 881.842 | 974.923 | 1010.649 | 222.480 | 268.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 10 | `1k-random-lz4-p12-saturate` | 1 | candidate | 131767 | 209.025 | 323.289 | 1083.318 | 32.220 | 139.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 11 | `100b-random-lz4-p12-saturate` | 1 | candidate | 625745 | 18.373 | 37.599 | 56.221 | 5.751 | 148.5 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 12 | `1k-zeros-lz4-p12-saturate` | 1 | candidate | 574464 | 320.037 | 846.470 | 897.814 | 2.687 | 171.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 13 | `1k-random-none-p12-saturate` | 1 | candidate | 161500 | 171.509 | 270.188 | 1234.584 | 24.847 | 179.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 14 | `100k-random-lz4-p12-saturate` | 1 | candidate | 1641 | 145.007 | 558.839 | 1369.050 | 1892.667 | 191.5 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 15 | `1k-random-lz4-p1-saturate` | 1 | candidate | 41057 | 723.643 | 772.083 | 799.254 | 44.960 | 195.9 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 16 | `1k-random-lz4-p12-fixed-20k` | 1 | candidate | 19993 | 4.510 | 6.258 | 9.462 | 63.233 | 199.4 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 17 | `1k-random-lz4-p1000-fixed-5k` | 1 | candidate | 4998 | 12.409 | 23.730 | 47.047 | 447.233 | 264.1 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 18 | `1k-random-lz4-p12-idle-burst` | 1 | candidate | 49734 | 68.605 | 197.540 | 226.193 | 141.340 | 281.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 19 | `1k-random-lz4-p12-saturate` | 2 | candidate | 113354 | 237.291 | 623.756 | 855.170 | 37.007 | 144.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 20 | `100b-random-lz4-p12-saturate` | 2 | candidate | 626270 | 17.421 | 34.008 | 51.519 | 5.776 | 147.1 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 21 | `1k-zeros-lz4-p12-saturate` | 2 | candidate | 401814 | 287.476 | 881.124 | 982.918 | 4.470 | 202.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 22 | `1k-random-none-p12-saturate` | 2 | candidate | 94894 | 338.383 | 509.631 | 643.233 | 48.890 | 206.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 23 | `100k-random-lz4-p12-saturate` | 2 | candidate | 1749 | 154.034 | 511.860 | 705.224 | 2028.000 | 216.8 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 24 | `1k-random-lz4-p1-saturate` | 2 | candidate | 41407 | 709.748 | 790.478 | 847.842 | 44.730 | 222.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 25 | `1k-random-lz4-p12-fixed-20k` | 2 | candidate | 19993 | 4.470 | 6.267 | 13.320 | 62.867 | 224.4 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 26 | `1k-random-lz4-p1000-fixed-5k` | 2 | candidate | 4998 | 12.090 | 22.647 | 48.698 | 439.933 | 286.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 27 | `1k-random-lz4-p12-idle-burst` | 2 | candidate | 49677 | 66.153 | 187.087 | 219.489 | 139.980 | 303.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 28 | `1k-random-lz4-p12-saturate` | 2 | reference | 134101 | 219.169 | 295.214 | 423.576 | 43.177 | 141.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 29 | `100b-random-lz4-p12-saturate` | 2 | reference | 842301 | 12.952 | 25.985 | 55.151 | 5.694 | 145.6 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 30 | `1k-zeros-lz4-p12-saturate` | 2 | reference | 541824 | 258.456 | 1254.163 | 1350.110 | 3.457 | 189.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 31 | `1k-random-none-p12-saturate` | 2 | reference | 134751 | 225.698 | 324.868 | 468.773 | 41.087 | 190.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 32 | `100k-random-lz4-p12-saturate` | 2 | reference | 1619 | 180.674 | 337.054 | 603.667 | 3055.333 | 197.6 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 33 | `1k-random-lz4-p1-saturate` | 2 | reference | 29627 | 995.282 | 1128.751 | 1292.780 | 64.430 | 197.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 34 | `1k-random-lz4-p12-fixed-20k` | 2 | reference | 19993 | 5.073 | 7.268 | 16.714 | 82.783 | 197.7 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 35 | `1k-random-lz4-p1000-fixed-5k` | 2 | reference | 4998 | 13.467 | 24.308 | 52.945 | 505.200 | 260.6 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 36 | `1k-random-lz4-p12-idle-burst` | 2 | reference | 48404 | 378.189 | 452.320 | 482.793 | 149.600 | 263.5 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 37 | `1k-random-lz4-p12-saturate` | 3 | reference | 149309 | 193.538 | 238.630 | 267.191 | 38.690 | 142.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 38 | `100b-random-lz4-p12-saturate` | 3 | reference | 862724 | 16.104 | 31.587 | 118.433 | 5.730 | 147.4 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 39 | `1k-zeros-lz4-p12-saturate` | 3 | reference | 683101 | 318.565 | 600.930 | 690.837 | 3.263 | 196.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 40 | `1k-random-none-p12-saturate` | 3 | reference | 138212 | 225.505 | 284.420 | 359.255 | 41.323 | 197.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 41 | `100k-random-lz4-p12-saturate` | 3 | reference | 1599 | 172.431 | 346.800 | 1190.310 | 2920.333 | 206.2 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 42 | `1k-random-lz4-p1-saturate` | 3 | reference | 29943 | 983.531 | 1106.366 | 1290.084 | 63.990 | 206.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 43 | `1k-random-lz4-p12-fixed-20k` | 3 | reference | 19993 | 4.923 | 6.826 | 11.206 | 81.317 | 207.3 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 44 | `1k-random-lz4-p1000-fixed-5k` | 3 | reference | 4998 | 13.722 | 25.172 | 49.252 | 508.167 | 273.9 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 45 | `1k-random-lz4-p12-idle-burst` | 3 | reference | 48227 | 417.416 | 482.952 | 505.717 | 151.080 | 277.0 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 46 | `1k-random-lz4-p12-saturate` | 3 | candidate | 167278 | 135.505 | 340.857 | 1256.766 | 21.433 | 140.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 47 | `100b-random-lz4-p12-saturate` | 3 | candidate | 950405 | 7.857 | 34.190 | 79.410 | 3.808 | 143.4 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 48 | `1k-zeros-lz4-p12-saturate` | 3 | candidate | 679415 | 144.614 | 386.469 | 460.850 | 2.603 | 185.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 49 | `1k-random-none-p12-saturate` | 3 | candidate | 163758 | 152.341 | 292.056 | 1587.557 | 22.900 | 189.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 50 | `100k-random-lz4-p12-saturate` | 3 | candidate | 1729 | 132.254 | 847.944 | 1253.537 | 1785.000 | 195.0 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 51 | `1k-random-lz4-p1-saturate` | 3 | candidate | 42425 | 700.674 | 749.584 | 765.217 | 43.710 | 197.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 52 | `1k-random-lz4-p12-fixed-20k` | 3 | candidate | 19992 | 4.429 | 6.281 | 14.252 | 63.317 | 206.5 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 53 | `1k-random-lz4-p1000-fixed-5k` | 3 | candidate | 4999 | 12.174 | 21.844 | 57.771 | 442.267 | 273.3 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 54 | `1k-random-lz4-p12-idle-burst` | 3 | candidate | 49680 | 62.681 | 168.722 | 210.340 | 138.080 | 296.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 55 | `1k-random-lz4-p12-saturate` | 4 | candidate | 174450 | 133.518 | 595.020 | 1046.673 | 20.573 | 136.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 56 | `100b-random-lz4-p12-saturate` | 4 | candidate | 963054 | 8.224 | 24.905 | 66.693 | 3.825 | 141.4 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 57 | `1k-zeros-lz4-p12-saturate` | 4 | candidate | 694741 | 89.472 | 183.198 | 251.590 | 2.567 | 201.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 58 | `1k-random-none-p12-saturate` | 4 | candidate | 166466 | 153.358 | 291.952 | 1379.191 | 22.547 | 206.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 59 | `100k-random-lz4-p12-saturate` | 4 | candidate | 1724 | 133.763 | 669.469 | 1368.947 | 1763.333 | 209.2 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 60 | `1k-random-lz4-p1-saturate` | 4 | candidate | 39994 | 732.855 | 829.341 | 903.237 | 45.970 | 210.1 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 61 | `1k-random-lz4-p12-fixed-20k` | 4 | candidate | 19993 | 4.407 | 6.191 | 16.692 | 62.667 | 214.9 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 62 | `1k-random-lz4-p1000-fixed-5k` | 4 | candidate | 4998 | 12.046 | 21.603 | 56.140 | 437.567 | 272.6 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 63 | `1k-random-lz4-p12-idle-burst` | 4 | candidate | 49685 | 59.504 | 171.964 | 208.884 | 136.080 | 291.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 64 | `1k-random-lz4-p12-saturate` | 4 | reference | 132411 | 184.898 | 265.367 | 1299.791 | 36.897 | 142.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 65 | `100b-random-lz4-p12-saturate` | 4 | reference | 588903 | 18.442 | 34.371 | 43.642 | 8.202 | 147.2 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 66 | `1k-zeros-lz4-p12-saturate` | 4 | reference | 369765 | 750.018 | 1641.617 | 1797.452 | 5.310 | 193.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 67 | `1k-random-none-p12-saturate` | 4 | reference | 106666 | 306.555 | 846.820 | 994.988 | 54.653 | 195.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 68 | `100k-random-lz4-p12-saturate` | 4 | reference | 1246 | 237.606 | 487.290 | 906.185 | 4349.333 | 207.7 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 69 | `1k-random-lz4-p1-saturate` | 4 | reference | 28485 | 1012.827 | 1178.606 | 1438.308 | 67.200 | 207.6 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 70 | `1k-random-lz4-p12-fixed-20k` | 4 | reference | 19993 | 4.911 | 6.922 | 26.896 | 80.633 | 208.6 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 71 | `1k-random-lz4-p1000-fixed-5k` | 4 | reference | 4999 | 13.975 | 56.004 | 117.268 | 516.900 | 270.9 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 72 | `1k-random-lz4-p12-idle-burst` | 4 | reference | 49438 | 260.257 | 386.664 | 410.948 | 143.060 | 273.9 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 73 | `1k-random-lz4-p12-saturate` | 5 | reference | 141467 | 198.809 | 296.170 | 575.516 | 39.903 | 142.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 74 | `100b-random-lz4-p12-saturate` | 5 | reference | 898989 | 12.708 | 23.647 | 38.563 | 5.439 | 146.9 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 75 | `1k-zeros-lz4-p12-saturate` | 5 | reference | 620229 | 260.994 | 813.776 | 904.930 | 3.190 | 191.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 76 | `1k-random-none-p12-saturate` | 5 | reference | 142431 | 208.578 | 272.050 | 1266.872 | 38.233 | 192.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 77 | `100k-random-lz4-p12-saturate` | 5 | reference | 1474 | 180.498 | 293.114 | 1277.976 | 3189.000 | 202.3 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 78 | `1k-random-lz4-p1-saturate` | 5 | reference | 20018 | 1483.224 | 1581.295 | 1643.082 | 99.220 | 202.1 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 79 | `1k-random-lz4-p12-fixed-20k` | 5 | reference | 19987 | 7.150 | 11.326 | 90.587 | 119.017 | 202.6 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 80 | `1k-random-lz4-p1000-fixed-5k` | 5 | reference | 4997 | 23.685 | 56.954 | 283.146 | 732.500 | 263.0 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 81 | `1k-random-lz4-p12-idle-burst` | 5 | reference | 49383 | 111.320 | 318.267 | 353.103 | 143.360 | 266.5 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 82 | `1k-random-lz4-p12-saturate` | 5 | candidate | 163557 | 144.359 | 314.775 | 1037.914 | 23.273 | 144.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 83 | `100b-random-lz4-p12-saturate` | 5 | candidate | 978050 | 7.724 | 24.969 | 66.615 | 3.662 | 148.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 84 | `1k-zeros-lz4-p12-saturate` | 5 | candidate | 701331 | 253.589 | 457.751 | 535.526 | 2.483 | 178.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 85 | `1k-random-none-p12-saturate` | 5 | candidate | 195795 | 152.163 | 242.717 | 548.981 | 22.303 | 191.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 86 | `100k-random-lz4-p12-saturate` | 5 | candidate | 1842 | 131.527 | 540.704 | 1348.457 | 1752.333 | 199.6 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 87 | `1k-random-lz4-p1-saturate` | 5 | candidate | 42648 | 697.523 | 745.309 | 769.578 | 43.170 | 200.8 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 88 | `1k-random-lz4-p12-fixed-20k` | 5 | candidate | 19993 | 4.292 | 5.984 | 9.372 | 60.250 | 205.7 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 89 | `1k-random-lz4-p1000-fixed-5k` | 5 | candidate | 4998 | 11.447 | 20.096 | 38.502 | 431.633 | 262.5 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 90 | `1k-random-lz4-p12-idle-burst` | 5 | candidate | 49715 | 60.244 | 134.689 | 182.093 | 135.780 | 286.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
