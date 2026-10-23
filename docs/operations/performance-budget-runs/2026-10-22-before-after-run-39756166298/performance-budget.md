# Performance budget: `rf3-regression-v1`

These results compare one candidate build with an earlier krabka revision under the `rf3-regression-v1` contract on the host below. They are measured limits of that contract, not a ranking of brokers and not a supported limit.

**Verdict: reject.**

- `1k-random-none-p12-saturate` records/s spread is +12.1%; the variance budget is +10.0%
- `1k-random-lz4-p12-idle-burst` p99 ms spread is +40.2%; the variance budget is +25.0%

## Provenance

| Side | Label | Revision | Image | Digest |
| :--- | :--- | :--- | :--- | :--- |
| reference | krabka | `ac9deff316d38418622b16c237c671df88f57c7a` | `docker.io/krabka-io/krabka-broker:budget-reference` | `sha256:30b24062e47106cae03c694b9ae37c4b12b27c08a31829fee287c0f161f8a5a6` |
| candidate | krabka | `54c132a41ccc3c8c50324c50177256f89b4fba03` | `docker.io/krabka-io/krabka-broker:budget-candidate` | `sha256:19bd76ed7f5a905db10683731a5e26f1a7b160fec1e9665e47170707b83cf248` |

Client: `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`). Contract digest: `88125f23e83026670713516c73f401d80cced33577e1a4244b33b3cac83e4d17`. Started 2026-10-22T09:52:14Z.

Host: Intel(R) Xeon(R) CPU @ 2.20GHz, 32 logical CPUs, 135056592896 bytes of memory, 1210858000384 bytes of free disk, kernel 7.0.0-1011-gcp.

Cluster: 3 combined broker/controllers, replication factor 3, `min.insync.replicas=2`, `acks=all`, idempotence, 65,536-byte batches, 5 ms linger. 5 interleaved rounds; each round runs every cell once per side.

Settings: `acks=all` `batch_size=65536` `broker_cpu_limit=2` `broker_disk_request=100Gi` `broker_memory_limit=2Gi` `enable_idempotence=true` `krabka_compilation_mode=opt` `linger_ms=5` `source_patch_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Budgets

Declared before the run. A regression is material when the median gets worse by more than the materiality floor and by more than the multiplier times the larger side's relative median absolute deviation. A side whose spread exceeds the variance budget makes the cell inconclusive.

| Metric | Materiality | Spread multiplier | Variance budget |
| :--- | ---: | ---: | ---: |
| records/s | 5.0% | 3.0 | 10.0% |
| p99 ms | 10.0% | 3.0 | 25.0% |
| CPU µs/record | 5.0% | 3.0 | 10.0% |
| peak RSS MiB | 10.0% | 3.0 | 10.0% |

## Results

Medians over the interleaved rounds. A change is relative to the reference median; positive is worse.

| Cell | Metric | Reference | Candidate | Change | Spread (ref / cand) | Allowed | Status |
| :--- | :--- | ---: | ---: | ---: | :--- | ---: | :--- |
| `1k-random-lz4-p12-saturate` | records/s | 145258 | 180137 | -24.0% | 6.6% / 1.3% | +19.8% | improved |
| `1k-random-lz4-p12-saturate` | p50 ms | 159.454 | 116.541 | -26.9% | 4.1% / 3.3% | — | recorded |
| `1k-random-lz4-p12-saturate` | p95 ms | 280.975 | 516.424 | +83.8% | 21.7% / 28.3% | — | recorded |
| `1k-random-lz4-p12-saturate` | p99 ms | 960.193 | 925.208 | -3.6% | 20.4% / 8.5% | — | recorded |
| `1k-random-lz4-p12-saturate` | CPU µs/record | 31.520 | 18.190 | -42.3% | 4.1% / 1.4% | +12.4% | improved |
| `1k-random-lz4-p12-saturate` | peak RSS MiB | 140.1 | 138.7 | -1.0% | 0.8% / 1.8% | +10.0% | within budget |
| `100b-random-lz4-p12-saturate` | records/s | 923365 | 1015038 | -9.9% | 3.6% / 0.5% | +10.8% | within budget |
| `100b-random-lz4-p12-saturate` | p50 ms | 9.123 | 5.312 | -41.8% | 13.6% / 3.0% | — | recorded |
| `100b-random-lz4-p12-saturate` | p95 ms | 21.129 | 16.836 | -20.3% | 14.9% / 4.1% | — | recorded |
| `100b-random-lz4-p12-saturate` | p99 ms | 39.550 | 59.459 | +50.3% | 22.6% / 9.8% | — | recorded |
| `100b-random-lz4-p12-saturate` | CPU µs/record | 4.997 | 3.398 | -32.0% | 3.0% / 0.2% | +9.0% | improved |
| `100b-random-lz4-p12-saturate` | peak RSS MiB | 144.3 | 146.0 | +1.2% | 1.0% / 2.3% | +10.0% | within budget |
| `1k-zeros-lz4-p12-saturate` | records/s | 728045 | 712408 | +2.1% | 4.2% / 2.4% | +12.5% | within budget |
| `1k-zeros-lz4-p12-saturate` | p50 ms | 173.245 | 204.666 | +18.1% | 21.0% / 14.5% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p95 ms | 313.538 | 364.321 | +16.2% | 16.9% / 23.5% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p99 ms | 364.467 | 421.489 | +15.6% | 14.4% / 9.3% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | CPU µs/record | 2.790 | 2.257 | -19.1% | 2.6% / 0.4% | +7.9% | improved |
| `1k-zeros-lz4-p12-saturate` | peak RSS MiB | 196.2 | 176.0 | -10.3% | 2.3% / 3.4% | +10.3% | improved |
| `1k-random-none-p12-saturate` | records/s | 157121 | 191841 | -22.1% | 6.9% / 12.1% | +36.3% | **excess variance** |
| `1k-random-none-p12-saturate` | p50 ms | 186.563 | 129.693 | -30.5% | 6.0% / 0.9% | — | recorded |
| `1k-random-none-p12-saturate` | p95 ms | 270.338 | 240.369 | -11.1% | 7.3% / 7.9% | — | recorded |
| `1k-random-none-p12-saturate` | p99 ms | 787.556 | 1042.472 | +32.4% | 21.0% / 24.9% | — | recorded |
| `1k-random-none-p12-saturate` | CPU µs/record | 33.893 | 19.010 | -43.9% | 5.8% / 0.5% | +17.4% | improved |
| `1k-random-none-p12-saturate` | peak RSS MiB | 200.7 | 181.5 | -9.6% | 2.2% / 1.8% | +10.0% | within budget |
| `100k-random-lz4-p12-saturate` | records/s | 1840 | 1860 | -1.1% | 2.2% / 2.3% | +7.0% | within budget |
| `100k-random-lz4-p12-saturate` | p50 ms | 135.830 | 114.385 | -15.8% | 5.2% / 3.5% | — | recorded |
| `100k-random-lz4-p12-saturate` | p95 ms | 398.606 | 729.198 | +82.9% | 36.5% / 17.6% | — | recorded |
| `100k-random-lz4-p12-saturate` | p99 ms | 1145.861 | 1408.850 | +23.0% | 4.8% / 13.9% | — | recorded |
| `100k-random-lz4-p12-saturate` | CPU µs/record | 2339.667 | 1472.000 | -37.1% | 4.3% / 2.8% | +13.0% | improved |
| `100k-random-lz4-p12-saturate` | peak RSS MiB | 208.3 | 185.9 | -10.8% | 1.0% / 2.4% | +10.0% | improved |
| `1k-random-lz4-p1-saturate` | records/s | 38722 | 49575 | -28.0% | 1.1% / 1.5% | +5.0% | improved |
| `1k-random-lz4-p1-saturate` | p50 ms | 768.925 | 596.463 | -22.4% | 1.8% / 1.2% | — | recorded |
| `1k-random-lz4-p1-saturate` | p95 ms | 832.939 | 648.949 | -22.1% | 1.8% / 4.4% | — | recorded |
| `1k-random-lz4-p1-saturate` | p99 ms | 866.804 | 669.310 | -22.8% | 3.4% / 4.9% | — | recorded |
| `1k-random-lz4-p1-saturate` | CPU µs/record | 50.220 | 37.050 | -26.2% | 0.6% / 2.0% | +6.1% | improved |
| `1k-random-lz4-p1-saturate` | peak RSS MiB | 207.9 | 186.9 | -10.1% | 1.0% / 2.4% | +10.0% | improved |
| `1k-random-lz4-p12-fixed-20k` | records/s | 19993 | 19994 | -0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p12-fixed-20k` | p50 ms | 4.369 | 3.939 | -9.8% | 2.5% / 2.2% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p95 ms | 6.152 | 5.583 | -9.2% | 1.4% / 0.9% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p99 ms | 9.628 | 10.947 | +13.7% | 10.9% / 9.3% | +32.8% | noisy, not material |
| `1k-random-lz4-p12-fixed-20k` | CPU µs/record | 68.183 | 52.133 | -23.5% | 1.8% / 3.3% | +9.8% | improved |
| `1k-random-lz4-p12-fixed-20k` | peak RSS MiB | 208.3 | 188.1 | -9.7% | 1.7% / 0.3% | +10.0% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | records/s | 4999 | 4998 | +0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | p50 ms | 11.563 | 10.122 | -12.5% | 0.9% / 2.6% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p95 ms | 18.900 | 15.937 | -15.7% | 5.6% / 4.9% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p99 ms | 38.977 | 31.581 | -19.0% | 17.7% / 4.2% | +53.2% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | CPU µs/record | 459.633 | 394.933 | -14.1% | 0.9% / 1.7% | +5.2% | improved |
| `1k-random-lz4-p1000-fixed-5k` | peak RSS MiB | 272.2 | 249.4 | -8.4% | 1.7% / 1.3% | +10.0% | within budget |
| `1k-random-lz4-p12-idle-burst` | records/s | 49443 | 49710 | -0.5% | 0.3% / 0.1% | +5.0% | within budget |
| `1k-random-lz4-p12-idle-burst` | p50 ms | 128.975 | 49.874 | -61.3% | 22.8% / 13.7% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p95 ms | 930.260 | 170.146 | -81.7% | 11.9% / 45.9% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p99 ms | 997.586 | 211.945 | -78.8% | 10.9% / 40.2% | +120.7% | **excess variance** |
| `1k-random-lz4-p12-idle-burst` | CPU µs/record | 140.460 | 130.780 | -6.9% | 1.5% / 1.9% | +5.6% | improved |
| `1k-random-lz4-p12-idle-burst` | peak RSS MiB | 276.2 | 260.6 | -5.7% | 0.8% / 1.8% | +10.0% | within budget |

## Raw trials

| Order | Cell | Round | Side | records/s | p50 ms | p95 ms | p99 ms | CPU µs/record | peak RSS MiB | Outcome |
| ---: | :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 1 | `1k-random-lz4-p12-saturate` | 1 | reference | 171267 | 152.912 | 249.973 | 774.565 | 30.220 | 143.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 2 | `100b-random-lz4-p12-saturate` | 1 | reference | 956665 | 7.997 | 16.368 | 28.885 | 4.847 | 148.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 3 | `1k-zeros-lz4-p12-saturate` | 1 | reference | 758392 | 136.857 | 260.695 | 312.020 | 2.717 | 199.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 4 | `1k-random-none-p12-saturate` | 1 | reference | 157121 | 186.563 | 270.338 | 787.556 | 33.893 | 203.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 5 | `100k-random-lz4-p12-saturate` | 1 | reference | 1924 | 134.887 | 266.864 | 1091.225 | 2293.000 | 210.4 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 6 | `1k-random-lz4-p1-saturate` | 1 | reference | 39166 | 756.718 | 819.871 | 837.643 | 49.910 | 209.9 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 7 | `1k-random-lz4-p12-fixed-20k` | 1 | reference | 19993 | 4.369 | 6.238 | 12.312 | 68.183 | 210.5 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 8 | `1k-random-lz4-p1000-fixed-5k` | 1 | reference | 4998 | 11.563 | 19.952 | 157.944 | 459.633 | 276.7 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 9 | `1k-random-lz4-p12-idle-burst` | 1 | reference | 42309 | 128.975 | 974.240 | 1033.382 | 140.460 | 278.5 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 10 | `1k-random-lz4-p12-saturate` | 1 | candidate | 181741 | 116.541 | 361.775 | 846.267 | 18.373 | 146.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 11 | `100b-random-lz4-p12-saturate` | 1 | candidate | 1000812 | 4.890 | 16.836 | 45.276 | 3.394 | 149.3 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 12 | `1k-zeros-lz4-p12-saturate` | 1 | candidate | 712408 | 204.666 | 364.321 | 401.325 | 2.267 | 171.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 13 | `1k-random-none-p12-saturate` | 1 | candidate | 172782 | 129.805 | 240.369 | 1302.355 | 19.737 | 180.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 14 | `100k-random-lz4-p12-saturate` | 1 | candidate | 1821 | 114.385 | 1083.143 | 1408.850 | 1492.667 | 185.9 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 15 | `1k-random-lz4-p1-saturate` | 1 | candidate | 48832 | 603.352 | 677.406 | 702.264 | 37.800 | 186.9 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 16 | `1k-random-lz4-p12-fixed-20k` | 1 | candidate | 19994 | 3.939 | 5.583 | 8.092 | 52.133 | 188.1 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 17 | `1k-random-lz4-p1000-fixed-5k` | 1 | candidate | 4998 | 10.122 | 15.937 | 32.914 | 394.933 | 246.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 18 | `1k-random-lz4-p12-idle-burst` | 1 | candidate | 43458 | 65.329 | 1122.208 | 1333.606 | 125.060 | 256.9 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 19 | `1k-random-lz4-p12-saturate` | 2 | candidate | 180137 | 120.438 | 516.424 | 925.208 | 18.437 | 138.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 20 | `100b-random-lz4-p12-saturate` | 2 | candidate | 1015038 | 5.154 | 16.151 | 59.459 | 3.444 | 150.5 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 21 | `1k-zeros-lz4-p12-saturate` | 2 | candidate | 700479 | 234.348 | 485.756 | 582.951 | 2.227 | 204.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 22 | `1k-random-none-p12-saturate` | 2 | candidate | 215058 | 143.175 | 221.430 | 270.959 | 20.377 | 207.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 23 | `100k-random-lz4-p12-saturate` | 2 | candidate | 1787 | 118.351 | 601.156 | 1441.563 | 1513.000 | 213.4 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 24 | `1k-random-lz4-p1-saturate` | 2 | candidate | 48454 | 599.425 | 695.032 | 746.790 | 37.950 | 214.7 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 25 | `1k-random-lz4-p12-fixed-20k` | 2 | candidate | 19994 | 4.030 | 5.715 | 9.927 | 53.883 | 215.7 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 26 | `1k-random-lz4-p1000-fixed-5k` | 2 | candidate | 4998 | 10.495 | 16.723 | 30.849 | 405.633 | 264.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 27 | `1k-random-lz4-p12-idle-burst` | 2 | candidate | 49710 | 57.826 | 170.146 | 211.945 | 133.240 | 276.9 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 28 | `1k-random-lz4-p12-saturate` | 2 | reference | 135669 | 173.825 | 561.603 | 1185.043 | 34.750 | 140.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 29 | `100b-random-lz4-p12-saturate` | 2 | reference | 858937 | 10.708 | 21.738 | 39.550 | 5.432 | 144.3 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 30 | `1k-zeros-lz4-p12-saturate` | 2 | reference | 694863 | 216.547 | 406.424 | 506.704 | 2.970 | 196.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 31 | `1k-random-none-p12-saturate` | 2 | reference | 142780 | 213.205 | 283.980 | 622.444 | 38.977 | 200.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 32 | `100k-random-lz4-p12-saturate` | 2 | reference | 1800 | 148.068 | 544.032 | 832.059 | 2530.667 | 208.3 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 33 | `1k-random-lz4-p1-saturate` | 2 | reference | 34737 | 846.131 | 942.571 | 957.968 | 55.240 | 207.9 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 34 | `1k-random-lz4-p12-fixed-20k` | 2 | reference | 19993 | 4.404 | 6.152 | 8.521 | 69.400 | 208.3 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 35 | `1k-random-lz4-p1000-fixed-5k` | 2 | reference | 4999 | 11.650 | 18.905 | 45.250 | 463.967 | 272.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 36 | `1k-random-lz4-p12-idle-burst` | 2 | reference | 49462 | 105.545 | 257.583 | 284.900 | 142.500 | 276.2 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 37 | `1k-random-lz4-p12-saturate` | 3 | reference | 141193 | 201.785 | 280.975 | 405.929 | 39.833 | 143.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 38 | `100b-random-lz4-p12-saturate` | 3 | reference | 802108 | 13.259 | 25.890 | 60.155 | 5.883 | 147.2 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 39 | `1k-zeros-lz4-p12-saturate` | 3 | reference | 728045 | 165.540 | 296.942 | 355.748 | 2.927 | 203.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 40 | `1k-random-none-p12-saturate` | 3 | reference | 134491 | 210.790 | 295.133 | 1221.159 | 38.203 | 205.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 41 | `100k-random-lz4-p12-saturate` | 3 | reference | 1840 | 146.226 | 251.723 | 1273.717 | 2510.333 | 210.2 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 42 | `1k-random-lz4-p1-saturate` | 3 | reference | 36145 | 816.470 | 898.160 | 968.273 | 53.420 | 210.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 43 | `1k-random-lz4-p12-fixed-20k` | 3 | reference | 19993 | 4.479 | 6.221 | 9.808 | 70.950 | 211.8 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 44 | `1k-random-lz4-p1000-fixed-5k` | 3 | reference | 4999 | 11.670 | 18.900 | 38.977 | 465.033 | 274.9 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 45 | `1k-random-lz4-p12-idle-burst` | 3 | reference | 49573 | 99.533 | 280.364 | 307.284 | 142.140 | 278.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 46 | `1k-random-lz4-p12-saturate` | 3 | candidate | 174350 | 118.581 | 277.856 | 1127.322 | 18.190 | 141.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 47 | `100b-random-lz4-p12-saturate` | 3 | candidate | 953334 | 5.312 | 62.800 | 117.422 | 3.330 | 146.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 48 | `1k-zeros-lz4-p12-saturate` | 3 | candidate | 691809 | 255.797 | 549.723 | 622.815 | 2.257 | 176.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 49 | `1k-random-none-p12-saturate` | 3 | candidate | 166380 | 128.463 | 950.494 | 1377.283 | 18.950 | 181.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 50 | `100k-random-lz4-p12-saturate` | 3 | candidate | 1860 | 104.411 | 745.031 | 1852.952 | 1416.667 | 183.9 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 51 | `1k-random-lz4-p1-saturate` | 3 | candidate | 50285 | 579.284 | 648.949 | 669.310 | 36.370 | 184.4 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 52 | `1k-random-lz4-p12-fixed-20k` | 3 | candidate | 19993 | 3.868 | 5.545 | 11.222 | 50.833 | 188.0 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 53 | `1k-random-lz4-p1000-fixed-5k` | 3 | candidate | 4999 | 9.949 | 15.607 | 31.581 | 392.767 | 248.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 54 | `1k-random-lz4-p12-idle-burst` | 3 | candidate | 47575 | 49.874 | 272.198 | 468.752 | 130.480 | 255.9 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 55 | `1k-random-lz4-p12-saturate` | 4 | candidate | 191697 | 110.411 | 662.571 | 890.964 | 17.777 | 134.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 56 | `100b-random-lz4-p12-saturate` | 4 | candidate | 1020518 | 5.344 | 16.427 | 57.685 | 3.404 | 140.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 57 | `1k-zeros-lz4-p12-saturate` | 4 | candidate | 729556 | 128.110 | 278.822 | 382.418 | 2.243 | 186.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 58 | `1k-random-none-p12-saturate` | 4 | candidate | 191841 | 129.693 | 255.079 | 1042.472 | 19.010 | 189.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 59 | `100k-random-lz4-p12-saturate` | 4 | candidate | 1904 | 109.919 | 729.198 | 1213.644 | 1472.000 | 200.3 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 60 | `1k-random-lz4-p1-saturate` | 4 | candidate | 49575 | 596.463 | 641.474 | 668.489 | 37.050 | 203.2 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 61 | `1k-random-lz4-p12-fixed-20k` | 4 | candidate | 19993 | 4.025 | 5.764 | 10.947 | 53.833 | 208.6 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 62 | `1k-random-lz4-p1000-fixed-5k` | 4 | candidate | 4998 | 10.388 | 16.746 | 28.603 | 401.800 | 264.3 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 63 | `1k-random-lz4-p12-idle-burst` | 4 | candidate | 49714 | 43.019 | 92.689 | 126.682 | 134.080 | 276.9 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 64 | `1k-random-lz4-p12-saturate` | 4 | reference | 145258 | 158.062 | 694.958 | 1155.772 | 31.520 | 139.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 65 | `100b-random-lz4-p12-saturate` | 4 | reference | 940780 | 9.123 | 21.129 | 48.487 | 4.997 | 142.9 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 66 | `1k-zeros-lz4-p12-saturate` | 4 | reference | 695392 | 215.926 | 425.127 | 550.189 | 2.790 | 187.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 67 | `1k-random-none-p12-saturate` | 4 | reference | 167944 | 179.937 | 243.439 | 572.984 | 32.603 | 189.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 68 | `100k-random-lz4-p12-saturate` | 4 | reference | 1844 | 135.830 | 398.606 | 1145.861 | 2339.667 | 198.5 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 69 | `1k-random-lz4-p1-saturate` | 4 | reference | 38722 | 768.925 | 817.965 | 851.375 | 50.220 | 198.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 70 | `1k-random-lz4-p12-fixed-20k` | 4 | reference | 19993 | 4.207 | 5.851 | 8.576 | 65.883 | 198.6 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 71 | `1k-random-lz4-p1000-fixed-5k` | 4 | reference | 4999 | 10.631 | 16.193 | 32.063 | 448.333 | 261.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 72 | `1k-random-lz4-p12-idle-burst` | 4 | reference | 49443 | 482.785 | 930.260 | 997.586 | 126.160 | 265.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 73 | `1k-random-lz4-p12-saturate` | 5 | reference | 162515 | 159.454 | 220.089 | 960.193 | 31.430 | 139.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 74 | `100b-random-lz4-p12-saturate` | 5 | reference | 923365 | 7.880 | 17.976 | 37.925 | 4.952 | 144.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 75 | `1k-zeros-lz4-p12-saturate` | 5 | reference | 749985 | 173.245 | 313.538 | 364.467 | 2.743 | 191.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 76 | `1k-random-none-p12-saturate` | 5 | reference | 164370 | 175.333 | 250.642 | 899.201 | 31.927 | 194.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 77 | `100k-random-lz4-p12-saturate` | 5 | reference | 1766 | 128.758 | 560.039 | 1171.358 | 2238.667 | 202.5 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 78 | `1k-random-lz4-p1-saturate` | 5 | reference | 39082 | 755.368 | 832.939 | 866.804 | 49.920 | 202.1 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 79 | `1k-random-lz4-p12-fixed-20k` | 5 | reference | 19993 | 4.233 | 5.907 | 9.628 | 67.183 | 202.7 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 80 | `1k-random-lz4-p1000-fixed-5k` | 5 | reference | 4998 | 11.155 | 17.734 | 30.921 | 457.367 | 266.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 81 | `1k-random-lz4-p12-idle-burst` | 5 | reference | 43372 | 532.661 | 1040.891 | 1106.043 | 133.660 | 269.4 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 82 | `1k-random-lz4-p12-saturate` | 5 | candidate | 177811 | 108.176 | 517.248 | 1205.112 | 17.780 | 138.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 83 | `100b-random-lz4-p12-saturate` | 5 | candidate | 1015983 | 5.716 | 21.906 | 65.300 | 3.398 | 143.6 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 84 | `1k-zeros-lz4-p12-saturate` | 5 | candidate | 745563 | 201.065 | 358.928 | 421.489 | 2.257 | 169.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 85 | `1k-random-none-p12-saturate` | 5 | candidate | 228317 | 128.347 | 218.878 | 837.933 | 18.923 | 178.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 86 | `100k-random-lz4-p12-saturate` | 5 | candidate | 2305 | 118.145 | 332.665 | 878.417 | 1419.333 | 181.4 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 87 | `1k-random-lz4-p1-saturate` | 5 | candidate | 51345 | 575.363 | 616.672 | 634.026 | 36.090 | 182.3 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 88 | `1k-random-lz4-p12-fixed-20k` | 5 | candidate | 19994 | 3.854 | 5.530 | 14.973 | 50.383 | 187.5 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 89 | `1k-random-lz4-p1000-fixed-5k` | 5 | candidate | 4999 | 9.841 | 14.971 | 35.528 | 386.100 | 249.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 90 | `1k-random-lz4-p12-idle-burst` | 5 | candidate | 49747 | 43.202 | 92.101 | 127.508 | 130.780 | 260.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
