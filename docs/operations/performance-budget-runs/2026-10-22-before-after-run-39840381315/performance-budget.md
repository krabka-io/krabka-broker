# Performance budget: `rf3-regression-v1`

These results compare one candidate build with an earlier krabka revision under the `rf3-regression-v1` contract on the host below. They are measured limits of that contract, not a ranking of brokers and not a supported limit.

**Verdict: pass.**

## Provenance

| Side | Label | Revision | Image | Digest |
| :--- | :--- | :--- | :--- | :--- |
| reference | krabka | `ac9deff316d38418622b16c237c671df88f57c7a` | `docker.io/krabka-io/krabka-broker:budget-reference` | `sha256:28790493f993d31710a2320edda66e262d17b9809faf1b349a7d84134550799a` |
| candidate | krabka | `bc037c1bff50a4a68b96a685e9a0ab58a2c35cb9` | `docker.io/krabka-io/krabka-broker:budget-candidate` | `sha256:abff322d1da88f2ab82c53fd75a7c146a8c38b4f58773d4895d1d01862496f7e` |

Client: `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`). Contract digest: `88125f23e83026670713516c73f401d80cced33577e1a4244b33b3cac83e4d17`. Started 2026-10-22T19:47:58Z.

Host: Intel(R) Xeon(R) CPU @ 2.20GHz, 32 logical CPUs, 135056588800 bytes of memory, 1211235811328 bytes of free disk, kernel 7.0.0-1011-gcp.

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
| `1k-random-lz4-p12-saturate` | records/s | 164369 | 187478 | -14.1% | 3.9% / 8.9% | +26.7% | within budget |
| `1k-random-lz4-p12-saturate` | p50 ms | 156.871 | 115.058 | -26.7% | 0.9% / 4.0% | — | recorded |
| `1k-random-lz4-p12-saturate` | p95 ms | 248.768 | 283.592 | +14.0% | 0.4% / 25.6% | — | recorded |
| `1k-random-lz4-p12-saturate` | p99 ms | 840.885 | 1016.844 | +20.9% | 20.0% / 10.3% | — | recorded |
| `1k-random-lz4-p12-saturate` | CPU µs/record | 31.290 | 17.953 | -42.6% | 1.3% / 0.3% | +5.0% | improved |
| `1k-random-lz4-p12-saturate` | peak RSS MiB | 140.7 | 139.6 | -0.8% | 0.7% / 1.7% | +10.0% | within budget |
| `100b-random-lz4-p12-saturate` | records/s | 932244 | 1013227 | -8.7% | 0.3% / 1.0% | +5.0% | improved |
| `100b-random-lz4-p12-saturate` | p50 ms | 8.599 | 5.079 | -40.9% | 3.6% / 4.3% | — | recorded |
| `100b-random-lz4-p12-saturate` | p95 ms | 19.303 | 14.135 | -26.8% | 9.7% / 10.3% | — | recorded |
| `100b-random-lz4-p12-saturate` | p99 ms | 51.784 | 39.139 | -24.4% | 25.6% / 32.1% | — | recorded |
| `100b-random-lz4-p12-saturate` | CPU µs/record | 4.942 | 3.408 | -31.0% | 0.8% / 0.4% | +5.0% | improved |
| `100b-random-lz4-p12-saturate` | peak RSS MiB | 144.8 | 143.5 | -0.9% | 0.9% / 2.3% | +10.0% | within budget |
| `1k-zeros-lz4-p12-saturate` | records/s | 712885 | 720374 | -1.1% | 4.2% / 1.7% | +12.6% | within budget |
| `1k-zeros-lz4-p12-saturate` | p50 ms | 218.943 | 185.345 | -15.3% | 15.1% / 24.6% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p95 ms | 415.420 | 393.033 | -5.4% | 2.8% / 18.7% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | p99 ms | 493.160 | 449.333 | -8.9% | 5.6% / 8.1% | — | recorded |
| `1k-zeros-lz4-p12-saturate` | CPU µs/record | 2.770 | 2.233 | -19.4% | 0.8% / 1.6% | +5.0% | improved |
| `1k-zeros-lz4-p12-saturate` | peak RSS MiB | 193.5 | 170.2 | -12.1% | 0.5% / 0.9% | +10.0% | improved |
| `1k-random-none-p12-saturate` | records/s | 161410 | 189077 | -17.1% | 3.0% / 2.9% | +9.1% | improved |
| `1k-random-none-p12-saturate` | p50 ms | 183.592 | 132.049 | -28.1% | 1.6% / 1.1% | — | recorded |
| `1k-random-none-p12-saturate` | p95 ms | 251.662 | 407.724 | +62.0% | 2.9% / 30.7% | — | recorded |
| `1k-random-none-p12-saturate` | p99 ms | 588.678 | 1257.767 | +113.7% | 34.4% / 11.2% | — | recorded |
| `1k-random-none-p12-saturate` | CPU µs/record | 33.170 | 19.580 | -41.0% | 1.3% / 1.2% | +5.0% | improved |
| `1k-random-none-p12-saturate` | peak RSS MiB | 195.5 | 177.4 | -9.3% | 0.1% / 1.7% | +10.0% | within budget |
| `100k-random-lz4-p12-saturate` | records/s | 1795 | 2113 | -17.7% | 3.6% / 9.2% | +27.7% | within budget |
| `100k-random-lz4-p12-saturate` | p50 ms | 131.790 | 111.657 | -15.3% | 0.9% / 1.3% | — | recorded |
| `100k-random-lz4-p12-saturate` | p95 ms | 391.956 | 261.799 | -33.2% | 41.6% / 6.2% | — | recorded |
| `100k-random-lz4-p12-saturate` | p99 ms | 1191.008 | 1268.317 | +6.5% | 3.1% / 12.1% | — | recorded |
| `100k-random-lz4-p12-saturate` | CPU µs/record | 2293.000 | 1453.667 | -36.6% | 0.2% / 0.4% | +5.0% | improved |
| `100k-random-lz4-p12-saturate` | peak RSS MiB | 202.6 | 187.0 | -7.7% | 0.4% / 0.5% | +10.0% | within budget |
| `1k-random-lz4-p1-saturate` | records/s | 37789 | 49363 | -30.6% | 2.0% / 1.2% | +6.1% | improved |
| `1k-random-lz4-p1-saturate` | p50 ms | 784.545 | 598.995 | -23.7% | 1.4% / 1.5% | — | recorded |
| `1k-random-lz4-p1-saturate` | p95 ms | 873.084 | 664.367 | -23.9% | 3.8% / 3.1% | — | recorded |
| `1k-random-lz4-p1-saturate` | p99 ms | 901.017 | 675.465 | -25.0% | 2.1% / 3.1% | — | recorded |
| `1k-random-lz4-p1-saturate` | CPU µs/record | 52.070 | 37.540 | -27.9% | 1.1% / 1.0% | +5.0% | improved |
| `1k-random-lz4-p1-saturate` | peak RSS MiB | 202.1 | 187.8 | -7.0% | 0.5% / 1.6% | +10.0% | within budget |
| `1k-random-lz4-p12-fixed-20k` | records/s | 19993 | 19994 | -0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p12-fixed-20k` | p50 ms | 4.383 | 3.993 | -8.9% | 0.4% / 0.3% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p95 ms | 6.096 | 5.607 | -8.0% | 0.2% / 0.6% | — | recorded |
| `1k-random-lz4-p12-fixed-20k` | p99 ms | 9.256 | 7.717 | -16.6% | 4.5% / 6.1% | +18.3% | within budget |
| `1k-random-lz4-p12-fixed-20k` | CPU µs/record | 69.633 | 53.250 | -23.5% | 0.1% / 0.2% | +5.0% | improved |
| `1k-random-lz4-p12-fixed-20k` | peak RSS MiB | 202.8 | 189.0 | -6.8% | 0.5% / 1.5% | +10.0% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | records/s | 4998 | 4999 | -0.0% | 0.0% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | p50 ms | 11.626 | 10.529 | -9.4% | 0.4% / 0.9% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p95 ms | 19.147 | 17.306 | -9.6% | 0.2% / 0.8% | — | recorded |
| `1k-random-lz4-p1000-fixed-5k` | p99 ms | 37.487 | 37.521 | +0.1% | 5.3% / 9.3% | +27.8% | within budget |
| `1k-random-lz4-p1000-fixed-5k` | CPU µs/record | 466.767 | 408.400 | -12.5% | 0.8% / 1.4% | +5.0% | improved |
| `1k-random-lz4-p1000-fixed-5k` | peak RSS MiB | 266.0 | 266.6 | +0.2% | 0.2% / 3.7% | +11.0% | within budget |
| `1k-random-lz4-p12-idle-burst` | records/s | 49504 | 49728 | -0.5% | 0.1% / 0.0% | +5.0% | within budget |
| `1k-random-lz4-p12-idle-burst` | p50 ms | 475.555 | 50.182 | -89.4% | 9.4% / 2.1% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p95 ms | 902.849 | 101.356 | -88.8% | 22.7% / 3.8% | — | recorded |
| `1k-random-lz4-p12-idle-burst` | p99 ms | 1091.722 | 139.444 | -87.2% | 6.1% / 1.6% | +18.2% | improved |
| `1k-random-lz4-p12-idle-burst` | CPU µs/record | 133.540 | 133.020 | -0.4% | 3.5% / 0.2% | +10.6% | within budget |
| `1k-random-lz4-p12-idle-burst` | peak RSS MiB | 269.7 | 279.1 | +3.5% | 0.3% / 2.1% | +10.0% | within budget |

## Raw trials

| Order | Cell | Round | Side | records/s | p50 ms | p95 ms | p99 ms | CPU µs/record | peak RSS MiB | Outcome |
| ---: | :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 1 | `1k-random-lz4-p12-saturate` | 1 | reference | 166631 | 154.320 | 201.766 | 1079.233 | 30.747 | 142.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 2 | `100b-random-lz4-p12-saturate` | 1 | reference | 932244 | 7.794 | 17.438 | 84.681 | 4.942 | 146.7 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 3 | `1k-zeros-lz4-p12-saturate` | 1 | reference | 712885 | 216.393 | 406.402 | 497.053 | 2.770 | 194.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 4 | `1k-random-none-p12-saturate` | 1 | reference | 169789 | 179.546 | 244.414 | 420.299 | 32.753 | 195.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 5 | `100k-random-lz4-p12-saturate` | 1 | reference | 1778 | 131.790 | 563.548 | 1210.028 | 2293.000 | 203.4 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 6 | `1k-random-lz4-p1-saturate` | 1 | reference | 37997 | 781.496 | 840.299 | 864.172 | 51.490 | 203.4 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 7 | `1k-random-lz4-p12-fixed-20k` | 1 | reference | 19993 | 4.371 | 6.047 | 9.419 | 68.850 | 203.9 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 8 | `1k-random-lz4-p1000-fixed-5k` | 1 | reference | 4998 | 11.626 | 19.118 | 37.487 | 467.100 | 266.6 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 9 | `1k-random-lz4-p12-idle-burst` | 1 | reference | 45350 | 520.471 | 1050.217 | 1144.241 | 128.820 | 268.8 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 10 | `1k-random-lz4-p12-saturate` | 1 | candidate | 217671 | 121.031 | 264.604 | 628.451 | 17.957 | 138.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 11 | `100b-random-lz4-p12-saturate` | 1 | candidate | 902827 | 5.577 | 65.363 | 1013.286 | 3.397 | 143.4 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 12 | `1k-zeros-lz4-p12-saturate` | 1 | candidate | 720374 | 230.910 | 432.173 | 485.931 | 2.233 | 168.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 13 | `1k-random-none-p12-saturate` | 1 | candidate | 190303 | 132.343 | 235.304 | 1342.105 | 19.807 | 174.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 14 | `100k-random-lz4-p12-saturate` | 1 | candidate | 2263 | 110.231 | 253.223 | 1114.222 | 1466.333 | 187.5 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 15 | `1k-random-lz4-p1-saturate` | 1 | candidate | 48780 | 604.591 | 675.032 | 702.231 | 37.710 | 188.8 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 16 | `1k-random-lz4-p12-fixed-20k` | 1 | candidate | 19994 | 4.043 | 5.687 | 8.188 | 54.367 | 191.8 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 17 | `1k-random-lz4-p1000-fixed-5k` | 1 | candidate | 4998 | 10.800 | 18.913 | 45.159 | 414.267 | 268.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 18 | `1k-random-lz4-p12-idle-burst` | 1 | candidate | 49630 | 52.862 | 105.159 | 139.444 | 133.120 | 280.2 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 19 | `1k-random-lz4-p12-saturate` | 2 | candidate | 207484 | 115.058 | 211.095 | 1016.844 | 17.897 | 139.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 20 | `100b-random-lz4-p12-saturate` | 2 | candidate | 1028463 | 4.861 | 12.674 | 39.139 | 3.394 | 143.5 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 21 | `1k-zeros-lz4-p12-saturate` | 2 | candidate | 707839 | 193.460 | 475.382 | 577.444 | 2.197 | 170.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 22 | `1k-random-none-p12-saturate` | 2 | candidate | 180612 | 130.577 | 532.749 | 1257.767 | 18.983 | 177.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 23 | `100k-random-lz4-p12-saturate` | 2 | candidate | 2113 | 113.125 | 245.538 | 1280.042 | 1453.667 | 188.0 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 24 | `1k-random-lz4-p1-saturate` | 2 | candidate | 49830 | 590.302 | 664.367 | 675.465 | 37.010 | 191.7 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 25 | `1k-random-lz4-p12-fixed-20k` | 2 | candidate | 19994 | 3.993 | 5.599 | 7.227 | 53.367 | 194.3 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 26 | `1k-random-lz4-p1000-fixed-5k` | 2 | candidate | 4998 | 10.429 | 17.353 | 39.208 | 401.300 | 276.4 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 27 | `1k-random-lz4-p12-idle-burst` | 2 | candidate | 49540 | 49.129 | 99.009 | 141.640 | 133.340 | 288.6 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 28 | `1k-random-lz4-p12-saturate` | 2 | reference | 177379 | 158.242 | 221.149 | 673.007 | 31.290 | 138.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 29 | `100b-random-lz4-p12-saturate` | 2 | reference | 903585 | 8.599 | 43.394 | 308.627 | 4.935 | 143.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 30 | `1k-zeros-lz4-p12-saturate` | 2 | reference | 764706 | 125.217 | 254.720 | 294.247 | 2.747 | 189.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 31 | `1k-random-none-p12-saturate` | 2 | reference | 161410 | 183.592 | 269.829 | 588.678 | 33.170 | 195.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 32 | `100k-random-lz4-p12-saturate` | 2 | reference | 2072 | 130.604 | 224.818 | 1191.008 | 2298.667 | 201.0 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 33 | `1k-random-lz4-p1-saturate` | 2 | reference | 37026 | 795.536 | 905.872 | 920.259 | 52.070 | 200.5 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 34 | `1k-random-lz4-p12-fixed-20k` | 2 | reference | 19993 | 4.383 | 6.104 | 16.272 | 69.600 | 201.7 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 35 | `1k-random-lz4-p1000-fixed-5k` | 2 | reference | 4998 | 12.096 | 21.137 | 39.483 | 474.500 | 269.2 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 36 | `1k-random-lz4-p12-idle-burst` | 2 | reference | 49548 | 475.555 | 902.849 | 1091.722 | 133.540 | 274.2 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 37 | `1k-random-lz4-p12-saturate` | 3 | reference | 157992 | 172.552 | 249.792 | 840.885 | 33.873 | 140.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 38 | `100b-random-lz4-p12-saturate` | 3 | reference | 859160 | 11.569 | 22.705 | 38.503 | 5.351 | 144.8 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 39 | `1k-zeros-lz4-p12-saturate` | 3 | reference | 682871 | 218.943 | 426.978 | 493.160 | 3.057 | 193.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 40 | `1k-random-none-p12-saturate` | 3 | reference | 148224 | 200.810 | 265.834 | 1212.734 | 35.917 | 196.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 41 | `100k-random-lz4-p12-saturate` | 3 | reference | 1730 | 145.355 | 391.956 | 1154.623 | 2478.667 | 202.6 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 42 | `1k-random-lz4-p1-saturate` | 3 | reference | 35165 | 834.738 | 933.061 | 1106.271 | 53.350 | 202.1 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 43 | `1k-random-lz4-p12-fixed-20k` | 3 | reference | 19994 | 4.415 | 6.096 | 8.842 | 69.733 | 202.8 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 44 | `1k-random-lz4-p1000-fixed-5k` | 3 | reference | 4998 | 11.582 | 19.152 | 40.414 | 466.767 | 266.0 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 45 | `1k-random-lz4-p12-idle-burst` | 3 | reference | 49542 | 95.141 | 185.979 | 220.176 | 142.900 | 270.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 46 | `1k-random-lz4-p12-saturate` | 3 | candidate | 187478 | 124.718 | 283.592 | 742.335 | 18.603 | 143.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 47 | `100b-random-lz4-p12-saturate` | 3 | candidate | 1023472 | 5.033 | 12.749 | 22.210 | 3.429 | 149.1 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 48 | `1k-zeros-lz4-p12-saturate` | 3 | candidate | 732231 | 98.926 | 201.031 | 259.367 | 2.297 | 175.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 49 | `1k-random-none-p12-saturate` | 3 | candidate | 168453 | 142.823 | 258.087 | 1398.593 | 20.287 | 180.4 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 50 | `100k-random-lz4-p12-saturate` | 3 | candidate | 1853 | 111.657 | 733.214 | 1268.317 | 1495.667 | 187.0 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 51 | `1k-random-lz4-p1-saturate` | 3 | candidate | 45436 | 646.179 | 752.701 | 838.683 | 40.110 | 187.8 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 52 | `1k-random-lz4-p12-fixed-20k` | 3 | candidate | 19993 | 4.005 | 5.641 | 9.026 | 53.250 | 189.0 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 53 | `1k-random-lz4-p1000-fixed-5k` | 3 | candidate | 4999 | 10.678 | 17.306 | 32.660 | 410.033 | 247.9 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 54 | `1k-random-lz4-p12-idle-burst` | 3 | candidate | 49728 | 48.854 | 92.651 | 134.560 | 132.600 | 264.4 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 55 | `1k-random-lz4-p12-saturate` | 4 | candidate | 170777 | 114.254 | 522.783 | 1121.284 | 17.953 | 136.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 56 | `100b-random-lz4-p12-saturate` | 4 | candidate | 1013227 | 5.332 | 15.616 | 51.704 | 3.408 | 140.2 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 57 | `1k-zeros-lz4-p12-saturate` | 4 | candidate | 736682 | 185.345 | 393.033 | 449.333 | 2.197 | 168.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 58 | `1k-random-none-p12-saturate` | 4 | candidate | 194542 | 132.049 | 407.724 | 777.290 | 19.580 | 172.9 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 59 | `100k-random-lz4-p12-saturate` | 4 | candidate | 2309 | 113.735 | 261.799 | 1039.944 | 1448.333 | 182.7 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 60 | `1k-random-lz4-p1-saturate` | 4 | candidate | 50003 | 590.033 | 637.963 | 654.444 | 37.170 | 184.8 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 61 | `1k-random-lz4-p12-fixed-20k` | 4 | candidate | 19994 | 3.983 | 5.607 | 7.717 | 53.150 | 186.9 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 62 | `1k-random-lz4-p1000-fixed-5k` | 4 | candidate | 4999 | 10.529 | 17.169 | 37.521 | 401.367 | 266.6 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 63 | `1k-random-lz4-p12-idle-burst` | 4 | candidate | 49731 | 50.182 | 101.356 | 137.431 | 133.020 | 279.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 64 | `1k-random-lz4-p12-saturate` | 4 | reference | 156455 | 156.124 | 248.768 | 1104.525 | 31.317 | 140.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 65 | `100b-random-lz4-p12-saturate` | 4 | reference | 935479 | 8.619 | 18.722 | 39.394 | 4.982 | 143.5 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 66 | `1k-zeros-lz4-p12-saturate` | 4 | reference | 659349 | 304.973 | 668.431 | 757.747 | 2.840 | 191.7 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 67 | `1k-random-none-p12-saturate` | 4 | reference | 161164 | 180.726 | 244.365 | 791.260 | 32.947 | 195.3 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 68 | `100k-random-lz4-p12-saturate` | 4 | reference | 1795 | 130.639 | 536.234 | 1357.690 | 2273.667 | 203.2 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 69 | `1k-random-lz4-p1-saturate` | 4 | reference | 37789 | 784.545 | 873.084 | 901.017 | 52.090 | 203.0 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 70 | `1k-random-lz4-p12-fixed-20k` | 4 | reference | 19992 | 4.531 | 6.306 | 9.256 | 71.750 | 203.5 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 71 | `1k-random-lz4-p1000-fixed-5k` | 4 | reference | 4999 | 11.649 | 19.147 | 33.114 | 463.000 | 265.7 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 72 | `1k-random-lz4-p12-idle-burst` | 4 | reference | 49504 | 86.627 | 160.207 | 196.199 | 142.040 | 268.1 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 73 | `1k-random-lz4-p12-saturate` | 5 | reference | 164369 | 156.871 | 249.249 | 817.347 | 30.870 | 141.8 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 74 | `100b-random-lz4-p12-saturate` | 5 | reference | 935487 | 8.287 | 19.303 | 51.784 | 4.899 | 146.0 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 75 | `1k-zeros-lz4-p12-saturate` | 5 | reference | 722719 | 251.962 | 415.420 | 465.736 | 2.750 | 193.5 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 76 | `1k-random-none-p12-saturate` | 5 | reference | 166318 | 184.765 | 251.662 | 303.667 | 33.837 | 195.6 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 77 | `100k-random-lz4-p12-saturate` | 5 | reference | 2012 | 132.491 | 228.784 | 961.086 | 2288.667 | 201.3 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 78 | `1k-random-lz4-p1-saturate` | 5 | reference | 40332 | 729.587 | 794.869 | 882.955 | 50.380 | 201.4 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 79 | `1k-random-lz4-p12-fixed-20k` | 5 | reference | 19993 | 4.366 | 6.081 | 8.807 | 69.633 | 201.8 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 80 | `1k-random-lz4-p1000-fixed-5k` | 5 | reference | 4998 | 11.372 | 18.510 | 36.327 | 459.167 | 265.1 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 81 | `1k-random-lz4-p12-idle-burst` | 5 | reference | 45718 | 512.906 | 1108.193 | 1158.119 | 130.700 | 269.7 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
| 82 | `1k-random-lz4-p12-saturate` | 5 | candidate | 174357 | 110.417 | 727.519 | 1033.474 | 17.790 | 142.0 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 83 | `100b-random-lz4-p12-saturate` | 5 | candidate | 1012262 | 5.079 | 14.135 | 33.892 | 3.440 | 147.1 | sent 10000000 / consumed 10000000 / duplicates 0 / errors 0 |
| 84 | `1k-zeros-lz4-p12-saturate` | 5 | candidate | 697331 | 132.629 | 319.716 | 416.056 | 2.267 | 170.2 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 85 | `1k-random-none-p12-saturate` | 5 | candidate | 189077 | 127.835 | 420.434 | 1089.261 | 19.373 | 178.1 | sent 3000000 / consumed 3000000 / duplicates 0 / errors 0 |
| 86 | `100k-random-lz4-p12-saturate` | 5 | candidate | 1739 | 108.530 | 863.274 | 2179.908 | 1447.333 | 181.6 | sent 30000 / consumed 30000 / duplicates 0 / errors 0 |
| 87 | `1k-random-lz4-p1-saturate` | 5 | candidate | 49363 | 598.995 | 643.916 | 660.359 | 37.540 | 181.8 | sent 1000000 / consumed 1000000 / duplicates 0 / errors 0 |
| 88 | `1k-random-lz4-p12-fixed-20k` | 5 | candidate | 19994 | 3.954 | 5.563 | 7.460 | 52.867 | 183.2 | sent 600000 / consumed 600000 / duplicates 0 / errors 0 |
| 89 | `1k-random-lz4-p1000-fixed-5k` | 5 | candidate | 4999 | 10.499 | 17.133 | 34.047 | 408.400 | 254.8 | sent 300000 / consumed 300000 / duplicates 0 / errors 0 |
| 90 | `1k-random-lz4-p12-idle-burst` | 5 | candidate | 49738 | 51.253 | 120.014 | 171.341 | 131.900 | 273.3 | sent 500000 / consumed 500000 / duplicates 0 / errors 0 |
