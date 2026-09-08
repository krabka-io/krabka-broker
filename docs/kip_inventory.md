# Kafka KIP inventory

This file is the roster of Kafka KIPs krabka's compatibility target covers. It
is hand-maintained and reviewed; `docs/KIP_MATRIX.md`, which sits beside it, is
generated and says how far each one is taken.

The two files are checked against each other by `aspect generate-kip-matrix`.
Every KIP listed here has to resolve to a `KIP_ANNOTATIONS` row in
[`api_catalog`](../crates/broker/src/api_catalog.rs), and so to a matrix row
whose status is `Implemented`, `Partial` or `Out of scope`. A KIP is therefore
absent from the matrix only by deleting its line below, which is a visible
decision in a diff, rather than by nobody having written `KIP-<n>` in a comment.

The roster carries the number alone. The claim, the status, the owning module,
the tests and the caveats all live in the annotation row, and duplicating any of
them here would only let the two drift apart. To add a KIP to the target, add
its line and its row together.

<!-- BEGIN KIP_INVENTORY -->
- KIP-13
- KIP-32
- KIP-48
- KIP-62
- KIP-73
- KIP-98
- KIP-101
- KIP-107
- KIP-108
- KIP-110
- KIP-112
- KIP-113
- KIP-124
- KIP-133
- KIP-207
- KIP-211
- KIP-219
- KIP-226
- KIP-227
- KIP-255
- KIP-257
- KIP-290
- KIP-320
- KIP-345
- KIP-360
- KIP-368
- KIP-371
- KIP-382
- KIP-392
- KIP-394
- KIP-405
- KIP-412
- KIP-429
- KIP-430
- KIP-447
- KIP-455
- KIP-460
- KIP-467
- KIP-482
- KIP-496
- KIP-500
- KIP-511
- KIP-516
- KIP-525
- KIP-534
- KIP-546
- KIP-554
- KIP-559
- KIP-584
- KIP-590
- KIP-595
- KIP-599
- KIP-612
- KIP-630
- KIP-631
- KIP-642
- KIP-664
- KIP-704
- KIP-714
- KIP-734
- KIP-768
- KIP-778
- KIP-827
- KIP-841
- KIP-848
- KIP-853
- KIP-858
- KIP-890
- KIP-903
- KIP-919
- KIP-932
- KIP-939
- KIP-950
- KIP-951
- KIP-966
- KIP-996
- KIP-1005
- KIP-1022
- KIP-1023
- KIP-1071
- KIP-1073
- KIP-1075
- KIP-1101
- KIP-1142
- KIP-1155
- KIP-1186
- KIP-1242
- KIP-1263
- KIP-1319
<!-- END KIP_INVENTORY -->
