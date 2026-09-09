# Performance qualification

The performance qualification compares one exact Krabka revision with Apache
Kafka 4.0.0, then measures Krabka's partition envelope. It is a workload
result, not a claim that either broker is universally faster or that the
largest passing tier is a supported limit.

Run the full protocol from a clean Linux host with at least 16 CPUs, 32 GiB of
available RAM, 40 GiB of free disk, and `fs.inotify.max_user_instances` set to
at least 512:

```sh
PERF_ARTIFACT_DIR=$PWD/performance-artifacts \
  packaging/performance/qualify.sh full
```

The harness creates and removes a dedicated four-worker Kind cluster. Set
`PERF_KEEP_CLUSTER=1` to retain it after a failure. `smoke` uses 30 and 100
partitions, one short comparison run and the same failure paths; it validates
the machinery but produces no publishable performance result.

## Fixed comparison contract

Both brokers run as three combined broker/controller pods. Each broker gets a
2 CPU limit, 2 GiB memory limit and a 100 GiB volume claim. The same Kafka 4.0.0
client image drives twelve partitions at replication factor 3,
`min.insync.replicas=2`, `acks=all`, idempotence enabled, LZ4 compression, a
65,536-byte batch and 5 ms linger. Each side gets three saturation runs and
three fixed-rate steady-state runs. Every run reports end-to-end p50/p95/p99
latency, and the producer and consumer counts must agree or the harness fails.

The output keeps exact Git and image revisions, host resources, manifests,
producer and consumer reports, end-to-end p50/p95/p99 latency, and process CPU,
RSS, file-descriptor, disk and network snapshots. Publish the directory
unchanged so a summary remains traceable to its raw inputs.

## Partition envelope contract

The full run starts a fresh four-broker Krabka cluster for each tier. It must
pass 1,000 partitions and attempts 10,000, always at replication factor 3. A
fixed-rate producer and consumer stay active while the controller leader is
deleted, its replacement is observed, the old pod becomes ready again, and up
to 1,000 partitions are reassigned across all four brokers. Each tier has a
30-minute deadline.

For each broker the evidence records total hosted replicas, metadata lag,
process RSS, file descriptors, disk and network use, and metrics scrape time,
bytes and series count. `scale/verdict.txt` names the highest passing tier;
failure of the 10,000-partition attempt is evidence, not permission to describe
1,000 as a product limit.
