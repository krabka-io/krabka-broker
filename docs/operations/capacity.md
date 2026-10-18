# Capacity

How to size a krabka-broker cluster and how to read the series that say
whether the size is right. The numbers here are the ones the broker enforces
or defaults to. The [measured envelope](#measured-envelope) lists every
measured throughput, latency and partition number with its workload, host and
artifact. None of them is a capacity promise. Re-run the workload on
production hardware before you size a cluster from it.

## What a broker spends

| Resource | Spent on | Read it from |
| :--- | :--- | :--- |
| Disk bytes | Every partition replica's log, leader or follower, plus the metadata log. | `krabka_broker_partition_disk_bytes`, summed. |
| Disk write bandwidth | Ingest on each partition this broker hosts: leader appends plus follower appends. | `rate(krabka_broker_partition_bytes_in_total[5m]) + rate(krabka_broker_replication_bytes_in_total[5m])`. |
| Network in | Producer bytes plus follower fetches from leaders. | `rate(krabka_broker_topic_bytes_in_total[5m]) + rate(krabka_broker_replication_bytes_in_total[5m])`. |
| Network out | Consumer bytes plus bytes served to followers. | `rate(krabka_broker_topic_bytes_out_total[5m])`, which includes both; `replication_bytes_out` is the follower share. |
| CPU | Request handling per partition. | `sum(rate(krabka_broker_partition_cpu_micros_total[5m])) / 1000000` is cores busy in handlers. |
| File descriptors | One per client connection plus the segment files of every replica. | `krabka_broker_active_connections`. |
| Memory | Partition state, fetch-session cache, in-flight request buffers, the diskless hot tail. | Request `512 MiB + 64 KiB × local replica count + 2 KiB × krabka_broker_incremental_fetch_partitions_cached + diskless_wal_hot_tail_max_size`; set the limit to twice the request. The reference 1 GiB request represents 2,048 replicas, 65,536 cached fetch partitions and a 256 MiB hot tail. |

## Sizing rules

**Ingest.** Every byte a producer writes lands on `replication.factor`
brokers. Cluster disk write bandwidth is ingest times the replication factor,
and cluster network in is the same. Cluster network out is consumer fan-out
times ingest, plus ingest times `replication.factor - 1` for the followers.

**Disk.** A topic's disk is ingest times retention times the replication
factor. Keep 20 percent free: the log cleaner and segment rolls need room,
and a disk that fills stops every partition on it, not one.

**Partitions.** Each partition costs a segment file set, a place in the
per-second gauge sampler, and a series in every per-partition family.
`krabka_broker_partitions_total` is the count to watch; the per-partition
families multiply by it. The reference workload passed 10,000 user
partitions at RF 3 on its stated 16-CPU host, with up to 9,718 hosted replicas
reported on one broker. Treat that as one measured point, not a supported
ceiling.

**Replication headroom.** A follower must take ingest faster than the leader
receives it, or it falls out of the ISR after `replica_lag_time_max` (default
30s). Leave the disk and the network at or below 70 percent of their measured
ceiling on every broker, because the follower's ingest arrives in bursts and
the leader's does not wait.

**Connections.** Set `max_connections` and `max_connections_per_ip` in the
config file below the process's file-descriptor limit, leaving room for the
segment files. The defaults are unbounded.

## Reading the phase families

`krabka_broker_request_local_duration_seconds`,
`krabka_broker_request_remote_duration_seconds` and
`krabka_broker_request_throttle_duration_seconds` say where a slow request
spent its time.

- Local high on Produce: the disk. Append latency on this broker's own log.
- Remote high on Produce with local low: replication. The `acks=all` gate
  waits for a follower. Look at that follower's disk and link, not this
  broker's.
- Remote high on Fetch: the long poll, which is normal, or the object store
  behind a tiered or diskless partition, which is not.
- Throttle high: a quota. `krabka_broker_quota_throttle_duration_seconds`
  names the one that produced the delay, and that is the one to raise.
- Total minus the three phases growing: handler CPU. Decode, authorization,
  validation and encode. Add cores or spread partitions.

Compare the `_sum` streams for that remainder. Do not subtract `_bucket`
streams: each family observes its own durations, so the buckets do not line up.

## Diskless WAL

A `krabka.diskless=true` partition has no local replica log. Its appends go
to `diskless_wal_local_replica_count` WAL voters (default 3), which write them
to a WAL directory on local disk. The leader flushes the committed WAL to the
object store every `diskless_wal_flush_interval` (default 250ms) or at
`diskless_wal_flush_max_size`, whichever comes first, publishes the index
record, and trims the local WAL behind the committed index.

- Local disk per diskless shard is the WAL between the trim frontier and the
  durable watermark, not the retention. At steady state that is a few flush
  intervals of ingest on each voter. While the object store is down it grows
  at the ingest rate on every voter, so size the WAL disk for the outage you
  want to ride through.
- `diskless_wal_hot_tail_max_size` (default 64 MiB) bounds an in-memory cache
  of committed batches that serves recent fetches without a disk read. It is
  memory per broker, not disk, and the cache evicts oldest first.
- Object-store write rate is `rate(krabka_broker_diskless_wal_flush_bytes_total[5m])`.
  Object-store request rate is `rate(krabka_broker_diskless_wal_flush_attempts_total[5m])`.
  A shorter flush interval means smaller, more frequent objects.
- A consumer that reads below `krabka_broker_diskless_wal_trim_frontier` costs
  an object-store read. `krabka_broker_diskless_wal_cold_read_hits_total`
  counts them. A fleet whose consumers keep up sees almost none.
- Bucket size per diskless topic is ingest times `retention.ms`, or
  `retention.bytes` where that is lower, plus a lag. Retention expires index
  ranges, and an object is deleted only once every partition sharing it has
  expired its own ranges, so size the bucket with headroom for one flush
  object's worth of co-tenancy and the five-minute reclaim grace on top of the
  retention itself. `krabka_broker_diskless_wal_expired_ranges_total` stuck at
  zero on a topic with a finite retention means nothing is being reclaimed.

## Tiered storage

With `[remote_storage]`, local disk holds only the segments newer than
`local.retention.ms` on each tiered topic. The rest is in the bucket. Local
disk is then ingest times local retention times the replication factor. A
Fetch below the local retention shows up in the remote phase.

The replication factor in that formula is there because every replica of a
tiered topic enforces `local.retention.*` on its own disk, followers included,
not only the leader. A follower drops a segment once the shared remote-log
metadata says the leader finished copying it, so a follower's footprint tracks
`local.retention.*` rather than `retention.*`, and a leadership change moves no
bytes.

## Scrape size

The `/metrics` body grows with the label sets that are live. The
per-partition families add one sample per hosted replica each, and the
per-topic families one per topic. The reference run measured at most 2.38 MB,
27,249 series and a 1.121-second scrape at roughly 10,000 hosted replicas.
Scrape at 30s or 60s rather than 15s, and drop the per-partition families at
the scrape with `metric_relabel_configs` when a dashboard does not need them.

## Measured envelope

Every number in this section is a measurement of one workload on one host at
one revision. The tables name all three. No number here was measured on a
release candidate: [#1401](https://github.com/krabka-io/krabka-broker/issues/1401)
has not frozen one, and the [support matrix](support-matrix.md) tracks the
reruns that the candidate needs.

Read the tables with these rules:

- **No maximum.** A row says what passed at the load it ran. The highest tier
  that passed is the highest tested tier, not the broker's maximum. Do not
  read a ceiling from it.
- **VM-indicative rows** come from a 4-vCPU cloud VM with one virtio disk.
  They show the direction of a change between two builds on that VM. Do not
  compare them with the benchmark-host rows, and do not size from them.
- **Same contract only.** A Kafka number beside a krabka number compares the
  two on that contract. It does not rank the brokers in general.

### Hosts

| Host | Hardware | Broker resources |
| :--- | :--- | :--- |
| Benchmark host, Kind | AMD EPYC 4344P, 16 logical CPUs, about 61 GiB RAM, RAID-backed disk | Three combined broker/controller pods, each 2 CPU and 2 GiB. |
| Benchmark host, OpenMessaging | ext4 on md RAID1 of two NVMe drives, Linux 7.0 | One broker, 4 CPU and 10 GiB. |
| Cloud VM (VM-indicative) | 4 vCPU, 15 GiB, ext4 on one virtio disk | Three combined broker/controllers on cores 0 to 2, client on core 3. |

### RF3 throughput and latency

All rows: RF 3, `min.insync.replicas=2`, `acks=all`, idempotence, 1 KiB
records, 12 partitions, a 65,536-byte batch and a 5 ms linger, on the
benchmark host under Kind.

| Workload | records/s | p99 | CPU per record | Artifact | Source |
| :--- | ---: | ---: | ---: | :--- | :--- |
| Random, LZ4, unlimited, Kafka 4.3.1 client | 342,679 | 553.9 ms | 7.293 µs | Baseline `40194573` with the release changes, image `sha256:66999791…` with the binary mounted | [Kafka 4.3.1 experiment](performance-kafka-4.3.1.md#three-brokers-rf3minisr2), 2026-09-30, median of three topics |
| Random, LZ4, 20,000 records/s, Kafka 4.3.1 client | 19,997 | 4.0 ms | 17.073 µs | As above | As above |
| LZ4, saturation, 100,000 records, Kafka 4.0.0 client | 172,020 | 311.533 ms | not recorded | `106ae27b`, image `sha256:d46923ca…` | [Broker performance results](performance-results.md#kafka-comparison), median of three runs |
| LZ4, 5,000 records/s, Kafka 4.0.0 client | 4,999 | 7.790 ms | not recorded | As above | As above |

The two experiments ran different revisions, clients and record counts. Their
saturation numbers differ by a factor of two, so use neither as the RF3
throughput of a later build.

### Partition count

From [Broker performance results](performance-results.md#partition-envelope),
at `106ae27b`. Each tier ran 600,000 acknowledged records at 1,000 records/s
while the test deleted the active controller and moved 1,000 partitions to a
fourth broker.

| User partitions | Replicas on the largest broker | Failover | p99 | Result |
| ---: | ---: | ---: | ---: | :--- |
| 1,000 | 1,000 | 22 s | 10,084 ms | pass |
| 10,000 | 9,667 | 23 s | 12,756 ms | pass |

10,000 is the highest tested tier. No tier above it ran, so this page claims
no partition limit.

### One partition at maximum rate

From [#1370](https://github.com/krabka-io/krabka-broker/issues/1370#issuecomment-6156154004),
on the OpenMessaging benchmark host: workload
`max-rate-1-topic-1-partition-1p-1c-1kb`, RF 1, the Kafka 3.6.1 client,
`acks=all`, idempotence, one in-flight request, 1 ms linger and no
compression.

| Build | `preallocate` | Publish rate | End-to-end p99 |
| :--- | :--- | ---: | ---: |
| `v1.0.3` (`2de40082`) | `true` | 444,000 to 651,000/s | 95 ms to 58.6 s |
| `main` with #1373 and #1376, revision not recorded | `true` | 801,000 to 903,000/s | 55 to 82 ms |
| Same host, page cache | `false` | 920,000 to 1,110,000/s | 117 to 253 ms |

The drives were not trimmed between some of these trials.
[A later comment](https://github.com/krabka-io/krabka-broker/issues/1370#issuecomment-6157203260)
shows that this moved direct-write throughput by up to a factor of three. The
runner trims before every trial since then. [#1438](https://github.com/krabka-io/krabka-broker/pull/1438)
changed the write alignment after these runs and has no benchmark-host rerun.

### VM-indicative before and after

These rows say only that a change moved a number on the cloud VM.

| Change | Workload | Before | After | Source |
| :--- | :--- | :--- | :--- | :--- |
| Delete retired segments outside the log lock | RF3, 1 partition, saturation, `krabka-bench-driver` | 89,300 records/s, ack p99.9 638 ms, at `4e342a55` | 109,700 records/s, ack p99.9 93 ms | [#1457](https://github.com/krabka-io/krabka-broker/pull/1457), five interleaved rounds |
| As above | RF3, 6 partitions, 20,000 records/s, Kafka 4.0.0 client | end-to-end p99 75 ms | 37 ms, with overlapping ranges | As above |
| krabka-client-rs consumer metadata refresh | RF3, 6 partitions, 20,000 records/s | consumer end-to-end p99 480 ms | 10 ms | [#1394 comment](https://github.com/krabka-io/krabka-broker/issues/1394#issuecomment-6227085667) |
| `O_DIRECT` alignment to the filesystem block | `log-iobench` `direct_append`, 1 MiB batches | 110 MB/s | 430 MB/s | [#1438](https://github.com/krabka-io/krabka-broker/pull/1438), six interleaved rounds |

### Not measured

- **The RF3 regression budgets.** The contract `rf3-regression-v1` is checked
  in, and [#1395](https://github.com/krabka-io/krabka-broker/issues/1395) has
  no run of it on the benchmark host yet. Its budgets are not calibrated.
- **TLS, SASL, tiered and diskless throughput.** No run above used
  authentication, encryption, a remote tier or the diskless WAL.
- **Sustained load.** The 72-hour soak has not run.
  [#1393](https://github.com/krabka-io/krabka-broker/issues/1393) owns it.
