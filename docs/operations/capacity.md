# Capacity

How to size a krabka-broker cluster and how to read the series that say
whether the size is right. The numbers here are the ones the broker
enforces or defaults to; the sizing rules are the ones a JVM Kafka fleet
uses, because the data path has the same shape.

## What a broker spends

| Resource | Spent on | Read it from |
| :--- | :--- | :--- |
| Disk bytes | Every partition replica's log, leader or follower, plus the metadata log. | `krabka_broker_partition_disk_bytes`, summed. |
| Disk write bandwidth | Ingest on each partition this broker hosts: leader appends plus follower appends. | `rate(krabka_broker_partition_bytes_in_total[5m]) + rate(krabka_broker_replication_bytes_in_total[5m])`. |
| Network in | Producer bytes plus follower fetches from leaders. | `rate(krabka_broker_topic_bytes_in_total[5m]) + rate(krabka_broker_replication_bytes_in_total[5m])`. |
| Network out | Consumer bytes plus bytes served to followers. | `rate(krabka_broker_topic_bytes_out_total[5m])`, which includes both; `replication_bytes_out` is the follower share. |
| CPU | Request handling per partition. | `sum(rate(krabka_broker_partition_cpu_micros_total[5m])) / 1000000` is cores busy in handlers. |
| File descriptors | One per client connection plus the segment files of every replica. | `krabka_broker_active_connections`. |
| Memory | Fetch-session cache, in-flight request buffers, the diskless hot tail. | `krabka_broker_incremental_fetch_partitions_cached`, `krabka_broker_in_flight_requests`. |

## Sizing rules

**Ingest.** Every byte a producer writes lands on `replication.factor`
brokers. Cluster disk write bandwidth is ingest times the replication factor,
and cluster network in is the same. Cluster network out is consumer fan-out
times ingest, plus ingest times `replication.factor - 1` for the followers.

**Disk.** A topic's disk is ingest times retention times the replication
factor. Keep 20 percent free: the log cleaner and segment rolls need room,
and a disk that fills stops every partition on it, not one.

**Partitions.** Each partition costs a segment file set, a place in the
per-second gauge sampler, and a series in every per-partition family. A
broker that hosts more than a few thousand replicas pays for it in metadata
image size and in scrape size. `krabka_broker_partitions_total` is the count
to watch; the per-partition families multiply by it.

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
per-topic families one per topic. A broker with ten thousand replicas serves
a body of some tens of megabytes; scrape it at 30s or 60s rather than 15s,
and drop the per-partition families at the scrape with `metric_relabel_configs`
when a dashboard does not need them.
