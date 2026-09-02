# Topic-backed RLMM bootstrap stuck

**Alert:** `KrabkaRlmmBootstrapStuck`, `min_over_time(krabka_broker_tiered_storage_rlmm_topic_backed[5m]) == 0 and rate(krabka_broker_tiered_storage_rlmm_bootstrap_attempts_total[5m]) > 0`.

## What it means

The broker has a `[remote_storage.kafka_metadata]` section and keeps retrying
the bootstrap of the topic-backed `RemoteLogMetadataManager`. Until the swap
succeeds, the broker answers remote-log metadata queries from the fail-closed
placeholder: tiered reads fail and the copy task does not upload segments.
Local reads and writes are not affected.

The rule pairs the gauge with the attempt counter so a cluster that never
asked for the topic-backed manager does not alert.

## Confirm

`krabka_broker_tiered_storage_rlmm_bootstrap_attempts_total` climbs while
`krabka_broker_tiered_storage_rlmm_topic_backed` stays at zero. The broker
log records each attempt with the error.

## Diagnose

1. The bootstrap creates and then consumes `__remote_log_metadata`. Check
   whether the topic exists: `kafka-topics --describe --topic
   __remote_log_metadata`. A creation that waits on the controller shows the
   `topic_create_timeout` in the log.
2. Check the `bootstrap` address in `[remote_storage.kafka_metadata]`. The
   broker dials it as a client, so it needs a listener that the broker's own
   credentials can reach.
3. Check `krabka_broker_under_min_isr_partition_count`. The metadata topic
   is created with `replication` replicas, and a cluster with fewer live
   brokers cannot create it.

## Fix

- Fix the bootstrap address or the credentials and let the retry succeed. No
  restart is needed.
- Create the topic by hand with the configured partition and replication
  counts when the automatic creation cannot complete.
- Set `in_memory = true` only for a test cluster. It stores metadata in
  memory and loses it on restart.
