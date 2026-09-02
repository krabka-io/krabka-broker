# Failed Produce requests

**Alert:** `KrabkaFailedProduceRequests`, `sum by (instance, topic) (rate(krabka_broker_topic_failed_produce_requests_total[5m])) > 0` for 5m.

## What it means

Produce partition responses on the topic carry a non-zero error code. The
counter moves once per failed partition, so one request with two failed
partitions adds two. Authorization denials, unknown-topic responses, quota
throttles and `NOT_ENOUGH_REPLICAS` all count here.

## Confirm

`rate(krabka_broker_topic_failed_produce_requests_total[5m]) /
rate(krabka_broker_topic_produce_requests_total[5m])` is the per-topic error
ratio. The producer's own log names the error code.

## Diagnose

Work through the causes from the most common:

1. `NOT_ENOUGH_REPLICAS`: `krabka_broker_under_min_isr_partition_count > 0`
   on the leader. Follow
   [under min ISR partitions](under-min-isr-partitions.md).
2. `TOPIC_AUTHORIZATION_FAILED`: the principal has no `Write` ACL on the
   topic. `kafka-acls --list --topic <topic>` shows what exists. The audit
   topic records each denial with the principal.
3. `UNKNOWN_TOPIC_OR_PARTITION`: the topic was deleted, or the producer has a
   stale metadata view during a reassignment.
4. `POLICY_VIOLATION` with a `[freeze]` section: a write freeze covers the
   topic. `krabka_broker_topic_freeze_rejections_total{topic}` climbs at the
   same rate. `krabka-guard freeze list` shows the entry.
5. `INVALID_RECORD` with a `[schema_registry]` section:
   `krabka_broker_schema_validation_rejections_total{topic, reason}` names the
   reason.
6. `NOT_LEADER_OR_FOLLOWER`: leadership moved and the producer has not
   refreshed metadata. This clears on its own after the producer's
   `metadata.max.age.ms`.

## Fix

Each cause above has its own fix: restore the ISR, grant the ACL, thaw the
freeze, or fix the producer's serializer. A `NOT_LEADER_OR_FOLLOWER` burst
during a rolling upgrade is expected and needs no action.
