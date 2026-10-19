# Offline partitions

**Alert:** `KrabkaOfflinePartitions`,
`krabka_broker_offline_partitions_count - krabka_broker_offline_audit_partitions > 0`
for 1m.

## What it means

A partition has no live leader. Its last leader is gone and no ISR member is
available to take over. Every produce and every fetch to the partition fails
until an ISR member returns or an unclean election runs. This is an outage for
that partition.

## Confirm

```
kafka-topics --bootstrap-server <broker>:9092 --describe --unavailable-partitions
```

The output shows `Leader: none` for each affected partition, with the replica
list and the last known ISR.

The alert does not count partitions of the audit topic, which is
`__krabka_audit` unless `[audit] topic` names another one. Each broker holds
one audit partition with no other replica, so that partition has no leader
exactly while its broker is down, a planned stop included.
`krabka_broker_offline_audit_partitions` counts those partitions, and the
command above still lists them. Ignore them here and follow
[broker down](broker-down.md) for their brokers. Do not run an unclean
election for an audit partition: it has no other replica to elect. The
alert fires only for a partition outside the audit topic.

## Diagnose

1. Find the brokers in the last ISR. Check whether they are running and
   whether the controller sees them. `krabka_broker_active_controller` says
   which broker is the controller; its log records each broker's fencing.
2. If an ISR member is up but fenced, its heartbeats are not reaching the
   controller. Check the controller listener, TLS material and DNS from that
   broker to the controller.
3. If every ISR member is lost for good, the partition needs a leader from
   outside the ISR. That is a data-losing choice.

## Fix

- Restart the ISR member. The controller elects it and the partition comes
  back with no data loss. This is the only fix that loses nothing.
- If no ISR member can return, elect an unclean leader:

  ```
  kafka-leader-election --bootstrap-server <broker>:9092 --election-type unclean \
      --topic <topic> --partition <n>
  ```

  With `[break_glass]` configured, the election needs an approved proposal.
  Use `krabka-guard` to create and approve one, then run the election. Records
  that only the lost replicas held are gone. Tell the topic's owner before you
  run it.

## Escalate

An unclean election is recorded in
`krabka_broker_unclean_leader_elections_total` and in the audit topic. Follow
[unclean leader election](unclean-leader-election.md) after the partition is
back.
