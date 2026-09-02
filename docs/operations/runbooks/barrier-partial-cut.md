# Barrier partial cut

**Alert:** `KrabkaBarrierPartialCut`, `rate(krabka_broker_barrier_epochs_published_partial_total[5m]) > 0`.

## What it means

The barrier coordinator published a cut for an epoch that names at least one
partition that got no marker. The coordinator consumed the epoch either way,
so the group's consumers see a cut that is not complete. A downstream job that
waits for a complete cut across the group's partitions cannot use this epoch.

## Confirm

The `group` label names the barrier group.
`krabka_broker_barrier_injection_duration_seconds` for the group shows how
close each injection runs to `barrier_injection_timeout`, which is the default
30s. `krabka_broker_barrier_latest_epoch` shows the epoch that was published.

## Diagnose

1. A partition that got no marker is one whose leader did not append it in
   time. Check `krabka_broker_under_replicated_partitions` and
   `krabka_broker_offline_partitions_count` for the group's topics. An offline
   partition cannot take a marker.
2. A leader on another broker costs an inter-broker round trip per marker.
   The p99 of the injection histogram near the timeout is a group that is too
   large for the timeout, not a failure.
3. Check `krabka_broker_barrier_markers_written_total{topic}` on each broker
   that leads a partition of the group. A broker that writes none is one the
   coordinator could not reach.

## Fix

- Restore the partition that got no marker. The next epoch completes.
- Raise `barrier_injection_timeout` when the group is large and the p99
  sits near it.
- Reduce the group with `krabka-barrier` when it spans more partitions than
  the timeout allows.

## Escalate

A group that publishes partial cuts while every partition is healthy and the
p99 is far below the timeout is a coordinator bug. Capture the coordinator's
log for one epoch and open an issue.
