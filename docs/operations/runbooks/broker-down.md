# Broker down

**Alert:** `KrabkaBrokerDown`, `up{job="krabka-broker"} == 0` for 2m.

## What it means

Prometheus cannot scrape the broker's `/metrics` endpoint on port 9404. The
process is stopped, the host is gone, or the network path from Prometheus to
the broker is broken.

This rule matters because every other rule in
[`alert-rules.yaml`](../alert-rules.yaml) is per-instance. A broker whose
process died exports no series, so its `krabka_broker_offline_partitions_count`
and `krabka_broker_under_replicated_partitions` rules stop firing rather than
fire. `up` is the Prometheus-side series that stays.

## Confirm

```
kafka-broker-api-versions --bootstrap-server <broker>:9092
```

A connection refused confirms the broker is not serving clients. Under
Kubernetes, `kubectl get pod` and `kubectl describe pod` name the restart
count and the last termination reason.

`krabka_broker_active_brokers` and `krabka_broker_fenced_brokers` on a broker
that is still up say whether the controller has fenced the missing node yet.
The controller fences it after `heartbeat_timeout`.

## Diagnose

1. Read the broker's own log for the last lines before it stopped. A clean
   `controlled shutdown complete` is a planned stop. An abrupt end is a crash
   or an external kill.
2. Check the exit reason. An OOM kill shows as exit 137 under Kubernetes and
   in `dmesg` on a host.
3. Check disk. A full log directory stops the broker. `df` on the log dir,
   and `krabka_broker_partition_disk_bytes` on the last scrape before it
   stopped.
4. If the process is running but the scrape fails, the broker is up and only
   the metrics listener is unreachable. Check `--metrics-listen-addr` and the
   path from Prometheus. In that case the cluster is healthy and the alert is
   a monitoring fault.
5. Check the effect on the cluster. `krabka_broker_under_replicated_partitions`
   and `krabka_broker_offline_partitions_count` on the surviving brokers say
   whether the loss cost availability. A broker that was a controller voter
   also shows up in [no active controller](no-active-controller.md).

## Fix

- Restart the broker. It recovers its log directories, rejoins the quorum and
  catches up on metadata. `/readyz` answers 200 when the catch-up is inside
  `--readiness-max-metadata-lag`.
- Free disk before the restart when the disk was the cause. A broker that
  boots into a full directory stops again.
- Replace the node when the host is gone for good. A replacement formats a new
  log directory. See [Format the log directory](../deploy.md#format-the-log-directory),
  and follow [metadata quorum loss](metadata-quorum-loss.md) when the lost
  node was a controller voter.

## Escalate

A broker that restarts in a loop with no message in its log is a bug. Capture
the log at `RUST_LOG=krabka_broker=debug` across one boot and open an issue.
