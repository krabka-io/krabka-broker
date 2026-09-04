# Slow broker

**Alerts:** `KrabkaRequestLatencyHigh`
(`histogram_quantile(0.99, sum by (le, instance, api_key) (rate(krabka_broker_request_duration_seconds_bucket[5m]))) > 1`
for 10m), `KrabkaQuotaThrottleHigh`
(`histogram_quantile(0.99, sum by (le, instance, quota_type) (rate(krabka_broker_quota_throttle_duration_seconds_bucket[5m]))) > 1`
for 15m), and `KrabkaInFlightRequestsClimbing`
(`min_over_time(krabka_broker_in_flight_requests[10m]) > 64` for 10m).

None of the three thresholds comes from a metric comment. They are starting
points; tune them against the fleet's own normal.

## What it means

The broker is up and every partition is in-ISR, and clients are still timing
out. This is the page that the availability rules cannot produce: nothing is
down, so nothing else fires.

The three rules are three faces of the same condition.

- **Latency.** `request_duration_seconds` is the full handler round trip,
  decode to encode. A p99 over a second on `Produce` or `Fetch` is already
  past what a default `request.timeout.ms` of 30s hides only because of
  retries.
- **Throttle.** `quota_throttle_duration_seconds` observes only requests the
  broker actually slept on, so it says how hard a quota bites, not how many
  clients meet it. A high p99 here is the broker deliberately slowing clients
  down, which is a capacity or quota decision rather than a fault.
- **In flight.** `in_flight_requests` is what the broker is handling right
  now. The metric's comment names a sustained climb as a handler stall or a
  wedged controller. `min_over_time` is what separates a busy broker, whose
  count returns to a low floor between bursts, from one whose floor has moved.

## Confirm

Split the latency by phase before anything else. The three phase families are
disjoint and do not add up to the total:

```
histogram_quantile(0.99, sum by (le, api_key) (rate(krabka_broker_request_duration_seconds_bucket[5m])))
histogram_quantile(0.99, sum by (le, api_key) (rate(krabka_broker_request_local_duration_seconds_bucket[5m])))
histogram_quantile(0.99, sum by (le, api_key) (rate(krabka_broker_request_remote_duration_seconds_bucket[5m])))
histogram_quantile(0.99, sum by (le, api_key) (rate(krabka_broker_request_throttle_duration_seconds_bucket[5m])))
```

The remainder — total minus the three — is the work no phase names: decode,
authorization, record validation and response encode. Compare the `_sum`
streams to size it; do not subtract `_bucket` streams, because each family
observes its own durations.

`krabka_broker_queued_requests` and `krabka_broker_queued_request_bytes`
beside the in-flight count say whether requests are waiting to start or
waiting to finish.

## Diagnose

1. **Local phase high.** The broker's own log is the cost: fsync latency, a
   slow device, or a partition count per disk beyond what the volume serves.
   Read `krabka_broker_partition_cpu_micros_total` for the top partitions and
   the node's disk-service-time metrics together.
2. **Remote phase high.** Not the disk. This is the `acks=all` high-watermark
   gate, the Fetch long poll, or an object-store round trip for a KIP-405
   tiered or diskless cold read. Check `krabka_broker_replica_lag_max_records`
   for the first, and the remote-tier fetch series and
   `krabka_broker_remote_log_reader_task_queue_size` for the third — see
   [remote-copy-lag](remote-copy-lag.md).
3. **Throttle phase high.** Quotas, and the `quota_type` label says which. A
   client that arrived with a new workload, or a quota that was tightened, are
   the two ordinary causes.
4. **Remainder high.** Handler CPU. KFC-7 schema validation is the most
   expensive optional step on the produce path and it can block on an external
   service — see [schema-registry-unavailable](schema-registry-unavailable.md).
   Record validation and down-conversion for old clients
   (`krabka_broker_fetch_message_conversions_total`) are the others.
5. **In flight climbing with a flat request rate** is a stall rather than
   load. `krabka_broker_metadata_lag_records` and
   `krabka_broker_active_controller` say whether handlers are waiting on a
   controller that is not committing.

## Fix

- Shed load first where you can: raise the quota that is throttling, or move
  the noisiest partitions with `kafka-reassign-partitions`.
- For a local-phase disk, move partitions off the volume; the broker does not
  get faster by being restarted.
- For a full queue, `queued_max_requests` and `queued_max_request_bytes` bound
  what the broker accepts before it stops reading sockets. Raising them buys
  buffering, not throughput, and makes the latency worse if the bottleneck is
  downstream.
- For an old-client down-conversion cost, upgrade the clients: a `Fetch v<4`
  consumer makes the broker rewrite every batch it serves.

## Escalate

Latency that is high on every API at once, with a low request rate and a
climbing in-flight floor, is a wedged broker rather than a loaded one.
Capture a CPU profile from `/debug/pprof/profile` on the metrics port and the
phase quantiles above before restarting it — a restart destroys the evidence
and the same condition returns.
