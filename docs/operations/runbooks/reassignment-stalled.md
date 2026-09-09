# Reassignment stalled

**Condition:** `krabka_broker_reassigning_partitions` stays above zero while
both KIP-73 throttled-byte rates are zero.

## Confirm

```sh
kafka-reassign-partitions --bootstrap-server "$BOOTSTRAP" --list
kafka-reassign-partitions --bootstrap-server "$BOOTSTRAP" --verify \
  --reassignment-json-file proposed.json --preserve-throttles
```

## Diagnose

1. Describe the topic and identify the replica absent from the ISR.
2. Confirm that target broker is registered and unfenced with
   `kafka-cluster list-endpoints`.
3. Compare `krabka_broker_replication_bytes_out_total` on the leader with
   `krabka_broker_replication_bytes_in_total` on the target. Zero on both is a
   listener, authentication, or routing failure; incoming bytes without ISR
   growth means the target disk cannot keep up or cannot write.
4. Check `krabka_broker_request_errors_total`, authentication failures, free
   disk, and the target broker log before changing the throttle.

## Fix

Restore the target broker or its inter-broker route. If transfer is merely
slower than ingest, resubmit the same plan with `--additional --throttle` at a
rate the target can sustain. Run `--verify` without `--preserve-throttles` once
the move completes so the temporary broker and topic configs are removed.

Do not cancel just to clear the gauge: cancellation is a break-glass action
when the two-person policy is enabled, and it leaves the old placement in use.
