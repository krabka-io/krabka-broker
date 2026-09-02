# Audit write failures and dropped records

**Alerts:** `KrabkaAuditWriteFailures`, `rate(krabka_broker_audit_write_failures_total[5m]) > 0`;
`KrabkaAuditRecordsDropped`, `rate(krabka_broker_audit_records_dropped_total[5m]) > 0`.

## What it means

The audit subsystem could not write a record to the audit topic. With a
`[audit.spool]` section, the record diverts to the durable spool and replays
later; `krabka_broker_audit_records_spooled_total` climbs. Without a spool, or
when the spool or the in-memory channel is full, the record is dropped and
`krabka_broker_audit_records_dropped_total` climbs. A dropped record is a gap
in the audit trail.

What the broker does with the request that produced the record depends on
`failure_mode` in `[audit]`. A fail-closed mode rejects the request; a
fail-open mode serves it and counts the loss.

## Confirm

`krabka_broker_audit_spool_depth` and `krabka_broker_audit_spool_bytes` show
the spool filling. `krabka_broker_audit_records_replayed_total` shows it
draining once the topic accepts writes again.

## Diagnose

1. The audit topic is a normal topic. Check its ISR:
   `kafka-topics --describe --topic __krabka_audit`. A partition below
   `min.insync.replicas` rejects the audit produce like any other.
2. Check `max_bytes` in `[audit.spool]` against the spool bytes. A spool at
   its cap drops.
3. Check `audit_event_queue_capacity` in `[runtime]`. A queue that fills
   faster than the writer drains it drops records before they reach the
   spool. The fix is a larger queue or a faster topic.

## Fix

- Restore the audit topic's ISR first. The spool replays on its own.
- Raise `max_bytes` on the spool when the topic outage is expected to last.
- Do not disable the audit subsystem to stop the alert. In a fail-closed
  mode that also stops the requests the audit records describe.

## Escalate

Every dropped record is a compliance finding. Record the time window and the
count from `krabka_broker_audit_records_dropped_total` for the audit owner.
