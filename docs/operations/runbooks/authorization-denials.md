# Authorization denials

**Alert:** `KrabkaAuthorizationDenials`,
`rate(krabka_broker_authorization_denied_total[5m]) > 0` for 10m.

## What it means

The authorizer has been answering Deny continuously for ten minutes. The
`operation` and `resource_type` labels name what was asked for, spelled the
way `AclOperation` and `ResourceType` spell it: `Write` on a `Topic`, `Read`
on a `Group`, and so on.

Kafka exports no counter for this. `StandardAuthorizer` writes a
`Principal = ... is Denied Operation = ...` line to the
`kafka.authorizer.logger` log4j logger and operators alert on log volume.
This series replaces that, and the broker keeps it whether or not
`audit.enabled` is set: the counting decorator wraps the configured
authorizer in both cases.

An occasional Deny is normal -- a client probing a topic it has no ACL for
bumps it once. The ten-minute window is what makes the alert mean "something
changed and stayed changed", which is almost always an ACL edit that locked
out a fleet.

## Confirm

Compare the labels against the last ACL change. `kafka-acls --list` against
the resource the labels name says whether the binding a client relies on is
still there.

The traffic series for the affected work stops at the same moment:
`krabka_broker_topic_bytes_in_total` for a producer fleet,
`krabka_broker_consumer_group_lag` climbing for a consumer group.

## Diagnose

1. With audit enabled, `__krabka_audit` carries the `AuthorizationDenied`
   record with the principal, the source address and the resource name. That
   is the fastest route to which client is being denied.
2. With audit disabled, the counter labels are all you have. Narrow with the
   API-key request counters: denials on `Write`/`Topic` beside a drop in
   Produce throughput names the fleet.
3. Read the ACL bindings for the resource. A `DENY` binding beats every
   `ALLOW`, and a prefix binding that stopped matching after a topic rename
   denies just as effectively as a deleted one.
4. If the cluster runs the OPA authorizer, a policy bundle that failed to
   load leaves the previous decision set in place, or fails closed. Check the
   broker log for the bundle load.

## Fix

- ACL edit that went too far: restore the binding with `kafka-acls --add`.
  The change takes effect on the next decision; clients do not need a
  restart.
- Principal changed shape (a renamed service account, a new certificate
  subject): add the binding for the new principal rather than widening the
  old one.
- Deliberate denial: the alert is doing its job. Silence it for the resource
  and tell the client's owner.

## Escalate

A rising denial rate with no ACL change behind it is a client reaching for
resources it never had. Hand the principal and source list from the audit
topic to the security owner.
