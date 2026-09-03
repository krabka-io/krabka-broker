# Authentication failures

**Alert:** `KrabkaAuthenticationFailures`,
`rate(krabka_broker_failed_authentication_total[5m]) > 1` for 5m.

## What it means

`SaslAuthenticate` frames are coming back with a non-zero error code faster
than one a second, sustained over five minutes. The `mechanism` label names
the wire mechanism the connection negotiated: `PLAIN`, `SCRAM-SHA-256`,
`SCRAM-SHA-512`, `OAUTHBEARER` or `GSSAPI`. Frames rejected with
`ILLEGAL_SASL_STATE` because no `SaslHandshake` ran first land under
`Unknown`.

A rotated credential that one deployment missed and a credential-stuffing
burst look the same in this series. What tells them apart is how many
distinct clients are failing and whether any of them ever succeeded.

## Confirm

`rate(krabka_broker_successful_authentication_total[5m])` by `mechanism`
gives the ratio. A fleet with a stale secret keeps a healthy success rate on
every other mechanism while one goes to zero. An attack pushes failures up
with the success rate flat.

`krabka_broker_active_connections` climbing alongside the failures, with
`krabka_broker_connection_closes_total{reason="decode_error"}` moving too, is
a client that reconnects on every reject.

## Diagnose

1. Split the failures by `mechanism`. One mechanism failing alone points at
   that credential store: SCRAM users in the metadata log, the OAuth issuer
   for `OAUTHBEARER`, the KDC and the broker keytab for `GSSAPI`.
2. Read the broker log around the first failure. It names the principal the
   frame claimed and the reason the mechanism refused it.
3. With audit enabled, `__krabka_audit` carries one `class_uid` 3002
   `Authentication` row per completed login, with `status_id` 2 for a failure,
   the mechanism in `auth_protocol`, the reason in `status_detail`, and the
   peer in `src_endpoint`. A single source is a broken deployment; many
   sources on many principals is not.
4. For `GSSAPI`, check the broker's ticket: an expired keytab entry or clock
   skew past the Kerberos tolerance fails every frame at once.

## Fix

- Stale credential: roll the client's secret to the current one, or add the
  new SCRAM credential with `kafka-configs` before retiring the old.
- Expired OAuth signing key: refresh the JWKS the broker fetches and confirm
  the issuer and audience the listener expects.
- Kerberos: renew the keytab, restart the affected broker listener, fix clock
  skew.
- A hostile source: block it at the load balancer or the network policy. The
  broker has no per-IP lockout.

## Escalate

Failures from many source addresses against many principals is a security
incident, not an operations one. Take the source list from the audit topic
and hand it to the security owner.
