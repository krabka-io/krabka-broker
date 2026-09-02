# Idle connection closes

**Alert:** `KrabkaIdleConnectionCloses`, `rate(krabka_broker_connection_closes_total{reason="idle"}[5m]) > 1` for 10m.

The metric's comment names this as the signal for a peer that connects and
then sends nothing. It names no threshold. One close per second for ten
minutes is a starting point; tune it to the fleet.

## What it means

The broker closed more than one connection per second because the connection
went `connections_max_idle` without a complete frame. A TLS handshake that
the peer opened and never drove counts too. The `max_connections` and
`max_connections_per_ip` caps do not surface this on their own, because the
idle connections are closed before they reach the cap.

## Confirm

`krabka_broker_active_connections` shows the live count beside the close
rate. A close rate that matches a rise in the accept rate is a peer in a
connect loop. The broker log records the peer address of each idle close at
`RUST_LOG=krabka_broker=debug`.

## Diagnose

1. A load balancer or a Kubernetes TCP probe that opens the port and closes
   it, or opens it and holds it, is the most common cause. The probe address
   in the log confirms it.
2. A client that opens a TLS connection with the wrong CA never completes the
   handshake. The broker sees an idle socket and closes it after the timeout.
3. A client pool that opens more connections than it uses parks the extra
   ones, and they idle out on the broker's timer.

## Fix

- Move the probe to the metrics port, which serves `/metrics` and answers a
  plain HTTP GET, or lower the probe frequency.
- Fix the client's trust store when the cause is a failed TLS handshake.
- Raise `connections_max_idle` per listener when the idle connections are
  legitimate. The cost is one file descriptor per idle connection.
