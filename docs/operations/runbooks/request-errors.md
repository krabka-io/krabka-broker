# Request handler errors

**Alert:** `KrabkaRequestErrors`, `sum by (instance, api_key) (rate(krabka_broker_request_errors_total[5m])) > 0` for 5m.

## What it means

A request handler returned an error instead of a response, and the dispatcher
closed the connection. This is a broker-side fault, not a Kafka error code: an
error code goes back to the client in a normal response and lands in
`krabka_broker_topic_failed_produce_requests_total` or the Fetch equivalent.
The client sees a dropped connection and reconnects.

## Confirm

The `api_key` label names the API. The broker log carries one line per
handler error with the error text and the peer address.

## Diagnose

1. Read the log line. A decode error on a request the broker registered is a
   client that sends malformed frames; a client library bug or a non-Kafka
   peer on the port.
2. An I/O error is the peer going away mid-request. A low steady rate from
   short-lived clients is normal for `peer_closed`; check
   `krabka_broker_connection_closes_total{reason="peer_closed"}` beside it.
3. A panic or an internal error in the handler is a broker bug. Its log line
   carries a backtrace when `RUST_BACKTRACE=1` is set.

## Fix

- Malformed frames: find the peer through the address in the log and stop
  or fix the client.
- A broker bug: collect the log lines, the `api_key`, the request version and
  a sample of the client's traffic, and open an issue. Restarting the broker
  does not remove the cause.

## Escalate

A rate that climbs with `krabka_broker_in_flight_requests` is a handler that
hangs and then fails. Capture a CPU profile from `/debug/pprof/profile` on
the metrics port before you restart.
