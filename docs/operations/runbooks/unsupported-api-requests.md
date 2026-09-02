# Unsupported API requests

**Alert:** `KrabkaUnsupportedApiRequests`, `sum by (instance, api_key) (rate(krabka_broker_unsupported_api_requests_total[5m])) > 0` for 5m.

## What it means

A client sends requests at an `api_key` and version pair the broker does not
register. The dispatcher rejects each one with `UNSUPPORTED_VERSION` and the
client normally retries. A well-behaved client negotiates with `ApiVersions`
first and never lands here. The usual causes are a client that skips
negotiation, a client pinned to a version the broker does not speak, or a
broker fleet in the middle of a rolling upgrade.

## Confirm

The `api_key` label names the API. `krabka_broker_client_software_versions_total`
lists the client libraries that connected. The broker log records the peer
address and the version it asked for.

## Diagnose

1. Compare the version the client asks for with the broker's advertised
   range: `kafka-broker-api-versions --bootstrap-server <broker>:9092`.
2. If the fleet is mid-upgrade, a client that learned the range from a new
   broker may send the same version to an old one. The alert clears when the
   roll completes.
3. If one client software name is the source, that client has a pinned
   version. Find the owner through the peer address in the log.

## Fix

- Finish the rolling upgrade. See [deploy](../deploy.md#rolling-upgrade).
- Reconfigure or upgrade the client. krabka follows Kafka's version ranges,
  so a client that works against the current Kafka release works here.
- If the API is one krabka does not implement at that version, check
  [KIP_MATRIX.md](../../KIP_MATRIX.md) and open an issue with the `api_key`
  and version.
