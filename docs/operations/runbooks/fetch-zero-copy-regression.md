# Fetch zero-copy path lost

**Alert:** `KrabkaFetchZeroCopyLost`,
`sum by (instance) (rate(krabka_broker_fetch_response_drain_total{path="sendfile"}[5m])) == 0`
and `sum by (instance) (rate(krabka_broker_fetch_response_drain_total[5m])) > 0`
for 15m.

## What it means

The broker is serving fetches and none of them leaves through the kernel's
`sendfile`. Every response is being copied through userspace instead.

`fetch_response_drain_total` counts one increment per drained response,
labelled with the strongest path any of its records regions took: `sendfile`
when the kernel moved a region with no userspace copy, `pread` when a
file-backed region had to be copied through a buffer anyway, and `vectored`
when the response carried no file-backed region at all. It is the only series
that says whether the zero-copy path is being used, which is why its own doc
comment names this query as the regression detector.

Nothing breaks. The broker serves the same bytes, spends more CPU and more
memory bandwidth doing it, and the cost shows up as latency under load rather
than as an error.

**This rule needs scoping.** A response is only eligible for `sendfile` when
it is plaintext, or TLS with working kernel TLS offload. A TLS listener
without kTLS encrypts in userspace and correctly reports no `sendfile` for
ever. Apply the rule to the brokers whose consumers read plaintext, or whose
`krabka_broker_ktls_enabled` is 1, and drop it elsewhere.

## Confirm

```
sum by (instance, path) (rate(krabka_broker_fetch_response_drain_total[5m]))
krabka_broker_ktls_enabled
```

All three `path` series exist from startup at zero, so a `path` that is
missing means the scrape is wrong, not that the path is unused. On Windows,
`sendfile` and `pread` stay at zero for the life of the process: the platform
has no safe file-to-socket call and every fetch is `vectored`.

Which path replaced `sendfile` is the first fork in the diagnosis:

- **`pread` carries the traffic.** The records are file-backed and the broker
  chose to copy them.
- **`vectored` carries the traffic.** The responses hold no file-backed region
  at all — the reads are being served from memory or from a non-file source.

## Diagnose

1. **`pread`, on a TLS listener.** Read `krabka_broker_ktls_enabled`. Zero
   means the startup probe found no working kTLS: the kernel module is absent,
   the cipher is one it does not offload, or the build has no kTLS support.
   That is expected behavior, not a regression — scope the rule off this
   broker.
2. **`pread`, on a plaintext listener.** The response was under
   `sendfile_min`, the size floor below which the copy is cheaper than the
   syscall. A workload that shifted to many small fetches moves every response
   under it. Compare the fetch rate with the bytes-out rate to see the average
   response size.
3. **`vectored` everywhere.** The records are not coming from a segment file:
   a diskless topic serving out of the WAL hot tail, or a tiered read serving
   from the object store, both drain this way. Check whether the consumers
   moved to a diskless or fully tiered topic.
4. **Neither, and the cluster is plaintext and file-backed.** This is the
   regression the metric exists to catch. It is a code change on the drain
   path, so bisect against the release where the ratio changed.

## Fix

- Lower `sendfile_min` if small responses are legitimate and the copy cost
  matters more than the syscall cost.
- For a kTLS-capable kernel that probed false, check the listener's cipher
  suite: only the offloadable ones probe true.
- For a genuine regression, this is a broker bug. Capture the three path rates
  and the listener protocol and open an issue; do not paper over it by
  scoping the rule away.

## Escalate

A regression that reaches the whole fleet at once after a rollout is a release
problem. Roll back and keep the panel: the ratio is what tells you the
rollback worked.
