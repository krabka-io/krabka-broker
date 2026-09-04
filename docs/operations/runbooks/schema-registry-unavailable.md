# Schema registry unreachable

**Alert:** `KrabkaSchemaRegistryUnavailable`,
`sum by (instance, topic) (rate(krabka_broker_schema_validation_rejections_total{reason="registry_unavailable"}[5m])) > 0`
for 2m.

## What it means

A topic has KFC-7 schema validation on, the broker could not get an answer
from the schema registry, and it refused the produce.

`[schema_registry].fail_open` decides that. It defaults to `false`: the
broker rejects a record it could not validate. The alternative, `true`,
admits whatever it is sent for the length of the outage. Neither setting is
free, and the choice is the whole content of this alert:

- **`fail_open = false` (default).** An unreachable registry is a produce
  outage on every validated topic. Availability follows an external HTTP
  service that is not part of the cluster.
- **`fail_open = true`.** Produces keep succeeding and the topic accepts
  records nobody checked. Consumers that trust the contract read them later,
  and there is no second gate that catches them.

Turning validation on makes the registry a hard dependency of the produce
path. That is the decision this page exists to make visible; the rejection
rate is only how you find out it was taken.

An unknown schema id or a body that does not match its schema is rejected
under either setting. Those carry other `reason` values (`unknown_id`,
`wrong_subject`, `body_mismatch`, `unframed`) and are the validator working,
not failing.

## Confirm

```
sum by (topic, reason) (rate(krabka_broker_schema_validation_rejections_total[5m]))
rate(krabka_broker_schema_validation_cache_misses_total[5m])
rate(krabka_broker_schema_validation_cache_hits_total[5m])
```

A rejection rate concentrated on `registry_unavailable` with a miss rate that
matches it is an outage. A `registry_unavailable` rate that is small beside a
high hit rate is a partial outage the cache is absorbing: the broker caches an
unavailable result for two seconds only, deliberately, so that recovery is
immediate rather than delayed by the full cache TTL.

Producers see `INVALID_RECORD` with the validator's reason as the message.

## Diagnose

1. Reach the registry from the broker's own network namespace:
   `curl -sv <schema_registry.url>/subjects`. DNS, a network policy and an
   expired TLS certificate all fail differently there.
2. `schema_registry_http_timeout` bounds one request. A registry that is up
   but slow times out and lands in the same counter as one that is down;
   the broker log carries the underlying error.
3. Check whether the registry is a single instance. One process behind the
   produce path of a whole cluster is the usual root cause, and no broker
   setting fixes it.
4. Confirm which topics are affected: only topics with
   `schema.validation.key` or `schema.validation.value` set run the check at
   all.

## Fix

- Restore the registry. The broker needs no restart and no cache flush: the
  unavailable result expires in two seconds and validated produces resume.
- Give the registry more than one instance behind a load balancer before the
  next outage. This is the durable fix, and it is not a broker change.
- Turning `fail_open = true` mid-incident buys availability by accepting
  unvalidated records for the length of the outage. Take that decision
  deliberately, record when it was set, and set it back — it is a security
  control, and its cost is records no consumer's contract covers.
- To take the dependency off one topic rather than the whole broker, unset
  `schema.validation.key` and `schema.validation.value` on that topic.

## Escalate

A registry outage that outlasts the producers' retry budget is a produce
outage with an external owner. Escalate to whoever runs the registry with the
per-topic rejection rate and the `curl` output, and decide the `fail_open`
question with the data owner rather than alone.
