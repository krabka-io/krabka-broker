# Scheduled delivery activating late

**Alert:** `KrabkaDeliveryActivationLate`,
`histogram_quantile(0.99, sum by (le, instance) (rate(krabka_broker_delivery_activation_lateness_seconds_bucket[5m]))) > 1`
for 10m.

## What it means

KFC-1 scheduled delivery holds a batch invisible until its activation time,
then makes it visible. `delivery_activation_lateness_seconds` is how far past
that deadline the batch actually became visible. A healthy broker reports
zero.

A rising tail says one of two things, in the metric's own words: the bound the
broker publishes is not honest, or the scheduler is starved of CPU. Both matter
because the bound is a promise to consumers. A partition with
`delivery.mode=scheduled` is telling its readers that a record does not appear
before its time and does appear at it; lateness is the second half of that
breaking while the first half still holds.

Nothing errors. The records arrive, late, and only this series says so.

## Confirm

```
histogram_quantile(0.99, sum by (le) (rate(krabka_broker_delivery_activation_lateness_seconds_bucket[5m])))
rate(krabka_broker_delivery_scheduler_wakeups_total[5m])
krabka_broker_delivery_pending_records
krabka_broker_delivery_clock_uncertainty_seconds
```

The wakeup counter is what separates the two causes: a scheduler that never
ran and one that ran late. A flat wakeup rate with a growing
`delivery_pending_records` is the first; a healthy wakeup rate with a high
lateness quantile is the second.

## Diagnose

1. **Scheduler not running.** Look for the broker being CPU-starved as a
   whole: request latency up across every API points at the node rather than
   at delivery — see [broker-saturation](broker-saturation.md).
2. **Scheduler running late.** The wake is timer-driven, so a late wake is a
   loaded runtime. `krabka_broker_in_flight_requests` and the phase histograms
   say whether handlers are occupying the executor.
3. **Clock.** `delivery_clock_uncertainty_seconds` is the KFC-8 bound the
   broker declares and adds before a batch activates. If a measured clock
   error on the node exceeds it, the bound is not honest and the lateness is
   the truth catching up. Compare with the node's own NTP or PTP offset.
4. **Leadership churn.** A partition whose leader moved re-arms its schedule
   on the new leader. A burst of lateness that coincides with
   `krabka_broker_isr_shrinks_total` or a preferred-leader election is that,
   and it does not persist.

## Fix

- For CPU starvation, take load off the broker: move partitions, raise the
  quota that is not throttling, or add a broker. The scheduler competes with
  request handlers for the same runtime.
- For clock error, fix time sync on the node first, then raise the declared
  uncertainty bound only if the true error cannot be brought inside it. A
  bound that is honest and wide is safer than one that is tight and wrong.
- For a topic whose deadlines are simply tighter than the broker can serve,
  `delivery.max.delay.ms` bounds how far ahead a batch may be scheduled but
  does not make activation faster. The fix is capacity.

## Escalate

Lateness that persists on an idle broker with a healthy wakeup rate and a
clock inside its bound is a scheduler bug. Capture the quantile, the wakeup
rate, the pending-record gauge and a CPU profile from `/debug/pprof/profile`,
and open an issue: the guarantee the topic sells is what is failing.
