# krabka-throttle

Shared KIP-73 token-bucket rate limiter for the broker's quotas.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

`TokenBucket` is the concurrent runtime around the pure `plan_consume`
arithmetic in [`krabka-verified`](../verified/README.md). It holds the bucket
state behind one lock, a lock-free copy of the rate for the unthrottled fast
path, and an injected monotonic clock.
`ThrottleState` bundles the three buckets the broker meters: leader-out and
follower-in replica traffic (KIP-73), and intra-broker log directory moves
(KIP-113).

## Features

- Byte, event, and plain token rates, each with an optional burst capacity.
  The bucket counts micro-tokens, so a fractional byte rate such as Kafka's
  `consumer_byte_rate = 0.5` is enforced as configured.
- A `try_consume` that grants at most the request in whole tokens, and a
  `record` that charges the whole request and leaves what the balance could
  not cover as debt, which the refill repays first. A quota caller turns the
  debt into its throttle delay, as Kafka's `Sensor.record` and
  `QuotaUtils.throttleTime` do. A change of rate keeps the balance and the
  debt. Each consume and each rate change is one critical section, so a
  consume never straddles a change and never loses the refill it claimed. An
  unthrottled bucket grants without the lock.
- A caller-injected clock, so a test drives refills with
  `qubit_clock::ManualMonotonicClock` rather than sleeping.
- The refill arithmetic is the Creusot-proved `plan_consume` kernel. A
  Stateright model in `tests/bucket_model.rs` checks the locking under
  concurrent consumers and resets, and shows the lock-free design it replaced
  breaking both properties.

## Usage

```rust
use krabka_throttle::TokenBucket;

let bucket = TokenBucket::new();
bucket.set_token_rate_with_burst(1_000, 5_000);

// Grants up to the request. A rate of zero grants everything.
let granted = bucket.try_consume(250);
assert2::assert!(granted <= 250);
```

## Documentation

- [Verification catalog](../../docs/verification.md), for the `plan_consume`
  proof and the bucket model
- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-throttle/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
