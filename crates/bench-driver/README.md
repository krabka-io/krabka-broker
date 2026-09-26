# krabka-bench-driver

[![CI](https://github.com/krabka-io/krabka-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/krabka-io/krabka-broker/actions/workflows/ci.yml)

Load driver and report aggregator for the Krabka vs Strimzi benchmark harness.

This crate is part of [Krabka](https://github.com/krabka-io/krabka-broker), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

This crate is workspace-only; use its path dependency from this repository.

## Usage example

Run one benchmark scenario against a reachable Kafka-compatible cluster and write the JSON report.

Every size, duration, and rate in the scenario carries its unit: `512B`, `5ms`, `20000/s`. The driver rejects a bare number instead of a guess. So a scenario cannot mean milliseconds where it meant seconds:

```bash
cat > /tmp/smoke.yaml <<'YAML'
name: smoke-produce-consume
mode_tag: ci
msg_size: 512B
partitions: 6
producers: 1
consumers: 1
mode:
  kind: saturate
acks: leader
linger: 5ms
batch_size: 16KiB
warmup: 5s
duration: 30s
YAML

krabka-bench-driver \
  --scenario /tmp/smoke.yaml \
  --bootstrap localhost:9092 \
  --stack krabka \
  --topic bench-topic \
  --broker-count 1 \
  --out /tmp/krabka-run.json
```

Paced runs replace `mode: {kind: saturate}` with an explicit event rate:

```yaml
mode:
  kind: fixed_rate
  rate: 20000/s
```

The `RunOutput` JSON that the driver writes encodes its measurements as exact integers instead. Latencies are in nanoseconds and sizes are in bytes. The report aggregator can then compare and plot them without rounding.

## Documentation

Generate the API documentation with `cargo doc -p krabka-bench-driver --open`.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
