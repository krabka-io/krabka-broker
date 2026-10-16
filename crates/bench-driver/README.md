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

## Sustained-load contract

The crate also ships `krabka-soak-check`, the judge of the sustained-load contract. A contract under `bench/soak/` declares the load, the limits and the faults of a long RF=3 run before it starts. The `soak_contract` suite of `krabka-broker` executes the contract and records evidence, and this binary grades the evidence:

```bash
krabka-soak-check plan --contract bench/soak/rf3-72h.yaml
krabka-soak-check judge --contract bench/soak/rf3-72h.yaml --evidence run/evidence.json
krabka-soak-check self-check --contract bench/soak/self-check.yaml
```

`self-check` without `--evidence` judges a synthetic run with one record removed, and passes only when the judge reports that record lost and the one impossible deadline missed. The [design document](docs/soak-contract-design.md) explains the contract, the fault schedule and the reconciliation rules.

## Documentation

Generate the API documentation with `cargo doc -p krabka-bench-driver --open`.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
