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

The client side of a contract run, the paced producers, the live consumer groups and the read-back, is the library module `soak::client`. The `soak_contract` suite uses it, and so does `krabka-soak-drill`, the executor of a disaster-recovery contract such as `bench/soak/dr-secured.yaml`. The `disaster-recovery` qualification gate runs it against a secured source cluster and the cluster restored from its capture, and `krabka-soak-check judge` grades the result. It reads the password from `KRABKA_DRILL_PASSWORD`, so the password is never on a command line:

```bash
krabka-soak-drill objectives --contract bench/soak/dr-secured.yaml
krabka-soak-drill load --contract bench/soak/dr-secured.yaml \
  --bootstrap broker-1:9094 --tls-ca ca.pem --tls-server-name broker-1 \
  --username soak --out drill/load.json
krabka-soak-drill probe --bootstrap restored-1:9094 --tls-ca ca.pem \
  --tls-server-name restored-1 --username soak --topic tiered-idempotent
krabka-soak-drill readback --contract bench/soak/dr-secured.yaml --load drill/load.json \
  --bootstrap restored-1:9094 --tls-ca ca.pem --tls-server-name restored-1 --username soak \
  --capture-started-ms 1792022400000 --disaster-ms 1792022430000 \
  --serving-ms 1792022500000 --out drill/evidence.json
krabka-soak-check judge --contract bench/soak/dr-secured.yaml --evidence drill/evidence.json
```

`load` runs until `SIGTERM`, which the drill sends when it loses the source cluster, and then abandons every send in flight. Its commands live in the library module `soak::drill`. Without Docker, the `soak_drill` suite of `krabka-broker` runs them over SCRAM and TLS against an in-process broker, and `soak_load` runs the soak's own load and read-back there. The [qualification guide](../../docs/qualification.md#disaster-recovery-declaration) declares the drill's backup boundary and its RPO and RTO.

## Documentation

Generate the API documentation with `cargo doc -p krabka-bench-driver --open`.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
