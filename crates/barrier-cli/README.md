# krabka-barrier

Administers krabka barrier groups: define a group, trigger a cut, list cuts,
and verify a cut against the log.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

A barrier group is a named set of topics. The broker's barrier coordinator
injects an epoch-stamped marker into every partition of the set and publishes
the resulting offsets as a cut. A cut is an exact and reproducible point in
every one of those topics at once. This crate is the operator's side of that
feature, which [KFC-4](../../docs/KFCs/KFC-4-cross-topic-snapshots.md)
describes.

Every subcommand speaks one krabka-private API key in the 1010 to 1014 range.
A JVM `AdminClient` cannot send those. The coordinator also publishes every cut
to the `__barrier_state` topic, so any Kafka consumer can read cuts without this
tool.

The crate is a library as well as a binary. Tests call
`krabka_barrier::run_from_args` in process, because a Bazel test sandbox has no
Cargo working tree to build a subprocess from.

## Usage

Every subcommand needs `--bootstrap-server` (`-b`), which also reads the
`KRABKA_BOOTSTRAP_SERVER` environment variable.

Define a group over two topics, cut it every five minutes, and keep the last
200 cuts:

```sh
krabka-barrier -b broker-1:9092 define \
  --group orders-cut \
  --topic orders --topic shipments \
  --interval 5m \
  --retained-cuts 200
```

Trigger a cut now, read the cuts back, and prove that the newest one is in the
log:

```sh
krabka-barrier -b broker-1:9092 trigger --group orders-cut --timeout 30s
krabka-barrier -b broker-1:9092 list --group orders-cut --max-results 1
krabka-barrier -b broker-1:9092 verify --group orders-cut --epoch 42
```

`verify` fetches the batch at every offset the cut names. It checks that each
batch is a barrier control batch that carries this group and this epoch.

## Subcommands

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `define` | `--group <name>`, `--topic <name>` (repeat, at least one), `--interval <time>`, `--retained-cuts <n>` (default `100`) | Create a group, or update one that exists. Omit `--interval` for on-demand cuts only. |
| `delete` | `--group <name>` | Delete a group and its cuts. |
| `describe` | `--group <name>` (repeat, or omit for every group) | Print a group's definition and its latest epoch. |
| `trigger` | `--group <name>`, `--timeout <time>` | Inject a cut now and print the offsets it took. The broker clamps `--timeout` to its configured ceiling. |
| `list` | `--group <name>`, `--from-epoch <n>` (default `0`), `--max-results <n>` (default `-1`, every retained cut) | Print a group's retained cuts. |
| `verify` | `--group <name>`, `--epoch <n>` | Read the log at a cut's offsets and check that each one holds that cut's marker. |

A `<time>` value takes any unit the broker configuration takes: `ns`, `us`,
`ms`, `s`, `m`, `h`, `d`, and their long forms. The tool refuses a number with
no unit, so `--timeout 30` is an error rather than a guess.

## Exit codes

A runbook branches on `$?`, so each number means one thing across this tool
and `krabka-guard`.

| Code | Meaning |
| --- | --- |
| `0` | The broker accepted the request. |
| `1` | The broker refused the request. The reason is on stderr. |
| `2` | The broker could not be reached. Nothing is known about the outcome. |
| `3` | A cut does not match the log. |

## Documentation

- [KFC-4: cross-topic snapshots](../../docs/KFCs/KFC-4-cross-topic-snapshots.md)
- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-barrier/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
