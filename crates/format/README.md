# krabka-format

Formats a fresh krabka broker log directory: `meta.properties.json`, the
bootstrap records, and the singleton `VotersRecord`.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

A KRaft node does not boot against an unformatted directory. The broker treats
one as an operator error and stops at startup. This tool is the counterpart of
`kafka-storage format`. It writes the directory identity, seeds the bootstrap
metadata records, and can seed SCRAM credentials and ACLs at the same time.

The crate is a library as well as a binary. Broker tests call
`krabka_format::run_from_args` in process, because a Bazel test sandbox has no
Cargo working tree to build a subprocess from. A restore tool that rebuilds a
cluster from a tiered-storage archive calls `run_with_records` and hands over
the topic and partition records it recovered, so the broker boots with those
topics present.

## Usage

Format a single-node cluster whose one node is the initial controller, with an
admin SCRAM credential:

```sh
krabka-format \
  --log-dir /var/lib/krabka \
  --node-id 1 --standalone --controller-listener broker-1:9093 \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=admin-secret]'
```

Format a node of a three-controller cluster. The node's own id must appear in
the list, and each entry names the controller's directory id:

```sh
krabka-format \
  --log-dir /var/lib/krabka \
  --node-id 2 \
  --initial-controllers '1@ctrl-1:9093:5e6b2c8a-3d1f-4b9e-9a7c-1f2e3d4c5b6a,2@ctrl-2:9093:0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d,3@ctrl-3:9093:7c6b5a4d-3e2f-4d1c-9b8a-0f9e8d7c6b5a' \
  --release-version 4.0
```

Format a dynamic controller that joins an existing quorum later. Pass the
cluster's existing id; without `--cluster-id` the tool generates a new one, and
the quorum rejects a joiner whose cluster id differs:

```sh
krabka-format \
  --log-dir /var/lib/krabka \
  --cluster-id 0d7e2f5a-9b1c-4c1e-8a3f-2b6d1e4c9f10 \
  --no-initial-controllers
```

The exit code is `0` on success and non-zero on every failure the operator can
cause, such as a directory that is not empty or a malformed `--add-scram` spec.
The reason is on stderr.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--log-dir <path>` | required | The directory to format. It must be empty or absent. |
| `--cluster-id <uuid>` | generated | The cluster id. |
| `--release-version <version>` | the broker's maximum | The bootstrap `metadata.version` (KIP-778), for example `4.0` or `4.0-IV3`. |
| `--feature <name>=<level>` | none | Set one feature's finalized level (KIP-1022), for example `transaction.version=2`. Repeat for each feature. Conflicts with `--release-version` for `metadata.version` only. |
| `--add-scram <spec>` | none | Seed a SCRAM credential. The spec is `SCRAM-SHA-256=[name=<u>,password=<p>,iterations=<n>]` or the `SCRAM-SHA-512` form. `iterations` defaults to `4096`. Repeat for each credential. |
| `--add-acl <spec>` | none | Seed an ACL entry. The spec is `principal=User:<name>,host=<ip or *>,operation=<Op>,permission=<Allow or Deny>,resource=<Type>:<Name>[:<Pattern>]`. `Pattern` defaults to `Literal`. Repeat for each entry. |
| `--node-id <id>` | none | This node's raft id. Required with `--standalone` and with `--initial-controllers`. |
| `--directory-id <uuid>` | generated | A stable directory identity, for an orchestrator that checks the exact node incarnation before it declares the node ready. |
| `--standalone` | off | Format this node as the sole initial controller voter. |
| `--initial-controllers <list>` | none | The initial controllers, as comma-separated `id@host:port:directory-id` entries. |
| `--no-initial-controllers` | off | Format a dynamic controller that joins an existing quorum. |
| `--controller-listener <host:port>` | none | This node's controller listener, written into the `VotersRecord` with `--standalone`. |

`--standalone`, `--initial-controllers`, and `--no-initial-controllers`
exclude each other.

## Documentation

- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-format/)
- [`krabka-restore`](../restore/README.md), which calls this crate to format a
  restored log directory

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
