# krabka-format

Formats the log directories of a krabka broker node: Kafka's
`meta.properties`, the bootstrap records, and the singleton `VotersRecord`.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

A KRaft node does not boot against an unformatted directory. The broker treats
one as an operator error and stops at startup. This tool is the counterpart of
`kafka-storage format`. It formats every log directory of the node in one run,
writes each directory's identity, seeds the bootstrap metadata records, and can
seed SCRAM credentials and ACLs at the same time. Each directory gets the
`meta.properties` file that `kafka-storage format` writes, with the cluster id,
the node id, and the directory id, so the Kafka tools read a krabka directory.
`kafka-metadata-quorum add-controller`, for one, reads the directory id of a new
controller from it. The ids are written and printed in Kafka's 22-character
base64 form, so an id moves between krabka and the Kafka tools unchanged.
[`docs/format-divergences.md`](../../docs/format-divergences.md) lists every
place the tool differs from `kafka-storage format`, and why.

The crate is a library as well as a binary. The broker reads and writes
`meta.properties` with its `MetaProperties`. Broker tests call
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
  --initial-controllers '1@ctrl-1:9093:Xmssij0fS56afB8uPUxbag,2@ctrl-2:9093:ChssPU5fSmuMfZ4PGis8TQ,3@ctrl-3:9093:fGtaTT4vTRybig-ejXxrWg' \
  --release-version 4.0
```

Format a dynamic controller that joins an existing quorum later. Pass the
cluster's existing id; without `--cluster-id` the tool generates a new one, and
the quorum rejects a joiner whose cluster id differs:

```sh
krabka-format \
  --log-dir /var/lib/krabka \
  --node-id 4 \
  --cluster-id DX4vWpscTB6KPyttHkyfEA \
  --no-initial-controllers
```

Format a node with three data disks. Without `--metadata-log-dir`, the first
`--log-dir` is also the metadata log directory, as Kafka's `metadata.log.dir`
defaults to the first entry of `log.dirs`. The broker's `--log-dir` names the
first directory, and its `--extra-log-dirs` names the others:

```sh
krabka-format \
  --log-dir /var/lib/krabka,/mnt/disk1/krabka,/mnt/disk2/krabka \
  --node-id 1 --standalone --controller-listener broker-1:9093
```

Format a node with a metadata disk apart from two data disks. The run formats
the `--metadata-log-dir` and every `--log-dir`, as `kafka-storage format`
formats `metadata.log.dir` and `log.dirs`. Give the broker the same
`--metadata-log-dir`, `--log-dir`, and `--extra-log-dirs`:

```sh
krabka-format \
  --metadata-log-dir /var/lib/krabka-metadata \
  --log-dir /mnt/disk1/krabka,/mnt/disk2/krabka \
  --node-id 1 --standalone --controller-listener broker-1:9093
```

Only the metadata log directory gets the bootstrap records, `bootstrap.json`
and `bootstrap.records.bin`. With a dynamic quorum flag, it also gets Kafka's
bootstrap snapshot,
`__cluster_metadata-0/00000000000000000000-0000000000.checkpoint`. Each data
directory gets `meta.properties` and nothing else.

`--ignore-formatted` skips a directory that is already formatted and formats
the rest, which is how a disk added later is formatted. Without it, one
formatted directory refuses the whole run. A run that fails partway can be run
again as it was: `meta.properties` is written last, and the next run removes
what the failed run left.

The reason for a failure is on stderr. The exit code names its cause:

| Code | Cause |
| :--- | :--- |
| `0` | Every directory is formatted, or was already and `--ignore-formatted` is set. |
| `2` | An `--add-scram` iteration count is below 4096, or the command line does not parse. |
| `3` | A directory is already formatted without `--ignore-formatted`, holds files that `krabka-format` did not write, or names another cluster or another node. |
| `4` | A write failed, or the quorum flags name an invalid voter set. |
| `5` | A `--feature`, `--release-version`, or quorum-mode combination is invalid. |

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--log-dir <path>` | required | A directory to format, an entry of Kafka's `log.dirs`. Repeat the flag or separate paths with commas. Without `--metadata-log-dir`, the first is also the metadata log directory. A directory must be absent, empty, or formatted by an interrupted run. |
| `--metadata-log-dir <path>` | the first `--log-dir` | The metadata log directory, Kafka's `metadata.log.dir`. The run formats it together with every `--log-dir`. It can also be one of them. |
| `--cluster-id <id>` | kept or generated | The cluster id, in Kafka's base64 form or the hyphenated form. Without it, the id of an already formatted directory is kept, else a new id is generated. |
| `--release-version <version>` | the broker's maximum | The bootstrap `metadata.version` (KIP-778), for example `4.0` or `4.0-IV3`. A string with more than two dot-separated segments keeps the first two, as in Kafka, so `4.3.1` is `4.3`. |
| `--feature <name>=<level>` | none | Set one feature's finalized level (KIP-1022), for example `transaction.version=2`. Repeat for each feature. Conflicts with `--release-version` for `metadata.version` only. |
| `--add-scram <spec>` | none | Seed a SCRAM credential. The spec is `SCRAM-SHA-256=[name=<u>,password=<p>,iterations=<n>]` or the `SCRAM-SHA-512` form. `iterations` defaults to `4096`. Repeat for each credential. |
| `--add-acl <spec>` | none | Seed an ACL entry. The spec is `principal=User:<name>,host=<ip or *>,operation=<Op>,permission=<Allow or Deny>,resource=<Type>:<Name>[:<Pattern>]`. `Pattern` defaults to `Literal`. Repeat for each entry. |
| `--node-id <id>` | required | This node's id, Kafka's `node.id`, from 0 to 2147483647. Every `meta.properties` records it, and the broker refuses to start on a directory of another node. Give the broker the same `--broker-id`. |
| `--directory-id <id>` | generated | The metadata log directory's id, in either form, for an orchestrator that checks the exact node incarnation before it declares the node ready. |
| `--standalone` | off | Format this node as the sole initial controller voter. |
| `--initial-controllers <list>` | none | The initial controllers, as comma-separated `id@host:port:directory-id` entries. The directory id is in either form. |
| `--no-initial-controllers` | off | Format a dynamic controller that joins an existing quorum. |
| `--controller-listener <host:port>` | none | This node's controller listener, written into the `VotersRecord` with `--standalone`. |
| `--controller-listener-name <NAME>` | `CONTROLLER` | The controller listener name, the first entry of Kafka's `controller.listener.names`. The voter endpoints that `--standalone` and `--initial-controllers` write carry it, in upper case. A leader refuses an `AddRaftVoter` whose endpoints lack its own listener name, so it has to match the name that `kafka-metadata-quorum add-controller` sends. |
| `--ignore-formatted` | off | Skip an already formatted directory and format the others. |

`--standalone`, `--initial-controllers`, and `--no-initial-controllers`
exclude each other.

## Documentation

- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-format/)
- [`krabka-restore`](../restore/README.md), which calls this crate to format a
  restored log directory

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
