# krabka-guard

Administers krabka topic write freezes and break-glass proposals: freeze a
scope, thaw one, and run the two-person rule.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

A write freeze is a broker-owned state. The cluster stays up, every read works,
and the broker refuses every new client write to a topic. A break-glass proposal
is a standing authorization that two different people agreed to, which one
privileged transition then spends. This crate is the operator's side of both
features, which
[KFC-9](../../docs/KFCs/KFC-9-topic-write-freeze-and-break-glass-workflows.md)
describes.

Two properties matter:

- `--sign-with` never leaves the machine. The tool reads a PKCS#8 Ed25519 key
  file, signs the canonical bytes locally, and sends only the `key_id` and the
  detached signature. A broker never holds an operator's private key.
- `freeze list --verify-signatures` checks the registry on this machine against
  operator public keys on this machine. The operator, not the broker that
  served the rows, decides that the registry is authentic.

Every subcommand speaks one krabka-private API key in the 1015 to 1019 range.
A JVM `AdminClient` cannot send those. The freeze is still visible to the JVM
tools: `DescribeConfigs` reports a read-only `write.freeze` key for every
topic, so `kafka-configs --describe` shows it.

The crate is a library as well as a binary. Tests call
`krabka_guard::run_from_args` in process, because a Bazel test sandbox has no
Cargo working tree to build a subprocess from.

## Usage

Every subcommand needs `--bootstrap-server` (`-b`), which also reads the
`KRABKA_BOOTSTRAP_SERVER` environment variable.

The broker's `[[operator_keys]]` table binds a key id to a principal and a
public key:

```toml
[[operator_keys]]
key_id = "alice-yubi"
principal = "User:alice"
public_key_path = "/etc/krabka/keys/alice-yubi.pub"
```

Freeze a topic during a DR cutover, with a signature, and then prove the
registry against the local trust set:

```sh
krabka-guard -b broker-1:9092 freeze set \
  --topic orders --reason "DR cutover" \
  --sign-with ~/.krabka/alice-yubi.pk8 --key-id alice-yubi --principal User:alice

krabka-guard -b broker-1:9092 freeze list \
  --verify-signatures --operator-keys /etc/krabka/broker.toml
```

A thaw is the dangerous direction, so it needs an approved break-glass
proposal and a signature. The proposer and the approver are two different
principals:

```sh
# Alice opens the proposal. The tool prints its id.
krabka-guard -b broker-1:9092 break-glass propose \
  --action thaw-topic-freeze --target orders \
  --reason "cutover complete" --ttl 30m

# Bob approves it.
krabka-guard -b broker-1:9092 break-glass approve \
  --proposal 6f1c2e0a-9b7d-4a3e-8c11-2d5f0e7b4a90 \
  --sign-with ~/.krabka/bob-yubi.pk8 --key-id bob-yubi

# Alice spends it.
krabka-guard -b broker-1:9092 freeze clear \
  --topic orders --proposal 6f1c2e0a-9b7d-4a3e-8c11-2d5f0e7b4a90 \
  --reason "cutover complete" \
  --sign-with ~/.krabka/alice-yubi.pk8 --key-id alice-yubi --principal User:alice
```

## Subcommands

### `freeze`

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `freeze set` | scope, `--reason <text>`, signing flags | Freeze a topic or a topic-name prefix. The broker takes an unsigned freeze unless `freeze.require_signature` is on. |
| `freeze list` | `--scope <name>`, `--verify-signatures`, `--operator-keys <path>` | Print the live registry entries. `--verify-signatures` needs `--operator-keys`, a TOML file that carries the `[[operator_keys]]` table, which can be the broker's own `broker.toml`. |
| `freeze clear` | scope, `--proposal <uuid>`, `--reason <text>` (default empty), signing flags | Lift a freeze. The scope must name the entry exactly: a freeze on `--prefix tenant-a.` is not lifted by naming one topic under it. |

The scope is exactly one of `--topic <name>` and `--prefix <prefix>`. A
literal scope names one topic. A prefixed scope names every topic whose name
starts with the prefix, including one the cluster creates later. The two words
are Kafka's ACL pattern types.

The signing flags are `--sign-with <path>`, `--key-id <id>`, and
`--principal <name>`, and they travel together. `--principal` is the name the
broker authenticates on this connection. The signed bytes carry it, and the
broker checks it against both the connection and the `[[operator_keys]]` entry.

### `break-glass`

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `break-glass propose` | `--action <action>`, `--target <name>`, `--reason <text>`, `--ttl <time>` | Open a proposal. The broker caps `--ttl` at `break_glass.proposal_ttl`, and uses that value when the flag is omitted. |
| `break-glass approve` | `--proposal <uuid>`, `--sign-with <path>`, `--key-id <id>` | Add one approval. The broker refuses the proposer, a principal outside `break_glass.approvers`, and a principal that already approved. |
| `break-glass withdraw` | `--proposal <uuid>` | Withdraw a proposal, so nothing can spend it. |
| `break-glass list` | `--pending`, `--proposal <uuid>` | Print the proposals and their approvals. `--pending` drops consumed, withdrawn, and expired proposals. |

`--action` is one of `thaw-topic-freeze`, `unclean-elect-leaders`,
`unclean-recovery`, `unregister-broker`, `cancel-reassignment`,
`delete-topic`, and `delete-records`. `--target` is a topic, a broker id, or a
partition, depending on the action.

A `<time>` value takes any unit the broker configuration takes: `ns`, `us`,
`ms`, `s`, `m`, `h`, `d`, and their long forms. The tool refuses a number with
no unit, so `--ttl 30` is an error rather than a thirty-millisecond proposal.

## Exit codes

A runbook branches on `$?`, so each number means one thing across this tool
and `krabka-barrier`. Code `3` keeps the meaning `krabka-barrier` gives it.

| Code | Meaning |
| --- | --- |
| `0` | The broker accepted the request. |
| `1` | The broker refused the request. The reason is on stderr. |
| `2` | The broker could not be reached. Nothing is known about the outcome. |
| `3` | The registry names an operator key this machine does not hold, so the tool could not check it. |
| `4` | The action needs a break-glass approval that does not exist. Go and get a second person. |
| `5` | A signature did not verify, or the broker needed one and did not get it. |

## Documentation

- [KFC-9: topic write freeze and break-glass workflows](../../docs/KFCs/KFC-9-topic-write-freeze-and-break-glass-workflows.md)
- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-guard/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
