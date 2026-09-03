# krabka-audit

Audit event model, OCSF serialization, hash-chained write pipeline, and the
`krabka-audit verify` tool.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

The broker records privileged operations as audit events. This crate defines
those events, serializes them as OCSF, and writes them to the `__krabka_audit`
internal topic as a hash chain. An Ed25519 signer emits signed checkpoints over
the chain at a configured cadence. When the topic is unavailable, a durable
spool keeps the events until the broker can replay them.

The broker calls this crate; a reader of the audit topic does not need to. The
WORM archive manifests of
[KFC-5](../../docs/KFCs/KFC-5-worm-archive-integrity-manifests.md) reuse the
same chain and signature primitives.

## Features

- An event model with the `Authentication`, `Authorization`, `ApiActivity`,
  and lifecycle classes, and their OCSF mapping.
- A per-record SHA-256 hash chain, with the previous head and sequence carried
  as record headers.
- Signed checkpoints with `FileEd25519Signer`, so an offline reader can attest
  the chain against a public key alone.
- A durable spool for the degraded path, with a byte cap and an fsync cadence.
- Three Creusot-proved kernels in [`krabka-verified`](../verified/README.md)
  decide spool admission, checkpoint admission, and loss-marker admission. A
  Stateright model checks the spool under crashes and replays.

## Which RPCs are audited

Every admin RPC that changes cluster state emits one `AdminOperation` event —
OCSF `ApiActivity`, `class_uid` 6003 — on its success path, naming the
principal, the source endpoint, the api, and the resources it changed. A
request that changed nothing writes nothing.

| RPC | Audited resources |
| --- | --- |
| `CreateTopics` | Topic per created topic |
| `DeleteTopics` | Topic per deleted topic |
| `CreatePartitions` | Topic per grown topic |
| `AlterConfigs` | The config resource, then a `ConfigKey` per changed key |
| `IncrementalAlterConfigs` | The config resource, then a `ConfigKey` per changed key |
| `AlterClientQuotas` | `ClientQuotaEntity` per changed entity, as `type=name` |
| `AlterUserScramCredentials` | User per changed credential |
| `CreateAcls` | Acl per created binding |
| `DeleteAcls` | Acl per deleted binding |
| `CreateDelegationToken` | `DelegationToken`, by token id |
| `RenewDelegationToken` | `DelegationToken`, by token id |
| `ExpireDelegationToken` | `DelegationToken`, by token id |
| `UpdateFeatures` | Feature per finalized feature |
| `ElectLeaders` | Partition per partition that changed leader |
| `AlterPartitionReassignments` | Partition per altered partition |
| `DeleteRecords` | Partition per trimmed partition |
| `UnregisterBroker` | The broker id |
| `AddRaftVoter`, `RemoveRaftVoter` | `RaftVoter`, by voter id |

Values never reach the record — only names and, for a config change, the names
of the keys that moved. A SCRAM `salt`, `salted_password`, `stored_key` and
`server_key` are not in it, and neither is a delegation token's HMAC, which is
the token's password equivalent; a token is named by its id.

The break-glass transitions — an unclean election, a reassignment cancel, a
topic freeze, an unregister — additionally emit `PrivilegedAction` events
carrying the approvals they spent. A refusal is recorded there, not here: this
class covers what succeeded.

Authentication is audited too, outside the RPC table because it happens before
any RPC. Every completed credential presentation emits one `Authentication`
event — OCSF `class_uid` 3002 — naming the mechanism, the principal in the
`User:<name>` form the privileged-action rows use, and the peer endpoint:

| What | `auth_protocol` | Outcome |
| --- | --- | --- |
| A `SaslAuthenticate` exchange that finished, initial or KIP-368 re-auth | The negotiated mechanism, e.g. `PLAIN` | Success, or failure with the broker's error message |
| A request refused by the pre-auth state gate (`ILLEGAL_SASL_STATE`) | The negotiated mechanism, or `unknown` | Failure |
| An mTLS client cert bound at accept time on a non-SASL listener | `SSL` | Success |

A failed exchange names the principal it claimed only where the mechanism sent
it in the clear, which is PLAIN. An anonymous connection presents no credential
and writes no row.

## The `krabka-audit` binary

`krabka-audit verify` reads an audit partition directory offline. It
recomputes the chain with the writer's primitives and checks every checkpoint
signature against a trusted key. It does no recovery and no truncation, so
tail corruption stays visible.

```sh
krabka-audit verify \
  --partition-dir /var/lib/krabka/__krabka_audit-0 \
  --key-id audit-2026 \
  --public-key /etc/krabka/keys/audit-2026.pub
```

| Flag | Description |
| --- | --- |
| `--partition-dir <path>` | The audit partition directory, for example `<log_dir>/__krabka_audit-0`. |
| `--key-id <id>` | The `key_id` the trusted public key belongs to. It is the `[audit.signing] key_id` the broker signs with. |
| `--public-key <path>` | The trusted Ed25519 public key, as raw 32 bytes. |

The exit code is `0` when the chain is continuous, every signature is valid,
and a signed checkpoint covers every record. It is `1` for a break in the chain,
a lost-record marker, or an unsigned tail, and the reason is on stderr.

## Usage

```rust
use std::path::Path;

use krabka_audit::{TrustedKeys, verify_partition_dir};

fn attest(partition_dir: &Path, public_key: Vec<u8>) -> bool {
    let trusted = TrustedKeys::single("audit-2026".to_owned(), public_key);
    match verify_partition_dir(partition_dir, &trusted) {
        Ok(report) => report.ok && report.losses.is_empty() && report.unanchored_records == 0,
        Err(_) => false,
    }
}
```

## Documentation

- [Verification catalog](../../docs/verification.md), for the proved kernels
  and the spool model
- [KFC-5: WORM archive integrity manifests](../../docs/KFCs/KFC-5-worm-archive-integrity-manifests.md)
- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-audit/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
