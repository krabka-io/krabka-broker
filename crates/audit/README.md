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
request that changed nothing writes nothing. From `krabka.version` 3 the record
carries the OCSF API Activity of the api in the last column; below it, every
row is activity 0. See [OCSF record versions](#ocsf-record-versions).

| RPC | Audited resources | Activity from `krabka.version` 3 |
| --- | --- | --- |
| `CreateTopics` | Topic per created topic | 1 Create |
| `DeleteTopics` | Topic per deleted topic | 4 Delete |
| `CreatePartitions` | Topic per grown topic | 1 Create |
| `AlterConfigs` | The config resource, then a `ConfigKey` per changed key | 3 Update |
| `IncrementalAlterConfigs` | The config resource, then a `ConfigKey` per changed key | 3 Update |
| `AlterClientQuotas` | `ClientQuotaEntity` per changed entity, as `type=name` | 3 Update |
| `AlterUserScramCredentials` | User per changed credential | 3 Update |
| `CreateAcls` | Acl per created binding | 1 Create |
| `DeleteAcls` | Acl per deleted binding | 4 Delete |
| `CreateDelegationToken` | `DelegationToken`, by token id | 1 Create |
| `RenewDelegationToken` | `DelegationToken`, by token id | 3 Update |
| `ExpireDelegationToken` | `DelegationToken`, by token id | 3 Update |
| `UpdateFeatures` | Feature per finalized feature | 3 Update |
| `ElectLeaders` | Partition per partition that changed leader | 3 Update |
| `AlterPartitionReassignments` | Partition per altered partition | 3 Update |
| `DeleteRecords` | Partition per trimmed partition | 4 Delete |
| `UnregisterBroker` | The broker id | 4 Delete |
| `UnregisterController` | `Controller`, by controller id | 4 Delete |
| `AddRaftVoter`, `RemoveRaftVoter` | `RaftVoter`, by voter id | 3 Update |
| `SetTopicFreeze` | `TopicFreeze`, by target, then the `break-glass-proposal` that approved a thaw | 3 Update |

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

## OCSF record versions

`__krabka_audit` records are a persisted format. A broker writes the krabka
1.0.0 record, version 1, until the cluster finalizes `krabka.version` 3, and
writes version 2 from then on. Each record keeps the version it was serialized
in, including one that waited in the spool across the upgrade. The hash chain
commits to a record's bytes, so a partition that mixes the two verifies like
any other.

| Event | Version 1 activity | Version 2 activity |
| --- | --- | --- |
| Application Lifecycle (6002) broker start | 1 `BrokerStarted` | 3 `Start` |
| Application Lifecycle (6002) broker stop | 4 `BrokerStopping` | 4 `Stop` |
| Application Lifecycle (6002) configuration change | 3 `ConfigApplied` | 99 `ConfigApplied` |
| Application Lifecycle (6002) TLS reload | 3 `TlsReloaded` | 99 `TlsReloaded` |
| Authorize Session (3003) ACL denial | 2 `Deny` | 99 `Deny` |
| API Activity (6003) admin operation | 0 | 1, 3 or 4 by api, as above; 0 for any other |
| API Activity (6003) privileged action | 0 | 0 |
| Authentication (3002) logon | 1 `Logon` | 1 `Logon` |

The `type_uid` is always `class_uid * 100 + activity_id`. A version 2
Application Lifecycle record also names the broker in `app`, with its product
name and version. Every version 2 record carries `"log_version": "2"` in its
`metadata` object, OCSF's event log schema version. A version 1 record has no
`log_version`.

## The `krabka-audit` binary

`krabka-audit verify` reads an audit partition directory offline. It
recomputes the chain with the writer's primitives and checks every checkpoint
signature against the trusted key for the `key_id` the checkpoint names. It
does no recovery and no truncation, so tail corruption stays visible.

A partition that spans a rotation of the audit key holds checkpoints under
each key id. Trust every one of them: repeat `--key-id` and `--public-key`,
paired by position.

```sh
krabka-audit verify \
  --partition-dir /var/lib/krabka/__krabka_audit-0 \
  --key-id audit-2025 \
  --public-key /etc/krabka/keys/audit-2025.pub \
  --key-id audit-2026 \
  --public-key /etc/krabka/keys/audit-2026.pub
```

| Flag | Description |
| --- | --- |
| `--partition-dir <path>` | The audit partition directory, for example `<log_dir>/__krabka_audit-0`. |
| `--key-id <id>` | A `key_id` to trust: an `[audit.signing] key_id` the broker has signed with. Required, and repeatable once per key. |
| `--public-key <path>` | The Ed25519 public key, as raw 32 bytes, for the `--key-id` at the same position. Give exactly one per `--key-id`; the tool exits `1` when the counts differ. |

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
