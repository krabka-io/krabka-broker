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
