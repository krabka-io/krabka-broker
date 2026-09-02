# krabka-restore

Offline point-in-time restore: rebuilds a complete, bootable krabka log directory from a KIP-405 tiered-storage archive.

Part of [Krabka](../../README.md), a Rust implementation of Apache Kafka. The [KIP compatibility matrix](../../docs/KIP_MATRIX.md) records the KIP-405 archive layout this tool reads and the tests behind it.

## Overview

`krabka restore` reads a tiered-storage archive from object storage and materializes a krabka data directory, replayed up to a bound and verified as it rehydrates. The bound is an offset, a timestamp, or a set of exclude-record predicates, so an operator can recover an event-sourced system to the state it held just before a bad write. Recovery of that kind is a hand-built runbook everywhere else.

The tool runs when the cluster does not. It reads the archive through [`krabka-object-store`](../object-store), decodes the archive keys with [`krabka-remote-storage`](../remote-storage), and formats the target through [`krabka-format`](../format). It does not depend on the broker.

## The binary is `krabka-restore`

The `krabka` operator CLI resolves an unknown subcommand to `krabka-<name>` on `PATH`, the way git resolves `git foo` to `git-foo`. The binary therefore carries the name `krabka-restore`, and not the `krabka-` prefix that the rest of the workspace uses. Put the binary on `PATH` and `krabka restore` works.

The crate is a library as well as a binary. A tool that embeds the restore calls `krabka_restore::restore`, which returns the structured report and the structured error. Only the binary maps an error onto an exit code.

## Usage

Restore every topic in an S3 archive, up to a point in time, into a fresh standalone cluster:

```sh
krabka restore \
  --archive-s3-bucket krabka-tier \
  --archive-s3-region eu-west-1 \
  --archive-prefix prod/ \
  --rlmm-snapshot /var/lib/krabka/remote-log-metadata/snapshot \
  --metadata-snapshot /var/lib/krabka/__cluster_metadata/@metadata-0/00000000000000123456-0000000042.checkpoint \
  --log-dir /var/lib/krabka-restored \
  --cluster-id 4c9e2f1a-1f7d-4a53-9a1e-7c0c8a2b6d31 \
  --node-id 1 --standalone --controller-listener 127.0.0.1:9093 \
  --to-timestamp 2026-08-24T09:15:00Z \
  --report json
```

Drop one bad producer's records from one partition, and check the plan first:

```sh
krabka restore --archive-local /mnt/tier --log-dir /tmp/check --dry-run \
  --topic orders \
  --to-offset orders:0=184320 \
  --exclude-producer-id 7 \
  --exclude-offset orders:0=184100..=184119
```

Run `krabka restore --help` for the whole flag surface. The exit codes are 0 for success, 2 for a bad argument, 3 for a target log directory that is not empty, 4 for an archive that is unreadable or empty, 5 for an integrity failure, and 6 for a materialization failure.

## Limits

A batch that a predicate filters is re-encoded from the records that survive. Its bytes are therefore not identical to the archived bytes, and only a batch that passes through untouched keeps the producer's original encoding.

`--exclude-key` and `--exclude-header` match the raw key and header bytes. The tool decodes no payload and knows no schema, so a pattern written against a JSON field or an Avro field does not match.

Without `--rlmm-snapshot` the restore has only the object keys to work from. A segment that the old cluster had marked for deletion is then indistinguishable from a live one, and the restore includes it. Supply the snapshot from a broker's `<log.dir>/remote-log-metadata/snapshot` when the archive holds segments that retention had already released.

Without `--metadata-snapshot`, topic configuration, ACLs, client quotas, SCRAM credentials, and finalized feature levels cannot be recovered. The report warns and names every restored topic whose configuration is unavailable. Supply a controller `<offset>-<epoch>.checkpoint` from `<log.dir>/__cluster_metadata/@metadata-0/` to restore that state.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
