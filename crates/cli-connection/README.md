# krabka-cli-connection

The cluster connection that every krabka operator CLI shares:
`--bootstrap-server` and a Kafka `--command-config` file.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

`krabka-guard`, `krabka-barrier` and `krabka-backup` all reach a cluster, and
all of them spell it the same way. `--bootstrap-server` (`-b`, or the
`KRABKA_BOOTSTRAP_SERVER` environment variable) says where the cluster is.
`--command-config <file>` names a Java properties file with the connection's
TLS and SASL settings, the file that `kafka-topics --command-config` and the
other JVM tools take. Without `--command-config` a tool connects in PLAINTEXT.

The crate gives a tool two clap flag groups to flatten:

- `ConnectionArgs` holds both flags, and its `connect(client_id)` builds a
  `krabka_client_core::Client` with the file's security policy.
  `krabka-guard` and `krabka-barrier` use it.
- `SecurityArgs` holds `--command-config` alone, for a tool whose bootstrap
  server is optional. `krabka-backup` uses it, because a capture can run
  without a cluster.

The file is read with the same port of Java's `Properties.load` that reads
`meta.properties`, so it reads what Kafka's tools read. The crate never logs or
prints the file's properties, and an error names the property that is wrong,
never its value.

## The properties file

| Property | Values |
| --- | --- |
| `security.protocol` | `PLAINTEXT` (the default), `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` |
| `ssl.truststore.type`, `ssl.keystore.type` | `PEM`, the only store type it reads |
| `ssl.truststore.location` | The CA certificates, as PEM. Without it the tool trusts the platform trust store. |
| `ssl.keystore.location`, `ssl.key.location` | The client certificate chain and private key, as PEM, for mutual TLS. They go together. |
| `ssl.server.name` | The TLS server name. Without it each connection uses its own target host. |
| `sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `sasl.kerberos.service.host` | The host the SASL handshake names. PLAIN and SCRAM ignore it. |
| `sasl.jaas.config` | The login module line that carries `username` and `password`. `tokenauth=true` makes a SCRAM login a delegation-token login. |

A `sasl.*` property needs a SASL protocol and an `ssl.*` property needs `SSL`
or `SASL_SSL`, so a file that would silently connect without its settings is
refused. A SASL login over TLS:

```properties
security.protocol=SASL_SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/krabka/ca.pem
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="<password>";
```

Keep the file's permissions tight, because it holds the password or the key
path.

## Documentation

- [`krabka-guard`](../guard-cli/README.md), [`krabka-barrier`](../barrier-cli/README.md) and [`krabka-backup`](../backup-cli/README.md)
- [Backup and restore: secured clusters](../../docs/operations/backup-restore.md#secured-clusters)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
