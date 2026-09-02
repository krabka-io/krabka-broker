# Deploy

How to install, configure, start and upgrade a krabka-broker cluster. The
flags and files below are the ones the binary on this branch reads. Run
`krabka-broker --help` for the full list.

## Install

The container image is the supported form. `bazel run //packaging:image_load`
builds it from locked Wolfi packages and loads it into the local daemon as
`docker.io/krabka-io/krabka-broker:dev`; CI pushes the same image to
`ghcr.io/krabka-io/krabka-broker`. The image runs as `nonroot` (65532), has
`/usr/bin/krabka-broker` as its entrypoint and `/var/lib/krabka` as its
working directory. It carries no shell and no package manager.

To run the binary directly, build it with Bazel and copy it out:

```
bazel build //:broker_bin //:format_bin
cp bazel-bin/crates/broker/krabka-broker bazel-bin/crates/format/krabka-format /usr/local/bin/
```

Both binaries link glibc and nothing else. The image's Wolfi base supplies
glibc and the CA bundle.

## Ports

| Port | Listener | Configured by |
| :--- | :--- | :--- |
| 9092 | Kafka client and inter-broker listener | `--listen-addr`, or `[[listeners]]` in the config file |
| 9093 | KIP-595 controller listener | Always `listen_addr` with the port set to 9093. Under `--config-file` it binds all interfaces. |
| 9404 | Prometheus `/metrics` and `/debug/pprof` | `--metrics-listen-addr`, `KRABKA_METRICS_LISTEN_ADDR`. `none` disables it. |

Every broker in the cluster must reach every other broker's 9093. Clients
need 9092 only.

## Format the log directory

A node refuses to boot on an unformatted log directory. `krabka-format`
writes `meta.properties.json`, the bootstrap checkpoint and the KIP-853
voter record. The directory must be empty or absent; the tool creates it.

A single node that is its own controller:

```
krabka-format --log-dir /var/lib/krabka --standalone --node-id 1 \
    --controller-listener broker-1.example:9093
```

A three-node static quorum. Every node runs the same command with its own
`--node-id`, its own `--directory-id` and the shared `--cluster-id`. The
`--initial-controllers` list names every voter as `id@host:port:directory-id`
and must include the node itself:

```
krabka-format --log-dir /var/lib/krabka --node-id 1 \
    --cluster-id 0d7e2f5a-9b1c-4c1e-8a3f-2b6d1e4c9f10 \
    --directory-id 3a1d6f2b-0c4e-4f7a-9b8d-5e2c1a7f4d21 \
    --initial-controllers 1@broker-1.example:9093:3a1d6f2b-0c4e-4f7a-9b8d-5e2c1a7f4d21,2@broker-2.example:9093:<dir-2>,3@broker-3.example:9093:<dir-3>
```

A node that will join an existing quorum later formats with
`--no-initial-controllers` and boots with `--controller-bootstrap-servers`
and, to become a voter, `--controller-auto-join`.

Other format-time options:

- `--release-version 4.0` pins the bootstrap `metadata.version`. The default
  is the broker's maximum.
- `--feature name=level` sets one KIP-1022 feature level. Repeat it.
- `--add-scram 'SCRAM-SHA-512=[name=admin,password=...]'` seeds a credential
  so the first broker can authenticate an admin before any `AlterUserScramCredentials` runs.
- `--add-acl 'principal=User:admin,host=*,operation=All,permission=Allow,resource=Cluster:kafka-cluster'`
  seeds an ACL for the same reason.

## Configure

The binary takes a TOML file with `--config-file`. When the file is present,
`--listen-addr` and `--advertised-listener` must not be set; the listeners
come from the file. Every other flag still applies and overrides the file's
`[runtime]` value. The flags all have an environment variable form, named in
`--help`; `KRABKA_CLUSTER_ID`, `KRABKA_PROCESS_ROLES` and
`KRABKA_METRICS_LISTEN_ADDR` are the ones a container normally sets.

A minimal three-node file for `broker-1`:

```toml
rack = "zone-a"
inter_broker_listener_name = "INTERNAL"
replica_lag_time_max = "30s"

# KIP-595 static voters, one entry per controller. Hosts are resolved on
# every connect, so a pod that restarts with a new IP is reached again.
controller_quorum_voters = [
  "1@broker-1.example:9093",
  "2@broker-2.example:9093",
  "3@broker-3.example:9093",
]

[process]
roles = ["controller", "broker"]

[[listeners]]
name = "INTERNAL"
bind_addr = "0.0.0.0:9092"
advertised = "broker-1.example:9092"
protocol = "Plaintext"

[[listeners]]
name = "CLIENT"
bind_addr = "0.0.0.0:9094"
advertised = "kafka.example:9094"
protocol = "SaslSsl"

[listeners.tls_config]
cert_path = "/etc/krabka/tls/tls.crt"
key_path = "/etc/krabka/tls/tls.key"

[listeners.sasl_config]
enabled_mechanisms = ["SCRAM-SHA-512"]

[runtime]
controlled_shutdown_drain_timeout = "20s"
```

The sections the file accepts are the fields of
`krabka_broker::file_config::FileConfig`. The ones an operator reaches for
first:

| Section | Purpose |
| :--- | :--- |
| `[[listeners]]` | Each listener's bind address, advertised address, protocol, TLS and SASL. |
| `[process]` | `roles`: `controller`, `broker`, and the `witness` modifier for a stretch cluster. |
| `[stretch]` | The three sites of a stretch cluster and the preferred leader site. All-or-nothing. |
| `[runtime]` | Timers and sizes: the controlled-shutdown drain, snapshot cadence, the diskless WAL flusher, queue capacities. Heartbeats and `replica_lag_time_max` sit at the top level. |
| `[remote_storage]` | KIP-405 tiered storage: the S3 or GCS bucket, the WORM policy and the topic-backed metadata manager. |
| `[audit]` | The audit topic, its failure mode, signing and the durable spool. |
| `[authorization]` | ACL or OPA authorizer. |
| `[schema_registry]` | KFC-7 schema validation. |
| `[freeze]`, `[break_glass]`, `[[operator_keys]]` | KFC-9 write freezes and the two-person rule. |
| `[server_properties]` | Kafka `server.properties` keys the broker honours, such as `log.retention.hours`. |

Timers are strings with a unit: `"30s"`, `"250ms"`. Sizes are strings too:
`"64MiB"`.

## Start

```
krabka-broker --config-file /etc/krabka/broker.toml \
    --log-dir /var/lib/krabka --broker-id 1 \
    --cluster-id 0d7e2f5a-9b1c-4c1e-8a3f-2b6d1e4c9f10
```

The broker logs `selected bootstrap mode` with `Bootstrap` on a fresh
directory and `Rejoin` on one it has run from before. It then logs
`metrics server listening` once `/metrics` is up. Logs are one JSON object
per line on stdout; `RUST_LOG` sets the level.

Start the three static voters within a few seconds of each other. The
quorum elects a leader once a majority is up, and the brokers register with
it. `kafka-metadata-quorum --bootstrap-server broker-1.example:9092 describe
--status` shows the leader and each voter's lag.

Extra data directories for KIP-113 JBOD go in `--extra-log-dirs` or
`KRABKA_EXTRA_LOG_DIRS`, comma-separated. The metadata log always stays on
`--log-dir`.

## Stop

`SIGTERM` and `SIGINT` both run a controlled shutdown. The broker asks the
controller to move every partition it leads to another ISR member, waits up
to `controlled_shutdown_drain_timeout` (default 20s) for that to complete,
then closes its listeners and exits. `kubectl delete pod` and `docker stop`
send `SIGTERM`. Give the process a termination grace period longer than the
drain timeout, or the orchestrator kills it mid-drain and the partitions it
led take an election instead of a hand-off.

## Rolling upgrade

Upgrade one broker at a time. The wire protocol is negotiated per
connection, so a mixed fleet serves clients throughout.

1. Check the fleet is healthy before you start.
   `krabka_broker_under_replicated_partitions` is zero on every broker and
   `krabka_broker_offline_partitions_count` is zero. Do not roll a cluster
   that is already under-replicated: the next stop drops a partition below
   `min.insync.replicas`.
2. Stop one broker with `SIGTERM`. Wait for its log to record the controlled
   shutdown, or for `krabka_broker_partitions_led` on it to reach zero.
3. Replace the binary or the image and start the broker on the same
   `--log-dir`. It boots in `Rejoin` mode, catches up, and rejoins each ISR.
4. Wait until `krabka_broker_under_replicated_partitions` is zero on every
   broker again. That is the only signal that the roll can continue.
5. Repeat for the next broker. Roll the controller leader last, so the
   quorum changes leader once rather than several times.
6. When the roll is complete, run a preferred election to move leadership
   back where the assignment puts it:
   `kafka-leader-election --bootstrap-server <broker>:9092 --election-type preferred --all-topic-partitions`.

A rise in `krabka_broker_unsupported_api_requests_total` during the roll is
a client that learned a new version range from an upgraded broker and sent it
to an old one. It clears when the roll completes.

krabka is undeployed and has no persisted-state compatibility guarantee
between builds. Read the release notes for a build before you roll it: a
build that changes an on-disk format needs a fresh `krabka-format` on every
node and a restore from the topic data, not a roll.

## Health checks

There is no dedicated health endpoint. Use these:

- Liveness: an HTTP `GET /metrics` on port 9404 returns 200 while the process
  serves requests.
- Readiness: `krabka_broker_partitions_total` on that broker is greater than
  zero once it has registered and taken its replicas. A TCP probe on 9092 is
  too early, because the listener opens before the broker has rejoined its
  ISRs.
- Controller: `krabka_broker_active_controller` is 1 on exactly one broker.

## Profiles

The metrics port also serves `/debug/pprof/profile` for a CPU profile. The
flags under `--profiling-*` set the default duration and the sample
frequency. A heap profile at `/debug/pprof/heap` needs a build with
`--features heap-profiling`.
