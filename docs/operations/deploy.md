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
working directory. It carries no shell and no package manager. The operator
tools ride beside the broker under `/usr/bin`: `krabka-format`,
`krabka-audit`, `krabka-barrier`, `krabka-guard`, `krabka-worm-verify` and
`krabka-restore`. A tool that is not in the image cannot run in a container,
because there is no shell to fetch one with.

The image is **linux/amd64 only**. `packaging/base.apko.yaml` builds the Wolfi
base for `amd64`, the manifest declares `linux/amd64`, and the tags CI pushes
to `ghcr.io/krabka-io/krabka-broker` are bare amd64 manifests rather than a
multi-architecture index. There is no arm64 image. On an aarch64 host
`bazel run //packaging:image_load` does not produce a runnable image: the
manifest still says amd64 while the broker binary in it is an aarch64 ELF, so
the container fails to exec. Bazel refuses to build `//packaging:image` off
x86_64 for that reason. Multi-architecture images are a separate piece of work.

To run the binary directly on another architecture, build it with Bazel and
copy it out. That path builds for the host it runs on, so it works anywhere the
Rust toolchain does:

```
bazel build //:broker_bin //:format_bin
cp bazel-bin/crates/broker/krabka-broker bazel-bin/crates/format/krabka-format /usr/local/bin/
```

Both binaries link glibc and nothing else. The image's Wolfi base supplies
glibc and the CA bundle.

### Kubernetes

[`packaging/k8s/`](../../packaging/k8s/) holds a reference deployment of the
image: a three-node StatefulSet, a headless Service with a bootstrap Service,
and a PodDisruptionBudget. It is a starting point to read and adapt, not a
chart. Before you apply it, set the image tag, the node architecture and the
storage class and size. Also replace the two placeholder identities:
`KRABKA_CLUSTER_ID` and the directory ids in `KRABKA_INITIAL_CONTROLLERS`.
The label the manifests read the pod ordinal from needs Kubernetes 1.29 or
later.

The StatefulSet pins itself to amd64 nodes with
`nodeSelector: kubernetes.io/arch: amd64`, because that is the only
architecture the image is published for. On a mixed-architecture cluster that
selector is what keeps a pod off a node it could never exec the image on;
change it only when the image you point it at is built for something else.

```
kubectl apply -f packaging/k8s/
```

The parts that carry the design:

- The format step is an init container on the same image. It runs
  `krabka-format --ignore-formatted` on every start. On the first boot it
  formats the volume. On every later boot it exits 0 and leaves the identity
  in place.
- The pod's ordinal is the node id and the broker id. The
  `apps.kubernetes.io/pod-index` label supplies it through the downward API.
- `terminationGracePeriodSeconds` is 60, which outlasts the 20s
  controlled-shutdown drain.
- `fsGroup: 65532` gives the `nonroot` user the volume. Without it the format
  step fails on `meta.properties.json`.
- The liveness probe polls `/healthz` and the readiness probe polls
  `/readyz`, both on the `health` port, 9405. See
  [Health checks](#health-checks).
- The headless `krabka` Service sets `publishNotReadyAddresses`, so a peer
  that is not ready yet still resolves and the quorum can reach it. The
  `krabka-bootstrap` Service honours readiness, so a client's first
  `Metadata` request never lands on a node that is behind the quorum.
- The PodDisruptionBudget sets `minAvailable: 2`, the majority of three. Scale
  the StatefulSet and move the budget with it.

## Ports

| Port | Listener | Configured by |
| :--- | :--- | :--- |
| 9092 | Kafka client and inter-broker listener | `--listen-addr` (default `127.0.0.1:9092`). Under `--config-file` each `[[listeners]]` entry sets its own port. |
| 9093 | KIP-595 controller listener | Always `listen_addr` with the port set to 9093. Under `--config-file` it binds all interfaces. |
| 9404 | Prometheus `/metrics` and `/debug/pprof` | `--metrics-listen-addr`, `KRABKA_METRICS_LISTEN_ADDR`. `none` disables it. |
| 9405 | `/healthz` and `/readyz` probes | `--health-listen-addr`, `KRABKA_HEALTH_LISTEN_ADDR`. `none` disables them. |

Every broker in the cluster must reach every other broker's 9093. Clients
need only the port of the listener they connect to: 9092 under
`--listen-addr`, or the `bind_addr` port of their `[[listeners]]` entry under
`--config-file`. The three-node file below serves clients on 9094.

## Format the log directory

A node refuses to boot on an unformatted log directory. `krabka-format`
writes `meta.properties.json`, the bootstrap checkpoint and the KIP-853
voter record. The directory must be empty or absent; the tool creates it.

A single node that is its own controller:

```
krabka-format --log-dir /var/lib/krabka --standalone --node-id 1 \
    --controller-listener broker-1.example:9093
```

A three-node quorum with a KIP-853 dynamic initial voter set. Every node
runs the same command with its own `--node-id`, its own `--directory-id` and
the shared `--cluster-id`. The `--initial-controllers` list names every voter
as `id@host:port:directory-id` and must include the node itself. The tool
writes `kraft.version` 1 and a `VotersRecord` that holds the list. The broker
takes its voter set from that record, not from `controller_quorum_voters`:

```
krabka-format --log-dir /var/lib/krabka --node-id 1 \
    --cluster-id 0d7e2f5a-9b1c-4c1e-8a3f-2b6d1e4c9f10 \
    --directory-id 3a1d6f2b-0c4e-4f7a-9b8d-5e2c1a7f4d21 \
    --initial-controllers 1@broker-1.example:9093:3a1d6f2b-0c4e-4f7a-9b8d-5e2c1a7f4d21,2@broker-2.example:9093:<dir-2>,3@broker-3.example:9093:<dir-3>
```

A node that will join an existing quorum later formats with
`--no-initial-controllers` and the existing `--cluster-id`. It boots with
`--controller-bootstrap-servers` and, to become a voter,
`--controller-auto-join`.

A format with none of `--standalone`, `--initial-controllers` and
`--no-initial-controllers` is the static KIP-595 path. `kraft.version` stays
0, no `VotersRecord` is written, and each broker derives the voter set from
`controller_quorum_voters` in its config file at start.

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
`[runtime]` value. Most flags also read an environment variable, named in
`--help`; `KRABKA_CLUSTER_ID`, `KRABKA_PROCESS_ROLES` and
`KRABKA_METRICS_LISTEN_ADDR` are the ones a container normally sets.
`--listen-addr`, `--config-file`, `--print-config-schema`, `--log-dir` and
`--broker-id` have no environment variable form.

A minimal three-node file for `broker-1`:

```toml
rack = "zone-a"
inter_broker_listener_name = "INTERNAL"
replica_lag_time_max = "30s"

# Controller addresses, one entry per controller. After a format with
# `--initial-controllers` the voter set comes from the VotersRecord, and the
# broker uses this list only to reach the leader before the committed voter
# set names it; `krabka_broker_ignored_static_voters` counts its entries. A
# broker-only node dials this list to fetch metadata. Hosts are resolved on
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
enabled_mechanisms = ["SCRAM-SHA-512", "PLAIN"]

# PLAIN has no dynamic credential path the way SCRAM does. The file holds one
# `username=password` per line; `#` starts a comment. A listener that offers
# PLAIN with no credentials loaded is refused at start.
[sasl_plain]
credentials_path = "/etc/krabka/sasl/plain-credentials"

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
| `[sasl_plain]` | `credentials_path`: the file the static SASL/PLAIN credential table is read from. Required once a listener offers `PLAIN`. |
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

The broker logs `health server listening` first, before it opens the log
directory, so the probes answer through recovery. It then logs
`selected bootstrap mode`: `Bootstrap` on a fresh directory, `Rejoin` on one
it has run from before. It logs `metrics server listening` once `/metrics` is
up. Logs are one JSON object
per line on stdout; `RUST_LOG` sets the level.

Start the three voters within a few seconds of each other. The
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

Pick the build first. A release is an annotated `vX.Y.Z` tag, and
[Releasing](../releasing.md) describes how one is cut and how to verify its
image signature. The [changelog](../../CHANGELOG.md) records what each tag
contains.

1. Check the fleet is healthy before you start.
   `krabka_broker_under_replicated_partitions` is zero on every broker and
   `krabka_broker_offline_partitions_count` is zero. Do not roll a cluster
   that is already under-replicated: the next stop drops a partition below
   `min.insync.replicas`.
2. Stop one broker with `SIGTERM`. Wait for its log to record the controlled
   shutdown, or for `krabka_broker_partitions_led` on it to reach zero.
3. Replace the binary or the image and start the broker on the same
   `--log-dir`. It boots in `Rejoin` mode, catches up, and rejoins each ISR.
   `/readyz` answers 503 until the log directory is recovered, the listeners
   are bound, and the metadata offset is within
   `--readiness-max-metadata-lag` of the quorum's committed offset.
4. Wait until `/readyz` answers 200 and
   `krabka_broker_under_replicated_partitions` is zero on every broker again.
   Readiness says the node can serve clients. The gauge says the roll can
   continue. Wait for both.
5. Repeat for the next broker. Roll the controller leader last, so the
   quorum changes leader once rather than several times.
6. When the roll is complete, run a preferred election to move leadership
   back where the assignment puts it:
   `kafka-leader-election --bootstrap-server <broker>:9092 --election-type preferred --all-topic-partitions`.

On Kubernetes the StatefulSet's `RollingUpdate` strategy does steps 2 to 4
for you. It replaces one pod at a time. It waits for the new pod's `/readyz`
before it moves to the next, so a restart cannot run ahead of the quorum.
The PodDisruptionBudget keeps two of three pods available through a node
drain or a cluster upgrade, so a voluntary disruption cannot take the
quorum's majority. It does not gate the rolling update itself, and it does
not watch `krabka_broker_under_replicated_partitions`. Check step 1 before
you change the image, and step 6 after the last pod is ready.

A rise in `krabka_broker_unsupported_api_requests_total` during the roll is
a client that learned a new version range from an upgraded broker and sent it
to an old one. It clears when the roll completes.

krabka is undeployed and has no persisted-state compatibility guarantee
between builds. Read the [changelog](../../CHANGELOG.md) entry for a build
before you roll it. A build that changes an on-disk format needs a fresh
`krabka-format` on every node and a restore from the topic data, not a roll.

## Health checks

The broker serves two HTTP probes on their own listener, port 9405 by
default. `--health-listen-addr` or `KRABKA_HEALTH_LISTEN_ADDR` moves it, and
`none` disables both probes. The listener starts before the broker opens the
log directory. It stays up and `/readyz` answers 503 through the
controlled-shutdown drain. An orchestrator gets an answer in both windows.

`GET /healthz` is the liveness probe. It answers 200 with the body `ok` for
as long as the process runs its event loop. It looks at no other condition.
A broker that replays a large log directory, or that fetches a long metadata
log, is alive but not yet usable. A liveness failure there would kill the
node in the middle of a recovery, and the restart would run the same recovery
again.

`GET /readyz` is the readiness probe. It answers 200 with the body `ready`
when every condition below holds. Otherwise it answers 503, and the body
names the first condition that fails, in this order:

| Body starts with | Condition |
| :--- | :--- |
| `not ready: shutting_down` | Controlled shutdown is in progress and draining partition leadership to other replicas. |
| `not ready: log_dir_recovery` | The log directories are not scanned yet, or a recovered partition has no writer yet. |
| `not ready: listeners_bound` | A data-plane listener is not bound yet, or its accept loop is not running. |
| `not ready: metadata_quorum_unreached` | The node has not reached the metadata quorum, so it cannot know whether it is caught up. |
| `not ready: metadata_lag` | The node's `__cluster_metadata` offset trails the quorum's committed offset by more than the bound. The body gives both numbers. |

`--readiness-max-metadata-lag` or `KRABKA_READINESS_MAX_METADATA_LAG` sets
the bound, in records. The default is 100. A node that is ahead of the
offset it last heard from the quorum has a lag of zero.

Point a Kubernetes `livenessProbe` at `/healthz` and a `readinessProbe` at
`/readyz`. Do not use a TCP probe on 9092 for readiness. The listener opens
before the node has caught up on metadata, and a client that reaches it
then gets `Metadata` from a stale image. Do not point the liveness probe at
`/readyz` either, or the kubelet restarts a node that is recovering. A
readiness failure only takes the pod out of the Service endpoints.

To find the active controller, read `krabka_broker_active_controller` on
`/metrics`. It is 1 on exactly one broker.

## Profiles

The metrics port also serves `/debug/pprof/profile` for a CPU profile. The
flags under `--profiling-*` set the default duration and the sample
frequency. A heap profile at `/debug/pprof/heap` needs a build with
`--features heap-profiling`.
