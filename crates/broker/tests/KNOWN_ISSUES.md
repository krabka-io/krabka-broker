# Known issues

## `console_producer_round_trip` / `kafka_topics_describe_smokes_metadata`

These tests run a Rust `krabka-broker` on the host. The JVM Kafka
command-line tools run inside a `mirror.gcr.io/confluentinc/cp-kafka` testcontainers
container. The JVM client in the container must reach the host broker.
The host broker's `advertised_listener` must also point at an address
that the container can reach. `Metadata` responses return that address.
If the address is not reachable, the JVM client connects to the
bootstrap server, learns the real address from `Metadata`, and then
cannot reconnect.

The network pattern depends on the Docker host:

- Linux hosts, including GitHub Actions ubuntu-latest: set
  `KRABKA_HOST_BOOTSTRAP=<docker_bridge_gateway>:<port>`. The value is
  usually `172.17.0.1:9092`. Find the bridge IP with:

  ```sh
  BRIDGE_IP=$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')
  ```

- Docker Desktop on macOS and Windows: the default is
  `host.docker.internal:9092`. Docker Desktop maps that name to the
  host loopback address. The test uses this case when
  `KRABKA_HOST_BOOTSTRAP` is not set.

The test binds the broker on `0.0.0.0:<port>`. It takes the port from
the last colon-separated segment of `KRABKA_HOST_BOOTSTRAP`. If that
environment variable is not set, the port is `9092`.

## Automated Bazel lane

The tests remain `#[ignore = "requires Docker"]` under Cargo, but the
digest-pinned Bazel Docker lane runs them through
`//crates/broker:jvm_acceptance_cli_docker_test`. Run that lane with:

```sh
bazel test --config=docker //crates/broker:jvm_acceptance_cli_docker_test
```

The log differential and KIP-631 checkpoint checks run in the same lane as
`//crates/log:integration_docker_test` and
`//crates/raft:kraft_checkpoint_jvm_docker_test`.

`jvm_role_separated_admin` and `jvm_broker_loggers` run in that lane too, as
`//crates/broker:jvm_role_separated_admin_docker_test` and
`//crates/broker:jvm_broker_loggers_docker_test`. The first drives
`kafka-configs --alter` and `kafka-features upgrade`/`downgrade` at a
broker-only node in a role-separated cluster, which is the topology
`324ab7d` and `4a8e6cb` made a broker survive in; the second drives
`kafka-configs --entity-type broker-loggers` through both its describe paths
and its alter.

## Scheduled GSSAPI validation

`gssapi_e2e` remains a manual Bazel target. Its locally built KDC fixture writes
`kafka.keytab` and `alice.keytab` into the fixture directory, so it cannot use
the digest-pinned Bazel lane and must be managed with Docker Compose. The
scheduled and `workflow_dispatch` `container · gssapi` CI job runs both the
`gssapi_e2e` and `auth_handlers` GSSAPI cases. For a local run, follow the
Compose and environment setup in `gssapi_e2e.rs`, then run:

```sh
cargo test -p krabka-broker --test gssapi_e2e --test auth_handlers gssapi -- --ignored --test-threads=1
```

## Windows validation

Docker on Windows with the bridge-gateway pattern is not reliable. The
JVM command-line tools also behave inconsistently under Docker Desktop
on Windows. The dedicated CI lane currently runs only on Ubuntu, so this
acceptance path has no validated Windows CI or runtime result.

## A follower replica of a compacted topic is never compacted

The soak lane (`soak.rs`) still fails its open-descriptor assertion, and this is
what is left behind it. Local retention now runs — `soak-retention`'s segments
are trimmed, its retention-cycle count clears ten, and the log-directory series
is level — but the descriptor count still climbs on all three brokers, about one
per second, and every one of those descriptors belongs to `soak-compacted`.

Counting the segment files inside each container 150 s into a run says why:

```text
krabka-soak-1  soak-compacted-0: 2   soak-compacted-1: 27  soak-compacted-2: 26
krabka-soak-2  soak-compacted-0: 28  soak-compacted-1: 2   soak-compacted-2: 28
krabka-soak-3  soak-compacted-0: 27  soak-compacted-1: 27  soak-compacted-2: 2
```

For each partition exactly one broker holds two segments and the other two hold
twenty-eight and rising. The one holding two is the leader. `crate::cleaner`'s
sweep skips a partition whose `current_leader` is not this node
(`crates/broker/src/cleaner/sweep.rs`), so a follower replica of a compacted
topic is never compacted, and since `cleanup.policy=compact` sets no
`retention.ms` there is nothing else to bound its segment count either. Kafka's
`LogCleanerManager` walks every log the broker hosts, exactly as
`LogManager.cleanupLogs` does, so a Kafka follower compacts its own replica.

The local-retention sweep added in `crate::log_retention` deliberately applies
no leadership filter for that reason. The cleaner's filter was left as it was:
changing when compaction runs is a separate decision from making retention run
at all, and it touches the `log_cleaner_uncleanable_partitions` contract, which
is documented as "partitions this broker leads".

Until that is settled the soak lane fails on the descriptor trend alone. Every
other assertion it makes — the cycle counts, the cleaner-failure count, the
resident set, the metric cardinality, the log-directory size, and the read-back
of every acked record — passes.
