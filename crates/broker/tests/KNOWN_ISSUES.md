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

## `jvm_role_separated_admin`: broker liveness in a role-separated cluster

`jvm_role_separated_admin` is a manual Bazel target, not a `container ·` CI
suite, because the broker cannot yet keep a broker-only node alive when the
only voter is a controller-only node.

`heartbeat::client::run` resolves the controller leader's address out of that
leader's `BrokerRegistrationRecord`:

```rust
let Some(broker_rec) = image.broker(leader_id) else {
    debug!("heartbeat: controller leader not in metadata image yet");
    continue;
};
```

`registration::register_broker` returns early for a node whose roles do not
include `Broker`, so a controller-only node writes no such record and that
lookup never resolves. A broker-only node therefore never sends a single
`BrokerHeartbeat`. The controller's liveness registry seeds every registered
broker `fenced = true` in `track_registered`, and only the broker-side
`BrokerHeartbeat` handler clears that fence, so within one liveness tick both
broker-only nodes are fenced, and one `heartbeat_timeout` later they are
`DEAD`. `Metadata` then lists no live broker and every JVM tool reports
`Timed out waiting for a node assignment`, whatever it was asked to do.

`assign_replicas_to_dirs` (KIP-858) fails in the same cluster for the same
reason, and logs `controller leader not in image` on every reconcile.

Reproduce it with:

```sh
RUST_LOG='warn,krabka_broker::heartbeat=debug' \
  cargo test -p krabka-broker --test jvm_role_separated_admin -- --ignored --nocapture
```

`heartbeat: controller leader not in metadata image yet` repeats for the whole
run.

`role_separation_observer` boots the same topology and passes only because it
finishes in well under a second — before the first liveness tick fences
anything. It is not evidence that the heartbeat path works.

The fix is a routing change, not a test change: Kafka sends `BrokerHeartbeat`
to the controller quorum over the *controller* listener, and krabka's own
controller listener already advertises and answers api key 63
(`crates/raft/src/server/registration/heartbeat.rs`). That handler answers from
the image alone and never touches `ControllerLivenessState`, so moving the
client onto it also needs the controller's liveness registry wired into the
raft listener. That is its own change, so the suite stays manual until then.

## Windows validation

Docker on Windows with the bridge-gateway pattern is not reliable. The
JVM command-line tools also behave inconsistently under Docker Desktop
on Windows. The dedicated CI lane currently runs only on Ubuntu, so this
acceptance path has no validated Windows CI or runtime result.
