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

## Manual GSSAPI validation

`gssapi_e2e` remains manual. Its locally built KDC fixture writes
`kafka.keytab` and `alice.keytab` into the fixture directory, so it cannot use
the digest-pinned Bazel lane and must be managed with Docker Compose. Follow
the command sequence in `gssapi_e2e.rs`.

## Windows validation

Docker on Windows with the bridge-gateway pattern is not reliable. The
JVM command-line tools also behave inconsistently under Docker Desktop
on Windows. The dedicated CI lane currently runs only on Ubuntu, so this
acceptance path has no validated Windows CI or runtime result.
