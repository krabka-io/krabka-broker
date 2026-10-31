# Jepsen Kafka workload

[Jepsen](https://jepsen.io) tests distributed systems for safety. It runs
operations against a cluster from many clients at the same time. A nemesis
injects faults on the way. A checker then examines the full history of the
operations for anomalies.

Jepsen's
[`jepsen.tests.kafka`](https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html)
workload tests a Kafka-compatible queue. Its clients send records to a set of
keys, each key one topic partition. Other clients poll the records back. With
`--txn`, the clients send and poll in Kafka transactions. The checker finds
lost writes, duplicates, polls that skip or go back, inconsistent offsets, and
transactional anomalies such as G0 and G1c.

`aspect jepsen` runs that workload against a krabka cluster in Docker
containers. The workload, its generator and its checker are Jepsen's. The
harness in [`tools/jepsen/`](../tools/jepsen/) adds the client, the database
setup, and the nemesis.

## Run the tests

```sh
aspect jepsen
```

The task needs Docker, Bazel, Maven, and a JDK for the Jepsen JVM. It
starts five node containers and a Jepsen JVM with a heap of up to 8 GB. The
first run downloads the Jepsen and Kafka client libraries into the local Maven
repository.

The task does these steps:

1. It builds `krabka-broker` and `krabka-format` with `bazel build -c opt` and
   installs them in `target/jepsen/bin/`.
2. It builds the node image `krabka-jepsen-node:latest` from
   [`tools/jepsen/node.Dockerfile`](../tools/jepsen/node.Dockerfile).
3. It resolves the harness classpath with Maven from
   [`tools/jepsen/pom.xml`](../tools/jepsen/pom.xml), into
   `target/jepsen/classpath.txt`.
4. It starts the node containers `n1` to `n5` on the Docker network
   `jepsen-krabka`.
5. It writes the selected manifest entries to `target/jepsen/selected.json`,
   and runs them with the harness from the sources in `tools/jepsen/src/`.
   Jepsen runs one test for each entry.
6. It compares each result with the manifest and writes
   `target/jepsen/report.md`.

To run part of the manifest, give one or more `--test` prefixes. A prefix
selects each entry whose name starts with it:

```sh
aspect jepsen --test assign-
aspect jepsen --test assign-none --test subscribe-txn
```

These options change the other steps:

| Option | Default | Effect |
| :--- | :--- | :--- |
| `--manifest FILE` | `tools/jepsen/suite.json` | The manifest, relative to the repository root. |
| `--test PREFIX` | every entry | Run only the entries whose name starts with `PREFIX`. Repeatable. |
| `--bin-dir DIR` | build with Bazel | Use the `krabka-broker` and `krabka-format` in `DIR`, and do not build. `DIR` is absolute or relative to the repository root. |
| `--image NAME` | build the node image | Start the nodes from the existing image `NAME`. |
| `--nodes N` | 5 | Start `N` node containers, `n1` to `n<N>`. |
| `--heap SIZE` | `8g` | The maximum heap of the Jepsen JVM, in the form that `-Xmx` takes. |
| `--keep-nodes` | off | Leave the node containers up after the run. |

The node containers have fixed names. A run removes each container named `n1`
to `n<N>` before it starts the nodes, also a container that the run did not
start. At the end the task removes the node containers, unless `--keep-nodes`
keeps them. Use `docker exec -it n1 bash` to examine a kept node. The task
keeps the `jepsen-krabka` network for the next run.

## How the harness works

The harness is a Clojure program in `tools/jepsen/src/jepsen/krabka/`. The
task does not compile it. `java -cp` runs it from source with `clojure.main`.
Its `suite` command runs every entry of a manifest, one Jepsen test each, and
writes a JSON summary.

- **Remote.** Jepsen usually reaches each node with SSH. With `--docker`, the
  harness reaches the container that has the name of the node with
  `docker exec`, and it copies files with `docker cp`. It also skips the
  operating system setup, because the node image already holds the tools that
  Jepsen uses.
- **Database.** Each test uploads the two binaries from `--bin-dir` to every
  node. Every node runs `krabka-broker` in KRaft combined mode, as a broker and
  as a voter of the static controller quorum. Each node logs to
  `/opt/krabka/broker.log`. At the end of a test, Jepsen copies the log, the
  `krabka.toml` file, and an archive of the data directory of each node into
  the store.
- **Client.** The client is Apache Kafka's Java client, at the release that
  `pom.xml` pins.
- **Nemesis.** The harness uses Jepsen's combined nemesis. `--nemesis` selects
  the faults: `partition` with `iptables`, `packet` delay, loss, duplication
  and reordering with `tc netem`, `kill` and `pause` of the broker process, and
  `clock` skew.
- **Checker.** Jepsen's Kafka checker examines the history. The harness also
  fails a test whose broker log has a Rust panic.

The node image holds only the tools that Jepsen needs on a node. Each
container runs with `--init`, so that a killed broker is reaped and does not
stay a zombie, and with the `NET_ADMIN` capability for `iptables` and `tc`.
Containers share the host's clock, so the harness refuses `--nemesis clock`
with `--docker`, and the nodes get no `SYS_TIME`.

## The manifest

[`tools/jepsen/suite.json`](../tools/jepsen/suite.json) lists the tests. It
crosses the two ways a consumer gets partitions (`assign` and `subscribe`),
with and without transactions, with each fault alone and with partitions,
kills and pauses together. It adds crashed clients, and KIP-848's `consumer`
group protocol:

```json
{
  "defaults": ["--safe", "--time-limit", "120", "--concurrency", "2n"],
  "tests": [
    {"name": "assign-none", "args": ["--sub-via", "assign"]},
    {"name": "assign-packet", "args": ["--sub-via", "assign", "--nemesis", "packet"], "requires": ["netem"]},
    {"name": "subscribe-txn-partition", "args": ["--sub-via", "subscribe", "--txn", "--nemesis", "partition"],
     "known_failure": "how it fails"}
  ]
}
```

- `defaults` is a list of harness options that every test gets. An entry's
  `args` come after them, so an entry can override a default.
- `name` identifies the entry. It holds lowercase letters, digits, and `-`
  only. Each name occurs one time.
- `args` is the list of harness options for this test. It can be empty.
- `requires` lists what the test needs from the machine: `netem` for packet
  faults, and `isolated-clock` for clock faults.
- `known_failure` tells how krabka fails the test.

The task checks the manifest before it starts. It stops with status 2 if the
manifest has an unknown key, or a field of the wrong type.

The harness skips an entry whose requirement the machine does not meet.
`netem` needs the `sch_netem` module in the host kernel. `isolated-clock`
needs a clock for each node, and Docker containers share the clock of the
host. So under Docker, the harness always skips an entry that requires
`isolated-clock`.

The `--safe` option sets the client settings under which Kafka promises the
most: `acks=all`, idempotent producers, `read_committed` consumers, and a
replication factor of 3. To list every harness option, run the harness with
`--help` after a first run of the task:

```sh
java -Djava.awt.headless=true -cp "tools/jepsen/src:$(cat target/jepsen/classpath.txt)" \
  clojure.main -m jepsen.krabka.core test --help
```

The JVM needs `-Djava.awt.headless=true`. Without it, a graph library that
Jepsen loads asks for a display and fails.

## Results

Jepsen writes each test to its store in `target/jepsen/store/`, in a directory
named for the test and its start time. The store holds the history, the
checker results, latency and rate plots, the Jepsen log, and the log and data
of each node. The task prints the store path of each regression. To look at
the store in a browser, run the harness's `serve` command from
`target/jepsen`.

The harness writes `target/jepsen/results.json` with one result for each
entry that it ran. Each result has one of these statuses:

| Status | Meaning |
| :--- | :--- |
| `valid` | The checker found no anomaly that makes the history invalid. |
| `invalid` | The checker found an anomaly. |
| `unknown` | The checker could not decide. |
| `crashed` | The test stopped with an exception, or its options were bad. |
| `skipped` | The machine does not meet a requirement of the entry. |

The task reports an entry with no result as `NOT RUN`. It gives each entry one
of five verdicts:

| Verdict | Expectation | Result |
| :--- | :--- | :--- |
| pass | pass | `valid` |
| regression | pass | `invalid`, `unknown`, `crashed` or `NOT RUN` |
| regression | `known_failure` | `crashed` or `NOT RUN` |
| known failure | `known_failure` | `invalid` or `unknown` |
| fixed | `known_failure` | `valid` |
| skipped | either | `skipped` |

`target/jepsen/report.md` has the totals, then one row for each entry. For a
regression, the row names the anomalies that made the history invalid, or the
first line of the error. The exit status of the harness is not the verdict.

The task exits with status 1 if one or more entries is a regression. When an
entry is fixed, remove its `known_failure`.

## Run one test by hand

The harness's `test` command runs one test with any options, against nodes
that are already up. Start them with `aspect jepsen --test assign-none
--keep-nodes`, then run from `target/jepsen`:

```sh
java -Djava.awt.headless=true -cp "../../tools/jepsen/src:$(cat classpath.txt)" \
  clojure.main -m jepsen.krabka.core test \
  --docker --nodes n1,n2,n3,n4,n5 --bin-dir bin \
  --safe --txn --nemesis partition,kill --time-limit 300 --test-count 5
```

Without `--docker`, the harness uses Jepsen's SSH remote and its Debian setup,
as Jepsen's other tests do. On Debian virtual machines with their own clocks,
`--nemesis clock` works too. `--bin-dir` is still a directory on the control
machine.

## Unit tests

The pure parts of the harness have unit tests: option parsing, the suite
runner's bookkeeping, the node configuration and the key mapping. They need no
cluster:

```sh
java -Djava.awt.headless=true \
  -cp "tools/jepsen/src:tools/jepsen/test:$(cat target/jepsen/classpath.txt)" \
  clojure.main -m jepsen.krabka.core-test
```

`aspect tests jepsen` tests the task's own functions.

## Known gaps

- Docker nodes share the clock of the host, so the task cannot run clock
  faults.
- A host kernel without `sch_netem` cannot run packet faults. The harness then
  reports those entries as skipped.
- The nemesis does not change the cluster's membership. It does not add or
  remove brokers or controller voters.
- All nodes are on one host. A partition stops traffic between containers, but
  the nodes share one kernel, one disk, and one CPU scheduler.
- The cluster uses `PLAINTEXT` listeners only. The task does not test TLS or
  SASL.
- The task tests one client, Apache Kafka's Java client.
