# Explicit endpoint routing checks

`RoutingEndpoints.java` uses released Kafka 4.4 codecs and requires two explicitly
mapped broker endpoints and their distinct node IDs. Unlike selecting the first
node returned by `Admin.describeCluster()`, the caller's endpoint mapping is
unambiguous in a multi-broker fixture.

Compile alongside `ProtocolScenarios.java` with the immutable released client
classpath. Run against a dedicated, enabled fixture:

```
java -cp "CLASSES:RELEASED_LIBS/*" RoutingEndpoints HOST1:PORT1 NODE1 HOST2:PORT2 NODE2 before
```

The ten assertions check each endpoint's correct identity, the other node's
identity, a wrong cluster, a partial identity, and absence of identity. Unexpected
errors or transport failures terminate unsuccessfully. Raw responses precede
assertion rows. A provisioning owner can crash/restart a node without changing
its persistent storage, then invoke the same command with `after-restart`.
Compare cluster identity across both calls; the fixture must not silently replace
the cluster. ApiVersions v5 carries cluster ID and node ID, **not an incarnation
ID**. A preserved identity should continue to be accepted after restart; a
swapped endpoint/node mapping should continue to be rejected.

This fixture is not proof of metadata invalidation after cluster replacement,
SASL behavior (covered separately), controller listener behavior, requests pipelined
behind a rejected handshake, minimum feature boundaries, or overall Kafka 4.4
qualification. Two-voter fixtures are sufficient for these read-only handshake
checks with both nodes running; they are not a failover availability proof.

## Minimum feature boundary work still required

`MixedFeatureProbe.java` records metadata-version preservation and explicit update
admission in three-node rolling configurations. It does not establish each new
surface's immediate-below/at-minimum behavior. The full matrix still needs isolated
released/candidate configurations at actual supported feature boundaries for DLQ,
remote-copy configuration, controller unregistration, topic-ID transactional
commits and Streams APIs, with raw feature advertisements and operation responses.
Do not equate default API gating or a metadata upgrade rejection with that matrix.

## Requests already pipelined behind the handshake

`RoutingPipeline.java` serializes both ApiVersions5 and CreateTopics7 before
reading any response. Use only dedicated fixtures: it creates a fresh topic.

```
javac --release 17 -cp "RELEASED_LIBS/*" -d CLASSES tools/kafka44/RoutingPipeline.java
java -cp "CLASSES:RELEASED_LIBS/*" RoutingPipeline HOST:PORT NODE FRESH_TOPIC wrong-node
java -cp "CLASSES:RELEASED_LIBS/*" RoutingPipeline HOST:PORT NODE OTHER_FRESH_TOPIC wrong-cluster
```

Each execution requires routing error129, successful creation on the same
connection, and independent Admin visibility. Unexpected closure, transport
errors, missing topics or preexisting topic names fail. Released4.4 **does apply**
the pipelined mutation; the handshake does not fence other requests. The paired
[receipt](../../qualification/kafka44-routing-pipeline-20261105.json) measures
the same behavior on enabled native code. Raw config rows differ and are not
claimed identical. This does not actually replace a cluster or test a stale
client after replacement.
