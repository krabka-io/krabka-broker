# Ecosystem support

Which Kafka ecosystem tools work against krabka and which cannot. The line is
the interface a tool reads, not the vendor. A tool that reads the Kafka
protocol works. A tool that reads a broker-side JVM interface does not, because
krabka is not a JVM and serves no JMX endpoint.

## The two interfaces

krabka serves the Apache Kafka wire protocol. Every request a tool sends
through a Kafka client, an `AdminClient`, a consumer or a producer reaches the
same handlers the JVM broker's do, and the answers are byte-compatible.

krabka serves no JMX. There is no MBean server, no RMI registry and no port to
attach `jconsole`, `jmxterm` or `jmx_prometheus_javaagent` to. `jmx.port` in
the deployment guide is the port the JMX exporter of a *JVM* broker uses; it is
not a port krabka opens. krabka exports the same measurements over Prometheus
on `/metrics`, and [metrics.md](metrics.md) names the JMX MBean and attribute
each series replaces.

krabka also runs no broker-side plugins. A tool that ships a JVM class to load
into the broker, such as a metrics reporter, an interceptor or a custom
authorizer, has nothing to load into.

## What works

| Tool class | Examples | What it reads |
| :--- | :--- | :--- |
| Admin command line | `kafka-topics`, `kafka-configs`, `kafka-consumer-groups`, `kafka-acls`, `kafka-leader-election`, `kafka-reassign-partitions` | The `AdminClient` protocol. |
| Client libraries | The JVM clients, librdkafka and everything on it, `kcat` | Produce, Fetch and the group protocols. |
| Consumer-lag monitors | Burrow, `kafka-lag-exporter`, `kafka-exporter` | `__consumer_offsets`, decoded with Kafka's own schemas, or `OffsetFetch` and `ListOffsets`. |
| Cluster dashboards on the protocol | Redpanda Console, AKHQ, Kafdrop, Conduktor's protocol views | `Metadata`, `DescribeCluster`, `DescribeConfigs`, `DescribeGroups`. |
| Replication and integration | MirrorMaker 2, Kafka Connect distributed workers, schema registries | Produce, Fetch, the `AdminClient` and the compacted internal topics they own. |

The lag monitors are the class with the sharpest failure mode: they decode the
`__consumer_offsets` records themselves, so a one-field divergence makes them
report zero consumers on a healthy cluster with no error anywhere. The
`jvm_consumer_offsets_formatter` suite runs Kafka's own `OffsetsMessageFormatter`
and `GroupMetadataMessageFormatter` over the topic krabka writes and compares
the decoded rows with what krabka committed, so that class is held to a real
Kafka decoder rather than to a claim.

## What does not work

| Tool | What it needs | Why it cannot work |
| :--- | :--- | :--- |
| Cruise Control | The `CruiseControlMetricsReporter` class loaded in the broker, plus JMX for the rest of its sampling | krabka loads no JVM plugin and serves no JMX. Its rebalance proposals have no input. |
| `jmx_prometheus_javaagent` and every JMX exporter | A Java agent inside the broker process | There is no JVM to attach to. Scrape `/metrics` instead. |
| CMAK (Kafka Manager), Burrow's JMX collector, Conduktor's JMX views | An open JMX port on each broker | krabka opens none. The protocol-only parts of these tools still work. |
| LinkedIn `kafka-monitor` broker-side checks | Broker-side JVM instrumentation | Same reason. Its end-to-end producer and consumer probes work, because those are protocol clients. |
| Heap, GC and JVM-thread dashboards | JVM runtime MBeans | krabka has no heap and no garbage collector. The panels have no counterpart, and the resource questions they answer are in [capacity.md](capacity.md). |

A tool in this table that has both a protocol path and a JMX path keeps the
protocol path. Turn the JMX collector off rather than the whole tool.

## Reporting a gap

A tool that speaks the protocol and still misbehaves is a bug in krabka, and
the wire bytes are the evidence. Capture the request and the response, name the
tool's release, and open an issue. A tool that needs JMX is on this page
instead, and a JMX endpoint is not planned.
