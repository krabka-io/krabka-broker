# Streams minimum-feature differential

Provision fresh caller-owned isolated oracle and enabled native brokers with
working topology-description backends. This fixture disables Streams with a safe
feature downgrade before creating any group, asserts both Heartbeatv1 and
TopologyDescriptionUpdatev0 return `UNSUPPORTED_VERSION`, upgrades
`streams.version=1`, and executes the existing nine-case `StreamsPositive`
lifecycle. Do not run it on existing clusters or repurpose existing data.

```
aspect kafka44-streams-minimum --oracle 127.0.0.1:64840 \
  --enabled 127.0.0.1:64842 --libs /absolute/pinned/released/libs \
  --output /fresh/private/receipt
aspect tests kafka44-streams-minimum
```

The oracle needs
`group.streams.topology.description.plugin.class=org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`.
The native harness `streams_fault_broker` installs the production filesystem
backend through the native embedding SPI; leave its fault flags healthy. This
is not arbitrary JVM plugin loading. Record the image, native executable/source,
configuration and owned lifecycle with the task's raw responses. The task does
not provision, stop or restart endpoints.

`qualification/kafka44-streams-minimum-20261104.json` retains direct paired
execution and a separate full AXL task execution on fresh runtimes. Both use
historical native source3af529aa6, not a fresh final-integration build. Each
endpoint passes two disabled-feature assertions and nine enabled lifecycle
assertions. Assignment timing/epochs may differ; only explicitly asserted
behavior is compared. No coordinator movement, restart, authorization or default
gate claim is made. `qualification_pass` remains false.
