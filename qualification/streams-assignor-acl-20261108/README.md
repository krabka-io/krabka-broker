# Combined ACL and custom callback failures (2026-11-08)

Two actual brokers execute the same released-codec probe with a dedicated
synthetic SASL/PLAIN actor while `failure` is selected. Eight scenario rows pass
per broker, matching error sequence30 (group denied),29 (source topic denied
after group READ grant),-1 (callback failure after topic READ grant). Describe
confirms the failed actor is absent after every error. Selecting healthy `first`
and joining c restores exact tasks0/1 at a through the same reconciliation check.

Native example base00cc632c8 plus this commit's opt-in ACL harness; binary hash in
receipt.json. Oracle image ID is the pinned apache/kafka4.4.0 image in the
invalid-output receipt. Both probe processes exit0. Native example build and
Java compilation pass. An initial probe invocation failed before network access
because its Admin timeout configuration was invalid; fixed request.timeout.ms,
retained successful final logs here and exploratory logs in private runtime.

## Reproduction

Compile StreamsAssignorAcl.java with the released jars, ProtocolScenarios,
StreamsPositive and StreamsAssignorWire. Start an isolated released broker with
the checked-in broker.properties and matching AssignorPlugin classes as in
STREAMS_ASSIGNOR_WIRE.md. Start native example with fresh DATA/FLAGS,
STREAMS_FIXTURE_ASSIGNORS=1 and STREAMS_FIXTURE_ACL=1 at127.0.0.1:65110;
its synthetic SASL listener is LISTEN+2, controller LISTEN+1. Native privileged
plaintext listener and released anonymous superuser are qualification-only.
The checked-in synthetic password is fixture data, never a supplied credential.

```
java -cp "$LIBS/*:$RUN/classes" StreamsAssignorAcl 127.0.0.1:65100 127.0.0.1:65102 fresh-oracle
java -cp "$LIBS/*:$RUN/classes" StreamsAssignorAcl 127.0.0.1:65110 127.0.0.1:65112 fresh-native
```

The probe grants ACLs only for its caller-named synthetic group/topic. It leaves
groups, topics and ACLs in the isolated fixture; it does not modify production.
No authorization race, distributed ACL propagation, plugin-removal restart,
malformed callback output, or every fault/ACL Cartesian product is established.
Runtime data: coordinator workspace `.private/assignor-acl-20261108/`.
