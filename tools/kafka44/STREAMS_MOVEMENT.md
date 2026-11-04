# Shared-backend Streams coordinator movement

Provision two caller-owned three-node clusters. Use RF3 coordinator state and
minISR2. All released nodes mount the same writable `/evidence` directory and
load `FaultTopologyPlugin` from the pinned released jars and compiled classes;
leave flag files healthy. All native nodes use the same FLAGS directory with
the production `FileTopologyDescriptionBackend`. For the embedding example set
`STREAMS_FIXTURE_NODE` and `STREAMS_FIXTURE_VOTERS` (`ID@CONTROLLER,...`) per node.
Each node retains its own DATA and listener; the controller uses client port+1.

```
aspect kafka44-streams-movement --mode prepare \
  --oracle HOST:PORT --enabled HOST:PORT \
  --oracle-identity /private/oracle-identity \
  --enabled-identity /private/native-identity \
  --libs /absolute/released/libs --output /fresh/private/prepare
```

The fixture creates a unique RF3 source and Streams group, stores a topology,
checks STORED and the exact source, leaves the member, and retains the original
coordinator ID. Send SIGKILL only to that exact owned coordinator on each cluster.
Wait for failure detection/election, then run the same task with `--mode recover`,
a fresh output path and reachable surviving bootstrap endpoints. Recovery
requires a different coordinator ID, STORED/exact source, successful deletion on
the new owner, and recreation with solicitation and no stored description.
The task does not kill, restart or provision arbitrary endpoints.

`qualification/kafka44-streams-movement-20261104.json` retains direct released
and native executions, actual coordinator identities, SIGKILL commands/times,
shared backend configuration and exact executable hashes. Its raw before/after
topology objects are also compared in the receipt audit. Released owner1 moves
to2; native owner2 moves to3. Each side passes one prepare plus four recovery
assertions. Direct Java execution—not full AXL orchestration—produced this
receipt; the AXL parser's positive/negative controls passed separately.

This tests an empty-member group with already stored topology. It does not test
concurrent plugin operations, live member reconciliation, interrupted external
writes, power-loss/fsync durability, or arbitrary JVM loading by the native
broker. The native source baseline and example-only embedding patch are retained;
no final-head/default qualification is inferred. `qualification_pass=false`.
