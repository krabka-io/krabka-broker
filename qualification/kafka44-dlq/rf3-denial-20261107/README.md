# Paired RF3 DLQ destination denial and restart

The [receipt](receipt.json) and raw logs record one bounded Kafka 4.4/Krabka
comparison on three combined broker/controller nodes per peer. This closes the
previously unmeasured **RF3 destination WRITE-denial** scenario for one rejected
source record, not arbitrary fault/ACL permutations or a complete parity suite.

## Observed result

The same released-client fixture runs against both peers. An anonymous direct
Produce positive control is refused with `TopicAuthorizationException`.
Authenticated `admin` produces one source record, consumes it using the real
`KafkaShareConsumer`, and commits REJECT successfully. The internal anonymous
DLQ producer is refused; actual manager logs report terminal failure and
archival (native error 29, `TOPIC_AUTHORIZATION_FAILED`). Neither destination
contains a record in the eight-second observation window.

Before and after a clean stop of **all three nodes**, restart on the same data,
and replication convergence, both peers report source startOffset=0 and lag=0;
a five-second share-consumer observation sees no redelivery. The fixture checks
source, destination and `__share_group_state` each have three replicas, three
in-sync replicas and minISR=2 during both verification runs.

Released `DumpLogSegments --share-group-state-decoder` decodes the actual state
logs on **all three replicas of both peers**, before and after restart. Each of
these twelve replica/phase observations contains ARCHIVING(3), then ARCHIVED(4),
with final startOffset=0 and deliveryCompleteCount=1. The
[state summary](state-summary.json) retains assertions extracted from those raw
logs. This extends the earlier RF1 clean-restart ordering evidence to the
measured RF3 denial path; it does not supersede historical crash receipts.

## Reproduction

Use fresh caller-owned directories, free ports and the pinned image/client jar
in the receipt. The checked-in six node configurations are the actual final
configs; rewrite their candidate data/credential paths to the new RUN. The
fixture password is a synthetic local test value. Compile the fixture using JDK
25 and the released 4.4 jars. Build the broker/formatter from the receipt's
revision (the Java-only working diff does not affect the production binary):

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build --locked \
  -p krabka-broker --bin krabka-broker -p krabka-format --bin krabka-format
javac -cp "$LIBS/*" -d "$RUN/classes" tools/kafka44/DlqDeniedDestination.java
```

Format each candidate node with one common cluster ID and explicit
`--initial-controllers id@host:port:directory-id` entries matching the three
node IDs, controller ports and local directory IDs. The receipt records all
three exact formatter command arrays. Start with the matching TOML and
`--controller-listen-addr`; metrics/health were disabled. Before installing ACLs,
candidate bootstrap configs temporarily list both admin and ANONYMOUS as super
users; run `prepare`, cleanly stop all nodes, remove ANONYMOUS from super users,
and restart. No unstable feature switches were used.

For Kafka, run the pinned image with host networking, caller UID/GID and each
node's directory mounted at `/evidence`. Format at `4.4-IV2` and run
`kafka-server-start.sh /evidence/broker.properties`. The **controller-only**
listener must authenticate as superuser admin: a fully cold PLAINTEXT anonymous
controller quorum cannot initialize StandardAuthorizer before loading metadata.
The destination-producing interbroker listener remains PLAINTEXT/ANONYMOUS.
Initial bootstrap used ANONYMOUS superuser then `prepare`; the final scenario
was rerun under the retained controller-SASL configs with only admin superuser.

```sh
java -cp "$RUN/classes:$LIBS/*" DlqDeniedDestination AUTH PLAIN prepare rf3final 3 2
# Once bootstrap-only ANONYMOUS superuser is removed and nodes restarted:
java -cp "$RUN/classes:$LIBS/*" DlqDeniedDestination AUTH PLAIN run rf3final 3 2
java -cp "$RUN/classes:$LIBS/*" DlqDeniedDestination AUTH PLAIN verify rf3final 3 2
```

`prepare` installs topics, group config and ACLs but sends no source record.
`run` accepts already-created topics, verifies topology/config, proves denial,
then produces/rejects the source record. Do not reuse `run` after its successful
source append: the offset-zero assertion deliberately refuses reused state.
`verify` does not mutate topic/group config or ACLs and sends no source record.
Use a fresh suffix for each independent scenario. After pre-restart verify,
cleanly stop every owned node, decode each state partition, restart all three,
repeat verify and decode again. The raw logs retain broker-specific topic IDs,
epochs and offsets rather than normalizing byte-level differences into parity.

## Bounds and retained failures

This is completed-write clean restart, **not** a crash during ARCHIVING,
leader-kill schedule, network partition, power-loss or fsync guarantee. It does
not cover denied destination creation, recovery after ACL repair, concurrent
records or arbitrary partitions. Native controller traffic was PLAINTEXT with
an anonymous cluster ALLOW; Kafka controller traffic used SASL/admin. These are
not identical controller-security configurations; the measured anonymous
internal destination WRITE denial is the same.

`qualification_pass` stays false because this is a bounded scenario receipt,
not the complete qualification suite. Denial archives without a DLQ copy on
both peers, which is released behavior, not an exactly-once guarantee. Initial
volume UID, oracle bootstrap-auth and unmanaged-process launch failures remain
in the receipt's private artifact directory. Final six-node configurations,
raw results and SHA-256 identities are retained here. Owned brokers were stopped;
stopped containers/data remain available for inspection.
