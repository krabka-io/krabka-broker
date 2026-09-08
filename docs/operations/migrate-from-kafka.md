# Migrate from Apache Kafka

How to move a running Apache Kafka cluster onto krabka with MirrorMaker 2, and
how to cut producers and consumers over when the mirror has caught up.

krabka serves the Kafka wire protocol, so MirrorMaker 2 treats it as an
ordinary Kafka cluster. You run the stock `connect-mirror-maker.sh` from a
Kafka release. You do not build anything, and you install nothing on either
broker.

The `mirror_maker2` suite in `crates/broker` runs this procedure end to end on
every change: MirrorMaker 2 out of `apache/kafka:4.3.1` mirrors a broker of
that release onto krabka, and the suite asserts the records, the headers,
MirrorMaker 2's own internal topics, the translated consumer-group position and
a topic config that changes after the mirror starts.

## What MirrorMaker 2 carries, and what it does not

| Carried | How |
| :--- | :--- |
| Records, with keys, values, headers and timestamps | The source connector reads the source topic and produces to the target. |
| Topic partition counts | The source connector creates the target topic with the source's partition count. |
| Topic configs, such as `retention.ms` | `sync.topic.configs` runs `describeConfigs` on the source and `incrementalAlterConfigs` on the target. |
| Consumer-group positions | `sync.group.offsets` translates each committed offset through the offset-syncs topic and writes it to the target. |
| Topic ACLs | `sync.topic.acls` runs `describeAcls` on the source and `createAcls` on the target. See [ACL sync](#acl-sync) below. |

| Not carried | What to do |
| :--- | :--- |
| Offsets as numbers | Target offsets differ from source offsets. Read a group's position from the target after the cutover, never from a source offset you wrote down. |
| Transactional state and producer ids | Restart transactional producers against krabka. An open transaction on the source does not move. |
| Delegation tokens, SCRAM credentials and quotas | Create them on krabka before the cutover. `kafka-configs` and `kafka-acls` do that. |
| Schema registry subjects | Copy them with the registry's own export. The `_schemas` topic is an ordinary topic, so MirrorMaker 2 can mirror it, but the registry then needs its own cutover. |

## Before you start

1. Deploy the krabka cluster and format its log directories. [deploy.md](deploy.md) has the steps.
2. Size the target for the source's retained bytes, not for its daily volume. MirrorMaker 2 replays every retained record. [capacity.md](capacity.md) has the numbers.
3. Create the principals, ACLs and quotas the migrated clients need. krabka does not receive them from the source cluster unless you turn ACL sync on and give the source an authorizer.
4. Choose the two cluster aliases. The alias of the source cluster becomes the prefix of every mirrored topic name, so `orders` on a source aliased `prod` arrives as `prod.orders`.

## Configure MirrorMaker 2

Write one properties file. This example mirrors a cluster aliased `prod` onto a
krabka cluster aliased `krabka`.

```properties
clusters = prod, krabka
prod.bootstrap.servers = kafka-1.example.com:9092,kafka-2.example.com:9092
krabka.bootstrap.servers = krabka-1.example.com:9092,krabka-2.example.com:9092

prod->krabka.enabled = true
prod->krabka.topics = .*
prod->krabka.groups = .*

replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 3

sync.topic.configs.enabled = true
sync.group.offsets.enabled = true
sync.group.offsets.interval.seconds = 10
emit.checkpoints.interval.seconds = 10
```

Start it with the stock script:

```bash
/opt/kafka/bin/connect-mirror-maker.sh mm2.properties
```

Three settings decide how the cutover behaves.

- `sync.group.offsets.enabled` is `false` by default. Set it to `true`. Without
  it MirrorMaker 2 writes checkpoints but never commits a translated position
  on krabka, and every migrated consumer starts from the beginning or from the
  end.
- `offset.lag.max` is `100` by default. MirrorMaker 2 emits one offset sync per
  that many records, and it translates a position with no exact sync
  conservatively. A conservative translation replays records. It never skips
  them. Lower the value to reduce the replay, and accept the extra writes to
  the offset-syncs topic.
- `offset-syncs.topic.location` is `source` by default, so the offset-syncs
  topic lives on the Kafka cluster you are leaving. Set it to `target` if you
  plan to shut the source down before the last consumer moves.

## Watch the mirror catch up

MirrorMaker 2 creates three internal topics for the flow. Each one has a single
partition and `cleanup.policy=compact`.

| Topic | Cluster | What it holds |
| :--- | :--- | :--- |
| `heartbeats` | Target | One record per heartbeat interval, which proves the flow is alive. |
| `<source alias>.checkpoints.internal` | Target | The translated position of every mirrored consumer group. |
| `mm2-offset-syncs.<alias>.internal` | Source, or target when you move it | The upstream-to-downstream offset pairs that translation reads. |

Read the lag of MirrorMaker 2's own consumers on the source cluster:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka-1.example.com:9092 \
  --describe --group prod-mm2
```

The mirror has caught up when that lag stays near zero and the target topic's
end offsets stop moving faster than the source's.

## Cut the consumers over

Do one consumer group at a time.

1. Stop the consumer group on the source cluster. Wait for it to leave the
   group.
2. Wait one `sync.group.offsets.interval.seconds`. MirrorMaker 2 does not move
   a group's position on the target while that group has a live member there,
   so the last translated write lands only after the group is quiet.
3. Read the group's position on krabka and confirm it names the mirrored topic:

   ```bash
   kafka-consumer-groups.sh --bootstrap-server krabka-1.example.com:9092 \
     --describe --group orders-riders
   ```

4. Start the consumer group against krabka. Point it at the mirrored topic
   name, which carries the source alias as a prefix.
5. Confirm the group reads forward. A group that starts at the wrong position
   reports no error, so check the lag rather than the log.

Set `replication.policy.class` to
`org.apache.kafka.connect.mirror.IdentityReplicationPolicy` if you cannot
change the topic names your consumers subscribe to. The mirrored topic then
keeps the source name. Choose this before you start MirrorMaker 2. A change of
policy after the first records arrive creates a second set of topics.

## Cut the producers over

Move the producers after the consumers, and move them last.

1. Stop the producers on the source cluster.
2. Wait for the mirror lag to reach zero one last time.
3. Start the producers against krabka.
4. Stop MirrorMaker 2.

Producers that move before their consumers split the topic across two clusters.
The order above keeps one writer at a time.

## Clean up

Delete the mirrored prefix from the topic names when nothing reads the old
names, and delete MirrorMaker 2's internal topics:

```bash
kafka-topics.sh --bootstrap-server krabka-1.example.com:9092 --delete \
  --topic 'heartbeats|prod\.checkpoints\.internal|mm2-.*\.internal'
```

## Roll back

Keep the source cluster running until the last consumer group has read past its
cutover position on krabka. A rollback is a second MirrorMaker 2 flow in the
other direction, so add `krabka->prod.enabled = true` to the same properties
file. Records that MirrorMaker 2 already carried do not travel back, because
`DefaultReplicationPolicy` refuses to replicate a topic whose name already
carries the other cluster's prefix.

## ACL sync

`sync.topic.acls.enabled` is `true` by default, and it works in one direction
only. MirrorMaker 2 reads the ACLs of the source cluster and writes them to the
target.

krabka runs the `allow_all` authorizer by default, which is not a decision
point. Under that authorizer krabka answers `DescribeAcls`, `CreateAcls` and
`DeleteAcls` with `SECURITY_DISABLED` (54) and the message
`No Authorizer is configured on the broker`. Apache Kafka answers the same way
when `authorizer.class.name` is unset, so the two brokers agree.

What you see depends on the source cluster.

- The source has no authorizer. MirrorMaker 2 catches the source's own
  `SecurityDisabledException`, logs that no ACL policy is enabled, and skips
  the sync. Nothing reaches krabka.
- The source has an authorizer. MirrorMaker 2 reads the source ACLs and calls
  `createAcls` on krabka. krabka refuses each one with `SECURITY_DISABLED`, and
  MirrorMaker 2 logs `Could not sync ACL of topic <name>` and continues.
  Replication is not affected.

Configure an authorizer on krabka before the cutover if the migrated clients
need ACLs, and create the ACLs with `kafka-acls`. Set
`sync.topic.acls.enabled = false` to keep the warning out of the MirrorMaker 2
log.

## Related documents

- [Ecosystem support](ecosystem-support.md): which tool classes work against
  krabka over the protocol, and which need JMX or a broker-side plugin.
- [Deploy](deploy.md): how to bring the target cluster up.
- [Backup and restore](backup-restore.md): what to capture once the migration
  is complete.
