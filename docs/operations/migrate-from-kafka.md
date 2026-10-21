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
| Some topic ACLs | `sync.topic.acls` runs `describeAcls` on the source and `createAcls` on the target. It copies literal topic ACLs only, and it drops every `WRITE` grant. See [ACL sync](#acl-sync) below. |

| Not carried | What to do |
| :--- | :--- |
| Offsets as numbers | Target offsets differ from source offsets. Read a group's position from the target after the cutover, never from a source offset you wrote down. |
| Transactional state and producer ids | Restart transactional producers against krabka. An open transaction on the source does not move. |
| Delegation tokens, SCRAM credentials and quotas | Create them on krabka before the cutover. `kafka-configs` and `kafka-acls` do that. |
| A schema registry's own cutover | MirrorMaker 2 mirrors the `_schemas` topic like any other topic. Moving the registry onto it is a separate step. See [Cut the schema registry over](#cut-the-schema-registry-over). |

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

MirrorMaker 2 looks for new consumer groups only every
`refresh.groups.interval.seconds`, 600 by default. A group that first commits
after MirrorMaker 2 starts gets no translated position on krabka until the
next scan. Lower the interval, or wait one interval, before you cut such a
group over. `refresh.topics.interval.seconds` (600) does the same for new
topics.

## Create the target topics first

MirrorMaker 2 creates each target topic with the source's partition count and
the source's configuration. When the target needs a different configuration,
create the topic on krabka before MirrorMaker 2 starts. A topic that tiers on
krabka is the common case: the source may not tier at all.

```bash
kafka-topics.sh --bootstrap-server krabka-1.example.com:9092 --create \
  --topic prod.orders --partitions 12 --replication-factor 3 \
  --config remote.storage.enable=true --config local.retention.ms=21600000
```

Use the mirrored name, `prod.orders` here, and the source's partition count.
A compacted topic cannot tier. krabka refuses `remote.storage.enable` with
`cleanup.policy=compact`, as Kafka does.

`sync.topic.configs` then keeps the target in step with the source, every
`sync.topic.configs.interval.seconds` (600 by default). Each sync does two
things to a key that `config.properties.exclude` does not name:

- It sets every key that the source sets, over the value on the target.
- It removes every key that only the target sets. The topic falls back to the
  broker default for that key.

So a sync undoes every target-only setting, `remote.storage.enable` and
`local.retention.ms` included. To keep a key, add it to the exclude list of
the flow. The list replaces the default list, so repeat the defaults:

```properties
prod->krabka.config.properties.exclude = follower.replication.throttled.replicas, \
  leader.replication.throttled.replicas, message.timestamp.difference.max.ms, \
  log.message.timestamp.before.max.ms, log.message.timestamp.after.max.ms, \
  message.timestamp.type, unclean.leader.election.enable, min.insync.replicas, \
  remote.storage.enable, local.retention.ms, local.retention.bytes
```

Set it on the flow, as `prod->krabka.config.properties.exclude`. With MirrorMaker
2 from Kafka 4.3.1, the same key without the flow prefix did not reach the
source connector. Check a topic with `kafka-configs --describe` after the
first sync interval.

## Secured clusters

Each cluster alias takes its own client settings, with the alias as a prefix.
This example connects to krabka over SASL/SCRAM and TLS:

```properties
krabka.security.protocol = SASL_SSL
krabka.sasl.mechanism = SCRAM-SHA-512
krabka.sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule \
  required username="mm2" password="<password>";
krabka.ssl.truststore.type = PEM
krabka.ssl.truststore.location = /etc/mm2/krabka-ca.pem
```

For mutual TLS, set `krabka.security.protocol = SSL` and the keystore settings
of the Kafka client. The source alias takes the same keys with its own prefix.

On a krabka cluster with an authorizer, the MirrorMaker 2 principal needs the
ACLs below. The table names the source alias `prod`. MirrorMaker 2 from Kafka
4.3.1 ran against krabka with exactly these and was denied nothing.

| Resource | Operations | Why |
| :--- | :--- | :--- |
| Topic prefix `prod.` | `Create`, `Write`, `Read`, `Describe`, `DescribeConfigs`, `AlterConfigs` | The mirrored topics, `prod.checkpoints.internal` and `prod.heartbeats`. `AlterConfigs` is for the topic configuration sync. `Read` is for the checkpoints topic and for the group positions. |
| Topic prefix `mm2-` | `Create`, `Write`, `Read`, `Describe`, `DescribeConfigs` | The worker's own `mm2-configs`, `mm2-offsets` and `mm2-status` topics, and `mm2-offset-syncs` when it is on the target. |
| Topic `heartbeats` | `Create`, `Write`, `Describe` | The heartbeat topic. |
| Group `prod-mm2` | `Read`, `Describe` | The worker group, `<source alias>-mm2`. |
| Each group in `prod->krabka.groups` | `Read`, `Describe` | The translated group positions. |
| Cluster | `Alter` | ACL sync only. Without it, krabka refuses each `createAcls`, and MirrorMaker 2 logs `Could not sync ACL of topic`. |

Without the group ACLs, MirrorMaker 2 logs `Unable to sync offsets for consumer
group` and no position reaches krabka. A consumer that starts there reads from
its `auto.offset.reset` position, so grant them before the cutover. The source
cluster needs its own grants for the same principal. Apache Kafka's
documentation for MirrorMaker 2 describes them.

## Transactional sources and Kafka Streams

MirrorMaker 2 reads the source with `read_uncommitted` by default. It then
copies the records of aborted transactions, and on krabka they are ordinary
records that every reader sees. In a test with MirrorMaker 2 from Kafka 4.3.1,
a source topic held 2,428 records, and 2,412 of them were committed. The
default mirror wrote all 2,428 to krabka.

Set the isolation level of the source alias's consumer when a source topic
receives transactions:

```properties
prod.consumer.isolation.level = read_committed
```

With that setting, the same test wrote exactly the 2,412 committed records.

MirrorMaker 2 does not copy transactions. The mirrored records carry no
transaction markers, and krabka holds no transaction state for them. Before
the producer cutover, stop the transactional producers on the source and wait
until `kafka-transactions list` on the source shows no ongoing transaction.
Then start the producers against krabka, where they begin new transactions.

A Kafka Streams application keeps its state in changelog and repartition
topics named after its `application.id`. Do not mirror them. Name the
application's input topics in `prod->krabka.topics`, and leave its own group
out of `prod->krabka.groups`. Start the application on krabka with empty local
state and `auto.offset.reset=earliest`. It rebuilds its state from the
mirrored inputs, so their retention has to cover every record that the state
needs.

## Watch the mirror catch up

MirrorMaker 2 creates three internal topics for the flow. Each one has a single
partition and `cleanup.policy=compact`.

| Topic | Cluster | What it holds |
| :--- | :--- | :--- |
| `heartbeats` | Target | One record per heartbeat interval, which proves the flow is alive. |
| `<source alias>.checkpoints.internal` | Target | The translated position of every mirrored consumer group. |
| `mm2-offset-syncs.<alias>.internal` | Source, or target when you move it | The upstream-to-downstream offset pairs that translation reads. |

MirrorMaker 2's source consumers assign their partitions directly and commit
no group offsets to the source cluster. They keep their position in the
`mm2-offsets.<source alias>.internal` topic on the target. So
`kafka-consumer-groups --describe --group prod-mm2` on the source answers
`GroupIdNotFoundException`. Use MirrorMaker 2's own metrics instead. It
exports `record-age-ms` and `replication-latency-ms` per partition over JMX,
under `kafka.connect.mirror:type=MirrorSourceConnector`. The mirror has caught
up when both stay near zero.

To check one topic without JMX, read the earliest and the end offsets on both
sides:

```bash
kafka-get-offsets.sh --bootstrap-server kafka-1.example.com:9092 --topic orders --time -2
kafka-get-offsets.sh --bootstrap-server kafka-1.example.com:9092 --topic orders
kafka-get-offsets.sh --bootstrap-server krabka-1.example.com:9092 --topic prod.orders --time -2
kafka-get-offsets.sh --bootstrap-server krabka-1.example.com:9092 --topic prod.orders
```

The mirror starts at offset 0, and the source keeps its own numbering after
retention removes old segments. So compare the end offset minus the earliest
offset on each side, not the end offsets. The mirror has caught up when that
difference stops changing after the source producers stop. The two
differences are equal only when the source partition holds no transaction
markers and no compacted gaps.

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
3. Start the producers against krabka. With `DefaultReplicationPolicy`, they
   write the source's topic name, `orders`, not the mirrored name
   `prod.orders`. Create `orders` on krabka first, with the configuration you
   want.
4. Stop MirrorMaker 2.

Producers that move before their consumers split the topic across two clusters.
The order above keeps one writer at a time.

The consumers then read two topics: `prod.orders` holds the records from
before the cutover, and `orders` holds the records after it. Subscribe each
consumer to both, for example with the pattern `(prod\.)?orders`, and give it
`auto.offset.reset=earliest` so that it reads `orders` from its start. Order
by key holds within each topic, not across the two. A consumer that needs one
order for each key across the cutover needs
`IdentityReplicationPolicy`, which keeps one topic name.

Producers that write the mirrored name `prod.orders` on krabka cannot be
rolled back: the reverse flow in [Roll back](#roll-back) never carries that
topic.

## Cut the schema registry over

A Confluent Schema Registry keeps its schemas in the compacted `_schemas`
topic, and every record names its schema by id. Mirror that topic, run a
second registry on krabka that only reads it, and make that registry writable
at the producer cutover. The schema ids then stay the same on both clusters.
This procedure was checked with the Confluent Schema Registry 7.4 image that
krabka's registry tests pin, and MirrorMaker 2 from Kafka 4.3.1.

1. Add `_schemas` to `prod->krabka.topics`. It arrives as `prod._schemas`,
   compacted, with one partition.
2. Start the target registry against krabka, with the mirrored topic and no
   right to lead:

   ```properties
   kafkastore.bootstrap.servers=SASL_SSL://krabka-1.example.com:9094
   kafkastore.topic=prod._schemas
   leader.eligibility=false
   ```

   It serves every read. It answers a write with error `50004`, `Leader not
   known`, so it cannot give out an id that the source registry gives out too.
3. Check the two registries against each other. Compare `GET /subjects`,
   `GET /schemas/ids/<id>` for every id, and `GET /config/<subject>` for every
   subject with its own compatibility level. Do not compare end offsets: the
   target registry writes its own marker record into the topic when it
   starts. Consumers that already read from krabka can use this registry.
4. At the producer cutover, freeze schema changes on the source with
   `PUT /mode` and the body `{"mode": "READONLY"}`. A producer can still look
   up and re-register a schema that exists. A new schema is refused with
   error `42205`.
5. Wait until the target registry serves the newest id of the source. Then
   remove `_schemas` from `prod->krabka.topics` and restart MirrorMaker 2, or
   stop it, so that the topic has one writer.
6. Restart the target registry without `leader.eligibility=false`. It now
   leads, and it gives out ids above the highest id in the topic.
7. The `READONLY` mode of step 4 was mirrored too, so the target registry is
   read-only as well. Leave it read-only until the rollback window closes.
   Then send `{"mode": "READWRITE"}` to its `PUT /mode`.

With `IdentityReplicationPolicy` the topic keeps the name `_schemas`, and the
target registry needs no `kafkastore.topic` setting.

A schema that is registered only on krabka cannot roll back. The reverse
flow in [Roll back](#roll-back) never carries `prod._schemas`, and a schema
registered again on the source can get another id. The read-only mode of
step 7 keeps every id on both registries until the rollback window closes.

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
file.

The reverse flow carries what the producers wrote on krabka, and nothing else.
With MirrorMaker 2 from Kafka 4.3.1 and `DefaultReplicationPolicy`:

- Records that producers wrote to `orders` on krabka arrive on the source as
  `krabka.orders`. Subscribe the source consumers to `(krabka\.)?orders`.
- Records in `prod.orders` on krabka never travel back, whoever wrote them.
  The policy refuses a topic whose name already carries the other cluster's
  prefix. That is what keeps the two flows from looping.

With `IdentityReplicationPolicy`, a topic has the same name on both clusters,
and the krabka copy also holds every record that the forward flow mirrored. A
new reverse flow reads from the earliest offset by default, so it would append
that whole history to the source topic again. Start the reverse flow at the
producer cutover, before any producer writes to krabka, and stop the forward
flow first so that the two cannot loop:

```properties
krabka->prod.enabled = true
krabka.consumer.auto.offset.reset = latest
```

A new flow with that setting starts at the end of each topic. In a test with
MirrorMaker 2 from Kafka 4.3.1 it copied the 12 records written after it
started and none of the 30 written before.

## ACL sync

`sync.topic.acls.enabled` is `true` by default, and it works in one direction
only. MirrorMaker 2 reads the ACLs of the source cluster and writes them to the
target.

krabka runs the `allow_all` authorizer by default, which is not a decision
point. Under that authorizer krabka answers `DescribeAcls`, `CreateAcls` and
`DeleteAcls` with `SECURITY_DISABLED` (54). The message is
`No Authorizer is configured on the broker` for `DescribeAcls` and
`No Authorizer is configured.` for `CreateAcls` and `DeleteAcls`. Apache Kafka
answers the same way when `authorizer.class.name` is unset, so the two brokers
agree.

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

With an authorizer on both clusters, the sync copies only part of the source
ACLs. With MirrorMaker 2 from Kafka 4.3.1 against krabka:

| Source ACL | On krabka |
| :--- | :--- |
| Literal topic ACL, `ALLOW` `READ` or `DESCRIBE` | Copied to the mirrored topic name. |
| Literal topic ACL, `DENY` | Copied. |
| Literal topic ACL, `ALLOW` `ALL` | Copied as `ALLOW` `READ`. |
| Literal topic ACL, `ALLOW` `WRITE` | Not copied. |
| Prefixed topic ACL | Not copied. |
| Group ACL | Not copied. |

The sync gives readers of the mirrored topics their access, and nothing more.
Create the producer, group and transactional-id ACLs with `kafka-acls`, under
the topic names that the clients use after the cutover. Then compare the
output of `kafka-acls --list` on both clusters. The sync runs every
`sync.topic.acls.interval.seconds`, 600 by default, and it needs `Alter` on
the cluster for the MirrorMaker 2 principal.

## Related documents

- [Ecosystem support](ecosystem-support.md): which tool classes work against
  krabka over the protocol, and which need JMX or a broker-side plugin.
- [Deploy](deploy.md): how to bring the target cluster up.
- [Backup and restore](backup-restore.md): what to capture once the migration
  is complete.
