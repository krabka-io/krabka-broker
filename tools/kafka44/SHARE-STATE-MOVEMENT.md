# Share state owner movement before rejection

Compile `ShareStateOwner.java` alongside
`qualification/kafka44-dlq/ShareBoundary.java` and `ShareFault.java` using the
pinned released Kafka4.4 client classpath. Provision three isolated combined
nodes with share state RF3/minISR2 and one state partition.

`ShareStateOwner BOOTSTRAP setup` creates `__share_group_state` explicitly on
replicas[3,1,2], minISR2. `read` prints its actual partition leader and requires
three replicas and at least two ISR members. Never infer state ownership from
source/DLQ leaders or from group coordinator lookup.

For each fresh `-Ddlq.run=RUN`, execute `ShareFault setup`, then run `reject`
with a nonexistent gate file. Setup asserts source leader1 and destination
leader2. After `SOURCE_ACQUIRED`, SIGKILL only state broker3. Poll
`ShareStateOwner read` until the actual state leader differs from3. Only then
create the rejection gate. The source and destination owners are not killed.
After the rejecting client exits, require `ShareFault resume` to return no
source record and run `ShareFault verify` with `-Ddlq.expected.count=1`.
Verification checks exact destination key/payload, one DLQ record and source
share lag0. Retain all actual owner rows, commit result, recovery and final rows.

This fault occurs after acquisition but **before REJECT/ARCHIVING**. It is not
mid-ARCHIVING state-owner loss, source-owner loss, destination-owner loss, or an
exactly-once archival guarantee for other schedules. Earlier append-before-state
confirmation tests retained real duplicate witnesses. Keep broad qualification
false, and retain terminal-prefix representation differences between brokers.
