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

The20261104 paired receipt retains a **failure**, not qualification: released
finished with one consumer-readable DLQ record, lag0 and no source redelivery.
Candidate recovery returned no source record but destination verification saw no
record within15seconds. Offline decoding later found **two valid DLQ records**
at offsets0/1 and terminal source state. Thus physical data loss is not proven;
consumer visibility and duplicate/retry behavior require investigation. Do not
turn the local append witness into a successful consumer-read assertion.

## Measured availability fix

Node3 is also the destination's follower (replicas[2,3]); this is a coupled
state-owner/DLQ-follower loss, **not a state-only fault**. Fresh baseline
instrumentation observed destination ISR[2], Fetch HW0/LSO0 and zero bytes,
then an actual `acks=all` timeout with local LEO1. The source logged its actual
30-second Produce timeout and retry; this second baseline eventually exposed two
consumer-readable copies. These observations establish unconfirmed-append retry,
not physical data loss or an isolation-setting explanation.

The ISR scan previously refreshed eligibility without reconsidering HW when no
new ISR proposal was needed. A recently caught-up removed follower could hold HW
under stale eligibility at ISR installation; fencing policy refresh then left
already-durable bytes behind the cached HW. The scan now recomputes and notifies
under the fresh policy, even on a no-proposal pass. It also handles time-only lag
expiry, preserves diskless WAL authority/minISR, and fences superseded epochs.

The fixed paired receipt `qualification/kafka44-share-state-movement-fixed-20261104.json`
uses fresh released/native clusters with the original coupled fault and unchanged
15-second verification: both finish with exactly one consumer-readable DLQ record,
lag0 and no source redelivery. Fixed native raw Fetch reaches HW1/LSO1 with389
record bytes, without the baseline timeout/retry. The earlier failed receipts
remain retained; this closes this observed schedule, not arbitrary duplicate/loss
or overall Kafka4.4 qualification.

HW refresh also requires the installed leader/epoch/ISR to match the metadata
row, and samples LEO while holding replica state so a queued refresh cannot
regress a concurrently published watermark. Pending ISR proposals retain their
existing commit/install path. The final local verification passes34 ISR tests,
including9 behavioral controls, full workspace/all-targets Clippy, formatting and
zero-repeat CPD. The receipt distinguishes readiness/notification checks from an
already-waiting live Produce; the owned wire rerun supplies actual Produce evidence.
