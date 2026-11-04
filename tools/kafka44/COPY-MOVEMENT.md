# Delayed copy across leader loss

`CopyMovementProbe.java` is an RF3 split-process extension of
`RemoteRecoveryProbe.java`, using released Kafka 4.4 client codecs. Compile it
alongside `CopyProbe.java` with the pinned released client libraries.

Modes (all require a caller-owned isolated tiered cluster and shared archive):

```
CopyMovementProbe seed BOOTSTRAP TOPIC ARCHIVE
CopyMovementProbe leader BOOTSTRAP TOPIC
CopyMovementProbe read BOOTSTRAP TOPIC LOCAL_OFFSET0_PATH ARCHIVE
```

Seed creates one RF3 partition with a 30-second copy-age threshold, writes48
records with exact distinct keys/64KiB payloads, validates acknowledged offsets,
and requires no early archive files. Discover the actual leader, SIGKILL only
that owned process, then discover a different leader through a surviving broker.
Wait for natural local offset0 eviction on that new leader. Read requires the
specified local offset0 file to be absent and topic archive files to exist;
it checks all48 offsets, keys and every payload byte through a Kafka consumer.
Retain raw leader rows, archive inventory and the complete provisioning commands.

The fault is **before upload eligibility**, not during upload I/O. Actual
mid-upload interruption still needs a deterministic backend cut: the released
`RecordingStorage.copyLogSegmentData` and native
`crates/remote-storage/src/local.rs` both perform synchronous artifact copies.
A permission-denied archive test fails before copying; an arbitrary timed kill
cannot prove that it interrupted an artifact. A future cut should signal after
partial log artifact persistence, block completion, kill the owner, then validate
metadata recovery, retry and remote reads. That backend seam must be equivalent
on both implementations before claiming paired mid-upload coverage.

RF3 leader-loss evidence does not establish arbitrary fault schedules, concurrent
config mutation, mid-upload atomicity, denied-copy authorization or full Kafka4.4
qualification. Keep `qualification_pass=false`.
