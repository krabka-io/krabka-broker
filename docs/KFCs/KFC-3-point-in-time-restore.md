# KFC-3: Point-in-Time Restore from a Tiered-Storage Archive

An offline tool that rebuilds a bootable krabka cluster from a KIP-405 archive, replayed to a bound and verified as it rehydrates.

## Status

**Adopted.** The implementation is the `krabka-restore` crate and the `krabka-restore` binary, merged in [#4](https://github.com/krabka-io/krabka-broker/pull/4). This document lands on branch `claude/point-in-time-restore-cli-9opk3c`.

No KIP defines restore, so this document is the specification for it. [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage) defines the archive that the restore reads, and the sections below state where the restore's behaviour sits next to it.

## Motivation

KIP-405 gave Kafka a tiered archive, and it gave no path back from one.

A broker with tiered storage copies each closed segment to object storage. The `.log` goes up with its offset index, its time index, its producer-state snapshot, and its leader-epoch checkpoint beside it, and with an aborted-transaction index when the partition has one. The archive holds the whole history of a partition, in the broker's own on-disk formats. A consumer can read that history back through the cluster that wrote it. Nothing reads it into a new cluster.

The gap shows up on the worst day. Four incidents put an operator in front of it: a corruption that a producer wrote and replicated, a ransomware event that reached the live data directories, a bulk change applied to the wrong topic, and a credential that an attacker used to write. Each one gives the operator the same two facts. The data was good at some point, and that point is behind them. What they need is a cluster that holds the log up to that point and nothing after it.

Every shop that needs this builds it by hand. The runbook lists the bucket, decodes the object keys back into topics and partitions, fetches five artifacts for each segment, places them into a data directory under the names a broker expects, formats cluster metadata around them, and seeds the topic records so the broker knows the partitions exist. Each step has a silent failure mode. The worst one is a restore that boots and serves a log whose offsets moved.

An event-sourced system makes that last failure fatal, because an offset in Kafka is a reference that other systems hold. A consumer group holds one per partition. A connector holds one. A downstream projection holds one as its checkpoint, and a lakehouse table holds one per file it ingested. A restore that renumbers records leaves every one of those references pointing at a different record than it did before, and it reports success while it does so.

The archive key layout, the record batch codec and its CRC, the index parsers, the partition directory layout, and the cluster formatter all live in this repository. A restore built anywhere else forks all five and drifts from them. That is why the tool ships from the broker's repository, and it is the reason the restore work started by making those seams public rather than by copying them.

The tool is not a broker feature, for two reasons. It has to run when the cluster does not, because a cluster whose data directories were encrypted or corrupted is exactly the cluster that cannot serve an admin request. It also has to sit outside the credential that caused the incident. An attacker who holds a cluster's admin credential must not thereby hold the restore.

A restore needs a KFC by the test in the [README](README.md): a stock Kafka client can tell a restored cluster from an ordinary one, and no KIP explains the difference. A consumer sees offsets that skip, batches that hold no records, and batch bytes that no producer wrote. The [Compatibility](#compatibility-deprecation-and-migration-plan) section states that contract in full.

## Public Interfaces

The feature adds one command. It adds no API key, no error code, no broker config, no topic config, and no metric, because it does not run inside a broker.

### The Command

`krabka restore` is the operator's spelling. The binary is named `krabka-restore`, because the `krabka` CLI resolves an unknown subcommand to `krabka-<name>` on `PATH`, the way git resolves `git foo` to `git-foo`. That spelling deliberately breaks the `krabka-` prefix the rest of the workspace uses, and it is the whole dispatch mechanism.

The crate is a library as well as a binary. `krabka_restore::restore` returns the structured report and the structured error, and only `run` and the binary turn an error into an exit code. A runbook that wraps the restore gets an exit code. A tool that embeds it keeps the error.

### Where the Archive Is

| Flag | Meaning |
| :--- | :--- |
| `--archive-local DIR` | Read the archive from a local directory tree. |
| `--archive-s3-bucket BUCKET` | Read from S3 or an S3-compatible store. `--archive-s3-region`, `--archive-s3-endpoint`, `--archive-s3-access-key-id`, `--archive-s3-secret-access-key`, and `--archive-s3-allow-http` refine it. |
| `--archive-gcs-bucket BUCKET` | Read from Google Cloud Storage. `--archive-gcs-service-account-path`, `--archive-gcs-endpoint`, and `--archive-gcs-allow-http` refine it. |
| `--archive-prefix PREFIX` | Key prefix inside the archive, for a bucket that holds more than the tiered tree. It applies to every backend. |
| `--rlmm-snapshot PATH` | A broker's `<log.dir>/remote-log-metadata/snapshot`. |

Exactly one backend is selected. A sub-flag of a backend the operator did not select is an error rather than a value the tool ignores, and the message names both flags. Credentials are optional on both cloud backends, so an operator can restore under an instance role or under Workload Identity and never put a secret on a command line.

### The Cluster the Restore Writes

`--log-dir`, `--cluster-id`, `--node-id`, `--standalone`, `--initial-controllers`, `--no-initial-controllers`, and `--controller-listener` carry the names `krabka format` gives them, and they are forwarded to the formatter unchanged. A restore formats the cluster it writes into, so an operator does not have to learn two spellings for one concept.

`--log-dir` must name an empty directory or a path that does not exist. `--node-id` is required, because every restored partition names the target node as its leader and its sole replica, and a restore must not default that identity to node 0. `--cluster-id` is generated when it is absent, and the report always states the value that was used.

### Selection and Bounds

| Flag | Meaning |
| :--- | :--- |
| `--topic NAME` | Restore this topic. Repeatable. Every topic in the archive is restored when the flag is absent. |
| `--to-offset TOPIC:PARTITION=N` | Keep offsets at or below `N` in one partition. `N` is inclusive, and the cut lands on a batch boundary. Repeatable, once per partition. |
| `--to-timestamp RFC3339\|EPOCH_MS` | Keep records whose timestamp is below this instant. It applies to every partition. |
| `--exclude-key REGEX` | Drop records whose key matches. Repeatable. |
| `--exclude-header NAME=REGEX` | Drop records that carry a header of this name whose value matches. Repeatable. |
| `--exclude-producer-id ID` | Drop records written by this producer id. Repeatable. |
| `--exclude-offset TOPIC:PARTITION=A..B` | Drop an offset range in one partition. `B` is exclusive, and `A..=B` includes it. Repeatable. |

Every exclude predicate is OR-combined. A record is dropped when any one of them matches it.

One check that belongs here does not run yet, and this note is here because nothing in the repository will report it. A bound that names a topic partition the archive does not hold is not rejected: the bound simply never applies, and the restore reports success over the partitions it did find. So an operator who mistypes a partition index gets a restore that is not the one they asked for. The error is defined for it and no stage raises it, so closing the gap is a check in the discover stage against the inventory, and not a change to this design.

Two flag combinations are rejected before the archive is read. A partition named twice by `--to-offset` is an error, because the second bound can only contradict the first. A partition whose `--exclude-offset` ranges cover every offset up to its `--to-offset` bound is an error, because the operator wrote a bound that keeps nothing. A bound that names a topic which `--topic` does not select is an error for the same reason.

### Behaviour

`--dry-run` runs discovery, verification, and the bound, and writes no partition data. It does write the target's cluster metadata, so it still needs an empty `--log-dir`, and it produces the same report a real run produces with `dry_run` set. An operator who wants to see what a bound will do runs it and reads the counts.

`--continue-on-corrupt` turns a segment that fails verification into a skipped segment in the report instead of an error. `--report text|json` selects the rendering.

### Exit Codes

| Code | Meaning |
| :--- | :--- |
| 0 | Success. |
| 2 | A bad argument, or a bound the other flags contradict. |
| 3 | The target log directory is not empty. |
| 4 | The archive cannot be read, or holds no selected topic. |
| 5 | An integrity failure: a checksum mismatch, a truncated segment, a torn copy, or a metadata disagreement. |
| 6 | A materialization failure: the formatter rejected the target, or the target could not be written. |

The split between 5 and 6 is the split a runbook branches on. Code 5 says the archive is not what it claims to be, and a second run against the same archive fails the same way. Code 6 says the target is wrong, and a different target may work.

### The Report

The report names the target, the cluster id, and each restored partition with its topic id, the segments it wrote, the offsets they now span, and the counts of batches and records kept, rewritten, emptied, and dropped. It lists every segment that verification rejected, with the reason.

The text rendering is for a person during an incident, and it sums the per-segment counters up to the partition. The JSON rendering is for a runbook that has to assert on the outcome, and it keeps the per-segment detail. Both render one model, so they cannot disagree.

## Proposed Changes

### Five Stages

A restore runs five stages in a fixed order: discover, verify, bound, materialize, report. Each one is a module with its own data model, and the driver is a short function that calls them in sequence.

The order carries one deliberate exception. The target directory is checked before the archive is opened. An operator who pointed `--log-dir` at a live data directory learns it in a second, and not after a full download of a terabyte-scale archive.

### Discover Reports Presence, and Judges Nothing

The discover stage lists the archive under the operator's prefix, decodes every key with the KIP-405 archive key codec, and groups the result into one inventory entry per archived segment. The codec is the same one the broker's copy path writes with, read backwards. Making it read backwards was most of the work: the key builders were private and had no inverse, so a bucket could not be turned back into an inventory.

The stage keeps every key it cannot attribute, rather than discarding it. An unrecognized key is the first sign that `--archive-prefix` points at the wrong tree, and a scan that silently drops what it does not understand reports an empty archive instead of a misconfiguration.

It decides nothing about completeness or integrity. A segment with four of its five artifacts is an entry with one field absent, and the verify stage is what calls that a torn copy. One stage that reports facts and a second that judges them is easier to reason about than one stage that does both, and the judgement then sits next to the bytes it judges.

### The Snapshot Decides Lifecycle, and a Disagreement Stops the Restore

Object keys alone cannot say whether a segment is live. A segment that retention released may still have bytes in the bucket, because deletion in object storage is not instant and the metadata that ordered it lives elsewhere. Without more information a restore includes it, and the restored cluster then holds records the old cluster had already dropped.

`--rlmm-snapshot` closes that. The snapshot is a broker's own remote-log-metadata state, and it is authoritative about lifecycle. The restore reconciles the bucket scan against it and treats the two states differently. A segment the snapshot marks `DeleteSegmentStarted` is dropped without comment, because deletion is in flight and leftover bytes are expected. A segment the snapshot marks `DeleteSegmentFinished` that still has bytes is a genuine disagreement, and so is a segment the scan found that the snapshot never mentions, and a segment the snapshot calls live that the scan did not find.

A disagreement stops the restore and names both sides. This is the one place where the design refuses to choose. The whole purpose of the tool is to answer "what did the log hold at time T", and two sources that answer differently mean the operator does not yet know. A tool that picked a winner would hide the one fact that changes what they should do next.

The flag is optional, because the snapshot lives in a data directory that an incident may have destroyed. Without it the restore runs on the object keys alone, and the [limits](#what-the-archive-holds-bounds-the-restore) apply.

### Verify Trusts the Batch Headers and Nothing Else

The verify stage fetches a segment's artifacts and walks the `.log` batch by batch. It checks the framing and the CRC of each batch without decoding records. A batch whose declared length overruns the object is a truncated segment. A batch whose CRC disagrees with its body is a checksum mismatch, reported with the object key and the byte position. A segment that carries a log but no time index is a torn copy, reported with the artifact that is absent.

Every fetch is capped. A `.log` is capped at 8 GiB, well above Kafka's 1 GiB default for `segment.bytes`, and each sidecar is capped by what its own format can produce. The caps exist because an offline tool reads objects that an attacker may have replaced, and a length field in a corrupt object must not become an allocation.

From the batch headers the stage derives the two facts the rest of the pipeline needs: the segment's true end offset and its highest record timestamp. Those two values also sit in the archive's own metadata, and the restore does not read them from there. A restore runs because something is wrong, and metadata about the bytes is not evidence about the bytes. The headers that the CRC covers are.

The `.log` is fetched exactly once. The verified bytes are handed forward, and the materialize stage writes from them rather than from a second download.

### What Verification Does Not Prove

A CRC proves that bytes did not rot. It does not prove that an attacker did not rewrite a segment and recompute the CRC over the result, and the motivation for this tool names two incidents where an attacker held a credential.

The archive answers that separately. `krabka-remote-storage`'s WORM mode signs a per-segment manifest, hash-chains it per partition, and `krabka-worm-verify` checks the chain and the signatures with read-only credentials against a key the auditor holds. Verification of that kind belongs to an auditor who does not hold the writer's keys, and it is already a tool of its own.

`krabka restore` does not consult those manifests today. A WORM archive's `.manifest` objects reach the discover stage as unrecognized keys, and the report does not render them. An operator restoring after a credential compromise should run `krabka-worm-verify` against the archive first, and treat the restore's own checks as an integrity check and not as an authenticity check. Folding the chain check into the verify stage, behind a flag that names a trusted key, is the obvious follow-up, and it is a change to this document rather than a new one.

### Offsets Are the Contract

A restore preserves the absolute offset of every record it keeps.

This is the invariant the rest of the design serves, and the [Motivation](#motivation) says why: an offset is a reference that systems outside the cluster hold. Kafka's own log cleaner makes the same promise. Compaction removes records and leaves the offsets of the survivors alone, and a consumer that reads a compacted partition sees gaps and steps over them. The restore drops records for a different reason, and it follows the same rule, so a restored partition looks to a client like a partition that was compacted.

Three mechanisms carry it. A batch keeps its archived `base_offset`. A batch that must be re-encoded keeps that base offset and recomputes `last_offset_delta` from the records that survive, so the gap a dropped record leaves is invisible to the log format. A batch whose records are all dropped is still written.

### An Emptied Batch Still Claims Its Offsets

The third case looks like waste, and it is not optional.

The target log accepts an append only at its current end offset. Both `Log::append_at` and `Log::append_verbatim_at` require the offset passed in to equal `log_end_offset()`, which is how the log guarantees a contiguous partition. A batch that is skipped entirely leaves the target's end offset behind the archive's, and every batch archived after it in that partition is then unappendable. The restore would fail, or it would renumber, and renumbering is the failure this design exists to prevent.

So an emptied batch becomes a bare header with zero records, carrying the archived `base_offset` and `last_offset_delta` unchanged. krabka's log cleaner already does this on its `RETAIN_EMPTY` path, for the same reason. The report counts these separately from the batches it rewrote, because "records dropped" alone does not tell an operator who ran `--exclude-key` whether the pattern matched anything.

### Two Bounds, Two Shapes

`--to-offset` truncates. The restore stops walking a partition at the first batch whose base offset is past the bound, and it never opens a segment that starts past it. The restored partition ends at the last batch that starts at or before the bound.

`--to-timestamp` filters. Every record whose timestamp is at or after the bound is dropped, wherever it sits, and a batch that holds only such records becomes the bare header above. The partition's offset range still reaches the archive's end.

The difference follows from the field. Offsets in a partition are monotonic by construction, so a bound on them names a suffix and cutting that suffix is exact. Record timestamps are not monotonic. A producer sets them, and a batch stamped late can sit in front of records stamped earlier. Truncating a partition at the first batch that crosses a timestamp bound would drop records from before the bound that happen to sit behind a late-stamped one, and the operator asked to keep those.

The consequence is worth stating for an operator. After a timestamp-bounded restore, a consumer that seeks to end lands past the last surviving record, and the partition holds empty batches between the two. That is the same shape compaction produces at the tail of a partition, and clients already handle it.

The offset cut is coarser than it looks, and an operator has to know it. The restore keeps a batch that straddles the bound whole, so the records that batch holds past the bound survive. The producer's batching therefore sets the granularity of `--to-offset`, and only a partition written one record per batch cuts exactly at the number. An operator who has to remove one record from inside a straddling batch removes it with `--exclude-offset`, which is a record-level predicate. Making the offset cut exact means re-encoding at most one batch per partition, with the machinery the exclude predicates already use, and this document does not claim that is done.

### Control Batches Are Never Filtered

A control batch carries a transaction commit or abort marker, and no exclude predicate is about transaction bookkeeping.

An operator who writes `--exclude-producer-id 7` means the data that producer wrote. They have no way to know, and no reason to expect, that the same id also names the marker that closes out that producer's transactions. Filtering such a marker would leave a restored partition whose transaction state is silently wrong, and a `read_committed` consumer would then stall at a last stable offset that never advances, or read records the cluster never committed. Restoring one record an operator meant to exclude is the smaller failure.

The exemption costs nothing at the bound. A control batch past a `--to-offset` bound is still dropped, because tail truncation happens before any predicate is consulted.

### Predicates Match Raw Bytes and Know No Schema

`--exclude-key` and `--exclude-header` match the raw key and header bytes, and only when those bytes are valid UTF-8. The tool decodes no payload and holds no schema, so a pattern written against a JSON field or an Avro field does not match. Bytes that are not valid UTF-8 never match, because a pattern that cannot even be checked against a byte string must not be treated as matching it.

The patterns use RE2 semantics through the `regex` crate, and the choice is a safety property rather than a preference. An operator's pattern runs over every record in an archive that can hold billions of them. A backtracking engine gives a pattern whose cost is exponential in the input, and a restore that stops making progress during an incident is a failed restore. RE2 is linear in the input, whatever the pattern.

The workspace also carries a `java_regex` crate for the KIP-664 `ListTransactions` wire surface, which must match Java's semantics exactly. It offers no such bound, so the restore does not use it. A pattern surface that an operator types is a different problem from a pattern surface that a Kafka client sends.

### Materialize Writes Through the Formatter, Not Around It

The materialize stage formats the target through `krabka-format`'s own entry point, forwarding the target-side flags unchanged, and seeds the topic and partition records the archive scan recovered into the same bootstrap stream. The restored cluster boots with its topics already present, with the topic ids the archive recorded, and with a partition count derived from the partitions that were found.

Seeding rides an entry point that `krabka-format` grew for this: a caller that recovered metadata hands records into the stream that reaches both the bootstrap records and the offset-zero checkpoint. The alternative was a second writer for the same on-disk format inside the restore crate, which is one format with two implementations and a guarantee that they will diverge.

Each restored partition names the target node as leader, sole replica, and sole in-sync replica, at leader epoch 0 and partition epoch 0. A restored cluster is a single node by construction, and growing it afterwards is an ordinary reassignment.

### Verbatim Where Nothing Changed

A batch that the bound does not touch is written through the log's verbatim path. The restore hands the log the batch's own base offset and leader epoch, read from the archived header, and the bytes go down untouched, so the producer's original CRC survives into the restored cluster.

That is not only an optimization. A restore that re-encoded every batch would hand back a log whose bytes no producer ever wrote, and the CRC of every batch would then attest to the restore tool rather than to the producer. Keeping the untouched path byte-exact means the restored log carries the producer's own evidence everywhere the operator did not deliberately change something.

The bound also decides this without decoding. A partition with no exclude predicate and no timestamp bound answers `Keep` for every batch after one check, so the common restore, which is a full restore or an offset-bounded one, never decodes a record at all.

### Leader Epochs Come Back Through the Batches

The verify stage parses the archived leader-epoch checkpoint and checks it. The materialize stage does not transplant it.

The epoch history reaches the target through the batch headers instead. Each archived batch carries its own `partition_leader_epoch`, and the target log rebuilds its own checkpoint as those batches are appended. One source of truth is better than two that can disagree, and the batches are the source the CRC already covers. An epoch that produced no surviving batch is lost, and it has nothing to describe.

### Corruption Stops the Restore by Default

A segment that fails verification stops the whole restore, and the error names the object and the byte position. This is the right default for a tool whose output an operator will trust: a partial restore that reports success is worse than a restore that stops.

`--continue-on-corrupt` is the escape, and it is honest about what it costs. Each skipped segment appears in the report with the reason, and a skipped segment leaves a hole in the partition that the restore cannot fill. An operator who takes that trade is choosing the records they can still have, and the report tells them exactly which ones they cannot.

The flag does not rescue a materialization failure. A target that will not accept a write is not a damaged segment, and continuing past it would produce nothing.

### What the Archive Holds Bounds the Restore

A restore cannot return what was never copied.

Tiering is per topic, gated by `remote.storage.enable`, so a topic that was never enabled has nothing in the archive. Tiering is also per closed segment, so the active segment and any segment the broker had not yet copied are not there. The restore's reachable point in time is the end of the last archived segment, and not the moment the incident started. An operator who wants a tighter bound tunes the copy path, and not the restore.

Internal topics follow the same rule. `__consumer_offsets` and `__transaction_state` are compacted and are not normally tiered, so committed group offsets and in-flight transaction state do not come back. The [Compatibility](#compatibility-deprecation-and-migration-plan) section lists what an operator has to restore by other means.

## Compatibility, Deprecation, and Migration Plan

There is no wire change, no new API key, no new error code, and no client change. The tool runs offline, and it depends on the object-store layer, the log, the archive codec, and the formatter. It does not depend on the broker, because a recovery tool must not carry the thing it recovers.

A restored directory is an ordinary krabka data directory, and it needs no restore-specific code in the broker to boot. The [Test Plan](#test-plan) says what proves that today and what does not.

What a stock client observes on a restored cluster is the contract this document adds.

- **Offsets are the offsets the archive held.** Every record the restore keeps sits at the offset it held in the archive, so an offset another system recorded still names the record it named.
- **Offsets skip where records were dropped.** A gap is what compaction produces, and clients step over one already.
- **A batch can hold no records.** This is the emptied batch above, and it is also what compaction leaves behind.
- **A filtered batch's bytes are not the producer's bytes.** Only a batch that passed through untouched keeps the producer's original encoding and CRC.
- **Transaction markers survive.** A `read_committed` consumer resolves a restored partition, because no predicate can remove a control batch.
- **A timestamp-bounded partition ends past its last surviving record.** Seek-to-end lands after the bound.

What does not come back, and has to be restored by other means: committed consumer group offsets, ACLs, quotas, dynamic broker and topic configs, and any topic property beyond the topic id and the partition count. The cluster id is new unless the operator supplies one, and the node identity is new in every case. This is a materialization of the log, and not a clone of a cluster.

krabka is greenfield and undeployed, so there is no migration. The tool reads the KIP-405 layout that this repository already writes, and it changes nothing about how the broker archives a segment.

## Test Plan

Every claim this document makes about a bound is a claim about bytes in a restored partition, so the suites build real archives and read the result back.

**Unit.** Each stage is tested against fixtures: key parsing round-trips over topic names that contain the hyphen the layout uses as a separator, corruption cases against hand-built bytes, predicate decisions against constructed batches, and the seeded metadata records against a pure function that builds them without running the formatter.

**Round-trip.** `tests/roundtrip.rs` builds a real archive the way the broker's copy path builds one: a real log, real batches, real sealed segments, archived through the real local tiered storage. It then drives the crate's own entry points and reads the restored partitions back with a fresh log. The fixture spans two topics, one with two partitions, and one partition with two segments, so discovery has grouping to do and materialization has to continue a partition across a segment boundary.

**Bounds.** `tests/bounds.rs` proves that each bound changes what a full restore writes, and not only what the predicate module decides in isolation. Every scenario reads the restored partition back and compares whole batches, which is what catches an offset that shifted.

**Corruption.** `tests/corruption.rs` damages one artifact of a real archive on disk and asserts the error variant, the exit code, and the object named. It also proves that `--continue-on-corrupt` skips exactly the damaged segment and restores the rest.

**Command line.** `tests/cli_surface.rs` runs the binary as a subprocess, which is what covers the flag surface and the exit codes a runbook branches on.

Two things are not proved yet, and no gate reports them. No test boots a broker on a restored directory, so the claim that a restored directory is bootable rests on the formatter's own tests plus the log layer reading the partitions back. No differential test drives a stock JVM consumer against a restored cluster, which is the test that would prove the client-visible contract above rather than argue it. Both are worth having, and the round-trip suite is where they belong.

## Rejected Alternatives

### A Broker Admin API

The restore could be an admin request, so an operator would run it against a live cluster with the tools they already have.

The premise fails. A cluster whose data directories were encrypted or corrupted cannot serve the request, and that cluster is the reason the tool exists. A restore also needs an empty target, and a running broker is not one.

The security argument is the stronger half. Two of the four incidents in the [Motivation](#motivation) are an attacker holding a credential. A restore reachable through the same admin surface is a restore that the attacker holds too, and an attacker who can restore can also overwrite a cluster with an archive of their choosing. The recovery path has to sit outside the credential that failed.

### A Restore That Merges Into a Live Cluster

The restore could write into an existing data directory and fill in what is missing.

There is no rule that says what a merge means. The archive and the target can disagree about the records at an offset, and the case where they disagree is the case that matters, because it is the corruption. Any merge policy silently picks a side.

An empty target makes the outcome a fact rather than a policy. The restored cluster holds what the archive held, up to the bound, and nothing else. An operator who wants the old cluster's data compared against the restored one has two clusters and can compare them.

### Dense Offsets

The restore could renumber records so the restored partition has no gaps, which would give a smaller log and a partition that looks untouched.

It breaks every offset reference outside the cluster, as the [Motivation](#motivation) describes, and it breaks them silently. A consumer group's committed offset would resolve to a different record, and nothing about the resulting read looks wrong.

Kafka's own precedent is unambiguous. Compaction removes records and preserves the offsets of the survivors, and gaps in a partition are a normal condition that every client handles. The restore follows the rule Kafka already set.

### Trusting the Archive's Metadata for a Segment's Facts

A segment's end offset and its highest timestamp are recorded in the archive's metadata. Reading them from there would let the restore skip the walk over the batch headers of a segment it does not need to filter.

A restore runs because something is wrong. Metadata about the bytes is a second copy that can be stale, can be truncated, or can have been written by whoever caused the incident, and none of those states are visible from the metadata itself. The batch headers are covered by the CRC the restore checks anyway, so deriving both facts from them costs one pass over data that has to be read regardless.

### The Broker's `RemoteStorageManager` as the Archive Handle

The broker already maps a TOML backend config onto an object store, and the restore could reuse it.

That would make the recovery tool depend on the broker crate, which means a recovery tool that carries the thing it recovers. It also inherits the broker's config shape, so an operator would express an archive location in the vocabulary of a cluster that is not running. The restore builds straight on the object-store layer and takes flags instead.

### A Batch-Granularity Timestamp Bound

`--to-timestamp` could compare against a batch's `max_timestamp`, which sits in the batch header. The restore would then keep or drop whole batches without decoding records, and every kept batch would take the verbatim path.

It answers the wrong question. A batch that straddles the bound holds records on both sides of it, and the operator asked for the records before the bound. Keeping the whole batch restores records the operator meant to drop, which is the bad write they are recovering from. Dropping the whole batch loses records they meant to keep.

The cost of the record-level bound is bounded and visible. A partition with no timestamp bound and no exclude predicate never decodes a record, and the report says how many batches were rewritten.

The same argument applies to the offset cut, which is batch-granular today. [Two Bounds, Two Shapes](#two-bounds-two-shapes) states that asymmetry, what it costs an operator, and what closing it would take.

### Skipping an Emptied Batch's Write

A batch whose records were all dropped could be left out of the target entirely, which is the intuitive reading of "drop these records".

It renumbers the partition. The target log's end offset would fall behind the archive's, and every later batch would be rejected or, worse, silently placed at a different offset. [An emptied batch still claims its offsets](#an-emptied-batch-still-claims-its-offsets) states the mechanism.

### Schema-Aware Predicates

`--exclude-key` and `--exclude-header` match raw bytes, so an operator cannot select on a JSON field or an Avro field. A schema-aware predicate is the first thing most readers want.

It puts a schema registry client and a decoder for every serialization format into a tool whose whole value is that it runs when nothing else does. A registry that is down, a schema that was deleted, or a format the tool does not know would each turn a recovery into a dependency hunt.

A raw-bytes predicate is honest about what it can do, and the two flags that carry it are the ones a producer controls directly. An operator who needs a field-level bound can restore the partition whole and filter downstream, where the schema already lives.

### Rebuilding Consumer Group Offsets

The restore could synthesize `__consumer_offsets` so that groups resume where they were.

Where they were is the wrong place. The point of a bounded restore is that records after the bound do not exist, and a group's committed offset was taken from a cluster where they did. Replaying it would place the group past the end of the restored partition, or exactly on the bad write the operator removed.

Nothing here can guess what the right position is, because the answer depends on what the consumer already did with the records it read. Committing a position for a group is an operator decision, and the tools for it already exist. A restore that made the decision silently would hide it.
