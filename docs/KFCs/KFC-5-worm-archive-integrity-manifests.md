# KFC-5: WORM Archive Mode with Integrity Manifests

A tiered-storage mode that never rewrites and never deletes what it wrote, seals every segment copy into a signed and chained manifest, and ships a verifier that audits the archive with no broker.

## Status

**Adopted.** The implementation is the `worm` module of `krabka-remote-storage`, the `PutRequest` and `PutOutcome` surface of `krabka-object-store`, and the `krabka-worm-verify` binary, merged in [#3](https://github.com/krabka-io/krabka-broker/pull/3). This document lands on branch `claude/worm-archive-integrity-manifests-2bb33t`.

[KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage) defines the archive that this mode writes into, and the copy path stays the KIP-405 copy path. No KIP defines write-once retention, archive attestation, or a refused remote read, so this document is the specification for those.

## Motivation

A backup that a compromised cluster can delete is not a backup.

KIP-405 tiering already copies every closed segment to object storage, and the archive it builds is the only full history of a partition that survives the loss of the local disks. The credential that writes that archive is the credential that deletes from it. Remote retention is a delete issued by the broker, and the broker holds delete rights on the bucket for exactly that reason. An attacker who takes the broker's credential holds the history as well as the cluster, and one `DeleteObject` loop removes the evidence of what they did.

That is one failure domain wearing two names. The live cluster and its backup share a credential, share a bucket policy, and share whoever administers both. A ransomware event, a stolen role, or an operator who runs the wrong script reaches the archive on the same path it reaches the data directories. The archive has to sit in a domain the cluster cannot reach, and the cluster has to hold no right that would let it reach in.

Deletion is the loud failure. The quiet one is a rewrite. An attacker who replaces one archived `.log` object with different bytes leaves an archive that still lists correctly, still has the right object count, and still restores. The record batch CRC that Kafka carries does not help here, because whoever wrote the replacement bytes computed the CRC over them. `krabka-restore` says so in its own terms: a CRC proves that bytes did not rot, and it proves nothing about who wrote them. See [KFC-3](KFC-3-point-in-time-restore.md#what-verification-does-not-prove).

Regulated retention asks for the same two properties for a different reason. SEC Rule 17a-4(f) is the example this work was scoped against. It is a rule for a broker-dealer and not for a broker process, and krabka certifies nothing against it. What the rule's shape asks for is durable and worth naming: records that nobody can rewrite or erase for a stated period, and a way to show that the record set is complete and unaltered. The 2022 amendment to the rule added an audit-trail condition beside the older non-rewriteable and non-erasable condition, so a verifiable trail of what was written and when is a first-class answer rather than a substitute for one.

Shops build both properties today out of infrastructure parts, and the parts do not meet in the middle. A bucket lock stops the delete, and it says nothing about content. A nightly script that hashes the bucket attests to whatever it found that night, including whatever an attacker left there before it ran. A second copy in a second account solves the credential problem and reintroduces the content problem one bucket over. Each part is real, and no combination of them answers the question an auditor asks, which is whether this archive holds every segment the cluster sealed, in the order it sealed them, with the bytes it sealed.

The broker is the only place that can answer it. Only the broker knows where a segment ended, which segment came next in a partition, what leader epochs the segment carried, and that a copy finished rather than died in the middle. An external crawler sees objects and infers the rest, and an inference over attacker-controlled input is not evidence. The attestation has to be produced by the writer at the moment of the write, before the bytes leave the writer's trust, and it has to be checkable later by somebody who holds none of the writer's keys.

This needs a KFC by the test in the [README](README.md). A stock Kafka client can tell a WORM topic from an ordinary tiered topic, and no KIP explains the difference. `retention.bytes` and `retention.ms` stop bounding the archive. `ListOffsets` EARLIEST stops advancing. `DeleteTopics` reports success over data that stays. Under `write_only = true` a fetch of an archived offset comes back `OFFSET_OUT_OF_RANGE`. The [Compatibility](#compatibility-deprecation-and-migration-plan) section states that contract in full.

## Public Interfaces

The feature adds one broker config table, one object in the archive layout, and one command. It adds no API key, no error code, no topic config, and no metric.

### The Broker Config Table

`[remote_storage.worm]` turns the mode on. The presence of the table is the switch, and every key inside it is optional.

| Key | Default | Meaning |
| :--- | :--- | :--- |
| `signing_key_path` | unset | Path to the PKCS#8 Ed25519 key that signs each manifest. |
| `signing_key_id` | unset | Stable id recorded in every signature, so a chain stays verifiable across a key rotation. |
| `write_only` | `false` | Refuse every remote fetch from this archive. |

The table layers over whichever object store `[remote_storage.s3]` or `[remote_storage.gcs]` selects. WORM is not a backend of its own, because write-once is a property of how the broker uses a store and not of which store it is.

Three combinations are rejected at load time, and each one is rejected because the silent reading of it is worse than a refusal.

A `storage_dir` backend with a WORM table is rejected. A local directory cannot enforce write-once against anything, so an archive written there would look attested and would not be.

`signing_key_path` without `signing_key_id`, and `signing_key_id` without `signing_key_path`, are both rejected. An unsigned archive is a legal configuration, and an empty table is how an operator asks for one. A half-configured key is a mistake, and it must not resolve to the same archive that a deliberate choice produces.

Two S3 settings that predate this work carry part of the guarantee, and a WORM deployment should leave both at their defaults. `conditional_put` sends `If-None-Match`, which is what makes a create-mode write fail instead of overwrite. `checksum_sha256` sends `x-amz-checksum-sha256`, so the server verifies each object as it lands.

### What the Archive Holds

A WORM copy writes one more object than an ordinary copy does. The manifest sits beside the segment it describes, under the segment's own key with a `.manifest` suffix, in the partition directory the KIP-405 layout already defines.

```
<prefix>/<topic>-<partition>-<topic-id>/<base-offset>-<segment-id>.log
<prefix>/<topic>-<partition>-<topic-id>/<base-offset>-<segment-id>.index
...
<prefix>/<topic>-<partition>-<topic-id>/<base-offset>-<segment-id>.manifest
```

The manifest is JSON. It names every object the copy wrote with that object's size, its `SHA-256` digest, its entity tag, and its version id. It names the segment it describes with the topic, the topic id, the partition, the segment id, the offset bounds, the highest record timestamp, the copying broker, the copy time, the segment size, and the leader-epoch map. It names its own position in the partition's hash chain. It carries an Ed25519 signature beside the body, when the archive has a signing key.

`MANIFEST_FORMAT_VERSION` covers both the JSON shape and the canonical byte layout that the chain hashes. A change to either is a change to that number, and a verifier refuses a version it does not know.

### The Command

`krabka-worm-verify verify` audits an archive. The name does not follow the `krabka <subcommand>` dispatch that `krabka restore` uses, and that is deliberate. The verifier is the auditor's tool and not the operator's, and an auditor installs it on a machine that runs no broker and holds no cluster config.

| Flag | Meaning |
| :--- | :--- |
| `--bucket NAME` | Read from S3 or an S3-compatible store. `--region`, `--endpoint`, and `--allow-http` refine it. |
| `--local-dir PATH` | Read from a local or mounted copy instead. |
| `--prefix`, `--topic`, `--partition` | Narrow the run. |
| `--key-id ID`, `--public-key PATH` | The trusted key. Both are needed, and each requires the other. |
| `--expect-head HEX` | The head the newest manifest must produce, taken from a record kept outside the archive. |
| `--deep` | Download every object and recompute its `SHA-256`. |
| `--allow-epoch-restarts` | Accept a chain restart instead of grading it as a hole. |
| `--strict-orphans` | Grade an object that no manifest names as a failure. |

There is no `--access-key` flag, and its absence is a design decision rather than an omission. Credentials come from the ambient AWS chain, so an auditor runs under a read-only role and never holds a copy of the writer's keys. A flag that took a key would invite the operator to paste the broker's key into an audit, which puts the auditor back inside the credential domain the archive exists to escape.

The verdict goes to stdout and the diagnostics go to stderr, so a script reads the grade without parsing the explanation.

| Verdict | Exit code | Meaning |
| :--- | :--- | :--- |
| `OK: N manifests over M partition(s), …` | 0 | The chain is continuous, every signature is valid, every object is present. One line per partition follows, and it names the tip for the next `--expect-head`. |
| `OK: empty archive` | 0 | Nothing to verify. |
| `TAMPER DETECTED at KEY (seq N)` | 1 | A manifest was rewritten, reordered, or removed, or an object does not match what its manifest records. |
| `HEAD MISMATCH: expected X, archive tip Y` | 1 | The chain is internally perfect and stops short of the head the run was given. |
| `ORPHAN OBJECTS: N object(s) with no manifest` | 0, or 1 under `--strict-orphans` | An object that no manifest names. Reported either way. |
| `INCOMPLETE ATTESTATION: chain restarted N time(s)` | 1 | The chain has holes between epochs. |
| `INCOMPLETE ATTESTATION: N manifest(s) unsigned, M signed by an untrusted key` | 1 | The archive is not attested to the key the run trusts. |

A failure to read the archive is a different outcome, and it exits with an error message and no verdict. "I could not look" and "I looked and it is broken" must not share an exit code, because only the second is evidence.

### What the Feature Does Not Add

There is no new Kafka error code. A refused remote read reaches a consumer as `OFFSET_OUT_OF_RANGE`, which is the code the fetch path already returns when the remote reader cannot answer.

There is no metric. A WORM deployment that wants to alert on a copy that stopped sealing manifests has nothing to alert on inside krabka today, and it has to watch the bucket instead. Two counters would close that, one for manifests sealed and one for seals that failed, and this document does not claim they exist.

## Proposed Changes

### Three Layers, and the Gap Each One Closes

The archive proves its own integrity in three layers. Each layer exists because the layer below it leaves a specific gap, and naming the gap is the only way to know what the archive does not prove.

**A `SHA-256` digest per object.** Every object a copy writes gets a digest in the manifest, computed as the bytes stream past on the way to the store. This detects a change to the bytes of any one object. It does not detect a change to the manifest, because the same writer produced both, and an attacker who can rewrite the object can rewrite the digest beside it.

**A hash chain per partition.** Each manifest records the chain head that preceded it and hashes its own canonical bytes onto that head. This binds every manifest to all the manifests before it. A manifest cannot be rewritten, reordered, or removed from the middle of a partition without breaking every head after it. It does not prove who wrote the chain, because an attacker who can write to the bucket can rebuild a whole chain from genesis.

**An Ed25519 signature per manifest.** The signature covers the chain head, under a key id the manifest names. This binds the chain to a key that the broker holds and the bucket does not. An attacker with full write access to the bucket can still not forge a manifest, because the private key is not in the bucket.

The three layers stack, and each one is worthless alone. A digest without a chain proves nothing about completeness or order. A chain without a signature proves nothing about authorship. A signature without a digest proves that somebody signed a claim about objects nobody checked.

### The Manifest Is the Commit Point

A copy writes its data objects first and its manifest last.

The order decides which of two failure shapes a crash leaves behind. Manifest last means a crash leaves data objects that no manifest names. Manifest first means a crash leaves a manifest naming objects that do not exist.

The second shape is much worse to meet in an audit. A manifest naming an absent object is indistinguishable from a deleted object, which is the exact attack the archive exists to detect. The first shape is a few unreferenced blobs that no reader ever reaches, and the retry runs under a fresh segment id, so its keys cannot collide with theirs.

That choice is why the verifier has an orphan concept at all, and [Why an Orphan Is Reported and Not Fatal](#why-an-orphan-is-reported-and-not-fatal) follows the consequence through.

### The Canonical Encoding Is the Contract

The chain hashes a byte encoding of the manifest body, and not the manifest's JSON.

JSON is not canonical. Key order, whitespace, and number rendering can all change without changing the value, and any of those changes would move the chain head. The writer and the verifier would then disagree about an archive that nobody touched, and a disagreement of that kind is unrecoverable, because there is no way to tell it apart from tampering.

`canonical_manifest_bytes` is the one definition, and both sides call it. Every integer is big-endian, every string is UTF-8, and every variable-length field carries a length prefix, so no two distinct bodies encode to the same bytes. The encoding opens with a domain separation constant, so a manifest body can never collide with any other value that krabka chains.

The length prefix is a `u64` and not a `u32`. A saturating `u32` prefix would make the encoding non-injective in principle, and "a field that large cannot occur" is a claim about callers rather than a property of the encoding. Four bytes per field is not worth an argument about a chain head.

The JSON layout is free, because the chain does not depend on it. The writer emits compact JSON to keep the object small, and a reader that wants it indented re-serializes it.

### The Signature Covers the Head, and Sits Beside the Body

A signature covers the chain head that the body produces, under the signing key's id. It does not cover the body directly.

Signing the head rather than the body is what makes the signature cheap to check and complete in what it covers. The head already commits to every field of the body, because the head is a hash of the canonical bytes, and it also commits to every manifest before this one. One signature over 32 bytes attests to the whole prefix of the chain.

The signature is a sibling of the body in the type, and never a member of it. A signature that lived inside the body would have to be excluded from what it signs, and that exclusion would be a rule the writer has to remember. Putting it beside the body makes the property hold by the shape of the type instead.

The key id is inside the signed bytes. Without it, a signature made under one key could be replayed as a signature under another, and the chain would lose the ability to say which key attested to a given run.

### The Chain Rides the KIP-405 Custom Metadata Channel

KIP-405 gives the `RemoteStorageManager` a way to hand an opaque value back to the metadata manager when a copy finishes. Ordinary tiering does not use it, because every object key is derivable from the segment metadata. WORM uses it as the chain receipt.

The receipt records the epoch, the sequence, the head before the manifest, the head the manifest produced, and the object-store version id of the manifest itself. The next copy in the partition reads the receipts back from the metadata manager and continues from the newest one.

The stamp goes on **before** the copy runs, and it goes on in its request form, which carries no head. Two properties follow. A durable metadata manager shows the intended chain position on the `CopySegmentStarted` record even if the broker dies mid-copy. And the backend can refuse a copy that arrives unstamped, which it does: an unstamped copy would upload every object and only then discover it cannot seal a manifest, leaving orphans that nothing can ever collect.

The archiver takes only the position from the incoming record. Any head the record already carries belongs to an earlier manifest, and reusing it would chain a manifest onto itself.

The seeding cost is zero extra requests. The copy pass already lists the partition's segments, and the chain seed reads that same listing rather than fetching a second one.

### A Broken Chain Restarts as a New Epoch, Not at Genesis

A broker that cannot read back its chain tip starts a new epoch with a fresh id, at genesis, instead of continuing the old chain at sequence zero.

The distinction is the whole point. A chain that restarts at sequence zero under the same epoch id looks exactly like a chain that was rewritten from the start, and an auditor cannot tell the two apart. A new epoch id says plainly that this run is not connected to the previous one, and the verifier reports the archive as attested in pieces.

The case that produces it is real and common in development. `RlmmKind::InMemory` does not survive a broker restart, so the receipts are gone and the tip is unreadable. The fix is the topic-backed metadata manager, which recovers its state from `__remote_log_metadata`, and the verifier's own message names that fix rather than making an auditor guess.

`next_chain_stamp` takes the new epoch id as an argument rather than minting one inside. That keeps the function pure, and a pure function that decides where a chain continues is a function a test can pin exactly.

### Retention Cannot Run Against a Write-Once Archive

KIP-405 remote retention deletes segments the tier still holds. A write-once archive has no delete to give, so the pass does not run.

The short-circuit sits at the top of the retention pass and again in the eviction-set computation, before either one lists anything. A WORM partition costs a 30-second tick nothing at all, rather than costing a listing whose result is then discarded.

Placing the refusal at the eviction set rather than at the store is deliberate. The backend refuses a delete too, and that refusal is the backstop. A pass that walked to an RSM delete would take an error back on every segment on every tick. That is a warning per segment per tick for behavior that is working exactly as configured, and an operator who reads ten thousand warnings a day stops reading warnings.

Local retention is untouched. The broker still evicts a local segment once the archive holds it, so a WORM deployment does not need local disk for the whole history. What it gives up is the ability to bound the archive, and the [Compatibility](#compatibility-deprecation-and-migration-plan) section states that as a client-visible fact.

Topic deletion follows the same rule. A `DeleteTopics` call removes the partition from the cluster and clears the broker's metadata, and it deletes nothing from the bucket. Erasing a compliance archive must not be a side effect of an admin call, and a retention decision the archive never agreed to must not arrive through the admin API.

### A Failed Copy Keeps Its Debris

An ordinary copy that fails deletes whatever it managed to write and retries. A WORM copy cannot, so it does not try.

The rollback path skips the RSM delete under write-once mode and drops only the metadata. The objects the failed copy wrote stay in the archive for good. They are inert, because the copy never sealed a manifest and no chain references them, and the retry runs under a fresh segment id, so its keys cannot collide.

This residue is the standing cost of a tier that can take nothing back, and it is the same residue the verifier reports as orphan objects. Stating it here rather than leaving it for an auditor to discover is the point: an archive that grows a little debris is the price of an archive that cannot be emptied.

### The Bucket Enforces Write-Once, and the Broker Enforces Write-Never

Two halves make the guarantee, and neither is enough alone.

The broker's half is that it issues no delete and serializes creation of every key. Small objects use a conditional create directly. Multipart objects first claim their key with an atomic empty-object create, then pin and verify the completed version and digest before committing the manifest. Every delete returns an error before any request reaches the store.

The bucket's half is what stops somebody who is not the broker. S3 uses versioning and Object Lock in compliance mode with a default retention period. GCS uses versioning and a locked retention policy. The broker confirms the selected policy before enabling WORM mode.

krabka does not set the lock, and that is a decision rather than a gap. `object_store` 0.13 models no `x-amz-object-lock-*` header, and the crate is pinned in lock-step with the datafusion revision and `parquet 59`, so bumping it alone splits the dependency graph. There is an untyped way through with default headers, and [Setting Object Lock Headers from the Broker](#setting-object-lock-headers-from-the-broker) says why that way is worse than the bucket default.

`PutMode::Create` still cannot bind multipart completion itself. The atomic reservation serializes cooperating writers, while bucket retention protects both the reservation and the completed version. A failed multipart upload deliberately consumes the key: retrying is less important than avoiding two writers that both believe they own it. Manifests are small and always take the direct conditional-create path.

### `write_only` Makes the Archive a Sink, Not a Tier

With `write_only = true` the backend refuses every remote fetch. A consumer that asks for an offset whose local segment was already evicted gets an error and not a slow read.

This is the strongest form of credential separation the broker can offer, and it is the reason the setting exists. An archive that the broker never reads is an archive the broker needs no read right on, so the deployment can hand the broker a credential that carries `s3:PutObject` and nothing else. A stolen broker credential then cannot list the archive, cannot read it, and cannot delete from it. The only thing it can do is add an object, and Object Lock decides what happens to that.

The cost is a cliff and not a slowdown. The data is in the bucket, and the broker refuses to serve it. `ListOffsets` still advertises the archive's earliest offset, because that answer comes from the metadata manager and not from the store, so the broker advertises an offset that a fetch then refuses. An operator should set `write_only = true` only when the archive exists for the auditor and the read path never needs it.

### The Verifier Reads the Archive and Nothing Else

Everything the verifier checks is written into the archive. It needs no broker, no metadata manager, and no cluster, because the manifest carries the digests, the chain position, and the signature.

That is what makes the tool usable years after the cluster that wrote the archive stopped running, and it is what makes an audit independent. An auditor who has to ask the cluster whether the archive is good is auditing the cluster's opinion.

The verifier's unit of work is a directory, not a partition record. It groups the listing by directory and treats each directory as one chain. The topic and partition filters read the manifest body and not the directory name. A directory name embeds a URL-safe Base64 topic id, and that alphabet contains the same `-` that separates the name's fields, so the name cannot be reliably split back apart.

The walk stops at the first break **per partition** and continues with the other partitions. One damaged partition must not hide the state of the rest, and an auditor who gets one error and no report learns almost nothing. It does no recovery and no truncation, so tail damage stays visible rather than being quietly walked around.

The report is deterministic. Two runs against an unchanged archive produce equal values, which is what lets a compliance program diff one run against the last.

### Two Depths, and What Each One Proves

A shallow run recomputes the chain, checks every signature, and confirms that every object a manifest names exists with the recorded size. It downloads no segment body, so it is cheap enough to run continuously against a large archive. It cannot see a body that was replaced with different bytes of the same length.

A deep run downloads every object and recomputes its `SHA-256`. It is the only depth that catches a same-size substitution, and it reads the whole archive to do it.

Splitting the two is what makes both useful. A single expensive depth would be run monthly, and a month is a long time to hold an undetected rewrite. A single cheap depth would leave the substitution attack open forever. A shop runs shallow on a schedule and deep on the period the retention policy names.

An object must live in the manifest's own partition directory. A manifest that points somewhere else names an object the walk cannot account for, and it counts as missing rather than as a cross-directory reference the verifier chases.

### Trust the Key the Auditor Holds, Never the Key in the Manifest

A manifest carries the public key that signed it, and the verifier does not check against that key.

It checks against the key registered under the manifest's `key_id` in the set the auditor supplied. An attacker who can rewrite a manifest can rewrite the public key beside it, sign the result with a key of their own, and produce a manifest that self-verifies perfectly. The key has to come from outside the archive, or the signature layer collapses into the chain layer.

A run with no trusted key still recomputes the chain and checks every object, and it counts every signed manifest as untrusted. It proves internal consistency and says nothing about who wrote the archive. The tool grades that as an incomplete attestation rather than a pass, because a verdict of "OK" over an archive nobody's key vouches for is the kind of green that trains an auditor to stop looking.

The key id is what lets a chain survive a key rotation, because each manifest names the key that signed it rather than assuming one key for the archive. The library's trusted-key set holds many keys for exactly that. The command line takes one `--key-id` and `--public-key` pair, so an archive that spans a rotation needs one run per key today, narrowed by prefix. Widening the flag to a repeatable pair is the obvious follow-up, and it is a change to this document rather than a new one.

### Tail Truncation Is Not Detectable from the Archive Alone

This is the honest limit of the design, and it belongs in the middle of the document rather than in a footnote.

An attacker who deletes the newest manifests of a partition, and every object those manifests name, leaves a shorter chain that is internally perfect. Every remaining head chains correctly. Every remaining signature verifies. No amount of reading the archive reveals what is gone, because nothing inside the archive says how long the chain should be.

The gap closes only outside the archive. A successful run prints each partition's tip head. An operator records that value somewhere the bucket's writers cannot reach, and passes it to the next run as `--expect-head`. A run that holds no expected head proves internal consistency and says nothing about completeness.

The tool treats an emptied archive as the extreme of the same attack. An expected head with no partition left to check is a mismatch and not a pass, because "there is nothing here" is precisely what a complete truncation looks like.

Grading a tip mismatch is the binary's job and not the library's. `verify_archive` can raise a mismatch as a break for a programmatic caller that wants one `ok` to test. The binary leaves that off and compares the tips itself. A chain that stops short is a different finding from a chain that was tampered with. An operator who reads `TAMPER DETECTED` when the real event was a missed run learns to distrust the tool.

### Grading Is Separate from Walking

The library reports what it found. The binary decides what that is worth.

The split matters because two of the findings are expected in one deployment and unacceptable in another. An epoch restart is routine in development and a hole in production. An orphan object is inert debris in a busy cluster and a red flag in an archive that is supposed to hold nothing else. A library that graded these would force one policy on every caller, and a caller who disagreed would have to parse strings to get around it.

`--allow-epoch-restarts` and `--strict-orphans` are the two knobs, and both sit in their own argument group for that reason. Neither one changes what the walk reads. Both change only which findings are worth a non-zero exit.

The same split governs errors. A `WormError` comes back only when the archive cannot be listed, a manifest cannot be read, or a deep walk cannot fetch a body. A tampered archive is a successful call with `ok` false. An auditor who cannot tell "the bucket is unreachable" from "the bucket was tampered with" has no finding at all.

### Why an Orphan Is Reported and Not Fatal

An orphan is an object no manifest names, and the default grade for one is not a failure. That reverses the obvious reading, so the reason has to be here.

The archiver writes the manifest last, so an interrupted copy leaves exactly this. A WORM archive refuses deletes, so the debris can never be cleared. Grading it a failure would mean one interrupted copy condemns the archive on every run from then on, with no action any operator could take to get back to green.

A verdict nobody can act on is a verdict they stop reading, and that costs more than the debris does. The orphans are reported in full instead, on stderr and on the `OK:` line, and `--strict-orphans` restores the hard grade for a deployment that wants the bucket to hold nothing but the archive.

What the default costs is worth stating rather than burying. Orphans are also what a **removed** manifest leaves behind. Only `--expect-head` settles that case, and it is the check to run when tail truncation is the concern.

### On a Versioned Bucket, Say Whether the Segment Is Recoverable

An overwrite on a versioned bucket does not replace the locked original. It stacks a new current version on top of it.

A deep walk reads the current version, because the current version is what a reader gets, so the overwrite is reported as tampering. That is the detection this feature exists for. What the mismatch alone does not say is whether the archive is recoverable, and on an Object Lock bucket it usually is.

So when the manifest recorded a version id, the run re-reads that pinned version and says in one clause whether it still holds the recorded bytes. That is the difference between restoring a segment and writing it off, and it is the first thing an operator needs after a detection.

The pinned version is never read first. A walk that always read it would confirm bytes no reader can reach by key any more, and it would be blind to the overwrite itself. The re-read is supplementary and never fails the run, because the mismatch is already the finding.

### The Archive Is Hostile Input

A verifier reads objects that an attacker may have written, so it is built to that assumption throughout.

Every manifest read goes through a capped reader that issues a `HEAD` first and refuses an oversized object before it buffers a byte. A real manifest describes one segment copy and is a few kilobytes, so a one-megabyte cap is generous and still bounds the damage. An oversized object grades as a break rather than an error, because an object in the archive is archive content and not a failure to look.

No allocation is ever sized from a count a manifest supplies. Object bodies are hashed chunk by chunk on a stream and never buffered whole, so a deep walk's memory cost does not follow the object size. A damaged or hostile archive produces a report and never a panic.

### The Primitives Come from `krabka-audit`

The chain hash formula and the Ed25519 signing come from the audit crate, which already carried both for the broker's own audit checkpoints. The WORM module adds only the domain separation and the canonical encoding that a segment manifest needs.

Two domain constants keep the reuse safe. The manifest body domain separates a chained manifest body from every other value krabka chains. The manifest signature domain is distinct from the checkpoint signature domain, so a signature produced for one purpose can never be replayed as a signature for the other. Sharing a domain across signature purposes is the classic way a correct primitive becomes an incorrect protocol.

## Compatibility, Deprecation, and Migration Plan

There is no wire change, no new API key, no new error code, no new topic config, and no client change. WORM mode changes what an existing surface answers, and not the shape of any answer.

What a stock client and a stock admin tool observe on a WORM topic is the contract this document adds.

- **`retention.bytes` and `retention.ms` do not bound the archive.** Both still bound the local log. The remote tier keeps every segment it ever received, whatever those settings say.
- **`ListOffsets` EARLIEST does not advance.** It returns the lowest start offset across the archive's finished segments, and no segment ever leaves that state, so the value stays at the partition's first archived offset for the life of the topic.
- **`DeleteTopics` succeeds and the archived data stays.** The topic leaves the cluster. The objects and the manifests stay in the bucket. Removal is a bucket-side action, taken deliberately after the Object Lock retention period ends.
- **Under `write_only = true`, an archived offset is not readable.** A fetch below the local log start comes back `OFFSET_OUT_OF_RANGE`, which is the same code a consumer sees for an offset that fell off the log. The two are indistinguishable to the client, and `ListOffsets` will still name the offset the fetch refuses.
- **Every remaining tiered behavior is unchanged.** With `write_only = false` a consumer reads history through the archive exactly as it reads through an ordinary tier, and the copy path, the key layout, and the fetch path are the KIP-405 ones.

Kafka defines none of the four, because Kafka has no write-once tier. The first three follow from a tier that cannot delete, and the fourth is the operator's own choice to trade the read path for a narrower credential.

krabka is greenfield and undeployed, so there is no migration and no compatibility shim. Two operational notes stand in for one.

Turning the table on over a bucket that already holds an ordinary tiered archive does not adopt what is there. The first manifest starts a fresh epoch at genesis, and every object written before the switch is an orphan, because no manifest names it. An archive that has to be attested from its first byte should start on an empty prefix.

A WORM deployment should use the topic-backed remote-log metadata manager. The in-memory one does not survive a restart, so the chain tip is unreadable after one and the broker starts a new epoch. The archive stays valid and it is attested in pieces, which the verifier reports as a hole.

## Test Plan

Every claim in this document is a claim about bytes in a bucket or a grade on a verdict, so the suites build real archives and read them back.

**Unit, the cryptographic core.** The manifest, chain, and archiver modules are tested against fixtures with no store. The manifest suite includes a preimage-coverage table that mutates one labelled field of a body at a time and asserts that the chain head moves, which is what proves that no field reached the manifest without reaching the hash. The signature suite covers the domain separation against the checkpoint domain, and the round trip through the JSON form.

**Unit, the backend.** The S3 backend's suite runs the real copy path against an in-memory object store: a manifest lands beside the segment, it lists every object the copy wrote with its digest, it omits the artifacts a copy did not write, the receipt carries the new head, a second write of the same manifest key is refused, a delete is refused, a copy with no chain stamp is refused, and a write-only backend refuses a fetch without touching the store. One test asserts that the backend's `Debug` reports the mode and the key id and never the key.

**Unit, the retention path.** The remote-log-manager suite drives a fake write-once archive that seals real unsigned manifests. It proves that the eviction set is empty whatever the retention settings say, that a failed copy leaves its objects and drops only the metadata, that consecutive copies chain, and that a broker whose metadata did not survive starts a new epoch instead of restarting the old chain.

**Command line.** `tests/worm_verify_cli.rs` builds an archive on disk the way a backend writes one, runs the real binary against it with `--local-dir`, and checks the exit code **and** that the message names the cause. Every verdict in the table above has a case: a clean archive that prints its tip, that tip satisfying `--expect-head` on the next run, a truncated object, a same-length edit that only `--deep` catches, an unsigned archive, a tip mismatch, an orphan under both gradings, a restarted chain under both gradings, an empty archive, and an emptied archive that still fails against an expected head.

**Object Lock, end to end.** `crates/broker/tests/worm_object_lock.rs` is the suite that proves the half krabka cannot prove about itself. Every other test shows that krabka issues no delete, and an auditor does not have to take krabka's word for that. This suite starts MinIO with Object Lock in compliance mode, boots a real broker with a real signing key, produces with the JVM producer, waits for sealed manifests, reads the whole topic back through the archive with the JVM consumer, then deletes every version of an archived segment body using credentials that hold every right the broker holds. The bucket refuses, and the test asserts both the failed exit and that the object is still listed, because a tool can fail after a server already removed bytes. It then runs a deep verify in process and asserts that the archive verifies and is fully attested.

The container suites are `#[ignore]`d and gated on Docker under Cargo, and their images are pinned by digest under Bazel.

Three things are not proved, and no gate reports them.

No test covers what a JVM consumer sees under `write_only = true`. The refusal has a unit test at the backend, and the mapping to `OFFSET_OUT_OF_RANGE` rests on reading the fetch path rather than on a differential test.

The multipart branch has behavioural coverage that forces a WORM segment above the threshold, replays the copy, and requires the reservation to reject the replay. The Object Lock suite separately proves bucket-side retention against deletion.

Nothing covers a key rotation across a chain, because the command line cannot express one yet.

## Rejected Alternatives

### A Bucket Policy Alone

Object Lock with a retention period stops the delete, and it is already what a compliance deployment configures. The archive could stop there and carry no manifests at all.

It answers only half of the question. A locked bucket says that what is in it cannot be removed. It says nothing about whether what is in it is what the cluster wrote, whether anything is absent, or whether the object at a key is the object that was put there. Versioning plus a lock even makes a rewrite survivable, and it still gives an auditor no way to notice one happened.

The two halves are complementary and neither substitutes for the other. This design keeps the bucket policy as the enforcement and adds the attestation the policy cannot carry.

### An External Crawler That Hashes the Bucket

A scheduled job could list the archive, hash every object, and store the digests somewhere else. It needs no broker change at all, which is its whole appeal.

It attests to whatever it found when it ran. An attacker who rewrites an object between two runs gets caught. An attacker who rewrites one before the first run over that object is recorded as the truth. A crawler cannot tell a segment that was never copied from a segment that was copied and removed, because it has no independent knowledge of what should exist.

Order is the harder gap. The broker knows that segment B followed segment A in a partition. A crawler infers it from offsets in object keys, which are attacker-controlled input. An inference over attacker-controlled input is not evidence.

The attestation has to be made by the writer, at the moment of the write, from facts only the writer holds.

### Digests Without a Chain

Each manifest could carry its object digests and stop there. That detects every rewrite of a segment body, which is the attack most people picture, and it needs no chain state on the copy path.

It leaves the archive open at the level of whole segments. An attacker removes a manifest and the objects it names, and nothing notices, because no other manifest refers to it. An attacker reorders two manifests and nothing notices. An attacker who compromises the broker for an hour writes an archive of their choosing for that hour, and every manifest in it is internally valid.

The chain is what turns a set of independent claims into one claim about a sequence. It costs one field in the manifest and one receipt on the copy path.

### A Merkle Tree per Partition

A tree over the manifests would give short inclusion proofs. An auditor could then be handed a proof that one segment is in the archive without reading the whole partition, which is a real advantage at scale.

The workload does not want it. A partition's manifests arrive strictly in order, one per segment copy, and the verifier's job is to walk all of them and report the first break. A chain is the exact shape of that workload, and a tree adds interior nodes, a rebuild rule, and a second thing to store, for a proof nothing in the design asks for.

The chain also degrades better. A break in a chain names the manifest it stopped at, and every manifest before it stays verified. A tree whose interior nodes live in the same bucket gives an attacker a second surface to rewrite. A tree rebuilt on every append also writes an object per level per copy into a store that cannot take anything back.

Nothing here forecloses a tree. A partition's chain heads are already the leaves one would build over, and adding a periodic tree above the chain would be an addition rather than a replacement.

### The Broker Signing Nothing, and the Bucket Being Trusted

The digests and the chain could ship without the signature, on the argument that an attacker who can rewrite the bucket has already won.

They have not already won, and that is the point. Object Lock means an attacker with the broker's credential cannot remove or replace what is there. What they can do is add. Without a signature, an attacker who adds a complete alternative chain from genesis produces an archive that verifies, and an auditor has two valid chains and no way to choose.

The signature is what makes the archive's authorship checkable with a secret the bucket never holds. It is also what makes an unsigned archive a reportable state rather than an invisible one.

### The Verifier as a Broker Admin API

Verification could be an admin request, so an operator would audit with the tools they already have and with no second binary to install.

The premise fails on both of the motivating incidents. An archive is audited precisely when the cluster is not trusted, and a verdict that the suspect cluster computed is worth nothing. An archive also outlives the cluster that wrote it, and a retention period measured in years will outlast several.

The credential argument is the stronger half. An audit reachable through the admin surface is an audit that whoever holds the admin credential controls, which puts the auditor back inside the domain the archive exists to escape. The verifier takes ambient read-only credentials and a public key, and it holds nothing else.

### Storing the Chain Tip in the Archive

The archive could hold a `HEAD` object per partition, updated on every copy, so that `--expect-head` would not need an operator to record anything.

It cannot work, and the reason is structural rather than practical. The tip object lives in the bucket the attacker writes to. An attacker who truncates the tail updates the tip object to match, and the archive is internally consistent again. An anti-tamper claim cannot be stored inside the thing it makes claims about.

It also cannot be written once. A tip that updates on every copy is a key that gets rewritten, which is the one thing a write-once archive must not do, and Object Lock would refuse the second write.

The expected head has to travel outside the archive. Printing the tip on every successful run and taking it back as a flag is the smallest form of that, and it puts the custody decision where it belongs, which is with whoever holds the audit records.

### Continuing the Old Chain After a Restart

When the broker cannot read its chain tip, it could continue the old epoch at sequence zero rather than mint a new epoch id. That would keep one epoch per partition and produce a simpler report.

It makes an unreadable tip indistinguishable from a rewrite. Both produce a chain that starts at genesis under the partition's existing identity, and an auditor looking at the result cannot tell an in-memory metadata manager from an attacker.

A new epoch id names the discontinuity. The verifier reports the archive as attested in pieces, which is exactly true, and it names the fix. A design that hid the hole would be reporting a stronger guarantee than it has.

### Remote Retention with a Grace Period

Retention could still run on a WORM tier once a segment is older than the bucket's lock period, so the archive would not grow without bound.

It puts the retention decision in the wrong system. The lock period lives on the bucket, the broker cannot read it, and a broker that guessed it would delete early on any bucket whose policy changed. Compliance retention is also not a size budget. It is a stated period after which removal is a deliberate act, and a background pass that removes records on a timer is the opposite of deliberate.

The archive is bounded by the bucket's lifecycle policy, which is where the period is already configured and which outlives any broker that writes to it.

### Deleting the Archive on `DeleteTopics`

A topic delete could cascade into the bucket, so an operator would not be left with archived data for a topic that no longer exists.

Erasing a compliance archive must not be a side effect of an admin call. The whole value of the archive is that the cluster cannot destroy its own history, and a delete path through the admin API is the shortest route back to the failure the design exists to prevent. It is also the route an attacker with an admin credential would take first, and it would not even look like an attack in an audit log.

The cluster's metadata is cleared, so the topic is gone from the cluster's point of view. The bytes are the bucket's to release.

### Setting Object Lock Headers from the Broker

The broker could set `x-amz-object-lock-retain-until-date` on each object with an untyped default header, so a deployment would not have to configure the bucket at all.

The header is an absolute timestamp. A default header pins one date for the lifetime of the process. Every object written by a broker that ran for a month would carry the same retain-until date, and not a period measured from its own write. A broker restart would change the date. That is not a retention policy, it is an artifact of the process lifetime.

A default header also rides every request the client makes, reads included, which is a second reason the untyped route is wrong for this.

A bucket default expresses the policy correctly, applies it per object from the object's own write, and outlives every broker that writes to the bucket.

### The `ETag` as the Integrity Proof

The store already returns an entity tag for every object. The manifest could record that instead of computing a `SHA-256`, which would cost the copy path nothing.

A multipart entity tag is a checksum of the part checksums and not of the object body, so it cannot confirm the bytes of exactly the large segments that matter most. It is also the store's claim about the object rather than the writer's, and an archive whose integrity claim comes from the store cannot detect a store that lies.

The manifest records the entity tag and the version id anyway, and it labels both as what they are. They are locators that find and pin an object. The digest is the only integrity claim in the entry, and it is computed by the writer as the bytes go past.

### One Chain for the Whole Archive

A single chain across every partition would be one head to record rather than one per partition, and `--expect-head` would take one value.

It serializes the copy path. Partitions are copied concurrently by design, and one chain would make every copy wait for a global tip, on a broker and across brokers. It would also make one partition's damage break every partition's chain after it, which turns a local finding into a total loss of attestation.

Per-partition chains match how the archive is written and how it is read. The cost is that an operator records one head per partition, and a successful run prints every one of them.
