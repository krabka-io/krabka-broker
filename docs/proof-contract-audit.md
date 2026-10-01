# Proof contract audit

The proof suite contains substantial search, conservation, progress, and quorum guarantees, but many smaller contracts only pin a decision table over facts already classified by a caller. Those guards are useful; they are not independent proofs of durability, authorization, or exactly-once behavior. The highest-value improvement is to connect the output of one proved kernel to the precondition and safety property of the next.

## Scope and method

This audit covers all 59 original modules in `crates/verified/src` at baseline `560ffc3a906b`, including logical helpers and contracts nested inside `cfg_attr`. The module table names every non-logical function found in those modules, including private helpers and executable proof lemmas. Derive-generated Clone obligations are plumbing and are not counted as separate safety achievements. The new `composition` module adds fifty-six cross-module compositions. The 28
sources with at least 300 lines now use matching subdirectories for kernels
and tests; the public module paths remain the same. Composition theorems are
grouped into 38 topic files. The layout-only move preserved all 305 function bodies and
contracts, passed all 264 tests, and preserved the 3,625-entry mutation
inventory. Saved proof sessions and catalog links follow the new paths.
Fresh generation and the final two-worker no-cache saved-session replay passed
all 515 proof files. Two obligations failed in an initial four-worker replay
and passed unchanged in isolation and in the final full run.

Contracts were assessed for a meaningful failure they exclude, necessary versus sufficient admission conditions, dependence on already-classified host facts, ordering/identity preconditions, and the connection between local results and aggregate behavior. There are no explicit trusted annotations, assumed-false preconditions, or assumed-false postconditions in the verified source. This is a source/specification audit and local Creusot validation, not a new proof of every host adapter or a fresh replay of all Stateright models.

A specification is not automatically weak because it resembles a short function. For example, deny precedence and checked successor arithmetic are real requirements. It is weak evidence for a larger claim when both the requirement and implementation consume an unverified `authorized`, `matches`, `valid`, or `caught_up` fact, or when a recursive reference model duplicates the same mistaken policy. The remedy is an independent consequence or composition, not an extra model with the same branches.

## Confirmed gaps and repairs

1. **Delegation-token mutation allowed total rejection.** The original Append and Retry contracts were implications. Replacing the function body with unconditional Reject still proved. Both contracts are now biconditionals; the same replacement fails verification. The Retry rule also states the validity conditions required by a same-expiry renew. With three variants, specifying Append and Retry exactly also fixes Reject as their complement.
2. **Retention exported reference equality without direct deletion safety.** The original local fold encoded blocking and empty-active-segment protection, but clients had to unfold that reference to use them. Local retention now explicitly guarantees that every selected segment is unblocked and an empty newest segment is retained. Local and remote results explicitly stay within the input length. A negative control removed the blocked check from both the implementation and its reference fold: the original equality contract still proved, while the strengthened proof failed. The new safety statements therefore reject a shared implementation/specification mistake.
3. **Quorum reasoning stopped at a module boundary.** The proved prefix-count monotonicity lemma was available, but its connection to opaque `count_ge` was hidden outside `consensus`. Opening that existing one-line logical definition lets the existing law establish quorum support for the Fetch limit. The existing prefix-count and member definitions are now also open so a concrete supporter list can be tied to the same count. A proved prefix-count equivalence lemma connects the host’s offset clamp to raw reported votes without copying vote-counting logic.
4. **Election jitter has intentionally narrow evidence.** Its range-only contract admits constant zero. It establishes arithmetic safety, not spreading, fairness, or election liveness. Treat it as a range check; a distribution/liveness claim would require a stated probabilistic or scheduling model.
5. **Scalar facts remain the dominant trust boundary.** Exact cache keys, complete ACL/transaction/segment enumeration, true signature results, durable fsync, and coherent metadata snapshots are not proved by boolean decision contracts. WAL placement/installation now establishes distinct identities and the new Fetch-support theorem carries them into an explicit supporter list; identity invariants in other adapters remain separate obligations. More proofs of the same scalar tables would not close those gaps.
6. **Saved artifacts had drifted from source.** Feature-update sessions still described obsolete fact types and cleanup decisions; the producer-decision artifact omitted the current empty-log/trunk sequence rule. Fresh generation identified these semantic differences separately from path/format churn. Those artifacts are refreshed, including the missing `FeatureKind` obligations. Sessions affected by current main's divergence-epoch, failover, token-requester, append-gap, and reassignment changes were also regenerated; unrelated generated changes were removed. Fresh generation of the snapshot-retry composition found four existing composition sessions still importing exact-base append contracts instead of the current compaction-gap admission. Those compositions also prove against the current contracts, and their saved sessions are refreshed.
7. **Audit settlement has a generation-exhaustion boundary.** Saturation at `u64::MAX` can let replay subtract the same marker twice when pending losses remain. The new theorem states the necessary non-exhaustion precondition, and a runnable witness demonstrates the excluded case. The host protocol still needs an explicit exhaustion policy before claiming unconditional replay idempotence.

8. **Sparse timestamp lookup could skip the earliest matching record.** The original floor search and record selector each proved their own contracts, but starting at the last running maximum equal to the target skipped earlier ties. Three single-record batches at timestamp 100 returned offset 2 instead of 0. The production scan now uses the last *strictly smaller* indexed maximum, with a checked predecessor at `i64::MIN`, reusing the existing binary search. New compositions derive index rows from actual prefix maxima and prove the indexed answer equals the global first match without sorting record timestamps. The same fix covers maximum-timestamp lookup.

9. **Structural time-index validation does not establish timestamp truth.** A sorted, in-range row `(timestamp=0, relative_offset=3)` passes the archive row validator, yet for records `(0,100), (3,300), (6,200)` and target 100 it skips the first match. A runnable witness separates this semantic corruption from ordering/extent validation. The new remote scan theorem requires each real row to bound earlier record timestamps; zero-offset padding has no earlier record to bound. Accurate decoded record arrays and truthful persisted maxima remain necessary host obligations.

10. **Trim admission does not bound inherited store frontiers.** With HWM and delivery at 5, a request for 5 is admitted, but a prior WAL start of 8 makes reconciliation choose local start 8. Both individual kernels satisfy their contracts. The new composition states the necessary bounds on both observed store starts and carries them through application, exact retry, snapshot selection, and replay cursor construction. A runnable witness demonstrates the excluded inconsistent state; the host must retain those bounds across prior trims, leadership changes, and recovery. Diskless object coverage/safety-lag composition has the analogous prior-WAL precondition.

11. **Duplicate installed WAL voters could manufacture a majority.** The installer checked only the expected length and local-first order. Installing `[1,2,2]` let one follower acknowledgement contribute its same durable offset twice and advance a three-voter watermark before the local fsync. A regression failed before the repair. Installation now calls the proved complete/nonempty/local-first/unique-ID validator. The production placement fold is also moved into the verified crate: it derives distinct nodes and racks from actual candidates instead of relying on the one-row selector alone. Two compositions establish installability and that a full placement of at least three voters retains the original majority after any one configured rack is lost.

12. **A raised WAL floor is not fresh quorum evidence.** The production adapter computes `max(current_watermark, log_start)` before quorum recomputation. With both old watermark and all durable votes at 0, log start 5 raises the result to 5 without new support. The consumer limit then exposes no retained records. The new installed-voter/Fetch theorem distinguishes this case from an advance that exposes retained records and returns a concrete majority of distinct supporting IDs for the latter. A separate witness shows why an unchanged inherited watermark still needs prior durability evidence. These are proof boundaries, not a new runtime defect.

13. **The earlier quorum/Fetch composition dropped progress guarantees.** Although the quorum kernel already proved maximality and Fetch already specified its exact cap, the composition exported only bounds and conditional support. Replacing its body with `(current, i64::MIN)` still proved its original contract: it never advanced or exposed records. The composition now exports the quorum maximality law and the exact HWM/LSO/delivery minimum. The same replacement fails both guarantees, which the installed-WAL/Fetch theorem carries across its concrete identity and clamp projection.

14. **Matching offset ranges alone do not establish byte agreement.** The production promotion path already compared the encoded bytes in its overlap with the canonical log, but that comparison was outside the proof boundary. It now delegates each batch comparison to the proved `wal_batch_equal` kernel. The arbitrary-length copy composition connects those actual byte checks to live append, Produce acknowledgement, and recovery, establishing matching logical frontiers and exact encoded-byte extent. It also proves admission completeness for complete, byte-identical, contiguous copies that fit the file. The separate majority-byte recovery routine is compiled only in the local test harness; this change does not establish a production remote-voter byte-majority protocol.

15. **Scalar checkpoint bounds did not establish a recoverable batch prefix.** A checkpoint `0..1` inside a three-record batch `0..3` passed recovery's range checks. Whole-batch truncation then dropped that batch, leaving actual end 0 while recovery published checkpoint end 1 and returned success. The production regression fails before the repair. Nonempty recovery now checks the observed batch successor before mutation; the actual recovered end is already a known boundary. Empty checkpoints reset the log at their floor, so missing checkpoints or empty ranges at an interior floor cannot leave log end below log start. Legitimate nonempty interior floors remain accepted and stable through reopen. The new composition establishes complete boundary admission, the exact batch prefix retained by truncation, and the exact clamped Fetch cap. It does not prove filesystem checkpoint publication or header truthfulness. Partial-checkpoint validation reuses the existing raw reader and can scan one segment's tail; a header-only boundary reader would reduce that cost if needed.

16. **Recoverable interior floors could not be promoted.** Recovery correctly admits floor 1 inside batch `0..3`, but promotion passed logical range `1..6` to an exact reader that requires first batch base 1. A real production regression failed with an incomplete-range error. Promotion now validates whole batches covering the logical floor, resets an empty destination at their physical base, copies those exact bytes, and restores the logical floor through the existing trim planner. Existing exact readers keep requiring exact starts. Overlap comparison also checks the whole containing batch. A higher canonical floor is checkpointed in the durable follower before rebasing, so a failed first destination append and reopen cannot forget it; this reuses the same synchronous trim path as asynchronous follower trimming. The regression covers empty, partial, already-complete, and higher-floor destinations, injected append failure before any destination records, stable reopen/retry, and exact retained bytes.

17. **A recursive exact-range reference could share an incorrect gap policy.** Removing contiguity from both the reference and executable validator still proves the original equivalence contract. The validator now additionally exports an independent global layout and bounds every admitted batch inside the requested physical interval. The same gap-accepting mutation fails those guarantees. The new covering-copy composition consumes that global layout to establish a concrete byte-identical copied-batch witness for every offset in the retained logical window, rather than relying on offset bounds alone.

18. **Byte-identical promotion did not preserve live transaction semantics.** A real WAL promotion of transactional data followed by an ABORT marker and ordinary data copied the correct bytes and reached end 3, but its live LSO stayed at 0 instead of 3. The header-only verbatim append treated the marker as transactional data. Replicated control batches now decode the marker and reuse owned append's transaction, producer, coordinator-epoch, sidecar, flush, and rollback bookkeeping while storing the original wire bytes. Data batches retain their header-only path. A regression compares ABORT, COMMIT, barrier, and unknown-control state against native append, then reopens and retries. The new composition independently binds actual key bytes and producer identity to closure, exact abort intervals, strict marker/HWM release, and the largest permissible read-committed Fetch prefix. Correct byte layout alone never established that state equivalence.

19. **General append admission does not prove contiguous WAL copying.** Replicated append intentionally accepts bases beyond the current end to preserve compaction holes. An earlier WAL-copy projection relied on that guard to establish contiguity. With gap-permitting append, it accepted batches `0..1` and `2..3` as an exact copy of `0..3`; its independent admission contract and existing gap test both failed. The projection now explicitly checks each source base against the copy cursor, matching the production exact reader. General replicated Fetch continues accepting compacted gaps, and its composition now distinguishes the divergence epoch that selects truncation from the end offset that supplies the cut.

20. **Trim checkpoint ordering could make a durable replica unrecoverable.** The native log published floor 1 before the WAL range advanced from `0..3`. An injected directory-sync error after that native rename left the files durable but recovery rejected the WAL checkpoint as outside actual range `1..3`. The follower now fsyncs all retained bytes, publishes the clamped WAL floor and synced end, then applies native trim and syncs its remaining state. Old and new native floors both fit the published WAL range. A production table covers 24 combinations of interior/end/overshooting trim, a prior checkpoint behind the tail, pre-sync failure, WAL checkpoint-write failure, and native floor failure before/after rename, with reopen and retry. The new aggregate establishes recoverability of the old/new durable prefix and the exact Fetch window after each modeled completed publication stage, including partial native floor advancement. It retains strict rejection of inconsistent legacy checkpoints; it does not prove all filesystem interleavings or the separate reset/truncate protocols.

21. **Snapshot row validity alone does not establish client retry behavior after recovery.** A valid persisted last batch must reconstruct its first sequence modulo 2^31, retain its original physical span, and classify a same-epoch retry as a duplicate at that original offset. The new composition connects snapshot admission, sequence arithmetic, the actual five-slot decision, append coordinates, and the acknowledgement frontier. It also proves older epochs fence, newer epochs cannot reuse the duplicate, and the same-epoch successor appends. Existing producer specification helpers are exposed as open logical definitions so other modules can use their exact meaning; their bodies and executable policy are unchanged. PID lookup, snapshot decoding, and absence of a replayed tail are explicit host facts. Marker-only entries have no retained data batch and are outside this theorem.

22. **A success flag hides the snapshot and replay witnesses.** The older truncation/snapshot composition checked bounds in its body and returned `true`. Its single postcondition did not describe the selected snapshot or cursor to another theorem. It now returns those witnesses, proves that selection names a globally newest eligible snapshot, rejects selection from the discarded suffix, and specifies the exact replay cursor as the maximum of the local floor and selected snapshot (or the logical floor when none survives). This rules out both an artificial empty selection and a cursor that skips all remaining replay while still staying within the old bounds. Duplicate newest offsets may name either matching index; decoding and corrupt-file fallback are separate host operations.

23. **Newest-survivor selection does not by itself prove corruption fallback.** The loader repeatedly drops a corrupt newest snapshot and retries, while a read I/O failure must stop rather than silently load older producer state. The new composition folds the actual newest-selection and replay-cursor kernels over candidate removals, preserves original snapshot identity, and derives the global newest non-corrupt outcome. Successful reads return the exact replay cursor; an I/O failure names the newest remaining non-corrupt candidate and prevents replay; all-corrupt/no-survivor recovery uses the exact logical/local floor. Arbitrary unordered and duplicate offsets are supported. This is a protocol projection over faithful read classifications, not a proof of decoding, directory enumeration, pruning/unlink durability, or error payloads.

24. **A recovered retry must name the matching retained batch, not the current tail.** Snapshot-only reconstruction does not establish answers for the four earlier batches replay can retain. The new window composition calls that reconstruction theorem for each coherent data row, installs the actual four-earlier/last-at-slot-four layout, and derives complete duplicate membership, first-match identity, original physical base and exclusive acknowledgement frontier, and non-duplicate policy from the newest row. Sequence wrap can alias retained rows, including an earlier match at the next sequence; neither “retry always names the newest batch” nor an unconditional successor-append law is sound for a populated window. A sparse real-log witness spans a full sequence wrap without allocating billions of records and returns the earlier alias. Complete replay, ring generation/eviction, faithful metadata projection, timestamps, and persistence remain host obligations.

25. **Token visibility is not established by its final boolean predicate alone.** The new composition folds complete User-owner ACL rows through resource, principal/host, operation and deny/default decisions, then connects token API admission to owner/requester/renewer/ACL visibility. It proves the complete visibility criterion: CreateTokens alone grants no description access, an unrelated resource ACL suppresses the no-ACL fallback, and neither superuser/relationship privilege nor an ACL grant bypasses a token-authenticated session or an excluding owner filter. The raw reference permits only All/DescribeTokens for this User operation. Faithful matching facts and complete enumeration, the separate Describe decision for this token, token-service configuration, HMAC generation, and response encoding remain host obligations.

26. **Quota selection is not a conservation proof for production charges.** The selector proves which configured candidate supplies the rate. The adapter constructs sensor tags from the concrete request and selected level; a default-user configuration can therefore feed a bucket tagged with the actual user, rather than the configured default entity. The existing verification table incorrectly described the returned key as coming directly from the selected candidate; it now distinguishes rate selection from sensor-key construction. Production request/producer quota enforcement uses record/record_bounded to charge the whole request and retain debt. plan_consume and the bucket model prove grant/refill conservation, not full debt accounting. A composition that treated granted tokens as all charged bytes would claim the wrong policy. The debt-accounting kernels and compositions below now cover the integer charge/refill/refund transitions, independently of quota lookup and sensor-key construction.

27. **Refund restoration has a debt-loss boundary.** The runtime comment claimed that a full refund always restores a charge's starting balance. At debt `u64::MAX`, charging one micro-token saturates and refunding it leaves debt `u64::MAX - 1`; a bounded charge can likewise discard debt and make its full refund over-credit the prior balance. The comment now states the required absence of discarded debt and intervening operations. The actual debt-first refill/refund and full-request charge arithmetic is extracted into `quota_credit` and `quota_charge`, both used by the production bucket. One composition establishes exact charge/refund restoration of the refilled signed balance and next consume budget, with complete proof admission exactly when the charge's debt is representable. Another establishes the exact balance after a capped charge and repayment, complete debt clearance once credit reaches the cap, and a guaranteed consume budget from credit beyond that cap. The proof wrapper excludes unrepresentable debt; production still saturates. Effective micro-token conversion, the rate/wait-to-cap calculation, clock claims, locking, and sensor identity remain host boundaries.

28. **The clock-claim adapter credited the same interval twice.** The retained production regression reproduces rate `1500 * MICROS_PER_TOKEN`, zero initial balance, and a fixed clock reading of 1 ns: calling `BucketState::consume(1, 0)` twice increased available micro-tokens from 1 to 2 while `last_refill_nanos` stayed 0. Converting rounded micro-tokens back into rounded claimed nanoseconds left the interval available to credit again. The adapter now claims the entire elapsed interval and stores its fractional micro-token numerator separately, preserving it across nonzero rate changes and discarding it at the burst. The production `quota_refill` kernel also keeps the wide credit through debt repayment, so a large refill can repay `u64::MAX` debt and still fill a representable burst. A new composition proves that any two-way partition of an elapsed interval gives exactly the same consume budget, debt, and fractional credit as a single refill, including burst saturation. Its aggregate rational-time ledger is independent of the kernel's local specification. The native regression checks repeated/backward clock readings, low/high rates, debt, and extreme counts against elapsed time since the initial anchor. The existing bucket model uses bounded whole-credit clocks and cannot exercise fractional nanosecond claims; its documentation now states that boundary. Accurate micro-rate conversion, serialized claims, and clock provenance remain host obligations.

29. **Conservation alone permits permanent starvation.** Replacing the production whole-token request selector with zero still proves boundedness and whole-token quantization. It also proves an arbitrary consume trace whose only aggregate guarantees are credit conservation and no overgrant: credit can remain in the bucket forever. The strengthened composition additionally requires a final request large enough to drain the burst to leave less than one whole token. That service guarantee rejects the zero-selector control. The extracted `quota_whole_request` kernel preserves the existing production expression and proves exact maximal whole-token selection; it is shared by the runtime and bounded model. Fixed positive rate/burst, serialized claims, and faithful clock readings remain host obligations. Zero-rate unlimited grants, rate changes, and intervening charge/refund operations are outside this trace.

30. **An exact ListOffsets decision table is not a complete visible-record lookup.** A matched candidate and a supplied isolation frontier do not establish either existence or first-record identity. The new composition connects the actual sparse timestamp scan, complete unstable-start fold, consumer/replica isolation choice, wire timestamp classification, and response selection. It resolves exactly when the decoded window contains a matching record below HWM and, for read-committed consumers, every pending transaction start; replicas use the log end. The returned offset/timestamp names the global first match, even with regressing timestamps, equal timestamps, and offset gaps. Scanning first and clamping afterward is complete because offset order, rather than timestamp order, prevents a later match from re-entering the visible prefix. A reference table edited together with its implementation can still justify unconditional unknown answers or an inclusive frontier. Independent aggregate contracts reject both. Complete decoded windows, truthful sparse maxima, coherent captured frontiers, epoch projection, and cross-tier enumeration remain host obligations. Scheduled-delivery gating is an additional Fetch/LATEST boundary, not part of positive-timestamp ListOffsets.

31. **Correct per-tier timestamp searches do not justify remote priority.** The copy pass continues after a failed copy, so a later finished object can coexist with an older local-only gap. The real-file regression originally returned remote offset 4/timestamp 2000 rather than local offset 2/timestamp 1600 for target 1500. A new candidate kernel chooses the least offset while preserving its actual timestamp, and the union composition proves complete first retained/visible membership across independently ordered, overlapping tiers. Combining candidates exposed the local scan’s missing logical retention floor: physically retained offset 2 still won after the floor advanced to 4. Both local windowed decoding and remote lookup now carry the logical floor; a new first-record kernel rejects earlier records even inside the same decoded batch. The native regression checks floors 4 and 5, and remote-reader tests check interior, end, and extreme floors. Timeout/oversized-remote-record failure policy remains explicit; successful remote hits below the local logical floor can bypass the later local scan. Complete per-tier enumeration, truthful decoded arrays, and coherent frontier capture remain host boundaries; overlapping remote-object enumeration is not proved by the two-tier union theorem.

32. **The timestamp type was outside the coordinate proof.** Remote byte decoding always used producer base time plus record delta, even for a `LogAppendTime` batch. A real encoded-batch regression fails to return a valid append timestamp of 2400 for target 1500. Local decoding substituted the append stamp only after evaluating producer arithmetic, so an ignored `i64::MAX + 1` field incorrectly rejected the batch. The new shared `timestamp_record_time` kernel chooses append time before arithmetic; both actual readers use it. A raw-record composition connects that choice to checked absolute offsets, the floor-aware record scan, cross-tier candidate selection, and exclusive ListOffsets visibility. It proves first-record identity, exact type-dependent wire timestamp, and completeness, including arbitrary ignored producer arithmetic under append time. CreateTime requires valid mathematical timestamp sums; decoding, the batch-type bit and append-stamp projection, and complete enumeration remain host obligations.

33. **Abort-row soundness permits missing every abort.** The old narrowing theorem returned a boolean and accepted invalid indexes without a selection witness. It now returns exactly the qualifying original row indices or rejects an invalid complete index. That completeness claim exposed two production omissions: remote Fetch read only the data segment’s transaction index, although the abort marker and its index can be in a later remote segment, and it ignored a later local marker. RemoteReader now scans the finished remote tail, and Fetch merges overlapping local index rows before deduplicating wire pairs. The new union theorem proves exact membership and uniqueness across both sources. A real encoded-log/archive/eviction regression covers remote-only, local-only, and duplicate marker ownership. Accurate complete index enumeration, current lineage, object truthfulness, captured frontiers, and client-side filtering remain external. Full-tail I/O and quadratic duplicate lookup have explicit profiling ceilings.

34. **Record filtering must preserve more than survivor offsets.** The old selection theorem exported only a boolean and returned `true` for invalid coordinates, leaving no selected-record evidence for a caller. It now exports exact original-index witnesses, complete invalid-input rejection, the original frontier and batch classification. The new composition sends every survivor through actual rewrite admission and connects the full archived span to modulo sequence arithmetic and producer-snapshot retry reconstruction. Dropping a trailing record must not shrink the batch sequence span; dropping every record still leaves the original header span. The real materializer already applies this rule, but its stale rationale incorrectly claimed local append always requires exact contiguity. The corrected explanation names the producer retry/sequence guarantee. The native regression restores encoded filtered/empty batches, reopens the log, checks complete header/record equality and producer metadata, and classifies the original retry at ordinary and wrapping sequences. Decoding, snapshot publication, complete replay and PID routing remain host obligations.

35. **A control exposed fifteen opaque boolean-only compositions; twelve remained at that checkpoint.** Replacing all fifteen `#[ensures(result)]` bodies with unconditional `true` still proves all fifteen files, and all 48 composition tests still pass. Their existing bodies check useful relationships through other kernels, but the exported contract promises only `true`; another theorem cannot reuse the checked relationship, and removing the body checks leaves the promised contract and tests intact. The append, reservation and read-committed Fetch contracts have since been repaired with concrete witnesses and independent oracles; twelve entries remained boolean-only procedural checks at that checkpoint. They still need relational postconditions or concrete returned witnesses, following the repaired snapshot, abort and restore-selection compositions. This is an open audit finding, not a claim that all composition contracts are now strong. All temporary replacements and proof artifacts were restored byte-for-byte.

## Compositions established

[`composition.rs`](../crates/verified/src/composition.rs) is compiled only for Creusot and tests. It introduces no production API or runtime work. The generated obligations call the existing kernels through their proved contracts; they do not copy kernel implementations into new reference models.

| Theorem | Guarantee | Remaining boundary |
| :--- | :--- | :--- |
| `append_frontiers_agree` | Returns the actual append, reservation, recovery, acknowledgement and scan outputs. Admission is exactly a nonnegative, representable batch span; every returned exclusive frontier is equal and every inclusive last offset agrees. | All paths must describe the same decoded batch. The one-byte recovery extent checks offset arithmetic only; byte integrity, fsync and concurrency remain host obligations. |
| `reservations_do_not_overlap` | Returns the actual start, split and end of two admitted reservations through the pending-frontier kernel. The two half-open intervals are adjacent, positive and disjoint; admission rejects either invalid count or total overflow. | The controller serializes use of the returned frontier. Failure rejects the proof pair and does not imply rollback of an already issued first reservation. |
| `reserved_pair_preserves_recovery_and_ack_order` | Two admitted reservations feed the concrete append witnesses. The second batch starts at the first acknowledgement frontier, so its offsets do not overlap the first batch and its acknowledgement, recovery and scan frontiers advance strictly. Invalid spans or combined overflow reject the pair. | The batches are accurately decoded and controller reservations are serialized. Byte integrity, fsync, quorum votes, concurrency and rollback are external. |
| `committed_fetch_excludes_unstable` | Returns the actual unstable frontier and all Fetch visibility fields. The consumer limit is the greatest prefix bounded by log end, HW, delivery and every supplied unstable start; starts beyond log end reject the input. | The starts are a coherent complete or minimum-equivalent projection of actual transaction state. The host uses all open starts and the earliest unreplicated key; map ordering and state maintenance remain external. Negative raw starts conservatively hide the window rather than admitting readable negative offsets. |
| `control_marker_bounds_committed_fetch` | Actual control-key bytes, matching producer identity, checked marker geometry, transaction closure, abort intervals, earliest unstable starts, and read-committed Fetch compose. Only matching COMMIT/ABORT markers close; the transaction still blocks until HWM strictly passes the marker's last offset. The returned limit is maximal subject to every remaining transaction and HWM/delivery/end, excluding artificially empty Fetch. | The admitted marker span and complete other live/unreplicated starts describe a coherent host state. Decoding, sidecar durability, stamping, and concurrent state application remain external; the production promotion/reopen regression checks the shared adapter. |
| `stable_abort_sources_cover_fetch` | Derives LSO from transaction state, replaces the inherited LSO, and returns the greatest bounded Fetch prefix plus exactly every qualifying unique abort row from both complete source indexes. A later marker owner is still represented, and an invalid source rejects the whole result. | Starts, indexes and watermarks must describe one coherent log lineage. Complete source enumeration, authoritative state projection, requested-floor authorization, bytes and client record filtering remain external. |
| `quorum_commit_bounds_fetch` | When HWM advances, at least the configured quorum of counted entries reaches the consumer Fetch limit; HWM remains inside the log and is maximal above the epoch gate. The Fetch limit is exactly the HWM/LSO/delivery minimum. | Entries represent distinct voters and matching log prefixes. An inherited unchanged HWM needs prior-epoch durability evidence. This is the consensus kernel, not the separate ISR high-watermark algorithm. |
| `installed_wal_quorum_bounds_fetch` | The actual voter validator, clamped explicit durable votes, majority computation, and consumer Fetch yield concrete distinct installed supporters at/above the exclusive limit when an advance exposes retained records. Exact configured size is checked; the unsynced leader end contributes no extra vote. Admission is complete for valid ID/count/projection shapes. The watermark is maximal among quorum-supported frontiers inside the log, and the Fetch limit is exactly the minimum of HWM, LSO, and delivery. | Reported offsets faithfully represent fsynced matching prefixes. Current/log-start floors need prior evidence; floor-only advancement exposes no retained records, and unchanged inherited watermarks carry no fresh support promise. The returned supporter list is a proof/test witness, not a production allocation. |
| `checked_wal_copy_replays_exactly` | An arbitrary sequence of actual byte-equal batch copies makes live append, Produce acknowledgement, and tail recovery agree on the exact exclusive end and total encoded-byte extent, with strict logical/byte progress for a nonempty copy. Success preserves every batch’s metadata and bytes; every complete valid copy that fits is admitted. Gaps, overlaps, metadata/byte divergence, zero-byte batches, incomplete copies, extent exhaustion, and wrong target ends are rejected. | Batch metadata faithfully decodes the compared bytes. Reading, copying, fsync, checkpoint publication, concurrency, and quorum-wide byte agreement remain host obligations. This is a pure complete-copy projection, not a crash/durability proof. |
| `checkpoint_truncation_bounds_fetch` | For arbitrary-length ordered physical batch ends, recovery admits exactly scalar-valid empty ranges or whole-batch ends. Nonempty truncation retains exactly the batches at/before that cut and reaches that exact end; empty recovery resets at its logical floor. Subsequent HWM/LSO/delivery clamping gives the exact consumer Fetch minimum and cannot expose the discarded suffix. Interior logical starts and integer extremes are allowed. | The ends are the complete accurately decoded local batch sequence, with physical start at/below logical start and the observed log end matching its final boundary. The returned count describes truncation before later prefix trimming. Byte preservation, actual I/O, fsync, concurrent changes, and checkpoint publication remain host obligations. |
| `published_trim_bounds_recovery` | Capping and reconciling an active trim, publishing its WAL range before native floor advancement, whole-batch recovery, and Fetch compose after every modeled publication stage. Before publication, recovery retains exactly the old durable prefix; after publication, either native floor recovers the synced full prefix at the new floor. The retained batch count and visible-offset membership are exact, including empty ranges and interior floors. | Atomic whole-file publication, truthful fsync completion, prefix-only unlinking, accurate remaining batch rows, and a coherent initial checkpoint are host facts. Partial native floors stay below the already published WAL floor. Reset/truncate and legacy inconsistent checkpoint repair are separate protocols. |
| `reloaded_snapshot_preserves_last_batch_retry` | An admitted snapshot data row reconstructs its first sequence modulo 2^31 and original base/acknowledgement frontier. Its same-epoch retry identifies slot 4 of the actual retained-batch ring, older epochs fence, newer epochs obey the first-sequence rule, and the next same-epoch sequence appends. Admission is exact and cannot be satisfied by rejecting all rows. | Faithful PID lookup and snapshot decoding, no replayed tail, and installation of the last batch in the host ring. Dedup compares sequence ranges, not payload bytes. Marker-only entries have no data batch to retry. |
| `replayed_window_preserves_first_retry_coordinates` | Snapshot-row reconstruction, the actual five-slot layout, sequence matching, and acknowledgement arithmetic compose for every populated coherent data window. A duplicate exists exactly when a same-epoch row matches; it returns the first matching row’s original index, physical base, and exclusive frontier. Non-duplicate classification uses the newest row. Aliases and unused earlier slots are allowed. | Complete same-PID/epoch replay, faithful row projection/order, retained-window generation and eviction, and timestamps are host facts. This does not equate a snapshot-only restart with a tail-rebuilt window, or compare payload bytes. |
| `token_description_preserves_authentication_and_acl_isolation` | Complete User-owner ACL folding derives the grant from matching rows and resource-level absence. Authentication admission and token visibility then prove the exact combined criterion, including deny precedence, unrelated-resource ACL default suppression, owner filtering, and refusal of token-authenticated sessions despite privilege. CreateTokens never implicitly grants DescribeTokens. | Configured token service, coherent complete ACL enumeration, faithful equality/prefix/CIDR and principal facts, the separate Describe grant on this token, HMAC and response encoding. The fold projects the native deny short-circuit; it does not prove alternative authorizers or authentication-state transitions. |
| `typed_timestamp_records_preserve_visibility` | Timestamp-type decoding, checked absolute offsets, floor-aware record selection, cross-tier minimum selection, and exclusive visibility return the first retained visible raw record with its exact type-dependent wire timestamp. Append time completely replaces producer arithmetic, including overflow; valid CreateTime values keep their base-plus-delta time. | One accurately decoded complete data batch with strictly ordered, valid absolute offsets; valid CreateTime sums, correctly projected batch type/append stamp, and coherent floor/visibility/epoch. Encoding and I/O remain external. Real remote bytes and remote-reader index/object fixtures check the append-time path; local decoding checks ignored overflow and continued offset rejection. |
| `tiered_timestamp_lookup_preserves_first` | Logical-floor record selection, cross-tier least-offset selection, and exclusive ListOffsets visibility return the global first retained visible match exactly when it exists. The chosen timestamp/epoch remain attached to that record; overlapping tier ranges, gaps, and timestamp regressions are allowed. | Complete accurately decoded, individually ordered tier windows and a coherent supplied floor/visibility frontier. Per-tier object enumeration, overlapping-record consistency, I/O errors, epoch lookup, and concurrent capture remain external. The native gap/floor fixture checks the actual merger and both reader adapters. |
| `timestamp_list_offsets_finds_first_visible` | Sparse first-record selection and transaction-derived isolation give a resolved answer exactly when a matching record exists in the visible window. Every resolved answer preserves the first match’s offset/timestamp and supplied epoch, strictly excludes all pending starts for read-committed consumers, and uses LEO for replicas. | Complete same-window record arrays, truthful sparse bounds, coherent HWM/end and complete pending starts. Faithful decoding, cross-tier window construction, epoch lookup, authorization/leadership, and concurrent capture remain external. Positive timestamps only; scheduled-delivery Fetch/LATEST caps are separate. |
| `metered_consumes_conserve_elapsed_credit` | Arbitrary-length whole-token consume traces conserve the exact scaled balance plus all grants and burst losses against elapsed-time credit. Repeated/backward clocks cannot create time; debt never increases, grants cannot exceed earned credit, and a final request covering the burst drains every whole token. | Fixed positive rate and burst, coherent initial balance, positive storage units per token, and serialized state publication. The trace excludes zero-rate unlimited grants, rate changes, and intervening charges/refunds; clock provenance and floating-point conversion remain host facts. |
| `refill_partition_preserves_consume_budget` | Splitting elapsed time preserves the exact consume grant, remaining balance, debt, and fractional credit of the aggregate rational-time ledger, including burst saturation. | Fixed rate and burst, coherent initial balance, representable total elapsed time, and no charge/consume between refills. The host must claim each interval once and publish the returned fraction under its lock; native adapter regressions exercise that publication. |
| `quota_charge_refund_restores_consume_budget` | Debt-first refill, full charge, refund, and grant arithmetic restore the refilled signed balance and exactly the next consume budget. Admission is complete exactly when the charge's debt fits in `u64`; saturation cannot silently establish restoration. | A coherent bucket has at most one of available/debt nonzero and available at/below burst. Counts are effective micro-tokens; the charge/refund pair must be serialized without intervening state changes. Integer representability, unit conversion, clock claims, locking, and quota lookup are distinct boundaries. |
| `bounded_quota_debt_cannot_outlast_repayment` | Capped charge, debt-first repayment, and consume give the exact signed-ledger balance, clear debt after credit at least the cap, and grant the full probe when credit exceeds the cap by that probe and the burst fits it. The exact balance law excludes fabricated full bursts. | A coherent starting bucket, accurately computed integer debt cap and credited refill, and serialized state publication. The theorem bounds credits, not elapsed wall time; rate/wait rounding and real-time progress remain external. |
| `covering_copy_preserves_logical_fetch` | The actual trim planner, whole-batch covering validation, byte comparison, live append, Produce acknowledgement, tail recovery, and Fetch compose across an interior logical floor. Complete valid copies that fit are admitted; every visible offset has a concrete copied batch with identical coordinates and bytes, while offsets before either floor have none. Encoded-byte progress equals exactly the copied lengths. | Source batches are the complete physical sequence covering the reconciled logical floor, possibly starting before it. Faithful decoding and completed I/O are assumed. Production persists a higher canonical floor in the follower before physical rebasing and has an injected-failure/reopen regression; the theorem does not prove filesystem publication or all crash interleavings. |
| `validated_index_bounds_lookup` | Returns exact floor and ceiling byte cursors after complete archive validation, with source membership, global floor/ceiling extremality and byte bounds; invalid archives are rejected exactly. | The same entries and extent must describe the actual file. Empty-index zero is a fallback; structural validity alone cannot establish truthful batch rows. |
| `indexed_offset_scan_preserves_first_batch` | Connects sparse rows to complete physical batch rows, consumes validated cursors and reuses the scalar first-match kernel. Every matching batch lies at or after the floor; the indexed scan returns the globally first qualifying batch, bounded by a present ceiling. | Complete faithfully decoded last-offset/byte-position batch rows. Actual header lengths, the host's extra batch skip, windowed decoding and I/O remain external. |
| `loss_settlement_is_idempotent` | Returns the settled and replayed states, proves exact remaining count and generation, and establishes that replay preserves both fields. | Generation is below `u64::MAX`; saturation can otherwise reuse a generation with a remainder. Durability/marker parsing remain external. |
| `admitted_loss_marker_preserves_pending` | Consumes that witness with actual marker admission to conserve pending losses, reject duplicate admission, and admit a fresh generation exactly when losses remain. | Matching snapshot generation, snapshot count at most pending count, and no generation exhaustion. Parsed shape facts and durable publication remain host obligations. |
| `validated_time_cursors_are_monotone` | Returns actual absolute inclusive lower/upper cursors and a strict predecessor scan start, with complete extent/archive admission and exact last-row relations. | Canonical structural validity alone does not establish truthful record prefix bounds. An upper cursor cannot safely exclude later timestamp regressions. |
| `validated_epochs_bound_truncated_fetch` | Returns invalid-history rejection, an unplaceable epoch, or the actual Kafka epoch cut with exact clamped watermarks and consumer view. Complete row validation establishes lookup ordering; no resolved cut grows the log or exposes its discarded tail. | Complete faithfully decoded epoch history and an accurate original end. Batch-aligned cuts, reset below retained floors, durable application and coherent publication remain host obligations. |
| `resolved_epoch_bounds_retained_replay` | Consumes that cut to select a globally newest surviving snapshot and exact replay cursor. Rejects replay below either retained floor; a discarded-tail snapshot cannot suppress reconstruction. Invalid history and unplaceable epochs retain distinct outcomes. | Complete snapshot enumeration, trustworthy snapshot bytes and floors, batch-aligned truncation and actual replay/I/O remain external. Rejection below a floor means this retained-history path cannot be used; the production full-reset path is outside the theorem. |
| `truncated_snapshot_selection_bounds_replay` | Selection returns a globally newest eligible snapshot, or no selection exactly when none survives. Replay starts at exactly the maximum of the local floor and selected snapshot, or the logical floor when there is no snapshot. A valid call always returns a witness; discarded-tail selection, artificial empty selection, and unnecessary replay skipping are excluded. | Log/local starts are nonnegative and no greater than the cut, which is at or below the old end. Snapshot bytes, pruning persistence, and actual replay remain external. |
| `corrupt_snapshot_fallback_preserves_replay` | Repeated newest selection and corrupt-candidate removal preserve every non-corrupt candidate and original identity. The result loads a globally newest decoded survivor with the exact replay cursor, stops at a globally newest read I/O failure, or uses the exact fallback cursor when every eligible candidate is corrupt. Candidate removal proves termination; duplicates and arbitrary input order are allowed. | Read outcome classification, complete file enumeration, pruning and filesystem effects remain host facts. Order-preserving deletion projects the host swap-removal; any tied maximum is permitted. I/O error payloads and faithful bytes are not proved. |
| `restored_aborts_remain_bounded_when_fetch_shrinks` | Complete admission returns original row-index witnesses for exactly every abort intersecting the narrowed Fetch window, in marker order. Invalid indexes return `None`; valid indexes cannot be rejected wholesale. Selected starts remain below HWM, LSO, and delivery, and narrowing introduces no new overlap. | Complete decoded index and truthful owner extent. Starts may decrease or precede the owner segment. This proves exact filtering of supplied indexes; transaction reconstruction and index enumeration remain external. |
| `restored_abort_sources_cover_committed_fetch` | Archive admission and those witnesses compose with inclusive remote overlap, half-open local selection, consumer visibility, and production wire-row deduplication. Every qualifying `(producer_id, first_offset)` from either source appears exactly once; no other row appears. Marker owners may lie entirely after the fetched data. | Complete accurate current-lineage source indexes, coherent frontiers, decoding, I/O, and client record/control filtering. The real Fetch regression covers remote-only, local-only, and duplicated markers in later segments. Missing optional indexes and full-tail scanning remain host policies. |
| `segment_maximum_proves_delivery` | Delivery of the actual maximum batch activation time is equivalent to delivery of every batch, including empty sets and signed/overflow boundaries. The result exports a quantified aggregate contract used by the prefix theorem. | Activation times are the complete decoded batch-header set. An unknown cached maximum cannot stand in for this computed maximum. |
| `scheduled_prefix_bounds_fetch` | Returns the greatest delivery frontier and exact consumer/follower views. Every waiting batch bounds the frontier, which is the end or an actual waiting batch base. Rejects malformed or incomplete walks, including corrupt tails after the first waiting batch. | Complete aligned decoded batch/timestamp arrays from one log window. Gaps and nonmonotone activation times are allowed; cached-cursor invalidation and clock accuracy remain external. |
| `scheduled_stable_prefix_bounds_fetch` | Consumes that frontier and the derived transaction LSO to expose exactly the greatest consumer prefix bounded by HWM, every waiting batch, and every unstable transaction start. Follower replication reaches the end. | Complete coherent batch/time and unstable-start arrays; every start is at most the end. Inherited delivery/LSO fields are ignored. Decoding, input completeness and durable host publication remain external. |
| `fenced_replication_bounds_fetch` | Stale/error responses preserve log end and HWM; divergence cannot grow either; successful append coordinates plus monotone HWM advancement bound consumer Fetch by the new local end. | Initial HWM is no greater than log end. The host must apply the same plan under its target lock. The composition uses the KRaft HWM kernel; the data-replica async helper's equivalent arithmetic is outside Creusot. Bytes, identity projection, and durable application remain external. |
| `restore_selection_respects_batch_extent` | Returns the complete archived frontier, Keep/Empty/Filter decision, and original selected indices in source order. Every qualifying row appears, no excluded row appears, and every survivor stays inside the original span. Invalid coordinates reject the complete input instead of returning `true`. | Complete decoded data rows and faithful exclusion facts. Selection alone does not establish encoded payload integrity, legal producer identity, or producer-state publication. |
| `filtered_restore_preserves_producer_retry` | Exact selection, rewritten header/record admission, modulo sequence arithmetic, and snapshot reconstruction return original-index/offset/timestamp witnesses and preserve the original retry sequence and acknowledgement frontier. Empty and trailing-filtered rewrites retain the full archived sequence span. | One admitted last non-transactional idempotent data batch with ordered deltas and truthful timestamp bounds. Payload encoding, snapshot durability, complete replay, PID routing, and ring installation remain host obligations; the native regression checks filtered/empty encoded restores through reopen and retry classification. |
| `constructed_wal_placement_is_installable` | Returns the actual node/rack placement, exact node-ID projection, and exact installer admission. Selected identities and racks are distinct; incomplete nonempty selections block every remaining candidate. | Faithful node/rack metadata projection and durable membership transitions remain external. Greedy selection is maximal, not globally maximum when metadata conflicts; the contract permits tied choices rather than pinning full greedy order. |
| `wal_placement_survives_one_rack_loss` | Consumes that witness and returns exactly all surviving node IDs in placement order, installer admission and quorum capacity. Any one rack removes at most one voter; every installed configuration of at least three voters retains its original majority. Incomplete installation cannot claim quorum capacity. | Physical failure-domain identity, survivor communication, fsync, election scheduling and durable membership transitions remain external. This is capacity under one-rack loss, not unconditional availability. |
| `trim_steps_converge` | Arbitrary traces of completed/paused steps keep a fixed global frontier, never regress either store, catch WAL up after one completed step, and catch both stores up after two. Local advancement requires WAL already at the frontier; replay after completion cannot advance either. | A completed step durably applies exactly the selected store frontier. Paused/failed entries leave observed state unchanged; partial I/O, checkpoint durability, acknowledgement loss, and concurrent mutation remain external. This is an aggregate theorem over the existing application kernel. |
| `admitted_trim_bounds_reload_and_retry` | Rejects with the actual admission reason; accepted requests export the exact completed logical floor, latest retained snapshot index and replay cursor, bounded by HWM/delivery/end. | Prior store frontiers obey those caps. Snapshot contents, durable host completion and whole-batch replay remain external; the producer replay cursor is not a data-read floor. |
| `diskless_trim_reconciliation_preserves_coverage` | Exports actual physical WAL/cache frontiers, exact unchanged state for disabled plans and bounded progress/completion for enabled traces. | Prior WAL eviction obeys coverage/lag bounds. A disabled plan cannot repair unsafe inherited cache state. Committed object coverage and durable checkpoint completion remain external; physical eviction does not change the logical floor. |
| `running_maximum_index_entry` | The actual earliest-maximum selector constructs a sparse row that bounds every record through its batch and at/before its indexed offset. | Ordered offsets and accurate complete record timestamps; the indexed row may name the batch base while its prefix includes the entire batch. |
| `indexed_timestamp_scan_finds_first` | A strict-predecessor index cursor plus the existing record selector returns the global earliest qualifying record, or establishes no match. Timestamp regressions and offset gaps are allowed. | Every sparse maximum bounds records strictly before its offset; the scan reads the complete suffix of the same log. |
| `remote_timestamp_scan_preserves_first` | Returns the actual counted prefix, scan floor and globally first matching record, even with offset padding and unsorted conservative timestamps. Records below the returned floor cannot match. | Every row with earlier records must bound those timestamps; zero-offset padding imposes no record bound. A complete suffix of the same log must be read. |
| `validated_remote_and_local_time_starts_agree` | Rejects exactly structurally invalid archives; valid inputs return the actual count and identical remote/local scan floors, with global row ordering and extent bounds. | Canonical rows exclude raw trailing padding. Agreement alone does not establish truthful prefix bounds or complete record bytes. |
| `constructed_time_index_preserves_first` | Returns the actual sparse rows with exact attained prefix maxima, source offset coordinates, monotone row columns, global prefix bounds and the globally first matching record or complete absence. | Complete faithfully decoded ordered record offsets and valid indexed/through positions. Rows here name actual record coordinates; header-only or padding coordinates, file encoding and I/O remain external. |
| `constructed_index_retained_candidate` | Consumes constructed index bounds to return the original index of the first retained matching record. A pruned match cannot hide a later retained match; no result excludes every retained match. | Coherent nonnegative base/floor and representable absolute record coordinates. Complete decoded windows and faithful row construction remain external. |
| `constructed_tiered_timestamp_preserves_first` | Consumes each tier's retained result, chooses the least absolute offset in their union and applies exclusive ListOffsets visibility while preserving the chosen record's timestamp and supplied epoch. Unknown excludes all retained visible matches. | Individually ordered complete tier windows may overlap and timestamps may regress. Cross-tier record consistency, epoch lookup, frontiers, byte decoding, I/O errors and publication remain host obligations. |
| `validated_retained_time_scan_agrees` | Checks decoded record shape, order, extent and every row’s prefix bound; consumes actual remote/local cursors to return the original first retained match, or exclude every retained match. Invalid inputs are rejected exactly. | Complete faithful decoding and enumeration, archived-byte parsing and physical reads remain external. Structural validity cannot substitute for the semantic bound checks. |
| `completed_trim_preserves_retained_timestamp` | Consumes the completed logical trim and snapshot replay witness to preserve the first retained timestamp match, including matches below the producer replay cursor. | Complete decoded windows, snapshot contents and coherent durable trim completion remain external. |
| `physical_eviction_routes_retained_timestamp` | Consumes physical frontiers to route the unchanged logical first match to surviving cache or committed remote coverage, preserving coverage/lag bounds. | Both inherited physical frontiers obey those bounds. Correct object coverage, complete record decoding and successful physical I/O remain external. |
| `constructed_time_range_preserves_first` | Consumes constructed prefix maxima and validated inclusive/strict cursors to return the globally first retained record in a closed timestamp interval, or complete absence. Invalid segment extents and out-of-range records reject exactly. | Complete faithfully decoded record enumeration, valid sparse row positions and coherent logical floors are external. Timestamps may regress; the upper cursor cannot crop the scan tail. |
| `stable_time_range_preserves_first` | Consumes the derived transaction/HW/delivery prefix and constructed interval scan to return the globally first retained match strictly below the actual visibility limit, or complete absence. Its limit is maximal under the supplied gates; inherited LSO is ignored. | Complete decoded records, a valid minimum-equivalent transaction snapshot, coherent publication, physical reads and client-side abort filtering remain external. Omitted transaction entries are not validated by the supplied-start guard. |
| `remote_coverage_bounds_local_retention` | Consumes actual remote interval geometry, derives whole-segment blocking, and returns the exact size/time prefix and exclusive deletion target. Every newly evicted offset is covered, including local gaps; selected segments remain covered at successor exhaustion. Every prefix eligible by size or expiry is selected independently of reference equality; each selected non-expired row fits the initial debt together with all older rows. | Ordered valid local segment ranges and truthful expiry/size facts, complete finished-copy metadata, coherent publication and durable physical deletion remain external. Coverage below an inherited oldest-local start is not established. |

The implication-shaped checks explicitly return true on rejected input. They prove a property of admitted operations, not successful admission of every input. `composition_boundary_witnesses` and `restored_state_composition_boundaries` exercise successful paths, corruption, empty sets, exact boundaries, epoch gating, repeated timestamps, cross-segment/interleaved transactions, stale snapshots, and integer exhaustion. This separates legitimate conditional safety from a vacuous all-reject decision: the decision kernels themselves still have admission-completeness contracts, and the token negative control checks that distinction directly.

## Every original proof module

**Invariant** means an independent aggregate, bound, conservation, or progress property. **Selection** means global extremality/membership/absence rather than equality to the search loop. **Guard** means a specified decision over supplied facts; its value depends on correctly constructing and applying those facts. A module can contain more than one kind.

| Module and executable/proof functions | Assessment | Critique and useful composition |
| :--- | :--- | :--- |
| [audit](../crates/verified/src/audit.rs): `audit_checkpoint_admission`, `audit_loss_marker_admission`, `settle_loss_batch`, `spool_append_decision` | Invariant + guard | Byte-cap preservation and exact loss conservation are substantial; checkpoint/signature inputs remain host facts. The new settlement composition proves duplicate replay is harmless before generation exhaustion. |
| [authz](../crates/verified/src/authz.rs): `acl_identity_match`, `acl_resource_match`, `acl_operation_match`, `request_auth_admission`, `acl_decision` | Guard | Deny precedence and one-way operation implication are useful policy contracts. Identity OR and handshake API tables add little alone; the token-description composition now connects complete row matching and deny/default folding to session admission and visibility. Faithful string/CIDR facts, complete ACL enumeration, and connection-phase transitions remain host obligations. |
| [barrier](../crates/verified/src/barrier.rs): `barrier_target_count_decision`, `barrier_marker_fence_decision`, `barrier_placement_decision`, `barrier_cut_classification` | Guard | Fence equality and checked target-count expansion prevent stale appends and overflow. Complete/Partial is only a boolean classification; neither it nor placement admission proves every target received a durable marker. |
| [break_glass](../crates/verified/src/break_glass.rs): `break_glass_admission`, `select_break_glass_candidate` | Selection + guard | The selected candidate is a global lexicographic minimum, not merely an eligible member. Admission alone cannot establish distinct approvers or one-time durable spending; those need lifecycle/cross-spend models. |
| [break_glass_persistence](../crates/verified/src/break_glass_persistence.rs): `break_glass_consumption_decision`, `break_glass_local_action_decision` | Guard | The local action depends on the named spend state and commit result. The missing link is that a committed controller consume and local action refer to the same proposal and survive restart. |
| [broker](../crates/verified/src/broker.rs): `replica_fetch_mutation`, `preferred_rebalance_admission`, `fetch_visibility`, `delete_records_trim_decision`, `delete_records_trim_application`, `effective_share_backlog`, `find_coordinator_admission`, `unclean_recovery_commit_admission`, `java_string_hash_partition` | Invariant + guard | Fetch bounds are explicit, unconditional safety properties. Trims, fencing, coordinator policy, and exact Java hash routing have useful contracts; target matching and consistent watermarks are host obligations. Quorum/transaction composition now connects the bounds to their producers. Trim-step folding and admission/retry/reload composition expose the required inherited-store bounds and establish finite progress, convergence, and replay cursor safety. |
| [chain](../crates/verified/src/chain.rs): `select_chain_tip`, `chain_step` | Selection + guard | Tip selection proves global maximality and first-tie behavior. Sequence advancement checks exactly one link; digest/head matching is supplied as a boolean. An entire authenticated chain requires carrying that state across all links. |
| [checkpoint](../crates/verified/src/checkpoint.rs): `checkpoint_id_newer`, `latest_checkpoint_index`, `checkpoint_id_retained` | Selection + guard | Latest selection proves a global maximum independent of directory order. Keeping the selected/previous IDs is policy, not recovery safety; existence, integrity, and durable publication remain outside the contract. |
| [compaction](../crates/verified/src/compaction.rs): `compaction_decode_step`, `compute_horizon`, `retain_decision` | Guard | Retention decisions protect live transactional markers and newest values given accurate metadata. Per-record decisions do not establish whole-log key preservation or a correct newest-key index; the compaction model provides the independent aggregate check. |
| [consensus](../crates/verified/src/consensus.rs): `majority_size`, `election_has_quorum`, `failover_action`, `select_best_recovery_replica`, `lemma_hwm_threshold_has_member`, `lemma_hwm_member_maximal`, `election_jitter_ms`, `log_is_up_to_date`, `candidate_has_majority`, `recompute_high_watermark`, `majority_watermark` | Invariant + guard | Watermarks have quorum support, maximality, monotonicity, and epoch gating. These are strong contracts. Jitter proves only a range: returning zero always would pass. Vote counting assumes distinct members; the installed-WAL/Fetch composition now discharges that identity assumption, returns concrete supporters, and carries watermark maximality and the exact Fetch limit across the boundary. An unchanged inherited watermark and a floor raised only to log start carry no new support guarantee; these cases have runnable witnesses. |
| [delegation_token](../crates/verified/src/delegation_token.rs): `scram_credential_source`, `token_describe_visible`, `token_api_admission`, `token_mutation_decision`, `token_deadline`, `bounded_period`, `create_token_deadlines`, `renew_token_expiry`, `token_is_active`, `expire_token_deadline` | Guard; completeness repaired | Mutation formerly specified only necessary conditions for Append/Retry, allowing Reject for every request. Both outcomes now have iff contracts, including validity of same-expiry renew retries. Deadline saturation is exact; authentication, committed generation equality, and persistence remain host obligations. |
| [delivery](../crates/verified/src/delivery.rs): `scheduled_delivery_visible`, `delivery_watermark_advance`, `coalesce_delivery_range` | Invariant + guard | Checked deadlines fail closed and watermark advancement preserves the clamped log range. Coalescing pins endpoints. The maximum-time aggregate and complete-prefix compositions now connect actual batch deadlines to the candidate and consumer Fetch. Watermark bounds and identity on an admitted candidate are exported directly. Cached-state invalidation and a complete I/O walk remain host obligations. |
| [directory](../crates/verified/src/directory.rs): `directory_assignment_decision`, `directory_response_decision` | Guard | Slot assignment and controller-response fencing are complete case tables. They prove no durable assignment, directory existence, or safe disk movement; compose the response fence with application rather than treating each variant as an independent safety theorem. |
| [diskless](../crates/verified/src/diskless.rs): `diskless_retention_prefix`, `diskless_object_reclaimable`, `diskless_logical_range`, `diskless_span_extension`, `diskless_batch_step`, `diskless_trim_decision` | Invariant + guard | Retention is a maximal expirable prefix that keeps the newest range; logical selection and span extension pin membership and arithmetic; trim stays below durable/index bounds. The new reconciliation composition carries object coverage and safety lag through completed steps; the existing lag-normalization definition is open so callers can use the actual signed lag in their proofs. Reference absence, actual object bytes, fsync, and metadata commit are supplied by the host/model. |
| [epoch](../crates/verified/src/epoch.rs): `exact_epoch_successor` | Arithmetic invariant | Exact successor or exhaustion is stronger than nondecrease: it rules out silent saturation. It cannot prove that two concurrent callers consume different epochs; the state owner must serialize the update. |
| [features](../crates/verified/src/features.rs): `feature_update_decision` | Guard + safety corollary | The reference table is structurally close to the implementation, but the explicit prohibition on metadata-changing downgrades is an independent safety consequence. Support/dependency booleans and metadata-change history are host inputs. |
| [freeze](../crates/verified/src/freeze.rs): `freeze_timestamp_in_window`, `freeze_signature_decision`, `freeze_scope_decision`, `freeze_refuses`, `freeze_mutation_decision`, `freeze_replacement_decision` | Guard | Signature admission binds identity, skew, and replacement time; scope selection preserves specificity. Replacement is a case table over supplied stored-state facts. Neither crypto correctness nor a complete scan of overlapping scopes follows from it. |
| [group_migration](../crates/verified/src/group_migration.rs): `classic_upgrade_epoch`, `consumer_downgrade_epoch`, `group_migration_record_plan` | Invariant + guard | The record plan pins complementary writes/tombstones and synchronized member actions; epoch conversion is exact. Atomic append of the entire plan and replay equivalence across both group protocols remain unproved by these scalar contracts. |
| [isr](../crates/verified/src/isr.rs): `isr_admission`, `follower_caught_up_credit`, `isr_maintenance_selected`, `replica_isr_eligible`, `isr_candidate_selected`, `isr_proposal_changed`, `leader_high_watermark` | Invariant + guard | Admission pins current epochs and eligibility, and the high watermark stays monotonic inside the log. Some rules equal recursive policy models. The host supplies valid ISR membership, distinct replica identity, elapsed-lag facts, and coherent epochs; offsets alone are not byte-agreement evidence. |
| [jwks](../crates/verified/src/jwks.rs): `jwks_cache_admission`, `jwks_on_demand_refresh_decision` | Guard | Generation parity/equality and cache age prevent admission of a torn or stale snapshot given accurate reads. The proof does not show the writer publishes generations around every key change, or that the two generation reads enclose the copied key material. |
| [leader_epoch](../crates/verified/src/leader_epoch.rs): `epoch_and_offset_for_entries` | Selection | The result satisfies the predecessor/successor reconciliation relation under strict epoch/offset ordering. This is substantive search correctness. The new restore/lookup/truncation composition establishes bounded cuts and Fetch visibility. Durable application and follower convergence still need the reconciliation model and host invariants. |
| [list_offsets](../crates/verified/src/list_offsets.rs): `list_offsets_kind`, `list_offsets_epoch_decision`, `list_offsets_bound_decision`, `list_offsets_earliest`, `list_offsets_selection_decision` | Selection + guard | Version sentinels and fences are policy; earliest and visible bounds prove extrema and prevent record-derived results beyond visibility. The new composition establishes existence, global first identity, and exclusive isolation for a complete decoded window through the actual sparse scan and pending-start fold. Cross-tier enumeration, decoded bytes, coherent capture, and epoch projection remain external; Fetch has an additional delivery cap. |
| [local_recovery](../crates/verified/src/local_recovery.rs): `local_recovery_swap_action`, `local_recovery_swap_replaces`, `local_recovery_segment_chain`, `local_recovery_sealed_last`, `local_recovery_batch_step`, `local_recovery_index_frontier` | Invariant + guard | Tail steps make positive byte/offset progress without crossing the file bound, segment bases are ordered, and index spans fit u32. The step reuses restore arithmetic. Successful append/recovery/scanning frontiers now compose; fsync/rename facts and record decoding remain external. |
| [log_index](../crates/verified/src/log_index.rs): `offset_index_lookup`, `time_index_lookup`, `offset_index_position_at_or_after`, plus the new `time_index_scan_start` | Selection | Binary searches prove the global floor or ceiling, including the no-result case, not just a returned slot. Their all-pairs order assumption was previously an adapter gap; the offset/time compositions derive it from the actual row validators and bound the resulting byte positions or absolute cursors. The new strict predecessor composes with prefix maxima and record selection to exclude skipped timestamp matches. |
| [oauth](../crates/verified/src/oauth.rs): `oauth_session_admission` | Guard + arithmetic | Admission binds principal and positive bounded session lifetime under the supplied token facts. A signature-valid boolean is not a JWT proof, and an absolute wall-clock expiry does not establish timer scheduling or reauthentication behavior. |
| [offset_allocator](../crates/verified/src/offset_allocator.rs): `wal_reservation_epoch_ready`, `wal_reservation_frontier`, `wal_reservation_response`, `reserve_offsets` | Arithmetic invariant + guard | Reservations establish exact nonnegative contiguous ranges and reject overflow; response admission binds the observed epoch. Two serialized reservations and the pending-chain extension now prove gap/overlap freedom. This does not prove concurrent controller serialization. |
| [opa](../crates/verified/src/opa.rs): `opa_cache_admission`, `opa_cache_expiry`, `opa_error_decision` | Guard + arithmetic | A cache hit preserves its decision exactly before the deadline; expiry creation is checked and fail-open policy is explicit. Key completeness, monotonic time, and publishing the completed decision remain adapter assumptions; policy evaluation itself is not proved. |
| [produce](../crates/verified/src/produce.rs): `produce_durability_frontier`, `produce_batch_admission` | Arithmetic invariant + guard | Acknowledgement frontier is an exact exclusive successor, not an arbitrary larger offset. It now agrees with allocation, append, recovery, and scanning. Header admission still assumes decoded fields and does not prove actual record count, payload validation, or durable acknowledgement waiting. |
| [producer](../crates/verified/src/producer.rs): `increment_sequence`, `decrement_sequence`, `producer_decision` | Arithmetic invariant + selection | Sequences are exact modulo 2^31 and duplicate selection is the first retained match before the ordering decision. The host must retain the correct five batches and serialize producer requests; a four-way answer alone does not prove exactly-once append across restart. |
| [producer_id](../crates/verified/src/producer_id.rs): `producer_id_block_allocation` | Arithmetic invariant + guard | Allocation pins epoch fencing and exact block endpoints, including exhaustion. Sequential block uniqueness follows only when the controller persists and reuses the returned next frontier; failover/replay of that frontier is outside this function. |
| [producer_snapshot](../crates/verified/src/producer_snapshot.rs): `producer_snapshot_reload_log_start`, `producer_snapshot_reload_keeps`, `producer_snapshot_stray`, `producer_snapshot_latest_index`, `producer_snapshot_entry_valid`, `producer_snapshot_replay_start` | Selection + guard | Latest selection proves global maximality within the reload window; replay start and entry validity are precise. Truncation, latest selection, and replay-start construction now compose with explicit newest-survivor and exact-cursor witnesses. Trim admission/application also composes with selection against an advanced log start. A valid snapshot may contain older producer history, and a physical replay may read the entire batch around its cursor. Snapshot bytes are not proved equivalent to replaying the retained log, snapshot rows and coherent replayed windows now compose with retry classification and original acknowledgement coordinates, but generation and eviction of the complete tail-rebuilt dedup window remain host obligations. |
| [quorum_state](../crates/verified/src/quorum_state.rs): `quorum_state_write_decision`, `quorum_state_load_decision` | Guard | Signed JSON field bounds and restoration classification exclude malformed vote encodings. Correct field ranges are not an encode/decode round trip, and at-most-one vote per epoch across crashes still requires durable write ordering. |
| [quota](../crates/verified/src/quota.rs): `user_client_quota_precedence`, `ip_quota_precedence`, `quota_credit`, `quota_charge`, `quota_refill`, `quota_whole_request` | Guard + conservation | Exact/default precedence is a complete policy table. Candidate presence is supplied by the host, so the proof does not establish canonical lookup keys or shared bucket identity. The selected candidate supplies the rate; sensor-key construction uses concrete request tags and is outside this proof. The extracted charge/credit kernels now connect debt accounting to grant conservation; the compositions establish refund restoration only without discarded debt and bounded repayment with an exact consume budget. The scaled refill ledger and exact whole-token selector compose into arbitrary consume traces with both credit conservation and a final-request service guarantee. Unit conversion, clock claims, and atomic publication remain host obligations. |
| [raft](../crates/verified/src/raft.rs): `fetch_response_mutation`, `advance_high_watermark`, `in_half_open_window`, `frontier_reaches`, `control_history_frontier`, `metadata_record_offset_deltas` | Invariant + guard | Response actions are fenced and exclusive, watermark advancement is exact and monotonic, replay windows are half-open, and generated deltas are contiguous. Epoch/leader identity projection and durable log mutation remain outside these kernels. |
| [reassignment](../crates/verified/src/reassignment.rs): `reassignment_set_membership`, `reassignment_plan_admission`, `reassignment_action` | Selection + guard | Set differences are disjoint and handoff chooses the first eligible target. Catch-up and eligible handoffs are already-decided facts; the reassignment model checks leadership/set invariants through transitions, but actual replicated-byte catch-up remains external. |
| [reconfiguration](../crates/verified/src/reconfiguration.rs): `voter_reconfiguration_decision`, `add_decision`, `remove_decision`, `update_decision`, `finalize_decision` | Guard + safety corollary | Admission preserves a nonempty voter count and supported KRaft version under current leadership. A count-changing plan does not prove old/new quorum intersection or durability of the membership change; caught-up is a supplied fact. |
| [recovery](../crates/verified/src/recovery.rs): `replay_record_decision`, `barrier_recovery_fold_action`, `barrier_recovery_finalize_decision`, `replay_cursor_decision`, `replay_batch_cursor_decision`, `should_capture_first_downgrade` | Invariant + guard | Record windows and batch cursors prove bounded strictly increasing replay, while barrier folds classify records. Termination/progress is useful; metadata key decoding and the state reconstructed by a whole replay need the host adapters and replay model. |
| [registration](../crates/verified/src/registration.rs): `broker_heartbeat_decision` | Guard | A heartbeat from a mismatched registered epoch cannot be Current, and catch-up is tied to the metadata offset. This does not establish that a Current broker possesses the data log or that an epoch was durably registered. |
| [remote_metadata](../crates/verified/src/remote_metadata.rs): `java_long_hash`, `java_int_image`, `java_objects_hash`, `reverse_bytes`, `kafka_murmur2_int`, `kafka_to_positive`, `remote_metadata_partition`, `remote_metadata_resume_cursor` | Arithmetic compatibility | Nested cfg_attr contracts prove the full Java hash/Murmur2 partition result, not just range. Resume cursor is exact or rejected. Hash/byte compatibility is meaningful; hash distribution and ownership of the resulting metadata partition are separate properties. |
| [remote_read](../crates/verified/src/remote_read.rs): `tiered_earliest_finished_index`, `tiered_latest_finished_index`, `tiered_owning_epoch_index`, `remote_time_index_candidate_count`, `remote_fetch_end_position`, `remote_read_relative_offset` | Selection + guard | Earliest/latest/owning-epoch choices are global extrema among valid candidates; relative coordinates respect the requested epoch and u32 bounds. The new composition proves first-match completeness for the strict-predecessor time prefix with padding and unsorted conservative maxima, given accurate bounds on earlier records. Validated local/remote starts agree; neither structural validity nor agreement proves truthful metadata. Completeness across overlapping segments is not proved. |
| [remote_txn](../crates/verified/src/remote_txn.rs): `remote_txn_overlap_decision` | Guard | Inclusive interval overlap and invalidity are fully specified. Compose with the half-open local Fetch convention to establish identical abort-filter results; a predicate over four offsets does not prove the transaction index is complete. |
| [restore](../crates/verified/src/restore.rs): `restore_record_selected`, `restore_batch_filter_decision`, `restore_batch_past_offset_bound`, `restore_archive_reconcile`, `restore_batch_step`, `restore_record_coordinates`, `restore_rewritten_batch_header`, `restore_rewritten_record` | Invariant + guard | Checked batch/record rewriting pins coordinates, producer/header legality, ordering, and filter decisions. Arithmetic reuse is substantive. The strengthened whole-batch selection composition exports complete original-row witnesses and exact Keep/Empty/Filter classification. Rewritten header/record admission now composes with producer snapshot reconstruction to preserve survivor coordinates, full sequence spans and original retry acknowledgements even for empty rewrites. Complete archive reconciliation and preservation of surviving record values still require host iteration and decoded bytes. |
| [restore_sidecar](../crates/verified/src/restore_sidecar.rs): `restore_index_frontier`, `restore_offset_index_entry_valid`, `restore_time_index_entry_valid`, `restore_txn_index_entry_valid`, `restore_leader_epoch_entry_valid`, `restore_producer_ids_strict` | Guard + ordering | Row validators establish adjacent ordering and extent bounds; producer IDs are checked strictly ordered. The offset/time/epoch theorems now lift row validation to global lookup preconditions and bounds. Transaction validation also composes with interval construction and monotone Fetch overlap filtering. |
| [retention](../crates/verified/src/retention.rs): `barrier_cut_expired`, `local_retention_prefix`, `remote_retention_prefix`, `retention_delete_target`, `remote_covered_through` | Reference equivalence and interval geometry; safety strengthened | Local deletion separately proves no blocked segment is selected, an empty newest segment survives, and the first kept segment before the limit is blocked or not expired. Independent prefix-byte bounds justify selected non-expired rows and require progress through fully funded unblocked prefixes; both prefix results are length-bounded. Remote coverage proves an attained, maximal prefix with no holes from actual input intervals. Completed-copy metadata truth, coherent publication and expiry classification remain host facts. |
| [schema](../crates/verified/src/schema.rs): `schema_failure_decision`, `schema_frame_id`, `schema_field_action`, `schema_batch_admission` | Guard + decoding arithmetic | Fail-open applies only to transient errors, frame ID bytes are decoded exactly, and complete walks require matching applicable/admitted counts. The count equality does not prove each distinct applicable field was checked exactly once; registry responses and decoding remain external. |
| [scram](../crates/verified/src/scram.rs): `scram_alteration_decision` | Guard | The alteration table rejects unauthorized, duplicate, malformed, or out-of-range changes and pins the accepted mechanism. It does not prove derived credential bytes, password secrecy, or durable all-or-nothing batch application. |
| [share](../crates/verified/src/share.rs): `share_offset_mutation_decision`, `share_prune_frontier` | Selection + guard | Mutation is epoch-fenced and exact retries do not advance state; pruning returns a global minimum. Ownership, acquired-range coverage, and persister durability need the share models; a minimum alone does not show all live groups supplied their frontier. |
| [snapshot](../crates/verified/src/snapshot.rs): `snapshot_install_decision`, `snapshot_prune_admission`, `snapshot_chunk_admission` | Guard + progress | Install/prune decisions bound snapshot endpoints; chunk admission binds identity, fixed size, contiguous byte position, and completion. Zero-length incomplete chunks may make no progress. Actual content integrity and crash-safe installation remain unproved here. |
| [stamp](../crates/verified/src/stamp.rs): `stamp_ranges_valid`, `stamp_range_insertion_index`, `exact_stamp_range_index`, `covering_stamp_range_index` | Selection + ordering | Insertion proves non-overlap against an ordered set; exact/covering lookup proves the first match or absence. Lookup correctness is substantive. Updating the vectors together and preserving sorted_disjoint after insertion are host obligations. |
| [storage](../crates/verified/src/storage.rs): `local_append_coordinates`, `local_truncation_plan`, `truncation_relative_offset`, `truncation_batch_retained`, `truncation_frontier`, `future_log_swap_admission`, `remote_segment_transition`, `remote_partition_delete_transition`, `remote_cache_action` | Invariant + guard | Append coordinates and truncation plans are exact; lifecycle/cache rules forbid reverse edges and resurrection. Append now composes with acknowledgement/recovery. Scalar lifecycle admission does not make object deletion, primary state, and derived indexes atomic. |
| [stretch](../crates/verified/src/stretch.rs): `lemma_two_sites_never_survive`, `site_loss_survivors`, `min_insync_is_site_loss_safe`, `quorum_survives_any_single_site_loss` | Aggregate invariant | Site-loss bounds are derived from a placement model, and the two-site impossibility lemma is a genuine aggregate result. Actual placement must match round-robin, and quorum_survives has no production caller; arithmetic is not a proof of network failure detection or availability. |
| [throttle](../crates/verified/src/throttle.rs): `plan_consume` | Conservation invariant | Grant plus remaining tokens equals the capped refill total even at overflow. This is one of the strongest small contracts. Exactly-once refill claims and atomic rate reset require the bucket protocol/model; they do not follow from arithmetic. |
| [timestamp](../crates/verified/src/timestamp.rs): `first_timestamp_index`, `first_timestamp_record_index`, `earliest_max_timestamp_index`, `timestamp_record_coordinates`, `timestamp_record_time`, `timestamp_scan_next`, `timestamp_scan_window` | Selection + progress | First qualifying and earliest-max scans prove global selection without assuming sorted timestamps; cursor/window steps prove progress and overflow rejection. The sparse-index compositions now establish global first-match selection from actual prefix maxima. Host headers and decoded timestamps must agree, and the I/O walk must enumerate the complete suffix. |
| [transaction](../crates/verified/src/transaction.rs): `transaction_marker_materialization_decision`, `transaction_reaper_completion_decision`, `transaction_pid_install_decision`, `transaction_partition_registration`, the new `log_batch_kind`, `first_unstable_offset`, `transaction_marker_closes`, `aborted_transaction_interval`, `aborted_transaction_overlaps`, `unique_aborted_transaction_rows`, `should_abort_idle_transaction`, `identity`, `next_producer_identity`, `init_producer_id_identity_decision`, `transaction_completion_decision` | Selection + guard | LSO is the global earliest unstable start, and identity/state rechecks fence completion. LSO now composes with actual marker-key classification and Fetch to exclude every unstable transaction, release only the matching producer after marker replication, and establish maximal visibility. Complete wire-row deduplication composes with admitted remote/local abort indexes and Fetch to preserve every qualifying distinct transaction. Atomic offset publication, complete abort-index construction/enumeration, and full persisted-entry equality remain host/model obligations. |
| [uniform_assignor](../crates/verified/src/uniform_assignor.rs): `uniform_quota_split`, `homogeneous_member_quotas`, `select_least_loaded` | Conservation + selection | Quota split conserves partitions with bounded remainder; per-member retain/fill targets and least-loaded choice are exact. Global assignment uniqueness and total actual ownership are not established by quota arithmetic; reconciliation models cover bounded ownership transitions. |
| [vote](../crates/verified/src/vote.rs): `vote_encode_decision`, `vote_wire_decision`, `vote_admission_decision` | Guard | Encode/decode bounds and target/membership admission are precise. The proof does not establish unique grants or one durable vote per epoch, so a majority count must never be interpreted as a majority of distinct voters without that host invariant. |
| [voter_set](../crates/verified/src/voter_set.rs): `voter_wire_decision`, `voter_set_wire_decision` | Guard | Wire admission rejects invalid IDs, endpoint/version ranges, and duplicate sets given a uniqueness boolean. Proving every accepted wire voter becomes exactly one persisted member requires the decoder and membership application. |
| [wal](../crates/verified/src/wal.rs): `exact_wal_batch_range`, `wal_covering_batch_range`, `contains`, `select_wal_voter_index`, `select_wal_voters`, `wal_voter_set_valid`, `wal_batch_equal`, `wal_checkpoint_range_valid`, `wal_fetch_admission` | Aggregate invariant + selection + guard | The exact-range fold now exports independent global contiguity and per-batch bounds, beyond reference equality. Covering validation permits an interior logical floor while preserving exact physical ends. The copy/trim/Fetch composition guarantees an actual byte-identical batch witness for every visible offset. The complete placement fold establishes distinct nodes/racks; installation rejects incomplete, empty, misordered, and duplicate-ID sets. Fetch binds authenticated membership/epoch. The actual batch comparator now proves coordinate/byte equality, and the copy composition carries that equality through append/ack/recovery. Checkpoint admission now checks the observed whole-batch end while allowing interior logical floors; empty ranges require a host reset. The checkpoint/truncation/Fetch composition rejects partial batch ends and exports complete admission and the exact visibility cap. Exact offsets alone are still not byte agreement, and neither the range check nor byte comparator proves fsync. Faithful node/rack metadata and decoded headers remain host obligations. |
| [worm](../crates/verified/src/worm.rs): `worm_signature_decision`, `worm_object_set_decision` | Guard | Signature/object-set admission is explicit over identity, availability, and digest facts. The reference table mirrors branches closely; this proves which bad fact blocks admission, not the truth of the facts or storage immutability. |

## Existing bounded compositions

The following is a critique of the documented model boundaries in the [Stateright inventory](verification.md#stateright-model-check-tier). These models were not rerun as part of this contract-only change. Existing state counts, witnesses, and negative controls are evidence from their catalog/runner definitions, not new run results here.

| Model family | Why its property is useful | What prevents a broader claim |
| :--- | :--- | :--- |
| Data path | Composes HWM, ISR, failover, ELR, visibility, and diskless reservations to check preservation of committed records. | Small single-partition logs; payloads, transactions, and tiers are abstract/outside. |
| Producer state | Independent retained-window classification catches a wrong duplicate decision. | One identity and bounded batches/epochs; no snapshots or restart. |
| Replica state | Every ISR member holds the committed prefix across append/fetch/ISR transitions. | Fixed leader, monotonic follower reports, and explicit eligibility actions. |
| Fetch session | Forget/merge preserves logical partition identity and requested byte limits. | Cache epoch lifecycle/eviction and codecs are outside. |
| Fetch visibility | Clamp and response-watermark probes test host projection and monotonicity. | Much of the probe repeats the kernel rule; watermarks are already constrained to valid ordering. Record reads are absent. |
| Leader failover | An independent rung/assignment-order oracle checks election choices. | Logs are absent; leader completeness comes from the data-path model. |
| Reassignment | Handoff/completion preserve leadership and replica-set invariants. | Catch-up is an explicit action, not a proof of replicated bytes. |
| Stretch cluster | Independent minority/witness properties and RED configurations test placement claims. | Modeled Produce gates and controller convergence; metadata quorum/logs are abstract. |
| Diskless crash | Composes reservation, quorum durability, index commit, trim, and handoff. | WAL/PUT/fsync are counters; pending reservations never become an image frontier; dedup rebuild is outside. |
| Client/server failover | Acknowledged records stay durable while retries preserve sequence and avoid duplicates. | One batch; elections assume committed-prefix possession and the actual wire client is absent. |
| Quorum WAL | Real retained logs must have byte-identical quorum support and recover without dropping committed prefixes. | It drives the local test harness, not the production remote-voter fetch/ack path. |
| Classic groups | Static identities, membership, timeout, and group phase stay coherent. | Offset-log durability and actor mailbox are outside. |
| Reconciler | Independent ownership checks reject simultaneous partition ownership. | Faithful bounded client and abstract persistence. |
| Consumer-group composition | Combines ownership with independent member/partition epoch commit fencing. | Offsets persistence and classic migration are outside. |
| Coordinator replay | Parent/child, key/value, epoch, and tombstone invariants constrain replay. | Outer log delivery, cursor/application orchestration, and bytes remain external. |
| Streams groups | Cooperative task ownership and replay projection are checked together. | Durable appends, metadata snapshots, and internal-topic I/O remain external. |
| EndTxn decision | Prepared generations cannot finalize twice or complete after stale identity/state. | Some InitProducerId/recovery transitions are modeled and identity space is bounded. |
| Exactly-once read composition | Producer/control-marker oracles independently check committed visibility and aborted-record exclusion. | Its LSO and abort-filter implementations are modeled; the new LSO-to-Fetch theorem closes only the scalar LSO part. |
| Two-phase commit timeout | Properties derive from request flags/timeouts rather than persisted encodings. | Atomic prepared completion and modeled InitProducerId/prepare stamping. |
| Share group | Membership/assignment invariants compose with replay projection. | Shared partition assignment across members is intentional; persister durability is outside. |
| Share partition | Real acquisition state preserves coverage, unique ownership, terminal acknowledgement, and monotonic counters through reload. | Persistence transport, Fetch, and real clocks are outside. |
| Break-glass lifecycle | At-most-one consume and approval/withdrawal properties cover event order. | Durable controller submission and signatures are outside. |
| Break-glass cross-spend | Independent request coverage sets catch target confusion; negative controls catch resurrection/double spend. | Under-approval is partly enforced by the action guard, so do not count it as independent permission evidence. |
| Compaction | Input/output log oracles check preservation of newest live values and necessary transaction markers. | Abstract pass/environment; actual I/O and crash publication are outside. |
| Leader epoch | Follower convergence and shared-prefix preservation test lookup plus truncation, with an assignment-at-election negative control. | Log-start recovery, remote tiers, wire, and I/O are outside. |
| KRaft | Real state machines plus stale-majority/crash schedules check leader completeness and log matching. | Replication/disk durability and volatile restart loss are abstract. |
| Throttle bucket | Whole-protocol token conservation catches lost or duplicate refill claims; former seqlock negative controls fail. | Bounded clocks/storage units and sequentially consistent memory model. |
| Audit spool | Independent record/loss accounting crosses marker fsync, settlement, crash, and reopen; superseded settlement is a negative control. | Filesystem/sink guarantees and explicit recovery of poisoned replay remain external. |

State-count pins and reachability witnesses are valuable defenses against silently pruning the action space. They do not establish unbounded correctness. A property that calls the same predicate used as its transition guard should be treated as a consistency check unless a separate oracle or counterexample mutation demonstrates independence.

## Next compositions worth doing

1. **Distinct identity to durable quorum.** Full WAL placement and installation now establish distinct node/rack identities, and the single-rack-loss composition connects actual placement to majority arithmetic. Installed identities now compose with explicit durable votes, the quorum kernel, and Fetch to return a concrete distinct supporter list. Production promotion now checks actual batch bytes through a verified comparator, and arbitrary copied-prefix append/ack/recovery geometry composes with it. Checkpoint recovery now composes observed batch ends with whole-batch truncation and Fetch, and a production regression covers invalid partial ends and valid interior/empty floors. Next connect remote authenticated acknowledgements and checkpoint publication to byte-identical durable prefixes; offsets-only quorum kernels still cannot establish matching bytes.
2. **Whole-sequence validation to all indexed readers.** The offset/time/epoch lifts now establish search preconditions. The new prefix-maximum/index/record compositions establish no skipped matches. Remote prefix selection now composes with that scan, and a real-segment file round trip checks both adapters against an independent record oracle. Next connect persisted batch headers and windowed decoding to those complete record arrays; ordering alone cannot establish accurate running maxima.
3. **Persistence round trips.** Connect snapshot/quorum/token record admission to encode, durable publication, decode, and replay, checking equality of the complete state. Extra scalar guards cannot prove this.
4. **Full plan application.** Trim admission, finite application progress, exact retry, and snapshot-cursor selection now compose under prior-store caps. Next connect those decisions to atomic durable checkpoints and complete host recovery, and connect migration/reconfiguration plans to their durable application. Focus on a real state-owning boundary rather than adding a generic protocol framework.

These are remaining proof boundaries, not requests for speculative abstractions. The existing models already cover many bounded event-order compositions; reuse them where the claimed behavior depends on crashes or concurrency.

## Local validation

The pinned Creusot 0.13.0 run proved 465 generated files before this change and 530 after forty-five added compositions, the strict timestamp-scan kernel, two WAL placement/installation kernels, the actual-batch byte comparator, the checkpoint-boundary and covering-range guards, a vote-count equivalence lemma, two byte-extent arithmetic helpers, the actual control-key classifier, and three quota debt/refill-accounting kernels and the production whole-token request selector, cross-tier timestamp candidate selector, and logical-floor first-record kernel, shared timestamp-type decoder, and complete wire-abort-row deduplicator. Fresh-target generation and full saved-artifact replay passed. The final no-cache replay used two prover workers; a concurrent sixteen-worker attempt had failed one unchanged `trim_steps_converge` obligation, which passed in the final replay. The 511-file replay initially failed one unchanged `published_trim_bounds_recovery` obligation while native tests were compiling; the same saved artifact then passed in isolation and in the full four-worker replay, without source or artifact changes. Generated files include derivations/helper obligations, so those numbers are tool output rather than counts of independent safety properties. `cargo test --locked -p krabka-verified` passed 287 tests, including `composition_boundary_witnesses`, `restored_state_composition_boundaries`, and `delivery_replication_and_restore_composition_boundaries`. The 90 WAL quorum tests passed, including the 24 interrupted-trim cases with checkpoint-write and native floor failures, control-marker semantic equivalence before and after promotion/reopen, the duplicate-voter regression, placement, authenticated routing, fsync acknowledgements, recovery, production promotion retry, equal-length divergent-byte rejection without destination mutation, checkpoint rejection without mutation and interior/empty-floor reopen, promotion across interior floors and failed-append retry with an independently persisted floor, and the existing bounded WAL model. Five existing production diskless-writer tests also passed, covering WAL failure/retry, equal trim frontiers, durable acknowledgement/reopen, and hot-tail invalidation. The remote-storage library suite passed 167 tests, including a real-segment archive scan over parsed time/offset indexes and encoded multi-record batches against local lookup and an independent record oracle, with gaps and padding. The log library suite passed 473 tests, including dense/sparse persisted-index regressions against independent first-match and earliest-maximum oracles. The Bazel verified/log/remote-storage-crate tests and broker WAL-quorum subset, all-target Clippy with warnings denied, formatting check, `aspect check-creusot-skip`, and `git diff --check` passed.

The token negative control is concrete: unconditional Reject proved under the original implication contracts and failed `vc_token_mutation_decision` under the strengthened contracts. A second negative control removed blocking from both the retention body and its reference model: the original contract proved and the strengthened contract failed `vc_local_retention_prefix`. All mutations were temporary; executable bodies and the reference model were restored unchanged. The boundary test also witnesses the audit-generation ceiling concretely: at `u64::MAX`, settling a marker of 2 against 5 pending losses twice leaves 1 instead of 3. No remote CI result is claimed.

Two further composition negative controls were also rejected: advancing the upper time cursor by one past its proved extent failed `vc_validated_time_cursors_are_monotone`, and selecting snapshots with the original log end instead of the truncation cut failed `vc_truncated_snapshot_selection_bounds_replay`. Both temporary edits were restored before final validation.

Four delivery/replication/restore negative controls failed their corresponding composition obligations: treating every nonempty segment as due, applying the follower Fetch bound to a consumer, appending regardless of the replica-response fence, and omitting the restore offset bound. The source was restored after each control batch and fully revalidated.

The timestamp regression failed before the fix: three equal indexed timestamps returned offset 2 instead of 0. Substituting the original inclusive floor lookup into `indexed_timestamp_scan_finds_first` fails its no-skipped-prefix obligation while that floor lookup still satisfies its own contract. The mutation was restored. The final regression covers timestamp regressions, ties, sparse/dense indexing, recovery after reopen, one-byte scan windows, and signed timestamp extremes.

Two remote composition negative controls failed their obligations: selecting the entire raw index instead of the strict-predecessor prefix, and comparing remote selection with the original inclusive local lookup. Both were restored before full validation. The remote boundary witness also establishes why row validation alone cannot prove truthful running maxima.

Three trim negative controls failed the corresponding composition obligations: applying a WAL step to the local store, selecting snapshots against the old log start, and ignoring the configured diskless safety lag. All were temporary and restored. The completed-step trace is a protocol projection, not a filesystem crash proof; it represents only exact completed effects and unchanged pauses/failures.

Two WAL negative controls distinguish aggregate safety from reference equality. Removing duplicate-ID rejection failed `vc_wal_voter_set_valid`. Removing rack exclusion from both the one-row selector and its `voter_blocked` specification still proved that original selector, but failed the complete fold’s distinct-rack invariant in `vc_select_wal_voters`. All temporary edits were restored before final validation.

The installed-WAL/Fetch proof rejects unconditional rejection, an empty supporter witness, counting the unsynced leader end as an additional vote, stalling at the old floor despite quorum support, and hiding every record behind `i64::MIN`. A deliberately nested quantified contract still accepted an empty witness: the majority guarantee sat inside a universal quantifier over an empty pair domain. Separate postconditions for supporter identity, membership, and majority reject that control. All temporary edits were restored. Sorted-offset and independent identity-set oracles check the returned watermark/supporters, including incomplete configured sets, integer extremes, delivery/LSO caps, floor-only advancement, and inherited watermark cases.

An earlier quorum/Fetch composition negative control returned `(current, i64::MIN)`: the original bounds/conditional-support contract proved, while the strengthened maximality/exact-limit contract failed. Both its executable body and the installed-WAL composition were restored before full proof generation and replay.

The actual-byte negative control weakens both comparator body and contract to coordinate/length equality: that comparator still proves, while the copy composition fails its independent byte-preservation obligation. The production equal-length divergent-byte regression also fails under that mutation because promotion returns success. Three further copy controls fail: rejecting every copy, substituting a synthetic one-byte encoded extent, and omitting the requested target-end check. All temporary changes were restored before final validation.

The checkpoint negative control weakens both guard and contract to scalar bounds: that guard proves while the aggregate fails its independent retained-end obligation. Returning rejection for every checkpoint, omitting the empty-range reset, and omitting Fetch frontier clamping also fail the composition. All temporary mutations were restored before final generation and replay. An independent sorted-boundary/count/minimum oracle exercises arbitrary batch spans and visibility fields; production recovery tests cover partial-end rejection with unchanged bytes/checkpoint, interior starts, empty ranges, missing checkpoints, and stable reopen.

The interior-floor controls reject copying from the logical floor instead of the physical batch base, exposing offsets below the logical floor, losing the prior canonical floor, and hiding every visible offset. A separate paired gap-policy mutation proves under the original recursive equivalence contract and fails the new independent layout/bounds contract. All temporary mutations were restored before final generation and replay. The production interrupted-copy control omits the WAL-first floor checkpoint and fails the injected-write/reopen regression: the durable source still records floor 1 instead of the required floor 2.

Five control-marker composition mutations fail verification: releasing at HWM equality, closing another producer, closing a barrier, returning an artificially empty Fetch, and dropping other unstable starts. The original byte-copy theorem still proves when the host skips control-state bookkeeping, while the actual promotion regression fails (live LSO 0 instead of 3). All temporary source edits were restored before final validation.

The trim-publication controls reject checkpointing after native trim, checkpointing before the sync stage, omitting the end clamp, retaining an obsolete durable end after sync/publication, exposing trimmed offsets, and hiding all visible offsets. The native pre-sync omission also fails the fault table: an injected sync failure must leave both the durable range and native floor unchanged. All source mutations are restored before final checks.

The snapshot-retry theorem has independent modulo-2^31 and physical-offset oracles, boundary cases for sequence wraparound and `i64::MAX`, and rejection checks for invalid and marker-only rows. A production test writes and reopens real snapshots, then checks the broker tracker’s duplicate base/last offsets and timestamp, stale-epoch fencing, new-epoch classification, and next-sequence admission.

Five snapshot-retry negative controls reject unconditional rejection, reversing sequence reconstruction, using the last physical offset as the base, returning the last offset as the acknowledgement frontier, and accepting the last sequence again as the successor. All temporary source and artifact changes are restored before final validation.

A paired negative control changes both the decrement kernel and its reference contract to increment instead. That erroneous kernel still proves, but the snapshot composition fails its independent modulo-2^31 and retry obligations. The correct source and all validated proof artifacts are restored byte-for-byte.

The strengthened snapshot-selection composition is checked against an independent filtered-maximum and replay-cursor oracle, including unordered/duplicate offsets, empty survivor sets, a local floor above the selected snapshot, exact truncation boundaries, and signed integer extremes.

The snapshot-selection negative controls reject unconditional failure, dropping the selection, replacing the exact replay cursor with the cut, losing the local floor, and choosing against the old end. A separate absent-selection control replaces only the fallback cursor with the cut, keeping that exact guarantee independent of quantified snapshot rows. A paired mutation changes the selection body to return the first eligible row and removes maximality from its reference contract: that kernel still proves, while the composition rejects it. All temporary edits are restored before final validation.

The corruption-fallback property test uses an independent maximum over eligible non-corrupt rows and checks error classification, original index, and exact replay frontier. Boundary tests cover corruption chains, duplicates, empty/all-corrupt sets, truncation, local floors, and `i64::MAX`. The 25 native producer-snapshot tests pass, including expanded zero-through-four corrupt-snapshot chains and a corrupt newest file followed by a read I/O failure and an older valid snapshot.

Eight corruption-fallback negative controls reject unconditional empty recovery, hiding I/O failures, stopping on corruption, loading corrupt state, confusing a post-removal position with the original snapshot identity, selecting against the old log end, skipping replay to the cut, and using the cut as the empty fallback cursor. A native control treats a read I/O failure as corruption and fails the expanded producer-snapshot regression. All temporary source and proof changes are restored before final checks.

The recovered-window property test independently computes the first matching sequence range modulo 2^31 and its original base/acknowledgement coordinates for one through five rows. Boundary witnesses cover unused slots, signed offset extremes, and sequence aliases. The 21 native producer-state tests pass: snapshot-plus-tail replay retains the last five multi-record batches across wrap and anonymous offset gaps, and a sparse real log spanning an entire sequence wrap chooses the earlier alias with its own original timestamp and acknowledgement frontier.

Six window controls reject unconditional append, missing duplicate metadata, wrong-slot lookup, wrong base, acknowledgement at the overall log end, and using the oldest row as current state. A paired control reverses the shared duplicate search and changes its reference contract to prefer the last match: that kernel and the snapshot-only theorem still prove, while the window composition fails its independent first-match guarantee. Omitting the host’s earlier-batch projection also fails the real snapshot-plus-tail regression; that host projection remains outside Creusot. All source and positive proof artifacts are restored before final validation.

The token-description composition is checked against independent resource/permission sets and the session gate. Boundary witnesses cover create-only grants, matching deny, resource ACLs for an unrelated identity, the no-ACL default, owner filtering, and privileged token-authenticated sessions. The 12 native token-description tests pass, including a live-controller matrix of DescribeTokens grants, CreateTokens-only grants, non-implied CreateTokens deny, explicit DescribeTokens deny, and unrelated-principal/default behavior across multiple owners.

Six token-description controls reject unconditional refusal, ignoring token authentication, ignoring owner filtering, querying CreateTokens, allowing despite a matching deny, and applying the no-ACL default despite resource ACLs. A paired control adds CreateTokens-to-DescribeTokens implication to both the matcher and its reference contract: that kernel still proves, while the composition rejects its independently forbidden grant. A native control changes the handler’s User-resource query to CreateTokens and fails the expanded controller regression. Correct source and positive proof artifacts are restored before final validation.

The quota compositions use an independent signed ledger in property tests, including the exact overflow admission boundary and capped debt repayment. Native bucket witnesses exercise representable round trips and the excluded cap/saturation cases. Six proof controls reject unconditional refusal, skipped charge/refund, artificial empty budgets, ignoring the debt cap, and artificial empty repayment. A fabricated full burst still proves the bounded-repayment contract without its exact balance law; adding that law rejects it. A paired control halves the unpaid charge in both the production kernel and its exact contract: the altered kernel still proves, while the refund composition and the native round-trip regression fail. Correct source and positive artifacts are restored before final validation.

For the quota extraction, the throttle library's 31 tests, all eight bucket-model tests, the broker's nine Fetch-throttle tests, and the three relevant Bazel targets pass. Verified/throttle all-target Clippy, formatting, and the Creusot skip check also pass. Those extraction checks did not cover the fixed-nanosecond adapter defect above; the refill repair adds direct elapsed-time and rate-change regressions.

The refill-partition composition is checked against an independent rational-time
ledger with both small, unsaturated values and 64-bit extremes. Deterministic
cases cover sub-micro-token intervals, fractional debt repayment, zero bursts
with debt, burst loss, and a refill large enough to repay maximal debt and fill
a maximal burst. Uniform 64-bit sampling alone mostly reaches the burst and
can conceal an incorrect refill rate.

A paired negative control halves the refill rate in both the production kernel
and its local contract: that altered kernel still proves, while the aggregate
time-ledger composition, its native oracle, and the production polling
regression fail. Dropping the carried fraction during a rate reset also fails
the native rate-change regression. All temporary source and proof changes are
restored. Fresh-target verification and the full two-worker no-cache saved
session replay pass all 517 files; the 266 verified
tests, 33 throttle tests, eight bucket-model tests, nine broker Fetch-throttle
tests, three relevant Bazel targets, all-target Clippy, formatting, and the
Creusot/mutation configuration checks pass.

The arbitrary consume-trace composition combines the production refill, whole-token request selector, and grant kernel. An independent signed rational ledger checks small unsaturated balances, fractional credit, debt repayment, burst loss, repeated/backward clocks, empty traces, and 64-bit extremes. Production tests exercise spending between refills and debt repayment. A paired zero-selector/weak-contract control proves both the altered selector and the conservation-only trace, but fails the restored service guarantee, independent ledger tests, and production quantizer tests. All temporary mutations are restored before validation.

CI repeatedly left the trace's accumulated conservation invariant unproved.
Its induction now keeps a ghost sum of granted storage units, and separately
proves that this sum equals the actual whole-token grants times their unit
scale. The credit ledger stays additive instead of combining token products
with all earlier cap losses. Public contracts and runtime bodies are unchanged.
Removing that link leaves the trace at 56 of 60 obligations; the positive
source is restored byte-for-byte. Pinned cache-free full generation passes all
537 sessions, all 303 native verified tests pass, and each previously failing
broker integration target passes three local runs. Fresh CI remains required.

The final consume-trace batch passes fresh generation and the two-worker
no-cache saved-session replay of all 519 proof files, 268 verified tests,
34 throttle tests, eight bucket-model tests, nine broker Fetch-throttle tests,
and the three relevant Bazel targets. All-target Clippy, formatting, and
Creusot/mutation configuration checks pass. The earlier partition theorem uses
explicit scaled-balance assertions to keep its unchanged guarantees tractable
under the stronger refill contract. Unrelated generated artifacts are restored
byte-for-byte; validation remains local.

The timestamp/ListOffsets composition is tested against an independent filtered
visible-record oracle, with offset gaps, timestamp regressions/ties, arbitrary
sparse rows, consumer/replica isolation, empty windows, and signed extremes.
A real broker fixture checks 120 positive-timestamp responses across HWM and
isolation levels, then checks COMMIT at HWM equality and strict passage. Two
paired body/reference controls still prove the original selector: unconditional
unknown timestamp answers and accepting equality at the visibility frontier.
Both fail the new composition, independent oracle, and production response
regression. All mutations are restored byte-for-byte before final validation.

The final timestamp/ListOffsets batch passes fresh generation and the full
two-worker no-cache saved-session replay of all 520 proof files, all 270
verified tests, all 38 broker ListOffsets tests, the verified Bazel target,
verified/broker all-target Clippy, formatting, and Creusot/mutation checks.
The production selector sources and all 1,038 prior proof artifact files are
preserved byte-for-byte. Validation is local on `more-proofs`.

The tiered timestamp composition checks independently ordered remote and local
record windows against a flattened, floor-filtered, visibility-filtered
minimum-offset oracle. It allows gaps and overlapping tiers, timestamp
regressions and ties, empty windows, interior floors, and signed extremes.
A real encoded-segment broker fixture first reproduces the upload-gap bug
(returning remote offset 4 instead of local offset 2), then reproduces the
local floor bug (returning discarded offset 2 instead of retained offset 4).
The repaired path also finds offset 5 when the logical floor lies inside its
batch. A separate remote-reader fixture checks floor exclusion directly.

Two paired negative controls retain provable local contracts: preferring every
remote hit after removing the local-minimum guarantee, and ignoring the logical
floor in both the record selector and its contract. Each fails the composed
first-retained-visible-match guarantee, the independent oracle, and the real
broker response regression. All temporary sources and artifacts are restored.
The composition assumes complete decoded per-tier windows and coherent
frontiers; enumeration of overlapping remote objects and concurrent floor
changes remain host obligations.

The fortieth-composition batch passes fresh pinned generation and the full
no-cache two-worker saved-proof replay of all 523 files. All 272 verified tests,
38 broker ListOffsets tests, eight remote-reader timestamp tests, and the full
log suite pass (including 473 library tests and 16 integration tests). The
verified/log Bazel tests, verified/log/broker all-target Clippy with warnings
denied, formatting, and Creusot/mutation configuration checks pass. The 1,040
prior generated artifact files are preserved byte-for-byte. Validation is
local on `more-proofs`; these changes have not been committed or pushed.

The typed-timestamp composition uses an independent raw-record oracle for both
batch timestamp types, arbitrary signed producer fields, ordered offset gaps,
logical floors, exclusive visibility frontiers, and epoch carry-through. Its
append-time domain permits producer arithmetic outside i64; CreateTime admits
only valid mathematical sums. The encoded remote-byte regression checks exact
append stamps, interior floors, and ignored positive/negative timestamp
overflow. A real remote-reader index/object fixture checks both stamped batches;
local decoding still rejects absolute offset overflow while ignoring producer
time overflow under append time.

Two paired controls separate the aggregate guarantee from local arithmetic
contracts. Ignoring append time and changing both its body and contract to
producer time still proves that kernel, but fails the aggregate, oracle, and
remote byte regression. Multiplying the offset base by its delta and changing
the coordinate contract accordingly also proves that kernel, but fails the
aggregate, oracle, and actual local maximum-timestamp scan. All temporary
sources and proof artifacts are restored byte-for-byte.

The forty-first-composition batch passes fresh pinned generation and the full
no-cache two-worker saved-proof replay of all 525 files. All 274 verified tests,
167 remote-storage library tests, the full log suite (473 library tests and
16 integration tests), nine broker remote timestamp tests, and 38 broker
ListOffsets tests pass. The verified/log/remote-storage Bazel tests, all-target
Clippy with warnings denied for those crates and the broker, formatting, and
Creusot/mutation configuration checks pass. All 1,046 prior artifact files are
preserved byte-for-byte. Every verified Rust source remains below 300 lines.
Validation is local on `more-proofs`; these changes are uncommitted/unpushed.


## Abort-source completeness and later marker ownership

The forty-second composition strengthens an existing boolean witness into exact
original-row selection, then joins archive admission, consumer visibility,
inclusive remote overlap, half-open local overlap, and the production wire-row
deduplicator. Its postcondition specifies both directions of membership and
uniqueness over the two complete supplied sources. A future marker segment is
not an excuse to omit an abort covering earlier data. Invalid source indexes
are rejected; valid sources are admitted even with an empty Fetch window.

The native Fetch fixture builds actual transaction/control batches, rolls
one-batch segments, copies through the real archive path, evicts local data,
and reads the archived transactional batch. It reproduces the original remote
omission, then the independent local-tail omission after only the remote fix.
The corrected path returns the exact wire row for remote-only and local-only
markers, and returns it once when both tiers retain the marker.

Two paired controls retain provable weakened local contracts: classifying every
valid remote interval as disjoint, and returning no rows from deduplication
while retaining its soundness/uniqueness bounds. Both local proofs pass. Both
fail the new aggregate proof, independent interval/set oracle, and real Fetch
regression. All temporary sources and proof artifacts are restored byte-for-byte.
Complete current-lineage enumeration, optional-object truthfulness, decoding,
coherent captured frontiers, publication durability, and client record/control
filtering remain outside the aggregate proof. The finished-tail scan and linear
duplicate lookup have explicit profiling ceilings in the production sources.


The forty-second-composition batch passes fresh pinned generation and full
no-cache two-worker replay of all 527 saved proof files. All 276 verified tests,
nine broker remote Fetch tests, and 39 RemoteReader tests pass. The verified
Bazel target and broker Fetch subset, all-target Clippy with warnings denied for
verified/broker/log/remote-storage, formatting, and the Creusot/mutation
configuration checks pass. Mutation discovery includes the strengthened witness,
new composition, and production deduplicator. All 1,048 unrelated prior artifact
files are preserved byte-for-byte; only the strengthened selection session and
two new sessions change. Every verified Rust source remains below 300 lines.
Validation is local on `more-proofs`; these changes are uncommitted/unpushed.


## Filtered restore and producer retry identity

The forty-third composition returns concrete original-index/offset/timestamp
witnesses from the actual rewrite-record validator, not just a success flag.
Its aggregate postcondition fixes complete survivor membership, source order,
exact original coordinates, and the original acknowledgement frontier and
retry sequence after snapshot reconstruction. Sequence wrap is allowed; a
filtered record count never replaces the archived offset/sequence delta.
The native filtered/empty restore fixture passes through encoding, materializing,
local append, producer snapshot publication and reopen, and checks the original
retry against the recovered sequence range. Kafka's
[magic-v2 compaction contract](https://github.com/apache/kafka/blob/3.8.0/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L67-L79)
and [retained-record builder](https://github.com/apache/kafka/blob/3.8.0/clients/src/main/java/org/apache/kafka/common/record/MemoryRecords.java#L268-L290)
require preservation of the original offset/sequence span for that same reason.

Three controls distinguish aggregate guarantees from locally consistent choices.
A weakened selection contract admitting an empty survivor list still proves
locally but fails the new composition and independent record oracle. Using the
survivors' highest delta for a partial rewrite leaves the unchanged header
validator provable, but fails the aggregate, oracle and real materializer
regression. A zero sequence increment satisfies a weakened range-only contract
but fails the aggregate, oracle and restore/reopen regression. Temporary sources
and proof artifacts are restored byte-for-byte. The proof covers one coherent
last data batch; payload bytes, crash-safe publication, complete replay and PID
routing remain outside it.


## Remaining boolean-only composition contracts

The full fifteen-function control is recorded separately from the three
restore/retry controls. At that checkpoint, replacing all fifteen boolean-only
bodies with `true` proved all fifteen affected files and passed all 48 composition
tests. The append and reservation entries now return concrete witnesses with
exact relational postconditions and independent arithmetic oracles. The
time-cursor contract now exports exact validated inclusive/strict cursors into
a complete retained interval scan. No boolean-only exported composition contracts
remain; that closes this specific vacuity gap, not the outstanding host obligations. The
thirteen-function checkpoint control proved all thirteen replacements and passed
all 53 composition tests. The read-committed Fetch entry has since been upgraded
and consumed by the stability/abort-source composition below. At the twelve-entry checkpoint, repeating the
control proved all twelve replacements and passed all 57 composition tests.
The audit-loss settlement entry has since been upgraded and consumed by the
marker-admission composition below. Scheduled delivery now exports its derived
frontier to the transaction-stability composition. Placement now exports its
actual voter set and installer admission to the rack-loss witness. Epoch
reconciliation now exports its resolved cut into retained snapshot replay. Offset
index validation now exports its cursors into complete-batch first-match selection.
Sparse construction now exports actual prefix maxima into retained lookup and
cross-tier visibility. Temporary
control changes and artifacts are
restored byte-for-byte:


The forty-third-composition batch passes fresh pinned generation and full
no-cache two-worker replay of all 528 saved proof files. All 278 verified tests
and 145 restore library tests pass, including the encoded filtered/empty
materializer regression through log recovery and explicit snapshot/reopen.
Verified/restore Bazel tests, all-target Clippy with warnings denied for
verified/restore/log, formatting and the Creusot/mutation configuration checks
pass. Mutation discovery includes the new theorem and strengthened selection
witness. All 1,052 unrelated prior artifact files are preserved byte-for-byte;
only the strengthened selection session and new retry session change. Every
verified Rust source remains below 300 lines. Validation is local on
`more-proofs`; these changes are uncommitted/unpushed. At that checkpoint, the fifteen boolean-only contracts remained open audit work.
The subsequent append/reservation repairs below reduce the current list to thirteen.

## Append/reservation witnesses and their consumer

`append_frontiers_agree` returns all five actual coordinate-path outputs instead
of an opaque success flag. `reservations_do_not_overlap` returns the original
first reservation's start and end and the second reservation's end through the
pending-frontier kernel. Their contracts specify complete admission and exact
relations, including rejection of a valid first reservation followed by an
unrepresentable second one. The pair projection does not imply rollback of the
first reservation.

The forty-fourth composition, `reserved_pair_preserves_recovery_and_ack_order`,
consumes those exported contracts. It returns both batches' concrete path
witnesses and establishes that the first recovered last offset precedes the
second reservation, whose base equals the first acknowledgement frontier. The
second acknowledgement, recovery and scan frontiers agree and advance strictly.
The admitted input is two accurately decoded batches and a serialized controller
reservation chain; bytes, fsync, quorum completion and concurrency remain host
obligations. Independent `i128` geometry oracles exercise whole outputs,
rejection, adjacency, large deltas and the second batch's exclusive-end overflow.

Three manual controls distinguish implementation correctness from contract
reuse. A zero acknowledgement under a weakened scalar contract proves locally
but fails the frontier composition and its oracle. A pending frontier that
returns its current value under a weakened scalar contract also proves locally
but fails the reservation composition and its oracle. Finally, removing the
frontier composition's equality postcondition leaves that local proof and all
five new runtime tests passing, while the dependent pair proof fails. Runtime
behavior is unchanged in that last control: the failure specifically exposes
the missing reusable contract. All temporary kernel/specification changes and
proof artifacts are restored byte-for-byte.

The prior restore/retry proof's count conversion now uses the existing bounded
unsigned-remainder cast pattern; its input length bound makes the remainder an
identity. No Clippy suppression is needed for that conversion.

The forty-fourth-composition batch passes fresh pinned generation and full
no-cache two-worker saved-session replay of all 529 proof files, plus all 283
verified tests. The verified Bazel target, all-target
Clippy with warnings denied, formatting and the Creusot/mutation configuration
checks pass. Mutation discovery includes both upgraded witness functions and
their pair consumer. Exactly 1,050 unrelated prior artifact files are preserved;
only the two upgraded sessions, their new consumer and the restore count-cast
session change. Every verified Rust source remains below 300 lines. These
changes remain local and uncommitted on `more-proofs`; the thirteen opaque
contracts above remain open work.

## Derived transaction stability and complete abort rows

The old read-committed Fetch composition returned only `true`, including when
a transaction start lay beyond log end. It now returns the actual first
unstable offset and the complete Fetch visibility decision, or rejects exactly
an out-of-range supplied start. Its limit is the greatest prefix bounded by
log end, HW, delivery and every supplied transaction start; this includes a
maximality guarantee so an arbitrarily smaller window cannot satisfy the
contract. Raw negative starts conservatively hide the nonnegative data window.
The inherited `FetchWatermarks::lso` is replaced by the derived frontier.

The forty-fifth composition, `stable_abort_sources_cover_fetch`, feeds that
frontier to the existing remote/local abort-source union and returns the derived
LSO, greatest narrowed limit and all necessary unique wire abort rows. The
bidirectional membership guarantee covers marker owners beyond the stable data
window. Invalid indexes reject the whole result even when the corrupt tail
would not overlap the query. The source arrays and transaction projection must
belong to one coherent log lineage. Production `Log::refresh_lso` supplies all
open starts and the earliest ordered unreplicated key; the latter is a
minimum-equivalent representative, whose construction and state maintenance
remain host obligations. Source enumeration, requested-floor authorization,
bytes and client filtering remain outside this coordinate/index proof.

Independent tests compare every returned visibility field and the complete
interval/set result. They cover unordered/duplicate starts, no transactions,
starts beyond end, signed extremes, stability-limited empty windows, stale
inherited LSO, duplicate source rows, later marker ownership and a corrupt
non-overlapping tail.

Three manual controls passed their intended checks. A weakened unstable-offset
kernel that ignores starts proves locally, but the strengthened Fetch proof,
all four new oracle tests and the real log's `transactional_batch_holds_lso`
regression reject it. Removing only the exported Fetch visibility guarantees
leaves the local proof and all four runtime tests passing, while the dependent
abort-source proof fails. Passing the inherited LSO to the union leaves that
unchanged union provable but fails the aggregate and its oracle. All temporary
kernel/specification changes and proof artifacts are restored byte-for-byte.
The current production implementation already follows the correct minimum;
this batch strengthens proof contracts and their composition.

The forty-fifth-composition batch passes fresh pinned generation and full
no-cache two-worker saved-session replay of all 530 proof files, 287 verified
tests and all 473 log library tests. Verified/log
Bazel targets, all-target Clippy with warnings denied, formatting and the
Creusot/mutation configuration checks pass. Mutation discovery includes the
upgraded Fetch witness and its abort-source consumer. All 1,056 unrelated prior
artifact files are preserved byte-for-byte; only the strengthened Fetch session
and new consumer session change. Every verified Rust source remains under
300 lines. That checkpoint left twelve opaque contracts open; the audit-loss repair
below reduced that checkpoint list to eleven. Remote CI is reported separately
from local results.

## Audit-loss replay witness and marker admission

The audit settlement helper now returns both actual settlement and replay states,
with exact pending-count and generation relations plus equality of both states.
The new forty-sixth composition consumes those exported relations together with
`audit_loss_marker_admission`. An admitted durable snapshot settles exactly its
reported count, preserves concurrent losses in the next generation, rejects
admission of the original marker again, and admits the remaining count's new
marker exactly when a positive remainder exists.

The snapshot must name the pending generation and report no more than its
pending count. These are host snapshot invariants. The input generation remains
below `u64::MAX`; the output can reach that last generation, so the theorem does
not establish arbitrarily many future settlements. Parsed marker shape and
actual durable publication remain external. The helper also covers mismatched
generations and clamped over-reporting; only the bounded matching-snapshot
consumer claims conservation.

Independent wide signed-ledger tests compare both returned states and every
admission result. They cover full and partial settlement, zero and over-reported
counts, unrelated generations, invalid headers/field counts, stale admissions,
`u64::MAX` counts and the final usable input generation. Three manual controls
are restored byte-for-byte. Hiding the helper's exported relations leaves its
own proof and all three oracle tests passing but fails the consumer proof.
Returning the original pending states fails the witness proof and its oracles.
A deliberately weakened settlement kernel that discards a partial remainder
still proves locally, but fails the strengthened witness and the real audit
spool's concurrent-loss snapshot/reopen regression.

The affected native suites pass all 290 verified and 96 audit library tests.
Verified/audit Bazel targets, workspace all-target Clippy with warnings denied,
the verified rustdoc target, repository formatting and Creusot/mutation gates
pass. Fresh pinned generation and full no-cache two-worker saved replay prove
all 531 files. Only the upgraded helper and new consumer sessions change; all
1,058 unrelated prior artifact files remain byte-identical. Mutation discovery
includes both witnesses. Every verified Rust source remains below 300 lines.
Eleven boolean-only composition contracts remained open at this checkpoint.
This proof work is local and uncommitted, separately from PR #1259's CI repairs.

## Scheduled-delivery witness and transaction stability

The old scheduled-prefix helper exported only `true`, including for malformed
or incomplete walks. It now returns the computed delivery frontier and actual
consumer/follower views, or rejects exactly invalid complete batch geometry.
Its contract bounds the frontier by every waiting batch and identifies it as
the log end or an actual waiting batch base. These properties establish the
greatest permitted frontier without duplicating its executable scan.

The forty-seventh composition consumes that witness and the existing derived
transaction-stability witness. Its exact consumer limit is the minimum of HWM,
the derived delivery frontier, and the least unstable transaction start. Any
prefix bounded by all three constraints is no greater than that limit. Follower
replication remains ungated. Stale inherited delivery and LSO fields cannot
substitute for either derived result.

Inputs must contain complete aligned decoded batches and activation times,
plus complete unstable transaction starts from a coherent log view. Batch gaps,
nonmonotone timers, and unordered duplicate transaction starts are allowed.
Invalid spans, overlap, incomplete tails and starts beyond the end are rejected.
Byte decoding, enumeration completeness, cached-cursor invalidation, hardware
clock accuracy and durable publication remain host obligations.

Independent wide-integer geometry/deadline and visibility oracles cover raw
invalid inputs and generated complete gapped walks, stale gates, signed clock
extremes and exclusive-offset overflow. Removing only the helper's exported
relations leaves its own proof and all three new tests passing, but fails the
consumer proof. Using inherited delivery instead of the returned frontier fails
the consumer proof and behavioral oracle. Dropping the clock deadline from both
the real delivery kernel and its contract leaves that kernel provable but fails
the compositions, behavioral oracle and native log watermark regression: the
mutant advances to offset 8 while the waiting batch requires offset 4. All
control edits and positive artifacts are restored byte-for-byte.

Positive validation passes all 293 verified and 473 log library tests and all 16
native delivery integration tests. Verified/log/delivery Bazel targets, workspace
all-target Clippy with warnings denied, rustdoc, formatting and Creusot/mutation
gates pass. Fresh pinned generation and full no-cache two-worker saved-session replay
prove all 532 files. Only the scheduled helper, its new consumer,
and the explicit whole-index close in the existing abort proof retain regenerated
sessions; all 1,058 unrelated prior artifact files remain byte-identical. Every
verified Rust source remains below 300 lines. Ten boolean-only composition
contracts remained open at this checkpoint. At that checkpoint this batch remained local, separately from PR #1259's
published CI repairs.

## Placement admission and rack-loss witnesses

Both old placement compositions exported only `true`; the rack-loss proof also
reran selection independently and returned `true` for incomplete placement.
The installation helper now exports the selected `(node, rack)` rows, exact
node-ID projection and actual installer admission. It preserves distinct node
and rack identities, source membership and maximality of nonempty incomplete
selection. Admission is exactly a complete, nonempty local-first node set.

The rack-loss composition consumes these exported guarantees instead of
selecting a second independent placement. Its returned survivor IDs are exactly
the selected nodes outside the failed rack, in placement order. At most one
selected voter disappears. For requested voter counts of at least three, every
admitted placement therefore retains its original strict majority; incomplete
installation reports no installed quorum capacity even if all selected voters
survive. The result includes both admission and the actual election-quorum
check. The two boolean-only entries are removed from the current open list.

This proves capacity of a coherent configured voter set. Faithful mapping of
metadata rack strings to physical failure domains, communication, fsync,
election scheduling and durable membership changes remain host obligations.
Greedy placement is maximal, not globally maximum on conflicting duplicate
metadata rows. The contract exports that maximality and source identity, but
does not claim to pin every greedy tie choice; the independent native oracle
additionally checks the actual first-eligible placement order.

The independent set-based oracle compares all returned placement rows, node IDs,
admission, survivor order and quorum capacity. Boundary witnesses include local
rack loss, absent/local-unregistered candidates, zero/two/three voters, duplicated
node/rack metadata, incomplete selection, and `usize::MAX`/`u64::MAX`.
Four restored controls distinguish the new contracts from tautological checks.
Removing only exported rack uniqueness keeps the installation helper provable
and both oracle tests passing but fails the rack-loss consumer. Reporting a
quorum for uninstalled configurations and dropping survivors fail both proof
and tests. Raising the majority threshold in both the election kernel and its
contract leaves that altered kernel and the placement helper provable but fails
the rack-loss consumer, its oracle and the existing strict-majority regression.
All temporary source changes and positive proof sessions are restored byte-for-byte.

All 293 verified library tests and all four native broker placement regressions
pass. Verified Bazel tests, verified rustdoc, workspace all-target Clippy with
warnings denied, formatting and Creusot/mutation gates pass. Fresh pinned full
generation and full no-cache two-worker saved-session replay prove all 532 files;
a final targeted generation checks the proof-only removal bookkeeping after its
normal-build cleanup. Only the two upgraded placement sessions and the quota CI
repair are retained; all 1,058 unrelated prior artifact files remain byte-identical.
Every verified Rust source remains below 300 lines. Eight boolean-only contracts
remained open at this checkpoint. The placement witnesses were local and uncommitted at that checkpoint.

The quota CI repair states two multiplication identities explicitly before
combining the whole-token and refill ledgers. It changes no contract or runtime
behavior. The latest inspected CI run passed the abort repair and its complete
build/lint/format/test/doc job, but timed out on two quota-trace obligations.
Locally, all 530 published files plus the quota repair pass in the matching CI
toolchain image under four CPUs; a no-cache targeted quota proof also passes
under one CPU. Remote CI for the new repair is reported separately.

## Resolved epoch cut and retained snapshot replay

The old epoch composition exported only `true`, including malformed archives
and unplaceable epoch requests. It now returns an exact witness: invalid archive
or window, an unplaceable epoch, or a resolved epoch and cut with all clamped
watermark and consumer fields. Row validation supplies the lookup's strict
ordering; the cut follows Kafka's existing `endOffsetFor` relation and lies in
the original segment/log extent.

The forty-eighth composition consumes that cut together with the existing
truncated-snapshot witness. The newest eligible snapshot is selected against
the resolved cut, with original-index membership and global maximality; replay
starts exactly at the selected snapshot/local floor maximum, or at the logical
and local floor maximum when no snapshot survives. Watermarks, consumer limit,
snapshot admission and replay all use the same derived cut. Invalid windows and
archives, unplaceable epochs and successful replay remain distinct outcomes.
A valid cut below either retained floor is rejected by this retained-history
composition; it cannot manufacture an earlier floor and replay pruned data.
Production uses a full reset when the cut falls below local data. This proof
neither replaces that reset path nor claims full recovery for that case.

Complete accurate epoch and snapshot enumeration, trustworthy persisted bytes,
coherent floor/end inputs, batch-aligned cuts, actual truncation and replay,
and durable publication remain host obligations. The cutoff is an exclusive
coordinate; scalar row validation alone does not prove its byte-level boundary.

Independent tree-map, filtered-maximum and exact-watermark oracles exercise
arbitrary malformed histories and generated valid ordered histories with gap
requests, empty caches, tied/unordered snapshots, cut-below-floor cases, discarded
tails and signed integer extremes. Five restored controls test the boundary.
Replacing the helper's exported relations with a tautology leaves its own proof
and all three oracle tests passing but fails the downstream consumer proof.
Using the old end for snapshot selection, fabricating earlier retained floors,
and rejecting every history each fail their proof and behavioral oracle.
A paired mutation makes both lookup body and contract always return the requested
epoch and original end: that kernel proves, but the validated-cut helper fails,
as do the composition oracle and the native log checkpoint case-table regression.
All temporary mutations and positive artifacts are restored byte-for-byte.

Final validation on continuation branch `codex/proof-witness-continuation`,
based on current `main` at `4afe33aa`, passes all 296 verified and 474 log
library tests. Isolated Cargo log tests explicitly enable
`krabka-compression/lz4`, required by main's new LZ4 timestamp regression;
Bazel's log suite also passes all 474 tests. Verified/log Bazel targets,
verified rustdoc, workspace all-target Clippy with warnings denied, formatting
and Creusot/mutation gates pass. Fresh pinned generation on that base and full
no-cache two-worker saved-session replay prove all 533 files. Only the upgraded
epoch helper and new replay consumer sessions are retained; all 1,062 unrelated
prior artifact files remain byte-identical. Both witnesses appear in mutation
discovery. Every verified Rust source remains below 300 lines. Seven boolean-only
composition contracts remained open at this checkpoint.

PR #1259 merged after every required CI check passed at `870877ca`. This
witness batch and the preceding witnesses are carried by the separate
continuation branch; they are not part of that merged PR's remote qualification.

## Offset-index cursors and complete batch seek

The old offset-index composition exported only `true`. It now returns exact
floor and ceiling byte positions after complete row validation, or rejects an
invalid archive. Source membership and global floor/ceiling extremality are
exported, including zero fallback and absent ceiling.

The forty-ninth composition connects sparse index rows to complete decoded
`(last_relative_offset, byte_position)` batch rows. Both sequences must be
strictly ordered in both columns; the first physical batch starts at byte zero
and every sparse row must exactly name a physical batch. The floor therefore
cannot skip any qualifying batch. Reusing the existing scalar first-match
search with earlier positions masked returns exactly the globally first batch
whose last offset reaches the target. A present ceiling bounds that result;
no match also implies no ceiling. This is the conservative index floor: the
host's additional header-length skip is outside this theorem.

The semantic membership check matters: physical batches `(10, 0), (20, 20)`
and sparse row `(0, 20)` pass structural validation in a 40-byte file, yet
target 5 would skip the first match. The consumer rejects that false row.
Complete accurate batch enumeration, decoding, physical lengths, windowed
read budgets and I/O remain host obligations; byte bounds alone cannot prove
any of those facts. This proof/test-only composition adds no production scan.

Three independent tree-map, set-membership and full linear-scan oracle tests
cover arbitrary malformed archives, generated ordered sparse indexes, exact
targets, absent ceilings, empty files, false source rows and integer extremes.
Two restored controls check the new dependency: hiding the helper's exported
relations keeps its own proof and all three tests passing but fails the
downstream proof; a floor at the final batch instead of the selected index
position fails both the consumer proof and the behavioral oracle.

Publication validation for this continuation passes 299 verified and 474 log
library tests, scoped verified/log Bazel tests, verified doctests/rustdoc,
workspace all-target Clippy with warnings denied, formatting and the
Creusot/mutation static gates. Fresh pinned Creusot 0.13.0 generation and
no-cache two-worker saved-session replay prove all 534 files. Only the upgraded
offset helper and new seek consumer retain regenerated sessions in this final
batch; all 1,064 unrelated prior artifact files remain byte-identical. All four
new consumers appear in mutation discovery, and every verified Rust source
remains below 300 lines. Six boolean-only contracts remain open. Full workspace
Bazel CI and the full mutation sweep are separate from these local checks.

## Quota trace CI follow-up

PR #1260's exact failed Creusot job left two obligations unproved in
`metered_consumes_conserve_elapsed_credit`, one of its 534 files. Setup's
unsupported command fell back successfully; the failure was in the solver's
large conservation context. A private proved ledger lemma now lifts refill
conservation through a whole-token grant before the trace composes it with
its accumulated balance. The trace contract and runtime behavior are unchanged;
no assumptions or trusted annotations are added.

The isolated published snapshot plus this repair proves all 535 files in
the exact CI image, digest `76ba64e5066553606a438b28ffc4cce7ae45cab2c2f9dd1855d5ef4ffe9be9c8`,
with four CPUs. Both affected proofs also pass a no-cache one-CPU run in that
image. Existing independent quota ledger tests, all-target workspace Clippy
with warnings denied, formatting and the Creusot/mutation skip gates pass.
Remote CI for the repair is reported separately.

## Constructed sparse indexes, retention and cross-tier visibility

The former construction composition exported only `true`. Its witness now
contains the actual sparse rows and first matching original record index. Every
row's timestamp is an attained maximum of its complete prefix, its coordinate
comes from the requested indexed record, both row columns obey global ordering,
and its maximum bounds every record at/before its coordinate. These are exported
relations derived from the existing maximum selector, not caller-supplied bounds.

The fiftieth composition consumes those rows to search the retained suffix and
returns the original first matching index exactly when a retained match exists.
It cannot return a pruned raw first match and thereby suppress later matches.
The fifty-first composition consumes both tiers' retained answers, derives their
least absolute coordinate, preserves its actual timestamp and supplied epoch,
and applies the exclusive ListOffsets visibility bound. Unknown means the union
contains no retained visible match. Timestamps may regress and tiers may overlap.

The actual time-index writer now uses the running header maximum and the last
offset of the batch that set it; this construction witness ranges over rows at
actual record coordinates. Header-only offsets or sparse padding coordinates
are not established by this construction. Faithful header/record timestamps,
complete decoding and enumeration, physical windowed reads, overlapping-record
consistency, coherent floors/visibility and epoch lookup remain host obligations.
The existing general scan theorem also admits truthful bounds at coordinates
without records, but constructing those bounds requires a separate host bridge.

Monotone timestamp cursors alone cannot safely bound a scan's tail by a second
timestamp cursor: later timestamps can regress into the requested range. The
new consumers scan the retained suffix and bound the resulting absolute offset,
which is the ordering the visibility gate requires.

Four independent maximum, retained-index and complete-union oracle tests cover
pruned first matches, regressions after a maximum, sparse rows spanning records,
empty indexes/windows, tied maxima, overlapping tiers, exact exclusive bounds
and signed timestamp/maximum absolute-coordinate extremes. Four restored
controls exercise the two-module dependency chain. Hiding construction bounds
leaves its own proof and all four tests passing but fails retained lookup; hiding
retained guarantees similarly fails tier selection. Returning the pruned raw
first match fails proof and tests. A paired mutation makes the primitive and
its contract return the indexed record's timestamp instead of the prefix maximum:
that altered primitive proves, while construction and its independent oracle fail.

A separate restored control replaces all five remaining boolean-only bodies
with `true`: all affected module proofs and all 303 library tests still pass.
That is evidence these five exported contracts remain too weak, not evidence
that their full intended relationships have been verified. Temporary source
changes, generated sessions and new failure seeds are restored or quarantined.

Fresh pinned generation and full no-cache two-worker saved-session replay prove
all 537 files. All 303 verified, 474 log and 167 remote-storage library tests
pass, including real archived/local timestamp and retention-floor fixtures.
Verified/log/remote-storage Bazel tests, verified doctests/rustdoc, formatting,
workspace all-target Clippy with warnings denied and the Creusot/mutation gates
pass. The two new consumers and upgraded construction appear in mutation
discovery. Only their three sessions are refreshed; all 1,068 unrelated prior
artifact files, including the published quota repair, remain byte-identical.
Every verified Rust source remains below 300 lines. These timestamp compositions
form a separate review layer above PR #1260's quota CI repair.

## Checked archived timestamps and retained remote/local agreement

Two former boolean-only entries now export usable witnesses. The raw remote
scan returns its actual counted prefix, chosen floor and globally first matching
record, including exclusion of matches below that floor. It continues to admit
padding and conservatively bounded rows whose timestamps are unsorted. The
canonical archive validator instead rejects exactly invalid extent/order and
returns the actual remote count and equal remote/local floors, including the
empty-index fallback and signed target extremes.

The fifty-second composition consumes both exports. It validates decoded array
shape, record order and extent, then checks every archived row's timestamp bound
against records strictly before that row's coordinate. Structural validity alone
is insufficient: the existing sorted in-range row `(0, 3)` skips a real match at
offset zero in records `[(0, 100), (3, 300), (6, 200)]`. The checked composition
rejects that row rather than assuming its semantic bound. Conservative upper
bounds need not be attained maxima; equality at the floor remains scan-visible.

It scans the retained suffix with both existing selectors, proves their answers
agree and returns the original first retained index. No match excludes all
retained records; an earlier pruned match cannot suppress a later retained one.
Explicit proved slice-coordinate equalities lift the first-match relation back
to original indices. No trusted annotation or narrowed input domain is added.
Complete faithful decoding/enumeration, byte parsing, physical reads and coherent
host publication remain external. This checker is proof/test-only and does not
add a quadratic validation pass to production reads.

Four new independent oracle tests compare complete record scans, adjacent
archive ordering and full record-prefix bounds. They cover valid generated
archives, malformed arrays, false semantic bounds, padding, repeated timestamps,
regressions, pruned matches, empty inputs and signed/unsigned extremes. All 307
verified library tests pass. Five restored controls demonstrate the dependencies:
hiding either helper's contract keeps its own five-file proof and all tests
passing but fails the consumer; removing the semantic check, bypassing retention
or rejecting every input fails both the consumer proof and independent tests.
New failure seeds are quarantined and all temporary source changes are restored.
Three boolean-only composition contracts remained at this checkpoint; the
subsequent trim and time-cursor repairs close them.

Fresh pinned Creusot 0.13.0 generation and the full no-cache two-worker replay
prove all 538 files. Workspace all-target Clippy with warnings denied, the four
verified/log/remote-storage Bazel test targets including verified doctests,
formatting and Creusot/mutation configuration gates pass. All three witness
functions appear in mutation discovery. Only their three sessions are refreshed;
all 1,070 unrelated prior artifact files remain byte-identical, and every verified
Rust source remains below 300 lines. This batch and the following trim witnesses
form a review layer above PR #1261; remote qualification of the parent covers
its published snapshot only.

## Trim witnesses distinguish logical visibility, replay and physical eviction

Both trim entries formerly exported only `true`. Logical trim now returns its
actual completed floor, newest retained snapshot index and exact producer replay
cursor. Malformed and out-of-range rejections preserve their distinct reasons;
valid admissions cannot be rejected indiscriminately. The floor is the admitted
request reconciled with both observed stores, inside HWM/delivery/end caps.
Snapshot absence excludes all eligible snapshots, and a selected snapshot is
maximal in `(floor, log_end]`. Inherited store frontiers must obey the same caps;
admission alone cannot establish that.

Physical eviction now returns the actual WAL/cache pair. Enabled traces preserve
object coverage and lag bounds, report monotone progress and converge after two
completed steps. Disabled plans return inherited state exactly, including an
unsafe cache start: a disabled operation does not make unrelated state safe.
The primitive's `trim_target` model was closed to callers, so its own contract
could not expose the exact `min(coverage, HWM - lag)` target to this composition.
The existing definition is now open; its lag helper is visible to that logical
body. Both changes affect Creusot specifications only, with no trusted assumption
or production behavior change.

The fifty-third composition consumes completed logical trim and retained sparse
lookup. It keeps the read floor distinct from producer replay progress: with
floor 1 and a snapshot at 6, a timestamp match at offset 2 remains the first
retained read. The fifty-fourth composition consumes the physical eviction pair
and reroutes the unchanged logical first match to surviving local cache or
committed remote coverage. Evicting cache to offset 8 cannot delete a visible
match at offset 3. Routing through remote coverage preserves the configured
HWM lag. Both inherited physical frontiers must already obey coverage/lag caps.

Four new independent frontier, snapshot and full-record oracles cover accepted
and rejected requests, delivery caps, stale store frontiers, duplicate snapshots,
missing snapshots, arbitrary paused/completed traces, retained matches below
replay/cache frontiers, disabled unsafe cache state and integer extremes. All 311
verified library tests pass. Faithful decoding, complete enumeration, actual
committed object coverage, snapshot contents, whole-batch replay and crash-safe
checkpoint completion remain host obligations. Time-cursor monotonicity was
the remaining boolean-only export at this checkpoint; the repair below closes it.

Six restored negative controls distinguish the contracts from their runtime
oracles. Hiding either trim export keeps the helper's three-file proof and all
311 tests passing but fails its new consumer. Substituting producer replay or
physical cache frontiers for the logical read floor fails proof and tests;
rejecting every logical trim also fails both. Making the target model closed
again leaves the primitive's own proof and all tests passing, while six exact
reconciliation obligations become unproved. Temporary source changes and new
failure seeds are restored or quarantined. These are scoped controls, not a
claim that a full mutation sweep has completed.

Fresh pinned Creusot 0.13.0 generation and full no-cache two-worker saved-session
replay prove all 540 files. All 311 verified library tests, focused verified/log/
remote-storage Bazel tests including verified doctests, workspace all-target
Clippy with warnings denied, formatting and Creusot/mutation configuration gates
pass. All four witness functions appear in mutation discovery. Only these five
sessions are refreshed; all 1,070 unrelated prior artifact files, including the
previous unpublished timestamp batch, remain byte-identical. Every verified
Rust source remains below 300 lines. Both batches form a review layer above
PR #1261, whose published snapshot separately passed its remote CI gate.
These local checks do not establish remote CI for this new layer.

## Validated time cursors compose into complete retained interval scans

The last boolean-only export now returns its actual absolute inclusive lower
and upper lookup cursors plus the strict predecessor start needed for a
`timestamp >= lower` scan. Invalid segment extents or canonical archive rows
are rejected exactly. Accepted outputs stay inside the segment, are ordered,
and identify the final qualifying inclusive/strict rows, with an exact zero
fallback. The composition reuses the existing archive validator rather than
copying its validation loop. Its input domain still admits repeated row maxima,
empty indexes and signed timestamp extremes.

The fifty-fifth composition constructs truthful sparse prefix maxima, consumes
these cursors, and returns the original globally first retained record in a
closed timestamp interval. Absence excludes every retained match. Invalid
segment extents and any record outside the segment reject exactly, including
unindexed tail records. The strict start excludes no potential match. There is
no assumed ordering of decoded record timestamps.

Inclusive cursor monotonicity cannot crop a record scan. For records at offsets
`[0, 3, 6]` with timestamps `[100, 300, 200]`, both inclusive cursors for
`[200, 250]` are zero, while the first interval match is at offset six. Repeated
maxima also require a strict lower start: timestamps `[100, 100, 300]` give an
inclusive lower cursor of three for `[100, 100]`, but the first match is at zero.
A logical retention floor is applied independently of these sparse cursors.

Four independent tests use adjacent archive ordering, last-row linear lookup,
actual prefix maxima and a complete retained record scan. Generated valid and
malformed archives, arbitrary regressing timestamps, empty and sparse rows,
logical floors, unindexed out-of-range tails and signed/absolute-coordinate
extremes are covered. This mask-based interval composition is proof/test-only;
it adds no production allocation or interval API. Complete faithful decoding,
physical reads, coherent floors and truthful header-to-record adaptation remain
host obligations. Removing boolean-only exports does not close those obligations.

Five restored negative controls distinguish useful exports from locally passing
code. Hiding either cursor guarantees or constructed prefix bounds leaves the
respective helper proof and all 315 tests passing while breaking the new
consumer. Cropping the scan at the upper cursor or substituting the inclusive
lower cursor for its strict start fails both proof and independent tests.
Rejecting every cursor request also fails both. Temporary source edits are
restored and new failure seeds are quarantined. These are scoped controls,
not a completed full mutation sweep.

Fresh pinned Creusot 0.13.0 generation and full no-cache two-worker saved-session
replay prove all 541 files. All 315 verified library tests, the four verified/
log/remote-storage Bazel targets including verified doctests, workspace all-target
Clippy with warnings denied and Creusot/mutation configuration checks pass.
Both witness functions appear in mutation discovery. Only these two sessions
are refreshed; all 1,078 unrelated artifact files, including the published
checked-scan/trim layer, remain byte-identical. Every verified Rust source stays
below 300 lines. The archive validator's stale writer comment is also corrected:
current writes require newer maxima, while the structural contract deliberately
admits valid repeated rows without asserting truthful prefix bounds. Its body,
contract and source line positions are unchanged. This new batch remains local;
PR #1263's published snapshot separately passed its remote Creusot job, with
its remaining CI still pending at that checkpoint. The published snapshot has
since passed its complete CI gate at `bb3ce3280f769802e7ab6d66a6b1fb782e026a3c`.

## Stable offset prefixes safely gate regressing timestamp intervals

The fifty-sixth composition consumes both the constructed interval witness and
the actual read-committed visibility witness. It returns the derived LSO, maximal
exclusive offset limit and original first retained interval match below that
limit. No result excludes every eligible record in the supplied window. Invalid
segment extents, out-of-range records and transaction starts beyond log end are
rejected exactly. There are no new HWM/LSO ordering assumptions; negative raw
frontiers conservatively hide the window, and arbitrary inherited LSO is ignored.

Unlike a timestamp cursor, an offset visibility limit safely gates the tail:
strict record offset order puts every record after a hidden first interval
candidate at or beyond the same limit. For offsets `[0, 3, 6]` and timestamps
`[100, 300, 200]`, interval `[200, 250]` still finds offset six when the derived
limit is seven. A pending start or delivery/HWM cap at six excludes that same
candidate by strict offset equality. An inherited LSO of zero cannot hide a
stable candidate, nor can a larger inherited LSO expose an unstable one.
The Fetch `empty` optimization is not a complete stability predicate: it ignores
LSO when comparing the fetch offset with HW/delivery. The actual returned limit
must gate the candidate.

Production `Log::refresh_lso` enumerates every open transaction plus only the
earliest unreplicated start. This projection preserves the minimum of a coherent
valid snapshot, which suffices for prefix visibility; it is not complete
validation of omitted entries. A supplied snapshot containing starts `[3, 8]`
at log end seven rejects, whereas its minimum-only projection `[3]` is admitted
and preserves limit three. The composition's complete rejection statement is
about supplied starts only. Faithful decoding, omitted-state validity, coherent
publication, physical reads and client-side aborted-record filtering remain
outside it. This is stable-prefix gating, not a proof that aborted data is absent.

Four independent oracle tests compare complete record scans with a minimum over
log end, HW, delivery and every supplied transaction start. They cover accepted
and rejected inputs, timestamp regressions, repeated maxima, retention floors,
HWM/delivery/transaction equality, stale inherited LSO, negative raw starts and
frontiers, unindexed invalid tails and absolute/signed extremes. The snapshot
projection example above is an explicit regression. All 319 verified tests pass.

Six restored negative controls distinguish the exports from body checks. Hiding
either stability or interval guarantees leaves its helper proof and all tests
passing but breaks this consumer. Substituting inherited LSO, admitting equality
at the exclusive offset cap, treating `!empty` as sufficient candidate visibility,
or rejecting every scan fails proof and independent tests. All temporary sources are restored and
new failure seeds are quarantined. These are scoped controls, not a completed
full mutation sweep.

Fresh pinned Creusot 0.13.0 generation and full no-cache two-worker saved-session
replay prove all 542 files. All 319 verified library tests, the four verified/
log/remote-storage Bazel targets including verified doctests, workspace all-target
Clippy with warnings denied and Creusot/mutation configuration checks pass.
The new witness appears in mutation discovery. Only its session is refreshed;
all 1,082 prior artifact files, including the previous local interval batch,
remain byte-identical. The catalog agrees with all 56 executable compositions
across 38 topic files; logical definitions are counted separately. Every verified
Rust source remains below 300 lines. These checks were completed locally before
publication. PR #1263's published checked-scan/trim snapshot separately passed
its complete [CI gate](https://github.com/krabka-io/krabka-broker/actions/runs/36790486763)
at `bb3ce3280f769802e7ab6d66a6b1fb782e026a3c`.


## Anchored remote coverage and copy resumption

The host coverage walk had a real anchoring bug. With finished ranges
`[(0, 9), (20, 29)]` and local start 20, it seeded coverage from the obsolete
first range and stopped at the gap, returning nine instead of 29. With only
`[(0, 9)]` it returned nine instead of no coverage. Because copying resumes at
that endpoint's successor, both cases resumed at ten, below the local start;
the same shared result could also suppress physical-retention progress.
Regressions in both existing broker test tables failed before the repair.

The shared adapter now sorts metadata and delegates to
[`remote_covered_through`](../crates/verified/src/retention/coverage.rs).
Its contract describes interval geometry rather than a reference implementation:
every offset from the anchor through the returned endpoint is covered by an
actual input range; some range attains the endpoint; and no overlapping or
adjacent range extends it. No coverage means malformed metadata or no range
containing the anchor. The implementation checks the entire listing before
walking coverage, so a corrupt tail beyond a gap also fails closed. Sorted starts
are the sole ordering precondition; overlaps, repeated starts, obsolete ranges
and signed extrema are admitted.

Three native tests compare bounded coordinate enumeration with the kernel and
exercise obsolete prefixes, real gaps, overlapping ranges, malformed tails,
inclusive endpoints and successor saturation. Copy completion, truth of object
metadata, snapshot coherence and physical deletion remain external assumptions.
The existing local-retention and copy callers share the repaired root; neither
adds a separate workaround. This kernel establishes availability of a remote
prefix, not the full physical-deletion protocol.

Publication validation: pinned Creusot 0.13.0 generation and full no-cache
saved-session replay prove all 543 files;
322 verified library tests and 92 broker remote-log-manager tests pass. The four
verified/log/remote-storage Bazel targets, a scoped broker Bazel run, workspace
all-target Clippy with warnings denied, formatting and Creusot/mutation checks
pass. The new kernel appears in mutation discovery. All 1,084 preceding proof
artifacts, including both timestamp interval batches, remain byte-identical;
only the new coverage session is added to that baseline. At that checkpoint
there were 56
executable compositions across 38 topic files, and every verified Rust source
remains below 300 lines. Local checks do not establish remote CI success.

## Remote intervals through the local-retention target

The coverage kernel closes only one boundary. `local_retention_prefix` consumes
boolean blocking flags, and a correct prefix selector cannot establish that
those flags faithfully describe actual remote availability. The new consumer
[`remote_coverage_bounds_local_retention`](../crates/verified/src/composition/covered_retention.rs)
derives each flag from finished-copy intervals and the oldest local offset,
consumes the existing size/time policy, and converts the last selected inclusive
offset through `retention_delete_target`. It returns the actual coverage,
segment facts, selected count and optional target.

The contract requires ordered, nonnegative, disjoint local segment ranges and
start-sorted remote metadata; it does not assume that remote metadata covers the
local segments. It exports coverage admission, an attained maximal endpoint,
exact fact construction and exact policy output. A target is precisely the
representable successor of the last selected end. Every offset from the oldest
local start up to that target belongs to an actual input remote interval,
including holes between local segment ranges. Every selected segment is also
covered when successor exhaustion prevents a target. An independently stated
progress guarantee selects at least every initially unblocked expired prefix,
regardless of the size debt. The active segment is always blocked, whether its supplied size is zero or positive.

This completeness matters: a weaker draft exporting only conditional deletion
safety and policy equality over its returned facts would still admit declaring
all segments blocked. Exporting complete maximal coverage closes that escape.
It also distinguishes legitimate successor exhaustion from an artificial
always-empty plan. The shared open logical predicate `remote_covers_offset`
defines membership in an actual input interval and makes the per-offset export
usable by downstream quantifiers. The kernel's interval algorithm is unchanged;
the consumer uses that predicate rather than another coverage walk.

Four native tests use independent coordinate enumeration and sequential size
and time passes. Generated profiles include malformed listings, valid listings,
and a guaranteed fully covered expired prefix, so successful deletion is not
left to chance. Explicit cases cover stale disconnected prefixes, real gaps,
local gaps, overlapping remote ranges, corrupt tails, zero-byte segments,
active-segment protection and signed offset/unsigned size exhaustion. All 326
verified tests pass in the positive native run. The consumer, coverage kernel and
strengthened selector prove with pinned Creusot 0.13.0.

Exact segment/expiry/size projection, complete truthful finished-copy metadata,
coherent publication and durable physical deletion remain external. This proves
coverage of newly evicted offsets; it does not establish remote availability
below an already advanced oldest-local start or the entire crash protocol.

Seven safety controls completed before the independent progress repair. Hiding
coverage, prefix or successor exports left each helper proof and all 326 native
tests passing while the consumer failed. A paired global-maximum walk and
weakened geometry contract proved its own helper but failed this consumer and
native oracles: maximum endpoints cannot hide holes. Declaring no coverage,
unblocking every segment or selecting nothing failed the consumer and native
tests. Sources were restored and new failure seeds quarantined.

A stronger paired no-op exposed another weakness: setting both the selector and
its reference fold to zero proved the helper and the original consumer while
native tests failed. Reference equality plus conditional deletion safety did
not establish progress. `local_retention_prefix` now independently guarantees
that its first kept row before the deletion limit is blocked or not expired.
The consumer independently exports selection of every initially unblocked
expired prefix. With the same paired zero mutation, the stronger primitive
fails. If its stopping guarantee is removed so the weak primitive proves again,
the stronger consumer still fails. Native tests fail in both cases. These scoped
controls close the all-zero selector/reference escape for expired prefixes.
A follow-up adds independent first-segment progress when its size fits the
active size debt, including non-expired and zero-byte segments. The local
kernel exports this law and the remote-coverage composition consumes it.
A paired mutation that stops both the selector and reference fold on
non-expired size-pass segments fails the stronger kernel. Removing only the
new progress clause lets that mutated kernel prove again, but leaves the
consumer at 37 of 38 obligations. Sources are restored byte-for-byte; pinned
forced generation and the four independent covered-retention native checks
pass with the stronger law. These checks are not a full
mutation sweep.

Final local validation passes: fresh-target generation and no-cache canonical
saved-artifact replay prove all 544 files; all 326 native verified tests and the
verified, verified-doc, log and remote-storage Bazel targets pass. Workspace
all-target Clippy with warnings denied, formatting, Creusot package/skip checks,
mutation configuration checks and `git diff --check` pass. The new consumer is
included in mutation discovery. All 1,082 unrelated existing proof artifacts
remain byte-identical; only the coverage and local-prefix sessions are refreshed
and the consumer session is added. There are 57 executable compositions across
39 topic files, and every verified Rust source remains below 300 lines.

This batch is prepared on `codex/proof-covered-retention`, above PR #1264.
The preceding published head `e894b2ba143c3de76b192fd67aafca37d6768d82` completed
[CI successfully](https://github.com/krabka-io/krabka-broker/actions/runs/36797609074).
That success does not qualify this new head; its remote checks run after
publication.

## Independent retention byte cost

The expired-prefix guarantee closes the all-zero escape, but does not require
size-driven progress. A paired control disables the size arm in both
`local_retention_model` and `local_retention_prefix`: the primitive and coverage
consumer still prove, while four native tests fail. Agreement with the fold plus
conditional coverage and time-driven progress still admits skipping all
non-expired segments, even when the budget fully funds them.

The selector now exports two independent byte-cost bounds. Every selected
non-expired row fits the initial debt together with every older row. If the first
kept row is unblocked and before the deletion limit, its prefix cost exceeds that
debt. The existing stopping guarantee separately requires it to be non-expired.
With no size pass, every selected row must be expired. These bounds preserve
time-driven deletion beyond the size debt; they do not cap all deleted bytes by
that debt.

[`local_prefix_bytes`](../crates/verified/src/retention/local_bytes.rs) sums exact
row sizes as mathematical integers, including totals above `u64::MAX`. Its proved
monotonicity lemma handles arbitrary zero-byte rows. Both definitions remain in
a small logical module rather than adding another native retention walk. The
selector's ghost accounting connects the remaining debt to that prefix sum
while sizing, and records that the initial debt has already been exceeded after
transition to expiry. Its runtime body and the reference fold are unchanged.

The coverage consumer now exports budget justification for each selected
non-expired local row, selection of every fully funded unblocked prefix, and
selection of every unblocked prefix whose rows individually qualify by expiry
or funded byte cost. This includes a funded prefix followed by expired rows;
neither homogeneous progress guarantee alone states that handoff. It
consumes the selector's byte-cost exports and the monotonicity lemma, alongside
coverage, exact fact construction and the checked successor. Neither a remote
maximum across a hole nor invented row sizes can satisfy those contracts.
Truthful segment sizes, exact host debt projection, coherent copy metadata and
physical deletion remain external.

A new oracle table exercises exact whole-segment debt boundaries, `None` versus
zero debt with zero-byte rows, totals exceeding `u64::MAX`, and expiry that must
not restart the size pass for a later non-expired row. It also includes remote
holes and zero/positive/exhausted active sizes. All 327 native tests pass in the
positive run; the primitive, byte lemmas and strengthened consumer prove.

Three scoped controls distinguish independent exports from matching code.
Hiding both byte-cost exports preserves the helper proof and all 327 native
tests but fails the consumer. Disabling the size arm in both selector and fold,
or making both select without charging debt, fails the stronger primitive.
Removing the byte exports and their ghost bookkeeping lets each weakened
primitive prove again, while the strengthened consumer still fails. Native tests
fail for both paired mutations. All source mutations are restored and newly
created failure seeds quarantined. These are targeted controls, not a full
mutation sweep.

Final local generation and no-cache canonical saved-artifact replay pass for all
546 files in the exact pinned CI image. The final mixed size/expiry export also
proves. All 327 native verified tests, the verified/verified-doc/log/remote-storage
Bazel targets, workspace all-target Clippy with warnings denied, formatting and
Creusot/mutation configuration checks pass. The final logical export was followed
by another verified-crate test and Clippy run. All 1,084 unrelated proof artifacts
remain byte-identical to the published repair baseline; only the local-prefix and
coverage-consumer sessions are refreshed, with two byte-lemma sessions added.
There are still 57 executable compositions across 39 topic files; every verified
Rust source remains below 300 lines.

The separate proof-replay repair is pushed to PR #1265 at
`29c8568e9221bca96e460f5bfdb5a751e45f2af9`. Its
[proof CI job passes](https://github.com/krabka-io/krabka-broker/actions/runs/36807206627/job/110194110803);
the full CI run is tracked separately. That repair carries positive coverage
membership directly in the coverage-loop invariant and exposes the exact
blocking projection after appending the active sentinel, without changing runtime
behavior or exported contracts. The byte-cost batch is included in the next
retention-ledger PR above that repair; the earlier published proof-CI result
does not qualify this new batch.


## Published trim through remote cleanup

The remote retention contract had the same paired-model weakness: replacing
both the reference fold and selector with zero still proved, while the existing
hand-calculated policy tests failed. The selector now independently justifies
each selected row by a floor breach, expiry, or a fully funded charged prefix.
Its first kept row must fail all three axes. The mathematical
[`remote_prefix_charge`](../crates/verified/src/retention/remote_bytes.rs) counts
expired rows but excludes floor breaches, including rows that breach both axes;
it handles prefix totals above `u64::MAX`. Ghost accounting connects this
independent ledger to the existing saturating debt walk. Runtime behavior and
the reference fold remain unchanged.

[`published_trim_bounds_remote_breach_cleanup`](../crates/verified/src/composition/remote_breach.rs)
connects DeleteRecords admission, retryable WAL/local trim completion, successful
floor publication, actual inclusive remote ranges and the remote retention
selector. It derives every breach from the published checkpoint. Partial store
progress or failed publication preserves the previous checkpoint. With size and
time retention disabled, an enabled cleanup selects exactly the initial whole-row
prefix ending strictly below that checkpoint; a straddling or retained row stops
it even if later metadata rows would qualify. A missing checkpoint and a tier
that refuses deletes select nothing. The canonical floor respects the committed
and delivery bounds, and two successful store steps complete reconciliation.

Completion and publication inputs mean actual durable outcomes. The theorem
proves the pure planner protocol under those observations, not filesystem sync,
atomic checkpoint publication, metadata truthfulness or physical object deletion.
It does not assume that observed store progress establishes a published floor.
Independent native oracles use cumulative `u128` byte cost, successful-stage
counts and whole-range prefix boundaries. Boundary tables cover zero debt,
free floor breaches, expired oversized rows, failed/partial trim attempts,
publication retry, disabled deletion, overlapping ranges and integer extremes.


Scoped mutations publish the new floor before both stores complete, or replace
cleanup with zero. Both fail the composition proof and both new native tests.
All temporary source changes are restored and generated failure seeds quarantined.
These controls complement the paired remote no-op control and the independent
local byte-budget controls above; they are not a full mutation sweep.

Final local generation and canonical no-cache saved-artifact replay pass for all
548 proof files in the exact pinned CI image. All 331 native tests, the four
verified/verified-doc/log/remote-storage Bazel targets, workspace all-target
Clippy with warnings denied, formatting and proof/mutation configuration checks
pass. All 1,090 artifacts unrelated to the remote extension remain byte-identical
to the completed byte-budget batch. Relative to the published parent, three
existing sessions change and four are added; 1,082 published artifact files are
preserved exactly. There are 58 executable compositions across 40 topic files,
and the largest verified Rust source is 288 lines.

This batch is prepared on `codex/proof-retention-ledgers`, above PR #1265.
The parent's [full CI rerun succeeds](https://github.com/krabka-io/krabka-broker/actions/runs/36807206627)
at `29c8568e9221bca96e460f5bfdb5a751e45f2af9`; that result does not qualify
the new layer. Remote CI for the new commit is tracked separately.


## Retention selection through completed remote deletion

Selection does not establish deletion completion or justify moving the logical
floor across a gap in finished remote metadata. The production pass already
stopped its floor at such gaps, but still computed `md.end_offset() + 1` directly.
The metadata constructor permits an inclusive end of `i64::MAX`. A new real
retention-pass regression reaches `DeleteSegmentFinished` and reproduces an
arithmetic overflow panic on that endpoint; `i64::MAX - 1` exercises the last
representable successor in the same test.

The host now delegates each completed range to
[`remote_retention_floor_step`](../crates/verified/src/retention/remote_floor.rs),
which consumes the existing checked `retention_delete_target`. The step exports
monotonicity and exact closure of the contiguous run at a gap, malformed
coordinates or successor exhaustion. Independently of its exact branch result,
every offset it crosses belongs to that completed range. Once closed, the run
cannot resume on a later row. The overflow regression passes after this change.

[`completed_remote_retention_bounds_floor`](../crates/verified/src/composition/remote_delete.rs)
derives floor, expiry and size facts from actual finished ranges, selects with
all three retention policies and consumes completion observations up to the
first failure. Its contract carries funded-prefix eligibility and maximal policy
selection, exact success-prefix bounds, and a monotone floor whose every crossed
offset belongs to a successfully completed deletion. A separate geometric
prefix predicate requires each representable range to reach the initial floor or
one of its predecessors. Every such completed prefix makes progress past each
of its inclusive endpoints, so unchanged-floor implementations cannot satisfy
the consumer. Earlier valid progress survives a later unrepresentable endpoint.

The composition admits overlapping, obsolete and arbitrarily ordered valid
ranges; the production listing is sorted. A gap freezes the floor even if a later
out-of-order row could fill it. Supplied expiry flags and size debt must reflect
the host's policy projection. A true completion observation means the delete
lifecycle actually completed, not that it was merely selected or attempted.
Metadata truth, object-store I/O, concurrent snapshots and durable publication
of the returned floor remain outside this pure planner proof.

Native oracles independently compute cumulative charged bytes and predecessor
connections in the completed range prefix. Tables cover missing/failed deletion
observations, later successes after failure, floor-only breaches, mixed expiry
and byte funding, zero debt, overlaps, gaps and successor exhaustion. No recursive
copy of the production floor fold is used as the native oracle.

Scoped negative controls checked four failure modes after the positive proof:

- Removing the gap check from both the step body and its exact branch contract
  still fails the independent crossed-offset bound. Hiding that bound lets the
  weakened step prove, while the stronger composition and native oracles reject
  it.
- Hiding the remote selected/stopping byte-budget exports leaves the selection
  helper provable and all 335 native tests green, but breaks the aggregate
  composition. Its budget claim therefore depends on semantic exports.
- Returning the initial floor satisfies monotonicity and empty crossed-offset
  coverage, but fails the connected-prefix progress claim and native oracle.
- Ignoring a failed completion observation fails both the success-prefix proof
  and the behavioral tests.

All temporary control edits were restored before final validation. Pinned-image
generation and cache-free replay of the saved artifacts each prove 551 files;
native verification passes 335 tests. Bazel passes
the broker retention target (27 tests), verified unit/doc targets, log tests and
remote-storage tests. Workspace all-target Clippy with warnings denied, repository
formatting and Creusot/mutation configuration checks pass. Only three new proof
sessions are retained; the 1,096 previously published artifact files remain
byte-identical. Every verified Rust source remains below 300 lines (largest: 288).

This batch is on `codex/proof-remote-delete-frontier`, above PR #1266. The parent's
Produce-readiness fixture repair is inherited from `d5bb60ed`; its remote proof
job passes, while its full workflow is still running with one image-tool download
HTTP 500 before tests. Parent CI does not qualify this new layer.

## Schema field identity through Produce admission

`schema_batch_admission` exports a scalar law over supplied counters. Equality
does not identify the fields counted: counting the same successful field again
could conceal a different rejected field. The production batch walk visits each
record's key and value, while the validator skips empty non-null fields before
framing or registry I/O. Those exemptions need to be included in any stronger
claim; fail-open is not a substitute for framing a nonempty field.

[`framed_schema_walk_admission`](../crates/verified/src/composition/schema_walk.rs)
constructs both counters by visiting each input position. It consumes the actual
field-selection, big-endian frame decoder, registry-failure policy and batch
admission kernels. The returned witness has exactly one row per position, the
correct key/value action, the exact four-byte schema ID when framing applies,
and that position's acceptance outcome. Batch admission is equivalent to a
complete walk in which every applicable position is admitted. Null and disabled
fields are skipped, empty fields pass, and nonempty applicable fields require a
valid frame plus successful validation or fail-open on a transient lookup failure.
Unknown, permanent and malformed lookup failures never gain that exception.

[`schema_checked_produce_frontier`](../crates/verified/src/composition/schema_produce.rs)
consumes that witness together with Produce header admission and the checked
durability frontier. It checks one key/value pair per decoded record and derives
the record count from those pairs. An admitted result exists exactly when the
pair shape, actual count/header relation, complete schema walk and representable
frontier all pass. Its exclusive frontier is the append base plus the actual
record count, and every returned position preserves its decoded ID and acceptance.
This rules out accepting malformed/incomplete walks and implementations that
always reject.

These are pure compositions of existing production decisions, not a replacement
for the asynchronous validator. Faithful enumeration of decoded physical fields,
complete parsing/CRC checks, coherent header facts, correct registry-failure
classification, subject/body validation and durable append remain host obligations.
The `checked` observation means those required checks succeeded; it is not proof
of serde or registry behavior. A failure observation overrides a contradictory
success flag, and skipped/empty fields do not consult either observation.

Native row oracles use `u32::from_be_bytes` and conjunction over actual positions
instead of scalar admission counters. The Produce oracle independently compares
the pair count to the header delta and uses `i128` frontier arithmetic. Tables and
properties cover field-role order, null/empty/disabled fields, missing/bad frames,
all four failure classes, definitive validation rejection, incomplete walks,
count mismatches, control/timestamp/producer gates and offset exhaustion.

Six scoped negative controls exercise the dependencies:

- Repeating the first field at every position, with a matching weakened walk
  contract, leaves that walk provable but breaks the Produce consumer and native
  row/count oracles. Equal successful counters cannot conceal another field.
- Hiding only the walk's batch-admission equivalence leaves its own proof and
  all 339 native tests passing, while the Produce consumer fails.
- Ignoring counter differences in both the scalar batch gate and its contract
  leaves that gate provable, while the field-walk proof and tests fail.
- Extending fail-open to every registry failure in both body and contract leaves
  the failure policy provable, while per-position admission and tests fail.
- Ignoring the record-count/header-delta relation in both Produce body and
  contract leaves header admission provable, while the aggregate count/frontier
  claim and behavioral tests fail.
- Returning rejection for every prepared append fails both the aggregate
  admission equivalence and the native oracle.

All temporary source edits were restored byte-for-byte before final validation.
The final aggregate also exports exact key/value actions for every returned
position, preserving the subject role as well as the ID and acceptance outcome.
Pinned-image generation and cache-free replay of the retained artifacts each
prove 553 files. All 339 native tests, the three focused
Bazel targets (including 81 broker schema tests), workspace all-target Clippy
with warnings denied, repository formatting and proof/mutation configuration
checks pass. Two new proof sessions are retained; all 1,102 previously published
artifact files are preserved exactly. Every verified Rust source remains below
300 lines (largest: 288), across 43 composition topic files.

This work remains local on `codex/proof-schema-walk`, above PR #1267. The parent's
[full CI retry](https://github.com/krabka-io/krabka-broker/actions/runs/36816668685)
passes at `a0600a966fefa23f6520e7b3c822c6449fea3d91` after retrying an image-mirror
blob failure before tests; all its checks are green. That published 551-file
layer does not qualify this new 553-file layer.

## Actual voter membership and adjacent quorum overlap

The reconfiguration kernel proves an exact next voter count and control-record
shape, but scalar counts do not identify the voters. Similarly,
`election_has_quorum` assumes its grant count came from unique current voters.
Neither fact alone proves that the old and next quorums share an actual node.
The controller builds its committed membership from `VoterSet`'s `BTreeMap`
and constructs one next set with `with_voter` or `without_voter`; it rejects
any count disagreement before appending control records.

[`constructed_voter_reconfiguration`](../crates/verified/src/composition/reconfiguration/membership.rs)
consumes the existing unique-ID validator and reconfiguration policy, then
constructs actual logical node IDs. It rejects empty/duplicate memberships
and an incoherent target-presence projection. An admitted add introduces only
the fresh target, a removal removes exactly that target, and update/finalization
preserves membership. The constructed set is nonempty and unique, its length
matches the proved plan, and the count changes by at most one. Admission is
an equivalence, so rejecting every request cannot satisfy the contract.
The first old ID anchors the reused validator; that reuse asserts neither
leadership nor installation of a WAL voter set.

[`reconfigured_majorities_overlap`](../crates/verified/src/composition/reconfiguration/quorum.rs)
consumes that actual membership. It derives two grant ledgers from one slot per
old ID plus a candidate slot, ignoring an already-present candidate and excluding
the removed ID from the next quorum. The returned counts equal the mathematical
ledger over actual membership. Each quorum uses its own actual set size. If both
majorities exist, the result names an actual shared voter with both grants;
absence of that witness is equivalent to absence of any shared grant. The proved
arithmetic lemma shows that the two majority thresholds exceed the size of the
union for a single change. It cannot establish that fact for arbitrary changes.

The independent native oracle uses `BTreeSet` membership and grant intersections,
not supplied counts or the proof's recursive ledger. It enumerates every pair of
ballot masks for one through five voters, including odd/even add and removal,
version-0 update preflight and finalization. Properties also cover duplicates,
empty/unsorted sets, wrong target projections and admission flags. A concrete
two-addition counterexample has old voters `{0,1,2}`, next voters `{0,1,2,3,4}`,
and disjoint majorities `{0,1}` and `{2,3,4}`. Sequential adjacent overlap does
not justify overlapping uncommitted changes or arbitrary history safety.

These are logical NodeID claims. Captured IDs/counts, grants, directory matching,
catch-up observations and epoch facts must be truthful. Unknown-directory
binding does not prove physical storage continuity. Control-record commitment,
durable replication, transport authentication and the complete Raft protocol
remain host obligations. The compositions mirror the production set operations;
they do not verify the external `BTreeMap` implementation or asynchronous adapter.

Four scoped negative controls check the composition boundary. Hiding the
constructor's exported membership guarantees leaves its proof and all 342 native
tests passing, while the quorum consumer fails. Weakening both majority-size
and quorum-gate contracts together with their bodies leaves both primitive
proofs passing, but breaks the consumer and behavioral tests. Unconditional
rejection fails the admission equivalence and tests; naming the change target
instead of the actual common voter fails the witness proof and set oracle.
All controls used cache-free proof search at depth two, where the positive five
sessions also pass. A broader search without constructor exports timed out;
that timeout is not counted as a proof rejection. Temporary source edits were
restored byte-for-byte and failing-control seeds kept outside the repository.

Pinned-image generation and cache-free canonical replay both prove all 558
sessions. All 342 verified-crate tests, all 380 Raft tests, the three focused
Bazel targets, workspace all-target Clippy with warnings denied, repository
formatting and proof/mutation configuration checks pass. Five new proof
sessions are retained, and all 1,106 parent artifact files are unchanged.
Every verified Rust source remains below 300 lines (largest: 288), with 47
composition topic files. This batch is local on `codex/proof-quorum-membership`,
above published PR #1268; it has no remote qualification yet. The parent's
[full CI](https://github.com/krabka-io/krabka-broker/actions/runs/36823054825)
and benchmark workflows pass at `bbd81b8fe5b9493e2f25f2d87e8c55dab5f354af`.
Those remote results qualify the parent's 553-session layer, not this local
558-session layer.

The membership constructor and overlap consumer also carry the admitted plan's
exact version and control-record flags, and the single-flight leadership facts.
Their bodies already obtained those guarantees from the production admission
kernel, but previously stopped exporting them. The stronger interface is proved
across all five reconfiguration sessions; all 342 native tests and workspace
all-target Clippy still pass. This enables a consumer to derive zero, one or two
actual control-record deltas from that same plan instead of trusting a count.

## Control-record geometry carried by actual replication prefixes

The logical voter-overlap proof accepts Boolean grants. Their counts and shared
ID are exact, but that interface alone says nothing about which log prefix each
grant covers. Metadata record deltas independently describe a contiguous batch;
a supplied batch count can still disagree with a reconfiguration's actual plan.
The controller builds its control records from the admitted plan flags, writes
the KRaftVersion record before Voters when both are present, and uses the latest
local voter view during replication. Version-0 update preflight writes no batch.

[`reconfiguration_control_prefix_support`](../crates/verified/src/composition/reconfiguration/control_support/prepare.rs)
constructs the record count from those actual flags and consumes the production
metadata-delta and local-append-coordinate kernels. It obtains the exact
exclusive end from the final actual delta, then constructs the old/new grant
slots by comparing each node's reported prefix with that end. The existing
membership/overlap composition derives the distinct counts and shared node.
An appended witness exists exactly when admission, representable geometry and
both actual prefix majorities qualify. Each returned record has its own exact
delta, and the shared node's old and new reports reach past every record in the
batch. A zero-record preflight returns an admitted empty-delta/no-append witness
without demanding prefix votes or a representable append base.

The admitted plan's version and control-record flags now survive both exported
interfaces. That makes one-record add/remove/update and two-record finalization
part of the aggregate guarantee. The independent native oracle derives the
record count from expected plan flags, uses wide `i128` end arithmetic, and
intersects actual `BTreeSet` prefix supporters. Exhaustive masks for one through
five voters cover reports ending exactly at, or one offset short of, the whole
batch; boundary tables cover negative bases and exclusive-end exhaustion near
`i64::MAX`. They also preserve no-append preflight with unusable bases/reports.

Reported positions must belong to those exact node IDs and matching logical
log histories. The shared node's old/new observations must refer to the same
physical storage identity. Complete prefix/control encoding, epoch and directory
facts remain host obligations. The KRaft core tracks fetched prefixes; the WAL engine records
fsynced offsets. Fsync follows only when the supplied observations establish it.
This witness qualifies support in both logical memberships. The controller's
latest-view commitment protocol, election histories, actual record bytes and
crash durability remain outside this composition. Its real append path already
uses `local_append_coordinates` to reject an unrepresentable successor before
writing, so this proof introduces no runtime guard or protocol change.

Four scoped controls test those dependencies. Hiding only the exported plan
facts leaves the membership and overlap helper proofs plus all 345 native tests
passing, while the control-prefix consumer fails. Returning all-zero metadata
deltas with a matching weakened contract leaves the metadata helper provable,
but fails the aggregate end/delta claim and native tests. Comparing reports to
the batch base instead of its exclusive end fails the prefix bridge and native
oracle. Rejecting every preflight fails the admission equivalence and tests.
The positive three sessions pass at the same cache-free depth-two search used
for the controls. All temporary edits were restored byte-for-byte, with failing
control seeds retained outside the repository.

Pinned-image generation and cache-free canonical replay both prove all 561
sessions, including the parent's stronger plan exports. All 345 verified-crate
tests, all 380 Raft tests, the three Bazel targets, workspace all-target Clippy
with warnings denied, formatting and proof/mutation configuration checks pass.
Three new sessions are retained; all 1,116 parent artifact files are unchanged.
Every verified Rust source remains below 300 lines (largest: 288), across 50
composition topic files. This layer remains local on
`codex/proof-reconfigured-append-support`, above `codex/proof-quorum-membership`;
neither local layer has a published PR or remote qualification. PR #1268 remains
green at its published 553-session schema commit.

## Whole-batch control commitment and waiter resolution

A shared whole-batch prefix witness does not yet connect the controller's
high-watermark advance, per-history lookup and reconfiguration waiter. In
particular, value equality is weaker than record commitment. Version
finalization writes KRaftVersion followed by the unchanged Voters value. After
the HWM passes only the first record, both latest control values can equal the
committed values even though the new Voters record is still outside the
committed prefix. The real pending-change guard and offset-based waiter must
continue to cover that second record; this observation alone is not a controller
safety defect.

[`reconfiguration_control_commit_waiter`](../crates/verified/src/composition/reconfiguration/control_support/commit.rs)
consumes the admitted plan, actual deltas and common prefix supporter. It builds
the absolute control offsets and advances the HWM against the supplied actual
log end, not just the control batch end. The ordered-history kernel yields the
committed prefix; the visibility kernel marks each actual row, including its
version/Voters role. The waiter frontier equals whole-prefix commitment. Its
readiness is equivalent to either the previous or requested HWM reaching the
batch's exclusive end, accounting for monotonic clamping. Later appends, an HWM
inside or beyond this batch, and backward requests preserve that equivalence.
No-append preflight consumes no coordinates or waiter. Appended batches reject
an incoherent HWM/log-end projection without adding an unchecked precondition.
The admission contract is an equivalence, so rejecting every input cannot prove
it.

The native oracle reuses the independent membership/prefix-set oracle and counts
visible actual rows linearly. It does not use the history binary search or the
waiter kernel to determine readiness. Properties cover arbitrary observations;
fixed tables cover all change kinds, short prefix reports, signed offset limits,
invalid coordinates and no-append preflight. Another table advances through and
past both finalization rows while subsequent metadata extends the log end.
The concrete equal-membership counterexample commits only the version row.
A real three-voter controller regression independently elects the leader,
commits its epoch, appends finalization and advances its HWM in two steps. It
checks the matching voter values with the new Voters record exactly at the HWM,
then verifies the pending waiter/channel until that final record is passed.

These guarantees require truthful IDs, reported prefixes, matching histories,
physical storage identity, actual control bytes and the real log end/HWM
projection. Admission facts describe this batch's admission; progress describes
a later advance in that same log. They need not describe the same instant, and
the theorem does not read or revalidate live controller state.
Both-membership prefix qualification does not establish the KRaft
core's latest-view consensus rule or election safety. The host still establishes
which HWM may be requested. Fetched prefixes do not prove fsync, and neither
this composition nor the regression proves crash durability or complete
asynchronous controller execution.

Five scoped negative controls test that boundary. Making history lookup
inclusive, making the waiter require a strictly greater HWM, or removing the
HWM's log-end clamp keeps each primitive provable under its correspondingly
changed contract, but fails the aggregate proof and native row oracle. Rejecting
every preflight fails admission equivalence and native tests. Replacing the real
controller's offset gate with equality of latest/committed control values makes
the new controller regression fail at the still-pending final record. The
positive composition passes the same cache-free depth-two search used for the
proof controls. Every temporary edit was restored byte-for-byte, and generated
failing-control seeds were kept outside the repository.

Pinned-image generation and cache-free canonical replay both prove all 562
sessions. All 349 verified-crate tests, all 381 Raft tests, the three focused
Bazel targets, workspace all-target Clippy with warnings denied, formatting and
proof/mutation configuration checks pass. One new session is retained; all
1,122 parent artifact files are unchanged. Every verified Rust source remains
below 300 lines (largest: 288), across 51 composition topic files. This layer is
local on `codex/proof-control-commit-waiter`, above published PR #1270, and has no
remote qualification yet.

## Whole-batch truncation and control-history restoration

The earlier fenced-replication composition clamps logical coordinates to the
requested divergence cut. Without actual batch extents, it cannot establish
that those coordinates fit the physical prefix left by whole-batch storage
truncation. Likewise, ordered control-history lookup proves which offsets
precede a supplied frontier, but does not prove that the host supplies the
retained log end. These are distinct integration obligations.

The production audit found that `KraftLog::truncate_to` clamped its HWM to the
requested cut, and both controller truncation paths restored control histories
at that cut. Storage discards a complete batch when the cut lies inside it. A
cut at 3 in batches ending at 2 and 4 therefore leaves physical end 2. The old
HWM could remain at 3, and the version row at offset 2 could survive in memory
after its batch was removed. These boundary regressions fail on the original
implementation. The fix reuses `truncation_frontier` at the actual retained log
end and passes that end to history restoration in both controller paths.

`whole_batch_truncation_bounds_controls` consumes the complete physical batch
ends and an ordered history, including snapshot/genesis baseline rows. It
derives exactly the whole-batch retained prefix, then composes the physical
end with HWM clamping, retained and committed history prefixes, and waiter
presence/readiness. History rows in the first discarded batch cannot survive,
even when their offset precedes the requested cut. The independent oracle
filters physical ends and history rows linearly. Properties and boundary
tables cover empty and shifted logs, every interior cut, offset limits, and
waiter equality.

A real log regression truncates inside a two-record batch with four prior
HWMs, advances again, and reopens the persisted log. A controller regression
uses actual version/Voters batches and exercises both the local action and
fenced follower-Fetch response, checking both restored histories and the
retained version. The follower's HWM remains before the discarded batch; this
is a boundary consistency test, not a demonstration that a valid Kafka leader
produces an interior divergence hint.

Complete batch decoding, truthful history association, valid consensus
divergence selection and successful storage effects remain host obligations.
The theorem permits the mechanical HWM clamp after any modeled cut; it does
not establish that truncating committed metadata is safe or roll back the
committed metadata image. Cuts below the first local offset use reset-to and
are outside this retained-prefix theorem. Snapshot replay and crash durability
are not established by this composition.

Six scoped variants fail as intended: using the requested cut for the composed
HWM or retained history, making history lookup inclusive, retaining a batch
whose last offset equals the cut, restoring the old host HWM clamp, and
restoring the local action's requested-cut history boundary. The inclusive
history and partial-batch variants change both primitive body and contract;
each helper remains provable while the aggregate proof and native oracle fail.
The positive composition passes the same cache-free depth-two search.

A seventh variant changes only the Fetch branch's restoration back to its
requested cut. The final-state regression still passes: Fetch later feeds the
core, which emits a second truncation action at the now-retained boundary.
This masks the first restoration in the final state. The regression therefore
does not independently qualify that first call site, although passing the
actual end there avoids transient discarded history. The local-action control
independently catches the mismatch. All temporary edits were restored, and
failing property seeds remain outside the repository.

Pinned-image generation and cache-free canonical replay both prove all 563
sessions. All 352 verified-crate tests, all 383 Raft tests, the three focused
Bazel targets, workspace all-target Clippy with warnings denied, formatting,
and proof/mutation configuration checks pass. One new session is retained;
all 1,124 parent artifact files are unchanged. Every authored verified Rust
source remains below 300 lines (largest: 288). This layer remains local on
`codex/proof-control-truncation`, above published PR #1271, with no remote
qualification yet.

## Physical truncation, rebuilt producer windows and retry waits

The physical-prefix theorem and the producer-window theorem previously met at
a host assumption: the window's snapshot rows must already be valid at the
new log end. The audit checked the real broker callers before changing them.
Producer tracking drops an entry by its last offset, which agrees with whole
batch removal. The future-log writer already rejects every swap while a
truncation/reset mark remains unreported, even if the log grew back to equal
ends. The offset-equality primitive alone does not establish byte/history
equality, and no runtime change is needed for either checked guard.

`truncated_replay_bounds_first_retry` derives the rebuilt window from the
complete physical batch-end sequence and an arbitrary-length history of
same-producer data batches with nondecreasing epochs. Each row's exclusive
frontier names a real batch end. The exact physical cut establishes valid
surviving rows at the retained end. The composition then selects the latest
surviving epoch's last five batches and consumes the full-window retry theorem.
It exports that window's bounds, exact retry classification, first matching
original history index, physical coordinates and exclusive acknowledgement.
Every duplicate frontier fits the retained end. Clamping the old HWM preserves
readiness for that returned frontier.

Tail deletion does not preserve the previous retry selection. If an older
sequence alias reenters the five-slot window, it can become the first match
even while the previously selected alias remains physically present. Deleting
all data at a newer epoch can also restore an earlier epoch. The theorem
therefore classifies requests against the rebuilt window and latest surviving
epoch, rather than assuming an unchanged window or original selection.

The independent oracle filters physical ends and data rows, takes the latest
epoch's five-batch suffix, and uses modular sequence arithmetic. Properties
include interleaved other-producer batches, histories longer than the window,
multiple epochs and arbitrary prefix cuts. Boundary tables cover interior
removal, slot repacking, wrap aliases, empty released/trunk rules and maximum
signed offsets. Real log/tracker regressions exercise aligned and interior
cuts, older-alias reentry and earlier-epoch restoration. Reopening qualifies
only the actual latest metadata loaded by the snapshot.

Complete decoded physical ends, faithful data-row span/PID/epoch association,
complete physical data replay and serialized producer operations remain host
obligations. A snapshot-only reopen may retain just the latest batch metadata;
this theorem does not imply that every physically surviving older batch remains
deduplicable after such a restart. Snapshot policy and marker-only entries are
outside its data-history boundary. Byte equality, safe consensus truncation
and crash durability are not established.

Six scoped controls test the dependency boundary. Hiding only the exported
exact physical end or first-alias minimality leaves the corresponding helper
proof and native tests passing, but rejects the aggregate proof. Using a row's
base instead of its last offset also rejects the proof, but the native oracle
still passes for faithful physical spans: at the actual whole-batch end those
two retention tests agree. That mutation is not claimed as a native negative
qualification. Selecting by the row's base at the requested interior cut,
requiring a strictly greater HWM for acknowledgement, or rejecting every empty
rebuilt window rejects both the aggregate proof and independent native oracle.
The strict-waiter primitive is still provable with its matching changed contract.
The positive composition passes the same cache-free depth-two search used for
those controls. All temporary edits were restored, and property seeds remain
outside the repository.
The final contract also exports fencing and successor classification from the
latest surviving row, so downstream compositions can consume those decisions.


Final qualification used the pinned Creusot v0.13.0 image and a fresh target.
Both cache-free generation and canonical replay prove all 564 sessions. All
356 verified-crate tests and 33 native producer-state tests pass, including
the three actual log/tracker regressions. Bazel verified tests/documentation
and the producer-state-filtered broker target pass, as do strict workspace
Clippy, all four proof/mutation configuration checks, and repository formatting.
The 1,126 parent artifact files remain byte-identical; only the new composition
session is retained. Authored verified Rust sources remain below 300 lines
(maximum 288). This layer is local on `codex/proof-truncated-producer-window`,
above `codex/proof-control-truncation`; these local checks do not claim remote CI.


## Snapshot-seeded producer replay

The physical-history composition does not describe snapshot recovery by itself.
A snapshot stores one last data batch per PID, including a batch below the log
or local floor. Recovery initializes that seed and replays only the uncovered
local tail; it does not reconstruct every earlier physical batch. Thus a retry
window can contain a seed whose bytes have been trimmed, and a surviving batch
covered by the snapshot need not appear in the window at all.

`loaded_snapshot_bounds_truncated_retry` composes whole-batch truncation, the
snapshot replay cursor, decoded seed admission, ring reconstruction, producer
classification and acknowledgement readiness. It derives the actual retained
end before admitting the loaded snapshot. This data-window theorem rejects
admission exactly when the end lies below a retained floor, the loaded offset is
outside the reload window,
or its supplied data seed is invalid or marker-only. An absent PID in a loaded
snapshot contributes no seed.

On admission, the projected history contains the seed exactly once and exactly
those local data rows whose last offsets reach the replay cursor and lie below
the actual end. This includes a whole first batch around an interior cursor.
Every replayed row retains its original source index; the seed has a distinct
origin. The latest contiguous epoch's last five rows define the retry window.
Duplicate classification, first-alias minimality, original coordinates and
exclusive acknowledgement are exported against that derived window. A seed
below either floor remains usable while it survives the window, but enough
replayed batches or an epoch transition can evict it. Every returned frontier
fits the actual end, and HWM clamping preserves readiness for that frontier.

`rebuilt_data_window_bounds_retry` shares the window construction with the
physical-history theorem, whose contract remains unchanged. It also admits
empty histories and epoch resets, following the real data-entry update path's
contiguous-epoch reset rule. The independent oracle computes physical cuts,
reload eligibility, seed/tail provenance, a five-row suffix and modular retry
classification. Real log/tracker regressions reopen explicit snapshots with
zero through seven tail batches, exclude an older covered batch from retry
metadata, and trim away the original producer segment
while a later surviving snapshot carries its retry below the local floor.

Snapshot identity and truthful coverage, complete decoded local rows, correct
request PID routing, a marker-free PID history and serialized replay remain
host obligations. Snapshot-file selection and corruption fallback are upstream
of this loaded-state boundary. Byte equality, presence of seed data below a
retained floor, transaction-marker epoch state, I/O and crash publication are
not established by these data-window proofs.


Six scoped controls qualify this composition at the positive proof's same
cache-free depth-four search. Removing only the exact physical end, exact
replay cursor, or first-alias minimality leaves the respective helper proof
and native oracle passing while rejecting the composition. Dropping the seed,
filtering an interior cursor by batch base instead of last offset, or returning
a merged-window position as the tail source index rejects both the composition
and independent oracle. All temporary edits were restored; property-test seeds
remain outside the repository.


Saved-artifact replay exposed a solver timeout in the no-duplicate branch's
existential alias check. The proof now states a checked per-row equivalence
between the projected seed/tail matcher and reconstructed row matching before
calling the shared window theorem. This changes no contract or runtime result;
it makes that provenance bridge explicit instead of relying on the solver to
find it inside the final quantified postcondition.


Final qualification used the pinned Creusot v0.13.0 image and a fresh target.
Cache-free generation and canonical saved-artifact replay both prove all 566
sessions. All 359 verified-crate tests and 35 native producer-state tests pass.
Bazel verified tests/documentation and the producer-state-filtered broker target
pass, as do strict workspace Clippy, all four proof/mutation configuration checks,
and repository formatting. The 1,126 unrelated parent artifact files remain
byte-identical; retained artifacts cover the two new sessions and the refactored
physical-history session with its unchanged contract. Authored verified Rust
sources remain below 300 lines (maximum 288). This layer is local on
`codex/proof-snapshot-tail-retry`; these checks do not establish remote CI.


## Deferred producer-completion order

The recovery proofs' complete, ordered histories do not establish the live
tracker's completion order. `PendingAck::finish` calls `AppendCommit::record`
after its independent HWM wait; the writer serialized the physical append,
but those callbacks can finish out of order. The old `ProducerState::commit`
protected only the epoch and transaction reopening. At the same epoch, a late
completion replaced the latest sequence, offset and timestamp and appended
retained metadata in completion order. A native counterexample first records
the batch ending at offset 21, then completes the older batch ending at 11:
the old tracker moves its latest offset back to 11. Ignoring every earlier
completion would preserve the newest batch but lose the earlier batch's retry.

The live tracker now delegates retained-window selection to
`producer_completion_window`. A lower producer epoch is rejected without
changing the entry. At the current epoch, distinct completed physical batches
are merged by last offset and the five greatest survive. A repeated physical
offset keeps the old metadata. A newer epoch starts its own window, including
the first data batch after marker-only state. Earlier completions can fill
retained slots while preserving the latest sequence, coordinates, timestamps
and current transaction state. In particular, repeating a data completion
after a same-epoch marker preserves that marker's closed state and timestamp.
The bounded Stateright tracker uses the same live projection; its independent
sequence oracle and reachable-state counts remain unchanged.

`completed_batches_preserve_first_retry` composes that actual selection with
snapshot-row validity, complete retry-window reconstruction, producer
classification and the acknowledgement frontier. It returns every retained
row's original source index, including the incoming completion's distinct
index. It exports epoch monotonicity, physical ordering, preservation of the
newest current-epoch batch after an earlier completion, exact duplicate
existence, the first matching alias, that alias's original base and exclusive
frontier, and readiness exactly when the HWM reaches that frontier. A rejected
completion against marker-only state keeps the marker's fence and zero-sequence
restart rule; it is not treated as a producer with no entry.

The independent oracle uses a sorted distinct-offset map and modular sequence
arithmetic. Native tracker tests enumerate all 720 orders of six completions,
then repeat every completion, including the evicted one, and compare each
retained batch's full metadata. Real partition-writer regressions append data
and COMMIT/ABORT markers at equal and higher epochs before completing earlier
data, compare the full live entry, and compare exact sequence decisions with
a tracker rebuilt from the reopened log.

These proofs cover completed truthful data batches and serialized tracker
installation. Equal offsets must identify the same surviving physical batch;
PID routing, assigned geometry, and faithful metadata projection remain host
obligations. They do not establish a complete physical window before all its
callbacks finish, fix the best-effort log-end/truncation race in
`AppendCommit::record`, reconstruct all transaction boundaries, prove timestamp
equality across marker snapshot recovery, or establish byte or crash durability.


Six scoped controls use fresh proof plans, no cached answers, and the positive
proof's same depth-six search. Removing only physical source ordering, retained
source epoch eligibility, or first-alias minimality leaves the respective
weakened helper provable and the native oracle passing, while rejecting the
composition. Returning a window position instead of its source index rejects
both the composition and oracle. Removing the five-batch crop rejects the
selector proof and oracle. Removing only the live preservation branch leaves
the pure proofs passing but rejects the broker regressions: installation is
covered by those real-host checks, not automatically certified by a theorem
over the projected window. All temporary mutations were restored, and negative
property seeds are retained outside the repository.

The selector now requires only at most five strictly ordered offsets and an
empty old window when no current epoch exists; its ordering law also covers
signed-offset and epoch boundaries. The composition separately establishes
valid nonnegative producer geometry for retry coordinates. A separate checked
insertion lemma connects inverse indices and the dropped prefix to maximal
retained offsets; a separate pointwise lemma supplies the strict order of any
two insertion positions. A checked slot witness constructs the retained position
of every old source outside the discarded prefix. The caller keeps that witness's
membership as a branch condition, so splitting an omitted-row conjunction cannot
discard the term needed to contradict omission. The remaining omitted old row is
explicitly tied to the discarded prefix before exporting the maximal-window
guarantee. Explicit new-epoch and unchanged-window branches preserve the same
selector contract while separating those cases from sorted insertion. These
proof-only changes retain every exported contract and the normal depth-six search.

The pinned Creusot v0.13.0 image compiles the frozen source from a fresh target
directory and proves all 571 sessions with ordinary cache-free generation. Full
canonical saved-ledger replay also proves all 571 sessions without cached answers.
Focused forced generation proves all five sessions added by this layer. Native
validation passes all 363 verified-crate tests and 38 producer-state tests, including the model,
all completion orders, and real marker/restart cases. Relevant Bazel tests/docs,
strict workspace Clippy, proof/mutation configuration checks and formatting pass.
The six scoped controls passed before the final proof-only decomposition; the
four mutated source files' positive bytes are unchanged. Later edits affect
only Creusot-only helper bodies and add the pointwise ordering and slot-witness
lemmas. The 1,132 parent artifact files remain byte-identical and all authored verified Rust files
remain below 300 lines (maximum 288). Local proof qualification is complete;
remote CI for the repaired head is tracked separately.

## Completed retry eviction and acknowledgement order

The completion selector exported maximal physical coverage, but
`completed_batches_preserve_first_retry` exported only ordering, origins and
retry coordinates. A caller could not establish why an eligible old physical
batch disappeared. The weaker interface permitted a one-row window containing
only a newer incoming batch, even when five old batches were eligible.

The composed contract now carries both maximality laws through the snapshot-row
projection: an omitted eligible old source, or an accepted incoming physical
offset absent from the window, requires five retained batches with strictly
greater last offsets. This strengthens the interface without changing selection,
retry classification or the existing preconditions.

`completed_eviction_bounds_waiters` consumes that contract and the actual
Produce-frontier and waiter kernels. It nominates an old current-epoch batch,
constructs every retained source's exclusive durability frontier, and exports
their readiness at a common HWM observation. If the nominated batch was evicted,
there are five strictly newer frontiers; every ready retained waiter implies that
the nominated frontier is already covered. Conversely, while the nominated
frontier remains uncovered, all five retained waiters are unready. The implication
is one-way: at HWM 3, an evicted batch ending at 2 is covered while the earliest
retained batch ending at 6 still waits for frontier 7.

Two contract-weakening controls separately hide old-source maximality and waiter
readiness. In both cases the weakened helper still proves and the native checks
pass, but the consumer proof fails. These controls distinguish the aggregate law
from a test that merely repeats the function's returned coordinates. All temporary
changes are restored before positive qualification.

This is a law about eligible completed physical batches and one HWM observation.
It excludes epoch replacement, incomplete callbacks, physical persistence and
transaction reconstruction. Eviction of one source does not exclude a Duplicate
decision for a different retained sequence alias, and HWM coverage does not prove
that evicted bytes remain readable.

Qualification uses pinned Creusot v0.13.0 compilation, forced cache-free
generation of both affected sessions, ordinary cache-free generation of all 572
sessions, and full cache-free saved-ledger replay of all 572 sessions. The final
verified-crate Bazel suite passes 368 native tests and the doc tests; strict
workspace Clippy, proof/mutation configuration checks and formatting also pass.
Both weakening controls use fresh depth-six plans and restore the same proof
bodies qualified by the positive runs. The 1,140 unaffected parent artifact files
remain byte-identical, and all authored verified Rust sources remain below 300
lines (maximum 288). Remote CI is tracked separately.
