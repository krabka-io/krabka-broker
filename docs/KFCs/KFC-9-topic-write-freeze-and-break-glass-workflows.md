# KFC-9: Topic Write-Freeze and Break-Glass Workflows

The broker refuses every new client write to a frozen topic, and needs two people to agree before a privileged operation, with no change to any Kafka request or response.

## Status

**Adopted.** The implementation lands on branch `claude/kfc-write-freeze-break-glass-pf6hum`. The metadata records and the five private wire messages land in a companion change on [`krabka-protocol`](https://github.com/krabka-io/krabka-protocol), which this repository pins by revision.

No KIP defines a topic write-freeze, and no KIP defines a two-person rule for a broker operation. Kafka's only way to stop a producer is the authorizer that KIP-11 defines. An ACL answers a question about a principal rather than a question about a topic. This document is the specification for both features.

The operations that the two-person rule gates keep their own KIPs exactly. KIP-460 defines the unclean election that `ElectLeaders` carries, KIP-455 defines the reassignment cancel, KIP-631 defines `UnregisterBroker`, and KIP-107 defines `DeleteRecords`. None of them gains a field here. KIP-108 defines `POLICY_VIOLATION`, the code that every refusal returns to a Kafka client.

Three earlier KFCs carry parts that this design reuses. [KFC-4](KFC-4-cross-topic-snapshots.md) established the krabka-private API key range, and the rule that the broker registers a private key for dispatch and never advertises it. [KFC-5](KFC-5-worm-archive-integrity-manifests.md) established the Ed25519 signing, the canonical byte encoding, and the domain separation constant that keeps one signature purpose apart from another. [KFC-8](KFC-8-clock-confidence-signal.md) measures the clock bound that the [signature skew window](#the-record-carries-a-signature-that-the-broker-cannot-make) assumes. A skew window is a clock assumption, and KFC-8 exists to measure the assumptions krabka already declares.

## Motivation

Incident response, migration, and disaster-recovery promotion all need one cluster state that krabka cannot express today. The cluster is up, every read works, and the broker must not accept a new write.

Operators build that state out of ACLs, and the result fails three ways.

An ACL edit is not atomic over a set of topics. The operator sends one change per topic, and a producer writes into the gap between the first change and the last.

An ACL denial reaches the producer as `TOPIC_AUTHORIZATION_FAILED`, which is the same answer a misconfigured principal gets. The producer's on-call engineer reads an authorization failure and looks for a credential fault that is not there. That costs incident time at the moment when incident time is most expensive.

Whoever can add a deny ACL can remove it. The freeze is then exactly as strong as one credential, and when the incident is a compromise, the attacker already holds that credential.

The same gap sits on the administrative side. A forced leader epoch bump, an unclean recovery, a broker unregistration, and a topic deletion are each one call from one credential. Each of them can lose committed data. A rule that needs a second person is standard practice for operations of that weight, and a Kafka broker has nowhere to keep one.

**A break-glass workflow needs two different people to agree before the broker does a privileged operation.** "Break-glass" is established security vocabulary rather than a description of a mechanism. This document treats the term the way it treats `WORM` and a `KIP-###` reference, and uses the exact phrase every time.

KFC-9 makes both states broker-understood. A write-freeze is a broker-owned registry entry that the produce path reads, and only a control plane with an approval can lift it. A break-glass workflow gates the privileged transitions. Every step of both reaches the signed and hash-chained audit log.

The broker is the right place for the freeze, for the reason [KFC-7](KFC-7-broker-side-schema-validation.md) gives about schema validation. The broker is the only place that every write passes through. A client-side stop protects the clients that run it, and those clients were never the problem. A `kafka-console-producer`, a connector, and a service written in another language all keep writing.

The broker is the right place for the two-person rule for a different reason. An approval that a separate service holds cannot be spent in the same step as the transition it authorizes. The broker's metadata log holds both records in one append. That is what stops an approval from being spent twice or lost in a crash.

This needs a KFC by the test in the [README](README.md). A stock Kafka client can tell the difference, and no KIP explains it. A produce to a frozen topic returns `POLICY_VIOLATION`, which no Apache Kafka broker returns from the produce path. `kafka-configs --describe` shows a `write.freeze` key that no Kafka broker has. `kafka-topics --delete` and `kafka-leader-election` can fail on a healthy cluster where the caller holds every right that Kafka asks for.

## Public Interfaces

The feature adds five API keys, seven krabka-private error codes, one synthesised topic config, three broker config sections, five metric families, one audit event, and one command. It reuses one existing Kafka error code. It adds no field to any Kafka request or response.

### The Freeze Registry

The registry is a set of entries in the metadata log. `DescribeTopicFreezes` returns the live entries, and each entry has this shape.

```text
freeze entry:
  scope         string      a topic name, or a topic-name prefix
  pattern_type  i8          3 literal, 4 prefixed
  reason        string      free text the operator supplied
  set_by        string      the principal that set the entry
  set_at_ms     i64         milliseconds since the Unix epoch
  proposal_id   uuid        the approval that authorized a thaw, nil on a freeze
  key_id        string      empty when the entry carries no signature
  signature     bytes       detached Ed25519 over the canonical bytes
```

`pattern_type` takes its values from Kafka's ACL pattern types, so a namespace freeze uses the vocabulary that an operator already knows from `kafka-acls`. A literal scope names one topic. A prefixed scope names every topic whose name starts with it.

A thaw is a record whose `frozen` flag is false, so `DescribeTopicFreezes` returns no entry for a thawed scope. The record removes the entry from the registry and stays in the metadata log. The log keeps the name of the person who lifted the freeze. This follows the sentinel pattern that a zero feature level and an empty topic-config map already use. It is also why there is no separate delete record.

### The API Keys

| Key | Request | Purpose | Authorization |
| :--- | :--- | :--- | :--- |
| 1015 | `SetTopicFreeze` | Freeze a scope, or thaw one with an approval. | `Alter` on `Cluster("kafka-cluster")` |
| 1016 | `DescribeTopicFreezes` | Read the registry. | `Describe` on `Cluster("kafka-cluster")` |
| 1017 | `ProposeBreakGlass` | Open a proposal. | `Alter` on `Cluster("kafka-cluster")` |
| 1018 | `ApproveBreakGlass` | Add one approval to a proposal, or withdraw it. | `Alter` on `Cluster("kafka-cluster")` |
| 1019 | `DescribeBreakGlass` | Read the proposals and their approvals. | `Describe` on `Cluster("kafka-cluster")` |

All five sit in the krabka-private range at 1000 and above. They fall between the barrier keys at 1010 to 1014 and the coordination keys at 1020 to 1022. Each one speaks version 0 only, with flexible framing. The broker registers them for dispatch and never advertises them, for the reason [KFC-4](KFC-4-cross-topic-snapshots.md#advertising-the-api-keys-in-apiversions) gives. A denied request returns `CLUSTER_AUTHORIZATION_FAILED` (31), which is what every other private key returns. A withdraw rides the approve key rather than taking a sixth key. It carries a `withdraw` flag on the approve request, which is the shape `AlterBarrierGroups` already uses for its `delete` flag. Approve and withdraw both name a proposal that exists and both act on it, so they share a request that already carries a proposal id. A propose names no proposal, because a propose is what creates one.

### The Refusal Code

A write that a freeze refuses gets `POLICY_VIOLATION` (44), and so does a gated transition that has no approval.

| Code | JVM exception | Retriable | What the producer does |
| :--- | :--- | :--- | :--- |
| 44 `POLICY_VIOLATION` | `PolicyViolationException` | No | Fails the batch, calls back with the broker's `error_message`, and does not re-enqueue. |
| 29 `TOPIC_AUTHORIZATION_FAILED` | `TopicAuthorizationException` | No | Fails the batch, and caches the topic as unauthorized in its metadata. |
| 17 `INVALID_TOPIC_EXCEPTION` | `InvalidTopicException` | No | Fails the batch, and caches the topic as permanently invalid in its metadata. |

KFC-1 and KFC-7 each reused an existing Kafka code, and both gave the same reason. `Errors.forCode` maps an unassigned value to `UNKNOWN_SERVER_ERROR`, and a client then classifies the outcome wrongly for retries. Code 44 is an existing Kafka code with an existing JVM mapping, so this is the same act rather than a new one. It is absent from krabka today only because no krabka path had a policy to violate. Apache Kafka returns 44 from `CreateTopicPolicy` and `AlterConfigPolicy`, and a broker-owned write policy is the same category of rule. The response carries an `error_message` that names the freeze and the scope that matched, and Produce v8 and later carry that field.

### The Private Error Codes

These codes ride the five private keys only, so none of them reaches a JVM client. They start at 1006, which leaves KFC-6's proposed 1001 to 1005 alone.

| Code | Name | Meaning |
| :--- | :--- | :--- |
| 1006 | `BREAK_GLASS_APPROVAL_REQUIRED` | The action needs an approved proposal, and the caller named none. |
| 1007 | `BREAK_GLASS_DUPLICATE_APPROVER` | The principal already approved this proposal. |
| 1008 | `BREAK_GLASS_NOT_AN_APPROVER` | The principal is not in the configured approver set. |
| 1009 | `OPERATOR_SIGNATURE_INVALID` | The signature did not verify against the trusted key set. |
| 1010 | `OPERATOR_SIGNATURE_REQUIRED` | The action needs a signature, and the caller sent none. |
| 1011 | `FREEZE_SCOPE_INVALID` | The scope is empty, or it resolves to an internal topic. |
| 1012 | `FREEZE_LIMIT_EXCEEDED` | The registry already holds `freeze.max_entries` entries. |

The freeze path and the break-glass path share the two signature codes, because both verify against one trusted key set under one set of rules.

Code 1009 covers six separate failures. A bad signature and an unknown `key_id` are two of them. A name that does not match the key's bound principal, and a name that does not match the connection's authenticated principal, are two more. A timestamp outside the skew window and a replayed timestamp are the last two. The response message says which one failed, and the code does not. An error code that separates them tells an attacker which check they got past.

The krabka-private error-code range and the krabka-private API key range are separate namespaces that share the floor 1000. Code 1010 is a legal error code here and api key 1010 is `AlterBarrierGroups`, and the two numbers mean nothing to each other.

### `write.freeze` Is a Read-Only Topic Config

`DescribeConfigs` reports a synthesised `write.freeze` key for every topic, with the read-only flag set. An operator who holds only the JVM tools sees the freeze through `kafka-configs --describe`.

The value on a frozen topic names the scope that freezes it, in the form `frozen:<pattern>:<scope>`. A topic frozen by its own name reads `frozen:literal:orders`, and a topic frozen by a namespace reads `frozen:prefixed:tenant-a.`. The value on every other topic is `false`.

The value names the scope because a `true` does not tell an operator what to do next. A topic can be frozen by its own name or by a prefix that covers a thousand other topics, and the thaw is a different command in each case. The scope in the value is the one piece of information that a reader of `kafka-configs --describe` cannot get any other way, because the JVM tools cannot call `DescribeTopicFreezes`.

The key is reported for an unfrozen topic as well, rather than left out. An absent key cannot be told apart from a broker that does not have the feature, and an operator who checks a freeze during an incident needs that difference. An internal topic is never freezable, so it reads `false`.

`AlterConfigs` and `IncrementalAlterConfigs` refuse the key by name with `INVALID_CONFIG` (40), and the message names `krabka-guard` as the tool that sets it. The key is never stored as a topic config, and it never reaches the topic-config record in the metadata log. This reuses the pattern that the controller-managed broker configs already follow.

There is no second internal topic. `__barrier_state` exists because a barrier cut has no other read path. Here the audit topic carries the history as OCSF JSON, and `DescribeConfigs` carries the current state. The key is never stored, so no snapshot and no restore can resurrect a stale freeze from a topic config.

### Broker Configuration

```toml
[[operator_keys]]                            # shared by freeze signatures and break-glass approvals
key_id = "alice-yubi"
principal = "User:alice"
public_key_path = "/etc/krabka/operator-keys/alice.pub"

[freeze]
max_entries = 1000
require_signature = false
signature_max_skew = "5m"

[break_glass]
approvers = ["User:alice", "User:bob", "User:carol"]
required_approvals = 2
proposal_ttl = "30m"
signed_actions = ["unclean_elect_leaders", "unclean_recovery", "delete_topic"]
background_unclean_recovery = "audit-only"
```

| Key | Default | Meaning |
| :--- | :--- | :--- |
| `operator_keys` | empty | The trusted key set. Each entry binds a `key_id` to a principal and a public key file. |
| `freeze.max_entries` | 1000 | Largest number of live registry entries. |
| `freeze.require_signature` | `false` | `true` makes a signature needed on a freeze as well as on a thaw. |
| `freeze.signature_max_skew` | `5m` | How far a signed timestamp may sit from the controller's clock. |
| `break_glass.approvers` | empty | The principals that may approve a proposal. |
| `break_glass.required_approvals` | 2 | Distinct approvals a proposal needs. The broker refuses a value below 2. |
| `break_glass.proposal_ttl` | `30m` | How long a proposal stays open. |
| `break_glass.signed_actions` | the three irreversible actions | Actions whose approvals need a signature. |
| `break_glass.background_unclean_recovery` | `audit-only` | `off`, `audit-only`, or `require`. See [the background path](#the-background-recovery-path-has-no-caller-to-refuse). |

`[[operator_keys]]` is a top-level section rather than a nested one, because two subsystems verify against it. A duration takes any unit that `krabka_units` accepts, and a bare number is refused.

The broker refuses three key-set conditions when it loads the file. Each one is refused because the silent reading of it is worse than a refusal. A key file that the broker cannot read is refused. A key file that does not hold an Ed25519 public key is refused. A duplicate `key_id` is refused. A `signed_actions` entry with no configured key is a startup error too, because the alternative is an action that quietly stops needing a signature.

### Metrics

| Family | Labels | Meaning |
| :--- | :--- | :--- |
| `topic_freeze_rejections` | topic | Produce partitions refused because their topic is frozen. |
| `topic_freezes_active` | none | Live registry entries. |
| `break_glass_proposals` | state | Proposals counted as pending, approved, expired, or consumed. |
| `break_glass_refusals` | action | Gated transitions refused for want of an approval. |
| `break_glass_bypassed` | action | Privileged transitions the broker did with no approval. |

Every label set is bounded. `break_glass_bypassed` is the one an operator should alert on, because it counts the data-losing transitions that no person approved.

### The Command

`krabka-guard` is one command for one incident. The crate is a library as well as a binary, for the reason `krabka-barrier` and `krabka-format` are. A test that spawns a binary needs a Cargo working tree, and a Bazel test sandbox has none. So the tests call `run_from_args` in process. `--bootstrap-server` is required and also reads `KRABKA_BOOTSTRAP_SERVER`, which is what `krabka-barrier` does.

```
krabka-guard freeze set --topic orders --reason "DR cutover" [--sign-with key.pk8 --key-id alice-yubi]
krabka-guard freeze set --prefix tenant-a. --reason "tenant offboarding"
krabka-guard freeze list [--verify-signatures]
krabka-guard freeze clear --topic orders --proposal <uuid> --sign-with key.pk8 --key-id alice-yubi
krabka-guard break-glass propose  --action delete-topic --target doomed --reason "..." --ttl 30m
krabka-guard break-glass approve  --proposal <uuid> [--sign-with key.pk8 --key-id alice-yubi]
krabka-guard break-glass withdraw --proposal <uuid>
krabka-guard break-glass list [--pending]
```

`--sign-with` takes a PKCS#8 key file and never sends it. The command builds the canonical bytes on the operator's own machine, signs them there, and puts only the `key_id` and the signature on the wire. The private key never reaches a broker.

`freeze list --verify-signatures` re-verifies each returned entry against the local public keys and exits 5 if any entry fails. That makes the operator's own machine the thing that says the registry is authentic, rather than the broker that served it.

| Exit code | Meaning |
| :--- | :--- |
| 0 | The broker accepted the request. |
| 1 | The broker refused the request. |
| 2 | A transport failure. Nothing is known about the outcome. |
| 3 | The tool looked, and what it found does not match what it was told. |
| 4 | The action needs an approval that does not exist. |
| 5 | A signature did not verify. |

Codes 0 to 3 keep the meanings that `krabka-barrier` gives them, code 3 included.

Code 3 is reserved rather than reused. `krabka-barrier` already exits 3 for a cut that does not match the log, and an operator runbook branches on `$?` without knowing which krabka tool answered it. One number has to mean one thing across both tools, so the two codes this feature adds take 4 and 5 rather than displacing it.

Code 4 is the one a runbook branches on: it says the cluster is healthy and the caller is authorized, and the operation still needs a second person. Code 5 keeps "the tool could not check" apart from "the tool checked and the answer is wrong". KFC-5's verifier draws the same distinction.

### The Audit Event

One new audit event covers both features. It names the outcome, the phase, the action, the target, and the proposal id. It also names the acting principal, the other people who approved, and a fingerprint of the approver set. The `key_id`, the raw signature, the verification result, and the timestamp that signature covers sit beside them. That last field is carried separately from the event's own time, because the event's time is the instant the broker emitted the record and the signed preimage holds the instant the operator signed: an auditor rebuilding those bytes needs the second one. The phase takes one of six values: proposed, approved, consumed, applied, refused, and bypassed. One variant covers both features, because a freeze is not a break-glass act and it carries the same evidence.

The event carries the raw signature and not only a verified flag. The audit log is hash-chained, and its checkpoints carry an Ed25519 signature. An auditor who trusts that chain and holds the operator public keys can re-verify who set every freeze from the audit topic alone. That check needs no broker and no metadata log. Every field the freeze preimage covers comes out of the event except the cluster id, which is a property of the cluster whose log the auditor is holding. That is a second copy of the proof, independent of the first.

The event's class is `ApiActivity`, which is the class the ordinary administrative event already uses. The spool frame tag, the spool reader, and `krabka-audit verify` are unchanged, and only the OCSF body mapping is new.

## Proposed Changes

### The Freeze Lives in the Metadata Log, and Not in Topic Config

A freeze is a broker-owned registry in the metadata log, with topic scope and prefix scope. It is not a topic config, and the [Rejected Alternatives](#the-writefreeze-topic-config-as-the-storage) section says why.

The metadata log is the right home for three reasons. Every broker holds the image, so the produce path reads the freeze from memory with no round trip. The controller writes it through the same path that writes every other cluster fact, so a freeze survives a restart and a leader change. And a thaw consumes a break-glass approval in the same raft append that removes the entry. [The proposal section](#the-proposal-lives-in-the-metadata-log-because-the-consume-must-be-atomic) rests on that property.

The image validator does not need the scope to name an existing topic. A prefix scope names no topic at all. An operator who freezes a namespace before a restore writes into it has the disaster-recovery case that this feature exists for. The snapshot writer emits every live entry, because a snapshot that dropped the registry would thaw a cluster silently.

### A Freeze Refuses New Data, and Never Stops Work the Cluster Already Accepted

> **The rule.** A freeze refuses every append that puts new client-authored data into a frozen topic's log, and every operation that removes data from it. It does not refuse an append that only completes work the cluster already accepted, and it never refuses replication.

Two invariants follow, and a break in either one is a bug. A freeze never stops an open transaction from completing. A freeze never stops a follower.

| Path | On a frozen topic | Why |
| :--- | :--- | :--- |
| Produce, plain or transactional | Refused | This is the feature. A producer inside a transaction has to abort, because it cannot add more. |
| `AddPartitionsToTxn` naming a frozen topic | Refused | The cheapest place to stop a transaction from ever reaching the topic. |
| `EndTxn` fan-out, `WriteTxnMarkers`, abort markers | Allowed | The decision is already durable in `__transaction_state`. |
| `OffsetCommit`, `OffsetDelete`, `TxnOffsetCommit` | Allowed | They append to `__consumer_offsets`, and a cutover needs reader positions recorded. |
| Follower replication | Allowed, always | Replication is what makes the frozen prefix durable. |
| Barrier marker injection | Allowed | The broker authors it, it carries no application data, and a freeze is when an operator wants a cut. |
| Compaction | Refused | Compaction removes records, and a promotion needs the frozen prefix byte-identical between sites. |
| Retention eviction | Refused by the rule, with no gate to write | See below. |
| Tiering copy | Allowed | A copy adds a replica. The local eviction that follows removes bytes, so the broker refuses that. |
| `DeleteRecords` and `DeleteTopics` | Refused | A break-glass approval for either one does not defeat a freeze. |

The transaction marker rule is the one that most needs its reason stated. Refusing a marker would leave a permanently open transaction, which pins the last stable offset and stops every `read_committed` consumer of the partition. The freeze would then break reads, and a frozen topic that cannot be read is not the state this feature offers.

That gives the rule its honest limit. **A freeze does not roll back a transaction that is already in flight.** A producer that enlisted a partition before the freeze can still commit what it already wrote. The log grows by those records after the freeze lands. The freeze stops the next `AddPartitionsToTxn` and the next produce, and it lets the open work finish. An operator who needs the log to stop at an exact offset should take a barrier cut, which is what [KFC-4](KFC-4-cross-topic-snapshots.md) is for.

The retention row is a rule with nothing to enforce it, and this note is here because nothing else in the repository reports it. **Nothing in this workspace calls the log tick outside tests**, so no code path applies time retention or size retention to a live partition today. The rule is stated so that a sweeper written later gates on the freeze. A gate written into a path that nothing calls would be dead code that reads like a guarantee.

### Internal Topics Are Never Freezable

The handler refuses a scope that resolves to a name starting with `__`, and the resolver treats such a name as no match. A prefix scope of the empty string would otherwise freeze `__consumer_offsets` and take the cluster down.

The `__` convention is the test, rather than the three-name internal-topic list the broker carries elsewhere. That list is already stale, and a new internal topic would be freezable the day it lands.

### The Produce Gate Runs Before the Broker Parses the Batch

The gate resolves once per topic per produce request, beside the schema-validation gate, and passes one boolean down to each partition. A partition of an unfrozen topic pays one boolean test and nothing else.

The refusal itself sits with the topic ACL denial, which is earlier than the point where KFC-1 and KFC-7 put their checks. That position is a decision rather than an accident. A freeze is an authority gate and not a content gate, so it ranks with the ACL denial. A frozen topic must not pay CRC verification and decompression for a batch that the broker will never accept.

The position has a second consequence that the test suite asserts. The gate returns before the idempotent-sequence gate, so the producer state is untouched and the log end offset does not move. A refusal that still appended would be the worst failure this feature can have, and the error code alone does not rule it out.

### A Literal Scope Beats Every Prefix, and the Longest Prefix Wins

The image holds two indexes: a hash map of literal scopes, and a sorted map of prefix scopes. A resolve on a cluster with no freeze is two emptiness tests. A resolve on a frozen cluster reads the literal map first. It then walks the sorted map backwards, and returns the longest prefix that matches.

This document states the precedence, and does not leave it to the data structure. A literal entry beats every prefix entry, so an operator can thaw one topic out of a frozen namespace by naming it. The longest matching prefix wins, so a narrower namespace rule beats a wider one.

The reverse walk is unbounded in the worst case, which is why `freeze.max_entries` bounds the registry and the handler enforces it. The sorted map is a deliberate choice over the flat list that prefixed ACLs use. This lookup is one hop from the produce path, and an ACL lookup is not.

### The Record Carries a Signature That the Broker Cannot Make

The name of the person who set a freeze is the broker's word for it. That is not good enough for the one record whose whole job is to say that a privileged person did a privileged thing. Anyone who can write the metadata log can write any name into that field. An auditor who reads the log a month later then has to trust the broker that minted the record.

So the record carries a detached Ed25519 signature made by the operator's own key. The operator's machine makes it before the request leaves. The broker verifies it before it accepts the record. The metadata log stores it, so it stays verifiable afterwards with no trust in any broker.

```text
FREEZE_DOMAIN = "krabka-topic-freeze-v1\0"
  cluster_id     length-prefixed
  pattern_type   u8
  scope          length-prefixed
  frozen         u8
  reason         length-prefixed
  set_by         length-prefixed
  set_at_ms      i64 big-endian
  proposal_id    16 bytes
```

The signature covers every field that changes the meaning of the record. Three of those fields answer a named attack.

The `frozen` flag is signed, so a captured signature for a freeze cannot be replayed as a thaw. Without it the two records differ by one byte and one signature would authorize both. The cluster id is signed, so a signed freeze cannot be replayed into a second cluster. The timestamp is inside the signed bytes, and the broker checks it two ways. It must sit inside `freeze.signature_max_skew` of the controller's clock, and it must be newer than the timestamp of the entry it replaces. Those two checks are what kill the replay of an old signed thaw, and the first of them is why this design cross-links [KFC-8](KFC-8-clock-confidence-signal.md).

The trust set is one shared set of operator keys, and each entry binds a `key_id` to a principal. That binding is what closes the loop. The broker checks the name in the record against the principal bound to the key. It checks the same name against the authenticated principal on the connection. A signature from Alice's key that claims Bob set the freeze is refused, and so is one presented over Bob's authenticated session.

`FREEZE_DOMAIN` is its own domain separation constant. It differs from the break-glass domain, and from the three that krabka already defines for the [WORM manifests](KFC-5-worm-archive-integrity-manifests.md#the-signature-covers-the-head-and-sits-beside-the-body) and the audit checkpoints. A domain shared across two signature purposes is the classic way a correct primitive becomes an incorrect protocol. One test asserts that all five domains in the workspace differ pairwise.

### A Freeze Takes One Command, and a Thaw Takes Two People

The broker needs a signature on a thaw. It does not need one on a freeze, and `freeze.require_signature = true` makes it need one on both.

The asymmetry is deliberate. A freeze is the safe direction. An operator has to reach it in one command during an incident, on a cluster where nobody installed key material yet. A thaw is the dangerous direction, and it already needs an approved break-glass proposal whose approvals carry their own signatures.

This leaves an honest gap that a reader should not have to infer. **An unsigned freeze is an attestation and not a proof.** The broker's word is all that stands behind it. So a registry can hold a mix of proved entries and attested ones. `freeze list --verify-signatures` is what tells the two apart, and `require_signature = true` is what removes the mixture. An operator who needs every entry proved should set it, and should install the operator keys before an incident rather than during one.

### An Approved Proposal Is a Standing Authorization, and Not a Request Field

This is the decision that keeps the Kafka wire format intact. An approved proposal authorizes one action on one target for a bounded window. It is not a parameter on the request that it gates.

So no Kafka request gains a field. An operator gets an approval out of band through the private API, then runs the ordinary JVM tool. The broker consults its own metadata image when the request arrives. `kafka-leader-election.sh` sends the stock RPC that KIP-460 defines, and there is nowhere to put a proposal id in it even if this design wanted one.

| Transition | Where the proposal comes from | How the broker refuses |
| :--- | :--- | :--- |
| Thaw a freeze | An explicit proposal id on the private request | `BREAK_GLASS_APPROVAL_REQUIRED` (1006) |
| Unclean `ElectLeaders` | Out of band. Preferred election is not gated. | 44 per partition |
| `UnregisterBroker` | Out of band, targeted by broker id | 44 at the top level |
| Reassignment cancel | Out of band. A completion is not a cancel and is not gated. | 44 per row |
| `DeleteTopics` | Out of band, targeted by topic | 44 per topic |
| `DeleteRecords` | Out of band, targeted by partition | 44 per partition |

The thaw is the one transition that names its proposal explicitly, because the request is krabka's own and it can carry the field.

### The Proposal Lives in the Metadata Log Because the Consume Must Be Atomic

A proposal is a metadata record with an expiry, and a sweeper removes it the way the delegation-token sweeper already removes an expired token.

KFC-4 put its cuts in an internal topic so that clients in other languages could read them. That reason does not apply here, because nothing outside the operator's own command reads a pending proposal.

The reason that decides it is atomicity. **The consume of an approval must be atomic with the transition it authorizes.** Each gated handler prepends the consumed proposal record to its own records and submits one raft append. A proposal held in an internal topic cannot be consumed in the same append as a metadata change. A crash between the two then either spends the approval twice or loses it.

An approval held in a separate service has the same fault and one more. Every gated transition already goes through the controller. An approval that lives anywhere else can be lost when a node stops being the controller leader.

### The Approver Set Comes From the Broker Configuration File

`break_glass.approvers` is a `broker.toml` value and not a record in the metadata log. An attacker who can write the metadata log has already won. The approver set has to come from a channel that the metadata log does not control. This is the same reasoning that keeps the super-user list out of the ACL store.

An ACL resource type is not available for it either. The resource type is Kafka's own wire enum, and krabka cannot claim a slot in it without risking a future Kafka assignment.

That choice carries a cost, and it is the same cost the super-user list already carries. **The approver set depends on every broker holding an identical `broker.toml`.** Two brokers that disagree accept different approvals, and nothing in the cluster reconciles them. The audit event records a fingerprint of the sorted approver list for exactly this reason. A divergence is visible after the fact, even though nothing prevents it.

### The Broker Reads the Approver Set When a Person Approves, and Not When It Acts

The broker checks the set at approval time. It does not check it again when the approval is spent.

A second check at consumption time would make the consume non-deterministic across brokers. The set is a per-node file value, and two nodes can disagree during a rolling config change. Two brokers have to reach the same answer about whether a record is valid, and a per-node value cannot decide that.

The consequence for an operator is the right one as well. An operator who removes a person stops that person from making new approvals. The removal does not silently invalidate an incident response that is already under way. The safety bound is the time to live. Wait out `proposal_ttl` and every pending approval by that principal is dead.

Three checks run when a person approves, and the broker enforces all three. The approver must be in the configured set. The approver must not be the proposer. The approver must not already appear in the approval list. Those checks are what make the rule a two-person rule rather than a two-click rule.

### Two Concurrent Approvals Need a Rule in the Image Validator

Two approvers can read the same image and each submit a proposal record that adds one approval. The loser overwrites the winner, and the proposal never reaches two approvals, so a third approval consumes it as if it were the second.

The image validator gets a real rule for the proposal record to stop that. It refuses a record whose approval list is not a strict extension of the stored list. It also refuses any change to a proposal that is already consumed or withdrawn.

This is worth stating because it is easy to miss. Almost every arm of that validator accepts unconditionally today, and a reader who copies the neighbouring arm writes the race back in.

### The Background Recovery Path Has No Caller to Refuse

Unclean recovery also runs from leader election and from a broker heartbeat, with no request context and nobody to send a refusal to. A two-person rule cannot exist on that path. This document says so plainly, because a silent gap would read as a guarantee that the code does not give.

The recovery job carries the proposal that authorized it when a person asked for it, and carries none when the controller started it. A three-valued setting decides what the broker does in the second case.

- `off` keeps today's behaviour, with no audit event and no counter.
- `audit-only` is the default. Recovery runs, and the broker emits an audit event with the bypassed phase that names the partition and the strategy, and increments `break_glass_bypassed`. An operator can prove after the fact that a data-losing election happened with no approval.
- `require` fails closed. The partition stays leaderless and visibly offline, and the broker audits the refusal.

The split default holds in one sentence. An operator who types an unclean election can be asked for a second signature, and a controller that reacts to a dead broker at 03:00 cannot.

### Every Step Reaches the Audit Log, and Names Both People

A proposal, each approval, the consume, the transition, and every refusal each produce one audit event. Every gated handler also emits its ordinary administrative success event with the proposal id attached, so the approval and the transition join on that id.

The existing administrative event carries one principal and no key material. The whole value of a two-person rule is that the record names both people. The whole value of a signature is that the record keeps it. A join across rows on a resource name reconstructs neither one, so this design adds an event that carries both.

One audit gap that already exists stays open here. The authentication event and two lifecycle events are defined and never emitted. This feature does not change that, and this document does not imply that it does.

### A Signature Proves Who Wrote the Record, and Not What the Broker Did

This limit belongs in the middle of the document rather than in a footnote. A reader can easily take the signature for more than it is.

An auditor with the operator public keys can prove that Alice authored the record that froze `orders`. The proof works from the metadata log or from the audit topic, with no broker in the loop. That is authorship, and it is the property the signature was added for.

It is not enforcement. A proof that every broker then refused every write to `orders` is a different kind of evidence. The [produce-path tier](#test-plan) of the test suite is what supplies it. [KFC-3](KFC-3-point-in-time-restore.md#what-verification-does-not-prove) draws the same line about a CRC: a CRC proves that bytes did not rot, and it proves nothing about who wrote them.

## Compatibility, Deprecation, and Migration Plan

No Kafka wire format changes. No Kafka API key changes, and no Kafka request or response shape is touched. The five new keys sit in the krabka-private range at 1000 and above. The broker registers them for dispatch and never advertises them, so `kafka-broker-api-versions.sh` prints no row for them. The seven new error codes ride those keys only, so none of them reaches a JVM client.

What a stock Kafka client observes on a cluster that uses this feature is the contract this document adds.

- **A produce to a frozen topic returns `POLICY_VIOLATION` (44).** The client fails the batch, does not retry, and calls back with the broker's message. Its metadata cache is unchanged, so the topic stays known and readable.
- **`kafka-configs --describe` shows a `write.freeze` key.** It is read-only, and an alter of it fails with `INVALID_CONFIG` (40) and a message that names the tool which sets it.
- **Five administrative calls can fail on a healthy cluster.** Unclean `ElectLeaders`, `UnregisterBroker`, a reassignment cancel, `DeleteTopics`, and `DeleteRecords` return 44 when no approved proposal covers them. The caller holds every right Kafka asks for, and the broker refuses anyway.
- **Reads are unchanged.** Fetch, `ListOffsets`, `Metadata`, and offset commits behave on a frozen topic exactly as they do on any other.
- **The isolation level changes nothing.** A freeze allows transaction markers, so a `read_committed` consumer of a frozen topic is not held behind an open transaction.
- **Preferred leader election is unchanged.** Only the unclean election type is gated.

An operator has to do nothing on a cluster that does not use the feature. The registry starts empty, and an empty registry costs the produce path two emptiness tests per topic. The gate on the five administrative transitions is active only when `[break_glass]` names an approver set. A cluster with no such section behaves exactly as it does today. This follows KFC-4's rule that a feature which nothing uses should cost nothing.

An operator who turns the two-person rule on has to install the operator keys and the approver set on every broker before the first proposal. A `signed_actions` entry with no matching key stops the broker at startup, so a half-configured cluster fails loudly rather than accepting an unsigned approval.

One existing consumer of krabka's own output sees a change. The audit topic carries a new event body, and a reader that switches on the OCSF class sees the class it already handles. The spool format, the spool reader, and `krabka-audit verify` are unchanged.

krabka is greenfield and undeployed, so there is no migration and no compatibility shim. There is nothing to deprecate.

## Test Plan

Six tiers cover the feature, and the load-bearing claim is that a refusal refuses. Every rejection case asserts the error code, and asserts that the partition's log end offset did not move. A refusal that still appended is the worst failure this design can have, and the code alone does not rule it out.

**Unit, in `krabka-protocol`.** The two new record kinds round-trip beside the existing ones. The image tests cover precedence, removal, the internal-topic exemption, and a snapshot that preserves the registry. The validator tests cover an approval list that does not extend the stored one, and any edit to a consumed or withdrawn proposal. The metadata translation tests carry each new record across to the Kafka metadata log and back unchanged.

**Unit, the resolver and the gate.** Table-driven cases over the empty-registry path, literal precedence, longest-prefix precedence, and the internal-topic exemption. The gate cases cover self-approval, a duplicate approver, a non-approver, expiry, an unsigned approval for an action that needs one, and a proposal that is already consumed. A model-checked state model covers every interleaving of approve, withdraw, expire, and consume. No interleaving consumes a proposal twice, and none consumes one with too few distinct principals.

**Unit, the signature layer.** Both signing modules reproduce their documented byte layout by hand, so the layout is pinned by a test rather than only by the code that writes it. One shared test asserts that all five domain separation constants in the workspace differ pairwise.

Then the attack table, one case per row. A good signature verifies. A signature captured from a freeze is refused against the otherwise identical thaw. A signature made for another cluster is refused, and so is an unknown key id.

A name that is not the key's bound principal is refused, and so is a name that is not the connection's authenticated principal. A timestamp outside the skew window is refused, and so is a replayed one. A one-bit flip anywhere in the canonical bytes is refused. Every refusal asserts `OPERATOR_SIGNATURE_INVALID` and asserts that the response does not say which check failed.

**Integration, in process against a live broker.** The freeze suite runs an unfrozen control topic beside every case, in the shape KFC-1's suite established. A result is then shown to be the configuration, and not a path that every topic now takes. It covers a literal freeze, a prefix freeze, and a topic created after a covering prefix freeze. It covers a thaw that restores writes, and a config alter that can neither set nor clear a freeze.

It also covers a transaction that enlisted the partition before the freeze and still commits. Signature cases run end to end through the wire, and one of them proves the durability claim: a signed freeze survives a controller restart and still verifies from the reloaded image.

The break-glass suite covers each gated transition with and without an approval, the three distinct-principal refusals, expiry, and a failover on a three-node cluster. It also proves that the approval and the transition land in one raft append.

**Audit.** Each step produces a chained record, the chain still verifies through `krabka-audit verify`, and the proposal id joins the approve event to the transition event. One case does the thing that nothing else proves. It reads a signed freeze's event back off the audit topic, and re-verifies the signature against the operator public key. It never touches the metadata image. A metrics case asserts that the counters move on a real request, which is the gap KFC-7's suite found late.

**JVM acceptance, tagged `manual`.** `kafka-console-producer` against a frozen topic gets a `PolicyViolationException` and exits non-zero. `kafka-topics --delete` with no proposal fails and carries the broker's message. `kafka-leader-election --election-type unclean` fails without a proposal and succeeds with one. `kafka-configs --alter` cannot set a freeze key.

## Rejected Alternatives

### The `write.freeze` Topic Config as the Storage

The freeze could be an ordinary topic config, set through `AlterConfigs`. Every JVM tool, every Terraform module, and every runbook that already knows `kafka-configs` would work unchanged. The broker would need no new API key and no new metadata record.

It loses on who can set it: anyone who holds `Alter` on the topic can set a topic config, and that is the producing team. A freeze has to hold against that team. A topic config also has no namespace scope. An operator who freezes a tenant would send one alter per topic, which is exactly the non-atomic edit that this feature replaces. A config carries no reason, no author, no timestamp, and nowhere to put a signature. The record that has to say who did this would say nothing.

This design keeps the tooling compatibility where it costs nothing. `DescribeConfigs` reports the freeze as a read-only key, so `kafka-configs --describe` shows it, and the alter path refuses it by name.

### A Deny ACL

This is what operators do today. An `AclBinding` that denies `Write` on the topic stops the producer, needs no new code at all, and every operator already knows how to write one.

It loses on all three counts the [Motivation](#motivation) names. The edit is not atomic over a topic set. The denial reaches the producer as an authorization failure, which sends its on-call engineer after a credential fault that does not exist. And the credential that adds the deny removes it. The freeze is exactly as strong as one credential, during an incident where that credential may be the problem.

A deny ACL is also a statement about a principal. A freeze is a statement about a topic, and it holds against every principal, including one that nobody has issued yet.

### A New Error Code for a Frozen Topic

A krabka-private code would let a client tell a freeze from any other policy refusal without reading a message, and it would name the condition exactly.

It loses on what an unknown code does at the client, which is where KFC-1 and KFC-7 each lost the same argument. `Errors.forCode` maps an unassigned value to `UNKNOWN_SERVER_ERROR`, and clients then disagree about whether to retry it. `POLICY_VIOLATION` (44) is an existing Kafka code with an existing JVM exception and an existing non-retriable classification. The `error_message` carries the detail that a new code would have carried.

The private codes in this design are not an exception to that rule. They ride krabka-private API keys only, where the caller is `krabka-guard` and no JVM client can reach them.

### Signing With the Broker's Key

The broker holds an Ed25519 key already for the audit checkpoints, and KFC-5's manifests sign with a broker key. Reuse would need no operator key file, no `--sign-with` flag, and no key material installed before an incident.

It loses on what the signature has to survive. A broker signature proves that a broker wrote the record, and the metadata log already establishes that. The attacker this record exists to survive is one who holds the broker, so a proof the broker can make is a proof that attacker can make. The signature has to be made somewhere the broker cannot reach, which means the operator's own machine and the operator's own key.

The broker key stays right for a manifest, because a manifest attests to what the broker itself observed while it copied a segment. A freeze attests to a decision that a person made.

### A Materialised Set of Frozen Topic Names in the Image

The image could resolve every prefix scope against every topic at apply time and keep a flat set of frozen topic names. The produce path would then do one hash lookup with no prefix walk at all.

It loses on what it costs to build. The image would have to rescan every topic record against every prefix entry. That is the quadratic rescan which the apply path deliberately removed to stay constant-time per record. A rescan put back for this feature would slow every metadata apply on every cluster, including the clusters that never freeze anything.

The sorted prefix map gets the produce path close enough. A cluster with no freeze pays two emptiness tests, and a cluster with freezes pays a bounded walk over a registry that `freeze.max_entries` caps.

### Blocking Transaction Markers

A freeze that refused transaction markers too would be simpler to state. Nothing at all is appended to a frozen topic, and the frozen prefix is fixed the instant the freeze lands.

It loses on what it does to readers. The commit or abort decision is already durable in `__transaction_state` when the marker is written, so refusing the marker does not undo the transaction. It leaves the transaction permanently open, which pins the last stable offset and stops every `read_committed` consumer of that partition. A freeze exists to keep a topic readable while it is not writable, and this alternative breaks the half the feature was meant to keep.

That allowance carries the honest limit stated above: a transaction already in flight can still add records after the freeze. That is a smaller and a recoverable cost.

### A Signature on Every Freeze

`require_signature` could be gone, and every freeze record could carry a signature. The registry would then hold one kind of entry. An auditor would never have to ask which entries are proved, and this document would not have to explain a mixture.

It loses on the incident it is for. A freeze is the safe direction. The first freeze often happens on a cluster where nobody installed operator keys, at the moment when nobody has time to install them. A rule that made the safe direction need key material would push an operator back to the deny ACL, which is the thing this feature replaces.

The mixture is the price, and the design pays for it in two ways. `freeze list --verify-signatures` separates a proved entry from an attested one, and `require_signature = true` removes the mixture for an operator who wants it removed.

### A Two-Person Gate on the Background Unclean-Recovery Path

The background recovery path loses committed data exactly as the operator-driven path does, so the two-person rule seems to belong on both. Anything less looks like a hole in the rule.

It loses on the absence of a caller. That path runs from leader election and from a broker heartbeat, with no request, no connection, and no principal. Nobody waits for an answer, so a refusal has no recipient. The only thing the broker can do is leave the partition offline until a person notices.

That is a real option and it is what `require` does. It is not the default. Every partition whose leader dies at 03:00 pays the availability cost, and not only the ones an incident touches. `audit-only` is the default because it gives the property that matters most and costs nothing. An operator can prove after the fact that a data-losing election happened with no approval, and `break_glass_bypassed` is the counter that says so at the time.

### An Internal Topic for the Proposals

KFC-4 put its cuts in `__barrier_state`, and a proposal is a similar kind of control-plane state. An internal topic would keep the proposals out of the metadata image, and it would let a reader in any language list them with an ordinary consumer.

It loses on atomicity. The consume of an approval must land in the same append as the transition it authorizes. A metadata change and an internal-topic write are two appends, and a crash between them either spends the approval twice or loses it. There is no ordering of the two writes that fixes this.

The reason KFC-4 chose a topic does not apply either. Non-Rust clients read a cut, and nothing outside `krabka-guard` reads a pending proposal.

### Break-Glass Approval as a Field on the Gated Request

Every gated request could carry a proposal id, which would make the authorization explicit at the call site and would need no standing state at all.

It loses on the wire format, which is the compatibility constraint that outranks everything else here. `kafka-leader-election.sh`, `kafka-topics`, `kafka-delete-records`, and `kafka-reassign-partitions` send the request shapes their KIPs define. krabka cannot add a field to any of them and stay a Kafka broker. An operator would then need a krabka-specific tool for operations that the JVM tools already do.

A standing authorization keeps every one of those tools working. The operator gets the approval through `krabka-guard`, runs the stock tool, and the broker looks the approval up. The one request that does name its proposal explicitly is the thaw, because that request is krabka's own.
