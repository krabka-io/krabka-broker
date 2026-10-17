# Sustained-Load Contract Design

A sustained-load contract declares the load, the limits and the faults of a long RF=3 run before the run starts, and a pure judge grades the run's evidence against it.

## Design Goals

Milestone M26 asks one question: does the broker stay correct during sustained load and injected failures? A long run answers that question only when three conditions hold.

- **The limits come first.** Every number the verdict uses is in a reviewed file before the run: host resources, disk capacity, seed, client settings, throughput targets, latency budgets per path and recovery deadlines per fault. A run cannot choose its limits after its results are known.
- **Loss is separate from permitted behaviour.** A producer that sees a failed send can have written the record. An application that sends again after a failure can write a record twice. These are correct behaviours under the declared semantics. A judge that reports them as failures teaches its readers to ignore it, so the judge classifies every observation against the semantics of its path.
- **The judge proves that it works.** A soak that reports zero loss is evidence only when the same machinery reports a loss that is known to exist. The self-check deletes one acknowledged record and sets one deadline that no cluster can meet. It passes only when the judge reports exactly those two failures.

The design also keeps a constraint from issue #1392: it reuses the harnesses that exist and adds no second load framework.

## Architecture Overview

The design has three parts with one direction of data flow.

```text
bench/soak/<contract>.yaml ──► executor ──► evidence.json, faults.jsonl ──► judge ──► verdict.json
        (declared)            (measures)          (recorded)              (pure)     (graded)
```

**The contract** is a YAML file under `bench/soak/`. [`rf3-72h.yaml`](../../../bench/soak/rf3-72h.yaml) is the 72-hour M26 soak that issue #1393 executes. [`self-check.yaml`](../../../bench/soak/self-check.yaml) is a run of about five minutes that proves the judge. `Contract::validate` refuses a contract that the harness cannot run safely or judge honestly, and it gives every reason at once.

**The executor** is the `soak_contract` suite of `krabka-broker` (`crates/broker/tests/soak_contract.rs`). It boots the three-broker cluster of the existing four-hour `soak` lane: real broker processes from the packaging image, one formatted three-voter quorum, and host-side access to each broker's `/proc` and data directory. It runs the declared load paths, executes the fault schedule, measures each recovery and reads every topic back. It records what it saw and makes no decision.

**The judge** is the `soak` module of `krabka-bench-driver`. It does no IO and has no clock. It expands the fault schedule from the seed, reconciles the ledgers, checks each recovery against its deadline and each path against its budgets, and returns a verdict. `krabka-soak-check` is its command-line front end: `plan` prints the schedule, `judge` grades evidence, and `self-check` runs the proof.

### What a contract declares

| Section | Content | 72-hour value |
| :--- | :--- | :--- |
| `host` | CPUs, memory, the data volume of each broker, the host free space that a disk fill must keep | 16 CPUs, 64 GiB, 500 GiB, 50 GiB |
| `cluster` | brokers, replication factor, `min.insync.replicas`, check-quorum window, `replica.lag.time.max.ms` | 3, 3, 2, 5 s, 30 s |
| `clients` | producer acks, compression, linger, batch, timeouts and send attempts; transaction size and abort cadence; consumer session, commit interval and poll timeout | `acks=all`, LZ4, 5 ms, 64 KiB, 5 attempts; 20 records, every fifth aborted; 5 s commits |
| `paths` | one entry per load path: semantics, storage, partitions, paced rate, throughput floor, payload, consumers, offset commits, and p50/p99/p999 budgets for produce and for consume | five paths, below |
| `deadlines` | leader failover, ISR rejoin, controller failover, catch-up after a pause | 15 s, 2 min, 15 s, 1 min |
| `faults` | named plans: kind, target, first offset, period, jitter, duration | seven plans, below |

The 72-hour paths cover every combination that the milestone names:

| Path | Semantics | Storage | Consumers | Commits |
| :--- | :--- | :--- | :--- | :--- |
| `classic-idempotent` | idempotent | classic | `read_uncommitted`, `read_committed` | yes |
| `classic-transactional` | transactional | classic | `read_committed`, `read_uncommitted` | yes |
| `classic-compacted` | compacted | classic | `read_uncommitted` | no |
| `tiered-idempotent` | idempotent | tiered | `read_uncommitted`, `read_committed` | yes |
| `diskless-idempotent` | idempotent | diskless | `read_uncommitted` | yes |

The rates are small on purpose. Every acknowledged record of every unbounded topic must still be on disk when the reconciler reads it back, so the contract sizes the rates to about 100 GiB for each broker over 72 hours. This is a correctness soak. The [performance qualification](../../../docs/operations/performance-qualification.md) measures throughput.

### The fault schedule

A fault plan is a template. The judge expands it with the contract's seed into occurrences named `<plan>#<n>`. Each occurrence has a start, a duration, a resolved target and the recoveries that its kind implies:

| Kind | Injection and cleanup | Recoveries measured |
| :--- | :--- | :--- |
| `pause` | `SIGSTOP`, then `SIGCONT` after the duration | catch-up after the pause; with `intends_failover`, also leader failover, and controller failover when the target is the controller |
| `restart` | `docker stop` (graceful) or `docker kill`, then `docker start` after the duration | leader failover and ISR rejoin; controller failover when the target is the controller |
| `slow_follower` | `tc netem` delay in the broker's network namespace, or a CPU quota on its container; removed after the duration | ISR rejoin |
| `disk_pressure` | a ballast file that fills the data volume to the watermark; deleted after the duration | ISR rejoin |

## Key Design Decisions

### The contract is data, and the judge is a pure function

The executor and the judge are separate on purpose. The executor talks to Docker, to the brokers and to the clock, so its tests need a cluster. The judge reads two values and returns a third, so every rule it applies has a unit test whose answer is known: a lost record, a visible aborted record, a committed offset that went back, a recovery that was late, a recovery that nobody measured. A change to a rule is a change to a table of tests, not a change to a five-minute container run.

The alternative was one test that measures and asserts in the same loop, as the four-hour `soak` lane does. That lane asserts resource drift, and a few thresholds in code suit it. A contract with five paths, four deadlines and seven fault plans does not suit it: its numbers must be reviewable as one declaration, and the same judge must grade a 72-hour run and a five-minute run.

### Extend the soak lane, do not add a load framework

The repository has three load harnesses. `packaging/performance/qualify.sh` compares throughput with Apache Kafka on Kind. `krabka-bench-driver` runs benchmark scenarios on Kubernetes against an operator-managed cluster. The `soak` suite runs a three-broker cluster of real broker processes for hours and reads each process's resources from `/proc`.

The contract extends the third and borrows from the second. The executor reuses the `soak` lane's cluster fixture and probes unchanged. A host-side Docker cluster is what lets the executor send `SIGSTOP` to one broker process, change its CPU quota, put a `netem` qdisc in its network namespace and fill its data volume, with no privileged sidecar and no Kubernetes API. The judge lives in `krabka-bench-driver`, the crate that already owns scenario files with units and the report aggregator. The contract file uses the same unit-carrying encoding as a scenario: `64KiB`, `15s`, `200/s`, never a bare number.

### Records are identified by producer and sequence, not by offset

Each producer numbers its sends from zero and writes `<producer>/<seq>;` at the start of every record value. A reader identifies a record from its value alone. An offset cannot do this job: a failover can truncate an uncommitted tail and write different records at the same offsets, which is the failure that the reconciler must catch.

The ledgers store sequence numbers as sorted, disjoint runs (`SeqSet`). A producer that acknowledges in order costs one run for any length of run. A reader that consumes all partitions at once costs a few runs at a time. The 72-hour run acknowledges about 200 million records, and its ledgers stay small.

### Every observation is classified against the declared semantics

The reconciler does not ask "did every record arrive". It asks what each observation means under the semantics of its path:

| Observation | Idempotent | Transactional | Compacted |
| :--- | :--- | :--- | :--- |
| acknowledged, not read | lost | lost if committed (`read_committed`), or lost at all (`read_uncommitted`) | not applicable |
| read twice, sent once | unexpected duplicate | unexpected duplicate | not applicable |
| read twice, sent again after a failure | permitted | not applicable | not applicable |
| read, but its send failed | permitted (maybe delivered) | permitted | permitted |
| read, never sent | phantom | phantom | phantom |
| aborted, read under `read_committed` | not applicable | aborted visible | not applicable |
| outcome unknown, partly visible under `read_committed` | not applicable | partial transaction | not applicable |
| latest acknowledged value of a key not read | not applicable | not applicable | compaction regression |

The verdict reports the permitted cases as counts (`maybe_delivered`, `permitted_duplicates`), so a reader can see them, but they do not fail the run.

The contract's semantics decide which rules apply, not the label in the ledger. A ledger labelled with other semantics fails. A transactional ledger fails when an acknowledged record is outside every recorded transaction: otherwise an empty transaction list would check `read_committed` against nothing.

Committed offsets get their own check. Before each fault, the executor records the last commit that the coordinator acknowledged for each partition of each committing group. After the fault's recoveries, it fetches the committed offset again. The fetched offset must be at least the acknowledged one and at most the log end. A `final` check repeats this after the load. The judge derives the checks it expects from the contract and the schedule, and a missing check fails the run.

### Faults are bounded, one at a time, and always cleaned up

The schedule refuses a contract whose occurrences overlap. An occurrence can start only when the previous one has ended and each recovery that it is measured for has had its full deadline. No occurrence can run, or recover, inside the `settle` tail, so the read-back always sees a cluster that had time to recover. Each bound has a reason:

- A pause that does not intend a failover must be shorter than the check-quorum window. A pause that intends one must be at least as long. Otherwise the run measures a different fault from the one the contract names.
- A disk fill goes to a fraction of the declared `disk_capacity`, at most 95%. The executor refuses a fill that would leave less than `disk_reserve` free on the host file system. It does not make the fill smaller, because a smaller fill is a different fault.
- A `netem` image must be pinned by digest, as every other image in this repository is.

Every injection has a cleanup that runs when the fault ends, and again from a drop guard if the test panics. The guard removes the ballast file, removes the qdisc, restores the CPU quota and sends `SIGCONT`. A failed run must not leave a broker stopped on a shared runner. The executor appends each fault to `faults.jsonl` when it injects the fault and again when it clears it, so a run that stops early still leaves its fault timeline.

The judge fails a fault that the schedule names but the evidence does not record. It also fails a recovery that the fault kind implies but nobody measured. A judge that grades only what the executor recorded passes a run whose injector did nothing. A fault record must also match its schedule: the same kind, the role that the plan targets, and a start no earlier than its offset and at most 30 s after it. When a fault that moves leadership lands on the active controller, the judge requires a controller-failover measurement, whatever selector the plan used.

### Recovery is measured from the cluster's own view

Each deadline starts from the event that makes the recovery possible:

| Check | Starts at | Ends when |
| :--- | :--- | :--- |
| leader failover | the injection | every partition that the target led has a live leader on another broker (`Metadata`) |
| controller failover | the injection | another node reports `krabka_broker_active_controller 1` |
| ISR rejoin | the cleanup | every partition of every contract topic has its full replica set in the ISR |
| catch-up after a pause | `SIGCONT` | every partition has its full replica set in the ISR again |

The executor polls these conditions and stops at twice the deadline. A recovery that it did not see is `NotObserved`, which fails as surely as a late one. The KIP-835 quorum gauges that issue #1341 adds (the controller's last applied record offset, timestamp and lag) are the natural signal for controller progress after a failover. When they land, the controller check can also require the new controller to catch up, not only to be elected.

### Budgets apply to the steady windows

A latency budget that includes the seconds around a leader failover measures the failover, which the recovery deadlines already judge. So the executor keeps two histograms for each path and direction. "Disturbed" covers each fault from its start to its end plus its longest recovery deadline. "Steady" covers everything else. The budgets and the throughput floor apply to the steady windows. The verdict keeps the disturbed percentiles as evidence, and issue #1393 publishes them.

### The self-check injects real failures

The self-check contract names one idempotent path and one recovery. After the load stops, the executor deletes the record at the log-start offset of one partition of that path with `DeleteRecords`. This is a record that the producer saw acknowledged. Then the read-back runs as in any soak. The contract also judges the first occurrence of one fault plan against a deadline of one millisecond. The cluster recovers in seconds, so the recovery really is late.

`judge_self_check` passes only when three conditions are true. The reconciler reports exactly the deleted record as lost, by every reader of its path, and reports nothing else. The deadline checker reports exactly the one impossible deadline as missed, and no other recovery. Nothing else in the verdict fails. A self-check with no injection fails, so the self-check cannot pass vacuously.

The same proof also runs with no cluster. `krabka-soak-check self-check` without `--evidence` builds a synthetic perfect run of the contract and removes one record from it. This runs in every pull request. It proves that each checked-in contract is consistent, because a perfect run of a soak contract passes. It also proves that the verdict depends on the evidence, because the same run with one record removed fails and names that record.

## Integration

**Cluster fixture.** The executor uses `crates/broker/tests/soak/cluster.rs`, which the four-hour `soak` lane also uses. A change to that fixture selects the `soak_contract` suite on a pull request.

**CI.** The `soak_contract` suite is in the `krabka_image` list of `crates/broker/BUILD.bazel`, so it runs as `//crates/broker:soak_contract_docker_test` in the `docker` matrix of `.github/workflows/ci.yml`. A pull request selects it when it changes the suite, the soak fixture, the judge or a contract (`.aspect/container_suites.axl`). The nightly full matrix always runs it. It takes minutes, so it does not need its own lane, unlike the four-hour `soak`. The hermetic tests of the judge (`krabka-bench-driver`'s unit tests, `contract_files` and `soak_check_cli`) run in `bazel test //...` on every pull request.

**The 72-hour run.** Issue #1393 runs the same executor with `KRABKA_SOAK_CONTRACT=bench/soak/rf3-72h.yaml` on a host that matches the contract's `host` section, with `KRABKA_SOAK_EVIDENCE_DIR` on retained storage. The evidence directory holds the contract, the expanded schedule, `faults.jsonl`, `evidence.json` and `verdict.json`. Publish it unchanged, as the performance qualification does.

**The diskless path.** A diskless WAL quorum serves a shard only to a voter that authenticates as `broker-<id>` on the inter-broker listener, and places one voter per rack. So when a contract has a diskless path, every node of the soak cluster binds a second listener, `SASL_PLAINTEXT`, that `inter_broker_listener_name` selects, and reports its own rack. Every client of the run stays on the anonymous `PLAINTEXT` listener. The broker's file configuration has no inter-broker PLAIN or SCRAM credential, so each node authenticates with `OAUTHBEARER`: an unsecured (`alg: none`) token whose `sub` is its principal, which the peers accept under `[oauthbearer] allow_unsecured = true`. That is a test-cluster shortcut, not a deployment shape. The three nodes share one local object store, the host directory that a tiered path already uses for its remote tier, and the topic-backed remote-log metadata manager that holds the WAL flush index. `crates/broker/tests/diskless_jepsen.rs` runs the same quorum in process with SASL/PLAIN.

**What the executor does not run yet.** The `netem` slowdown is implemented but has not run on a kernel with the `sch_netem` module. Issue #1393 closes that before the 72-hour run.

**Why the self-check restarts a follower.** The self-check runs in the pull-request container matrix, so it uses faults whose recovery the packaged broker meets every time: a quiet pause, a CPU throttle, a disk fill and a graceful follower restart held past the broker's 9 s `heartbeat_timeout`. The faults that found broker problems during development stay in the 72-hour contract, where a failure is evidence: a killed leader that restarts inside the heartbeat timeout is not fenced and keeps its partitions, a gracefully stopped controller is replaced only after 20 to 24 s, and the read-back of a tiered topic from its earliest offset returned no records.

**Metrics and alerts.** The executor reads `krabka_broker_active_controller` from each broker's `/metrics`. A 72-hour run also scrapes the brokers into the observability stack, so the alert rules in [`docs/operations/alert-rules.yaml`](../../../docs/operations/alert-rules.yaml) can fire and resolve around each recorded fault. Issue #1393 retains those alerts beside the fault timeline.

## Kafka / KIP Compliance

The contract tests Kafka semantics. It does not define new semantics.

- **Idempotent and transactional producers ([KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)).** An idempotent producer writes each send at most once, so a duplicate of a record sent once is a failure. A `read_committed` consumer reads up to the last stable offset and never sees an aborted record. A transaction whose `EndTxn` answer was lost has committed or aborted, but it has done so as a whole. The reconciler checks each of these statements separately.
- **Committed offsets.** `OffsetCommit` and `OffsetFetch` follow Kafka. A commit that the coordinator acknowledged is durable in `__consumer_offsets`, so a coordinator failover must not lose it.
- **Tiered storage ([KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)).** A tiered path sets a short local retention, so the read-back from the earliest offset reads remote segments.
- **`DeleteRecords` ([KIP-107](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+deleteRecordsBefore%28%29+API+in+AdminClient)).** The self-check uses the Kafka API to remove exactly one record, so the injection goes through the same path that an operator uses.
- **KRaft timeouts ([KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum)).** The broker's `controller_election_timeout` covers Kafka's `controller.quorum.fetch.timeout.ms`, which is 5 s by default. It is the watchdog after which followers replace a silent controller leader. The contract's `check_quorum_window` declares this value, so a pause below it does not move the controller. The leader's own check-quorum resignation comes later, at 1.5 times the timeout.
- **ISR ([`replica.lag.time.max.ms`](https://kafka.apache.org/documentation/#brokerconfigs_replica.lag.time.max.ms)).** A paused follower that stays below this bound does not leave the ISR. Its catch-up is then immediate, and the verdict records that.

## Testing

- `crates/bench-driver/src/soak/*.rs`: unit tests for every validation rule, the seeded schedule, sequence sets, every reconciliation class, the deadline and budget checks, and the self-check judge.
- `crates/bench-driver/tests/contract_files.rs`: each contract under `bench/soak/` validates, and a synthetic run of it is judged as its profile says.
- `crates/bench-driver/tests/soak_check_cli.rs`: `krabka-soak-check` as an operator runs it.
- `crates/broker/tests/soak_contract.rs`: the live executor, and the self-check against a real three-broker cluster.
- `crates/broker/tests/soak.rs`: the four-hour resource-drift soak that this design extends.
