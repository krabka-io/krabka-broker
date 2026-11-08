# Active members across custom-assignor removal (2026-11-08)

**10 matching scenario rows per broker** (four prepare, six verify) passed on
released Kafka 4.4.0 and the immutable native candidate identified in
[receipt.json](receipt.json). All four Java probe processes exited zero. This
closes the previously unmeasured **clean single-broker active-member plugin
removal** schedule for that candidate. No production fix was required.

## Actual assertions

- Two members `a` and `b` join; persisted override `last` executes its callback,
  and cooperative reconciliation assigns source tasks 0/1 to `b`.
- Neither member leaves. Both servers stop cleanly, then reopen the same data
  with only `sticky` registered. Both old member IDs are still present.
- Describe reports `sticky` while DescribeConfigs retains raw override `last`.
  Both old tasks/targets remain with `b` before a new assignment is required.
- Member `c` joins, forcing the fallback implementation to assign again. All
  three members remain; target assignment is at the current group epoch, covers
  exactly tasks 0/1 once each, and actual assignments converge to targets through
  heartbeats acknowledging only retained tasks. Override `last` remains stored.

We deliberately do not require the same sticky tie-breaking placement across
implementations: coverage, uniqueness, current epoch and cooperative convergence
are the compatibility assertions. Initial custom placement is exact.

## Reproduce

Use the [previous restart instructions](../streams-assignor-restart-20261107/README.md)
with this directory's before/after properties, oracle ports 64960/64961 and native
port 64964. Compile the same five Java sources, including the updated
`tools/kafka44/StreamsAssignorRestart.java`. Use fresh caller-owned data and flags
folders, never production data. The Java commands are:

```sh
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64960 active08 prepare-active
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64964 active08 prepare-active
# Clean-stop both, remove custom registration, restart same data without format.
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64960 active08 verify-active
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64964 active08 verify-active
```

Preparation starts native with `STREAMS_FIXTURE_ASSIGNORS=1`; reopening omits
that variable and passes the harness's final `rejoin` argument. The measured
native binary is the prior candidate pinned by the empty-member receipt; no new
native compilation is claimed. Registry and config code match the combined
source, but other actor/storage changes exist, so this is **not** a combined-tree
qualification or a new full broker test run. Java compilation and
`git diff --check` also passed. Runtime: coordinator workspace
`.private/assignor-active-20261108/`.

## Safety divergence disposition

The [unknown-owner measurement](../streams-assignor-invalid-20261108/README.md)
remains a real non-parity result. Its callback gives **all** tasks to a ghost
member. Merely ignoring that owner would omit every active task; mapping it to a
real member would invent plugin intent. Neither preserves the documented complete
valid-target contract. Native therefore retains validation and atomic rollback;
no compatibility translation or production relaxation is justified by this
measurement. Existing raw rejection/repair logs establish no partial state.

RF3 movement with inconsistent plugin registries or differing implementations
behind the same name remains unqualified. Identical registration/behavior on
all brokers remains the supported host contract. This result must not be
relabeled full arbitrary-plugin parity, crash durability or unbounded liveness.
