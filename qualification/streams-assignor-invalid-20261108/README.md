# Invalid custom-assignor owner: characterized divergence (2026-11-08)

This bounded probe deliberately assigns the two source tasks to an ID absent
from the member set. It is **not a parity pass**. The released Kafka 4.4.0 oracle
accepts member `b` (heartbeat error 0); native validation rejects unknown members.
We preserve that safety boundary rather than silently weaken it to match malformed
plugin output. Healthy `first` selection plus a fresh join repairs the oracle.

Oracle image ID: `sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f`.
Native base revision: `234dadc776e574fd91b0ec9c4d2d84cfa1a0c33e` plus the
fixture/probe changes in the containing commit. No production code is changed.

The retained raw wire logs and callback traces are evidence, not a claim that
all invalid task sets behave identically. Earlier exploratory oracle attempts
expected error -1 and failed; they revealed this divergence and are retained
in the caller-owned runtime directory, not relabelled passes.

## Reproduce

Follow `tools/kafka44/STREAMS_ASSIGNOR_WIRE.md` with this directory's
`broker.properties` (ports64940/64941). Compile `StreamsAssignorInvalid.java`
in addition to its existing four Java dependencies. Start native with
`STREAMS_FIXTURE_ASSIGNORS=1` and a fresh data/flags directory (port64944).
The fixture registry now includes `unknown-member` after `failure`.

```sh
java -cp "$LIBS/*:$RUN/classes" StreamsAssignorInvalid 127.0.0.1:64940 fresh-oracle 0
java -cp "$LIBS/*:$RUN/classes" StreamsAssignorInvalid 127.0.0.1:64944 fresh-native -1
```

Expected errors are intentionally explicit and different. A successful probe
means characterization passed, **not matching broker behavior**. The rejecting
mode additionally checks absent failed-member persistence, unchanged group and
assignment epochs, and unchanged original member actual/target assignments.
Both modes then require healthy callback placement and cooperative recovery.

RF3 movement, combined ACL/callback failures, active-member restart fallback,
other malformed-output classes, crash windows and arbitrary schedules remain
outside this receipt.

## Execution result

Both final probe invocations exited zero, seven scenario rows each. Native
returned -1 and retained the previous member/epoch/assignment state; oracle
returned 0 and accepted `b`. Both recovered exact tasks0/1 at `a` after repair.
See `receipt.json` for hashes and identities. Native build command:

```sh
CARGO_TARGET_DIR="$EXISTING_TARGET" CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo build -p krabka-broker --example streams_fault_broker
```

Runtime data and exploratory logs: coordinator workspace `.private/assignor-invalid-20261108/`.
