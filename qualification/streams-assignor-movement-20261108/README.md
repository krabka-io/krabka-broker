# Active custom-assignor RF3 coordinator movement (2026-11-08)

Both real brokers pass five preparation and seven recovery scenario rows. Source
topics and coordinator state use RF3. With active members a/b and `last` selected,
both tasks0/1 belong to b. SIGKILL of the exact coordinator moves oracle1→2 and
native3→1. Recovery checks both persisted members, selected `last`, exact target
and actual assignments; selecting `first` and joining c then exercises the new
owner callback and cooperative movement back to a. This is bounded evidence,
not arbitrary fault-schedule or power-loss qualification.

Native binary hash and source identity are in receipt.json. Oracle is the same
pinned apache/kafka4.4.0 image as the invalid-output receipt. Native uses
STREAMS_FIXTURE_NODE/VOTERS and shared healthy flags with the existing fixture.
The production helper `with_internal_topics_for(3)` configures internal topics.

Run StreamsAssignorMovement BOOTSTRAP IDENTITY prepare; SIGKILL only the
returned owned coordinator; run StreamsAssignorMovement SURVIVOR IDENTITY recover.
Compile with the released jars and StreamsAssignorWire dependencies. Properties
and the actual provisioning script are retained; script paths are historical
caller-owned runtime paths and must be changed to fresh private paths to repeat.
Native ports65020/22/24, oracle65000/02/04, controllers each next port.

Initial recovery attempts correctly failed because coordinator discovery still
reported the killed owner before failure detection. The final probe now waits
boundedly for a different owner; recover2 logs are successful final executions.
No failures were relabelled successes. Runtime exploratory logs remain in
coordinator workspace `.private/assignor-movement-20261108/`.

Not covered: simultaneous member crash, plugin inconsistency across nodes,
combined ACL faults, active-member plugin-removal restart, power loss.
