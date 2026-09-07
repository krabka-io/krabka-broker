# Roadmap

krabka plans work as milestones. Each milestone asks one question about a claim
the project makes, and it closes when the evidence answers that question. A
milestone is not a feature list. It names the signals that are decorative, the
claims that no test supports, and the exit criteria that end the argument.

The [GitHub milestones](https://github.com/krabka-io/krabka-broker/milestones)
are the source of truth. Each one carries the full statement of the gap, with
the file and test citations behind it. This page gives the sequence and the
reason for it.

## Delivered

| Milestone | Question |
| :--- | :--- |
| M1 | Can CI catch a regression, and is the dependency policy real? |
| M2 | Does what krabka persists survive failure, and are the proved decisions proved? |
| M3 | Is the diskless WAL reachable, safe and observable? |
| M4 | Is the Kafka compatibility claim true and evidenced? |
| M5 | Can anyone run this, measure it, and know it is healthy? |
| M6 | Can someone else operate or contribute to this? |
| M7 | Formal verification: safety-critical algorithms. |
| M8 | Does the cluster survive a partition, a prune and a restart without losing metadata or records? |
| M9 | Does a stock Kafka Streams, Connect or share-consumer deployment run against krabka? |
| M10 | Can an operator secure the cluster and prove who did what? |

## In progress

[M11](https://github.com/krabka-io/krabka-broker/milestone/11) asks whether an
operator can see the cluster's health, roll it, repair the quorum and keep
tenants fair without guessing.
[M12](https://github.com/krabka-io/krabka-broker/milestone/12) asks whether what
ships is the thing that was tested, and whether the evidence the documents cite
holds up.

## Next

The next four milestones run in the order below. The order follows one rule: a
wrong answer that an adopter finds in production comes first, and a wrong answer
that an evaluator finds in an hour comes after it.

### [M13: Does an acknowledged record survive a kill, and does anything page when the broker starts failing?](https://github.com/krabka-io/krabka-broker/milestone/13)

Every stop in the test suite today is a graceful drain, and every broker runs
in process. No suite has had a broker vanish between an acknowledgement and an
`fsync`. The background paths under that acknowledgement discard their own
errors, the fault-injection seam reaches only the active `.log` file, and no
alert rule fires on a broker that is up and slow. M13 makes a real process die,
widens the seam to every durable write, and gives the failures a metric and an
alert.

M13 comes first because the rest of this list is built on the answer. A soak
lane and a fault-injection seam are also what M14 needs to test an object store
that misbehaves.

### [M14: Is the tiered archive a complete copy, and can a cluster be rebuilt from it?](https://github.com/krabka-io/krabka-broker/milestone/14)

After local retention removes a segment, the archive holds the only copy. The
copy pass decides what to copy by base offset, which the local-retention module
itself documents as unsound across replicas, so a leader change can leave a
permanent hole. A failed cold-tier read reaches the client as
`OFFSET_OUT_OF_RANGE`, and a follower is served the same way. M14 derives the
copy point from the coverage watermark, corrects the error mapping, and writes
the backup and restore procedure that `krabka restore` needs.

### [M15: Can the clients and tools a stranger already runs talk to krabka, and does the matrix they read say so?](https://github.com/krabka-io/krabka-broker/milestone/15)

The non-JVM client evidence is one 2021 build of `kcat`, and 86 cells of the
KIP matrix client column read `none`. The matrix itself is built by a string
scan of `crates/`, so a KIP that no comment names has no row. The client
listener advertises eleven APIs that Kafka does not, and MirrorMaker 2 appears
nowhere in the tree. M15 adds a modern librdkafka client and one independent
client, filters the advertised table by listener type, builds the matrix from a
checked-in inventory, and replicates a topic onto krabka with MirrorMaker 2.

### [M16: Can this be deployed on Kubernetes, grown under load, and given the next build on the same disks?](https://github.com/krabka-io/krabka-broker/milestone/16)

The reference manifests under `packaging/k8s/` have never been applied, they
declare no anti-affinity and no resource requests, and the StatefulSet cannot
scale because the controller bootstrap flag takes an address and not a DNS
name. Every reassignment test reaches completion by writing the finished ISR
into the metadata log, so no test has moved a byte onto a new broker. No CI lane
boots the current build on a directory an earlier tag wrote. M16 proves each of
the three operations that turn a trial into a deployment.

## Deferred

Two blocks of work are ready and wait for a later milestone.

- **Performance and scale.** The bench lane records a baseline that nothing
  gates on, no suite compares krabka with a JVM broker end to end, and the
  largest partition count in any bench is 200. None of this work loses a record,
  so it follows M13 through M16.
- **Configuration and parity remainder.** Placement for a broker with no rack,
  `AlterReplicaLogDirs` for a replica the broker does not host, the KIP-704
  leader recovery state, and the empty description cells in the generated
  configuration reference.

## How a milestone starts

A milestone starts as a survey of the tree. The survey reads the code, the
tests, the CI lanes and the documents, and it records each claim that the
evidence does not support. Every finding carries the file, the test or the
workflow job that shows it. A finding with no citation does not become an issue.
