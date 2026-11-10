# Kafka 4.4 follow-up integration (2026-11-08)

Base: `234dadc77`. Local branch: `finish/kafka44-qualified-followups`.
This integrates recovered worker artifacts; it does not graduate universal parity.
No remote publication, merge, release or production change was performed.

## Integrated work

- Topology: directory fsync after namespace mutations, retry semantics, stale queued-push regressions. [Receipt](streams-storage-durability-20261108/README.md): 5 filesystem and 29 actor tests passed again during recovery using the exact source/binary hashes; syscall ordering and directory-sync EIO trace included.
- DLQ: phase-two persistence crash schedules using the production acquisition-state machine. [Receipt](kafka44-dlq/phase2-model-20261108/receipt.json): 4 tests passed again during recovery; normal graph 128 unique/185 generated states. Lost-recovery mutant is detected. Forty historical ordering/RF3 denial artifact hashes revalidated. Updated stale summary to respect later ordering evidence.
- Assignors: [invalid output](streams-assignor-invalid-20261108/README.md), [RF3 movement](streams-assignor-movement-20261108/README.md), and [ACL faults](streams-assignor-acl-20261108/README.md). All 54 retained scenario rows passed their characterization expectations and source hashes match receipts. Live brokers were not rerun during recovery.
- [JVM extension assessment](native-extension-assessment-20261108/README.md): released signatures and native SPI boundaries; no JVM loader claimed.

## Remaining work and decisions

1. Kafka accepts an unknown assignment owner while native rejects it without partial state. This is a measured behavioral difference, not a parity pass. Validation remains intact.
2. An opt-in deterministic Java assignor worker is implemented; see [bridge qualification](jvm-assignor-bridge-20261108/README.md). It is not universal JVM compatibility: persistent topology-storage and custom no-argument DLQ-manager adapters are now implemented; see [extension qualification](jvm-extensions-20261108/README.md). Full assignor metadata, stock Java DLQ broker-service construction and distributed deployment remain absent.
3. The [paired live RF3 source-crash run](kafka44-dlq/rf3-crash-20261108/README.md) now covers SIGKILL during durable ARCHIVING after DLQ append with confirmation held: both peers recover with two copies and six final ARCHIVED/start0/count1 replica rows (offline checked). It uses the immutable retained candidate, not a combined-tree build. The subsequent [phase-two transport-failure run](kafka44-dlq/persister-failure-20261108/README.md) observes native replay with two copies and oracle recovery with one; the transport cut points differ, and the corrected harness was not rerun end-to-end. Negative persister-response injection remains unqualified; the bounded model is separate evidence. Existing RF3 denial remains a separate completed-write/clean-restart schedule. No exactly-once guarantee.
4. Topology directory sync is not a physical power-cut test or distributed fencing. The backend has no ownership generation/CAS; concurrent old/new owners remain outside the supported host contract.
5. [Active-member plugin-removal restart](streams-assignor-active-restart-20261108/README.md) is now measured on both peers for a clean single-broker restart. Arbitrary fault schedules and inconsistent plugins across nodes remain unqualified.

The combined tree was assembled without conflicts. The subsequent [Java-extension qualification](jvm-extensions-20261108/README.md) passes the combined broker-library suite (9,157 default passes plus all 7 ignored JVM checks in separate lanes) and broker-library Clippy. No whole-workspace/all-targets Clippy run or refreshed combined live-broker qualification is claimed. The retained base is intentionally pinned, not silently rebased onto a moving remote main.

## Combined candidate rerun (2026-11-09)

The [fresh paired persister run](kafka44-dlq/combined-persister-20261109/README.md) supersedes the stale-executable and corrected-harness-not-rerun limitations above for this one schedule. Locked combined broker/formatter build passed; both peer runners and all six replica-state assertions passed. Native observed two DLQ copies and oracle one, both lag zero. The remaining transport-cut-point, negative-response, deployment and durability limits remain unchanged.
