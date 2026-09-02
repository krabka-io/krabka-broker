# Controller leader flapping

**Alert:** `KrabkaControllerLeaderFlapping`, `rate(krabka_broker_controller_leader_changes_total[5m]) > 0` for 10m.

## What it means

The raft leader of the metadata quorum keeps changing. One election is normal
after a controller restarts. A rate that stays above zero for ten minutes means
the quorum cannot hold a leader. While it flaps, no metadata change commits:
topic creation, ISR changes, fencing and reassignment all wait.

## Confirm

`krabka_broker_active_controller` shows which broker holds leadership at each
scrape. A value that moves between brokers confirms the flap. `kafka-metadata-quorum
--bootstrap-server <broker>:9092 describe --status` prints the leader, the epoch
and each voter's lag.

## Diagnose

1. Compare `controller_election_timeout` (default 5s) and
   `controller_heartbeat_interval` with the round-trip time between the
   controller listeners. A stretch cluster with a slow site link needs a
   longer timeout.
2. Check the controller listener from each voter to each other voter. A voter
   that cannot reach the leader starts an election on its own.
3. Check the broker logs at the moments `krabka_broker_active_controller`
   changed. A voter that restarts in a loop shows up as a repeated boot.
4. Check CPU on the controllers. A starved leader misses heartbeats. The
   controller's own handler time is in
   `krabka_broker_request_duration_seconds{api_key="Fetch"}` on the controller
   listener.
5. Check `krabka_broker_ignored_static_voters`. A non-zero value means the
   static voter list in the configuration no longer matches the dynamic quorum
   and the operator should update the file.

## Fix

- Raise `controller_election_timeout` on every controller when the cause is
  link latency. Roll the change one controller at a time.
- Fix the network path or the TLS material when the cause is an unreachable
  voter.
- Remove a voter that restarts in a loop with `kafka-metadata-quorum
  remove-controller` until it is repaired.

## Escalate

If the leader changes with no error in any controller log, capture
`RUST_LOG=krabka_raft=debug` on every voter for a few minutes and open an
issue.
