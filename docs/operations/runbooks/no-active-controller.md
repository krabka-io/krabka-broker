# No active controller

**Alert:** `KrabkaNoActiveController`, `(sum(krabka_broker_active_controller) != 1) or absent(krabka_broker_active_controller)` for 1m.

## What it means

Exactly one broker in a cluster holds raft leadership of the metadata quorum.
The sum of `krabka_broker_active_controller` over every scraped broker is that
count.

A sum of zero means the quorum has no leader. No metadata change commits:
topic creation, ISR updates, fencing, reassignment and ACL writes all wait.
Produce and fetch on existing leaders continue until a leader fails, and then
that partition goes offline with no controller to replace it.

A sum above one means two brokers each believe they lead. In a healthy quorum
this cannot last past one election timeout. A sustained value above one means
the scrape covers two clusters, or two nodes formatted with the same node id
and different directory ids.

## Confirm

```
kafka-metadata-quorum --bootstrap-server <broker>:9092 describe --status
```

The output names the leader id, the leader epoch and each voter's last fetch
timestamp. `LeaderId: -1` confirms a quorum with no leader.

`krabka_broker_raft_current_state` gives the same answer per broker: one
series per state, and the `leader` series is 1 on the controller. A cluster
where every voter sits in `candidate` is in a failed election loop.

## Diagnose

1. Count the live voters. A quorum needs a majority. Two voters down out of
   three elects nobody. `up{job="krabka-broker"}` shows which brokers
   Prometheus reaches; follow [broker down](broker-down.md) for each one that
   is gone.
2. Check the controller listener, port 9093, from each voter to each other
   voter. A voter that cannot reach the others starts an election it cannot
   win.
3. Read `krabka_broker_raft_current_epoch` on each voter. An epoch that
   climbs on every scrape is an election loop, not a lost leader. Follow
   [controller leader flapping](controller-leader-flapping.md).
4. Check `krabka_broker_ignored_static_voters`. A non-zero value means the
   `controller_quorum_voters` list in the config file no longer matches the
   dynamic voter set, and a restarted node reads the wrong peers.
5. For a sum above one, compare the cluster ids. `kafka-metadata-quorum
   describe --status` prints the cluster id on each broker. Two different ids
   under one Prometheus job is a scrape-configuration error, not a broker
   fault.

## Fix

- Restart the failed voters. The quorum reforms and elects a leader with no
  operator action. This is the fix for the common case.
- If a voter's log directory is lost, remove it from the quorum and add it
  back after a re-format. Follow
  [metadata quorum loss](metadata-quorum-loss.md).
- Fix the network path or the TLS material when the voters are up but cannot
  reach each other.

## Escalate

A quorum with a majority of voters alive, an unchanging epoch and no leader
is a bug. Capture `RUST_LOG=krabka_raft=debug` on every voter for two minutes
and open an issue with the `describe --status` output.
