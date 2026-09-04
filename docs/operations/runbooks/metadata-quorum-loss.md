# Metadata quorum loss

**Alert:** `KrabkaMetadataLag`, `krabka_broker_metadata_lag_records > 100` for 5m.

## What it means

`krabka_broker_metadata_lag_records` is the distance between the quorum's
committed high watermark and the `__cluster_metadata` offset this node has
applied. A node that trails does not see the newest topics, ACLs, quotas and
leader assignments. Clients that reach it get stale `Metadata` responses.

The threshold is 100 records, which is
`DEFAULT_READINESS_MAX_METADATA_LAG` in `crates/broker/src/config.rs`. It is
the same bound `/readyz` reports `metadata_lag` against, so a node over the
threshold also answers 503 and leaves the `krabka-bootstrap` Service.
`--readiness-max-metadata-lag` moves both.

A node lags for one of three reasons: it is catching up after a restart, it
cannot keep up with the metadata write rate, or the quorum it fetches from
has lost a voter and cannot serve it.

## Confirm

```
kafka-metadata-quorum --bootstrap-server <broker>:9092 describe --status
```

The output names the leader, the leader epoch, the high watermark and each
voter's `LastFetchTimestamp` and `LastCaughtUpTimestamp`. A voter whose last
fetch is old is the one that lags.

```
kafka-metadata-quorum --bootstrap-server <broker>:9092 describe --replication
```

This prints one row per replica with its log end offset and its lag.

`krabka_broker_raft_high_watermark` and `krabka_broker_raft_current_epoch`
show the same numbers per broker on the dashboard.

## Diagnose

1. Check whether the lag falls. A node that just restarted catches up in
   seconds to minutes, and the alert clears on its own. A flat or rising lag
   is the case to act on.
2. Check the quorum has a leader. With no leader nothing commits and every
   observer's lag rises together. Follow
   [no active controller](no-active-controller.md).
3. Check the controller listener, port 9093, from the lagging node to the
   leader. A node that cannot fetch cannot apply.
4. Check disk and CPU on the lagging node. A saturated disk slows the
   metadata log append, which holds back the applied offset.
5. Check whether the node's log directory is intact. A voter whose directory
   was lost and re-created with a new directory id is not the voter the
   `VotersRecord` names, and it never catches up.

## Fix

- Wait, when the lag falls. The node reaches the quorum and `/readyz`
  returns 200.
- Restart the lagging node when the lag is flat. It refetches from the
  leader's snapshot.
- Fix the network path or the disk when those are the cause.
- Replace a lost voter. This is the case that needs operator action, and the
  steps are below.

## Replace a lost voter

A voter whose log directory is gone cannot rejoin under its old identity: the
quorum's `VotersRecord` names the old directory id. Remove it, format a new
directory, and add the new identity back. Do this one voter at a time, and
only while a majority of the remaining voters is alive.

1. Read the current voter set. The `CurrentVoters` line names each voter's id
   and directory id.

   ```
   kafka-metadata-quorum --bootstrap-server <broker>:9092 describe --status
   ```

2. Remove the lost voter from the quorum. The directory id is the one
   `CurrentVoters` prints for that node.

   ```
   kafka-metadata-quorum --bootstrap-controller <controller>:9093 \
       remove-controller --controller-id 3 --controller-directory-id <old-dir-3>
   ```

   The quorum refuses to remove its last voter.

3. Format a new log directory on the replacement node. It formats with
   `--no-initial-controllers` and the cluster's existing `--cluster-id`,
   because the quorum already exists and the node joins it rather than
   bootstraps it.

   ```
   krabka-format --log-dir /var/lib/krabka --node-id 3 \
       --cluster-id 0d7e2f5a-9b1c-4c1e-8a3f-2b6d1e4c9f10 \
       --directory-id <new-dir-3> --no-initial-controllers
   ```

   A single-node cluster that lost its only directory has no quorum to join.
   It formats with `--standalone` and starts a new cluster, and every record
   the old directory held is gone. See
   [Format the log directory](../deploy.md#format-the-log-directory).

4. Start the node with `--controller-bootstrap-servers` pointed at the live
   controllers. With `--controller-auto-join` the node adds itself as a voter
   and step 5 is not needed.

5. Without auto-join, promote the node by hand. `add-controller` takes the
   joining node's own identity from a properties file, so run it with a
   `--command-config` that names that node's `node.id`, its controller
   listener and its `log.dirs`.

   ```
   kafka-metadata-quorum --bootstrap-controller <controller>:9093 \
       --command-config /etc/krabka/controller-3.properties add-controller
   ```

6. Confirm. `describe --status` lists the new directory id, and the node's
   `krabka_broker_metadata_lag_records` falls to zero.

## Escalate

A node whose lag stays flat with a healthy leader, a reachable listener and an
idle disk is a bug. Capture `RUST_LOG=krabka_raft=debug` on the node and on
the leader for two minutes, and open an issue with the `describe --replication`
output.
