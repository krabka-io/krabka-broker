# Leader site drift

**Alert:** `KrabkaLeaderSiteDrift`, `krabka_broker_leader_site_drift_partitions > 0` for 15m.

## What it means

This broker leads partitions from a site other than the stretch cluster's
`preferred_leader_site`. The usual cause is a failover: the preferred site
went down, leadership moved, the site came back, and no rebalance has moved
leadership home. Clients in the preferred site now cross a site link for every
produce. The cluster is healthy; it is in the wrong shape.

The gauge stays at zero on a cluster with no `[stretch]` section.

## Confirm

```
kafka-topics --bootstrap-server <broker>:9092 --describe
```

Compare each leader's rack, which is the site name, with `preferred_leader_site`
in the broker configuration.

## Diagnose

1. Check that every broker in the preferred site is up and unfenced. A fenced
   broker cannot take leadership back.
2. Check `leader_imbalance_check_interval` and `leader_imbalance_per_broker`.
   The automatic preferred-replica election runs only when the imbalance on a
   broker passes the ratio. A small drift can sit below it.
3. Check `krabka_broker_under_replicated_partitions` in the preferred site. A
   replica that is not in the ISR cannot be elected.

## Fix

Run a preferred election:

```
kafka-leader-election --bootstrap-server <broker>:9092 --election-type preferred --all-topic-partitions
```

The gauge falls to zero as leadership returns.

## Escalate

If the election reports `PREFERRED_LEADER_NOT_AVAILABLE`, the preferred
replica is out of the ISR. Follow
[under-replicated partitions](under-replicated-partitions.md) first.
