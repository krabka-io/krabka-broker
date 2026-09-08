# Scale a cluster

Move replicas before removing capacity. A registered broker is not useful
capacity until partitions are assigned to it, and stopping a broker does not
move its replicas elsewhere.

## Add brokers

Format each new log directory as a KIP-853 joiner, then start the broker with
an existing controller DNS name. Keep the cluster id from the existing fleet.

```sh
krabka-format --log-dir /var/lib/krabka --node-id 4 \
  --cluster-id "$CLUSTER_ID" --no-initial-controllers
krabka-broker --log-dir /var/lib/krabka --broker-id 4 \
  --controller-bootstrap-servers controllers.example:9093 \
  --controller-auto-join
```

Wait for broker 4 to appear unfenced in `kafka-cluster list-endpoints`. Then
generate a plan over every usable broker. Save the second JSON document printed
by `--generate` as `proposed.json` and review it before execution.

```sh
printf '{"topics":[{"topic":"orders"}],"version":1}\n' > topics.json
kafka-reassign-partitions --bootstrap-server "$BOOTSTRAP" --generate \
  --broker-list 1,2,3,4 --topics-to-move-json-file topics.json
kafka-reassign-partitions --bootstrap-server "$BOOTSTRAP" --execute \
  --reassignment-json-file proposed.json --throttle 52428800
kafka-reassign-partitions --bootstrap-server "$BOOTSTRAP" --verify \
  --reassignment-json-file proposed.json
```

During the move, `krabka_broker_reassigning_partitions` must fall to zero and
`rate(krabka_broker_replication_throttled_bytes_{in,out}_total[5m])` should sit
near the configured rate. `--verify` reports completion and clears the KIP-73
throttle. If the gauge stays flat while both byte rates are zero, use the
[stalled reassignment runbook](runbooks/reassignment-stalled.md).

The Kubernetes operator already owns joiner formatting and controller
discovery. Increase `KafkaNodePool.spec.replicas`, wait for its Ready condition,
then create and approve a `KafkaRebalance`; do not reproduce the commands above
inside the pod template.

## Remove a broker

Generate a plan whose `--broker-list` omits the broker, execute it, and verify
it exactly as above. Do not continue until `kafka-topics --describe` names the
broker in neither `Replicas` nor `Isr` for any partition.

When `[break_glass]` is configured, unregistering needs a proposal approved by
a second principal. The proposal target is the decimal broker id.

```sh
krabka-guard -b "$BOOTSTRAP" break-glass propose \
  --action unregister-broker --target 4 --reason "capacity removal" --ttl 30m
krabka-guard -b "$BOOTSTRAP" break-glass approve \
  --proposal "$PROPOSAL_ID" --sign-with bob.pk8 --key-id bob
kafka-cluster unregister --bootstrap-server "$BOOTSTRAP" --id 4
```

The unregister consumes the matching approved proposal atomically. A missing
approval returns `POLICY_VIOLATION (44)`. Stop the broker and release its disk
only after `kafka-cluster list-endpoints` no longer lists it. For an
operator-managed pool, reduce `spec.replicas` only after the explicit
reassignment omitting that broker verifies complete; the operator owns the
StatefulSet and retained PVC lifecycle.
