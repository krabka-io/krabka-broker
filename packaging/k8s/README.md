# Secure reference Kubernetes deployment

The reference deployment is secure by default: both Kafka and KRaft use mutual
TLS, the simple authorizer denies requests without an ACL, the controller port
is broker-only, and the release image is pinned by digest. It intentionally
cannot start until cluster-specific identities are supplied.

## Prerequisites

1. Issue one CA and three broker certificates. Each certificate must be usable
   for both TLS server and client authentication, have subject `CN=broker-N`,
   and contain DNS SANs `krabka`, `krabka-bootstrap` (the Service clients
   bootstrap through) and `krabka-N.krabka` for its ordinal.
2. Issue an administrator client certificate with subject `CN=admin` from the
   same CA. The manifest makes this bootstrap identity a super user; use it to
   create least-privilege ACLs, then remove it from `super_users` if desired.
3. Create the required Secret. The key names are an interface and are checked
   by the StatefulSet before the broker starts:

   ```sh
   kubectl create secret generic krabka-tls \
     --from-file=ca.crt \
     --from-file=broker-0.crt --from-file=broker-0.key \
     --from-file=broker-1.crt --from-file=broker-1.key \
     --from-file=broker-2.crt --from-file=broker-2.key
   ```

Apply all resources (and replace the example cluster and directory UUIDs before
first boot):

```sh
kubectl apply -f packaging/k8s/configmap.yaml
kubectl apply -f packaging/k8s/service.yaml
kubectl apply -f packaging/k8s/networkpolicy.yaml
kubectl apply -f packaging/k8s/poddisruptionbudget.yaml
kubectl apply -f packaging/k8s/statefulset.yaml
```

Only pods in the same namespace labelled `krabka.io/client=true` can connect to
Kafka. Clients must also trust `ca.crt`, present a CA-signed client certificate,
and have matching Kafka ACLs. Label monitoring pods `krabka.io/monitor=true` to
scrape port 9404 directly from pod IPs. The controller (9093), metrics (9404),
and health (9405) ports are intentionally absent from Services. Kubelet probes
still reach the pod-local health port; Kubernetes NetworkPolicy does not block
traffic to a pod from its node.

## What CI verifies

`aspect check-k8s-security` runs on every pull request. It reads these
manifests and fails unless both listeners require client certificates, the
authorizer denies requests that match no ACL, the Services publish only 9092,
the NetworkPolicy restricts 9093 to brokers and 9092 and 9404 to their labels,
and the image is pinned by digest.

`aspect kind-smoke` deploys the manifests on the Kind cluster in
[`.github/kind.yaml`](../../.github/kind.yaml) and checks the same contract on
a live cluster. The `kubernetes` CI job runs it whenever a change reaches its
inputs, and every nightly runs it too. Each negative check runs inside the
cluster through `kubectl exec` under a bounded timeout, and any connection or
operation that succeeds where it should not fails the task:

1. **NetworkPolicy is enforced.** Before it deploys anything, the task starts
   a canary server, proves a client reaches it, applies a deny-all policy, and
   fails loudly unless the same connection then times out.
2. **The happy path works.** The three brokers spread over three nodes, a
   `CN=admin` client creates an RF3 topic and produces a record, and the
   record survives a rolling restart.
3. **Listener protocols.** A client without a certificate is refused at the
   TLS handshake (`certificate_required`) on `krabka-bootstrap:9092`. A
   PLAINTEXT client fails.
4. **Authorization fails closed.** A CA-signed `CN=nobody` certificate with no
   ACLs gets `TOPIC_AUTHORIZATION_FAILED` from `kafka-topics --create` and from
   `kafka-console-producer`, and the topic's end offset does not move.
   `kafka-topics --list` succeeds but shows no topics: as in Kafka, a
   `Metadata` response leaves out the topics the caller cannot describe.
5. **Reachability.** Pods with every combination of the opt-in labels probe
   the brokers with `agnhost connect`. A refusal is not a pass, only a
   policy timeout:
   - only `krabka.io/client=true` pods reach 9092, through both
     `krabka-bootstrap` and the per-pod names;
   - only `krabka.io/monitor=true` pods reach 9404;
   - no non-broker pod reaches 9093 on any broker, even one with both labels.

   Every denied port except 9093 is paired with a pod that reaches it, so a
   denial cannot pass just because the port is dead. The running quorum shows
   that brokers reach 9093.

Run it locally against a cluster made from the same config:

```sh
kind create cluster --name krabka --config .github/kind.yaml
aspect kind-smoke
```

## Compatibility and rollout

This is a breaking security change for the old plaintext example. Existing
clients must switch to SSL/mTLS and receive ACLs before rollout. Do not roll the
new StatefulSet over an existing plaintext quorum one broker at a time: the
controller protocol changes cluster-wide. Schedule a maintenance migration (or
create a secured replacement cluster and replicate data), provision the Secret
and ACL bootstrap identity first, then deploy all quorum members with this
configuration. NetworkPolicy enforcement requires a CNI that implements the
Kubernetes NetworkPolicy API.

The image digest is deliberately immutable. Update it only to a verified,
signed release digest and update both image fields together.
