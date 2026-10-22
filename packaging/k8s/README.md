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
