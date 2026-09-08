#!/usr/bin/env bash
set -euo pipefail

image=docker.io/krabka-io/krabka-broker:dev
cluster=${KIND_CLUSTER_NAME:-krabka}

bazel run //packaging:image_load
kind load docker-image --name "${cluster}" "${image}"
for worker in worker worker2 worker3; do
  docker exec "${cluster}-${worker}" mkdir -p /var/local/krabka
done

kubectl apply -f - <<'YAML'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: krabka-kind}
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: v1
kind: PersistentVolume
metadata: {name: krabka-kind-1}
spec:
  capacity: {storage: 100Gi}
  accessModes: [ReadWriteOnce]
  storageClassName: krabka-kind
  local: {path: /var/local/krabka}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - {key: kubernetes.io/hostname, operator: In, values: [krabka-worker]}
---
apiVersion: v1
kind: PersistentVolume
metadata: {name: krabka-kind-2}
spec:
  capacity: {storage: 100Gi}
  accessModes: [ReadWriteOnce]
  storageClassName: krabka-kind
  local: {path: /var/local/krabka}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - {key: kubernetes.io/hostname, operator: In, values: [krabka-worker2]}
---
apiVersion: v1
kind: PersistentVolume
metadata: {name: krabka-kind-3}
spec:
  capacity: {storage: 100Gi}
  accessModes: [ReadWriteOnce]
  storageClassName: krabka-kind
  local: {path: /var/local/krabka}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - {key: kubernetes.io/hostname, operator: In, values: [krabka-worker3]}
YAML

manifest=$(mktemp)
trap 'rm -f "${manifest}"' EXIT
sed '/accessModes: \["ReadWriteOnce"\]/a\        storageClassName: krabka-kind' \
  packaging/k8s/statefulset.yaml >"${manifest}"
kubectl apply -f packaging/k8s/service.yaml
kubectl apply -f packaging/k8s/poddisruptionbudget.yaml
kubectl apply -f "${manifest}"
kubectl rollout status statefulset/krabka --timeout=5m

nodes=$(kubectl get pods -l app.kubernetes.io/name=krabka \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l)
test "${nodes}" -eq 3

kubectl run kafka-tools --image=apache/kafka:4.0.0 --restart=Never \
  --command -- sleep 3600
kubectl wait --for=condition=Ready pod/kafka-tools --timeout=2m
kubectl exec kafka-tools -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server krabka-bootstrap:9092 \
  --create --topic kind-smoke --partitions 1 --replication-factor 3
printf 'milestone-16\n' | kubectl exec -i kafka-tools -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-bootstrap:9092 --topic kind-smoke
consumed=$(kubectl exec kafka-tools -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server krabka-bootstrap:9092 --topic kind-smoke \
  --from-beginning --max-messages 1 --timeout-ms 30000)
test "${consumed}" = milestone-16

kubectl rollout restart statefulset/krabka
kubectl rollout status statefulset/krabka --timeout=5m
consumed=$(kubectl exec kafka-tools -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server krabka-bootstrap:9092 --topic kind-smoke \
  --from-beginning --max-messages 1 --timeout-ms 30000)
test "${consumed}" = milestone-16
