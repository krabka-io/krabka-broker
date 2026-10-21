#!/usr/bin/env bash
set -euo pipefail

image=docker.io/krabka-io/krabka-broker:dev
cluster=${KIND_CLUSTER_NAME:-krabka}

bazel run -c opt //packaging:image_load
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

work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

# The production manifests require operator-issued identities. The smoke test
# creates an ephemeral CA with the same subjects and SAN contract.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=krabka-smoke-ca \
  -keyout "${work}/ca.key" -out "${work}/ca.crt" >/dev/null 2>&1
for ordinal in 0 1 2; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=broker-${ordinal}" \
    -keyout "${work}/broker-${ordinal}.key" \
    -out "${work}/broker-${ordinal}.csr" >/dev/null 2>&1
  printf 'subjectAltName=DNS:krabka,DNS:krabka-%s.krabka\nextendedKeyUsage=serverAuth,clientAuth\n' \
    "${ordinal}" >"${work}/broker-${ordinal}.ext"
  openssl x509 -req -days 1 -in "${work}/broker-${ordinal}.csr" \
    -CA "${work}/ca.crt" -CAkey "${work}/ca.key" -CAcreateserial \
    -extfile "${work}/broker-${ordinal}.ext" \
    -out "${work}/broker-${ordinal}.crt" >/dev/null 2>&1
done
openssl req -newkey rsa:2048 -nodes -subj /CN=admin \
  -keyout "${work}/admin.key" -out "${work}/admin.csr" >/dev/null 2>&1
printf 'extendedKeyUsage=clientAuth\n' >"${work}/admin.ext"
openssl x509 -req -days 1 -in "${work}/admin.csr" \
  -CA "${work}/ca.crt" -CAkey "${work}/ca.key" -CAcreateserial \
  -extfile "${work}/admin.ext" -out "${work}/admin.crt" >/dev/null 2>&1
openssl pkcs12 -export -passout pass:changeit -name admin \
  -inkey "${work}/admin.key" -in "${work}/admin.crt" \
  -certfile "${work}/ca.crt" -out "${work}/admin.p12"
openssl pkcs12 -export -nokeys -passout pass:changeit -name ca \
  -in "${work}/ca.crt" -out "${work}/trust.p12"
kubectl create secret generic krabka-tls --dry-run=client -o yaml \
  --from-file="${work}/ca.crt" \
  --from-file="${work}/broker-0.crt" --from-file="${work}/broker-0.key" \
  --from-file="${work}/broker-1.crt" --from-file="${work}/broker-1.key" \
  --from-file="${work}/broker-2.crt" --from-file="${work}/broker-2.key" \
  --from-file="${work}/admin.p12" --from-file="${work}/trust.p12" | kubectl apply -f -

# Local Bazel builds have a mutable daemon-local tag; only this test replaces
# the immutable production digest.
sed -e '/accessModes: \["ReadWriteOnce"\]/a\        storageClassName: krabka-kind' \
  -e "s|ghcr.io/krabka-io/krabka-broker@sha256:[0-9a-f]*|${image}|g" \
  packaging/k8s/statefulset.yaml >"${work}/statefulset.yaml"
kubectl apply -f packaging/k8s/configmap.yaml
kubectl apply -f packaging/k8s/service.yaml
kubectl apply -f packaging/k8s/networkpolicy.yaml
kubectl apply -f packaging/k8s/poddisruptionbudget.yaml
kubectl apply -f "${work}/statefulset.yaml"
kubectl rollout status statefulset/krabka --timeout=5m

nodes=$(kubectl get pods -l app.kubernetes.io/name=krabka \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l)
test "${nodes}" -eq 3

kubectl create configmap kafka-client-config --from-literal=client.properties="security.protocol=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/krabka/tls/admin.p12
ssl.keystore.password=changeit
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/krabka/tls/trust.p12
ssl.truststore.password=changeit"
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: kafka-tools
  labels:
    krabka.io/client: "true"
spec:
  restartPolicy: Never
  containers:
    - name: kafka-tools
      image: apache/kafka:4.0.0
      command: ["sleep", "3600"]
      volumeMounts:
        - {name: tls, mountPath: /etc/krabka/tls, readOnly: true}
        - {name: config, mountPath: /etc/krabka/client, readOnly: true}
  volumes:
    - name: tls
      secret: {secretName: krabka-tls}
    - name: config
      configMap: {name: kafka-client-config}
YAML
kubectl wait --for=condition=Ready pod/kafka-tools --timeout=2m
kubectl exec kafka-tools -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server krabka-bootstrap:9092 \
  --command-config /etc/krabka/client/client.properties \
  --create --topic kind-smoke --partitions 1 --replication-factor 3
printf 'survives-restart\n' | kubectl exec -i kafka-tools -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-bootstrap:9092 --topic kind-smoke \
  --producer.config /etc/krabka/client/client.properties
consumed=$(kubectl exec kafka-tools -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server krabka-bootstrap:9092 --topic kind-smoke \
  --consumer.config /etc/krabka/client/client.properties \
  --from-beginning --max-messages 1 --timeout-ms 30000)
test "${consumed}" = survives-restart

kubectl rollout restart statefulset/krabka
kubectl rollout status statefulset/krabka --timeout=5m
consumed=$(kubectl exec kafka-tools -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server krabka-bootstrap:9092 --topic kind-smoke \
  --consumer.config /etc/krabka/client/client.properties \
  --from-beginning --max-messages 1 --timeout-ms 30000)
test "${consumed}" = survives-restart
