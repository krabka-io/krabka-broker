#!/usr/bin/env bash
set -euo pipefail

mode=${1:-full}
cluster=${KIND_CLUSTER_NAME:-krabka-performance}
namespace=broker-performance
root=$(git rev-parse --show-toplevel)
artifact_dir=${PERF_ARTIFACT_DIR:-${root}/performance-artifacts}
runs=${PERF_RUNS:-3}
saturation_records=${PERF_SATURATION_RECORDS:-100000}
steady_records=${PERF_STEADY_RECORDS:-300000}
steady_rate=${PERF_STEADY_RATE:-5000}
message_bytes=${PERF_MESSAGE_BYTES:-1024}
scale_records=${PERF_SCALE_RECORDS:-600000}
scale_rate=${PERF_SCALE_RATE:-1000}
scale_deadline=${PERF_SCALE_DEADLINE:-30m}

if [[ ${mode} == smoke ]]; then
  runs=1
  saturation_records=2000
  steady_records=2000
  steady_rate=1000
  scale_records=4000
  scale_rate=500
  tiers=(30 100)
elif [[ ${mode} == full ]]; then
  tiers=(1000 10000)
else
  echo "usage: $0 [full|smoke]" >&2
  exit 2
fi

for command in awk bazel docker git javac jq kind kubectl sed timeout; do
  command -v "${command}" >/dev/null || { echo "missing command: ${command}" >&2; exit 2; }
done
if [[ $(</proc/sys/fs/inotify/max_user_instances) -lt 512 ]]; then
  echo "fs.inotify.max_user_instances must be at least 512 for the five-node Kind cluster" >&2
  exit 2
fi

mkdir -p "${artifact_dir}"
exec > >(tee "${artifact_dir}/qualification.log") 2>&1

git status --short --untracked-files=all >"${artifact_dir}/source-status.txt"
git diff --binary HEAD >"${artifact_dir}/source.patch"
while IFS= read -r path; do
  git diff --binary --no-index /dev/null "${path}" >>"${artifact_dir}/source.patch" || [[ $? == 1 ]]
done < <(git ls-files --others --exclude-standard)
sha256sum "${artifact_dir}/source.patch" >"${artifact_dir}/source.patch.sha256"

cleanup() {
  if [[ -n ${build_dir:-} && -d ${build_dir} ]]; then
    rm -rf -- "${build_dir}"
  fi
  if [[ ${PERF_KEEP_CLUSTER:-0} != 1 ]]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cat >"${artifact_dir}/settings.env" <<EOF
mode=${mode}
runs=${runs}
saturation_records=${saturation_records}
steady_records=${steady_records}
steady_rate=${steady_rate}
message_bytes=${message_bytes}
scale_tiers=${tiers[*]}
scale_records=${scale_records}
scale_rate=${scale_rate}
scale_deadline=${scale_deadline}
krabka_commit=$(git rev-parse HEAD)
kafka_image=apache/kafka:4.0.0
broker_cpu_limit=2
broker_memory_limit=2Gi
broker_disk_request=100Gi
replication_factor=3
min_insync_replicas=2
acks=all
compression=lz4
batch_size=65536
linger_ms=5
EOF
uname -a >"${artifact_dir}/uname.txt"
lscpu >"${artifact_dir}/lscpu.txt"
free -b >"${artifact_dir}/memory.txt"
df -B1 "${root}" >"${artifact_dir}/disk.txt"
docker version >"${artifact_dir}/docker-version.txt"
kubectl version --client >"${artifact_dir}/kubectl-version.txt"

kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
kind create cluster --name "${cluster}" --config "${root}/packaging/performance/kind.yaml"
kubectl create namespace "${namespace}"

build_dir=$(mktemp -d)
client_container=$(docker create apache/kafka:4.0.0)
docker cp "${client_container}:/opt/kafka/libs/." "${build_dir}/libs"
docker rm "${client_container}" >/dev/null
mkdir -p "${build_dir}/classes"
javac --release 21 -cp "${build_dir}/libs/*" -d "${build_dir}/classes" \
  "${root}/packaging/performance/BrokerPerformanceWorkload.java"

bazel run //packaging:image_load
kind load docker-image --name "${cluster}" docker.io/krabka-io/krabka-broker:dev
docker image inspect apache/kafka:4.0.0 docker.io/krabka-io/krabka-broker:dev \
  >"${artifact_dir}/image-inspect.json"

reset_namespace() {
  kubectl delete namespace "${namespace}" --wait=true >/dev/null 2>&1 || true
  kubectl create namespace "${namespace}"
}

start_tools() {
  kubectl -n "${namespace}" run kafka-tools --image=apache/kafka:4.0.0 --restart=Never \
    --command -- sleep 86400
  kubectl -n "${namespace}" wait --for=condition=Ready pod/kafka-tools --timeout=3m
  kubectl -n "${namespace}" cp "${build_dir}/classes/." kafka-tools:/tmp/performance-classes
}

wait_bootstrap() {
  local bootstrap=$1
  for _ in $(seq 1 60); do
    if kubectl -n "${namespace}" exec kafka-tools -- \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${bootstrap}" --list \
        >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  echo "bootstrap server did not become ready: ${bootstrap}" >&2
  return 1
}

start_kafka() {
  kubectl -n "${namespace}" apply -f "${root}/packaging/performance/kafka.yaml"
  kubectl -n "${namespace}" rollout status statefulset/kafka --timeout=10m
  start_tools
  wait_bootstrap kafka-bootstrap:9092
}

start_krabka() {
  local manifest
  manifest=$(mktemp)
  sed '/accessModes: \["ReadWriteOnce"\]/a\        storageClassName: standard' \
    "${root}/packaging/k8s/statefulset.yaml" >"${manifest}"
  kubectl -n "${namespace}" apply -f "${root}/packaging/k8s/service.yaml"
  kubectl -n "${namespace}" apply -f "${root}/packaging/k8s/poddisruptionbudget.yaml"
  kubectl -n "${namespace}" apply -f "${manifest}"
  rm -f "${manifest}"
  kubectl -n "${namespace}" patch statefulset krabka --type=strategic --patch-file /dev/stdin <<'EOF'
spec:
  template:
    metadata:
      labels:
        performance.krabka.io/broker: "true"
    spec:
      shareProcessNamespace: true
      containers:
        - name: probe
          image: busybox:1.37.0
          command: [sh, -c, "trap : TERM INT; sleep infinity & wait"]
          resources:
            requests: {cpu: 5m, memory: 8Mi}
            limits: {cpu: 20m, memory: 32Mi}
          volumeMounts:
            - {name: data, mountPath: /data, readOnly: true}
EOF
  kubectl -n "${namespace}" rollout status statefulset/krabka --timeout=10m
  start_tools
  wait_bootstrap krabka-bootstrap:9092
}

create_topic() {
  local bootstrap=$1 topic=$2 partitions=$3
  local output
  for _ in $(seq 1 60); do
    if output=$(kubectl -n "${namespace}" exec kafka-tools -- \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${bootstrap}" \
        --create --topic "${topic}" --partitions "${partitions}" \
        --replication-factor 3 --config min.insync.replicas=2 2>&1); then
      echo "${output}"
      return
    fi
    sleep 2
  done
  echo "${output}" >&2
  return 1
}

snapshot() {
  local selector=$1 process=$2 destination=$3 write_mode=${4:-truncate}
  if [[ ${write_mode} == truncate ]]; then
    : >"${destination}"
  fi
  while read -r pod; do
    kubectl -n "${namespace}" exec "${pod}" -c probe -- sh -c '
      pid=$(pidof '"${process}"' | awk "{print \$1}")
      test -n "${pid}"
      echo pod='"${pod}"'
      awk "{print \"cpu_ticks=\" \$14 + \$15}" "/proc/${pid}/stat"
      awk "/^VmRSS:/ {print \"rss_kib=\" \$2}" "/proc/${pid}/status"
      echo fd_count=$(ls "/proc/${pid}/fd" 2>/dev/null | wc -l)
      awk -F "[: ]+" "NR > 2 && \$2 != \"lo\" {rx += \$3; tx += \$11} END {print \"network_rx_bytes=\" rx; print \"network_tx_bytes=\" tx}" "/proc/${pid}/net/dev"
      echo disk_bytes=$(du -sb /data | awk "{print \$1}")
    ' >>"${destination}"
  done < <(kubectl -n "${namespace}" get pods -l "${selector}" -o name | sed 's|pod/||' | sort)
}

run_throughput() {
  local side=$1 bootstrap=$2 run=$3 shape=$4 records=$5 throughput=$6
  local topic="perf-${side}-${shape}-${run}"
  local out="${artifact_dir}/${side}/run-${run}/${shape}"
  mkdir -p "${out}"
  create_topic "${bootstrap}" "${topic}" 12
  snapshot "${side_selector}" "${side_process}" "${out}/resources-before.txt"
  kubectl -n "${namespace}" exec kafka-tools -- java \
    -cp '/tmp/performance-classes:/opt/kafka/libs/*' BrokerPerformanceWorkload \
    "${bootstrap}" "${topic}" "perf-${side}-${shape}-${run}" "${records}" \
    "${message_bytes}" "${throughput}" 300 >"${out}/workload.json" 2>"${out}/workload.log"
  jq -e ".sent == ${records} and .consumed == ${records} and .duplicates == 0 and .errors == 0" \
    "${out}/workload.json" >/dev/null
  snapshot "${side_selector}" "${side_process}" "${out}/resources-after.txt"
}

run_comparison_side() {
  local side=$1 bootstrap=$2
  mkdir -p "${artifact_dir}/${side}"
  kubectl -n "${namespace}" get statefulset -o yaml >"${artifact_dir}/${side}/deployment.yaml"
  for run in $(seq 1 "${runs}"); do
    run_throughput "${side}" "${bootstrap}" "${run}" saturation "${saturation_records}" -1
    run_throughput "${side}" "${bootstrap}" "${run}" steady "${steady_records}" "${steady_rate}"
  done
}

side_selector='app=kafka'
side_process=java
start_kafka
run_comparison_side kafka kafka-bootstrap:9092

reset_namespace
side_selector='app.kubernetes.io/name=krabka'
side_process=krabka-broker
start_krabka
run_comparison_side krabka krabka-bootstrap:9092

scale_tier() {
  local partitions=$1
  local topic="perf-scale-${partitions}"
  local out="${artifact_dir}/scale/${partitions}"
  mkdir -p "${out}"
  create_topic krabka-bootstrap:9092 "${topic}" "${partitions}"
  kubectl -n "${namespace}" exec kafka-tools -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server krabka-bootstrap:9092 --describe --topic "${topic}" \
    >"${out}/topic-before.txt"
  kubectl -n "${namespace}" apply -f "${root}/packaging/performance/krabka-joiner.yaml"
  kubectl -n "${namespace}" rollout status statefulset/krabka-joiner --timeout=10m
  kubectl -n "${namespace}" get statefulset -o yaml >"${out}/deployment.yaml"
  snapshot 'app.kubernetes.io/name=krabka' krabka-broker "${out}/resources-before.txt"
  snapshot 'app=krabka-joiner' krabka-broker "${out}/resources-before.txt" append

  kubectl -n "${namespace}" exec kafka-tools -- java \
    -cp '/tmp/performance-classes:/opt/kafka/libs/*' BrokerPerformanceWorkload \
    krabka-bootstrap:9092 "${topic}" "perf-scale-${partitions}" "${scale_records}" \
    "${message_bytes}" "${scale_rate}" 1800 >"${out}/workload.json" 2>"${out}/workload.log" &
  local workload_pid=$!
  sleep 5

  local controller=-1
  for id in 0 1 2; do
    kubectl -n "${namespace}" exec "krabka-${id}" -c probe -- \
      wget -qO /tmp/metrics-before.txt http://127.0.0.1:9404/metrics
    kubectl -n "${namespace}" cp "krabka-${id}:/tmp/metrics-before.txt" \
      "${out}/metrics-${id}-before.txt" -c probe
    if awk '$1 == "krabka_broker_active_controller" && $2 == 1 {found=1} END {exit !found}' \
      "${out}/metrics-${id}-before.txt"; then
      controller=${id}
    fi
  done
  test "${controller}" -ge 0
  local restart_start restart_end failover_end new_controller=-1
  restart_start=$(date +%s)
  kubectl -n "${namespace}" delete pod "krabka-${controller}" --wait=false
  while [[ ${new_controller} -lt 0 ]]; do
    for id in 0 1 2; do
      if [[ ${id} -ne ${controller} ]] && kubectl -n "${namespace}" exec "krabka-${id}" -c probe -- \
          wget -qO- http://127.0.0.1:9404/metrics 2>/dev/null | \
          awk '$1 == "krabka_broker_active_controller" && $2 == 1 {found=1} END {exit !found}'; then
        new_controller=${id}
      fi
    done
    [[ ${new_controller} -ge 0 ]] || sleep 1
  done
  failover_end=$(date +%s)
  kubectl -n "${namespace}" wait --for=condition=Ready "pod/krabka-${controller}" --timeout=10m
  restart_end=$(date +%s)
  echo "controller=${controller}" >"${out}/restart.txt"
  echo "new_controller=${new_controller}" >>"${out}/restart.txt"
  echo "controller_failover_seconds=$((failover_end - restart_start))" >>"${out}/restart.txt"
  echo "readiness_seconds=$((restart_end - restart_start))" >>"${out}/restart.txt"

  local move_count=${partitions}
  if [[ ${move_count} -gt 1000 ]]; then
    move_count=1000
  fi
  jq -n --arg topic "${topic}" --argjson count "${move_count}" \
    '{version:1,partitions:[range(0;$count) as $p | {topic:$topic,partition:$p,replicas:[$p%3,($p+1)%3,3]}]}' \
    >"${out}/reassignment.json"
  kubectl -n "${namespace}" cp "${out}/reassignment.json" kafka-tools:/tmp/reassignment.json
  kubectl -n "${namespace}" exec kafka-tools -- /opt/kafka/bin/kafka-reassign-partitions.sh \
    --bootstrap-server krabka-bootstrap:9092 --execute \
    --reassignment-json-file /tmp/reassignment.json >"${out}/reassignment-execute.txt"
  until kubectl -n "${namespace}" exec kafka-tools -- \
      /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server krabka-bootstrap:9092 \
      --verify --reassignment-json-file /tmp/reassignment.json \
      >"${out}/reassignment-verify.txt" 2>&1 && \
      ! grep -Eq 'still in progress|rather than' "${out}/reassignment-verify.txt"; do
    sleep 5
  done

  wait "${workload_pid}"
  jq -e ".sent == ${scale_records} and .consumed == ${scale_records} and .duplicates == 0 and .errors == 0" \
    "${out}/workload.json" >/dev/null
  local reassignment_ready_start reassignment_ready_end
  reassignment_ready_start=$(date +%s)
  if ! kubectl -n "${namespace}" wait --for=condition=Ready pod/krabka-joiner-0 --timeout=10m; then
    kubectl -n "${namespace}" get pods -o wide >"${out}/pods-after.txt"
    kubectl -n "${namespace}" exec krabka-joiner-0 -c probe -- \
      wget -qO- http://127.0.0.1:9405/readyz >"${out}/readyz-after.txt" 2>&1 || true
    kubectl -n "${namespace}" logs krabka-joiner-0 -c broker --tail=1000 \
      >"${out}/joiner-after.log" 2>&1 || true
    return 1
  fi
  reassignment_ready_end=$(date +%s)
  echo "post_reassignment_readiness_seconds=$((reassignment_ready_end - reassignment_ready_start))" \
    >>"${out}/restart.txt"
  snapshot 'app.kubernetes.io/name=krabka' krabka-broker "${out}/resources-after.txt"
  snapshot 'app=krabka-joiner' krabka-broker "${out}/resources-after.txt" append
  local pod
  id=0
  for pod in krabka-0 krabka-1 krabka-2 krabka-joiner-0; do
    local scrape_start scrape_end
    scrape_start=$(date +%s%N)
    kubectl -n "${namespace}" exec "${pod}" -c probe -- \
      wget -qO /tmp/metrics.txt http://127.0.0.1:9404/metrics
    scrape_end=$(date +%s%N)
    kubectl -n "${namespace}" cp "${pod}:/tmp/metrics.txt" \
      "${out}/metrics-${id}-after.txt" -c probe
    {
      echo "scrape_nanoseconds=$((scrape_end - scrape_start))"
      echo "scrape_bytes=$(wc -c <"${out}/metrics-${id}-after.txt")"
      echo "scrape_series=$(grep -vc '^#' "${out}/metrics-${id}-after.txt")"
      grep '^krabka_broker_metadata_lag_records ' "${out}/metrics-${id}-after.txt"
      grep '^krabka_broker_partitions_total ' "${out}/metrics-${id}-after.txt"
      grep '^krabka_broker_active_controller ' "${out}/metrics-${id}-after.txt"
    } >"${out}/scrape-${id}.txt"
    id=$((id + 1))
  done
}

mkdir -p "${artifact_dir}/scale"
passed_tier=none
export root artifact_dir namespace scale_records message_bytes scale_rate
export -f create_topic snapshot scale_tier
for tier in "${tiers[@]}"; do
  reset_namespace
  side_selector='app.kubernetes.io/name=krabka'
  side_process=krabka-broker
  start_krabka
  if timeout "${scale_deadline}" bash -euo pipefail -c 'scale_tier "$1"' _ "${tier}"; then
    echo pass >"${artifact_dir}/scale/${tier}/status.txt"
    passed_tier=${tier}
  else
    mkdir -p "${artifact_dir}/scale/${tier}"
    echo fail >"${artifact_dir}/scale/${tier}/status.txt"
    break
  fi
done
echo "highest_passing_tier=${passed_tier}" >"${artifact_dir}/scale/verdict.txt"
if [[ ${passed_tier} == none ]]; then
  exit 1
fi
