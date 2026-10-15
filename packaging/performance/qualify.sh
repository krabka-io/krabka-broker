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
contract=${PERF_CONTRACT:-${root}/packaging/performance/regression-contract.json}

usage() {
  echo "usage: $0 [full|smoke|budget <before_after|pinned_kafka> [reference-revision]]" >&2
  exit 2
}

if [[ ${mode} == budget ]]; then
  # A predeclared regression contract: interleaved reference and candidate
  # trials, judged afterwards by `krabka-parse-benches evaluate`.
  budget_mode=${2:-}
  case ${budget_mode} in
    before_after)
      [[ $# -eq 3 ]] || usage
      reference_revision=$(git rev-parse --verify --quiet "$3^{commit}") || {
        echo "not a commit: $3" >&2
        exit 2
      }
      ;;
    pinned_kafka)
      [[ $# -eq 2 ]] || usage
      ;;
    *)
      usage
      ;;
  esac
elif [[ ${mode} == smoke ]]; then
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
  usage
fi

for command in awk bazel docker getconf git javac jq kind kubectl nproc realpath sed sha256sum \
    timeout; do
  command -v "${command}" >/dev/null || { echo "missing command: ${command}" >&2; exit 2; }
done
if [[ $(</proc/sys/fs/inotify/max_user_instances) -lt 512 ]]; then
  echo "fs.inotify.max_user_instances must be at least 512 for the five-node Kind cluster" >&2
  exit 2
fi
if [[ ${mode} == budget ]]; then
  # `bazel run` executes in its runfiles directory, so the paths handed to
  # krabka-parse-benches must be absolute.
  artifact_dir=$(realpath -m -- "${artifact_dir}")
  contract=$(realpath -e -- "${contract}") || { echo "missing contract: ${contract}" >&2; exit 2; }
fi

if [[ -e ${artifact_dir} && ! -d ${artifact_dir} ]] \
  || [[ -d ${artifact_dir} && -n $(find "${artifact_dir}" -mindepth 1 -print -quit) ]]; then
  echo "artifact directory must not exist or must be empty: ${artifact_dir}" >&2
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

remove_reference_worktree() {
  if [[ -n ${reference_parent:-} && -d ${reference_parent} ]]; then
    if [[ -d ${reference_parent}/reference ]]; then
      (cd "${reference_parent}/reference" && bazel shutdown) >/dev/null 2>&1 || true
    fi
    git -C "${root}" worktree remove --force "${reference_parent}/reference" >/dev/null 2>&1 || true
    rm -rf -- "${reference_parent}"
    git -C "${root}" worktree prune || true
  fi
  reference_parent=
}

cleanup() {
  if [[ -n ${build_dir:-} && -d ${build_dir} ]]; then
    rm -rf -- "${build_dir}"
  fi
  remove_reference_worktree
  if [[ ${PERF_KEEP_CLUSTER:-0} != 1 ]]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

reset_namespace() {
  kubectl delete namespace "${namespace}" --wait=true >/dev/null 2>&1 || true
  kubectl create namespace "${namespace}"
}

start_tools() {
  local image=${1:-apache/kafka:4.0.0}
  kubectl -n "${namespace}" run kafka-tools --image="${image}" --restart=Never \
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
  local image=${1:-apache/kafka:4.0.0} client_image=${2:-apache/kafka:4.0.0}
  local manifest
  manifest=$(mktemp)
  sed "s|image: apache/kafka:4.0.0|image: ${image}|" \
    "${root}/packaging/performance/kafka.yaml" >"${manifest}"
  kubectl -n "${namespace}" apply -f "${manifest}"
  rm -f "${manifest}"
  kubectl -n "${namespace}" rollout status statefulset/kafka --timeout=10m
  start_tools "${client_image}"
  wait_bootstrap kafka-bootstrap:9092
}

start_krabka() {
  local image=${1:-docker.io/krabka-io/krabka-broker:dev} client_image=${2:-apache/kafka:4.0.0}
  local manifest
  manifest=$(mktemp)
  sed -e '/accessModes: \["ReadWriteOnce"\]/a\        storageClassName: standard' \
    -e "s|docker.io/krabka-io/krabka-broker:dev|${image}|g" \
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
  start_tools "${client_image}"
  wait_bootstrap krabka-bootstrap:9092
}

create_topic() {
  local bootstrap=$1 topic=$2 partitions=$3 replication_factor=${4:-3} min_insync_replicas=${5:-2}
  local output
  for _ in $(seq 1 60); do
    if output=$(kubectl -n "${namespace}" exec kafka-tools -- \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${bootstrap}" \
        --create --topic "${topic}" --partitions "${partitions}" \
        --replication-factor "${replication_factor}" \
        --config "min.insync.replicas=${min_insync_replicas}" 2>&1); then
      echo "${output}"
      return
    fi
    sleep 2
  done
  echo "${output}" >&2
  return 1
}

# Each pod's block is all or nothing: the probe script stops at the first
# missing value, and a failed exec fails the snapshot.
snapshot() {
  local selector=$1 process=$2 destination=$3 write_mode=${4:-truncate}
  if [[ ${write_mode} == truncate ]]; then
    : >"${destination}"
  fi
  while read -r pod; do
    kubectl -n "${namespace}" exec "${pod}" -c probe -- sh -c '
      set -e
      pid=$(pidof '"${process}"' | awk "{print \$1}")
      test -n "${pid}"
      echo pod='"${pod}"'
      awk "{print \"cpu_ticks=\" \$14 + \$15; print \"process_start_ticks=\" \$22}" "/proc/${pid}/stat"
      awk "/^VmRSS:/ {print \"rss_kib=\" \$2}" "/proc/${pid}/status"
      awk "/^VmHWM:/ {print \"rss_hwm_kib=\" \$2}" "/proc/${pid}/status"
      echo fd_count=$(ls "/proc/${pid}/fd" 2>/dev/null | wc -l)
      awk -F "[: ]+" "NR > 2 && \$2 != \"lo\" {rx += \$3; tx += \$11} END {print \"network_rx_bytes=\" rx; print \"network_tx_bytes=\" tx}" "/proc/${pid}/net/dev"
      echo disk_bytes=$(du -sb /data | awk "{print \$1}")
    ' >>"${destination}" || return 1
  done < <(kubectl -n "${namespace}" get pods -l "${selector}" -o name | sed 's|pod/||' | sort)
}

uname -a >"${artifact_dir}/uname.txt"
lscpu >"${artifact_dir}/lscpu.txt"
free -b >"${artifact_dir}/memory.txt"
df -B1 "${root}" >"${artifact_dir}/disk.txt"
docker version >"${artifact_dir}/docker-version.txt"
kubectl version --client >"${artifact_dir}/kubectl-version.txt"

kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
kind create cluster --name "${cluster}" --config "${root}/packaging/performance/kind.yaml"
kubectl create namespace "${namespace}"

# Print `key=value`, refusing what krabka-parse-benches could not read back.
env_line() {
  if [[ $1 == *=* || $1 == *$'\n'* || $2 == *$'\n'* ]]; then
    echo "cannot record ${1}: the key holds '=' or a newline, or the value a newline" >&2
    return 1
  fi
  printf '%s=%s\n' "$1" "$2"
}

# A snapshot that failed, or that lacks a complete block for every broker, is
# removed: krabka-parse-benches then records the trial as failed instead of
# rejecting the whole artifact directory.
budget_snapshot() {
  local selector=$1 process=$2 destination=$3
  if ! snapshot "${selector}" "${process}" "${destination}" \
      || ! awk -F= -v brokers="${brokers}" '{seen[$1]++}
          END {exit !(seen["pod"] == brokers && seen["cpu_ticks"] == brokers &&
            seen["process_start_ticks"] == brokers && seen["rss_hwm_kib"] == brokers)}' "${destination}"; then
    echo "resource snapshot failed: ${destination}" >&2
    rm -f -- "${destination}"
  fi
}

# One trial of one cell. Nothing in here aborts the run: a trial that fails
# leaves no workload.json, and its workload.log ends with the reason.
budget_trial() {
  local round=$1 side=$2 side_dir=$3 bootstrap=$4 selector=$5 process=$6 cell=$7
  local cell_id partitions records record_bytes rate timeout_seconds compression payload \
    idle_seconds
  IFS=$'\t' read -r cell_id partitions records record_bytes rate timeout_seconds compression \
    payload idle_seconds <<<"${cell}"
  local out=${side_dir}/${cell_id}
  local topic="budget-r${round}-${side}-${cell_id}"
  mkdir -p "${out}"
  echo "${order}" >"${out}/order"
  order=$((order + 1))
  echo "trial: round ${round}, ${side}, ${cell_id}"
  if ! create_topic "${bootstrap}" "${topic}" "${partitions}" "${replication_factor}" \
      "${min_insync_replicas}" >"${out}/topic.txt" 2>&1; then
    echo "could not create topic ${topic}" >>"${out}/workload.log"
    return 0
  fi
  budget_snapshot "${selector}" "${process}" "${out}/resources-before.txt"
  local guard=$((timeout_seconds + idle_seconds + 300)) status=0
  timeout "${guard}" kubectl -n "${namespace}" exec kafka-tools -- java \
    -cp '/tmp/performance-classes:/opt/kafka/libs/*' BrokerPerformanceWorkload \
    "${bootstrap}" "${topic}" "${topic}" "${records}" "${record_bytes}" "${rate}" \
    "${timeout_seconds}" "${compression}" "${payload}" "${idle_seconds}" \
    >"${out}/workload.json" 2>"${out}/workload.log" || status=$?
  if [[ ${status} -ne 0 ]]; then
    rm -f -- "${out}/workload.json"
    if [[ ${status} -eq 124 ]]; then
      echo "the workload did not exit within ${guard} seconds" >>"${out}/workload.log"
    fi
  fi
  budget_snapshot "${selector}" "${process}" "${out}/resources-after.txt"
}

# One side of one round: a fresh cluster, then every cell in contract order.
budget_side() {
  local round=$1 side=$2
  local side_dir=${artifact_dir}/trials/round-${round}/${side}
  local image bootstrap selector process
  reset_namespace
  if [[ ${side} == reference && ${budget_mode} == pinned_kafka ]]; then
    bootstrap=kafka-bootstrap:9092
    selector='app=kafka'
    process=java
    start_kafka "${kafka_tag}" "${kafka_tag}"
  else
    image=${candidate_image}
    if [[ ${side} == reference ]]; then
      image=${reference_image}
    fi
    bootstrap=krabka-bootstrap:9092
    selector='app.kubernetes.io/name=krabka'
    process=krabka-broker
    start_krabka "${image}" "${kafka_tag}"
  fi
  mkdir -p "${side_dir}"
  kubectl -n "${namespace}" get statefulset -o yaml >"${side_dir}/deployment.yaml"
  local cell
  for cell in "${cells[@]}"; do
    budget_trial "${round}" "${side}" "${side_dir}" "${bootstrap}" "${selector}" "${process}" \
      "${cell}"
  done
}

if [[ ${mode} == budget ]]; then
  # The contract is fixed before the first trial: its digest goes into the
  # provenance, and evaluation refuses a contract whose bytes changed since.
  cp -- "${contract}" "${artifact_dir}/contract.json"
  (cd "${artifact_dir}" && sha256sum contract.json >contract.sha256)
  contract_sha256=$(awk '{print $1}' "${artifact_dir}/contract.sha256")
  bazel run //crates/parse-benches:krabka-parse-benches -- \
    check-contract "${artifact_dir}/contract.json"

  run_contract=${artifact_dir}/contract.json
  rounds=$(jq -er '.rounds' "${run_contract}")
  brokers=$(jq -er '.cluster.brokers' "${run_contract}")
  replication_factor=$(jq -er '.cluster.replication_factor' "${run_contract}")
  min_insync_replicas=$(jq -er '.cluster.min_insync_replicas' "${run_contract}")
  kafka_image=$(jq -er '.kafka_image' "${run_contract}")
  mapfile -t cells < <(jq -r '.cells[] | [.id, .partitions, .records, .record_bytes,
    (.rate // -1), .timeout_seconds, .compression, .payload, .idle_seconds] | @tsv' \
    "${run_contract}")
  if [[ ${#cells[@]} -eq 0 ]]; then
    echo "the contract declares no cells" >&2
    exit 2
  fi
  if [[ ${brokers} != 3 ]]; then
    echo "the performance manifests run three brokers; the contract asks for ${brokers}" >&2
    exit 2
  fi
  # The pods name the image by tag, which `kind load` can carry; the digest
  # pin is enforced here, by pulling it and checking the tag resolves to it.
  if [[ ${kafka_image} != *@sha256:* ]]; then
    echo "kafka_image must be pinned by digest: ${kafka_image}" >&2
    exit 2
  fi
  kafka_tag=${kafka_image%@*}
  kafka_release=${kafka_tag##*:}
  docker pull "${kafka_image}"
  docker tag "${kafka_image}" "${kafka_tag}"
  kafka_image_digest=$(docker image inspect --format '{{.Id}}' "${kafka_image}")
  if [[ $(docker image inspect --format '{{.Id}}' "${kafka_tag}") != "${kafka_image_digest}" ]]; then
    echo "${kafka_tag} does not resolve to ${kafka_image}" >&2
    exit 1
  fi

  build_dir=$(mktemp -d)
  client_container=$(docker create "${kafka_tag}")
  docker cp "${client_container}:/opt/kafka/libs/." "${build_dir}/libs"
  docker rm "${client_container}" >/dev/null
  mkdir -p "${build_dir}/classes"
  javac --release 17 -cp "${build_dir}/libs/*" -d "${build_dir}/classes" \
    "${root}/packaging/performance/BrokerPerformanceWorkload.java"

  candidate_revision=$(git rev-parse HEAD)
  candidate_image=docker.io/krabka-io/krabka-broker:budget-candidate
  bazel run -c opt //packaging:image_load
  docker tag docker.io/krabka-io/krabka-broker:dev "${candidate_image}"
  candidate_image_digest=$(docker image inspect --format '{{.Id}}' "${candidate_image}")
  kind load docker-image --name "${cluster}" "${candidate_image}" "${kafka_tag}"

  if [[ ${budget_mode} == before_after ]]; then
    reference_label=krabka
    reference_image=docker.io/krabka-io/krabka-broker:budget-reference
    reference_parent=$(mktemp -d)
    git -C "${root}" worktree add --detach "${reference_parent}/reference" "${reference_revision}"
    (cd "${reference_parent}/reference" && bazel run -c opt //packaging:image_load)
    docker tag docker.io/krabka-io/krabka-broker:dev "${reference_image}"
    reference_image_digest=$(docker image inspect --format '{{.Id}}' "${reference_image}")
    kind load docker-image --name "${cluster}" "${reference_image}"
    remove_reference_worktree
    docker image inspect "${kafka_tag}" "${candidate_image}" "${reference_image}" \
      >"${artifact_dir}/image-inspect.json"
  else
    reference_label=kafka
    reference_revision=${kafka_release}
    reference_image=${kafka_image}
    reference_image_digest=${kafka_image_digest}
    docker image inspect "${kafka_tag}" "${candidate_image}" >"${artifact_dir}/image-inspect.json"
  fi
  # An idle Bazel server keeps its heap on the host the brokers share.
  bazel shutdown

  cpu_model=$(awk '/^model name/ {sub(/^[^:]*:[ \t]*/, ""); sub(/[ \t]+$/, ""); print; exit}' \
    /proc/cpuinfo)
  memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
  {
    env_line cpu_model "${cpu_model:-unknown}"
    env_line logical_cpus "$(nproc)"
    env_line memory_bytes "$((memory_kib * 1024))"
    env_line disk_available_bytes \
      "$(df -B1 --output=avail "${artifact_dir}" | awk 'NR == 2 {print $1}')"
    env_line kernel "$(uname -r)"
    env_line clock_ticks_per_second "$(getconf CLK_TCK)"
  } >"${artifact_dir}/host.env"

  {
    env_line mode "${budget_mode}"
    env_line contract_sha256 "${contract_sha256}"
    env_line reference_label "${reference_label}"
    env_line reference_revision "${reference_revision}"
    env_line reference_image "${reference_image}"
    env_line reference_image_digest "${reference_image_digest}"
    env_line candidate_label krabka
    env_line candidate_revision "${candidate_revision}"
    env_line candidate_image "${candidate_image}"
    env_line candidate_image_digest "${candidate_image_digest}"
    env_line client_image "${kafka_image}"
    env_line client_image_digest "${kafka_image_digest}"
    env_line started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    env_line source_patch_sha256 "$(awk '{print $1}' "${artifact_dir}/source.patch.sha256")"
    env_line krabka_compilation_mode opt
    env_line broker_cpu_limit 2
    env_line broker_memory_limit 2Gi
    env_line broker_disk_request 100Gi
    env_line acks all
    env_line enable_idempotence true
    env_line batch_size 65536
    env_line linger_ms 5
  } >"${artifact_dir}/provenance.env"

  # Interleaved rounds: odd rounds run the reference first, even rounds the
  # candidate, so drift over the run does not land on one side only.
  order=1
  for round in $(seq 1 "${rounds}"); do
    if ((round % 2 == 1)); then
      sides=(reference candidate)
    else
      sides=(candidate reference)
    fi
    for side in "${sides[@]}"; do
      budget_side "${round}" "${side}"
    done
  done

  bazel run //crates/parse-benches:krabka-parse-benches -- \
    assemble "${artifact_dir}" --output "${artifact_dir}/performance-record.json"
  status=0
  bazel run //crates/parse-benches:krabka-parse-benches -- \
    evaluate "${artifact_dir}/performance-record.json" --contract "${contract}" \
    --output "${artifact_dir}/verdict.json" --markdown "${artifact_dir}/performance-budget.md" \
    || status=$?
  exit "${status}"
fi

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
krabka_compilation_mode=opt
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

build_dir=$(mktemp -d)
client_container=$(docker create apache/kafka:4.0.0)
docker cp "${client_container}:/opt/kafka/libs/." "${build_dir}/libs"
docker rm "${client_container}" >/dev/null
mkdir -p "${build_dir}/classes"
javac --release 17 -cp "${build_dir}/libs/*" -d "${build_dir}/classes" \
  "${root}/packaging/performance/BrokerPerformanceWorkload.java"

bazel run -c opt //packaging:image_load
kind load docker-image --name "${cluster}" docker.io/krabka-io/krabka-broker:dev
docker image inspect apache/kafka:4.0.0 docker.io/krabka-io/krabka-broker:dev \
  >"${artifact_dir}/image-inspect.json"

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
