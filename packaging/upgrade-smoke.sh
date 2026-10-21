#!/usr/bin/env bash
set -euo pipefail

previous=$(git describe --tags --abbrev=0 --match 'v*')
old_image=ghcr.io/krabka-io/krabka-broker:${previous}
head_image=docker.io/krabka-io/krabka-broker:dev
suffix="${GITHUB_RUN_ID:-$$}"
network=krabka-upgrade-${suffix}
volume=krabka-upgrade-${suffix}
old_container=krabka-upgrade-old-${suffix}
head_container=krabka-upgrade-head-${suffix}
tools_container=krabka-upgrade-tools-${suffix}
probe_container=krabka-upgrade-probe-${suffix}
cluster_id=3d70a78c-bb3a-4af5-a767-51e31f403c62
head_log=$(mktemp)
topic_tool=$(mktemp -d)
old_config=$(mktemp)

cat >"${old_config}" <<'TOML'
broker_id = 1
log_dir = "/var/lib/krabka"
rack = "zone-a"
controller_quorum_voters = ["1@krabka-upgrade-old:9093"]

[[listeners]]
name = "PLAINTEXT"
bind_addr = "0.0.0.0:9092"
advertised = "krabka-upgrade-old:9092"
protocol = "Plaintext"

# Kafka's offsets.topic.replication.factor defaults to 3, which one broker
# cannot place: FindCoordinator would answer COORDINATOR_NOT_AVAILABLE and the
# console consumers would time out with nothing read.
[runtime]
offsets_topic_replication_factor = 1

[remote_storage]
storage_dir = "/var/lib/krabka/objects"

[remote_storage.kafka_metadata]
bootstrap = "krabka-upgrade-old:9092"
num_partitions = 1
replication = 1
TOML
chmod 0644 "${old_config}"

# Every step after the image build has a time limit, so a command that blocks
# fails the job with a name and the diagnostics below, instead of running
# silently into the job's own limit (issue #1508).
STEP_TIMEOUT=${UPGRADE_SMOKE_STEP_TIMEOUT:-120}
PROBE_TIMEOUT=${UPGRADE_SMOKE_PROBE_TIMEOUT:-20}
# A step that pulls an image gets longer.
PULL_TIMEOUT=${UPGRADE_SMOKE_PULL_TIMEOUT:-600}

# One line per step on stderr: UTC time, free space on the root filesystem,
# and the step's name.
step() {
  local free
  free=$(df -Ph / 2>/dev/null | awk 'NR == 2 { print $4 }')
  printf 'upgrade-smoke: %s [%s free] %s\n' "$(date -u +%H:%M:%S)" "${free:-?}" "$*" >&2
}

# Run a command with the step time limit. A command that runs out of time
# fails the script with exit status 124 and says which command it was.
bounded() {
  local status=0
  timeout --kill-after=10s "${STEP_TIMEOUT}" "$@" || status=$?
  if [ "${status}" -eq 124 ] || [ "${status}" -eq 137 ]; then
    echo "upgrade-smoke: timed out after ${STEP_TIMEOUT}s: $*" >&2
  fi
  return "${status}"
}

# What a failed run leaves to diagnose it. Each command has its own limit,
# because a stalled Docker daemon is one of the things it has to show.
diagnose() {
  echo "upgrade-smoke: diagnostics after a failure" >&2
  df -h >&2 || true
  timeout 30 docker system df >&2 || echo "docker system df did not answer" >&2
  timeout 30 docker ps -a >&2 || echo "docker ps did not answer" >&2
  for container in "${old_container}" "${head_container}" "${probe_container}"; do
    if timeout 30 docker inspect "${container}" >/dev/null 2>&1; then
      echo "upgrade-smoke: last log lines of ${container}" >&2
      timeout 30 docker logs --tail 200 "${container}" >&2 || true
    fi
  done
  timeout 60 docker run --rm -v "${volume}:/data" alpine:3.22 sh -c 'du -sh /data /data/*' >&2 ||
    echo "the volume ${volume} could not be measured" >&2
}

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    diagnose
  fi
  docker rm -f "${old_container}" "${head_container}" "${tools_container}" "${probe_container}" >/dev/null 2>&1 || true
  docker volume rm "${volume}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
  rm -f "${head_log}"
  rm -f "${old_config}"
  rm -rf "${topic_tool}"
}
trap cleanup EXIT

step "build the HEAD image"
bazel run -c opt //packaging:image_load
step "pull ${old_image}"
STEP_TIMEOUT=${PULL_TIMEOUT} bounded docker pull "${old_image}"
bounded docker network create "${network}"
bounded docker volume create "${volume}"
STEP_TIMEOUT=${PULL_TIMEOUT} bounded docker run --rm --user 0 -v "${volume}:/data" alpine:3.22 chown 65532:65532 /data
step "format the volume with ${previous}"
# Format with the release being upgraded so its broker owns the original layout.
bounded docker run --rm -v "${volume}:/var/lib/krabka" \
  --entrypoint /usr/bin/krabka-format "${old_image}" \
  --log-dir /var/lib/krabka --cluster-id "${cluster_id}" --standalone \
  --node-id 1 --controller-listener krabka-upgrade-old:9093
bounded docker run --rm --user 0 -v "${volume}:/data" alpine:3.22 sh -c \
  'mkdir /data/objects && chown 65532:65532 /data/objects'
step "start ${previous}"
bounded docker run -d --name "${old_container}" --network "${network}" \
  --network-alias krabka-upgrade-old \
  -v "${volume}:/var/lib/krabka" -e KRABKA_CLUSTER_ID="${cluster_id}" \
  -v "${old_config}:/etc/krabka.toml:ro" "${old_image}" \
  --config-file /etc/krabka.toml \
  --metadata-snapshot-interval-records 1 \
  --diskless-wal-local-replica-count 1 --diskless-wal-flush-interval 100ms
step "start the Kafka tools container"
STEP_TIMEOUT=${PULL_TIMEOUT} bounded docker run -d --name "${tools_container}" --network "${network}" \
  apache/kafka:4.0.0 sleep 3600
bounded docker cp "${tools_container}:/opt/kafka/libs/kafka-clients-4.0.0.jar" "${topic_tool}/"
bounded javac --release 17 -cp "${topic_tool}/kafka-clients-4.0.0.jar" \
  -d "${topic_tool}" packaging/CreateTopic.java
bounded docker cp "${topic_tool}/CreateTopic.class" "${tools_container}:/tmp/CreateTopic.class"

step "wait for ${previous} to serve"
old_ready=false
for _ in $(seq 1 60); do
  if [ "$(bounded docker inspect --format '{{.State.Running}}' "${old_container}")" != true ]; then
    break
  fi
  if timeout "${PROBE_TIMEOUT}" docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server krabka-upgrade-old:9092 --list >/dev/null 2>&1; then
    old_ready=true
    break
  fi
  sleep 1
done
if [ "${old_ready}" != true ]; then
  echo "Previous release ${previous} did not become ready" >&2
  exit 1
fi
step "write a record and a group offset with ${previous}"
bounded docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server krabka-upgrade-old:9092 --create --topic upgrade-smoke \
  --partitions 1 --replication-factor 1
printf 'survives-upgrade\n' | bounded docker exec -i "${tools_container}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-smoke
# The console consumer exits 0 when it times out, so check what it read.
consumed=$(bounded docker exec "${tools_container}" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-smoke \
  --group upgrade-smoke --from-beginning --max-messages 1 --timeout-ms 30000)
test "${consumed}" = survives-upgrade
step "write a diskless record with ${previous}"
bounded docker exec "${tools_container}" java -cp '/tmp:/opt/kafka/libs/*' CreateTopic \
  krabka-upgrade-old:9092 upgrade-diskless krabka.diskless true
printf 'diskless-before-upgrade\n' | bounded docker exec -i "${tools_container}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-diskless
# DeleteRecords must make the release write its own log-start checkpoint.
step "raise a log start with DeleteRecords on ${previous}"
bounded docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server krabka-upgrade-old:9092 --create --topic upgrade-retention \
  --partitions 1 --replication-factor 1
printf 'deleted\nretained\n' | bounded docker exec -i "${tools_container}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-retention
bounded docker exec -i "${tools_container}" sh -c 'cat >/tmp/delete-records.json' <<'JSON'
{"partitions":[{"topic":"upgrade-retention","partition":0,"offset":1}],"version":1}
JSON
bounded docker exec "${tools_container}" /opt/kafka/bin/kafka-delete-records.sh \
  --bootstrap-server krabka-upgrade-old:9092 --offset-json-file /tmp/delete-records.json
step "wait for ${previous} to write its snapshot, WAL index and log-start checkpoint"
surfaces_ready=false
for attempt in $(seq 1 30); do
  # A probe that does not answer is removed by name, so the next one can start.
  if timeout --kill-after=5s "${PROBE_TIMEOUT}" docker run --rm --name "${probe_container}" \
      -v "${volume}:/data" alpine:3.22 sh -c '
      test -n "$(find /data/__cluster_metadata-0 -name "*.checkpoint" ! -name "00000000000000000000-0000000000.checkpoint" -print -quit)" &&
      test -n "$(find /data -path "*/__diskless_wal_index-*" -print -quit)" &&
      test -n "$(find /data -path "*/upgrade-retention-0/log-start-offset-checkpoint" -print -quit)"
    '; then
    surfaces_ready=true
    break
  fi
  timeout 30 docker rm -f "${probe_container}" >/dev/null 2>&1 || true
  step "probe ${attempt} of 30 did not find every file"
  sleep 1
done
test "${surfaces_ready}" = true
step "stop ${previous}"
bounded docker stop "${old_container}" >/dev/null
bounded docker rm "${old_container}" >/dev/null

step "read the changelog since ${previous}"
# A release that breaks the on-disk format says so with this marker, as
# docs/releasing.md asks. Only the marker counts: prose such as "the on-disk
# format does not change" is no declaration, and reading it as one ran HEAD in
# the foreground on a directory it opens normally (issue #1508). A marker
# already present in the previous release is not a new break.
break_marker='**Breaking, on-disk format.**'
changes=$(bounded git diff --unified=0 "${previous}" -- CHANGELOG.md | sed -n 's/^+//p')
if grep -Fq "${break_marker}" <<<"${changes}"; then
  step "check that HEAD refuses the declared format break"
  head_status=0
  bounded docker run --name "${head_container}" --network "${network}" \
    --network-alias krabka-upgrade-old \
    -v "${volume}:/var/lib/krabka" -e KRABKA_CLUSTER_ID="${cluster_id}" \
    -v "${old_config}:/etc/krabka.toml:ro" "${head_image}" \
    --config-file /etc/krabka.toml >"${head_log}" 2>&1 || head_status=$?
  if [ "${head_status}" -eq 0 ]; then
    echo "HEAD opened a directory despite the declared format break" >&2
    exit 1
  fi
  if [ "${head_status}" -eq 124 ] || [ "${head_status}" -eq 137 ]; then
    echo "HEAD kept serving the directory ${previous} wrote, although the changelog declares an on-disk format break" >&2
    exit 1
  fi
  grep -Eq 'No readable meta.properties files found|Unsupported meta.properties version|INCONSISTENT_CLUSTER_ID' "${head_log}"
else
  step "start HEAD on the volume ${previous} wrote"
  bounded docker run -d --name "${head_container}" --network "${network}" \
    --network-alias krabka-upgrade-head \
    --network-alias krabka-upgrade-old \
    -v "${volume}:/var/lib/krabka" -e KRABKA_CLUSTER_ID="${cluster_id}" \
    -v "${old_config}:/etc/krabka.toml:ro" "${head_image}" \
    --config-file /etc/krabka.toml
  head_ready=false
  for _ in $(seq 1 60); do
    if [ "$(bounded docker inspect --format '{{.State.Running}}' "${head_container}")" != true ]; then
      break
    fi
    if timeout "${PROBE_TIMEOUT}" docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server krabka-upgrade-head:9092 --list >/dev/null 2>&1; then
      head_ready=true
      break
    fi
    sleep 1
  done
  if [ "${head_ready}" != true ]; then
    echo "HEAD did not become ready after upgrading ${previous}" >&2
    exit 1
  fi
  step "read back with HEAD"
  consumed=$(bounded docker exec "${tools_container}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server krabka-upgrade-head:9092 --topic upgrade-smoke \
    --from-beginning --max-messages 1 --timeout-ms 30000)
  test "${consumed}" = survives-upgrade
  floor=$(bounded docker exec "${tools_container}" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server krabka-upgrade-head:9092 --topic upgrade-retention --time -2)
  test "${floor}" = upgrade-retention:0:1
fi
step "passed"
