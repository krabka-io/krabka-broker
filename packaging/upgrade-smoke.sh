#!/usr/bin/env bash
set -euo pipefail

previous=$(git describe --tags --abbrev=0)
old_image=ghcr.io/krabka-io/krabka-broker:${previous}
head_image=docker.io/krabka-io/krabka-broker:dev
suffix="${GITHUB_RUN_ID:-$$}"
network=krabka-upgrade-${suffix}
volume=krabka-upgrade-${suffix}
old_container=krabka-upgrade-old-${suffix}
head_container=krabka-upgrade-head-${suffix}
tools_container=krabka-upgrade-tools-${suffix}
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

[remote_storage]
storage_dir = "/var/lib/krabka/objects"

[remote_storage.kafka_metadata]
bootstrap = "krabka-upgrade-old:9092"
num_partitions = 1
replication = 1
TOML
chmod 0644 "${old_config}"

cleanup() {
  docker rm -f "${old_container}" "${head_container}" "${tools_container}" >/dev/null 2>&1 || true
  docker volume rm "${volume}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
  rm -f "${head_log}"
  rm -f "${old_config}"
  rm -rf "${topic_tool}"
}
trap cleanup EXIT

bazel run -c opt //packaging:image_load
docker pull "${old_image}"
docker network create "${network}"
docker volume create "${volume}"
docker run --rm --user 0 -v "${volume}:/data" alpine:3.22 chown 65532:65532 /data
# Format with the release being upgraded so its broker owns the original layout.
docker run --rm -v "${volume}:/var/lib/krabka" \
  --entrypoint /usr/bin/krabka-format "${old_image}" \
  --log-dir /var/lib/krabka --cluster-id "${cluster_id}" --standalone \
  --node-id 1 --controller-listener krabka-upgrade-old:9093
docker run --rm --user 0 -v "${volume}:/data" alpine:3.22 sh -c \
  'mkdir /data/objects && chown 65532:65532 /data/objects'
docker run -d --name "${old_container}" --network "${network}" \
  --network-alias krabka-upgrade-old \
  -v "${volume}:/var/lib/krabka" -e KRABKA_CLUSTER_ID="${cluster_id}" \
  -v "${old_config}:/etc/krabka.toml:ro" "${old_image}" \
  --config-file /etc/krabka.toml \
  --metadata-snapshot-interval-records 1 \
  --diskless-wal-local-replica-count 1 --diskless-wal-flush-interval 100ms
docker run -d --name "${tools_container}" --network "${network}" \
  apache/kafka:4.0.0 sleep 3600
docker cp "${tools_container}:/opt/kafka/libs/kafka-clients-4.0.0.jar" "${topic_tool}/"
javac --release 17 -cp "${topic_tool}/kafka-clients-4.0.0.jar" \
  -d "${topic_tool}" packaging/CreateTopic.java
docker cp "${topic_tool}/CreateTopic.class" "${tools_container}:/tmp/CreateTopic.class"

old_ready=false
for _ in $(seq 1 60); do
  if [ "$(docker inspect --format '{{.State.Running}}' "${old_container}")" != true ]; then
    break
  fi
  if docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server krabka-upgrade-old:9092 --list >/dev/null 2>&1; then
    old_ready=true
    break
  fi
  sleep 1
done
if [ "${old_ready}" != true ]; then
  docker logs "${old_container}" >&2
  echo "Previous release ${previous} did not become ready" >&2
  exit 1
fi
docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server krabka-upgrade-old:9092 --create --topic upgrade-smoke \
  --partitions 1 --replication-factor 1
printf 'survives-upgrade\n' | docker exec -i "${tools_container}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-smoke
docker exec "${tools_container}" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-smoke \
  --group upgrade-smoke --from-beginning --max-messages 1 --timeout-ms 30000
docker exec "${tools_container}" java -cp '/tmp:/opt/kafka/libs/*' CreateTopic \
  krabka-upgrade-old:9092 upgrade-diskless krabka.diskless true
printf 'diskless-before-upgrade\n' | docker exec -i "${tools_container}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-diskless
# DeleteRecords must make the release write its own log-start checkpoint.
docker exec "${tools_container}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server krabka-upgrade-old:9092 --create --topic upgrade-retention \
  --partitions 1 --replication-factor 1
printf 'deleted\nretained\n' | docker exec -i "${tools_container}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server krabka-upgrade-old:9092 --topic upgrade-retention
docker exec -i "${tools_container}" sh -c 'cat >/tmp/delete-records.json' <<'JSON'
{"partitions":[{"topic":"upgrade-retention","partition":0,"offset":1}],"version":1}
JSON
docker exec "${tools_container}" /opt/kafka/bin/kafka-delete-records.sh \
  --bootstrap-server krabka-upgrade-old:9092 --offset-json-file /tmp/delete-records.json
surfaces_ready=false
for _ in $(seq 1 30); do
  if docker run --rm -v "${volume}:/data" alpine:3.22 sh -c '
      test -n "$(find /data/__cluster_metadata/@metadata-0 -name "*.checkpoint" ! -name "00000000000000000000-0000000000.checkpoint" -print -quit)" &&
      test -n "$(find /data -path "*/__diskless_wal_index-*" -print -quit)" &&
      test -n "$(find /data -path "*/upgrade-retention-0/log-start-offset-checkpoint" -print -quit)"
    '; then
    surfaces_ready=true
    break
  fi
  sleep 1
done
test "${surfaces_ready}" = true
docker stop "${old_container}" >/dev/null
docker rm "${old_container}" >/dev/null

# A declaration already present in the previous release is not a new break.
changes=$(git diff --unified=0 "${previous}" -- CHANGELOG.md | sed -n 's/^+//p')
if grep -Eqi 'format changed|on-disk format|fresh `krabka-format`' <<<"${changes}"; then
  if docker run --name "${head_container}" --network "${network}" \
      --network-alias krabka-upgrade-old \
      -v "${volume}:/var/lib/krabka" -e KRABKA_CLUSTER_ID="${cluster_id}" \
      -v "${old_config}:/etc/krabka.toml:ro" "${head_image}" \
      --config-file /etc/krabka.toml >"${head_log}" 2>&1; then
    echo "HEAD opened a directory despite the declared format break" >&2
    exit 1
  fi
  grep -Eq 'unsupported meta.properties version|INCONSISTENT_CLUSTER_ID' "${head_log}"
else
  docker run -d --name "${head_container}" --network "${network}" \
    --network-alias krabka-upgrade-head \
    --network-alias krabka-upgrade-old \
    -v "${volume}:/var/lib/krabka" -e KRABKA_CLUSTER_ID="${cluster_id}" \
    -v "${old_config}:/etc/krabka.toml:ro" "${head_image}" \
    --config-file /etc/krabka.toml
  consumed=$(docker exec "${tools_container}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server krabka-upgrade-head:9092 --topic upgrade-smoke \
    --from-beginning --max-messages 1 --timeout-ms 30000)
  test "${consumed}" = survives-upgrade
  floor=$(docker exec "${tools_container}" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server krabka-upgrade-head:9092 --topic upgrade-retention --time -2)
  test "${floor}" = upgrade-retention:0:1
fi
