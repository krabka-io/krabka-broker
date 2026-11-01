#!/usr/bin/env python3
"""Collect released Kafka evidence; successful collection is NOT parity qualification."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

IMAGE = "apache/kafka:4.4.0@sha256:76af3d0116abbb33b4cd964af410d4921c33d1f663e7a95d5453a9c35a84869b"
BIN = "/opt/kafka/bin/"
PROBES = {
    "api-versions": ("kafka-broker-api-versions.sh",),
    "features": ("kafka-features.sh", "describe"),
    "broker-configs": ("kafka-configs.sh", "--describe", "--entity-type", "brokers", "--entity-default", "--all"),
}


def command(args):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=90, check=False)
        return {"command": args, "returncode": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except subprocess.TimeoutExpired as error:
        return {"command": args, "returncode": 124, "stdout": str(error.stdout or ""),
                "stderr": "probe timed out after 90s"}


def tool(name, bootstrap, probe, container=None):
    args = ["docker", "exec", container] if container else [
        "docker", "run", "--rm", "--network", "host", IMAGE]
    return command(args + [BIN + name, "--bootstrap-server", bootstrap] + list(probe))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="exact Krabka commit tested")
    parser.add_argument("--default-bootstrap", help="host-network address")
    parser.add_argument("--enabled-bootstrap", help="host-network address")
    parser.add_argument("--default-config", type=Path)
    parser.add_argument("--enabled-config", type=Path)
    args = parser.parse_args()
    if len(args.revision) != 40 or any(c not in "0123456789abcdef" for c in args.revision):
        parser.error("--revision must be a full lowercase 40-character commit SHA")
    inputs = [args.default_bootstrap, args.enabled_bootstrap, args.default_config, args.enabled_config]
    if any(inputs) and not all(inputs):
        parser.error("supply both bootstrap addresses and both configuration inputs, or none for oracle-only evidence")
    # Keep private effective configuration inputs out of public evidence. A checksum
    # identifies the exact input; operators must retain the files separately.
    configs = {key: {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
               for key, path in [("default", args.default_config), ("enabled", args.enabled_config)] if path}
    args.output.mkdir(parents=True, exist_ok=False)
    name = "krabka-kafka44-oracle-" + uuid.uuid4().hex[:12]
    evidence = {"schema_version": 1, "status": "incomplete", "krabka_revision": args.revision,
                "oracle_image": IMAGE, "configs": configs, "sides": {},
                "qualification_pass": False,
                "unexecuted": ["authorization", "feature-level boundaries", "share DLQ",
                               "delayed tiered uploads", "misrouting", "controller unregistration",
                               "topic-ID transactional commits", "Streams topology APIs",
                               "restart/recovery", "multi-broker"]}
    started = False
    try:
        env = {
            "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://localhost:9092",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
        }
        invocation = ["docker", "run", "-d", "--name", name]
        for key, value in env.items():
            invocation.extend(["-e", key + "=" + value])
        evidence["oracle_configuration"] = env
        evidence["startup"] = command(invocation + [IMAGE])
        started = evidence["startup"]["returncode"] == 0
        if not started:
            raise RuntimeError("oracle startup failed")
        for _ in range(30):
            ready = tool("kafka-topics.sh", "localhost:9092", ["--list"], name)
            if ready["returncode"] == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("oracle never became ready")
        evidence["client_version"] = command(["docker", "exec", name, BIN + "kafka-topics.sh", "--version"])
        if evidence["client_version"]["returncode"] or not evidence["client_version"]["stdout"].startswith("4.4.0"):
            raise RuntimeError("pinned client did not identify as 4.4.0")
        for side, bootstrap, container in [
            ("oracle", "localhost:9092", name),
            ("default", args.default_bootstrap, None),
            ("enabled", args.enabled_bootstrap, None),
        ]:
            if not bootstrap:
                continue
            evidence["sides"][side] = {
                key: tool(probe[0], bootstrap, probe[1:], container)
                for key, probe in PROBES.items()
            }
        failed = any(result["returncode"] for probes in evidence["sides"].values() for result in probes.values())
        evidence["status"] = "probe-failure" if failed else "collected-not-qualified"
        return 1 if failed else 0
    except (RuntimeError, OSError) as error:
        evidence["error"] = str(error)
        return 1
    finally:
        if started:
            evidence["oracle_logs"] = command(["docker", "logs", name])
            evidence["cleanup"] = command(["docker", "rm", "-f", name])
        (args.output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
