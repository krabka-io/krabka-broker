#!/usr/bin/env python3
"""Regression checks for the secure-by-default reference manifests."""
from pathlib import Path
import re
import sys
import tomllib

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
stateful = (root / "statefulset.yaml").read_text()
services = (root / "service.yaml").read_text()
policy = (root / "networkpolicy.yaml").read_text()
configmap = (root / "configmap.yaml").read_text()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


# A tag can be retargeted. Both containers must use the same sha256 reference.
images = re.findall(r"^\s+image:\s+(\S+)$", stateful, re.MULTILINE)
require(len(images) == 2, "expected formatter and broker image fields")
require(images[0] == images[1], "formatter and broker images must match")
require(re.fullmatch(r"ghcr\.io/krabka-io/krabka-broker@sha256:[0-9a-f]{64}", images[0]) is not None,
        "broker image must be pinned by digest")
require(":dev" not in stateful, "mutable development image remains")

require("--config-file=/etc/krabka/config/broker-$(KRABKA_POD_INDEX).toml" in stateful,
        "broker does not require the security config")
for token in ("secretName: krabka-tls", "name: config", "name: tls", "readOnly: true"):
    require(token in stateful, f"missing required mount setting: {token}")

# Extract the ConfigMap's literal TOML blocks without accepting YAML aliases.
blocks: dict[str, str] = {}
lines = configmap.splitlines()
for index, line in enumerate(lines):
    match = re.fullmatch(r"  (broker-[0-2]\.toml): \|", line)
    if not match:
        continue
    body = []
    for item in lines[index + 1:]:
        if item.startswith("    "):
            body.append(item[4:])
        elif item == "":
            body.append("")
        else:
            break
    blocks[match.group(1)] = "\n".join(body) + "\n"
require(set(blocks) == {f"broker-{i}.toml" for i in range(3)}, "need one config per ordinal")
for ordinal in range(3):
    cfg = tomllib.loads(blocks[f"broker-{ordinal}.toml"])
    require(cfg["controller_listener_protocol"] == "Ssl", "controller must use TLS")
    require(cfg["tls_config"]["client_auth"] == "Required", "controller must require mTLS")
    require(cfg["listeners"][0]["protocol"] == "Ssl", "Kafka listener must use TLS")
    require(cfg["listeners"][0]["tls_config"]["client_auth"] == "Required",
            "Kafka listener must require mTLS")
    require(cfg["authorization"]["type"] == "simple", "authorizer must be deny-capable")
    require(cfg["authorization"]["allow_everyone_if_no_acl_found"] is False,
            "authorizer must deny resources without ACLs")
    require("ANONYMOUS" not in cfg["authorization"]["super_users"],
            "anonymous principal must never be privileged")
    bindings = cfg["inter_broker_principal_node_ids"]
    require(bindings == {f"CN=broker-{i}": i for i in range(3)},
            "controller/broker principal-to-node bindings are incomplete")

# Neither shared Service may publish an administrative or observability port.
service_ports = {int(port) for port in re.findall(r"^\s+port:\s+(\d+)\s*$", services, re.MULTILINE)}
require(service_ports == {9092}, f"unexpected Service ports: {sorted(service_ports)}")
require("port: 9093" in policy and "app.kubernetes.io/name: krabka" in policy,
        "controller NetworkPolicy restriction is missing")
require('krabka.io/client: "true"' in policy, "Kafka client access is not explicit opt-in")
require('krabka.io/monitor: "true"' in policy, "metrics access is not restricted")
print("secure Kubernetes manifest validation passed")
