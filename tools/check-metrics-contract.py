#!/usr/bin/env python3
"""Check the published metrics contract against a `/metrics` body.

The reference dashboard and the alert rules under `docs/operations/` name
Prometheus series. Every name they spell must be one the broker exports, or a
panel stays empty and an alert never fires. This script extracts every
`krabka_broker_*` name from the two files and fails on any that the body
passed as the argument does not carry.

The body is the OpenMetrics text a broker serves on `/metrics`. In CI it is
`docs/operations/metrics-body.txt`, which the `metrics_contract` suite in
`crates/broker/tests/` regenerates and keeps in step with the registry.

    tools/check-metrics-contract.py docs/operations/metrics-body.txt

The name rules match that suite: a `# TYPE` line of kind `counter` exports
`<name>_total`, a `gauge` exports `<name>`, and a `histogram` exports
`<name>_bucket`, `<name>_sum` and `<name>_count`.
"""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "docs/operations/grafana-dashboard.json"
RULES = ROOT / "docs/operations/alert-rules.yaml"
NAME = re.compile(r"\bkrabka_broker_[A-Za-z0-9_]+")
HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")


def exported_sample_names(body: str) -> set[str]:
    """Every sample name the body can carry, expanded from its `# TYPE` lines.

    A family with no live label set writes no sample line, so the `# TYPE`
    line is the one place every registered family appears.
    """
    names: set[str] = set()
    for line in body.splitlines():
        if not line.startswith("# TYPE "):
            continue
        parts = line[len("# TYPE ") :].split()
        if len(parts) < 2:
            continue
        name, kind = parts[0], parts[1]
        if kind == "counter":
            names.add(f"{name}_total")
        elif kind in ("gauge", "unknown"):
            names.add(name)
        elif kind == "histogram":
            names.update(f"{name}{suffix}" for suffix in HISTOGRAM_SUFFIXES)
        else:
            raise SystemExit(f"unexpected metric type {kind} on {name}")
    return names


def dashboard_expressions(path: Path) -> list[str]:
    """Every `expr` string in the dashboard, at any nesting depth."""
    found: list[str] = []

    def walk(node) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "expr" and isinstance(value, str):
                    found.append(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(json.loads(path.read_text()))
    return found


def rule_fields(path: Path, field: str) -> list[str]:
    """Every `<field>:` value in the rules file, block scalars included.

    The file is plain enough that a line scan reads it: a rule field sits on
    one line as `field: value`, or opens a `|` or `>` block whose lines are
    indented deeper than the key.
    """
    values: list[str] = []
    lines = path.read_text().splitlines()
    index = 0
    key = re.compile(rf"^(\s*)(?:-\s+)?{re.escape(field)}:\s*(.*)$")
    while index < len(lines):
        match = key.match(lines[index])
        index += 1
        if not match:
            continue
        indent, value = len(match.group(1)), match.group(2).strip()
        if value in ("|", ">", "|-", ">-"):
            block: list[str] = []
            while index < len(lines):
                line = lines[index]
                if line.strip() and len(line) - len(line.lstrip()) <= indent:
                    break
                block.append(line.strip())
                index += 1
            value = " ".join(block)
        values.append(value.strip("\"'"))
    return values


def referenced_names(expressions: list[str]) -> set[str]:
    return {name for expression in expressions for name in NAME.findall(expression)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("body", type=Path, help="a /metrics body file")
    parser.add_argument("--dashboard", type=Path, default=DASHBOARD)
    parser.add_argument("--rules", type=Path, default=RULES)
    args = parser.parse_args()

    exported = exported_sample_names(args.body.read_text())
    if not exported:
        print(f"{args.body}: no `# TYPE` lines; not a /metrics body", file=sys.stderr)
        return 1

    failed = False
    sources = {
        args.dashboard: dashboard_expressions(args.dashboard),
        args.rules: rule_fields(args.rules, "expr"),
    }
    for path, expressions in sources.items():
        referenced = referenced_names(expressions)
        if not referenced:
            print(f"{path}: names no krabka_broker_* series", file=sys.stderr)
            failed = True
        for name in sorted(referenced - exported):
            print(f"{path}: {name} is not a series the broker exports", file=sys.stderr)
            failed = True

    for runbook in rule_fields(args.rules, "runbook"):
        if not (ROOT / runbook).is_file():
            print(f"{args.rules}: runbook {runbook} does not exist", file=sys.stderr)
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
