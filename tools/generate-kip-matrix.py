#!/usr/bin/env python3
"""Generate the checked-in Kafka compatibility evidence matrix."""

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "crates/log/tests/integration.rs"
LOG_BUILD = ROOT / "crates/log/BUILD.bazel"
BROKER_BUILD = ROOT / "crates/broker/BUILD.bazel"
IMAGES = ROOT / "bazel/images/BUILD.bazel"
MODULE = ROOT / "MODULE.bazel"
API_VERSIONS = ROOT / "crates/broker/tests/fixtures/api_versions/divergence.json"
API_VERSIONS_ORACLE = ROOT / "crates/broker/tests/api_versions_differential/oracle.rs"
THROTTLE_AUDIT = ROOT / "crates/broker/src/network/dispatch/throttle_audit.rs"
ROWS = (
    (
        "JVM -> krabka",
        "Open and read a JVM-written Kafka log directory",
        "read_jvm_produced_log_dir",
    ),
    (
        "krabka -> JVM",
        "Restart Kafka over a krabka-written log directory and consume it",
        "jvm_consumes_rust_written_log_dir",
    ),
)
# How `divergence.json` labels a row, and how the matrix says it.
VERDICTS = {
    "same": "match",
    "range_differs": "range differs",
    "krabka_only": "krabka only",
    "kafka_only": "Kafka only",
}


# KIP-219 (throttle-then-respond) responses whose `ThrottleTimeMs` is not the
# leading field of the response body, so the dispatch loop -- which reports a
# request-quota delay by patching a leading int32 -- cannot reach it.
#
# The rows below carry only the schema-layout half. The `(api_key, versions)`
# set and the per-API runtime reach are both parsed out of
# `THROTTLE_ECHO_DIVERGENCES` in the audit module, and rendering fails unless
# the parsed set is exactly the set below, so a divergence added to the Rust
# constant cannot be dropped from this page.
THROTTLE_ECHO_ROWS = {
    (0, "1-13"): (
        "Produce",
        "Sits behind the `Responses` array, at an offset the response header "
        "does not fix",
    ),
    (18, "1-5"): (
        "ApiVersions",
        "Sits behind the `ApiKeys` array, at an offset the response header "
        "does not fix",
    ),
    (38, "1-3"): (
        "CreateDelegationToken",
        "Last field, behind the principal strings, the token timestamps and "
        "the HMAC",
    ),
    (39, "1-2"): (
        "RenewDelegationToken",
        "Last field, behind `ErrorCode` and the new expiry timestamp",
    ),
    (40, "1-2"): (
        "ExpireDelegationToken",
        "Last field, behind `ErrorCode` and the new expiry timestamp",
    ),
    (41, "1-3"): (
        "DescribeDelegationToken",
        "Last field, behind `ErrorCode` and the variable-length token list",
    ),
    (47, "0"): (
        "OffsetDelete",
        "Leads with `ErrorCode`; the field is at a fixed offset of 2, but the "
        "dispatch loop patches leading fields only",
    ),
}

# What each `QuotaReach` variant of the audit constant means on the wire. The
# variant is the audit's own claim about the dispatch entry's
# `RequestQuotaPolicy`, pinned there by
# `recorded_reach_matches_the_dispatch_registry`.
QUOTA_REACH_EFFECT = {
    "SelfAccounted": (
        "None. The handler charges its own quota and sets `ThrottleTimeMs` on "
        "the typed response before encoding, so the client does see the delay"
    ),
    "FallbackAccounted": (
        "An ordinary request can be held by the request quota, and the "
        "response it waits behind reports `throttle_time_ms = 0`"
    ),
    "UnsupportedVersionOnly": (
        "The ordinary path is `InlineExempt`, so it is never held. Only a "
        "request outside the advertised version range is, and that reply "
        "reports `throttle_time_ms = 0`"
    ),
}
DIVERGENCE_RE = re.compile(
    r"\(\s*(\d+),\s*(\d+),\s*(\d+),\s*QuotaReach::(\w+)\s*,?\s*\)"
)
THROTTLE_AUDIT_TEST = "throttle_echo_divergences_are_the_recorded_ones"
THROTTLE_REACH_TEST = "recorded_reach_matches_the_dispatch_registry"


def parse_divergences(audit: str) -> list[tuple[int, str, str]]:
    """Read `THROTTLE_ECHO_DIVERGENCES` out of the audit module.

    Returns one `(api_key, version range, QuotaReach variant)` triple per
    entry, with the range rendered the way the table shows it.
    """
    start = audit.find("const THROTTLE_ECHO_DIVERGENCES")
    if start < 0:
        raise SystemExit(f"missing THROTTLE_ECHO_DIVERGENCES in {THROTTLE_AUDIT}")
    end = audit.index("\n];", start)
    rows = []
    for key, low, high, reach in DIVERGENCE_RE.findall(audit[start:end]):
        if reach not in QUOTA_REACH_EFFECT:
            raise SystemExit(f"unknown QuotaReach variant {reach} in {THROTTLE_AUDIT}")
        rows.append((int(key), low if low == high else f"{low}-{high}", reach))
    if not rows:
        raise SystemExit(f"THROTTLE_ECHO_DIVERGENCES parsed empty in {THROTTLE_AUDIT}")
    return rows


def match(pattern: str, text: str, source: Path) -> str:
    found = re.search(pattern, text, re.MULTILINE)
    if not found:
        raise SystemExit(f"missing matrix evidence in {source}: {pattern}")
    return found.group(1)


def image_for(key: str) -> str:
    """The tag //bazel/images loads image `key` under."""
    return match(
        rf'^\s*"{re.escape(key)}"\s*:\s*"([^"]+)"',
        IMAGES.read_text(),
        IMAGES,
    )


def digest_for(key: str) -> str:
    """The digest //MODULE.bazel pins image `key` to.

    The pin is the last field of the tuple, whatever the fields before it are:
    the rules_img work widened the row from `(name, tag, digest)` to
    `(name, registry, repository, tag, digest)`, and a pattern that counted the
    fields in between silently stopped matching.
    """
    return match(
        rf'^\s*\("{re.escape(key)}",(?:\s*"[^"]+",)+\s*"(sha256:[^"]+)"\)',
        MODULE.read_text(),
        MODULE,
    )


def log_contract() -> tuple[str, str]:
    """The on-disk log evidence rows, and the image that produced them."""
    tests = TESTS.read_text()
    for _, _, test in ROWS:
        match(rf"^async fn ({re.escape(test)})\(\)", tests, TESTS)

    image_key = match(
        r'docker\s*=\s*\{\s*"integration"\s*:\s*\["([^"]+)"\]',
        LOG_BUILD.read_text(),
        LOG_BUILD,
    )
    image = image_for(image_key)
    test_image = match(r'^const KAFKA_IMAGE: &str = "([^"]+)";', tests, TESTS)
    test_tag = match(r'^const KAFKA_TAG: &str = "([^"]+)";', tests, TESTS)
    if f"{test_image}:{test_tag}" != image:
        raise SystemExit(
            f"test image {test_image}:{test_tag} does not match Bazel image {image}"
        )
    pinned = f"{image}@{digest_for(image_key)}"

    rows = "\n".join(
        f"| {direction} | Implemented | {contract} | "
        f"[`crates/log`](../crates/log) | "
        f"[`{test}`](../crates/log/tests/integration.rs) | `{pinned}` |"
        for direction, contract, test in ROWS
    )
    return rows, pinned


def api_versions() -> tuple[str, str]:
    """The advertised-version rows, and the oracle image they were read from.

    Both version columns come from `divergence.json`, which
    `//crates/broker:api_versions_differential` writes from a live krabka broker
    beside a live Kafka broker. Nothing here re-derives them; what this checks is
    that the oracle the file names is still the one the suite drives and the one
    Bazel pins, so a stale recording cannot pass itself off as current evidence.
    """
    report = json.loads(API_VERSIONS.read_text())

    image_key = match(
        r'"api_versions_differential":\s*\["([^"]+)"\]',
        BROKER_BUILD.read_text(),
        BROKER_BUILD,
    )
    image = image_for(image_key)
    suite_image = match(
        r'^pub\(crate\) const ORACLE_IMAGE: &str = "([^"]+)";',
        API_VERSIONS_ORACLE.read_text(),
        API_VERSIONS_ORACLE,
    )
    for whose, named in (("suite", suite_image), ("recorded", report["oracle_image"])):
        if named != image:
            raise SystemExit(f"{whose} oracle {named} does not match Bazel image {image}")

    def span(versions: dict | None) -> str:
        return "not advertised" if versions is None else f"{versions['min']}-{versions['max']}"

    rows = "\n".join(
        f"| {row['name']} | {row['api_key']} | {span(row['krabka'])} | "
        f"{span(row['kafka'])} | {VERDICTS[row['verdict']]} |"
        for row in report["apis"]
    )
    return rows, f"{image}@{digest_for(image_key)}"


def throttle_echo() -> str:
    """The KIP-219 throttle-echo divergence rows."""
    audit = THROTTLE_AUDIT.read_text()
    match(rf"^fn ({re.escape(THROTTLE_AUDIT_TEST)})\(\)", audit, THROTTLE_AUDIT)
    match(rf"^fn ({re.escape(THROTTLE_REACH_TEST)})\(\)", audit, THROTTLE_AUDIT)
    divergences = parse_divergences(audit)
    parsed_keys = {(key, versions) for key, versions, _ in divergences}
    if parsed_keys != set(THROTTLE_ECHO_ROWS):
        missing = sorted(parsed_keys - set(THROTTLE_ECHO_ROWS))
        extra = sorted(set(THROTTLE_ECHO_ROWS) - parsed_keys)
        raise SystemExit(
            "THROTTLE_ECHO_DIVERGENCES and THROTTLE_ECHO_ROWS disagree: "
            f"recorded but unrendered {missing}, rendered but unrecorded {extra}"
        )

    return "\n".join(
        f"| {THROTTLE_ECHO_ROWS[(key, versions)][0]} | {key} | {versions} | "
        f"{THROTTLE_ECHO_ROWS[(key, versions)][1]} | {QUOTA_REACH_EFFECT[reach]} |"
        for key, versions, reach in divergences
    )


def render() -> str:
    log_rows, log_image = log_contract()
    api_rows, api_image = api_versions()
    throttle_rows = throttle_echo()
    return f"""# Kafka KIP compatibility matrix

<!-- Generated by tools/generate-kip-matrix.py; do not edit by hand. -->

This matrix records the repository's JVM differential evidence: the Kafka
on-disk log contract, and the `ApiVersions` table every client negotiates
against.

## On-disk log contract

The suite covers timestamped record batches (KIP-32), segment and index
discovery, and record key/value recovery in both directions. It ran against
`{log_image}`.

| Direction | Status | Contract | Owner | Differential test | Kafka image |
| :--- | :--- | :--- | :--- | :--- | :--- |
{log_rows}

## Advertised API versions

`kafka-broker-api-versions` read both tables below.
[`api_versions_differential`](../crates/broker/tests/api_versions_differential.rs)
asserts krabka's against
[`api_catalog`](../crates/broker/src/api_catalog.rs) and records the join in
[`divergence.json`](../crates/broker/tests/fixtures/api_versions/divergence.json),
which is where these two version columns come from. A range that moves, or a key
that appears on one side only, changes that file and so changes this table.

The oracle is `{api_image}`.

It is a stock single-node KRaft broker, read on its PLAINTEXT *broker* listener.
Both facts shape the Kafka column, so a `krabka only` verdict means "that broker
did not advertise the key there", not "Kafka does not have the API". Two reasons
account for every such row:

- Kafka scopes an `ApiVersions` response to the listener it arrives on
  (`ApiVersionsResponse.filterApis` takes a `ListenerType`), and answers the
  controller-plane APIs on the controller listener, which this table does not
  read. krabka runs both roles behind one listener and advertises the union.
- Kafka advertises `GetTelemetrySubscriptions` and `PushTelemetry` only while a
  client-telemetry exporter is configured, and this oracle configures none.
  krabka advertises them unconditionally.

| API | Key | krabka | Kafka | Verdict |
| :--- | ---: | :--- | :--- | :--- |
{api_rows}

The Bazel Docker lane supplies each image from a digest-pinned OCI repository.
CI regenerates this page and fails when the test names, lane, image tag, digest,
recorded API versions, or checked-in output drift.

## KIP-219 throttle-echo divergences

The broker reports a request-quota delay by patching the leading
`ThrottleTimeMs` int32 of an already-encoded response, so it can only echo the
delay on responses whose schema puts that field first. Every advertised API is
audited by
[`throttle_audit`](../crates/broker/src/network/dispatch/throttle_audit.rs),
which encodes each response at each advertised version and compares where
`ThrottleTimeMs` lands against the broker's table.

The APIs below carry `ThrottleTimeMs` behind another field, so the patch
cannot reach it. That is a statement about the schema, not about what a client
observes: whether a request-quota delay is ever applied to one of these APIs is
decided separately, by its dispatch entry's `RequestQuotaPolicy`. The two
columns are kept apart for that reason.

* `SelfAccounted` entries charge the quota in the handler and set
  `ThrottleTimeMs` on the typed response before encoding, so the buried field
  costs nothing and the client does see the delay.
* `ApplyFallbackAccounting` entries are the ones the dispatch loop charges and
  delays, so a buried field there really does mean latency without a back-off
  signal.
* `InlineExempt` entries -- most of the admin, ACL and delegation-token
  surface -- are exempt from the request quota on the ordinary path, so they
  are never delayed by it. The unsupported-version reply path charges every
  `api_key` regardless of policy, so a request outside the advertised version
  range is the one case where such an API is held without an echo.

`recorded_reach_matches_the_dispatch_registry` in the audit pins the last
column against the assembled dispatch registry, so a policy change on any of
these APIs fails the build until this page is regenerated. Echoing the field on
any of them needs it set on the typed response before encoding rather than a
byte patch.

| API | api_key | Versions | Why the field cannot be patched | Runtime effect |
| :--- | :--- | :--- | :--- | :--- |
{throttle_rows}
"""


output = Path(sys.argv[1]) if len(sys.argv) == 2 else ROOT / "docs/KIP_MATRIX.md"
if len(sys.argv) > 2:
    raise SystemExit(f"usage: {Path(sys.argv[0]).name} [OUTPUT]")
output.write_text(render())
