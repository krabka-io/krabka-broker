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

# The per-KIP rows. `KIP_ANNOTATIONS` in the API catalog is the source of
# truth; this file parses it as text between the two marker comments and never
# compiles Rust. The inventory it is checked against is every `KIP-<n>` under
# `crates/`, except the crate CHANGELOGs (history, not a claim about the tree)
# and the annotation block itself (which would keep a stale row alive).
CRATES = ROOT / "crates"
API_CATALOG = ROOT / "crates/broker/src/api_catalog.rs"
ANNOTATION_BEGIN = "// BEGIN KIP_ANNOTATIONS"
ANNOTATION_END = "// END KIP_ANNOTATIONS"
ENTRY_RE = re.compile(r"KipAnnotation\s*\{(.*?)\n\s*\},", re.DOTALL)
KIP_RE = re.compile(r"\bKIP-\d+\b")
CITATION_RE = re.compile(r"\b(crates/[\w./-]+\.rs):(\d+)\b")
STATUS = {
    "Implemented": "Implemented",
    "Partial": "Partial",
    "OutOfScope": "Out of scope",
}
# The one non-JVM client suite, and what it drives.
LIBRDKAFKA = ROOT / "crates/broker/tests/librdkafka_conformance.rs"
LIBRDKAFKA_TEST = "round_trip_group_join_and_api_versions_with_kcat"
CLIENT_EVIDENCE = {"NotCovered", "Kcat"}
# The scope decision the out-of-scope rows cite, and the words the cited line
# has to still say.
MIXED_QUORUM_WORDS = "Mixed JVM and Krabka controller quorums"
# Image keys that are a Kafka broker, as opposed to a client or object store.
KAFKA_IMAGE_PREFIXES = ("apache_kafka_", "cp_kafka_")


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


def rust_const(source: str, name: str) -> str:
    """The value of a `pub const NAME: &str = "...";` line in `source`."""
    return match(rf'^pub const {re.escape(name)}: &str = "([^"]+)";', source, API_CATALOG)


def annotation_field(body: str, name: str, pattern: str) -> str:
    found = re.search(rf"\b{name}:\s*{pattern}", body, re.DOTALL)
    if not found:
        raise SystemExit(f"KipAnnotation entry without a parsable `{name}` in {API_CATALOG}")
    return found.group(1)


def parse_annotations(catalog: str) -> list[dict]:
    """Read `KIP_ANNOTATIONS` out of the API catalog, one dict per row."""
    start = catalog.find(ANNOTATION_BEGIN)
    end = catalog.find(ANNOTATION_END)
    if start < 0 or end < start:
        raise SystemExit(f"missing {ANNOTATION_BEGIN}/{ANNOTATION_END} markers in {API_CATALOG}")
    rows = []
    for body in ENTRY_RE.findall(catalog[start:end]):
        status = annotation_field(body, "status", r"KipStatus::(\w+)")
        clients = annotation_field(body, "clients", r"ClientEvidence::(\w+)")
        if status not in STATUS:
            raise SystemExit(f"unknown KipStatus variant {status} in {API_CATALOG}")
        if clients not in CLIENT_EVIDENCE:
            raise SystemExit(f"unknown ClientEvidence variant {clients} in {API_CATALOG}")
        rows.append(
            {
                "key": annotation_field(body, "key", r'"([^"]*)"'),
                "claim": annotation_field(body, "claim", r'"([^"]*)"'),
                "status": status,
                "module": annotation_field(body, "module", r'"([^"]*)"'),
                "tests": re.findall(r'"([^"]*)"', annotation_field(body, "tests", r"&\[(.*?)\]")),
                "clients": clients,
                "note": annotation_field(body, "note", r'"([^"]*)"'),
            }
        )
    if not rows:
        raise SystemExit(f"KIP_ANNOTATIONS parsed empty in {API_CATALOG}")
    keys = [row["key"] for row in rows]
    if len(set(keys)) != len(keys):
        raise SystemExit(f"duplicate KIP_ANNOTATIONS keys in {API_CATALOG}")
    return rows


def kip_inventory(catalog: str) -> set[str]:
    """Every `KIP-<n>` a file under `crates/` names."""
    found = set()
    for path in sorted(CRATES.rglob("*")):
        if not path.is_file() or path.name == "CHANGELOG.md" or "target" in path.parts:
            continue
        if path == API_CATALOG:
            start = catalog.find(ANNOTATION_BEGIN)
            end = catalog.find(ANNOTATION_END)
            text = catalog[:start] + catalog[end:]
        else:
            text = path.read_text(errors="ignore")
        found.update(KIP_RE.findall(text))
    return found


def kip_order(key: str) -> tuple[int, int | str]:
    """KIP rows first, by number; the scope-only rows after them, by name."""
    if key.startswith("KIP-"):
        return (0, int(key[len("KIP-") :]))
    return (1, key)


def docker_map(build: Path) -> dict[str, list[str]]:
    """The `docker = {...}` map of a crate's `crate_tests` call: stem to image keys."""
    text = build.read_text()
    start = text.find("docker = {")
    if start < 0:
        return {}
    begin = start + len("docker = ")
    depth = 0
    for index in range(begin, len(text)):
        depth += text[index] == "{"
        depth -= text[index] == "}"
        if depth == 0:
            block = re.sub(r"#[^\n]*", "", text[begin : index + 1])
            return {
                stem: re.findall(r'"(\w+)"', images)
                for stem, images in re.findall(r'"(\w+)":\s*\[([^\]]*)\]', block)
            }
    raise SystemExit(f"unbalanced docker map in {build}")


def suite_images(path: str) -> list[str]:
    """The Kafka image keys the suite that `path` belongs to runs against.

    `crates/<crate>/tests/<stem>.rs` and `crates/<crate>/tests/<stem>/<file>.rs`
    both belong to suite `<stem>`. Anything under `src/` runs in process.
    """
    parts = Path(path).parts
    if len(parts) < 4 or parts[0] != "crates" or parts[2] != "tests":
        return []
    stem = parts[3][: -len(".rs")] if len(parts) == 4 else parts[3]
    images = docker_map(ROOT / "crates" / parts[1] / "BUILD.bazel").get(stem, [])
    return [key for key in images if key.startswith(KAFKA_IMAGE_PREFIXES)]


def check_test(key: str, test: str) -> str:
    """Confirm the file (and function, if named) behind a test entry exists."""
    path, _, function = test.partition("::")
    file = ROOT / path
    if not file.is_file():
        raise SystemExit(f"{key} names a test file that does not exist: {path}")
    if function and not re.search(rf"\bfn {re.escape(function)}\s*[(<]", file.read_text()):
        raise SystemExit(f"{key} names a test that {path} does not define: {function}")
    return path


def link_citations(note: str) -> str:
    """Turn `crates/x.rs:<line>` in a note into a link, after checking the line exists."""

    def link(found: re.Match) -> str:
        path, line = found.group(1), int(found.group(2))
        lines = (ROOT / path).read_text().splitlines()
        if line > len(lines):
            raise SystemExit(f"citation {path}:{line} is past the end of the file")
        return f"[`{path}:{line}`](../{path}#L{line})"

    return CITATION_RE.sub(link, note)


def librdkafka_evidence() -> str:
    """What the librdkafka suite establishes, for the client-family column."""
    suite = LIBRDKAFKA.read_text()
    match(rf"^async fn ({re.escape(LIBRDKAFKA_TEST)})\(\)", suite, LIBRDKAFKA)
    image, program, library = re.search(
        r'CLIENTS: \[\(&str, &str, &str\); 1\] = \[\(\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",?\s*\)\]',
        suite,
        re.DOTALL,
    ).groups()
    image_key = docker_map(BROKER_BUILD).get("librdkafka_conformance", [None])[0]
    if image_key is None or image_for(image_key) != image:
        raise SystemExit(f"librdkafka client image {image} is not the one Bazel loads")
    digest_for(image_key)
    return (
        f"{program} {image.rsplit(':', 1)[1]} ({library}): "
        f"[`{LIBRDKAFKA_TEST}`](../crates/broker/tests/librdkafka_conformance.rs)"
    )


def kip_rows() -> tuple[str, str]:
    """The per-KIP rows, and the image legend they refer to."""
    catalog = API_CATALOG.read_text()
    rows = parse_annotations(catalog)
    citation = rust_const(catalog, "OUT_OF_SCOPE_CITATION")
    mixed_quorum = rust_const(catalog, "MIXED_QUORUM_KEY")

    cited_path, cited_line = citation.rsplit(":", 1)
    cited = (ROOT / cited_path).read_text().splitlines()[int(cited_line) - 1]
    if MIXED_QUORUM_WORDS not in cited:
        raise SystemExit(f"{citation} no longer says '{MIXED_QUORUM_WORDS}': {cited}")

    annotated = {row["key"] for row in rows}
    claimed = kip_inventory(catalog)
    unannotated = sorted(claimed - annotated, key=kip_order)
    if unannotated:
        raise SystemExit(
            "KIPs named under crates/ without a KIP_ANNOTATIONS row in "
            f"{API_CATALOG}: {', '.join(unannotated)}"
        )
    stale = sorted(key for key in annotated - claimed if key.startswith("KIP-"))
    if stale:
        raise SystemExit(f"KIP_ANNOTATIONS rows for KIPs nothing under crates/ names: {', '.join(stale)}")
    if mixed_quorum not in annotated:
        raise SystemExit(f"KIP_ANNOTATIONS has no {mixed_quorum} row")
    ordered = [row["key"] for row in rows]
    if ordered != sorted(ordered, key=kip_order):
        raise SystemExit("KIP_ANNOTATIONS is not in KIP order")

    kcat = librdkafka_evidence()
    used_images: set[str] = set()
    rendered = []
    for row in rows:
        key = row["key"]
        if not (ROOT / row["module"]).is_file():
            raise SystemExit(f"{key} names an owner that does not exist: {row['module']}")
        if row["status"] == "OutOfScope":
            if row["tests"] or not row["note"]:
                raise SystemExit(f"{key} is out of scope; it needs a note and no tests")
        elif not row["tests"]:
            raise SystemExit(f"{key} is {row['status']} without a test")
        if key in (mixed_quorum, "KIP-590") and citation not in row["note"]:
            raise SystemExit(f"{key} does not cite {citation}")
        if row["clients"] == "Kcat" and not any(
            test == f"crates/broker/tests/librdkafka_conformance.rs::{LIBRDKAFKA_TEST}"
            for test in row["tests"]
        ):
            raise SystemExit(f"{key} claims kcat evidence without listing {LIBRDKAFKA_TEST}")

        images: list[str] = []
        tests = []
        for test in row["tests"]:
            path = check_test(key, test)
            images.extend(image for image in suite_images(path) if image not in images)
            shown = test[len("crates/") :]
            tests.append(f"[`{shown}`](../{path})")
        used_images.update(images)

        owner = row["module"][len("crates/") :]
        image_column = ", ".join("`" + image + "`" for image in images) or "in process"
        note = link_citations(row["note"]).replace("|", "\\|")
        rendered.append(
            f"| {key} | {STATUS[row['status']]} | {row['claim']} | "
            f"[`{owner}`](../{row['module']}) | "
            f"{'<br>'.join(tests) or 'none'} | {image_column} | "
            f"{kcat if row['clients'] == 'Kcat' else 'none'} | {note} |"
        )

    legend = "\n".join(
        f"| `{key}` | `{image_for(key)}@{digest_for(key)}` |" for key in sorted(used_images)
    )
    return "\n".join(rendered), legend


def render() -> str:
    kip_table, image_legend = kip_rows()
    log_rows, log_image = log_contract()
    api_rows, api_image = api_versions()
    throttle_rows = throttle_echo()
    return f"""# Kafka KIP compatibility matrix

<!-- Generated by tools/generate-kip-matrix.py; do not edit by hand. -->

This matrix records what the tree claims about Kafka compatibility and where
the evidence for each claim is: one row per KIP that any file under `crates/`
names, then the repository's JVM differential evidence, which is the Kafka
on-disk log contract and the `ApiVersions` table every client negotiates
against.

## KIP status

Every row comes from `KIP_ANNOTATIONS` in
[`api_catalog`](../crates/broker/src/api_catalog.rs). The generator scans
`crates/` for `KIP-<n>` mentions, except the crate CHANGELOGs, and fails when a
KIP is named without a row here or a row names a KIP that nothing mentions, so a
compatibility claim cannot enter `src/` without evidence beside it. It also
fails when a row's owner or test does not exist, or when a `path::function`
entry names a function the file does not define.

- **Status** is `Implemented`, `Partial` (the note says what is missing), or
  `Out of scope` (the note cites the decision).
- **Owner** is the module that holds the behavior.
- **Tests** establish the status. A test under `src/` is a unit or model test.
- **Kafka image** is what the listed suites run against, read from each crate's
  `BUILD.bazel` `docker` map and pinned below; `in process` means no suite in
  the row starts a Kafka container.
- **librdkafka** is the non-JVM client evidence:
  [`librdkafka_conformance`](../crates/broker/tests/librdkafka_conformance.rs)
  drives the stock `kcat` image against the broker.

| KIP | Status | Contract | Owner | Tests | Kafka image | librdkafka | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
{kip_table}

### Kafka images

| Key | Image |
| :--- | :--- |
{image_legend}

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
