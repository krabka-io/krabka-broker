#!/usr/bin/env python3
"""Check the subsystem design documents against the design-doc style guide.

The check fails when:

- a design document under ``crates/<crate>/docs/`` is missing from the index
  table in ``docs/style_guides/design_doc_style_guide.md``, or the index names
  a file that does not exist;
- a design document lacks one of the section headings the guide's template
  requires;
- a design document is not linked from its crate's ``README.md``;
- a relative link or a ``#fragment`` anchor inside a design document, the
  index, or the diskless crash model's doc comment does not resolve.

The repository-wide link checker in CI does not follow fragments, so the
anchor check here is what keeps ``Slice 5`` and ``Slice 6`` in
``crates/broker/src/diskless_crash_model.rs`` pointing at real sections.

Run from the repository root: ``python3 tools/check-design-docs.py``.
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
GUIDE = ROOT / "docs" / "style_guides" / "design_doc_style_guide.md"
CRASH_MODEL = ROOT / "crates" / "broker" / "src" / "diskless_crash_model.rs"
REQUIRED_HEADINGS = (
    "## Design Goals",
    "## Architecture Overview",
    "## Key Design Decisions",
    "## Integration",
    "## Kafka / KIP Compliance",
    "## Testing",
)
LINK = re.compile(r"\]\(([^)\s]+)\)")
REFERENCE_LINK = re.compile(r"^//! \[[^\]]+\]: (\S+)$", re.MULTILINE)
HEADING = re.compile(r"^(#+)\s+(.*)$")


def slug(heading: str) -> str:
    """Return the GitHub-style anchor for a Markdown heading."""
    text = re.sub(r"[`*]", "", heading.strip().lower())
    text = re.sub(r"[^a-z0-9 _-]", "", text)
    return text.replace(" ", "-")


def anchors(path: pathlib.Path) -> set[str]:
    found: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = HEADING.match(line)
        if match:
            found.add(slug(match.group(2)))
    return found


def check_links(source: pathlib.Path, links: list[str], base: pathlib.Path) -> list[str]:
    problems: list[str] = []
    for link in links:
        if link.startswith(("http://", "https://", "mailto:")):
            continue
        target_part, _, fragment = link.partition("#")
        target = (base / target_part).resolve() if target_part else source
        if not target.exists():
            problems.append(f"{source.relative_to(ROOT)}: missing link target {link}")
            continue
        if fragment and target.suffix == ".md" and fragment not in anchors(target):
            problems.append(f"{source.relative_to(ROOT)}: missing anchor {link}")
    return problems


def design_docs() -> list[pathlib.Path]:
    return sorted(ROOT.glob("crates/*/docs/*design*.md"))


def indexed_docs() -> set[pathlib.Path]:
    indexed: set[pathlib.Path] = set()
    for link in LINK.findall(GUIDE.read_text(encoding="utf-8")):
        if link.startswith("http"):
            continue
        candidate = (GUIDE.parent / link.partition("#")[0]).resolve()
        if candidate.is_relative_to(ROOT / "crates") and candidate.suffix == ".md":
            indexed.add(candidate)
    return indexed


def main() -> int:
    problems: list[str] = []
    docs = design_docs()
    if not docs:
        problems.append("no design documents found under crates/*/docs/")

    indexed = indexed_docs()
    for doc in indexed:
        if not doc.exists():
            problems.append(f"{GUIDE.relative_to(ROOT)}: index names missing file {doc.relative_to(ROOT)}")
    for doc in docs:
        relative = doc.relative_to(ROOT)
        if doc.resolve() not in indexed:
            problems.append(f"{relative}: not listed in {GUIDE.relative_to(ROOT)}")
        text = doc.read_text(encoding="utf-8")
        for heading in REQUIRED_HEADINGS:
            if not re.search(rf"^{re.escape(heading)}\s*$", text, re.MULTILINE):
                problems.append(f"{relative}: missing required heading '{heading}'")
        readme = doc.parent.parent / "README.md"
        if not readme.exists():
            problems.append(f"{relative}: crate has no README.md to link it from")
        else:
            readme_links = {
                (readme.parent / link.partition("#")[0]).resolve()
                for link in LINK.findall(readme.read_text(encoding="utf-8"))
                if not link.startswith("http")
            }
            if doc.resolve() not in readme_links:
                problems.append(f"{relative}: not linked from {readme.relative_to(ROOT)}")
        problems.extend(check_links(doc, LINK.findall(text), doc.parent))

    problems.extend(check_links(GUIDE, LINK.findall(GUIDE.read_text(encoding="utf-8")), GUIDE.parent))

    if CRASH_MODEL.exists():
        links = REFERENCE_LINK.findall(CRASH_MODEL.read_text(encoding="utf-8"))
        if not links:
            problems.append(f"{CRASH_MODEL.relative_to(ROOT)}: doc comment has no design-doc reference links")
        problems.extend(check_links(CRASH_MODEL, links, CRASH_MODEL.parent))

    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    print(f"design docs ok: {len(docs)} documents checked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
