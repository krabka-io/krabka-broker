#!/usr/bin/env python3
"""Reject deep `crate::...` paths in Rust code outside imports and macros."""

from pathlib import Path
import subprocess
import sys

from tree_sitter import Language, Parser
import tree_sitter_rust


PATH_TYPES = {"scoped_identifier", "scoped_type_identifier"}
BLOCKED_ANCESTORS = {
    "use_declaration",
    "extern_crate_declaration",
    "attribute_item",
    "inner_attribute_item",
    "macro_invocation",
    "macro_definition",
    "line_comment",
    "block_comment",
    "doc_comment",
    "visibility_modifier",
}


def is_path(node):
    return node.type in PATH_TYPES


def starts_crate(node):
    return is_path(node) and node.text.startswith(b"crate::")


def path_chain(root):
    chain = [root]
    current = root
    while True:
        child = next(
            (item for item in current.named_children if is_path(item) and starts_crate(item)),
            None,
        )
        if child is None:
            return chain
        chain.append(child)
        current = child


def is_type_name(segment):
    return (
        bool(segment)
        and segment[0].isupper()
        and not segment.isupper()
        and "_" not in segment
    )


def deep_paths(parser, source):
    tree = parser.parse(source)
    found = []

    def visit(node, blocked=False):
        blocked = blocked or node.type in BLOCKED_ANCESTORS
        if not blocked and starts_crate(node):
            parent = node.parent
            if not (parent and starts_crate(parent)):
                chain = path_chain(node)
                selected = None
                for part in reversed(chain):
                    tail = part.text.decode("utf-8", "replace").split("::")[-1]
                    if is_type_name(tail):
                        selected = part
                        break
                if selected is None:
                    selected = node
                path = selected.text.decode("utf-8", "replace")
                line = source[: selected.start_byte].count(b"\n") + 1
                found.append((line, path))
        for child in node.named_children:
            visit(child, blocked)

    visit(tree.root_node)
    return sorted(set(found))


def rust_files(root):
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            "*.rs",
        ],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    )
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        path = root / raw_path.decode("utf-8", "surrogateescape")
        if path.is_file():
            yield path


def main():
    root = Path(__file__).resolve().parents[2]
    parser = Parser(Language(tree_sitter_rust.language()))
    violations = []
    for path in rust_files(root):
        source = path.read_bytes()
        for line, qualified_path in deep_paths(parser, source):
            violations.append((path.relative_to(root), line, qualified_path))

    if violations:
        print("Crate-qualified Rust paths in code must be imported at module scope:")
        for path, line, qualified_path in violations:
            print(f"  {path}:{line}: {qualified_path}")
        print("Use a module-scope `use` and refer to the imported name.")
        return 1

    print("No unimported crate-qualified Rust paths found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
