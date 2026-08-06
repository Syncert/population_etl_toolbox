"""Enforce coverage for executable application lines changed from a Git base."""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
from collections import Counter
from pathlib import Path

HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
APPLICATION_PREFIXES = ("apps/", "src/")


def _changed_lines(base_ref: str) -> dict[str, set[int]]:
    result = subprocess.run(
        [
            "git",
            "diff",
            "--ignore-all-space",
            "--unified=0",
            "--no-color",
            f"{base_ref}...HEAD",
            "--",
            "*.py",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    changed: dict[str, set[int]] = {}
    current: str | None = None
    for line in result.stdout.splitlines():
        if line.startswith("+++ b/"):
            current = line[6:].replace("\\", "/")
            if not current.startswith(APPLICATION_PREFIXES):
                current = None
            continue
        match = HUNK.match(line)
        if current and match:
            start = int(match.group(1))
            count = int(match.group(2) or "1")
            changed.setdefault(current, set()).update(range(start, start + count))
    return changed


def _coverage_files(path: Path) -> dict[str, dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        name.replace("\\", "/"): details
        for name, details in payload.get("files", {}).items()
    }


def _git_output(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout


def _statement_signature(node: ast.AST) -> str:
    """Return a location-free signature without folding nested statement bodies."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        value = (
            type(node).__name__,
            node.name,
            ast.dump(node.args, include_attributes=False),
            tuple(
                ast.dump(item, include_attributes=False) for item in node.decorator_list
            ),
            ast.dump(node.returns, include_attributes=False) if node.returns else None,
        )
        return repr(value)
    if isinstance(node, ast.ClassDef):
        value = (
            type(node).__name__,
            node.name,
            tuple(ast.dump(item, include_attributes=False) for item in node.bases),
            tuple(ast.dump(item, include_attributes=False) for item in node.keywords),
            tuple(
                ast.dump(item, include_attributes=False) for item in node.decorator_list
            ),
        )
        return repr(value)
    if isinstance(node, (ast.If, ast.While)):
        return repr(
            (type(node).__name__, ast.dump(node.test, include_attributes=False))
        )
    if isinstance(node, (ast.For, ast.AsyncFor)):
        return repr(
            (
                type(node).__name__,
                ast.dump(node.target, include_attributes=False),
                ast.dump(node.iter, include_attributes=False),
            )
        )
    if isinstance(node, (ast.With, ast.AsyncWith)):
        return repr(
            (
                type(node).__name__,
                tuple(
                    (
                        ast.dump(item.context_expr, include_attributes=False),
                        ast.dump(item.optional_vars, include_attributes=False)
                        if item.optional_vars
                        else None,
                    )
                    for item in node.items
                ),
            )
        )
    if isinstance(node, ast.ExceptHandler):
        return repr(
            (
                type(node).__name__,
                ast.dump(node.type, include_attributes=False) if node.type else None,
                node.name,
            )
        )
    if isinstance(node, (ast.Try, ast.TryStar)):
        return repr(
            (
                type(node).__name__,
                len(node.handlers),
                bool(node.orelse),
                bool(node.finalbody),
            )
        )
    if isinstance(node, ast.Match):
        return repr(
            (type(node).__name__, ast.dump(node.subject, include_attributes=False))
        )
    return ast.dump(node, include_attributes=False)


def _statements(source: str, filename: str) -> list[ast.AST]:
    tree = ast.parse(source, filename=filename)
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.stmt, ast.ExceptHandler))
    ]


def _semantic_changed_lines(base_ref: str, filenames: set[str]) -> dict[str, set[int]]:
    merge_base = _git_output("merge-base", base_ref, "HEAD").strip()
    result: dict[str, set[int]] = {}
    for filename in filenames:
        current_path = Path(filename)
        if not current_path.is_file():
            continue
        current_nodes = _statements(current_path.read_text(encoding="utf-8"), filename)
        base_source = subprocess.run(
            ["git", "show", f"{merge_base}:{filename}"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        base_signatures: Counter[str] = Counter()
        if base_source.returncode == 0:
            base_signatures.update(
                _statement_signature(node)
                for node in _statements(base_source.stdout, filename)
            )

        for node in sorted(current_nodes, key=lambda item: item.lineno):
            signature = _statement_signature(node)
            if base_signatures[signature]:
                base_signatures[signature] -= 1
            else:
                result.setdefault(filename, set()).add(node.lineno)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--minimum", type=float, default=80.0)
    args = parser.parse_args()

    textual_changes = _changed_lines(args.base_ref)
    changed = _semantic_changed_lines(args.base_ref, set(textual_changes))
    coverage = _coverage_files(args.coverage)
    executable = 0
    covered = 0
    missed: list[str] = []

    for filename, lines in sorted(changed.items()):
        details = coverage.get(filename)
        if details is None:
            continue
        executed = set(details.get("executed_lines", []))
        missing = set(details.get("missing_lines", []))
        statements = executed | missing
        changed_statements = lines & statements
        executable += len(changed_statements)
        covered += len(changed_statements & executed)
        missed.extend(
            f"{filename}:{line}" for line in sorted(changed_statements & missing)
        )

    percentage = 100.0 if executable == 0 else covered / executable * 100
    print(
        f"Changed executable line coverage: {covered}/{executable} "
        f"({percentage:.2f}%, required {args.minimum:.2f}%)"
    )
    if missed:
        print("Uncovered changed lines:")
        print("\n".join(missed))
    return int(percentage < args.minimum)


if __name__ == "__main__":
    raise SystemExit(main())
