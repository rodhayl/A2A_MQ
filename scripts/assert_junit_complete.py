"""Validate required JUnit evidence; absent, inconsistent or skipped evidence fails."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import stat
import xml.etree.ElementTree as ET

MAX_BYTES = 8 * 1024 * 1024
OUTCOMES = ("failure", "error", "skipped")
ATTRS = ("tests", "failures", "errors", "skipped")


class EvidenceError(ValueError):
    """The evidence cannot establish a complete passing required test gate."""


def _tag(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _read(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    if path.is_symlink():
        raise EvidenceError("JUnit evidence must be a regular non-symlink file")
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise EvidenceError("JUnit evidence must be a regular file")
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise EvidenceError("JUnit evidence exceeds the byte limit")
    return raw


def counts(path: Path) -> tuple[int, int, int, int]:
    """Return actual case counts, rejecting unsafe XML and inconsistent summaries."""
    try:
        text = _read(path).decode("utf-8-sig", errors="strict")
        if "\x00" in text or re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", text, re.I):
            raise EvidenceError("DTD, entities and non-UTF-8 XML are not accepted")
        declaration = re.match(r"\s*<\?xml\s+[^?]*encoding\s*=\s*['\"]([^'\"]+)", text, re.I)
        if declaration and declaration[1].lower() not in {"utf-8", "utf8"}:
            raise EvidenceError("JUnit evidence must use UTF-8")
        root = ET.fromstring(text)
    except (OSError, UnicodeError, ET.ParseError) as exc:
        raise EvidenceError(f"Unreadable JUnit evidence: {type(exc).__name__}") from exc
    if _tag(root) not in {"testsuites", "testsuite"}:
        raise EvidenceError("JUnit root must be testsuite or testsuites")
    parents = {child: parent for parent in root.iter() for child in parent}
    cases = [node for node in root.iter() if _tag(node) == "testcase"]
    seen: set[tuple[str, str]] = set()
    for case in cases:
        parent = parents.get(case)
        if parent is None or _tag(parent) != "testsuite":
            raise EvidenceError("testcase must be a direct child of testsuite")
        identity = (case.get("classname", ""), case.get("name", ""))
        if not identity[1] or identity in seen:
            raise EvidenceError("Test cases must have unique, nonempty identities")
        seen.add(identity)
        if sum(_tag(child) in OUTCOMES for child in case) > 1:
            raise EvidenceError("A test case has multiple outcomes")
    for node in root.iter():
        if _tag(node) in OUTCOMES:
            parent = parents.get(node)
            if parent is None or _tag(parent) != "testcase":
                raise EvidenceError("Suite-level or misplaced test outcome")
    result = (len(cases), *(sum(any(_tag(c) == key for c in case) for case in cases)
                             for key in OUTCOMES))
    for suite in root.iter():
        if _tag(suite) not in {"testsuite", "testsuites"}:
            continue
        members = [node for node in suite.iter() if _tag(node) == "testcase"]
        actual = (len(members), *(sum(any(_tag(c) == key for c in case) for case in members)
                                 for key in OUTCOMES))
        for key, value in zip(ATTRS, actual, strict=True):
            declared = suite.get(key)
            if declared is not None and (not re.fullmatch(r"[0-9]+", declared)
                                         or int(declared) != value):
                raise EvidenceError(f"JUnit summary disagrees with cases: {key}")
    return result


def require_complete(path: Path) -> tuple[int, int, int, int]:
    result = counts(path)
    if result[0] == 0 or any(result[1:]):
        raise EvidenceError(f"Required gate incomplete: tests/failures/errors/skips={result}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    failed = False
    for path in args.paths:
        try:
            total, failures, errors, skipped = require_complete(path)
            print(f"{path}: tests={total} failures={failures} errors={errors} skipped={skipped}")
        except EvidenceError as exc:
            print(f"{path}: INCOMPLETE: {exc}")
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
