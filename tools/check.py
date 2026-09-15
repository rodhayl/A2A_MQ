"""Offline repository checks; not a substitute for Ruff, mypy or live tests."""
from pathlib import Path
import ast
import json
import re
import tomllib

root = Path(__file__).resolve().parents[1]
assert (root / "spec/envelope.schema.json").read_bytes() == (root / "src/a2a_mq/schema.json").read_bytes()
for path in list((root / "src").rglob("*.py")) + list((root / "tests").rglob("*.py")):
    ast.parse(path.read_text(), filename=str(path))
for path in (root / "spec").rglob("*.json"):
    json.loads(path.read_text())
tomllib.loads((root / "pyproject.toml").read_text())
for path in [root / "README.md", *list((root / "docs").rglob("*.md"))]:
    for target in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", path.read_text()):
        if "://" not in target:
            assert (path.parent / target).exists(), (path, target)
print("Repository syntax, JSON, schema-copy and local-link checks passed")
