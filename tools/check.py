"""Offline repository checks; not a substitute for Ruff, mypy or live tests."""
from pathlib import Path
import ast
import json
import re
import tomllib

root = Path(__file__).resolve().parents[1]
assert (root / "spec/envelope.schema.json").read_bytes() == (root / "src/a2a_mq/schema.json").read_bytes()
assert (root / "spec/binding.json").read_bytes() == (root / "src/a2a_mq/binding.json").read_bytes()
for path in (list((root / "src").rglob("*.py")) + list((root / "tests").rglob("*.py"))
            + list((root / "deploy").rglob("*.py")) + list((root / "scripts").rglob("*.py"))
            + list((root / "tools").rglob("*.py"))):
    ast.parse(path.read_text(), filename=str(path))
for path in (root / "spec").rglob("*.json"):
    json.loads(path.read_text())
project = tomllib.loads((root / "pyproject.toml").read_text())
markdown = [
    root / "README.md",
    root / "CHANGELOG.md",
    root / "deploy/README.md",
    *list((root / "docs").rglob("*.md")),
]
for path in markdown:
    text = path.read_text()
    for target in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", text):
        if "://" not in target:
            assert (path.parent / target).exists(), (path, target)
    for target in re.findall(r"\bpython(?:3)?\s+(scripts/[A-Za-z0-9_./-]+\.py)\b", text):
        assert (root / target).is_file(), (path, target)

entry_points = project["project"]["scripts"]
for command in ("a2amq-local", "a2amq-local-workflow"):
    assert command in entry_points, command

required_user_files = (
    "scripts/smoke_local.py",
    "docs/CONFIGURATION.md",
    "docs/TROUBLESHOOTING.md",
    "docs/API_EXAMPLES.md",
    "docs/DEPLOYMENT.md",
    "examples/a2a/send-message.json",
    "examples/a2a/get-task.json",
    "examples/a2a/list-tasks.json",
    "examples/a2a/cancel-task.json",
    "examples/a2a/send-streaming-message.json",
    "examples/a2a/subscribe-task.json",
)
for relative in required_user_files:
    assert (root / relative).is_file(), relative

production_tree = ast.parse((root / "src/a2a_mq/production_env.py").read_text())
production_config = {
    node.value
    for node in ast.walk(production_tree)
    if isinstance(node, ast.Constant)
    and isinstance(node.value, str)
    and re.fullmatch(r"(?:A2A|DAPR|APP)_[A-Z0-9_]+", node.value)
}
configuration_doc = (root / "docs/CONFIGURATION.md").read_text()
missing_config = sorted(name for name in production_config if f"`{name}`" not in configuration_doc)
assert not missing_config, ("undocumented production configuration", missing_config)

runtime_text = (root / "src/a2a_mq/local_runtime.py").read_text()
bootstrap_text = (root / "scripts/bootstrap_local_tools.py").read_text()
runtime_dapr = re.search(r'^LOCAL_DAPR_VERSION = "([^"]+)"' + '$', runtime_text, re.M)
runtime_nats = re.search(r'^LOCAL_NATS_VERSION = "([^"]+)"' + '$', runtime_text, re.M)
bootstrap_dapr = re.search(r'^DAPR_VERSION = "([^"]+)"' + '$', bootstrap_text, re.M)
bootstrap_nats = re.search(r'^NATS_VERSION = "([^"]+)"' + '$', bootstrap_text, re.M)
assert runtime_dapr and bootstrap_dapr and runtime_dapr.group(1) == bootstrap_dapr.group(1)
assert runtime_nats and bootstrap_nats and runtime_nats.group(1) == bootstrap_nats.group(1)

print(
    "Repository syntax, JSON, schema/binding copies, docs links, user scripts "
    "and configuration coverage checks passed"
)
