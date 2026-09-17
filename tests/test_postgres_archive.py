import importlib.util
from pathlib import Path
import subprocess

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "postgres_archive.py"
spec = importlib.util.spec_from_file_location("postgres_archive", SCRIPT)
assert spec is not None and spec.loader is not None
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)


def test_service_names_are_bounded_and_not_connection_strings():
    assert archive.service("prod-primary") == "prod-primary"
    for value in ("", "service=prod", "prod password=secret", "../prod", "x" * 129):
        with pytest.raises(archive.ArchiveError, match="INVALID_SERVICE"):
            archive.service(value)


def test_backup_argv_contains_service_only_not_secret(monkeypatch, tmp_path):
    commands = []
    output = tmp_path / "backup.dump"

    monkeypatch.setattr(archive, "binary", lambda name: "/usr/bin/" + name)

    def fake_run(argv, *, capture=True):
        commands.append(argv)
        if argv[0].endswith("pg_dump"):
            Path(argv[argv.index("--file") + 1]).write_bytes(b"fixture")
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 0, "SCHEMA - a2amq owner\n", "")

    monkeypatch.setattr(archive, "run", fake_run)
    result = archive.backup("prod", output)
    flattened = " ".join(part for command in commands for part in command)
    assert "service=prod" in flattened and "password" not in flattened.lower()
    assert output.read_bytes() == b"fixture" and result["service"] == "prod"


def test_restore_refuses_nonempty_target_before_pg_restore(monkeypatch, tmp_path):
    source = tmp_path / "backup.dump"
    source.write_bytes(b"fixture")
    calls = []
    monkeypatch.setattr(archive, "binary", lambda name: "/usr/bin/" + name)

    def fake_run(argv, *, capture=True):
        calls.append(argv)
        if argv[0].endswith("psql"):
            return subprocess.CompletedProcess(argv, 0, "1\n", "")
        return subprocess.CompletedProcess(argv, 0, "SCHEMA - a2amq owner\n", "")

    monkeypatch.setattr(archive, "run", fake_run)
    with pytest.raises(archive.ArchiveError, match="RESTORE_TARGET_NOT_EMPTY"):
        archive.restore("target", source, confirm_empty=True)
    assert not any(call[0].endswith("pg_restore") and "--dbname" in call for call in calls)
