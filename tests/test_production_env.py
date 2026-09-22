import os

import pytest

from a2a_mq.domain import PermanentError
from a2a_mq.production_env import _bool, _secret, _storage_config


def test_storage_backend_requires_exactly_one_choice(tmp_path):
    with pytest.raises(PermanentError, match="EXACTLY_ONE_STORAGE_BACKEND_REQUIRED"):
        _storage_config({})
    with pytest.raises(PermanentError, match="EXACTLY_ONE_STORAGE_BACKEND_REQUIRED"):
        _storage_config({"A2A_SQLITE_PATH": str(tmp_path / "db"),
                         "A2A_POSTGRES_DSN": "postgresql://db/example"})
    path, dsn, insecure = _storage_config({"A2A_SQLITE_PATH": str(tmp_path / "db")})
    assert path == str(tmp_path / "db") and dsn is None and insecure is False


def test_direct_postgres_conninfo_allows_spaces_but_not_control_lines():
    dsn = "host=db.internal port=5432 dbname=a2amq user=app sslmode=verify-full sslrootcert=/ca.pem"
    path, actual, insecure = _storage_config({"A2A_POSTGRES_DSN": dsn})
    assert path is None and actual == dsn and insecure is False
    for invalid in (dsn + "\npassword=x", dsn + "\rpassword=x", "host=db\x00dbname=a2amq"):
        with pytest.raises(PermanentError, match="INVALID_POSTGRES_DSN"):
            _storage_config({"A2A_POSTGRES_DSN": invalid})


def test_secret_configuration_is_unambiguous_and_file_is_permission_checked(tmp_path):
    secret = tmp_path / "token"
    secret.write_text("mounted-token\n", encoding="ascii")
    os.chmod(secret, 0o600)
    assert _secret({"TOKEN_FILE": str(secret)}, "TOKEN", "TOKEN_FILE", required=True) == "mounted-token"
    with pytest.raises(PermanentError, match="AMBIGUOUS_SECRET_CONFIG"):
        _secret({"TOKEN": "direct", "TOKEN_FILE": str(secret)}, "TOKEN", "TOKEN_FILE", required=True)


@pytest.mark.parametrize("raw,expected", [("true", True), ("false", False)])
def test_boolean_configuration_is_explicit(raw, expected):
    assert _bool({"FLAG": raw}, "FLAG") is expected


def test_boolean_configuration_rejects_loose_truthiness():
    for value in ("1", "TRUE", "yes", ""):
        with pytest.raises(PermanentError, match="INVALID_CONFIG"):
            _bool({"FLAG": value}, "FLAG")
