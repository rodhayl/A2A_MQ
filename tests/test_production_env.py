import os

import pytest

from a2a_mq.domain import PermanentError
import a2a_mq.production_env as production_env
from a2a_mq.production_env import (_bool, _dapr_grpc_port, _dapr_http_endpoint,
                                   _mcp_resource_url, _secret, _storage_config)


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


def test_private_postgres_conninfo_file_allows_spaces(tmp_path):
    dsn = tmp_path / "postgres.dsn"
    value = "host=db.internal port=5432 dbname=a2amq user=app sslmode=verify-full sslrootcert=/ca.pem"
    dsn.write_text(value + "\n", encoding="ascii")
    os.chmod(dsn, 0o600)
    path, actual, insecure = _storage_config({"A2A_POSTGRES_DSN_FILE": str(dsn)})
    assert path is None and actual == value and insecure is False


@pytest.mark.parametrize("value", [
    "http://agent.example/mcp",
    "https://agent.example/not-mcp",
    "https://agent.example//mcp",
    "https://agent.example/mcp ",
])
def test_mcp_resource_url_is_exact_https_endpoint(value):
    with pytest.raises(PermanentError, match="MCP_HTTPS_RESOURCE_REQUIRED"):
        _mcp_resource_url(value)


@pytest.mark.parametrize("env,code", [
    ({"A2A_PUBLIC_URL":"http://agent.example"}, "PUBLIC_HTTPS_URL_REQUIRED"),
    ({"A2A_PUBLIC_URL":"https://agent.example",
      "A2A_MCP_RESOURCE_URL":"http://agent.example/mcp"}, "MCP_HTTPS_RESOURCE_REQUIRED"),
    ({"A2A_PUBLIC_URL":"https://agent.example",
      "A2A_JWT_ISSUER":"http://issuer.example"}, "HTTPS_ISSUER_REQUIRED"),
])
def test_api_factory_rejects_unsafe_identity_urls_before_storage(monkeypatch,env,code):
    monkeypatch.setattr(
        production_env, "_stores",
        lambda *a, **k: pytest.fail("storage must not open before identity URL validation"),
    )
    with pytest.raises(PermanentError, match=code):
        production_env.create_api_from_env(env)


@pytest.mark.parametrize("value", [
    "https://dapr.example:3500",
    "http://10.0.0.5:3500",
    "http://user@127.0.0.1:3500",
    "http://127.0.0.1:3500/path",
    "http://127.0.0.1:70000",
])
def test_production_dapr_endpoint_is_colocated(value):
    with pytest.raises(PermanentError, match="COLOCATED_DAPR_REQUIRED"):
        _dapr_http_endpoint(value)
    assert _dapr_http_endpoint("http://127.0.0.1:3500") == "http://127.0.0.1:3500"
    assert _dapr_http_endpoint("http://[::1]:3500/") == "http://[::1]:3500"


@pytest.mark.parametrize("env", [
    {"DAPR_GRPC_ENDPOINT":"http://127.0.0.1:50001"},
    {"DAPR_GRPC_ENDPOINT":"http://dapr.example:50001"},
    {"DAPR_GRPC_PORT":"0"},
    {"DAPR_GRPC_PORT":"70000"},
    {"DAPR_GRPC_PORT":"not-a-port"},
])
def test_production_workflow_grpc_uses_explicit_colocated_port(env):
    with pytest.raises(PermanentError, match="COLOCATED_DAPR_REQUIRED"):
        _dapr_grpc_port(env)
    assert _dapr_grpc_port({}) == "50001"
    assert _dapr_grpc_port({"DAPR_GRPC_PORT":"51001"}) == "51001"


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
