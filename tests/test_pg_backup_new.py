"""Native-tool command contracts; these fixtures are not a PostgreSQL restore test."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from a2a_mq.sqlite_archive import sha256
from a2a_mq.domain import PermanentError
from a2a_mq.pg_backup import backup_postgres, restore_postgres, service_environment
from a2a_mq.configuration import https_issuer


@pytest.fixture
def config(tmp_path):
    path=tmp_path/'pgservice'
    path.write_text('[a2amq-backup]\nhost=db.example\ndbname=source\nuser=reader\npassword=secret\nsslmode=verify-full\nsslrootcert=/ca.pem\n'
        '[a2amq-restore]\nhost=db.example\ndbname=restored-new\nuser=writer\npassword=secret\nsslmode=verify-full\nsslrootcert=/ca.pem\n')
    path.chmod(0o600)
    return path


def test_private_service_snapshot_removes_ambient_pg_settings(config,monkeypatch):
    monkeypatch.setenv('PGPASSWORD','unrelated-secret')
    with service_environment(config,'a2amq-backup') as (env,database):
        assert database=='source' and 'PGPASSWORD' not in env
        private=Path(env['PGSERVICEFILE'])
        assert private.stat().st_mode & 0o077==0
    assert not private.exists()


def test_native_backup_atomic_file_and_no_credentials_in_argv(config,tmp_path,monkeypatch):
    calls=[]
    def run(args,**kw):
        calls.append(args)
        if args[0]=='pg_dump':
            Path(args[args.index('--file')+1]).write_bytes(b'PGDMP-test-fixture')
        return SimpleNamespace(returncode=0,stdout=b'')
    monkeypatch.setattr('a2a_mq.pg_backup.subprocess.run',run)
    out=tmp_path/'out.dump'
    result=backup_postgres(config,out)
    assert result['sha256']==sha256(out)
    assert out.stat().st_mode & 0o077==0
    assert not any('secret' in arg for args in calls for arg in args)
    with pytest.raises(PermanentError,match='DESTINATION_EXISTS'):
        backup_postgres(config,out)


@pytest.mark.parametrize('count',['1','99','not-a-count'])
def test_restore_rejects_nonempty_or_unknown_target(config,tmp_path,monkeypatch,count):
    source=tmp_path/'in.dump'
    source.write_bytes(b'PGDMP-fixture')
    calls=[]
    def run(args,**kw):
        calls.append(args)
        return SimpleNamespace(returncode=0,stdout=count.encode())
    monkeypatch.setattr('a2a_mq.pg_backup.subprocess.run',run)
    with pytest.raises(PermanentError,match='TARGET_NOT_EMPTY'):
        restore_postgres(source,config,expected_sha256=sha256(source),confirm_database='restored-new')
    assert all(args[0]!='pg_restore' for args in calls)


def test_restore_single_transaction_never_drops_or_creates_database(config,tmp_path,monkeypatch):
    source=tmp_path/'in.dump'
    source.write_bytes(b'PGDMP-fixture')
    calls=[]
    def run(args,**kw):
        calls.append(args)
        return SimpleNamespace(returncode=0,stdout=b'0')
    monkeypatch.setattr('a2a_mq.pg_backup.subprocess.run',run)
    assert restore_postgres(source,config,expected_sha256=sha256(source),confirm_database='restored-new')['restored']
    assert '--single-transaction' in calls[-1] and '--clean' not in calls[-1] and '--create' not in calls[-1]


def test_restore_confirmation_checked_before_client_execution(config,tmp_path,monkeypatch):
    source=tmp_path/'in.dump'
    source.write_bytes(b'PGDMP-fixture')
    def run(*args,**kwargs):
        pytest.fail('native client must not execute')
    monkeypatch.setattr('a2a_mq.pg_backup.subprocess.run',run)
    with pytest.raises(PermanentError,match='NOT_CONFIRMED'):
        restore_postgres(source,config,expected_sha256=sha256(source),confirm_database='source')


@pytest.mark.parametrize('issuer',['https://idp.example/realms/a','https://idp.example/realm/'])
def test_oidc_issuer_preserves_exact_path_and_trailing_slash(issuer):
    assert https_issuer(issuer)==issuer


@pytest.mark.parametrize('issuer',['http://idp/a','https://u:p@idp/a','https://idp/a?b','https://idp/a#b'])
def test_issuer_rejects_insecure_or_ambiguous_urls(issuer):
    with pytest.raises(PermanentError):
        https_issuer(issuer)


def test_restore_uses_private_verified_archive_not_mutable_source(config,tmp_path,monkeypatch):
    source=tmp_path/"in.dump"
    source.write_bytes(b"PGDMP-original")
    expected=sha256(source)
    consumed=[]
    def run(args,**kwargs):
        if args[0]=="psql":
            source.write_bytes(b"replacement-after-verification")
        elif args[0]=="pg_restore":
            private=Path(args[-1])
            assert private!=source and private.stat().st_mode & 0o077==0
            consumed.append(private.read_bytes())
        return SimpleNamespace(returncode=0,stdout=b"0")
    monkeypatch.setattr("a2a_mq.pg_backup.subprocess.run",run)
    restore_postgres(source,config,expected_sha256=expected,confirm_database="restored-new")
    assert consumed==[b"PGDMP-original",b"PGDMP-original"]
