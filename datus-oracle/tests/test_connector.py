import pytest

from datus_oracle import OracleConfig, OracleConnector


def test_config_requires_identifier():
    with pytest.raises(ValueError):
        OracleConfig(host="localhost", port=1521, username="scott", password="tiger")


def test_connection_string_service_name():
    config = OracleConfig(
        host="localhost",
        port=1521,
        database="FREEPDB1",
        username="scott",
        password="tiger",
    )
    connector = OracleConnector(config)
    expected = "oracle+oracledb://scott:tiger@localhost:1521/?service_name=FREEPDB1"
    assert connector.connection_string == expected
    assert connector.dialect == "oracle"


def test_connection_string_sid():
    config = OracleConfig(
        host="localhost",
        port=1521,
        sid="XE",
        username="hr",
        password="secret",
    )
    connector = OracleConnector(config)
    expected = "oracle+oracledb://hr:secret@localhost:1521/?sid=XE"
    assert connector.connection_string == expected


def test_full_name_quotes_identifiers():
    config = OracleConfig(
        host="localhost",
        port=1521,
        sid="XE",
        username="hr",
        password="secret",
    )
    connector = OracleConnector(config)

    full_name = connector.full_name(schema_name="hr", table_name='Employees "Temp"')
    assert full_name == '"hr"."Employees ""Temp"""'


def test_switch_context_uses_alter_session(monkeypatch):
    config = OracleConfig(
        host="localhost",
        port=1521,
        sid="XE",
        username="hr",
        password="secret",
    )
    connector = OracleConnector(config)

    executed: list[str] = []

    class DummyConnection:
        def begin(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            executed.append(str(sql))

    connector.connection = DummyConnection()
    monkeypatch.setattr(connector, "connect", lambda: None)

    connector.do_switch_context(schema_name="analytics")
    assert any("ALTER SESSION SET CURRENT_SCHEMA = \"ANALYTICS\"" in stmt for stmt in executed)


def test_init_oracle_client_called(monkeypatch):
    calls = {"init": False, "init_kwargs": None, "thin_checked": False}

    class DummyOracle:
        def is_thin_mode(self):
            calls["thin_checked"] = True
            return True

        def init_oracle_client(self, **kwargs):
            calls["init"] = True
            calls["init_kwargs"] = kwargs

    dummy = DummyOracle()
    monkeypatch.setattr("datus_oracle.connector.oracledb", dummy)

    config = OracleConfig(
        host="localhost",
        port=1521,
        database="FREEPDB1",
        username="user",
        password="pass",
        client_lib_dir="/opt/oracle/instantclient",
    )
    OracleConnector(config)

    assert calls["thin_checked"] is True
    assert calls["init"] is True
    assert calls["init_kwargs"] == {"lib_dir": "/opt/oracle/instantclient"}
