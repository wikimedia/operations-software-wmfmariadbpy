import os.path

import pytest

import wmfmariadbpy.dbutil as dbutil


@pytest.fixture(autouse=True)
def set_dbutil_section_ports(monkeypatch):
    # This is a bit ugly, but needed to make it work without being dependent
    # on the current dir.
    monkeypatch.setenv(
        dbutil.DBUTIL_SECTION_PORTS_ENV,
        os.path.join(os.path.dirname(__file__), "..", "section_ports.csv"),
    )


def test_read_section_ports_list():
    port2sec, sec2port = dbutil.read_section_ports_list()
    assert port2sec[10112] == "f2"
    assert sec2port["alpha"] == 10320


@pytest.mark.parametrize(
    "path,setenv,expected",
    [
        (None, False, "/default"),
        (None, True, "/env"),
        ("/path", False, "/path"),
        ("/path", True, "/path"),
    ],
)
def test_read_section_ports_list_env_path(monkeypatch, mocker, path, setenv, expected):
    # Unset the env var so this test is hermetic.
    monkeypatch.delenv(dbutil.DBUTIL_SECTION_PORTS_ENV, raising=False)
    mocker.patch("wmfmariadbpy.dbutil.SECTION_PORT_LIST_FILE", "/default")
    m = mocker.patch("builtins.open", mocker.mock_open())
    if setenv:
        monkeypatch.setenv(dbutil.DBUTIL_SECTION_PORTS_ENV, "/env")
    dbutil.read_section_ports_list(path=path)
    m.assert_called_once_with(expected, mode="r", newline="")


@pytest.mark.parametrize(
    "section,port",
    [
        ("alpha", 10320),
        ("nonexistent", 3306),
        (None, 3306),
    ],
)
def test_get_port_from_section(section, port):
    assert dbutil.get_port_from_section(section) == port


@pytest.mark.parametrize(
    "port, section",
    [
        (10112, "f2"),
        (10320, "alpha"),
        (655359, None),
        (-1, None),
        (0, None),
        (None, None),
    ],
)
def test_get_section_from_port(port, section):
    assert dbutil.get_section_from_port(port) == section


@pytest.mark.parametrize(
    "port, datadir",
    [
        (10110, "/srv/sqldata.f0"),
        (10320, "/srv/sqldata.alpha"),
        (101, "/srv/sqldata"),
        (None, "/srv/sqldata"),
    ],
)
def test_get_datadir_from_port(port, datadir):
    assert dbutil.get_datadir_from_port(port) == datadir


@pytest.mark.parametrize(
    "port, sock",
    [
        (10111, "/run/mysqld/mysqld.f1.sock"),
        (101, "/run/mysqld/mysqld.sock"),
        (None, "/run/mysqld/mysqld.sock"),
    ],
)
def test_get_socket_from_port(port, sock):
    assert dbutil.get_socket_from_port(port) == sock


@pytest.mark.parametrize(
    "target, host, port",
    [
        ("localhost", "localhost", 3306),
        ("localhost:3311", "localhost", 3311),
        ("localhost:f1", "localhost", 10111),
        ("db1001", "db1001.eqiad.wmnet", 3306),
        ("db5999:3321", "db5999.eqsin.wmnet", 3321),
        ("db4999:alpha", "db4999.ulsfo.wmnet", 10320),
        ("db2001.codfw.wmnet", "db2001.codfw.wmnet", 3306),
        ("dbmonitor1001.wikimedia.org", "dbmonitor1001.wikimedia.org", 3306),
    ],
)
def test_resolve(target, host, port):
    assert dbutil.resolve(target) == (host, port)
