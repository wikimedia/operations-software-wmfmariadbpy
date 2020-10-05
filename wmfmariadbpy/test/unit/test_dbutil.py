import pytest

import wmfmariadbpy.dbutil as dbutil

csv_data = ("s1, 1", "s2, 2", "m3, 100")


@pytest.fixture
def mock_csv(mocker):
    m = mocker.patch("builtins.open", mocker.mock_open())
    # XXX(kormat): Work-around python < 3.7's mock_open not supporting iteration.
    # https://bugs.python.org/issue21258#msg285508
    m.return_value.__iter__ = lambda self: iter(csv_data)


def test_read_section_ports_list(mock_csv):
    port2sec, sec2port = dbutil.read_section_ports_list()
    assert port2sec[2] == "s2"
    assert sec2port["m3"] == 100


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
    mocker.patch("wmfmariadbpy.dbutil.SECTION_PORT_LIST_FILE", "/default")
    m = mocker.patch("builtins.open", mocker.mock_open())
    if setenv:
        monkeypatch.setenv(dbutil.DBUTIL_SECTION_PORTS_ENV, "/env")
    dbutil.read_section_ports_list(path=path)
    m.assert_called_once_with(expected, mode="r", newline="")


@pytest.mark.parametrize(
    "section,port",
    [
        ("m3", 100),
        ("nonexistent", 3306),
        (None, 3306),
    ],
)
def test_get_port_from_section(mock_csv, section, port):
    assert dbutil.get_port_from_section(section) == port


@pytest.mark.parametrize(
    "port, section",
    [
        (2, "s2"),
        (100, "m3"),
        (655359, None),
        (-1, None),
        (0, None),
        (None, None),
    ],
)
def test_get_section_from_port(mock_csv, port, section):
    assert dbutil.get_section_from_port(port) == section


@pytest.mark.parametrize(
    "port, datadir",
    [
        (1, "/srv/sqldata.s1"),
        (100, "/srv/sqldata.m3"),
        (101, "/srv/sqldata"),
        (None, "/srv/sqldata"),
    ],
)
def test_get_datadir_from_port(mock_csv, port, datadir):
    assert dbutil.get_datadir_from_port(port) == datadir


@pytest.mark.parametrize(
    "port, sock",
    [
        (2, "/run/mysqld/mysqld.s2.sock"),
        (101, "/run/mysqld/mysqld.sock"),
        (None, "/run/mysqld/mysqld.sock"),
    ],
)
def test_get_socket_from_port(mock_csv, port, sock):
    assert dbutil.get_socket_from_port(port) == sock


@pytest.mark.parametrize(
    "target, host, port",
    [
        ("localhost", "localhost", 3306),
        ("localhost:3311", "localhost", 3311),
        ("localhost:s1", "localhost", 1),
        ("db1001", "db1001.eqiad.wmnet", 3306),
        ("db5999:3321", "db5999.eqsin.wmnet", 3321),
        ("db4999:m3", "db4999.ulsfo.wmnet", 100),
        ("db2001.codfw.wmnet", "db2001.codfw.wmnet", 3306),
        ("dbmonitor1001.wikimedia.org", "dbmonitor1001.wikimedia.org", 3306),
    ],
)
def test_resolve(mock_csv, target, host, port):
    assert dbutil.resolve(target) == (host, port)
