import ipaddress
import os.path
import socket

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
    "ip",
    [
        ("192.0.2.1"),
        ("2001:db8::11"),
    ],
)
def test_resolve_ip(mocker, ip):
    m = mocker.patch("wmfmariadbpy.dbutil._resolve_ip")
    assert dbutil.resolve(ip) == m.return_value
    m.assert_called_once_with(ipaddress.ip_address(ip))


def test_resolve_host(mocker):
    m = mocker.patch("wmfmariadbpy.dbutil._dc_map")
    assert dbutil.resolve("db2099") == m.return_value
    m.assert_called_once_with("db2099")


@pytest.mark.parametrize(
    "ip, host",
    [
        ("127.0.0.1", "localhost"),
        ("::1", "localhost"),
        ("192.0.2.1", "host-192.0.2.1"),
        ("2001:db8::11", "host-2001:db8::11"),
    ],
)
def test__resolve_ip(mocker, ip, host):
    m = mocker.patch("wmfmariadbpy.dbutil.socket.gethostbyaddr")
    m.side_effect = lambda ip: ("host-%s" % ip, None, None)
    assert dbutil._resolve_ip(ipaddress.ip_address(ip)) == host


def test__resolve_ip_err(mocker):
    m = mocker.patch("wmfmariadbpy.dbutil.socket.gethostbyaddr")
    m.side_effect = socket.herror()
    with pytest.raises(ValueError):
        dbutil._resolve_ip(ipaddress.ip_address("1.1.1.1"))


@pytest.mark.parametrize(
    "host, fqdn",
    [
        ("es1024", "es1024.eqiad.wmnet"),
        ("db2085", "db2085.codfw.wmnet"),
        ("localhost", "localhost"),
    ],
)
def test__dc_map(host, fqdn):
    assert dbutil._dc_map(host) == fqdn


def test__dc_map_err():
    with pytest.raises(ValueError):
        dbutil._dc_map("pc9999")


@pytest.mark.parametrize(
    "addr, host, port",
    [
        ("2001:db8::11", "2001:db8::11", 3306),
        ("[2001:db8::11]", "2001:db8::11", 3306),
        ("[2001:db8::11]:3317", "2001:db8::11", 3317),
        ("[2001:db8::11]:f1", "2001:db8::11", 10111),
        ("192.0.2.1", "192.0.2.1", 3306),
        ("192.0.2.1:3317", "192.0.2.1", 3317),
        ("192.0.2.1:f1", "192.0.2.1", 10111),
        ("db2099", "db2099", 3306),
        ("db2099:3317", "db2099", 3317),
        ("db2099:f2", "db2099", 10112),
        ("db2099.codfw.wmnet", "db2099.codfw.wmnet", 3306),
        ("db2099.codfw.wmnet:3317", "db2099.codfw.wmnet", 3317),
        ("db2099.codfw.wmnet:alpha", "db2099.codfw.wmnet", 10320),
    ],
)
def test_addr_split(addr, host, port):
    assert dbutil.addr_split(addr) == (host, port)


@pytest.mark.parametrize(
    "addr",
    [("[2001:db8::11"), ("[2001:db8::11]::3")],
)
def test_addr_split_err(addr):
    with pytest.raises(ValueError):
        dbutil.addr_split(addr)


@pytest.mark.parametrize(
    "port_sec, port",
    [
        ("f0", 10110),
        ("alpha", 10320),
        ("1234", 1234),
        ("u7", 3306),
    ],
)
def test__port_sec_to_port(port_sec, port):
    assert dbutil._port_sec_to_port(port_sec) == port
