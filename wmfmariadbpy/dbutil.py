import configparser
import csv
import ipaddress
import os
import pwd
import re
import socket
import tempfile
from typing import Dict, Optional, Tuple, Union

SECTION_PORT_LIST_FILE = "/etc/wmfmariadbpy/section_ports.csv"
DBUTIL_SECTION_PORTS_TEST_DATA_ENV = "DBUTIL_SECTION_PORTS_TEST_DATA"
SECTION_PORTS_TEST_DATA = """\
f0, 10110
f1, 10111
f2, 10112
f3, 10113
alpha, 10320
"""


def read_section_ports_list(
    path: Optional[str] = None,
) -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    Reads the list of section and port assignment file and returns two dictionaries,
    one for the section -> port assignment, and the other with the port -> section
    assignment.
    """
    if path is None and DBUTIL_SECTION_PORTS_TEST_DATA_ENV in os.environ:
        tmpfile = tempfile.NamedTemporaryFile()
        tmpfile.write(SECTION_PORTS_TEST_DATA.encode("utf-8"))
        tmpfile.flush()
        path = tmpfile.name
    assert path is not None
    port2sec = {}
    sec2port = {}
    with open(path, mode="r", newline="") as section_port_list:
        reader = csv.reader(section_port_list)
        for row in reader:
            sec2port[row[0]] = int(row[1])
            port2sec[int(row[1])] = row[0]
    return port2sec, sec2port


def get_port_from_section(section: str) -> int:
    """
    Returns the port integer corresponding to the given section name. If the section
    is None, or an unrecognized one, return the default one (3306).
    """
    _, sec2port = read_section_ports_list()
    return sec2port.get(section, 3306)


def get_section_from_port(port: int) -> Optional[str]:
    """
    Returns the section name corresponding to the given port. If the port is the
    default one (3306) or an unknown one, return a null value.
    """
    port2sec, _ = read_section_ports_list()
    return port2sec.get(port, None)


def get_datadir_from_port(port: int) -> str:
    """
    Translates port number to expected datadir path
    """
    section = get_section_from_port(port)
    if section is None:
        return "/srv/sqldata"
    else:
        return "/srv/sqldata." + section


def get_socket_from_port(port: int) -> str:
    """
    Translates port number to expected socket location
    """
    section = get_section_from_port(port)
    if section is None:
        return "/run/mysqld/mysqld.sock"
    else:
        return "/run/mysqld/mysqld." + section + ".sock"


def get_credentials(
    host: str,
    port: int,
    database: str,
) -> Tuple[str, Optional[str], Optional[str], Optional[Dict[str, str]]]:
    """
    Given a database instance, return the authentication method, including
    the user, password, socket and ssl configuration.
    """
    pw = pwd.getpwuid(os.getuid())
    user_my_cnf = os.path.join(pw.pw_dir, ".my.cnf")
    mysql_sock = None  # type: Optional[str]
    if "TESTENV_MY_CNF" in os.environ:
        config = configparser.ConfigParser(interpolation=None, allow_no_value=True)
        config.read(os.environ["TESTENV_MY_CNF"])
        user = config["client"]["user"]
        password = config["client"]["password"]  # type: Optional[str]
        ssl = None
        mysql_sock = None
    elif host == "localhost":
        user = pw.pw_name
        # connnect to localhost using plugin_auth:
        config = configparser.ConfigParser(
            interpolation=None, allow_no_value=True, strict=False
        )
        config.read("/etc/my.cnf")
        mysql_sock = get_socket_from_port(port)
        ssl = None
        password = None
    elif host == "127.0.0.1":
        # connect to localhost throught the port without ssl
        config = configparser.ConfigParser(interpolation=None, allow_no_value=True)
        config.read(user_my_cnf)
        user = config["client"]["user"]
        password = config["client"]["password"]
        ssl = None
        mysql_sock = None
    elif not host.startswith("labsdb") and not host.startswith("clouddb"):
        # connect to a production remote host, use ssl and prod pass
        config = configparser.ConfigParser(interpolation=None, allow_no_value=True)
        config.read(user_my_cnf)
        user = config["client"]["user"]
        password = config["client"]["password"]
        ssl = {"ca": "/etc/ssl/certs/Puppet_Internal_CA.pem"}
        mysql_sock = None
    else:
        # connect to a labs remote host, use ssl and labs pass
        config = configparser.ConfigParser(interpolation=None)
        config.read(user_my_cnf)
        user = config["clientlabsdb"]["user"]
        password = config["clientlabsdb"]["password"]
        ssl = {"ca": "/etc/ssl/certs/Puppet_Internal_CA.pem"}
        mysql_sock = None

    return (user, password, mysql_sock, ssl)


def resolve(host: str) -> str:
    """
    Return the full qualified domain name for a database hostname. Normally
    this return the hostname itself, except in the case where the
    datacenter and network parts have been omitted, in which case, it is
    completed as a best effort.
    If the original address is an IPv4 or IPv6 address, leave it as is
    """
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return _resolve_ip(ip)

    return _dc_map(host)


def _resolve_ip(ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address]) -> str:
    if ip.is_loopback:
        return "localhost"
    try:
        host, _, _ = socket.gethostbyaddr(ip.compressed)
    except socket.herror:
        raise ValueError("Unable to resolve ip address: '%s'" % ip) from None
    return host


def _dc_map(host: str) -> str:
    dcs = {
        1: "eqiad",
        2: "codfw",
        3: "esams",
        4: "ulsfo",
        5: "eqsin",
    }
    dc_rx = re.compile(r"^[a-zA-Z]+(?P<dc_id>\d)\d{3}$")
    m = dc_rx.match(host)
    if not m:
        return socket.getfqdn(host)
    dc_id = int(m.group("dc_id"))
    if dc_id not in dcs:
        raise ValueError("Unknown datacenter ID '%d' (from '%s')" % (dc_id, host))
    return "%s.%s.wmnet" % (host, dcs[dc_id])


def addr_split(addr: str, def_port: int = 3306) -> Tuple[str, int]:
    """Split address into (host, port).

    Supports:
    - Plain ipv4: 192.0.2.1
    - ipv4+port: 192.0.2.1:3007
    - Plain ipv6: 2001:db8::11 or [2001:db8::11]
    - ipv6+port: [2001:db8::11]:3116
    - Plain hostname: db2034
    - Hostname+port: db2054.codfw.wmnet:3241

    Any port aliases (e.g. :s4) are mapped to the tcp port number.
    If the address doesn't contain a port, the def_port argument is used.
    No validation of the formatting of hostnames or ip addresses is done.

    Returns:
        Tuple(str, int): Host/IP + port.
    """
    port = def_port
    if addr.count(":") > 1:
        # IPv6
        if addr[0] == "[":
            # [ipv6]:port
            addr_port_rx = re.compile(r"^\[(?P<host>[^]]+)\](?::(?P<port>\w+))?$")
            m = addr_port_rx.match(addr)
            if not m:
                raise ValueError("Invalid [ipv6]:port format: '%s'" % addr)
            addr = m.group("host")
            port_sec = m.group("port")
            if port_sec is not None:
                port = _port_sec_to_port(port_sec)
        # plain ipv6
    elif ":" in addr:
        addr, port_sec = addr.split(":")
        port = _port_sec_to_port(port_sec)
    return addr, port


def _port_sec_to_port(port_sec: str) -> int:
    try:
        port = int(port_sec)
    except ValueError:
        port = get_port_from_section(port_sec)
    return port
