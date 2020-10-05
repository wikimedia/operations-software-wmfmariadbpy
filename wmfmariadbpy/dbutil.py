import configparser
import csv
import ipaddress
import os
import pwd
import re
import socket
from typing import Dict, Optional, Tuple, Union, cast

SECTION_PORT_LIST_FILE = "/etc/wmfmariadbpy/section_ports.csv"
DBUTIL_SECTION_PORTS_ENV = "DBUTIL_SECTION_PORTS"


def read_section_ports_list(
    path: Optional[str] = None,
) -> Tuple[Dict[int, str], Dict[str, int]]:
    """
    Reads the list of section and port assignment file and returns two dictionaries,
    one for the section -> port assignment, and the other with the port -> section
    assignment.
    """
    if path is None:
        path = os.getenv(DBUTIL_SECTION_PORTS_ENV, SECTION_PORT_LIST_FILE)
    port2sec = {}
    sec2port = {}
    # Use cast() to make mypy happy.
    with open(
        cast(Union[Union[str, bytes], int], path), mode="r", newline=""
    ) as section_port_list:
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
    if host == "localhost":
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


def resolve(host: str, port: int = 3306) -> Tuple[str, int]:
    """
    Return the full qualified domain name for a database hostname. Normally
    this return the hostname itself, except in the case where the
    datacenter and network parts have been omitted, in which case, it is
    completed as a best effort.
    If the original address is an IPv4 or IPv6 address, leave it as is
    """
    if ":" in host:
        # we do not support ipv6 yet
        host, port_sec = host.split(":")
        try:
            port = int(port_sec)
        except ValueError:
            port = get_port_from_section(port_sec)
    try:
        ipaddress.ip_address(host)
        return (host, port)
    except ValueError:
        pass

    if "." not in host and host != "localhost":
        domain = ""
        if re.match("^[a-z]+1[0-9][0-9][0-9]$", host) is not None:
            domain = ".eqiad.wmnet"
        elif re.match("^[a-z]+2[0-9][0-9][0-9]$", host) is not None:
            domain = ".codfw.wmnet"
        elif re.match("^[a-z]+3[0-9][0-9][0-9]$", host) is not None:
            domain = ".esams.wmnet"
        elif re.match("^[a-z]+4[0-9][0-9][0-9]$", host) is not None:
            domain = ".ulsfo.wmnet"
        elif re.match("^[a-z]+5[0-9][0-9][0-9]$", host) is not None:
            domain = ".eqsin.wmnet"
        else:
            localhost_fqdn = socket.getfqdn()
            if "." in localhost_fqdn and len(localhost_fqdn) > 1:
                domain = localhost_fqdn[localhost_fqdn.index(".") :]
        host = host + domain
    return (host, port)
