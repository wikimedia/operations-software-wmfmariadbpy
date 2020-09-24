import configparser
import csv
import glob
import ipaddress
import os
import pwd
import re
import socket

SECTION_PORT_LIST_FILE = "/etc/wmfmariadbpy/section_ports.csv"


def read_section_ports_list(path=SECTION_PORT_LIST_FILE):
    """
    Reads the list of section and port assignment file and returns two dictionaries,
    one for the section -> port assignment, and the other with the port -> section
    assignment.
    """
    sections = {}
    ports = {}
    with open(path, mode="r", newline="") as section_port_list:
        reader = csv.reader(section_port_list)
        for row in reader:
            ports[row[0]] = int(row[1])
            sections[int(row[1])] = row[0]
    return sections, ports


def get_port_from_section(section):
    """
    Returns the port integer corresponding to the given section name. If the section
    is None, or an unrecognized one, return the default one (3306).
    """
    _, ports = read_section_ports_list()
    return ports.get(section, 3306)


def get_section_from_port(port):
    """
    Returns the section name corresponding to the given port. If the port is the
    default one (3306) or an unknown one, return a null value.
    """
    sections, _ = read_section_ports_list()
    return sections.get(port, None)


def get_datadir_from_port(port):
    """
    Translates port number to expected datadir path
    """
    section = get_section_from_port(port)
    if section is None:
        return "/srv/sqldata"
    else:
        return "/srv/sqldata." + section


def get_socket_from_port(port):
    """
    Translates port number to expected socket location
    """
    section = get_section_from_port(port)
    if section is None:
        return "/run/mysqld/mysqld.sock"
    else:
        return "/run/mysqld/mysqld." + section + ".sock"


def get_credentials(host, port, database):
    """
    Given a database instance, return the authentication method, including
    the user, password, socket and ssl configuration.
    """
    if host == "localhost":
        user = pwd.getpwuid(os.getuid()).pw_name
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
        config.read("/root/.my.cnf")
        user = config["client"]["user"]
        password = config["client"]["password"]
        ssl = None
        mysql_sock = None
    elif not host.startswith("labsdb") and not host.startswith("clouddb"):
        # connect to a production remote host, use ssl and prod pass
        config = configparser.ConfigParser(interpolation=None, allow_no_value=True)
        config.read("/root/.my.cnf")
        user = config["client"]["user"]
        password = config["client"]["password"]
        ssl = {"ca": "/etc/ssl/certs/Puppet_Internal_CA.pem"}
        mysql_sock = None
    else:
        # connect to a labs remote host, use ssl and labs pass
        config = configparser.ConfigParser(interpolation=None)
        config.read("/root/.my.cnf")
        user = config["clientlabsdb"]["user"]
        password = config["clientlabsdb"]["password"]
        ssl = {"ca": "/etc/ssl/certs/Puppet_Internal_CA.pem"}
        mysql_sock = None

    return (user, password, mysql_sock, ssl)


def resolve(host, port=3306):
    """
    Return the full qualified domain name for a database hostname. Normally
    this return the hostname itself, except in the case where the
    datacenter and network parts have been omitted, in which case, it is
    completed as a best effort.
    If the original address is an IPv4 or IPv6 address, leave it as is
    """
    if ":" in host:
        # we do not support ipv6 yet
        host, port = host.split(":")
        try:
            port = int(port)
        except ValueError:
            port = get_port_from_section(port)
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


def get_wikis(shard=None, wiki=None):
    """
    Returns a tuple of hosts, ports and database names for all wikis from
    the given shard. If shard is the string 'ALL', return all wikis from
    all servers. The returned list is ordered by instance- that means,
    wikis from the same instance are grouped together.

    Currently implemented with shard lists on disk, this logic should be
    simplified in the future with a dynamic database. The following assumes
    there are not repeated shards/hosts (except in the same instance has
    more than one shard), so no virtual dblists or hosts files.
    """
    if shard == "ALL":
        # do a recursive call for every shard found
        wiki_list = []
        shard_dblists = glob.glob("*.dblist")
        for file in shard_dblists:
            shard = re.search(r"([^/]+)\.dblist", file).group(1)
            wiki_list += get_wikis(shard=shard, wiki=wiki)
        return wiki_list
    elif shard is None and wiki is None:
        # No shards or wikis selected, return the empty list
        print("No wikis selected")
        return list()
    elif shard is None and wiki is not None:
        # TODO: shard is not set, search the shard for a wiki
        dbs = [wiki]
        shard_dblists = glob.glob("*.dblist")
        for file in shard_dblists:
            shard_dbs = []
            with open(file, "r") as f:
                shard_dbs = f.read().splitlines()
            # print('{}: {}'.format(file, shard_dbs))
            if wiki in shard_dbs:
                shard = re.search(r"([^/]+)\.dblist", file).group(1)
                break
        if shard is None or shard == "":
            print("The wiki '{}' wasn't found on any shard".format(wiki))
            return list()
    elif shard is not None and wiki is not None:
        # both shard and wiki are set, check the wiki is really on the
        # shard
        shard_dbs = []
        with open("{}.dblist".format(shard), "r") as f:
            shard_dbs = f.read().splitlines()
        if wiki not in shard_dbs:
            print("The wiki '{}' wasn't found on the shard '{}'".format(wiki, shard))
            return list()
        dbs = [wiki]
    else:
        # shard is set, but not wiki, get all dbs from that shard
        dbs = []
        with open("{}.dblist".format(shard), "r") as f:
            dbs = f.read().splitlines()

    with open("{}.hosts".format(shard), "r") as f:
        hosts = list(csv.reader(f, delimiter="\t"))

    # print(hosts)
    # print(dbs)

    return sorted([([h[0], int(h[1])] + [d]) for h in hosts for d in dbs])
