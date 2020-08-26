import configparser
import csv
import glob
import ipaddress
import os
import pwd
import re
import socket

# requires python3-pymysql
import pymysql

SECTION_PORT_LIST_FILE = "/etc/wmfmariadbpy/section_ports.csv"


class WMFMariaDB:
    """
    Wrapper class to connect to MariaDB instances within the Wikimedia
    Foundation cluster. It simplifys all authentication methods by providing a
    unique, clean way to do stuff on the databases.
    """

    def __init__(
        self,
        host,
        port=3306,
        database=None,
        debug=False,
        connect_timeout=10.0,
        query_limit=None,
        vendor="MariaDB",
    ):
        """
        Try to connect to a mysql server instance and returns a python
        connection identifier, which you can use to send one or more queries.
        """
        self.__last_error = None
        self.debug = debug
        self.vendor = vendor
        (host, port) = WMFMariaDB.resolve(host, port)
        (user, password, socket, ssl) = WMFMariaDB.get_credentials(host, port, database)

        try:
            self.connection = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                db=database,
                charset="utf8mb4",
                unix_socket=socket,
                ssl=ssl,
                connect_timeout=connect_timeout,
                autocommit=True,
            )
        except (pymysql.err.OperationalError, pymysql.err.InternalError, OSError) as e:
            self.connection = None
            self.__last_error = [e.args[0], e.args[1]]
            if self.debug:
                print("ERROR {}: {}".format(e.args[0], e.args[1]))
        self.host = host
        self.socket = socket
        self.port = int(port)
        self.database = database
        self.connect_timeout = connect_timeout
        self.set_query_limit(query_limit)  # we ignore it silently if it fails
        if self.debug:
            print("Connected to {}".format(self.name()))

    def name(self, show_db=True):
        if self.host == "localhost":
            address = "{}[socket={}]".format(self.host, self.socket)
        else:
            host = self.host.split(".")[0]
            if self.port == 3306:
                address = host
            else:
                address = "{}:{}".format(host, self.port)
        if show_db:
            if self.database is None:
                database = "(none)"
            else:
                database = self.database
            return "{}/{}".format(address, database)
        else:
            return address

    def is_same_instance_as(self, other_instance):
        """
        Returns True if the current WMFMariaDB is connected to the same one than the one given.
        False otherwise (not the same, they are not WMFMariaDB objects, etc.)
        """
        return (
            self is not None
            and self.host is not None
            and other_instance is not None
            and other_instance.host is not None
            and self.host == other_instance.host
            and self.port == other_instance.port
            and (
                (self.socket is None and other_instance.socket is None)
                or self.socket == other_instance.socket
            )
        )

    @staticmethod
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

    @staticmethod
    def get_port_from_section(section):
        """
        Returns the port integer corresponding to the given section name. If the section
        is None, or an unrecognized one, return the default one (3306).
        """
        sections, ports = WMFMariaDB.read_section_ports_list()
        return ports.get(section, 3306)

    @staticmethod
    def get_section_from_port(port):
        """
        Returns the section name corresponding to the given port. If the port is the
        default one (3306) or an unknown one, return a null value.
        """
        sections, ports = WMFMariaDB.read_section_ports_list()
        return sections.get(port, None)

    @staticmethod
    def get_datadir_from_port(port):
        """
        Translates port number to expected datadir path
        """
        section = WMFMariaDB.get_section_from_port(port)
        if section is None:
            return "/srv/sqldata"
        else:
            return "/srv/sqldata." + section

    @staticmethod
    def get_socket_from_port(port):
        """
        Translates port number to expected socket location
        """
        section = WMFMariaDB.get_section_from_port(port)
        if section is None:
            return "/run/mysqld/mysqld.sock"
        else:
            return "/run/mysqld/mysqld." + section + ".sock"

    @staticmethod
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
            mysql_sock = WMFMariaDB.get_socket_from_port(port)
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

    @property
    def debug(self):
        """debug getter"""
        return self.__debug

    @debug.setter
    def debug(self, debug):
        """debug setter"""
        if not debug:
            self.__debug = False
        else:
            self.__debug = True

    @property
    def last_error(self):
        """last_error getter"""
        return self.__last_error

    @staticmethod
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
                port = WMFMariaDB.get_port_from_section(port)
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

    def change_database(self, database):
        """
        Changes the current database without having to disconnect and reconnect
        """
        # cursor = self.connection.cursor()
        # cursor.execute('use `{}`'.format(database))
        # cursor.close()
        if self.connection is None:
            print("ERROR: There is no connection active; could not change db")
            return -1
        try:
            self.connection.select_db(database)
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
            self.__last_error = [e.args[0], e.args[1]]
            if self.debug:
                print("ERROR {}: {}".format(e.args[0], e.args[1]))
            return -2
        self.database = database
        if self.debug:
            print("Changed database to '{}'".format(self.database))

    def set_query_limit(self, query_limit):
        """
        Changes the default query limit to the given value, in seconds. Fractional
        time, e.g. 0.1, 1.5 are allowed. Set to 0 or None to disable the query
        limit.
        """
        if query_limit is None or not query_limit or query_limit == 0:
            self.query_limit = 0
        elif self.vendor == "MariaDB":
            self.query_limit = float(query_limit)
        else:
            self.query_limit = int(query_limit * 1000.0)

        if self.vendor == "MariaDB":
            result = self.execute(
                "SET SESSION max_statement_time = {}".format(self.query_limit)
            )
        else:
            result = self.execute(
                "SET SESSION max_execution_time = {}".format(self.query_limit)
            )
        return result[
            "success"
        ]  # many versions will not accept query time restrictions

    def execute(self, command, timeout=None, dryrun=False):
        """
        Sends a single query to a previously connected server instance, returns
        if that query was successful, and the rows read if it was a SELECT
        """
        # we are not connected, abort immediately
        if self.connection is None:
            return {
                "query": command,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "success": False,
                "errno": self.last_error[0],
                "errmsg": self.last_error[1],
            }
        cursor = self.connection.cursor()
        if timeout is not None:
            original_query_limit = self.query_limit
            self.set_query_limit(timeout)

        try:
            if dryrun:
                print(
                    (
                        "We will *NOT* execute '{}' on {}:{}/{} because"
                        "this is a dry run."
                    ).format(command, self.host, self.port, self.database)
                )
                cursor.execute("SELECT 'success' as dryrun")
            else:
                if self.debug:
                    print("Executing '{}'".format(command))
                cursor.execute(command)
        except (
            pymysql.err.ProgrammingError,
            pymysql.err.OperationalError,
            pymysql.err.InternalError,
        ) as e:
            cursor.close()
            query = command
            host = self.host
            port = self.port
            database = self.database
            self.__last_error = [e.args[0], e.args[1]]
            if self.debug:
                print("ERROR {}: {}".format(e.args[0], e.args[1]))
            result = {
                "query": query,
                "host": host,
                "port": port,
                "database": database,
                "success": False,
                "errno": self.last_error[0],
                "errmsg": self.last_error[1],
            }
            if timeout is not None:
                self.set_query_limit(original_query_limit)
            return result

        rows = None
        fields = None
        query = command
        host = self.host
        port = self.port
        database = self.database
        if cursor.rowcount > 0:
            rows = cursor.fetchall()
            if cursor.description:
                fields = tuple([x[0] for x in cursor.description])
        numrows = cursor.rowcount
        cursor.close()
        if timeout is not None:
            self.set_query_limit(original_query_limit)

        return {
            "query": query,
            "host": host,
            "port": port,
            "database": database,
            "success": True,
            "numrows": numrows,
            "rows": rows,
            "fields": fields,
        }

    def get_version(self):
        """
        Returns the version of the db server in the form of a (major, minor, patch) tuple.
        """
        result = self.execute("SELECT @@VERSION")
        if not result["success"]:
            return ()
        ver_nums = result["rows"][0][0].split("-")[0]
        return tuple(map(int, ver_nums.split(".")))

    @staticmethod
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
                wiki_list += WMFMariaDB.get_wikis(shard=shard, wiki=wiki)
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
                print(
                    "The wiki '{}' wasn't found on the shard '{}'".format(wiki, shard)
                )
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

    @staticmethod
    def execute_many(command, shard=None, wiki=None, dryrun=True, debug=False):
        """
        Executes a command on all wikis from the given shard, once per
        instance, serially. If dryrun is True, not execute, but connect and
        print what would be done.
        """
        result = []
        connection = None
        dblist = WMFMariaDB.get_wikis(shard=shard, wiki=wiki)

        for host, port, database in dblist:
            if (
                connection is not None
                and connection.host == host
                and connection.port == port
                and connection.database != database
            ):
                connection.change_database(database)
                if connection.database != database:
                    print("Could not change to database {}".format(database))
                    continue
            else:
                if connection is not None:
                    connection.disconnect()
                connection = WMFMariaDB(
                    host=host, port=port, database=database, debug=debug
                )

            if connection.connection is None:
                print(
                    "ERROR: Could not connect to {}:{}/{}".format(host, port, database)
                )
                resultset = None
            else:
                resultset = connection.execute(command, dryrun)

            if resultset is None:
                result.append(
                    {
                        "success": False,
                        "host": host,
                        "port": port,
                        "database": database,
                        "numrows": 0,
                        "rows": None,
                        "fields": None,
                    }
                )
            else:
                result.append(resultset)

        if connection.connection is not None:
            connection.disconnect()
        return result

    def disconnect(self):
        """
        Ends the connection to a database, freeing resources. No more queries
        will be able to be sent to this connection id after this is executed
        until a new connection is open.
        """
        if self.debug:
            print(
                "Disconnecting from {}:{}/{}".format(
                    self.port, self.host, self.database
                )
            )
        if self.connection is not None:
            self.connection.close()
            self.connection = None
