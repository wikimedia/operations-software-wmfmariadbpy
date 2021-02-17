# requires python3-pymysql
import pymysql

import wmfmariadbpy.dbutil as dbutil


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
        host, port = dbutil.addr_split(host, port)
        host = dbutil.resolve(host)
        user, password, socket, ssl = dbutil.get_credentials(host, port, database)

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
        if self.host == "localhost" and self.socket:
            address = "{}[socket={}]".format(self.host, self.socket)
        else:
            host = self.host
            if not host[0].isdigit():
                # Don't split on . if host is an IP address
                host = host.split(".")[0]
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
