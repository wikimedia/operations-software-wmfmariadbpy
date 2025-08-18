# WMFReplication.py
from __future__ import annotations
import configparser
import ipaddress
import os
import pwd
import socket
import time
from multiprocessing.pool import ThreadPool
from typing import Optional, Iterable

from wmfmariadbpy.WMFMariaDB import WMFMariaDB, Status

# noqa: F821


class WMFReplication:
    """
    Class to control replication at WMF MariaDB/MySQL Cluster
    """

    def __init__(
        self, connection: WMFMariaDB, timeout: float = 5.0, sleep: float = 5.0
    ):
        """
        The constructore requires an open connection in the form of a WMFMariaDB object
        """
        self.connection = connection
        self.timeout: float = timeout
        self.sleep = sleep

    def slave_status(self) -> Optional[Status]:
        """
        Returns a dictionary with the slave status. If the server is not configured as a slave, it will return None.
        """
        result = self.connection.execute("SHOW SLAVE STATUS", timeout=self.timeout)
        if not result["success"]:
            return {
                "success": False,
                "errno": result["errno"],
                "errmsg": result["errmsg"],
            }
        if result["numrows"] == 0:
            return None
        status: Status = dict(zip([key.lower() for key in result["fields"]], result["rows"][0]))  # type: ignore
        status["success"] = True
        return status

    def master_status(self) -> Optional[Status]:
        """
        Returns a dictionary with the master status. If the server is not configured as a master (binary log is
        disabled), it will return None.
        """
        result = self.connection.execute("SHOW MASTER STATUS")
        if not result["success"]:
            return {
                "success": False,
                "errno": result["errno"],
                "errmsg": result["errmsg"],
            }
        if result["numrows"] == 0:
            return None
        status: Status = dict(zip([key.lower() for key in result["fields"]], result["rows"][0]))  # type: ignore
        status["success"] = True
        return status

    def setup(
        self,
        master_host: Optional[str] = None,
        master_port: int = 3306,
        master_log_file: Optional[str] = None,
        master_log_pos: int = 0,
        master_ssl: int = 1,
    ) -> Status:
        """
        Sets up replication configuration for a server for the first time
        (but does not start it).
        If replication is already configured (even if the replica is stopped),
        it fails. Stop and reset replication before trying to run it.
        """
        config = configparser.ConfigParser(interpolation=None)
        if "TESTENV_MY_CNF" in os.environ:
            config.read(os.environ["TESTENV_MY_CNF"])
        else:
            pw = pwd.getpwuid(os.getuid())
            config.read(os.path.join(pw.pw_dir, ".my.cnf"))
        master_user = config["clientreplication"]["user"]
        master_password = config["clientreplication"]["password"]
        slave_status = self.slave_status()
        if (
            slave_status is not None
            and slave_status["success"]
            and "slave_sql_running" in slave_status
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": "Replica is setup, reset it before setting it up again.",
            }

        query = """CHANGE MASTER TO
                   MASTER_HOST = '{}',
                   MASTER_PORT = {},
                   MASTER_USER = '{}',
                   MASTER_PASSWORD = '{}',
                   MASTER_LOG_FILE = '{}',
                   MASTER_LOG_POS = {},
                   MASTER_SSL = {}
                 """.format(
            master_host,
            str(int(master_port)),
            master_user,
            master_password,
            master_log_file,
            str(int(master_log_pos)),
            str(int(master_ssl)),
        )
        result = self.connection.execute(query)
        return result

    def is_direct_replica_of(self, master: WMFMariaDB) -> bool:
        """
        Checks if the current instance is a direct replica of the given master. Only returns true
        if it can be confirmed. If it gives an error or it not a direct replica, it will return
        false
        """
        current_master = self.master()
        return current_master is not None and current_master.is_same_instance_as(master)

    def is_sibling_of(self, sibling: WMFMariaDB) -> bool:
        """
        Checks if current instance is replicating directly from the same host than "sibling".
        Returns false if the same hosts is sent as the sibling to check (they have to be different hosts).
        """
        current_master = self.master()
        sibling_master = WMFReplication(sibling).master()
        return (
            current_master is not None
            and sibling_master is not None
            and not self.connection.is_same_instance_as(sibling)
            and current_master.is_same_instance_as(sibling_master)
        )

    def reset_slave(self) -> Status:
        """
        Resets all replication information for the instance. It only works if both the io thread and the sql thread
        are currently stopped- run stop slave otherwise beforehand.
        """
        slave_status = self.slave_status()
        assert slave_status is not None
        if (
            not slave_status["slave_sql_running"] == "No"
            or not slave_status["slave_io_running"] == "No"
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": "Replica is running, stop slave before resetting it",
            }
        return self.connection.execute("RESET SLAVE ALL")

    def lag(self) -> Optional[int]:
        """
        Returns the current lag of the replication, as provided by SHOW SLAVE STATUS.
        Use heartbeat functions if a more accurate and less locking way is needed (e.g. for
        frequent updates). Returns None on error.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status["success"]:
            return None
        else:
            return slave_status["seconds_behind_master"]

    def start_slave(
        self,
        thread: Optional[str] = None,
        until_log: Optional[str] = None,
        until_pos: Optional[int] = None,
    ) -> Status:
        """
        Starts the replication thread given (allowed values are 'sql', 'io', or None- the default,
        which will try to start both threads).
        If all the given threads are already running, it returns with an error.
        For convenience, if the action is succesful, it will return a copy of the replication
        status.
        """
        slave_status = self.slave_status()
        if slave_status is None:
            return {
                "success": False,
                "errno": -1,
                "errmsg": "The server is not configured as a slave",
            }

        if thread is None:
            if (
                slave_status["slave_sql_running"] != "No"
                and slave_status["slave_io_running"] != "No"
            ):
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": (
                        "Both IO and SQL threads are already running or trying to "
                        "connect"
                    ),
                }
            slave_thread = ""
        elif thread.lower() == "sql":
            if slave_status["slave_sql_running"] != "No":
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "SQL thread is already running",
                }
            slave_thread = "SQL_THREAD"
        elif thread.lower() == "io":
            if slave_status["slave_io_running"] != "No":
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "IO thread is already running or trying to connect",
                }
            slave_thread = "IO_THREAD"
        else:
            return {
                "success": False,
                "errno": -1,
                "errmsg": "Slave Thread can only be SQL, IO or None (default)",
            }

        if until_log is not None and until_pos is not None:
            query = (
                ("START SLAVE {} UNTIL MASTER_LOG_FILE = '{}', " "MASTER_LOG_POS = {}")
                .format(slave_thread, until_log, until_pos)
                .rstrip()
            )
        else:
            query = "START SLAVE {}".format(slave_thread).rstrip()
        result = self.connection.execute(query, timeout=self.timeout)

        if not result["success"]:
            return result

        slave_status = self.slave_status()
        assert slave_status is not None
        if not slave_status["success"]:
            return slave_status
        timeout_start = time.time()
        while (
            (
                slave_thread == ""
                and (
                    slave_status["slave_io_running"] != "Yes"
                    or slave_status["slave_sql_running"] != "Yes"
                )
            )
            or (
                slave_thread == "SQL_THREAD"
                and slave_status["slave_sql_running"] != "Yes"
            )
            or (
                slave_thread == "IO_THREAD"
                and slave_status["slave_io_running"] != "Yes"
            )
        ):
            time.sleep(0.1)
            if time.time() > (timeout_start + self.timeout):
                break
            slave_status = self.slave_status()
            assert slave_status is not None

        if slave_thread == "" and (
            slave_status["slave_io_running"] != "Yes"
            or slave_status["slave_sql_running"] != "Yes"
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": slave_status["last_io_error"]
                + slave_status["last_sql_error"],
            }
        elif (
            slave_thread == "SQL_THREAD" and slave_status["slave_sql_running"] != "Yes"
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": slave_status["last_sql_error"],
            }
        elif slave_thread == "IO_THREAD" and slave_status["slave_io_running"] != "Yes":
            return {
                "success": False,
                "errno": -1,
                "errmsg": slave_status["last_io_error"],
            }
        else:
            return slave_status

    def stop_slave(self, thread: Optional[str] = None) -> Status:
        """
        Stops the replication thread given (allowed values are 'sql', 'io', or None- the default,
        which will try to stop both threads). If all the given threads are already stopped, it
        returns with an error.
        For convenience, if the action is succesful, it will return a copy of the replication
        status.
        """
        slave_status = self.slave_status()
        assert slave_status is not None
        if thread is None:
            if (
                slave_status["slave_sql_running"] == "No"
                and slave_status["slave_io_running"] == "No"
            ):
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "Both IO and SQL threads are already stopped",
                }
            slave_thread = ""
        elif thread.lower() == "sql":
            if slave_status["slave_sql_running"] == "No":
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "SQL thread is already stopped",
                }
            slave_thread = "SQL_THREAD"
        elif thread.lower() == "io":
            if slave_status["slave_io_running"] == "No":
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "IO thread is already stopped",
                }
            slave_thread = "IO_THREAD"
        else:
            return {
                "success": False,
                "errno": -1,
                "errmsg": "Slave Thread can only be SQL, IO or None (default)",
            }

        query = "STOP SLAVE {}".format(slave_thread).rstrip()
        result = self.connection.execute(query, timeout=self.timeout)

        if not result["success"]:
            return result

        slave_status = self.slave_status()
        assert slave_status is not None
        if not slave_status["success"]:
            return slave_status
        timeout_start = time.time()
        while (
            (
                slave_thread == ""
                and (
                    slave_status["slave_io_running"] != "No"
                    or slave_status["slave_sql_running"] != "No"
                )
            )
            or (
                slave_thread == "SQL_THREAD"
                and slave_status["slave_sql_running"] != "No"
            )
            or (
                slave_thread == "IO_THREAD" and slave_status["slave_io_running"] != "No"
            )
        ):
            time.sleep(0.1)
            if time.time() > (timeout_start + self.timeout):
                break
            slave_status = self.slave_status()
            assert slave_status is not None

        if slave_thread == "" and (
            slave_status["slave_io_running"] != "No"
            or slave_status["slave_sql_running"] != "No"
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": slave_status["last_io_error"]
                + slave_status["last_sql_error"],
            }
        elif slave_thread == "SQL_THREAD" and slave_status["slave_sql_running"] != "No":
            return {
                "success": False,
                "errno": -1,
                "errmsg": slave_status["last_sql_error"],
            }
        elif slave_thread == "IO_THREAD" and slave_status["slave_io_running"] != "No":
            return {
                "success": False,
                "errno": -1,
                "errmsg": slave_status["last_io_error"],
            }
        else:
            return slave_status

    def master(self) -> Optional[WMFMariaDB]:
        """
        Returns a WMFMariaDB object of the current master of the replicating instance or None if
        the current instance does not have a working replica.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status["success"]:
            return None
        else:
            return WMFMariaDB(
                host=slave_status["master_host"], port=slave_status["master_port"]
            )

    def __connect(self, row: tuple[str, str, str]) -> Optional[WMFMariaDB]:
        host = row[1]
        if host is None or host == "":
            # --report-host is not set, use server_id as the ipv4, and perform an inverse dns resolution
            server_id = int(row[0])
            ip = str(ipaddress.IPv4Address(server_id))
            host = socket.gethostbyaddr(ip)[0]
        port = int(row[2])
        if port not in (0, 3306):
            host += ":" + str(port)
        connection = WMFMariaDB(host)
        if WMFReplication(connection).is_direct_replica_of(self.connection):
            return connection
        return None

    def __connect_in_parallel(
        self, hosts: list[tuple[str, str, str]]
    ) -> tuple[WMFMariaDB, ...]:
        n = len(hosts)
        pool = ThreadPool(processes=n)
        async_result = list()
        conn = list()
        for host in hosts:
            async_result.append(pool.apply_async(self.__connect, (host,)))

        for i in range(n):
            mysql = async_result[i].get()
            if mysql is None or mysql.connection is None:
                print("Could not connect to instance {}, skipping".format(hosts[i][0]))
            else:
                conn.append(mysql)
        pool.close()
        pool.join()
        return tuple(conn)

    def slaves(self) -> Iterable[WMFMariaDB]:
        """
        Returns a list of WMFMariaDB objects of all currently connected replicas of the instances, as detected by
        show slave hosts. It requires --report-host correctly configured on the replicas.
        If report-host is not configured, it fails back to assume its server_id is the same as its ipv4 (which has
        the limitation that there cannot be more than 1 server from the same ip).
        This means it will only detect replicas which io_thread is running (connected to the master).
        If no replica is detected, it returns the empty array.
        TODO: SHOW SLAVE HOSTS is a limited way to discover replicas; however, processlist doesn't show the source
        port.
        """
        slaves: list[WMFMariaDB] = []
        result = self.connection.execute("SHOW SLAVE HOSTS")
        assert result is not None
        if (
            not result["success"]
            or result["numrows"] == 0
            or result["fields"][1] != "Host"
            or result["fields"][2] != "Port"
            or result["fields"][0] != "Server_id"
        ):
            return slaves
        return self.__connect_in_parallel(result["rows"])  # type: ignore

    def caught_up_to_master(self, master: Optional[WMFMariaDB] = None) -> bool:
        """
        Checks if replication has caught up to the direct master (normally after the master
        stopped replication or is set as read_only). The optional parameters allows to provide
        a master host explicitly, mainly if we are already connected to it so we can reuse the
        connection or to force certain connection options, but if none is given, we will
        auto-discover it.
        Returns true if the slave caught up, and false if there was any error or there is lag
        between the master and the slave. Note that if writes are ongoing on the master, this
        will be meaningless, as the slave will lag and catch up continuously- use
        lag() or heatbeat_status() to check ongoing lag.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status["success"]:
            return False
        if master is None:
            # Autodiscover master
            master = self.master()

        assert master is not None
        master_replication = WMFReplication(master)
        master_status = master_replication.master_status()
        if master_status is None or not master_status["success"]:
            return False
        return (
            slave_status["relay_master_log_file"] == master_status["file"]
            and slave_status["exec_master_log_pos"] == master_status["position"]
        )

    def __restore_replication_status(
        self,
        new_master_replication: WMFReplication,
        slave_status: Status,
        start_if_stopped: bool,
    ) -> Status:
        """
        Returns replication to the original state on both the current host and the master,
        based either on the forced start parameter (start_if_stopped), or on the original
        state (slave_status).
        Not intended to be used directly, but only when calling move()
        """
        if start_if_stopped:
            self.start_slave()
            new_master_replication.start_slave()
        elif (
            slave_status["slave_sql_running"] == "Yes"
            and slave_status["slave_io_running"] == "Yes"
        ):
            self.start_slave()
        elif slave_status["slave_sql_running"] == "Yes":
            self.start_slave(thread="sql")
        elif slave_status["slave_io_running"] == "Yes":
            self.start_slave(thread="io")

        # TODO: What about GTID mode restoration?
        out = self.slave_status()
        assert out
        return out

    def __move_precheck(self, slave_status: Status, new_master: WMFMariaDB) -> Status:
        # TODO: Does the new master have working replication credentials?
        if slave_status is None or not slave_status["success"]:
            return {
                "success": False,
                "errno": -1,
                "errmsg": "The host is not configured as a replica",
            }
        # is the host already replicating from the new master ?
        if new_master.host == slave_status["master_host"] and new_master.port == int(
            slave_status["master_port"]
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": (
                    "The host is already configured "
                    "as a master of {}".format(
                        ":".join((new_master.host, str(new_master.port)))
                    )
                ),
            }
        # is the instance to be changed and the new master the same instance (are we trying
        # to connect it to itself)?
        if (
            new_master.host == self.connection.host
            and new_master.port == self.connection.port
        ):
            return {
                "success": False,
                "errno": -1,
                "errmsg": "The host is trying to connect to itself",
            }
        return {
            "success": True,
        }

    def __move_same_master_and_stopped(
        self,
        slave_status: Status,
        new_master: WMFMariaDB,
        new_master_replication: WMFReplication,
        new_master_master_status: Optional[Status],
        new_master_slave_status: Status,
        start_if_stopped: bool,
    ) -> Optional[Status]:
        # Are both hosts replicating from the same master and stopped on the same coordinate?
        # If yes, then just move them directly
        if (
            self.is_sibling_of(new_master)
            and slave_status["slave_sql_running"] == "No"
            and new_master_slave_status["slave_sql_running"] == "No"
            and slave_status["relay_master_log_file"]
            == new_master_slave_status["relay_master_log_file"]
            and slave_status["exec_master_log_pos"]
            == new_master_slave_status["exec_master_log_pos"]
        ):
            assert new_master_master_status is not None
            self.stop_slave()  # in case io was still running
            self.reset_slave()
            result = self.setup(
                master_host=new_master.host,
                master_port=new_master.port,
                master_log_file=new_master_master_status["file"],
                master_log_pos=new_master_master_status["position"],
            )
            if result["success"]:
                return self.__restore_replication_status(
                    new_master_replication, slave_status, start_if_stopped
                )
            else:
                return result
        else:
            return None

    def __move_master_master_stopped(
        self,
        current_master: WMFMariaDB,
        new_master: WMFMariaDB,
        new_master_replication: WMFReplication,
        slave_status: Status,
        start_if_stopped: bool,
        current_master_replication: WMFReplication,
        current_master_slave_status: Optional[Status],
    ) -> Optional[Status]:
        # Is the host we are replicating from replicating directly from the new master, stopped,
        # and the replica caught up? -> Also move it directly
        if (
            current_master_slave_status is not None
            and current_master_replication.master().is_same_instance_as(new_master)  # type: ignore
            and current_master_slave_status["slave_sql_running"] == "No"
            and self.caught_up_to_master(current_master)
        ):
            self.stop_slave()
            self.reset_slave()
            result = self.setup(
                master_host=new_master.host,
                master_port=new_master.port,
                master_log_file=current_master_slave_status["relay_master_log_file"],
                master_log_pos=current_master_slave_status["exec_master_log_pos"],
            )
            if result["success"]:
                return self.__restore_replication_status(
                    new_master_replication, slave_status, start_if_stopped
                )
            else:
                return result
        else:
            return None

    def stop_in_sync_with_sibling(self, sibling: WMFMariaDB) -> Optional[Status]:
        # 1. Check both hosts are replicating from the same host, replication is running and
        #    more or less caught up
        current_status = self.slave_status()
        sibling_replication = WMFReplication(sibling, timeout=self.timeout)
        sibling_status = sibling_replication.slave_status()
        current_lag = self.lag()
        sibling_lag = sibling_replication.lag()

        if (
            current_status is not None
            and current_status["success"]
            and sibling_status is not None
            and sibling_status["success"]
            and self.is_sibling_of(sibling)
            and current_lag is not None
            and current_lag < self.timeout
            and sibling_lag is not None
            and sibling_lag < self.timeout
        ):
            # 2. Stop replication on current host
            self.stop_slave(thread="sql")
            # 3. Wait
            time.sleep(self.sleep)
            # 4. Stop replication on sibling
            sibling_replication.stop_slave(thread="sql")
            # 5. Ensure sibling replicating (slave status) coordinates are equal or higher than
            #    the current host's (slave status)
            time_waited = 0
            replication_reached = False
            while time_waited < self.timeout:
                time.sleep(1)
                time_waited += 1
                current_status = self.slave_status()
                assert current_status is not None
                sibling_status = sibling_replication.slave_status()
                assert sibling_status is not None
                if not (
                    current_status["success"]
                    and sibling_status["success"]
                    and current_status["slave_sql_running"] == "No"
                    and sibling_status["slave_sql_running"] == "No"
                    and (
                        sibling_status["relay_master_log_file"]
                        > current_status["relay_master_log_file"]
                        or (
                            sibling_status["relay_master_log_file"]
                            == current_status["relay_master_log_file"]
                            and sibling_status["exec_master_log_pos"]
                            >= current_status["exec_master_log_pos"]
                        )
                    )
                ):
                    continue
                else:
                    replication_reached = True
                    break
            if not replication_reached:
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": (
                        "We expected the other host "
                        "replication to be stopped and "
                        "ahead of the current, but it "
                        "was behind or other error "
                        "happened"
                    ),
                }
            # 6. Run on current host "start slave until" sibling coordinates (slave status)
            self.start_slave(
                thread="sql",
                until_log=sibling_status["relay_master_log_file"],
                until_pos=sibling_status["exec_master_log_pos"],
            )
            time_waited = 0
            replication_reached = False
            while time_waited < self.timeout:
                time.sleep(1)
                # 7. Check both hosts are stopped replication and on the same (slave status)
                #    coordinates
                current_status = self.slave_status()
                assert current_status is not None
                sibling_status = sibling_replication.slave_status()
                assert sibling_status is not None
                if not (
                    current_status["success"]
                    and sibling_status["success"]
                    and current_status["slave_sql_running"] == "No"
                    and sibling_status["slave_sql_running"] == "No"
                    and sibling_status["relay_master_log_file"]
                    == current_status["relay_master_log_file"]
                    and sibling_status["exec_master_log_pos"]
                    == current_status["exec_master_log_pos"]
                ):
                    continue
                else:
                    replication_reached = True
                    break
            if not replication_reached:
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": (
                        "We expected both hosts to be "
                        "stopped and in sync, but they "
                        "are not, or other error "
                        "happened"
                    ),
                }
            else:
                return {
                    "success": True,
                    "log_file": sibling_status["relay_master_log_file"],
                    "log_pos": sibling_status["exec_master_log_pos"],
                }  # type: ignore
        else:
            return None

    def move_sibling_to_child(self, sibling: WMFMariaDB) -> Optional[Status]:
        """
        Move self from being a sibling to new master to replicate directly from it
        (check relationship first)
        """
        # 1. Check current host and its new master are replicating from the same, other
        #    host, replication is running and both are caught up (more or less)
        current_status = self.slave_status()
        sibling_replication = WMFReplication(sibling, timeout=self.timeout)
        sibling_status = sibling_replication.slave_status()

        if (
            current_status is not None
            and sibling_status is not None
            and current_status["success"]
            and sibling_status["success"]
            and self.is_sibling_of(sibling)
            and current_status["slave_sql_running"] == "Yes"
            and self.lag() < self.timeout  # type: ignore
            and sibling_status["slave_sql_running"] == "Yes"
            and sibling_replication.lag() < self.timeout  # type: ignore
        ):
            # 2. Stop them in sync
            result = self.stop_in_sync_with_sibling(sibling)
            assert result is not None
            if not result["success"]:
                return result
            # 3. CHANGE MASTER current host to new master current (SHOW MASTER) coordinates
            sibling_master_status = sibling_replication.master_status()
            assert sibling_master_status is not None
            if not sibling_master_status["success"]:
                return sibling_master_status
            result = self.stop_slave()
            if not result["success"]:
                return result
            result = self.reset_slave()
            if not result["success"]:
                return result
            result = self.setup(
                master_host=sibling.host,
                master_port=sibling.port,
                master_log_file=sibling_master_status["file"],
                master_log_pos=sibling_master_status["position"],
            )
            if not result["success"]:
                return result
            # 4. Restart replication on both hosts, wait a bit, make sure replication
            #    continues and both caught up
            result = self.start_slave()
            if not result["success"]:
                return result
            result = sibling_replication.start_slave()
            if not result["success"]:
                return result
            time_waited = 0
            replication_reached = False
            while time_waited < self.timeout:
                time.sleep(1)
                current_status = self.slave_status()
                assert current_status is not None
                sibling_status = sibling_replication.slave_status()
                assert sibling_status is not None
                if (
                    current_status["success"]
                    and sibling_status["success"]
                    and current_status["slave_io_running"] == "Yes"
                    and current_status["slave_sql_running"] == "Yes"
                    and sibling_status["slave_io_running"] == "Yes"
                    and sibling_status["slave_sql_running"] == "Yes"
                    and self.lag() < self.timeout  # type: ignore
                    and sibling_replication.lag() < self.timeout  # type: ignore
                ):
                    replication_reached = True
                    break
                else:
                    continue
            if replication_reached:
                return {
                    "success": True,
                }
            else:
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "Host replication did not caught up after",
                }
        else:
            return None

    def move_child_to_sibling(
        self, current_master: WMFMariaDB, new_master: WMFMariaDB
    ) -> Optional[Status]:
        """
        Move self from being a grandchild of new master to replicate directly from it
        (check relationship first)
        """
        # 1. Check current master's master is the new master, and current host and its
        #    direct master are both replicating and caught up (more or less)
        current_master_replication = WMFReplication(current_master)
        if (
            current_master_replication is not None
            and current_master_replication.is_direct_replica_of(new_master)
        ):
            current_status = self.slave_status()
            assert current_status is not None
            current_master_slave_status = current_master_replication.slave_status()
            assert current_master_slave_status is not None
            if not (
                current_status["success"]
                and current_master_slave_status["success"]
                and current_status["slave_sql_running"] == "Yes"
                and self.lag() < self.timeout  # type: ignore
                and current_master_slave_status["slave_sql_running"] == "Yes"
                and current_master_replication.lag() < self.timeout  # type: ignore
            ):
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "There is lag on some of the "
                    "hosts, or replication is "
                    "otherwise problematic (e.g. "
                    "one of the hosts is not a "
                    "replica)",
                }
            # 2. Stop current master's replication
            current_master_replication.stop_slave()
            # 3. Wait for current host to be completely in sync with its master
            timeout_start = time.time()
            while not self.caught_up_to_master(current_master):
                if time.time() > (timeout_start + self.timeout):
                    break
                time.sleep(0.1)
            if not self.caught_up_to_master(current_master):
                current_master_replication.start_slave()
                # FIXME: errno should be int
                return {
                    "success": False,
                    "errno": "-1",  # type: ignore
                    "errmsg": "We were unable to sync the "
                    "replica by stopping it, tried "
                    "restarting replication on "
                    "master",
                }  # type: ignore
            # 4. Stop current host replication
            self.stop_slave()
            # 5. CHANGE MASTER current host to its master current slave (SHOW SLAVE) coordinates
            #    so it replicates from the same host as it
            current_master_slave_status = current_master_replication.slave_status()
            assert current_master_slave_status is not None
            if not current_master_slave_status["success"]:
                return current_master_slave_status
            result = self.reset_slave()
            if not result["success"]:
                return result
            result = self.setup(
                master_host=current_master_slave_status["master_host"],
                master_port=current_master_slave_status["master_port"],
                master_log_file=current_master_slave_status["relay_master_log_file"],
                master_log_pos=current_master_slave_status["exec_master_log_pos"],
            )
            if not result["success"]:
                return result
            # 6. Restart replication on both hosts, wait a bit, make sure replication continues and
            #    both caught up
            result = self.start_slave()
            if not result["success"]:
                return result
            result = current_master_replication.start_slave()
            if not result["success"]:
                return result
            time.sleep(self.timeout)
            current_status = self.slave_status()
            assert current_status is not None
            old_master_status = current_master_replication.slave_status()
            assert old_master_status is not None
            if (
                current_status["success"]
                and old_master_status["success"]
                and current_status["slave_io_running"] == "Yes"
                and current_status["slave_sql_running"] == "Yes"
                and old_master_status["slave_io_running"] == "Yes"
                and old_master_status["slave_sql_running"] == "Yes"
                and self.lag() < self.timeout  # type: ignore
                and current_master_replication.lag() < self.timeout  # type: ignore
            ):
                return {
                    "success": True,
                }
            else:
                return {
                    "success": False,
                    "errno": -1,
                    "errmsg": "Host replication did not caught up after",
                }
        else:
            return None

    def move(self, new_master: WMFMariaDB, start_if_stopped: bool = False) -> Status:
        """
        Highly WIP, only the most trivial or simple scenarios implemented.
        Switches the current instance from the current master to another (new_master,
        a WMFMariaDB instance), affecting the current master as little as possible.
        The original master can have replication stopped or ongoing, but it must be
        already setup.
        If the change cannot be done or it is tried and it fails, it will return to
        the replication state before the change.
        new_master is a WMFMariaDB object (an open connection to the instance that
        will be the new master.
        """

        # pre-sanity checks
        slave_status = self.slave_status()
        assert slave_status is not None
        precheck = self.__move_precheck(slave_status, new_master)
        if not precheck["success"]:
            return precheck

        # get replication status of the to-be-new-master
        new_master_replication = WMFReplication(new_master)
        new_master_master_status = new_master_replication.master_status()
        new_master_slave_status = new_master_replication.slave_status()
        assert new_master_slave_status is not None

        # Trivial case: Same master and stopped
        result = self.__move_same_master_and_stopped(
            slave_status,
            new_master,
            new_master_replication,
            new_master_master_status,
            new_master_slave_status,
            start_if_stopped,
        )
        if result is not None:
            return result

        # Trivial case: new master is the current master's master, and master is stopped
        current_master = self.master()
        assert current_master is not None
        current_master_replication = WMFReplication(current_master)
        current_master_slave_status = current_master_replication.slave_status()
        # current_master_master_status = current_master_replication.master_status()
        result = self.__move_master_master_stopped(
            current_master,
            new_master,
            new_master_replication,
            slave_status,
            start_if_stopped,
            current_master_replication,
            current_master_slave_status,
        )
        if result is not None:
            return result

        # Normal case: current host is a sibling of new master, so it has to be moved to child
        result = self.move_sibling_to_child(new_master)
        if result is not None:
            return result

        # Normal case: current host is a grandchild of new master, it has to become a sibling of
        #              current master
        result = self.move_child_to_sibling(current_master, new_master)
        if result is not None:
            return result

        # Other cases are not supported for now
        return {
            "success": False,
            "errno": -1,
            "errmsg": (
                "The topology change cannot be done at the moment- "
                "check its relationship, replication status or replication lag"
            ),
        }

    def debug(self):
        """
        Prints in a human readable format the replication status of the host.
        """
        # master_status = self.master_status()  # print binlog coordinates too?
        slave_status = self.slave_status()
        if slave_status is None or not slave_status["success"]:
            print("{}> Not configured as a slave".format(self.connection.name()))
        else:
            string = "{}> master: {}, io: {}, sql: {}, lag: {}, pos: {}"
            print(
                string.format(
                    self.connection.name(),
                    slave_status["master_host"]
                    + ":"
                    + str(slave_status["master_port"]),
                    slave_status["slave_io_running"],
                    slave_status["slave_sql_running"],
                    str(self.lag()),
                    slave_status["relay_master_log_file"]
                    + ":"
                    + str(slave_status["exec_master_log_pos"]),
                )
            )

    def gtid_mode(self) -> Optional[str]:
        """
        Returns the current gtid mode of the replication. Returns None on error.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status["success"]:
            return None
        else:
            return slave_status["using_gtid"]

    def set_gtid_mode(self, mode: str) -> bool:
        """
        Changes the gtid mode of a replica to the one given (no, slave_pos or current_pos). It
        returns true if it is succesful, false otherwise.
        """
        if mode.lower() not in ("no", "slave_pos", "current_pos"):
            print("Incorrect mode")
            return False
        slave_status = self.slave_status()
        if slave_status is None or not slave_status["success"]:
            print(
                "Server is not a slave or other error happened on checking the current status"
            )
            return False
        if slave_status["using_gtid"].lower() == mode.lower():
            print('The server is already on "{}" mode'.format(mode))
            return True
        if (
            slave_status["slave_io_running"] == "Yes"
            or slave_status["slave_sql_running"] == "Yes"
        ):
            stop_slave = self.stop_slave()
            if not stop_slave["success"]:
                print("Could not stop slave: {}".format(stop_slave["errmsg"]))
                return False
        change_master_sql = "CHANGE MASTER TO MASTER_USE_GTID = {}".format(mode)
        change_master = self.connection.execute(change_master_sql)

        if not change_master["success"]:
            print("Could not change gtid mode: {}".format(change_master["errmsg"]))
        if (
            slave_status["slave_io_running"] == "Yes"
            and slave_status["slave_sql_running"] == "Yes"
        ):
            start_slave = self.start_slave()
        elif slave_status["slave_io_running"] == "Yes":
            start_slave = self.start_slave(thread="io")
        elif slave_status["slave_sql_running"] == "Yes":
            start_slave = self.start_slave(thread="sql")
        else:
            start_slave = dict()
            start_slave["success"] = True

        if not start_slave["success"]:
            print(
                "Could not restart slave after change master: {}".format(
                    start_slave["errmsg"]
                )
            )
        return change_master["success"]

    def heartbeat_status(self):
        """
        Returns the status of the replication, according to heartbeat (without running show slave
        status).
        Useful when a blocking query like show slave status wants to be avoided (e.g. it has to be
        fast and/or run many times) or a better lag detection is needed.
        """
        pass
