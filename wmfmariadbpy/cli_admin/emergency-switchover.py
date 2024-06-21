#!/usr/bin/python3

import argparse
import sys
import time

from wmfmariadbpy.RemoteExecution.CuminExecution import (
    CuminExecution as RemoteExecution,
)
from wmfmariadbpy.WMFMariaDB import WMFMariaDB
from wmfmariadbpy.WMFReplication import WMFReplication

HEARTBEAT_SERVICE = "pt-heartbeat-wikimedia"

ZARCILLO_INSTANCE = "db1215"  # instance_name:port format


def handle_parameters():
    parser = argparse.ArgumentParser(
        description=(
            "Performs an emergency master to direct replica switchover "
            "in the WMF environment, automating the most "
            "error-prone steps. Example usage: "
            "emergency-switchover.py db1052 db1067"
        )
    )
    parser.add_argument(
        "oldmaster",
        help=("Original master host, in hostname:port format, " "to be switched from"),
    )
    parser.add_argument(
        "newmaster",
        help=(
            "Direct replica host, in hostname:port format, to be "
            "switched to, and will become the new master"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help=(
            "Timeout in seconds, to wait for several operations before returning "
            "an error (e.g. for START SLAVE). It will also mark the maximum "
            "amount of lag we can tolerate."
        ),
    )
    parser.add_argument(
        "--skip-slave-move",
        action="store_true",
        help="When set, it does not migrate current master replicas to the new host",
    )
    parser.add_argument(
        "--only-slave-move",
        action="store_true",
        help=(
            "When set, it only migrates current master replicas to the new hosts"
            ", but does not perform the rest of the operations (read only, "
            "replication inversion, etc.)"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="When set, do not ask for confirmation before applying the changes.",
    )
    options = parser.parse_args()
    return options


def do_preflight_checks(master_replication, slave_replication, timeout):
    slave = slave_replication.connection
    print("Starting preflight checks...")

    # Read only values are expected 0/1 for a normal switch, 1/1 for a read only switch
    slave_result = slave.execute("SELECT @@GLOBAL.read_only")
    if not slave_result["success"]:
        print("[ERROR]: Read only status could be not read from the new master")
        sys.exit(-1)
    elif not slave_result["rows"][0][0] == 1:
        print(
            (
                "[ERROR]: Initial read_only status "
                "original slave read_only: {}"
            ).format(slave_result["rows"][0][0])
        )
        sys.exit(-1)
    print(
        (
            "* Original read only values are as expected "
            "(slave: read_only=True)"
        )
    )


def wait_for_slave_to_catch_up(master_replication, slave_replication, timeout):
    timeout_start = time.time()
    while not slave_replication.caught_up_to_master(master_replication.connection):
        time.sleep(0.1)
        if time.time() > (timeout_start + timeout):
            break
    if not slave_replication.caught_up_to_master(master_replication.connection):
        print(
            "[ERROR]: We could not wait to catch up replication, trying now to "
            "revert read only on the master back to read-write"
        )
        result = master_replication.connection.execute("SET GLOBAL read_only = 0")
        if not result["success"]:
            print(
                "[ERROR]: We could not revert the master back to read_only, "
                "server may be down or other issues"
            )
        else:
            print("Switchover failed, but we put back the master in read/write again")
        print("Try increasing the timeout parameter, or debuging the current status")
        sys.exit(-1)

    print(
        "Slave caught up to the master after waiting {} seconds".format(
            str(time.time() - timeout_start)
        )
    )


def stop_slave(slave_replication):
    print("Stopping original master->slave replication")
    result = slave_replication.stop_slave()
    if not result["success"]:
        print("Could not stop slave: {}".format(result["errmsg"]))
        sys.exit(-1)


def set_replica_in_read_write(master_replication, slave_replication):
    slave = slave_replication.connection
    print("Setting up replica as read-write")
    result = slave.execute("SET GLOBAL read_only = 0")
    if not result["success"]:
        print("[ERROR]: Could not set the slave as read write")
        sys.exit(-1)

    slave_result = slave.execute("SELECT @@GLOBAL.read_only")
    if (
        not slave_result["success"]
        or not slave_result["numrows"] == 1
        or not slave_result["rows"][0][0] == 0
    ):
        print(
            "[ERROR]: Post check failed, current status: "
            "original slave read_only: {}".format(slave_result["rows"][0][0])
        )
        sys.exit(-1)
    print(
        "All commands where successful, current status: "
        "original slave read_only: {}".format(slave_result["rows"][0][0])
    )


def invert_replication_direction(
    master_replication, slave_replication, master_status_on_switch
):
    slave = slave_replication.connection
    print("Trying to invert replication direction")
    result = master_replication.setup(
        master_host=slave.host,
        master_port=slave.port,
        master_log_file=master_status_on_switch["file"],
        master_log_pos=master_status_on_switch["position"],
    )
    if not result["success"]:
        print("[ERROR]: We could not repoint the original master to the new one")
        sys.exit(-1)
    result = slave_replication.reset_slave()
    if not result["success"]:
        print("[ERROR]: We could not reset replication on the new master")
        sys.exit(-1)

## DONE UP TO HERE


def setup_new_master_replication(slave_replication, old_master_slave_status):
    """
    Restore old replication setup from the old master into the new master
    """
    #TODO: I don't understand this
    # change master
    result = slave_replication.setup(
        master_host=old_master_slave_status["master_host"],
        master_port=old_master_slave_status["master_port"],
        master_log_file=old_master_slave_status["relay_master_log_file"],
        master_log_pos=old_master_slave_status["exec_master_log_pos"],
    )
    if not result["success"]:
        print(
            (
                "[ERROR]: Old replication setup was not able to be recovered, "
                "new master will not be configured as a slave"
            )
        )
        return -1
    # start slave
    if (
        old_master_slave_status["slave_io_running"] != "No"
        and old_master_slave_status["slave_sql_running"] != "No"
    ):
        print("Restarting new master replication (both threads)")
        result = slave_replication.start_slave()
    elif (
        old_master_slave_status["slave_io_running"] != "No"
        and old_master_slave_status["slave_sql_running"] == "No"
    ):
        print("Restarting new master replication io thread")
        result = slave_replication.start_slave(thread="io")
    elif (
        old_master_slave_status["slave_io_running"] == "No"
        and not old_master_slave_status["slave_sql_running"] != "No"
    ):
        print("Restarting new master replication sql thread")
    else:
        result = dict()
        result["success"] = True
    if not result["success"]:
        print(
            (
                "[ERROR]: Old replication setup was not able to be recovered, "
                "new master will not be configured as a slave"
            )
        )
        return -1
    # set gtid
    if old_master_slave_status["using_gtid"].lower() in ["slave_pos", "current_pos"]:
        changed = slave_replication.set_gtid_mode(old_master_slave_status["using_gtid"])
        if not changed:
            print("[ERROR]: Original GTID mode was not recovered on the new master")
    return 0


def verify_status_after_switch(
    master_replication, slave_replication, timeout, replicating_master, read_only_master
):
    slave = slave_replication.connection
    print("Verifying everything went as expected...")
    slave_result = slave.execute("SELECT @@GLOBAL.read_only")
    if not slave_result["success"]:
        print("[ERROR] read_only status of one or more servers could not be checked")
        sys.exit(-1)
    elif not read_only_master and slave_result["rows"][0][0] == 0:
        print(
            "[ERROR]: Read_only status verification failed: "
            "original slave read_only: {}".format(
                slave_result["rows"][0][0]
            )
        )
        sys.exit(-1)
    elif read_only_master and not slave_result["rows"][0][0] == 1:
        print(
            "[ERROR]: Read_only status verification failed: "
            "original slave read_only: {}".format(
                slave_result["rows"][0][0]
            )
        )
        sys.exit(-1)

    slave_status = slave_replication.slave_status()
    if replicating_master and slave_status is None:
        print(
            "[ERROR]: --replicating-master was set, but the new master is not replicating from anywhere"
        )
        sys.exit(-1)


def move_replicas_to_new_master(master_replication, slave_replication, timeout):
    """
    Migrates all old master direct slaves to the new master, maintaining the consistency.
    """
    print("Disabling GTID on new master...")
    slave_replication.set_gtid_mode("no")
    clients = 0
    # TODO: here needs to get it from orch
    for replica in master_replication.slaves():
        print(
            "Checking if {} needs to be moved under the new master...".format(
                replica.name()
            )
        )
        if replica.is_same_instance_as(slave_replication.connection):
            print("Nope")
            continue  # do not move the target replica to itself
        replication = WMFReplication(replica, timeout)
        print("Disabling GTID on {}...".format(replica.name()))
        replication.set_gtid_mode("no")
        print("Waiting some seconds for db to catch up...")
        time.sleep(timeout)
        result = replication.move(
            new_master=slave_replication.connection, start_if_stopped=True
        )
        if result is None or not result["success"]:
            print(
                "[ERROR]: {} failed to be moved under the new master".format(
                    replica.name()
                )
            )
            sys.exit(-1)
        print("Reenabling GTID on {}...".format(replica.name()))
        replication.set_gtid_mode("slave_pos")
        print("{} was moved successfully under the new master".format(replica.name()))
        clients += 1

    query = "SHOW GLOBAL STATUS like 'Rpl_semi_sync_master_clients'"
    result = slave_replication.connection.execute(query)
    if (
        not result["success"]
        or result["numrows"] != 1
        or int(result["rows"][0][1]) < clients
    ):
        print("[WARNING]: Semisync was not enabled on all hosts")
        return -1
    return 0


def stop_heartbeat(master):
    """
    Stops pt-heartbeat on the host. On failure, the process exits with an error.
    """
    print("Stopping heartbeat on %s" % master.name())
    runner = RemoteExecution()
    result = runner.run(master.host, "systemctl stop %s" % HEARTBEAT_SERVICE)
    if result.returncode != 0:
        print("[ERROR]: Could not stop the %s service" % HEARTBEAT_SERVICE)
        sys.exit(-1)


def start_heartbeat(master):
    """
    Starts heartbeat on the given master. On failure, the process exits with an error.
    """
    print("Starting heartbeat on %s" % master.name())
    runner = RemoteExecution()
    result = runner.run(
        master.host,
        "systemctl start %s; systemctl is-active %s"
        % (HEARTBEAT_SERVICE, HEARTBEAT_SERVICE),
    )
    if result.returncode != 0:
        print(
            "[ERROR]: Could not run pt-heartbeat-wikimedia, got output: {} {}".format(
                runner.stdout, runner.stderr
            )
        )
        sys.exit(-1)


def update_zarcillo(slave, section, dc):
    """
    After switching over the master role from the 'master' host to the 'slave' one,
    update zarcillo so it reflects reality
    """
    print("Updating zarcillo...")
    # get section and dc of the original master
    zarcillo = WMFMariaDB(ZARCILLO_INSTANCE, database="zarcillo")
    section = result["rows"][0][0]
    dc = result["rows"][0][1]
    # update section with section name from the former slave
    query = (
        "SET STATEMENT binlog_format='ROW' FOR "  # Workaround for T272954
        "UPDATE masters "
        "SET instance = (SELECT name "
        "                FROM instances "
        "                WHERE server = '{}' AND port = {})"
        "WHERE section = '{}' AND dc = '{}' LIMIT 1"
    )
    result = zarcillo.execute(query.format(slave.host, slave.port, section, dc))
    if not result["success"]:
        print("[WARNING] New master could not be updated on zarcillo")
        return -1
    print(
        ("Zarcillo updated successfully: " "{} is the new master of {} at {}").format(
            slave.name(), section, dc
        )
    )
    return 0


def handle_new_master_semisync_replication(slave):
    # Disable semi_sync_replica and enable semi_sync_master on the new master
    result = slave.execute("SET GLOBAL rpl_semi_sync_slave_enabled = 0")
    if not result["success"]:
        print("[WARNING] Semisync slave could not be disabled on the new master")
    if slave.get_version() < (10, 3, 0):
        slave.execute("UNINSTALL PLUGIN rpl_semi_sync_slave")
        slave.execute("INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so'")
    result = slave.execute("SET GLOBAL rpl_semi_sync_master_enabled = 1")
    if not result["success"]:
        print("[WARNING] Semisync could not be enabled on the new master")


def update_events(slave):
    # TODO full automation- requires core db detection
    print(
        (
            "Please remember to run the following commands as root to "
            "update the events if they are Mediawiki databases:"
        )
    )
    print(
        "curl -sS 'https://gerrit.wikimedia.org/r/plugins/gitiles/operations/software/+/refs/heads/master/dbtools/events_coredb_slave.sql?format=TEXT' | base64 -d | db-mysql old master"
    )
    print(
        "curl -sS 'https://gerrit.wikimedia.org/r/plugins/gitiles/operations/software/+/refs/heads/master/dbtools/events_coredb_master.sql?format=TEXT' | base64 -d | db-mysql {}".format(
            slave
        )
    )
    return 0


def ask_for_confirmation(slave):
    """
    Prompt console for confirmation of action of stopping instances replication
    """
    answer = ""
    while answer not in ["yes", "no"]:
        answer = input(
            "Are you sure you want to switchover to "
            "promote {} as master [yes/no]? ".format(slave)
        ).lower()
        if answer not in ["yes", "no"]:
            print('Please type "yes" or "no"')
    if answer == "no":
        print("Aborting switchover without touching anything!")
        sys.exit(0)


def main():
    # Preparatory steps
    options = handle_parameters()
    master = WMFMariaDB(options.master)
    slave = WMFMariaDB(options.slave)
    timeout = options.timeout
    slave_replication = WMFReplication(slave, timeout)
    master_replication = WMFReplication(master, timeout)
    replicating_master = options.replicating_master
    read_only_master = options.read_only_master

    do_preflight_checks(
        master_replication,
        slave_replication,
        timeout,
        replicating_master,
        read_only_master,
    )

    if not options.skip_slave_move:
        handle_new_master_semisync_replication(slave)
        move_replicas_to_new_master(master_replication, slave_replication, timeout)

    if options.only_slave_move:
        print(
            "SUCCESS: All slaves moved correctly, but not continuing further because --only-slave-move"
        )
        sys.exit(0)

    if not options.force:
        ask_for_confirmation(options.master, options.slave)

    # core steps
    if not options.skip_heartbeat:
        stop_heartbeat(master)

    wait_for_slave_to_catch_up(master_replication, slave_replication, timeout)

    slave_status_on_switch = slave_replication.slave_status()
    master_status_on_switch = slave_replication.master_status()
    print(
        "Servers sync at master: {} slave: {}".format(
            slave_status_on_switch["relay_master_log_file"]
            + ":"
            + str(slave_status_on_switch["exec_master_log_pos"]),
            master_status_on_switch["file"]
            + ":"
            + str(master_status_on_switch["position"]),
        )
    )
    stop_slave(slave_replication)

    if not read_only_master:
        set_replica_in_read_write(master_replication, slave_replication)

    invert_replication_direction(
        master_replication, slave_replication, master_status_on_switch
    )

    if not options.skip_heartbeat:
        start_heartbeat(slave)

    if replicating_master:
        setup_new_master_replication(slave_replication, old_master_slave_status)

    verify_status_after_switch(
        master_replication,
        slave_replication,
        timeout,
        replicating_master,
        read_only_master,
    )

    print("SUCCESS: Master switch completed successfully")

    # Additional steps
    update_zarcillo(slave, options.section, options.dc)
    update_events(options.slave)

    sys.exit(0)


if __name__ == "__main__":
    main()
