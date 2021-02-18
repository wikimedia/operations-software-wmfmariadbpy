import datetime
import os
import subprocess
import time
from typing import Any, Dict, List, Tuple, Union, cast

import pymysql

from wmfmariadbpy.test.integration_env import common, dbver


def query_db(port: int, query: str) -> Union[Tuple[()], List[Dict[str, Any]]]:
    """Run a query against an instance running in the integration-env"""
    print(
        "%s Querying localhost:%d: %s"
        % (datetime.datetime.now().isoformat(), port, query)
    )
    mycnf = os.path.join(os.path.dirname(__file__), "..", "integration_env", "my.cnf")
    assert os.path.exists(mycnf), mycnf
    conn = pymysql.connect(host="localhost", port=port, read_default_file=mycnf)
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(query)
    # Cursor.fetchall() has a very generic return type annotation as it doesn't know
    # which type of cursor has been instantiated.
    return cast(Union[Tuple[()], List[Dict[str, Any]]], cur.fetchall())


def query_db_one(port: int, query: str) -> Dict[str, Any]:
    """Same as query_db, but only returns the first result"""
    results = query_db(port, query)
    assert len(results) > 0
    return cast(List[Dict[str, Any]], results)[0]


def refresh_slave_hosts(port: int, dver: dbver.DBVersion, target: int) -> None:
    """Wait until 'show slave hosts' contains the expected number of entries."""
    starttime = time.time()
    timeout = 5
    # Poll at 1hz until either we timeout, or the target number of entries is reached.
    while time.time() - starttime < timeout:
        ret = query_db(port, "show slave hosts")
        print(ret)
        if len(ret) == target:
            print("Converged after %.2fs" % (time.time() - starttime))
            return
        time.sleep(1.0 - ((time.time() - starttime) % 1.0))
    assert False, "Failed to converge within %ds" % timeout


def tree(port: int = common.BASE_PORT + 1) -> List[str]:
    """Run db-replication-tree against localhost:port, and return the output"""
    cmd = [
        "db-replication-tree",
        "--no-color",
        "localhost:%d" % port,
    ]
    ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
    lines = ret.stdout.decode("utf8").splitlines()
    print("\n".join(lines))
    return lines


def enable_gtid(master_port: int, slave_ports: List[int]) -> None:
    """Enable gtid replication on a cluster"""
    for port in [master_port] + slave_ports:
        query_db(port, "set global gtid_domain_id = %d" % port)
    for port in slave_ports:
        query_db(port, "stop slave")
        ss_start = query_db_one(port, "show slave status")
        log_file = ss_start["Relay_Master_Log_File"]
        log_pos = ss_start["Exec_Master_Log_Pos"]
        gtid_pos = query_db_one(
            10111,
            "select binlog_gtid_pos('%s', %d) as gtid_pos" % (log_file, log_pos),
        )["gtid_pos"]
        query_db(port, "set global gtid_slave_pos = '%s'" % gtid_pos)
        query_db(port, "change master to master_use_gtid=slave_pos")
        query_db(port, "start slave")
        ss_end = query_db_one(port, "show slave status")
        assert ss_end["Using_Gtid"] == "Slave_Pos"
        assert ss_end["Gtid_IO_Pos"] == gtid_pos
