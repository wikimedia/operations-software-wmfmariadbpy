import subprocess
import time

from wmfmariadbpy.test.integration_env import common, dbver
from wmfmariadbpy.test.utils import query_db


class TestMoveReplicaBasic:
    def _tree(self):
        cmd = [
            "db-replication-tree",
            "--no-color",
            "localhost:%d" % (common.BASE_PORT + 1),
        ]
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        lines = ret.stdout.decode("utf8").splitlines()
        print("\n".join(lines))
        assert len(lines) == 3
        return lines

    def _flush(self, port: int, dver: dbver.DBVersion, target: int):
        query_db(port, "flush no_write_to_binlog binary logs")
        starttime = time.time()
        timeout = 15
        while time.time() - starttime < timeout:
            ret = query_db(port, "show slave hosts")
            print(ret)
            if len(ret) == target:
                return
            time.sleep(1.0 - ((time.time() - starttime) % 1.0))
            if dver.flavor == dbver.FLAVOR_MARIADB and dver.ver.startswith("10.1."):
                # XXX(kormat): There seems to be a bug with mariadb 10.1 where the
                # output of 'show slave hosts' can be stale for a long time.
                # Repeatedly flushing the logs works around this (somehow).
                query_db(port, "flush no_write_to_binlog binary logs")
        assert False, "Failed to converge within %ds" % timeout

    def _assert_flat(self):
        lines = self._tree()
        assert lines[0].startswith("localhost:10111, ")
        assert lines[1].startswith("+ localhost:10112, ")
        assert lines[2].startswith("+ localhost:10113, ")

    def _assert_vertical(self):
        lines = self._tree()
        assert lines[0].startswith("localhost:10111, ")
        assert lines[1].startswith("+ localhost:10112, ")
        assert lines[2].startswith("  + localhost:10113, ")

    def test_sibling_to_child(self, deploy_replicate_all_versions):
        self._assert_flat()
        cmd = [
            "db-move-replica",
            "--force",
            "--timeout=1",
            "localhost:10113",
            "localhost:10112",
        ]
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        print(ret.stdout.decode("utf8"))
        self._flush(10111, deploy_replicate_all_versions, 1)
        self._assert_vertical()

    def test_child_to_sibling(self, deploy_replicate_all_versions):
        self._assert_flat()
        # Tell :10113 to replicate from :10112
        query_db(10113, "stop slave")
        query_db(10113, "change master to master_port=10112")
        query_db(10113, "start slave")
        self._flush(10111, deploy_replicate_all_versions, 1)
        self._assert_vertical()
        # Now we're in the expected state to run the test
        cmd = [
            "db-move-replica",
            "--force",
            "--timeout=1",
            "localhost:10113",
            "localhost:10111",
        ]
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        print(ret.stdout.decode("utf8"))
        self._flush(10112, deploy_replicate_all_versions, 0)
        self._assert_flat()
