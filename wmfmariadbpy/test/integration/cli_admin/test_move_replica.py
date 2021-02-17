import subprocess

import pytest

from wmfmariadbpy.test.integration.utils import (
    enable_gtid,
    flush_slave_hosts,
    query_db,
    tree,
)


class TestMoveReplicaBasic:
    def _assert_flat(self):
        lines = tree(10111)
        assert len(lines) == 3
        assert lines[0].startswith("localhost:10111, ")
        assert lines[1].startswith("+ localhost:10112, ")
        assert lines[2].startswith("+ localhost:10113, ")

    def _assert_vertical(self):
        lines = tree(10111)
        assert len(lines) == 3
        assert lines[0].startswith("localhost:10111, ")
        assert lines[1].startswith("+ localhost:10112, ")
        assert lines[2].startswith("  + localhost:10113, ")

    def _enable_gtid(self):
        enable_gtid(10111, [10112, 10113])

    @pytest.mark.parametrize(
        "with_gtid", [(False), (True)], ids=lambda x: ("GTID" if x else "Non-GTID")
    )
    def test_sibling_to_child(self, deploy_replicate_all_versions, with_gtid):
        if with_gtid:
            self._enable_gtid()
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
        flush_slave_hosts(10111, deploy_replicate_all_versions, 1)
        self._assert_vertical()

    @pytest.mark.parametrize(
        "with_gtid", [(False), (True)], ids=lambda x: ("GTID" if x else "Non-GTID")
    )
    def test_child_to_sibling(self, deploy_replicate_all_versions, with_gtid):
        if with_gtid:
            self._enable_gtid()
        self._assert_flat()
        # Tell :10113 to replicate from :10112
        query_db(10113, "stop slave")
        query_db(10113, "change master to master_port=10112")
        query_db(10113, "start slave")
        flush_slave_hosts(10111, deploy_replicate_all_versions, 1)
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
        flush_slave_hosts(10112, deploy_replicate_all_versions, 0)
        self._assert_flat()
