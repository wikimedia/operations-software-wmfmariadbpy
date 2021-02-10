import subprocess

import pytest

from wmfmariadbpy.test.integration_env import common


class TestReplicationTree:
    def _ver_assert(self, ver, line):
        assert ("version: %s," % ver) in line

    @pytest.mark.usefixtures("deploy_replicate_all_versions")
    def test_repl(self, deploy_replicate_all_versions):
        ver = deploy_replicate_all_versions
        cmd = [
            "db-replication-tree",
            "--no-color",
            "localhost:%d" % (common.BASE_PORT + 1),
        ]
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        lines = ret.stdout.decode("utf8").splitlines()
        assert lines[0].startswith("localhost")
        self._ver_assert(ver, lines[0])
        for line in lines[1:]:
            assert line.startswith("+ localhost")
            self._ver_assert(ver, line)

    @pytest.mark.usefixtures("deploy_replicate_all_versions")
    def test_single(self, deploy_replicate_all_versions):
        ver = deploy_replicate_all_versions
        cmd = [
            "db-replication-tree",
            "--no-color",
            "localhost:%d" % (common.BASE_PORT + 2),
        ]
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        lines = ret.stdout.decode("utf8").splitlines()
        assert len(lines) == 1
        assert lines[0].startswith("localhost")
        self._ver_assert(ver, lines[0])

    def test_failure(self):
        cmd = [
            "db-replication-tree",
            "--no-color",
            "localhost:1",
        ]
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        lines = ret.stdout.decode("utf8").splitlines()
        assert len(lines) == 1
        assert lines[0].startswith("localhost")
        assert lines[0].endswith("DOWN")
