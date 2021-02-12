import subprocess

import pytest

from wmfmariadbpy.test.integration_env import common


class TestReplicationTree:
    def _run(self, port):
        cmd = [
            "db-replication-tree",
            "--no-color",
            "localhost:%d" % port,
        ]
        return subprocess.run(cmd, check=True, stdout=subprocess.PIPE)

    def _line_assert(self, line, prefix, port, ver):
        start = "%slocalhost:%d," % (prefix, port)
        assert line.startswith(start)
        ver_part = " version: %s," % ver
        assert ver_part in line

    @pytest.mark.usefixtures("deploy_replicate_all_versions")
    def test_repl(self, deploy_replicate_all_versions):
        ver = deploy_replicate_all_versions
        port = common.BASE_PORT + 1
        ret = self._run(port)
        lines = ret.stdout.decode("utf8").splitlines()
        assert len(lines) == 3
        for i, line in enumerate(lines, start=port):
            self._line_assert(line, "" if i == port else "+ ", i, ver)

    @pytest.mark.usefixtures("deploy_replicate_all_versions")
    def test_single(self, deploy_replicate_all_versions):
        ver = deploy_replicate_all_versions
        port = common.BASE_PORT + 2
        ret = self._run(port)
        lines = ret.stdout.decode("utf8").splitlines()
        assert len(lines) == 1
        self._line_assert(lines[0], "", port, ver)

    def test_failure(self):
        ret = self._run(1)
        lines = ret.stdout.decode("utf8").splitlines()
        assert len(lines) == 1
        assert lines[0].startswith("localhost:1,")
        assert lines[0].endswith("DOWN")
