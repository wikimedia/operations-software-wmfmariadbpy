from wmfmariadbpy.test.integration.utils import tree
from wmfmariadbpy.test.integration_env import common


class TestReplicationTree:
    def _line_assert(self, line, prefix, port, ver):
        start = "%slocalhost:%d," % (prefix, port)
        assert line.startswith(start)
        ver_part = " version: %s," % ver
        assert ver_part in line

    def test_repl(self, deploy_replicate_all_versions_class):
        port = common.BASE_PORT + 1
        lines = tree(port)
        assert len(lines) == 3
        for i, line in enumerate(lines, start=port):
            self._line_assert(
                line,
                "" if i == port else "+ ",
                i,
                deploy_replicate_all_versions_class.ver,
            )

    def test_single(self, deploy_replicate_all_versions_class):
        port = common.BASE_PORT + 2
        lines = tree(port)
        assert len(lines) == 1
        self._line_assert(lines[0], "", port, deploy_replicate_all_versions_class.ver)

    def test_failure(self):
        lines = tree(1)
        assert len(lines) == 1
        assert lines[0].startswith("localhost:1,")
        assert lines[0].endswith("DOWN")
