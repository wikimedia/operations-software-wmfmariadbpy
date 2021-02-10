import subprocess
from typing import List

import pytest

from wmfmariadbpy.test.integration_env import common


def arg_id(arg):
    return "_".join(arg)


@pytest.mark.usefixtures("deploy_single_all_versions")
class TestMysqlpy:
    args = [
        ("-h", "127.0.0.1:"),
        ("-h127.0.0.1:",),
        ("--host", "127.0.0.1:"),
        ("--host=127.0.0.1:",),
    ]

    def build_cmd(self, args: List, addition: str) -> List[str]:
        cmd = ["mysql.py"] + list(args)
        cmd[-1] += addition
        cmd += ["-BNe", "SELECT @@VERSION"]
        return cmd

    def check_output(self, output: bytes, version: str) -> None:
        # Output looks like this: b'10.1.44-MariaDB\n'
        text = output.decode("utf8")
        assert text.split("-")[0] == version

    @pytest.mark.parametrize("args", args, ids=arg_id)
    def test_port(self, deploy_single_all_versions, args):
        ver = deploy_single_all_versions
        cmd = self.build_cmd(list(args), str(common.BASE_PORT))
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        self.check_output(ret.stdout, ver)

    @pytest.mark.parametrize("args", args, ids=arg_id)
    def test_section(self, deploy_single_all_versions, args):
        ver = deploy_single_all_versions
        section = "f0"
        cmd = self.build_cmd(list(args), section)
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        self.check_output(ret.stdout, ver)
