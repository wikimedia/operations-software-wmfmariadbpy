import subprocess
from typing import List, Tuple

import pytest

from wmfmariadbpy.dbutil import get_section_from_port
from wmfmariadbpy.test.integration_env import common, dbver


@pytest.mark.usefixtures("deploy_single_all_versions")
class TestMysqlpy:
    args = [
        ("-h", "127.0.0.1:"),
        ("-h127.0.0.1:",),
        ("--host", "127.0.0.1:"),
        ("--host=127.0.0.1:",),
    ]

    def get_metadata(self, index: int) -> Tuple[dbver.DBVersion, int, str]:
        d = dbver.DB_VERSIONS[index]
        port = common.BASE_PORT + index
        section = get_section_from_port(port)
        assert section, port
        return d, port, section

    def build_cmd(self, args: List, addition: str) -> List[str]:
        cmd = ["mysql.py"] + list(args)
        cmd[-1] += addition
        cmd += ["-BNe", "SELECT @@VERSION,@@PORT"]
        return cmd

    def check_output(self, output: bytes, version: str, port: int) -> None:
        # Output looks like this: b'10.1.44-MariaDB\t10110\n'
        parts = output.decode("utf8").split("\t")
        assert len(parts) == 2, output
        ver_part, port_part = parts
        assert ver_part.split("-")[0] == version
        assert port_part.strip() == str(port)

    @pytest.mark.parametrize("args", args)
    def test_port(self, args, single_idx):
        d, port, _ = self.get_metadata(single_idx)
        cmd = self.build_cmd(list(args), str(port))
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        self.check_output(ret.stdout, d.ver, port)

    @pytest.mark.parametrize("args", args)
    def test_section(self, args, single_idx):
        d, port, section = self.get_metadata(single_idx)
        cmd = self.build_cmd(list(args), section)
        ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
        self.check_output(ret.stdout, d.ver, port)
