"""Tests for CuminExecution class."""

import pytest

from wmfmariadbpy.RemoteExecution.CuminExecution import CuminExecution


@pytest.fixture
def inst():
    return CuminExecution()


def test_config(inst, mocker):
    m = mocker.patch("wmfmariadbpy.RemoteExecution.CuminExecution.cumin.Config")
    conf1 = inst.config
    conf2 = inst.config
    assert conf1 == m.return_value
    assert conf2 == m.return_value
    assert m.call_count == 1


def test_format_command(inst):
    cmd = "some command"
    assert inst.format_command(cmd) == cmd


def test_format_command_list(inst):
    assert inst.format_command(["some", "command"]) == "some command"


def test_run_invalid_host(inst, mocker):
    mocker.patch(
        "wmfmariadbpy.RemoteExecution.CuminExecution.cumin.Config",
        return_value={"transport": "clustershell", "default_backend": "knownhosts"},
    )
    host = "wrong_host.eqiad.wmnet"
    ret = inst.run(host, "some command")
    assert ret.returncode == 1
    assert ret.stdout is None
    assert ret.stderr == "host is wrong or does not match rules"
