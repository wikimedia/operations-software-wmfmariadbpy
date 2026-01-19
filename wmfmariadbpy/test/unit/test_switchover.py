"""
Test wmfmariadbpy/cli_admin/switchover.py
"""

from unittest.mock import MagicMock, patch
from wmfmariadbpy.cli_admin.switchover import change_replication
import pytest

# import wmfmariadbpy.cli_admin.switchover as so


# # fixtures # #


@pytest.fixture
def m_replica():
    replica = MagicMock()
    replica.name.return_value = "db1234.example.org"
    replica.is_same_instance_as.return_value = False
    return replica


@pytest.fixture
def m_slaverepl():
    return MagicMock()


@pytest.fixture
def m_replication():
    repl = MagicMock()
    repl.move.return_value = {"success": True}
    return repl


@pytest.fixture(autouse=True)
def mock_sleep():
    with patch("wmfmariadbpy.cli_admin.switchover.time.sleep"):
        yield


# # tests # #


def test_change_replication_nope(m_replica, m_slaverepl, capsys):
    m_replica.is_same_instance_as.return_value = True

    change_replication(m_replica, m_slaverepl, 5, 5)

    # no exception: it's ok
    out = capsys.readouterr().out
    exp = """\
Checking if db1234.example.org needs to be moved under the new master...
Nope
"""
    assert exp == out


@patch("wmfmariadbpy.cli_admin.switchover.WMFReplication", autospec=True)
def test_change_replication_move_returns_none(m_replication_class, m_replica, m_slaverepl, capsys):
    m_repl = MagicMock()
    m_repl.move.return_value = None
    m_replication_class.return_value = m_repl

    with pytest.raises(RuntimeError, match="Failed to update"):
        change_replication(m_replica, m_slaverepl, 5, 5)

    exp = """\
Checking if db1234.example.org needs to be moved under the new master...
Disabling GTID on db1234.example.org...
Waiting some seconds for db to catch up...
[ERROR]: db1234.example.org failed to be moved under the new master
"""
    assert capsys.readouterr().out == exp
    m_repl.set_gtid_mode.assert_called_once()  # no resetting GTID


@patch("wmfmariadbpy.cli_admin.switchover.WMFReplication", autospec=True)
def test_change_replication_successful_move(m_replication_class, m_replica, m_slaverepl, capsys):
    m_repl = MagicMock()
    m_repl.move.return_value = {"success": True}
    m_replication_class.return_value = m_repl

    change_replication(m_replica, m_slaverepl, 5, 5)

    m_replication_class.assert_called_once_with(m_replica, 5)
    assert m_repl.set_gtid_mode.call_count == 2
    m_repl.set_gtid_mode.assert_any_call("no")
    m_repl.set_gtid_mode.assert_any_call("slave_pos")
    m_repl.move.assert_called_once_with(new_master=m_slaverepl.connection, start_if_stopped=True)

    exp = """\
Checking if db1234.example.org needs to be moved under the new master...
Disabling GTID on db1234.example.org...
Waiting some seconds for db to catch up...
Reenabling GTID on db1234.example.org...
db1234.example.org was moved successfully under the new master
"""
    assert capsys.readouterr().out == exp
