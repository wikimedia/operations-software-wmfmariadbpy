"""
Switchover helper unit and functional tests

Run coverage test with:
```
pytest --cov=switchover_helper --cov-report=html --cov-report=term
# or
just coverage
```

Captured outputs are in `__snapshots__/switchover_helper_test.ambr`

Update expected output (snapshot fixture) using:
```
./.tox/py3/bin/pytest --snapshot-update
```

"""

from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch, call
from pytest import fixture
import json
import logging
import pytest
import warnings

# flake8: noqa: E402
warnings.filterwarnings("ignore", category=UserWarning, message="pkg_resources is deprecated as an API.*")

import switchover_helper as sh
from spicerack.mysql import Instance as MInst


# safety and autouse fixtures #


@pytest.fixture(autouse=True, scope="module")
def safety_automock():
    # block stuff that will be mocked later on
    sh.httpx = None  # type: ignore
    sh._http_client = None  # type: ignore
    sh._zarcillo_client = None  # type: ignore


@pytest.fixture(autouse=True, scope="module")
def mock_timestamp():
    sh._maintenance_completion_timestamp = Mock(return_value="<mock ts>")


@pytest.fixture(autouse=True, scope="module")
def mock_ask():
    def ask(msg: str) -> bool:
        log.info(f"asking: {msg}")
        return True

    sh.ask = ask
    sh.ask_stop = ask


@pytest.fixture(autouse=True, scope="module")
def mock_dload_sql():
    sh._download_events_sql_query = Mock(return_value="mock-sql")


# logging #

log = logging.getLogger()


@pytest.fixture(autouse=True)
def setup_logging(caplog):
    caplog.set_level(logging.DEBUG, logger="")

    fmt = logging.Formatter("%(message)s")
    for h in log.handlers:
        h.setFormatter(fmt)


# fixtures and helpers #


def _mock_fetch_text(url: str) -> str:
    # Read text from disk instead of http get
    log = logging.getLogger()
    log.info(f"mock-fetching {url}")
    fname = (
        url.replace(":", "_")
        .replace("/", "_")
        .replace(".", "_")
        .replace("+", "_P_")
        .replace("?", "_Q_")
        .replace("=", "_EQ_")
    )
    test_file = Path(__file__).parent / "tests" / "data" / fname
    # log.info(f"mock-fetching {url} reading {test_file}") FIXME

    # cache files
    if True and not test_file.is_file():
        import httpx

        # import base64

        log.info(f"Fetching and storing {url}")
        c = httpx.get(url).text
        # decoded = base64.b64decode(c).decode("utf-8")
        # test_file.write_text(decoded)
        test_file.write_text(c)

    return test_file.read_text()


@fixture(autouse=True)
def mock_fetch_text():
    with patch.object(sh, "fetch_text", autospec=True) as m:
        m.side_effect = _mock_fetch_text
        yield m


def mock_runcmd_s8(dryrun: bool, cmd: str) -> str:
    log.info(f"mock-running <<{cmd}>>")
    expected_cmds = [
        # section: s8 db2165 -> db2161
        "sudo db-switchover --skip-slave-move db2165 db2161",
        '''sudo db-mysql db2161 heartbeat -e "DELETE FROM heartbeat WHERE file LIKE 'db2165%';"''',
        "sudo cumin 'dborch*' 'orchestrator-client -c untag -i db2161 --tag name=candidate'",
        "sudo cumin 'dborch*' 'orchestrator-client -c tag -i db2165 --tag name=candidate'",
        "sudo db-switchover --timeout=25 --only-slave-move db2165 db2161",
    ]
    if cmd == r"sudo db-mysql db2161 -e 'SHOW SLAVE STATUS' -B":
        return Path("tests/data/db2161.replica_status").read_text()
    elif cmd == r"sudo db-mysql db2165 -e 'SHOW SLAVE STATUS' -B":
        # db2165 is old primary
        return Path("tests/data/db2165.replica_status").read_text()

    elif cmd in expected_cmds:
        return f"<mock cmd output for {cmd}>"

    raise NotImplementedError(f"No mock for <<{cmd}>>")


def mock_runcmd_tests4(dryrun: bool, cmd: str) -> str:
    log.info(f"mock-running <<{cmd}>>")
    expected_cmds = [
        # section: test-s4
        # test: test_run_switch_on_standby_dc_eqiad_section_tests4
        "sudo db-switchover --timeout=25 --only-slave-move db2230 db-test2001",
        "sudo db-switchover --skip-slave-move db2230 db-test2001",
        '''sudo db-mysql db-test2001 heartbeat -e "DELETE FROM heartbeat WHERE file LIKE 'db2230%';"''',
        "sudo cumin 'dborch*' 'orchestrator-client -c untag -i db-test2001 --tag name=candidate'",
        "sudo cumin 'dborch*' 'orchestrator-client -c tag -i db2230 --tag name=candidate'",
    ]
    if cmd == "sudo db-mysql db2230 -e 'SHOW SLAVE STATUS' -B":
        return Path("tests/data/db2230.replica_status").read_text()
    elif cmd == "sudo db-mysql db-test2001 -e 'SHOW SLAVE STATUS' -B":
        return Path("tests/data/db-test2001.replica_status").read_text()

    elif cmd in expected_cmds:
        return f"<mock cmd output for {cmd}>"

    raise NotImplementedError(f"No mock for <<{cmd}>>")


def mock_runcmd_s3(dryrun: bool, cmd: str) -> str:
    log.info(f"mock-running <<{cmd}>>")

    expected_cmds = [
        #
        # section: s3
        # test: test_run_switch_on_standby_dc
        #
        "sudo db-switchover --timeout=25 --replicating-master --read-only-master --only-slave-move db1223 db1189",
        "sudo db-switchover --replicating-master --read-only-master --skip-slave-move db1223 db1189",
        '''sudo db-mysql db1189 heartbeat -e "DELETE FROM heartbeat WHERE file LIKE 'db1223%';"''',
        "sudo cumin 'dborch*' 'orchestrator-client -c untag -i db1189 --tag name=candidate'",
        "sudo cumin 'dborch*' 'orchestrator-client -c tag -i db1223 --tag name=candidate'",
        "sudo db-switchover --timeout=25 --only-slave-move db1223 db1189",
        "sudo db-switchover --skip-slave-move db1223 db1189",
    ]

    # s3 test
    if cmd == r"sudo db-mysql db1189 -e 'SHOW SLAVE STATUS' -B":
        return Path("tests/data/db1189.replica_status").read_text()

    elif cmd == r"sudo db-mysql db1223 -e 'SHOW SLAVE STATUS' -B":
        return Path("tests/data/db1223.replica_status").read_text()

    elif cmd in expected_cmds:
        return f"<mock cmd output for {cmd}>"

    raise NotImplementedError(f"No mock for <<{cmd}>>")


def mock_vertical_queries(hn: str):
    def _mock(query: str) -> list[dict]:
        assert query in ("SHOW VARIABLES", "SHOW SLAVE STATUS")
        if query == "SHOW VARIABLES":
            log.info(f"<<mocked SHOW VARIABLES on {hn}>>")
            with open(f"tests/data/{hn}.show_variables.json") as f:
                return json.load(f)
        else:
            log.info(f"<<mocked SHOW SLAVE STATUS on {hn}>>")
            with open(f"tests/data/{hn}.replica_status.json") as f:
                return json.load(f)

    return _mock


def mock_minst_for_run_vert_query(hn: str):
    m = Mock(name="<<mock MInst>>", spec=MInst)
    m.run_vertical_query.side_effect = mock_vertical_queries(hn)
    return m


def mock_extract_db_instance(hn1: str, hn2: str):
    # The wrapping is needed for the assert

    def _extract_db_instance(spicerack, hn: str, dc: str):
        assert hn in (hn1, hn2), f"Missing _extract_db_instance mock for {hn}"
        return mock_minst_for_run_vert_query(hn)

    return _extract_db_instance


# tests #


def test_validate_hostname_extract_dc_fqdn():
    data = [["db1234", "eqiad", "db1234.eqiad.wmnet"], ["db2234", "codfw", "db2234.codfw.wmnet"]]
    for hn, dc, fqdn in data:
        assert sh.validate_hostname_extract_dc_fqdn(hn) == (dc, fqdn)

    for hn in ["", "d1234", "db123", "db12345", "db3333", "-db1111", "db-1111"]:
        with pytest.raises(ValueError):
            sh.validate_hostname_extract_dc_fqdn(hn)

    with pytest.raises(ValueError):
        sh.validate_hostname_extract_dc_fqdn("db123")

    with pytest.raises(ValueError):
        sh.validate_hostname_extract_dc_fqdn("db12345")

    with pytest.raises(ValueError):
        sh.validate_hostname_extract_dc_fqdn("db7777")


@patch("switchover_helper._runcmd", side_effect=mock_runcmd_s8)
def test_compare_mariadb_vars_ok_active_dc(mrun, caplog, snapshot):
    """s8 db2165 -> db2161 flip in the active DC"""
    m1 = mock_minst_for_run_vert_query("db2165")
    m2 = mock_minst_for_run_vert_query("db2161")
    ok = sh._compare_mariadb_variables("db2165", m1, "db2161", m2, True)
    assert ok
    assert caplog.text == snapshot


@patch("switchover_helper._runcmd", side_effect=mock_runcmd_s8)
def test_compare_mariadb_vars_standby_dc(mrun, caplog, snapshot):
    """s8 db2165 -> db2161 flip but they are configured for active DC
    while the DC is standby"""
    m1 = mock_minst_for_run_vert_query("db2165")
    m2 = mock_minst_for_run_vert_query("db2161")
    ok = sh._compare_mariadb_variables("db2165", m1, "db2161", m2, False)
    assert caplog.text == snapshot
    assert not ok


def test_compare_mariadb_vars_wrong_direction_active(caplog, snapshot):
    """We are trying to do the flip in the reverse dir"""
    m1 = mock_minst_for_run_vert_query("db2161")
    m2 = mock_minst_for_run_vert_query("db2165")

    ok = sh._compare_mariadb_variables("db2161", m1, "db2165", m2, True)
    # Two items erroring in the log
    assert caplog.text == snapshot
    assert not ok


def test_compare_mariadb_vars_wrong_direction_standby(caplog, snapshot):
    """We are trying to do the flip in the reverse direction, plus the DC is standby
    but db2165 is RW"""
    m1 = mock_minst_for_run_vert_query("db2161")
    m2 = mock_minst_for_run_vert_query("db2165")

    ok = sh._compare_mariadb_variables("db2161", m1, "db2165", m2, False)
    # "✖ db2165 is not read-only"
    assert caplog.text == snapshot
    assert not ok


def test_find_candidates():
    replica_names = ["db1176", "db2209"]
    candidates = sh.find_candidate_masters(replica_names)
    assert candidates == ["db2209"]


def test_find_oldpri_and_candidates():
    oldpri, cands = sh._fallback_find_oldpri_and_candidates("codfw", "s2")
    assert oldpri == "db2204"
    assert cands == ["db2207"]


def test_find_primary_dc():
    dc = sh._find_active_dc()
    assert dc == "codfw"


def test_find_candidate_masters(caplog, snapshot):
    with pytest.raises(Exception):
        sh.find_candidate_masters(["db1234"])

    assert sh.find_candidate_masters(["db1184", "db1234"]) == ["db1184"]

    # assert caplog.text == snapshot


@patch("spicerack.Spicerack", autospec=True)
@patch("switchover_helper.run_dbctl_cmd")
@patch("switchover_helper._runcmd", side_effect=mock_runcmd_s8)
@patch("switchover_helper.zarcillo_post")
@patch("switchover_helper.zarcillo_get")
@patch("switchover_helper.find_candidate_masters")
def test_run_switch_on_active_dc(m_find_cand, mzarc_get, mzarc_post, mruncmd, mrun_dbctl, mspicer, caplog, snapshot):

    log.info("Test switchover in s8, active DC codfw, db2165 -> db2161")
    # Mimick https://phabricator.wikimedia.org/T409818

    mock_sr = mspicer.return_value
    dbctl = mock_sr.dbctl()
    mock_sr.admin_reason.return_value = "<mocked admin reason>"

    def mock_zarc_get(url: str) -> tuple[str, str, sh.Dc]:
        log.info(f"<Mocked Fetching Zarcillo API {url}>")
        assert url == "/api/v1/section_status/s8", f"Wrong url {url}, see mock_zarc_get"
        # Fail here to test the yaml fallback
        raise Exception("mock-zarc-err")

    mzarc_get.side_effect = mock_zarc_get

    def mock_get_dbc_inst(hn: str):
        r = Mock(name="_extract_db_instance mock", spec=MInst)  # to provide .sections
        if hn == "db2165":
            r.sections = {"s8": {"candidate_master": False, "percentage": 100, "pooled": True, "weight": 500}}
        elif hn == "db2161":
            r.sections = {"s8": {"candidate_master": True, "percentage": 100, "pooled": True, "weight": 500}}
        else:
            assert False, f"Missing mock for {hn}"
        return r

    dbctl.instance.get = mock_get_dbc_inst

    def mock_find_candidate_masters(c):
        log.info("<Mocked out find_candidate_masters>")
        assert c == ["db2166", "db2195", "db2181", "db2167", "db2152", "db2164", "db2163", "db2161", "db2154"]
        return ["db2161"]

    m_find_cand.side_effect = mock_find_candidate_masters

    sh._extract_db_instance = mock_extract_db_instance("db2165", "db2161")

    # dbctl.config.diff(force_unified=False) to show no changes

    # Mock dbctl.config.diff to show no changes
    dcd0 = Mock()
    dcd0.success = True
    dcd0.exit_code = 0
    dbctl.config.diff.return_value = [dcd0, None]

    m_dbc_set_weight = dbctl.instance.weight
    m_dbc_set_weight.return_value.success = True
    m_dbc_set_weight.return_value.announce_message = "<dbctl.instance.weight announce_message>"

    # Run the switchover
    taskid = "T409818"
    sh._run(mock_sr, "s8", "codfw", taskid, "switch")

    assert caplog.text == snapshot

    m_dbc_set_weight.assert_called_once_with("db2161", 0)

    exp = [
        call.dbctl(),
        call.dbctl(),
        call.dbctl(),
        call.admin_reason("primary switchover in s8 T409818"),
        call.run_cookbook(
            "sre.hosts.downtime",
            ["--hours", "1", "-r", "Primary switchover s8 T409818", "A:db-section-s8"],
            confirm=True,
        ),
        call.puppet("db2165"),
        call.puppet("db2161"),
        call.sal_logger.info("Starting s8 codfw failover from db2165 to db2161 - T409818"),
        call.puppet("db2165"),
        call.puppet("db2161"),
        call.run_cookbook(
            "sre.mysql.depool", ["--reason", "<mocked admin reason>", "-t", "T409818", "db2165"], confirm=True
        ),
    ]

    assert exp == mock_sr.method_calls


@patch("spicerack.Spicerack", autospec=True)
@patch("switchover_helper.zarcillo_post")
@patch("switchover_helper.zarcillo_get")
@patch("switchover_helper._check_replication_health")
@patch("switchover_helper._compare_mariadb_variables")
@patch("switchover_helper.find_candidate_masters")
def test_run_switch_on_active_dc_dryrun(
    m_find_cand, _m_comp_var, m_check_replication_healt, mzarc_get, mzarc_post, mspicer, caplog, snapshot
):
    log.info("Test dryrun switchover in s8, active DC codfw, db2165 -> db2161")
    # Mimick https://phabricator.wikimedia.org/T409818

    mock_sr = mspicer.return_value
    dbctl = mock_sr.dbctl()
    mock_sr.admin_reason.return_value = "<mocked admin reason>"

    def mock_zarc_get(url: str) -> tuple[str, str, sh.Dc]:
        log.info(f"<Mocked Fetching Zarcillo API {url}>")
        assert url == "/api/v1/section_status/s8", f"Wrong url {url}, see mock_zarc_get"
        # Fail here to test the yaml fallback
        raise Exception("mock-zarc-err")

    mzarc_get.side_effect = mock_zarc_get

    def mock_get_dbc_inst(hn: str):
        r = Mock()  # to provide .sections
        if hn == "db2165":
            r.sections = {"s8": {"candidate_master": False, "percentage": 100, "pooled": True, "weight": 500}}
        elif hn == "db2161":
            r.sections = {"s8": {"candidate_master": True, "percentage": 100, "pooled": True, "weight": 500}}
        else:
            assert False, f"Missing mock for {hn}"
        return r

    dbctl.instance.get = mock_get_dbc_inst

    sh._extract_db_instance = mock_extract_db_instance("db2165", "db2161")

    def mock_find_candidate_masters(c):
        log.info("<Mocked out find_candidate_masters>")
        assert c == ["db2166", "db2195", "db2181", "db2167", "db2152", "db2164", "db2163", "db2161", "db2154"]
        return ["db2161"]

    m_find_cand.side_effect = mock_find_candidate_masters

    # dbctl.config.diff(force_unified=False) to show no changes

    # Mock dbctl.config.diff to show no changes
    dcd0 = Mock()
    dcd0.success = True
    dcd0.exit_code = 0
    dbctl.config.diff.return_value = [dcd0, None]

    m_dbc_set_weight = dbctl.instance.weight
    m_dbc_set_weight.return_value.success = True
    m_dbc_set_weight.return_value.announce_message = "<dbctl.instance.weight announce_message>"

    # Run the dryrun switchover
    taskid = "T409818"
    sh._run(mock_sr, "s8", "codfw", taskid, "dryrun")

    assert caplog.text == snapshot

    exp = [
        call.dbctl(),
        call.dbctl(),
        call.dbctl(),
        call.admin_reason("primary switchover in s8 T409818"),
    ]

    assert exp == mock_sr.method_calls

    m_dbc_set_weight.assert_not_called()


@patch("spicerack.Spicerack", autospec=True)
@patch("switchover_helper.run_dbctl_cmd")
@patch("switchover_helper._runcmd", side_effect=mock_runcmd_s3)
@patch("switchover_helper.zarcillo_post")
@patch("switchover_helper.zarcillo_get")
def test_run_switch_on_standby_dc_eqiad_section_s3(
    mzarc_get, mzarc_post, mruncmd, mrun_dbctl, mspicer, caplog, snapshot
):

    log.info("Test switchover in s3, standby DC eqiad, db1223 -> db1189")

    mock_sr = mspicer.return_value
    dbctl = mock_sr.dbctl()

    def mock_get_dbc_inst(hn: str):
        r = Mock()  # to provide .sections
        if hn == "db1223":
            r.sections = {"s3": {"candidate_master": False, "percentage": 100, "pooled": True, "weight": 500}}
        elif hn == "db1189":
            r.sections = {"s3": {"candidate_master": True, "percentage": 100, "pooled": True, "weight": 500}}
        else:
            assert False, f"Missing mock for {hn}"
        return r

    dbctl.instance.get = mock_get_dbc_inst

    sh._extract_db_instance = mock_extract_db_instance("db1223", "db1189")

    def mock_zarc_get(url: str) -> tuple[str, str, sh.Dc]:
        log.info(f"<Mocked Fetching Zarcillo API {url}>")
        assert url == "/api/v1/section_status/s3", f"Wrong url {url}, see mock_zarc_get"
        return json.loads(_mock_fetch_text(url))

    mzarc_get.side_effect = mock_zarc_get

    # dbctl.config.diff(force_unified=False) to show no changes

    # Mock dbctl.config.diff to show no changes
    dcd0 = Mock()
    dcd0.success = True
    dcd0.exit_code = 0
    dbctl.config.diff.return_value = [dcd0, None]

    m_dbc_set_weight = dbctl.instance.weight
    m_dbc_set_weight.return_value.success = True
    m_dbc_set_weight.return_value.announce_message = "<dbctl.instance.weight announce_message>"

    # Run the switchover
    sh._run(mock_sr, "s3", "eqiad", None, "switch")
    assert "Unable to fetch section" not in caplog.text  # uses zarcillo
    assert caplog.text == snapshot
    m_dbc_set_weight.assert_called_once_with("db1189", 0)


@patch("switchover_helper.find_candidate_masters")
@patch("spicerack.Spicerack", autospec=True)
@patch("switchover_helper.run_dbctl_cmd")
@patch("switchover_helper._runcmd", side_effect=mock_runcmd_s3)
@patch("switchover_helper.zarcillo_post")
@patch("switchover_helper.zarcillo_get")
def test_show_on_standby_dc_eqiad_section_s3(
    mzarc_get, mzarc_post, mruncmd, mrun_dbctl, mspicer, m_find_cand, caplog, snapshot
):

    log.info("Test show action in s3, standby DC eqiad, db1223 -> db1189")

    mock_sr = mspicer.return_value
    dbctl = mock_sr.dbctl()

    def mock_get_dbc_inst(hn: str):
        r = Mock()  # to provide .sections
        if hn == "db1223":
            r.sections = {"s3": {"candidate_master": False, "percentage": 100, "pooled": True, "weight": 500}}
        elif hn == "db1189":
            r.sections = {"s3": {"candidate_master": True, "percentage": 100, "pooled": True, "weight": 500}}
        else:
            assert False, f"Missing mock for {hn}"
        return r

    dbctl.instance.get = mock_get_dbc_inst

    def _m_find_cand(replicas: list[str]):
        if replicas == ["db1166", "db1189", "db1212", "db1198", "db1175", "db1157"]:
            return ["db1189"]
        assert False, "Missing mock for {replicas}"

    m_find_cand.side_effect = _m_find_cand

    def mock_zarc_get(url: str) -> tuple[str, str, sh.Dc]:
        log.info(f"<Mocked Fetching Zarcillo API {url}>")
        assert url == "/api/v1/section_status/s3", f"Wrong url {url}, see mock_zarc_get"
        return json.loads(_mock_fetch_text(url))

    mzarc_get.side_effect = mock_zarc_get

    sh._extract_db_instance = mock_extract_db_instance("db1223", "db1189")

    # dbctl.config.diff(force_unified=False) to show no changes

    # Mock dbctl.config.diff to show no changes
    dcd0 = Mock()
    dcd0.success = True
    dcd0.exit_code = 0
    dbctl.config.diff.return_value = [dcd0, None]

    m_dbc_set_weight = dbctl.instance.weight
    m_dbc_set_weight.return_value.success = True
    m_dbc_set_weight.return_value.announce_message = "<dbctl.instance.weight announce_message>"

    # Run the switchover
    sh._run(mock_sr, "s3", "eqiad", None, "show")
    assert "Unable to fetch section" not in caplog.text

    assert caplog.text == snapshot


@patch("spicerack.Spicerack", autospec=True)
@patch("switchover_helper.run_dbctl_cmd")
@patch("switchover_helper._runcmd", side_effect=mock_runcmd_tests4)
@patch("switchover_helper.zarcillo_post")
@patch("switchover_helper.zarcillo_get")
def TODO_test_run_switch_on_standby_dc_eqiad_section_tests4(
    mzarc_get, mzarc_post, mruncmd, mrun_dbctl, mspicer, caplog, snapshot
):

    log.info("Test switchover in test-s4, active DC")

    mock_sr = mspicer.return_value
    dbctl = mock_sr.dbctl()

    def mock_get_dbc_inst(hn: str):
        r = Mock()  # to provide .sections
        if hn == "db2230":
            r.sections = {"test-s4": {"candidate_master": False, "percentage": 100, "pooled": True, "weight": 500}}
        elif hn == "db-test2001":
            r.sections = {"test-s4": {"candidate_master": True, "percentage": 100, "pooled": True, "weight": 500}}
        else:
            assert False, f"Missing mock for {hn}"
        return r

    dbctl.instance.get = mock_get_dbc_inst

    sh._extract_db_instance = mock_extract_db_instance("db2230", "db-test2001")

    # dbctl.config.diff(force_unified=False) to show no changes

    # Mock dbctl.config.diff to show no changes
    dcd0 = Mock()
    dcd0.success = True
    dcd0.exit_code = 0
    dbctl.config.diff.return_value = [dcd0, None]

    m_dbc_set_weight = dbctl.instance.weight
    m_dbc_set_weight.return_value.success = True
    m_dbc_set_weight.return_value.announce_message = "<dbctl.instance.weight announce_message>"

    # Run the switchover
    sh._run(mock_sr, "test-s4", "codfw", None, "switch")
    assert "Unable to fetch section" not in caplog.text

    assert caplog.text == snapshot

    m_dbc_set_weight.assert_called_once_with("db1189", 0)


def test_no_fixmes_in_snapshot():
    p = Path("__snapshots__/switchover_helper_test.ambr")
    assert "FIXME" not in p.read_text()
