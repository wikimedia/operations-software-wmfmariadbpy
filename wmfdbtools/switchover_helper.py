#!/usr/bin/env python3
# Copyright 2025 Wikimedia Foundation
#                Federico Ceratto <fceratto@wikimedia.org>
# Released under GPLv3
"""
Switchover helper

Run modes (action parameter):
 - show: gather switchover data, show it and exit
 - dryrun: perform dry run. Zarcillo lock is acquired, read-only DB queries are run
 - switch: perform real switchover

See:
- https://phabricator.wikimedia.org/T409926
- https://phabricator.wikimedia.org/T389376
- https://phabricator.wikimedia.org/T196366
- https://phabricator.wikimedia.org/T371483
- https://gitlab.wikimedia.org/repos/sre/wmfmariadbpy/-/merge_requests/3
"""
#
# TODO:
#  - [ ] support switching to a new master when the old master is unreachable
#  - [ ] Also use this for cross-DC switchover
#

from datetime import datetime, timedelta, timezone
from typing import Generator, Optional, Tuple
from typing import Literal
import argparse
import base64
import httpx
import logging
import os
import re
import shlex
import subprocess
import yaml
import time

from conftool.extensions.dbconfig.action import ActionResult
from conftool.extensions.dbconfig.cli import DbConfigCli
from pydantic import BaseModel
from spicerack.dbctl import Dbctl
from spicerack import Spicerack, Reason, confirm_on_failure
from spicerack.mysql import Instance as MInst
import conftool.extensions.dbconfig

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("switchover_helper")
log.setLevel(logging.DEBUG)


Dc = Literal["codfw", "eqiad"]

hostname_regex = re.compile(r"[a-z][a-z-]*[a-z](\d{4})")


def validate_hostname_extract_dc_fqdn(hn: str) -> Tuple[str, str]:
    m = hostname_regex.fullmatch(hn)
    if not m:
        raise ValueError(f"Invalid hostname '{hn}'")

    dcnum = m.group(1)[0]
    if dcnum == "1":
        dc = "eqiad"
    elif dcnum == "2":
        dc = "codfw"
    else:
        raise ValueError(f"Invalid hostname '{hn}'")

    return dc, f"{hn}.{dc}.wmnet"


def say(s: str) -> None:
    log.info(f"▶ {s}")


def run_dbctl_cmd(dryrun: bool, cmdline: str) -> None:
    if dryrun:
        log.info("Dry-running dbctl cmdline: '%s'", cmdline)
        return
    log.info("Running dbctl cmdline: '%s'", cmdline)
    args = shlex.split(cmdline)
    c = conftool.extensions.dbconfig.parse_args(args)
    dbc = DbConfigCli(c)
    dbc.setup()
    exit_code = dbc.run_action()
    if exit_code != 0:
        raise RuntimeError(f"dbctl command [{cmdline}] failed")


def _runcmd(dryrun: bool, cmdline: str) -> None:
    if dryrun:
        log.info(f"Dry-running local cmd {cmdline}")
        return ""

    log.info(f"Running local cmd {cmdline}")
    subprocess.check_call(cmdline, shell=True, text=True)


def puppet(sr: Spicerack, hn: str):
    rh = sr.remote().query(f"{hn}.*")
    return sr.puppet(rh)


# # HTTP helpers # #

_http_client = httpx.Client(timeout=15.0, transport=httpx.HTTPTransport(retries=3))
_unix_username = os.getlogin()
_zarcillo_client = httpx.Client(
    base_url="https://zarcillo.wikimedia.org",
    timeout=15.0,
    transport=httpx.HTTPTransport(retries=3),
    headers={"X-WMF-Username": _unix_username},
)


# TODO: move into zarclient


def zarcillo_get(path: str) -> dict:
    log.info(f"Fetching Zarcillo API: {path}")
    resp = _zarcillo_client.get(path)
    resp.raise_for_status()
    return resp.json()


def zarcillo_post(path: str, data: dict) -> dict:
    log.info(f"POSTing to Zarcillo API: {path}")
    resp = _zarcillo_client.post(path, json=data)
    resp.raise_for_status()
    return resp.json()


def zarcillo_lock(section: str, task: Optional[str]) -> str:

    name = f"switchover-{section}"
    desc = task or name
    req = {
        "section_names": section,
        "hostnames": "",
        "instance_names": "",
        "name": name,
        "prio": "2",
        "desc": desc,
    }
    resp = zarcillo_post("api/v1/acquire_lock", req)
    if resp.get("success", False) is False:
        raise RuntimeError(resp)

    return resp["uuid"]


def zarcillo_release_lock(uuid: str) -> None:
    resp = zarcillo_post(f"api/v1/release_lock/{uuid}", {})
    if resp.get("success", False) is False:
        raise RuntimeError(resp)


def fetch_text(url: str) -> str:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=15.0, transport=transport) as client:
        log.info(f"Fetching {url}")
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


def fetch_b64text(url: str) -> str:
    """Fetch base64 encoded text from an URL and decode it"""
    t = fetch_text(url)
    return base64.b64decode(t).decode("utf-8")


def fetch_json(url: str) -> dict:
    import json

    return json.loads(fetch_text(url))


def _download_events_sql_query(host_type: str) -> str:
    assert host_type in ["master", "slave"]
    url = "https://gerrit.wikimedia.org/r/plugins/gitiles/operations/software/+/refs/heads/master/dbtools/events_coredb_{}.sql?format=TEXT"
    url = url.format(host_type)
    log.info("Fetching %s", url)
    resp = _http_client.get(url)
    resp.raise_for_status()
    sql = base64.b64decode(resp.content).decode()
    log.info("Fetched SQL query of %s chars", len(sql))
    return sql


def _find_active_dc() -> Dc:
    # TODO: fetch this from Zarcillo with fallback on mediawiki.yaml
    url = "https://config-master.wikimedia.org/mediawiki.yaml"
    y = yaml.safe_load(fetch_text(url))
    dc = y["primary_dc"]
    assert dc in ["eqiad", "codfw"]
    return dc


def find_candidate_masters(replica_names: list[str]) -> list[str]:
    # Code copied from https://switchmaster.toolforge.org/
    tpl = (
        "https://gerrit.wikimedia.org/r/plugins/gitiles/operations/puppet/+/refs/"
        "heads/production/hieradata/hosts/%s.yaml?format=TEXT"
    )

    candidates = []
    for hn in replica_names:
        r = hn.split(":")[0]
        url = tpl % r
        content = fetch_b64text(url)
        # sigh
        if "candidate master" in content:
            candidates.append(hn)

    if candidates:
        return candidates

    raise RuntimeError(f"Candidate master not found for {replica_names}")


def _fallback_find_oldpri_and_candidates(dc: Dc, section: str) -> tuple[str, list[str]]:
    # Code copied from https://switchmaster.toolforge.org/
    log.info("fetching dbconfig JSON")
    config = fetch_json(f"https://noc.wikimedia.org/dbconfig/{dc}.json")
    section_in_config = "DEFAULT" if section == "s3" else section
    type_in_config = "sectionLoads" if section.startswith("s") else "externalLoads"
    oldpri = list(config[type_in_config][section_in_config][0].keys())[0]

    replicas = list(config[type_in_config][section_in_config][1].keys())
    candidates = find_candidate_masters(replicas)
    return (oldpri, candidates)


def _extract_db_instance(spicerack: Spicerack, hostname: str, dc: Dc) -> MInst:
    fqdn = f"{hostname}.{dc}.wmnet"
    db = spicerack.mysql().get_dbs(fqdn).list_hosts_instances()
    if len(db) != 1:
        raise RuntimeError(f"Expected 1 instance, found {len(db)}")

    return db[0]


def ask(msg: str) -> bool:
    log.info(f">> {msg}")
    while True:
        r = input("Press y to continue or s to skip\n").lower()
        if r in ("y", "s"):
            return r == "y"


def ask_stop(msg: str):
    log.info(f">> {msg}")
    while True:
        r = input("Press y to continue or ctrl-c to terminate\n").lower()
        if r == "y":
            return


# TODO: move this stuff to zarclient.py
#
class InstanceData(BaseModel):
    dc: str
    fqdn: str
    hostname: str
    instance_group: str
    instance_name: str
    mariadb_version: Optional[str]
    port: int
    alerts: list
    candidate_score: int = 0
    is_candidate_on_dbctl: bool | None = None
    is_lagging: bool | None = None
    lag: float | None = None
    pooled_value: int | None = None
    role: str | None = None
    uptime_s: int | None = None
    uptime_human: str | None = None
    tags: set = set()
    preferred_candidate: bool = False


class SectionHealth(BaseModel):
    """Describes a section and its health status in terms of number of pooled/unpooled hosts,
    alerts, replication lag, and other errors and warnings.
    """

    name: str
    groups: list[str] = []
    instance_cnt: int = 0
    # autodepool_enabled: bool = False
    # autodepooled_cnt: int = 0
    # manually_depooled_cnt: int = 0
    error_msgs: list[str] = []
    warn_msgs: list[str] = []
    roots: list
    instances: list[InstanceData]
    hp: int | None


def _print_host_summary(sh, target_dc) -> None:
    log.info("Section summary:")
    log.info("  hostname         lag       tags")
    for i in sh.instances:
        if i.dc != target_dc or i.instance_group != "core":
            continue

        lag = int(i.lag)
        line = f"  {i.hostname:12} {lag:7}       "
        for tag in ("pooled", "candidate", "preferred"):
            for t in i.tags:
                if t.endswith(tag):
                    line += f"{t:15}"

        # TODO: split pages and warnings
        # if i.alerts:
        #     line += " ALARMING"

        log.info(line.rstrip())


def _fetch_oldpri_newpri_from_zarcillo(section: str, target_dc: Dc) -> tuple[str, str, Dc, bool]:
    """Returns old primary, new primary, currently primary/active DC name"""
    active_dc = _find_active_dc()
    # log.info(f"Active DC: {active_dc}")
    path = f"/api/v1/section_status/{section}"
    d: dict = zarcillo_get(path)
    sh = SectionHealth(**d)
    del d

    # We cannot assume the primary is in the active DC as the script could be used
    # during a datacenter flip
    flipping_primary_dc = target_dc == active_dc

    # TODO: Move this logic into Zarcillo
    _print_host_summary(sh, target_dc)

    oldpri = ""
    if flipping_primary_dc:
        # Primary in active DC
        oldpri = sh.roots[0]["name"]

    else:
        # Find primary in standby DC
        for n in sh.roots[0]["children"]:
            if n["role"] == "master":
                oldpri = n["name"]

    for i in sh.instances:
        if i.dc != target_dc:
            continue

        if i.preferred_candidate:
            newpri = i.hostname

    if not oldpri:
        raise ValueError(f"Unable to detect primary in {sh}")

    return (oldpri, newpri, active_dc, flipping_primary_dc)


def _fallback_pick_best_candidate(candidates):
    # TODO: Ensure the new primary has no active alarms and has low lag
    return candidates[0]


def _run(spicerack: Spicerack, section: str, dc: Dc, task_id: Optional[str], action: str) -> None:

    try:
        oldpri, newpri, active_dc, is_active_dc = _fetch_oldpri_newpri_from_zarcillo(section, dc)
    except Exception as e:
        log.error(f"Unable to fetch section data from Zarcillo: {e}, falling back to scraping github :(")
        oldpri, candidates = _fallback_find_oldpri_and_candidates(dc, section)
        log.info(f"Candidates: {candidates}")
        newpri = _fallback_pick_best_candidate(candidates)
        active_dc = _find_active_dc()
        is_active_dc = dc == active_dc

    log.info(f"Old primary: {oldpri}")
    log.info(f"New primary: {newpri}")

    if is_active_dc:
        log.info(f"DC: {dc}. Note: This is a switchover in the ACTIVE DC")
    else:
        log.info(f"DC: {dc} (standby)")

    old_pri_mi = _extract_db_instance(spicerack, oldpri, dc)
    new_pri_mi = _extract_db_instance(spicerack, newpri, dc)

    dbctl = spicerack.dbctl()
    if section != "test-s4":
        preflight_check_dbctl(dbctl, section, oldpri, old_pri_mi, newpri, new_pri_mi, is_active_dc)
    else:
        log.info("Skipping dbctl for test-s4")

    if action == "show":
        say("Run in show mode completed.")
        return

    zarc_lock = ""
    try:
        if ask("Lock section on zarcillo"):
            zarc_lock = zarcillo_lock(section, task_id)

        dryrun = action != "switch"
        _run_switchover(spicerack, dryrun, section, dc, task_id, oldpri, newpri, is_active_dc)

    finally:
        if zarc_lock:
            log.info("Releasing lock")
            zarcillo_release_lock(zarc_lock)


def step(slug: str, msg: str) -> None:
    log.info("[%s] %s", slug, msg)


def _log_dbctl_result(res: ActionResult) -> None:
    for msg in res.messages:
        log.info(msg) if res.success else log.error(msg)
    if res.announce_message:
        log.info(res.announce_message)


def _wait_for_dbctl_diff_empty(dbctl: Dbctl) -> None:
    step("dbctl", "Waiting for dbctl diff to be empty")
    for retry in range(100):
        has_changes, _ = _get_dbctl_config_diff(dbctl)
        if not has_changes:
            return
        log.debug("Attempt %d to get clean dbctl config diff", retry)
        time.sleep(5)
    e = "Timed out while waiting for dbctl config diff to be empty"
    raise RuntimeError(e)


def _get_dbctl_config_diff(dbctl: Dbctl) -> tuple[bool, Generator]:
    """Return True for changes and a diff line generator"""
    for attempt in range(5):
        ret, diff = dbctl.config.diff(force_unified=False)
        if ret.success:
            has_changes = bool(ret.exit_code)
            return (has_changes, diff)
        time.sleep(5)
    raise RuntimeError("Unable to run `dbctl config diff` %s", ret)


def set_weight_in_dbctl(dryrun: bool, dbctl: Dbctl, reason: Reason, hn: str, weight: int) -> bool:
    """Set weight in dbctl and commit"""
    _wait_for_dbctl_diff_empty(dbctl)
    if dryrun:
        return True
    for attempt in range(5):
        step("set_weight", f"Setting weight for {hn} to {weight}")
        res = dbctl.instance.weight(hn, weight)
        _log_dbctl_result(res)
        if res.success:
            break
        time.sleep(10)
    else:
        log.error("Failed to update, exiting immediately")
        has_changes, _ = _get_dbctl_config_diff(dbctl)
        if has_changes:
            # TODO: try rolling back any change if possible
            log.error("`dbctl config diff` is not clean!")
        raise RuntimeError

    has_changes, diff = _get_dbctl_config_diff(dbctl)
    if not has_changes:
        return False

    log.info("Changes:")
    for row in diff:
        log.debug(row.rstrip())

    log.info("Committing dbctl config")
    ret = dbctl.config.commit(batch=True, comment=reason.reason)
    _log_dbctl_result(ret)
    return True


def _compare_mariadb_variables(
    oldpri: str, old_pri_mi: MInst, newpri: str, new_pri_mi: MInst, is_active_dc: bool
) -> bool:
    """Compare SHOW VARIABLES.
    Expects oldpri to be RW and newpri to be RO.
    """
    step("check_vars", "Comparing MariaDB variables")
    varnames = [
        # Storage engine
        "innodb_buffer_pool_size",
        "innodb_log_write_ahead_size",
        "innodb_flush_log_at_trx_commit",
        "innodb_file_per_table",
        # Replication
        "binlog_format",
        "gtid_mode",
        "sync_binlog",
        "log_slave_updates",
        # Connection/Query
        "max_connections",
        "max_allowed_packet",
        "sql_mode",
        # Character sets
        "character_set_server",
        "collation_server",
    ]

    # e.g. [{ "Slave_IO_State": "Waiting for master to send event", "Master_Host": "db2230.codfw.wmnet", ... }]
    old_vars = old_pri_mi.run_vertical_query("SHOW VARIABLES")[0]
    new_vars = new_pri_mi.run_vertical_query("SHOW VARIABLES")[0]
    log.info("MariaDB variables check:")
    ok = True
    for vn in varnames:
        a = old_vars.get(vn, "<missing>")
        b = new_vars.get(vn, "<missing>")
        if a == b:
            log.info(f"  ✓ {vn}")
        else:
            if vn == "innodb_buffer_pool_size":
                delta = abs(int(a) - int(b)) / float(a) * 100
                if delta < 1:
                    log.info(f"  ✓ {vn}  percentage difference: {delta:.2}%")
                    continue

            log.error(f"  ✖ {vn} differ: {a} {b}")
            ok = False

    old_is_read_only = old_vars["read_only"] == "ON"

    if is_active_dc:
        # in an active DC the old primary should be read-write
        if old_is_read_only:
            log.error(f"  ✖ {oldpri} is read-only but the DC is active")
            ok = False
        else:
            log.info(f"  ✓ {oldpri} is read-write (the DC is active)")

    else:
        # in a standby DC both primaries should be read-only
        if old_is_read_only:
            log.info(f"  ✓ {oldpri} is read-only (the DC is standby)")
        else:
            log.error(f"  ✖ {oldpri} is read-write but the DC is standby")
            ok = False

    if new_vars["read_only"] != "ON":
        log.error(f"  ✖ {newpri} is not read-only")
        ok = False

    return ok


def _check_replication_health(oldpri: str, oldpri_mi: MInst, newpri: str, newpri_mi: MInst, is_active_dc: bool) -> bool:
    log.info("Checking replication status")

    ok = True

    def good(msg: str) -> None:
        log.info(f"  ✓ {msg}")

    def warn(msg: str) -> None:
        log.warning(f"  ⚠ {msg}")

    def error(msg: str) -> None:
        nonlocal ok
        log.error(f"  ✖ {msg}")
        ok = False  # TODO

    old_repl = oldpri_mi.run_vertical_query("SHOW SLAVE STATUS")[0]
    new_repl = newpri_mi.run_vertical_query("SHOW SLAVE STATUS")[0]

    old_is_following = old_repl.get("Slave_IO_Running") == "Yes"

    if is_active_dc:
        if old_is_following:
            error(f"{oldpri} has replication running as a follower (should be primary)")
        else:
            good(f"{oldpri} is the primary and not following replication")

    else:
        if old_is_following:
            good(f"{oldpri} is in a standby DC and following replication")
        else:
            error(f"{oldpri} is in a standby DC and not following replication")

    good(f"{oldpri} is not replicating (confirmed as current primary)")

    if new_repl.get("Slave_IO_Running") != "Yes":
        error(f"{newpri} Slave_IO_Running: {new_repl.get('Slave_IO_Running')}")
    else:
        good(f"{newpri} IO thread running")

    if new_repl.get("Slave_SQL_Running") != "Yes":
        error(f"{newpri} Slave_SQL_Running: {new_repl.get('Slave_SQL_Running')}")
    else:
        good(f"{newpri} SQL thread running")

    last_io_error = new_repl.get("Last_IO_Error", "")
    last_sql_error = new_repl.get("Last_SQL_Error", "")
    if last_io_error:
        warn(f"{newpri} Last_IO_Error: {last_io_error}")

    if last_sql_error:
        warn(f"{newpri} Last_SQL_Error: {last_sql_error}")

    if not last_io_error and not last_sql_error:
        good(f"{newpri} no replication errors")

    seconds_behind = new_repl.get("Seconds_Behind_Master") or 9999
    try:
        lag = int(seconds_behind)
        if lag > 60:
            error(f"{newpri} has {lag} seconds lag")
        elif lag > 10:
            warn(f"{newpri} has {lag} seconds")
        else:
            good(f"{newpri} lag is {lag} seconds")
    except (ValueError, TypeError):
        error(f"{newpri} invalid Seconds_Behind_Master: {seconds_behind}")

    master_host = new_repl.get("Master_Host", "")
    src_hostname = master_host.split(":", 1)[0].split(".", 1)[0]
    src_port = new_repl.get("Master_Port")
    log.info(f"  ℹ {newpri} is replicating from {master_host} port {src_port}")

    if oldpri != src_hostname:
        warn(f"{newpri} is replicating from {master_host}, not {oldpri}")

    using_gtid = new_repl.get("Using_Gtid", "No")
    if using_gtid != "No":
        log.info(f"  ℹ {newpri} Using_Gtid: {using_gtid}")
        if using_gtid != "Slave_Pos":
            warn(f"{newpri} Using_Gtid: {using_gtid}")

    return ok


def preflight_check_dbctl(
    dbctl: Dbctl, section: str, oldpri: str, oldpri_mi: MInst, newpri: str, newpri_mi: MInst, is_active_dc: bool
) -> None:
    say("Check configuration differences between new and old primary:")
    step("check_dbctl", "Check dbctl conf")
    # E.g.
    # >>> dbc.instance.get('db1162').sections
    # {'s2': {'candidate_master': True, 'percentage': 100, 'pooled': True, 'weight': 500}}
    old = dbctl.instance.get(oldpri)
    if old is None:
        raise RuntimeError(f"No dbctl entity for {oldpri} found")
    old_s = old.sections
    log.debug(f"Old primary {oldpri} dbctl struct: {old_s}")
    assert section in old_s, f"Section {section} not in {old_s}"
    ks = old_s[section].keys()
    if "group" in ks:
        ask_stop(f"Unsupported 'groups' in {old_s}")

    new = dbctl.instance.get(newpri)
    if new is None:
        raise RuntimeError(f"No dbctl entity for {newpri} found")
    new_s = new.sections
    log.debug(f"new primary {newpri} dbctl struct: {new_s}")
    assert section in new_s
    ks = new_s[section].keys()
    if "group" in ks:
        ask_stop(f"Unsupported 'groups' in {new_s}")

    if new_s[section].get("candidate_master", False) is False:
        ask_stop(f"Missing candidate_master=True in {new_s}")

    ok = _compare_mariadb_variables(oldpri, oldpri_mi, newpri, newpri_mi, is_active_dc)
    if not ok and ask("WARNING: check failed, continue anyways?") is False:
        raise RuntimeError("Dbctl check failed")


def _run_cookbook(dryrun: bool, name: str, args) -> None:
    cmd = ["sudo", "cookbook", name] + args
    if dryrun:
        log.info(f"Dry-running cookbook: {cmd}")
        return
    log.info(f"Running cookbook: {cmd}")
    subprocess.check_output(cmd)


def _maintenance_completion_timestamp() -> str:
    t = datetime.now(timezone.utc) + timedelta(hours=1)
    return t.strftime("%H:%M UTC")


def _run_switchover(
    spicerack: Spicerack,
    dryrun: bool,
    section: str,
    dc: Dc,
    taskid: Optional[str],
    oldpri: str,
    newpri: str,
    is_active_dc: bool,
) -> None:

    dbctl = spicerack.dbctl()
    dbctl_dryrun = dryrun or section == "test-s4"
    oldpri_mi = _extract_db_instance(spicerack, oldpri, dc)
    newpri_mi = _extract_db_instance(spicerack, newpri, dc)
    oldprifq = f"{oldpri}.{dc}.wmnet"
    newprifq = f"{newpri}.{dc}.wmnet"

    if taskid:
        admin_reason = spicerack.admin_reason(f"primary switchover in {section} {taskid}")
    else:
        admin_reason = spicerack.admin_reason(f"primary switchover in {section} (no Phab task)")

    if ask("Silence alerts on all hosts"):
        step("downtime", f"Setting downtime on A:db-section-{section}")
        args = ["--hours", "1", "-r", f"Primary switchover {section} {taskid}", f"A:db-section-{section}"]
        _run_cookbook(dryrun, "sre.hosts.downtime", args)

    if section != "test-s4":
        if ask(f"Set new primary {newpri} dbctl weight to 0"):
            run_dbctl_cmd(dbctl_dryrun, f"instance {newpri} set-weight 0")
            set_weight_in_dbctl(dryrun, dbctl, admin_reason, newpri, 0)

    if ask("Topology changes, move all replicas under the new primary"):
        if is_active_dc:
            cmd = f"sudo db-switchover --timeout=25 --only-slave-move {oldprifq} {newprifq}"
            _runcmd(dryrun, cmd)
        else:
            cmd = "sudo db-switchover --timeout=25 --replicating-master --read-only-master "
            cmd += f"--only-slave-move {oldprifq} {newprifq}"
            _runcmd(dryrun, cmd)

    if ask(f"Disable puppet on old primary {oldpri}"):
        if not dryrun:
            p = puppet(spicerack, oldpri)
            p.disable(admin_reason)

    if ask(f"Disable puppet on new primary {newpri}"):
        if not dryrun:
            p = puppet(spicerack, newpri)
            p.disable(admin_reason)

    say("Merge gerrit puppet change to promote the primary")
    say("DIY: run this after merging on Gerrit: ssh puppetserver1001.eqiad.wmnet -t sudo -i puppet-merge")
    ask("Continue?")

    # TODO: check the puppet change has been merged and deployed

    say("Entering primary failover section")

    if ask("Log the failover on irc"):
        if not dryrun:
            spicerack.sal_logger.info(f"Starting {section} {dc} failover from {oldpri} to {newpri} - {taskid}")

    if section != "test-s4" and is_active_dc and ask(f"Set section {section} in read-only in dbctl?"):
        step("section_readonly", f"Setting section {section} read-only")
        tstamp = _maintenance_completion_timestamp()
        run_dbctl_cmd(dbctl_dryrun, f"--scope eqiad section {section} ro 'Maintenance until {tstamp} - {taskid}'")
        run_dbctl_cmd(dbctl_dryrun, f"--scope codfw section {section} ro 'Maintenance until {tstamp} - {taskid}'")
        run_dbctl_cmd(dbctl_dryrun, f"config commit -m 'Set {section} {dc} as read-only for maintenance - {taskid}'")
        say(f"Check that {section} is indeed read-only")

    if ask("Switch primaries"):
        step("switch_primary", f"Switching {oldpri} {newpri} in {section}")
        if is_active_dc:
            _runcmd(dryrun, f"sudo db-switchover --skip-slave-move {oldprifq} {newprifq}")
        else:
            cmd = f"sudo db-switchover --replicating-master --read-only-master --skip-slave-move {oldprifq} {newprifq}"
            _runcmd(dryrun, cmd)

    repl_is_ok = _check_replication_health(oldpri, oldpri_mi, newpri, newpri_mi, is_active_dc)
    if repl_is_ok is False and ask("WARNING: Replication check failed!") is False:
        log.warning("Exiting")
        return

    if section == "test-s4":
        pass

    elif is_active_dc:
        if ask("Promote new primary in dbctl and set both sections to read-write"):
            run_dbctl_cmd(dbctl_dryrun, f"--scope {dc} section {section} set-master {newpri}")
            run_dbctl_cmd(dbctl_dryrun, f"--scope eqiad section {section} rw")
            run_dbctl_cmd(dbctl_dryrun, f"--scope codfw section {section} rw")
            run_dbctl_cmd(
                dbctl_dryrun,
                f"config commit -m 'Promote {newpri} to {section} primary and set both sections read-write {taskid}'",
            )
    else:
        if ask("Promote new primary in dbctl"):
            run_dbctl_cmd(dbctl_dryrun, f"--scope {dc} section {section} set-master {newpri}")
            run_dbctl_cmd(dbctl_dryrun, f'config commit -m "Promote {newpri} to {section} primary {taskid}"')

    _cleanup(spicerack, dryrun, oldpri, newpri, dc, section, admin_reason, taskid, is_active_dc)


def _cleanup(
    spicerack: Spicerack,
    dryrun: bool,
    oldpri: str,
    newpri: str,
    dc: Dc,
    section: str,
    admin_reason: Reason,
    taskid: Optional[str],
    is_active_dc: bool,
) -> None:
    say("Entering cleanup phase")
    dbctl_dryrun = dryrun or section == "test-s4"
    oldpri_mi = _extract_db_instance(spicerack, oldpri, dc)
    newpri_mi = _extract_db_instance(spicerack, newpri, dc)

    # see https://gerrit.wikimedia.org/r/c/operations/puppet/+/1217492
    if ask(f"Clean up heartbeat table(s) on new primary {newpri}"):
        sql = "DELETE FROM heartbeat.heartbeat WHERE file LIKE %s"
        like = f"{oldpri}-bin%"
        if dryrun:
            log.info(f"Dry-running '{sql}' with '{like}'")
        else:
            log.info(f"Running query '{sql}' with '{like}'")
            newpri_mi.execute(sql, (like,))

    if ask(f"Enable and run puppet on old primary {oldpri}"):
        step("run_puppet", "Run puppet on old primary")
        if not dryrun:
            p = puppet(spicerack, oldpri)
            confirm_on_failure(p.run, batch_size=50, enable_reason=admin_reason)

    if ask(f"Enable and run on new primary {newpri}"):
        step("run_puppet", "Run puppet on new primary")
        if not dryrun:
            p = puppet(spicerack, newpri)
            confirm_on_failure(p.run, batch_size=50, enable_reason=admin_reason)

    if ask(f"Run set-master query on {newpri}"):
        sql = _download_events_sql_query("master")
        say("Changing events for query killer")
        if not dryrun:
            newpri_mi.execute(sql)

    if ask(f"Run set-replica query on {oldpri}"):
        sql = _download_events_sql_query("slave")
        say("Changing events for query killer")
        if not dryrun:
            oldpri_mi.execute(sql)

    if is_active_dc:
        say(
            "DIY: Merge the related CR for DNS configuration in Puppet then run"
            " ssh dns1004.wikimedia.org -t sudo authdns-update"
        )
        # TODO check DNS
        ask("Confirm when done")

    else:
        log.info("No DNS change needed.")

    if section != "test-s4" and ask(
        f"Update candidate primary dbctl setting {oldpri} as candidate and {newpri} not candidate"
    ):
        run_dbctl_cmd(dbctl_dryrun, f"instance {oldpri} set-candidate-master --section {section} true")
        run_dbctl_cmd(dbctl_dryrun, f"instance {newpri} set-candidate-master --section {section} false")

    if ask("Update Orchestrator candidate tags"):
        _runcmd(dryrun, f"sudo cumin 'dborch*' 'orchestrator-client -c untag -i {newpri} --tag name=candidate'")
        _runcmd(dryrun, f"sudo cumin 'dborch*' 'orchestrator-client -c tag -i {oldpri} --tag name=candidate'")

    if section != "test-s4" and ask(f"Depool {oldpri}"):
        if taskid:
            args = ["--reason", admin_reason, "-t", taskid, oldpri]
        else:
            args = ["--reason", admin_reason, oldpri]

        depool_dryrun = dryrun or section == "test-s4"
        _run_cookbook(depool_dryrun, "sre.mysql.depool", args)

    say("Completed")


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=["show", "switch", "dryrun"])
    ap.add_argument("section", help="Section")
    ap.add_argument("dc", choices=["codfw", "eqiad"], help="Datacenter")
    ap.add_argument("-t", "--task-id", help="Phabricator task ID (e.g. T12345).")
    # TODO
    # ap.add_argument("--new-pri", help="Set new primary instead of autodetection")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    if args.action == "switch":
        spicerack = Spicerack(dry_run=False)
    else:
        spicerack = Spicerack(dry_run=True)

    _run(spicerack, args.section, args.dc, args.task_id, args.action)


if __name__ == "__main__":
    main()
