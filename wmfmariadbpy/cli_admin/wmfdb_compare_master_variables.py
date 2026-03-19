#!/usr/bin/env python3
"""
Compare MariaDB variables across all sections primary master <--> DC master
Used before DC active<->standby switchovers
"""

import logging
from urllib.request import urlopen
from wmfmariadbpy.WMFMariaDB import WMFMariaDB
import json
import time

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

VAR_NAMES = [
    "version",
    "binlog_format",
    "log_bin",
    "expire_logs_days",
    "binlog_expire_logs_seconds",
    "gtid_strict_mode",
    # "gtid_binlog_pos",
    # "gtid_current_pos",
    "log_slave_updates",
    "sync_binlog",
    "innodb_flush_log_at_trx_commit",
    "read_only",
    "super_read_only",
    "relay_log_purge",
    "binlog_row_image",
    "innodb_buffer_pool_size",
    "max_connections",
]


def fetch_url(url: str) -> bytes:
    with urlopen(url) as resp:
        return resp.read()


def fetch_sections() -> list[dict]:
    data = fetch_url("https://zarcillo.wikimedia.org/api/v0/sections")
    parsed = json.loads(data)
    return parsed


def find_masters(section: dict) -> list[str]:
    # find both primary and DC master
    masters = []
    if not section["roots"]:
        return []
    rootd = section["roots"][0]
    masters = [
        rootd["name"],
    ]
    for c in rootd["children"]:
        if c["children"]:
            masters.append(c["name"])

    return masters


def fetch_variables(hn: str) -> dict[str, str]:
    vars_in = ", ".join(f"'{v}'" for v in VAR_NAMES)
    sql = f"SELECT variable_name, variable_value FROM information_schema.global_variables WHERE variable_name IN ({vars_in})"
    log.debug(f"connecting to {hn}")
    db = WMFMariaDB(hn)
    res = db.execute(sql)
    db.disconnect()
    if not res or not res.get("rows"):
        return {}
    return {row[0].lower(): row[1] for row in res["rows"]}


def compare_vars(masters: list[str]) -> None:
    vars_by_host = [fetch_variables(hn) for hn in masters]

    all_vars = sorted({v for vmap in vars_by_host for v in vmap})
    for var in all_vars:
        values = [vmap.get(var, "<missing>") for vmap in vars_by_host]
        if len(set(values)) > 1:
            for hn, val in zip(masters, values):
                log.info(f"{hn}: {var} = {val}")
            log.info("-" * 40)


def main() -> None:
    for section in fetch_sections():
        s = section["name"]
        log.info(f"--- {s} ---")
        masters = find_masters(section)
        if len(masters) != 2:
            log.warning(f"Skipping {s}: masters: {masters}")
            continue

        compare_vars(masters)
        log.debug("Sleeping")
        time.sleep(60)


if __name__ == "__main__":
    main()
