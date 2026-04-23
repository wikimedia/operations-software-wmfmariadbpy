#!/usr/bin/env python3
"""
Zarcillo API client

It can be used as a library and CLI tool
"""
# Released under GPLv3 Copyright 2025-2026 Wikimedia Foundation, Federico Ceratto <fceratto@wikimedia.org>

import getpass
import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from typing import Optional, Literal

import httpx
from pydantic import BaseModel


log = logging.getLogger()

version = 4

_BASEURL = "https://zarcillo.wikimedia.org"
_BASE_DELAY = 1  # seconds
_MAX_DELAY = 60  # seconds


# # Internals # #


def _get_unix_login() -> str:
    try:
        return os.getlogin()
    except OSError:
        return getpass.getuser()


class Lock(BaseModel):
    uuid: str


# # Public functions # #


def httpclient(retries=10, timeout=15, conn_timeout=5) -> httpx.Client:
    transport = httpx.HTTPTransport(retries=retries)
    timeout = httpx.Timeout(timeout, connect=conn_timeout)
    headers = {"X-WMF-Username": _get_unix_login()}
    return httpx.Client(transport=transport, timeout=timeout, headers=headers)


def acquire_lock(
    desc: str,
    section_names: list[str] = [],
    section_at_dc_names: list[str] = [],
    hostnames: list[str] = [],
    instance_names: list[str] = [],
    prio: int = 2,
    baseurl: str = _BASEURL,
) -> Lock:
    url = f"{baseurl}/api/v1/acquire_lock"

    payload = dict(
        section_names=section_names,
        section_at_dc_names=section_at_dc_names,
        hostnames=hostnames,
        desc=desc,
        instance_names=instance_names,
        prio=prio,
    )

    client = httpclient(retries=1000)
    resp = client.post(url, json=payload)
    resp.raise_for_status()
    j = resp.json()
    if "uuid" in j:
        return Lock(uuid=j["uuid"])

    raise Exception(f"Unable to get lock {j}")


def release_lock(uuid: str, baseurl: str = _BASEURL) -> None:
    client = httpclient()
    url = f"{baseurl}/api/v1/release_lock/{uuid}"
    log.debug(f"Releasing lock by calling {url}")
    resp = client.post(url)
    resp.raise_for_status()
    log.debug("Lock released")


def show_locks(baseurl: str = _BASEURL) -> None:
    client = httpclient()
    url = f"{baseurl}/api/v1/show_locks"
    resp = client.get(url)
    resp.raise_for_status()
    locks = resp.json()["locks"]
    ks = ["locked_at", "expires_at", "instance", "locked_by", "prio", "uuid", "description"]
    fmt = "%-19s %-19s %-18s %-12s %-4s %-36s %s"
    print()
    print(fmt % tuple(ks))
    for lo in locks:
        row = [lo[k] for k in ks]
        print(fmt % tuple(row))

    print()


class MDBInstance(BaseModel):
    section: Optional[str]
    hostname: str
    role: Optional[str]
    dc: Optional[str]


def fetch_instances_tmp(baseurl: str = _BASEURL) -> list[MDBInstance]:
    client = httpclient()
    # Not a stable API yet
    url = f"{baseurl}/api/v1/db_instances"
    resp = client.get(url)
    resp.raise_for_status()
    li = resp.json()["db_hosts"]
    return [MDBInstance(**d) for d in li]


# # Schema change API


def update_schema_change_status(
    path: str, section: str, instances: list[str], status: str, baseurl: str = _BASEURL
) -> None:
    """Update schema change status"""
    client = httpclient()
    assert section
    assert "/" in path and path.startswith("20")

    url = f"{baseurl}/api/v0/update_schema_change_status"
    payload = dict(instances=instances, section_name=section, schema_change_path=path, status=status)

    resp = client.post(url, json=payload)
    resp.raise_for_status()


class SchemaChangeStatus(BaseModel):
    # See zarcillo
    instance: str
    hostname: str
    section: str
    schema_change_path: str
    author: str
    status: Literal["ongoing", "done", "failed", "wait"]
    role: str


def fetch_schema_change_status(path: str, baseurl: str = _BASEURL) -> list[SchemaChangeStatus]:
    """Get schema change status for a specific path"""
    client = httpclient()
    assert path and "/" in path and path.startswith("20")

    url = f"{baseurl}/api/v0/schema_change_status"
    params = {"path": path}
    resp = client.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return [SchemaChangeStatus(**i) for i in data if i is not None]


# # CLI # #


def _parse_args() -> Namespace:
    ap = ArgumentParser(description="Zarcillo API tool")
    ap.add_argument("--baseurl", default=_BASEURL, help="Zarcillo API url")
    subparsers = ap.add_subparsers(dest="action", required=True)

    # Acquire lock
    ac = subparsers.add_parser("lock", help="Acquire a lock")
    ac.add_argument("--section-names", nargs="*", default=[], help="List of section names")
    ac.add_argument("--section-at-dc-names", nargs="*", default=[], help="List of section@dc names e.g. s3@codfw")
    ac.add_argument("--hostnames", nargs="*", default=[], help="List of hostnames")
    ac.add_argument("--instance-names", nargs="*", default=[], help="List of instance names")
    ac.add_argument("--desc", default="", help="Description")
    ac.add_argument("--prio", type=int, default=2, help="Priority (default=2)")

    # Release lock
    release = subparsers.add_parser("unlock", help="Release a lock")
    release.add_argument("uuid", help="UUID of the lock")

    # Show locks
    subparsers.add_parser("show-locks", help="Display locks")

    args = ap.parse_args()
    return args


def _run_cli() -> None:
    """CLI"""
    args = _parse_args()

    if args.action == "lock":
        lock = acquire_lock(
            section_names=args.section_names,
            section_at_dc_names=args.section_at_dc_names,
            hostnames=args.hostnames,
            instance_names=args.instance_names,
            baseurl=args.baseurl,
            desc=args.desc,
            prio=args.prio,
        )
        print(f"Lock acquired. UUID: {lock.uuid}")

    elif args.action == "unlock":
        release_lock(args.uuid, baseurl=args.baseurl)
        print("Lock released.")

    elif args.action == "show-locks":
        show_locks(baseurl=args.baseurl)


if __name__ == "__main__":
    try:
        _run_cli()
    except Exception as e:
        print(f"Failed: {e}", file=sys.stderr)
        sys.exit(1)
