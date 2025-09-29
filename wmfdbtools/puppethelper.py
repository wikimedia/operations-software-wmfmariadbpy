#!/usr/bin/env python3
"""
Puppet helper tool
"""

import argparse
import subprocess
import sys
import re
import json
from pathlib import Path
import yaml

import httpx  # debdeps: python3-httpx


def git_commit(file: Path, msg: str) -> None:
    # TODO: multiple files
    fn = file.as_posix()
    subprocess.run(["git", "add", fn], check=True)
    try:
        subprocess.run(["git", "commit", fn, "-m", msg], check=True)
        print(f"Committed")
    except subprocess.CalledProcessError as e:
        print(f"Git failed: {e} - remember cleanup the git repo")
        sys.exit(1)


def extract_dc(hostname: str) -> str:
    # urgh
    match = re.match(r"([a-zA-Z]+)([0-9]+)", hostname)
    assert match
    dc_num = match.group(2)[0]
    assert dc_num in ("1", "2")
    return "eqiad" if dc_num == "1" else "codfw"


def ask_confirmation(q: str) -> bool:
    while True:
        response = input(f"y/n? ").strip().lower()
        if response in ("y", "yes"):
            return True
        if response in ("n", "no"):
            return False
        print("Please answer 'y' or 'n'")


def inject_block(file: Path, content: str, block_id: str) -> None:
    """Inject/update a block of configuration between 2 markers"""
    begin_tok = f"# puppethelper '{block_id}' begin"
    end_tok = f"# puppethelper '{block_id}'"
    pattern = re.compile(rf"{re.escape(begin_tok)}.*?{re.escape(end_tok)}", re.DOTALL)

    old_blob = file.read_text()
    if not pattern.search(old_blob):
        print(f"Error: unable to find markers:\n---\n{begin_tok}\n{end_tok}\n---\nin {file}")
        return

    new_block = f"{begin_tok}\n{content}\n{end_tok}"
    new_blob = pattern.sub(new_block, old_blob)
    file.write_text(new_blob)


def handle_file(file: Path, file_data: dict[str, str], interactive: bool) -> bool:
    """Process a single file according to its metadata."""
    content = file_data.get("content", "")
    mode = file_data.get("mode", "overwrite")
    block_id = file_data.get("block_id")

    if mode == "inject":
        action = f"inject block '{block_id}' into"
    else:
        action = "overwrite"

    exists = file.exists()
    status = "exists" if exists else "new"

    if interactive:
        prompt = f"{action} {file} ({status})?"
        if not ask_confirmation(prompt):
            print(f"Skipped")
            return False

    try:
        if mode == "inject":
            assert block_id
            inject_block(file, content, block_id)
        else:
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(content)

        return True
    except Exception as e:
        print(f"Error processing {file}: {e}", file=sys.stderr)
        return False


def cmd_remove_preseed(args):
    """Remove hostname from preseed.yaml"""
    hostname = args.hostname
    file = Path("modules/profile/data/profile/installserver/preseed.yaml")
    blob = file.read_text()
    blob2 = blob.replace(f"|{hostname}", "")
    blob2 = blob2.replace(f"{hostname}|", "")

    if blob == blob2:
        print(f"Hostname {hostname} not found in {file}")
        sys.exit(1)

    file.write_text(blob2)

    msg = f"preseed.yaml: Remove {hostname} from preseeding"
    if args.t:
        msg += f"\n\nBug: {args.t}"

    git_commit(file, msg)


def cmd_enable_notif(args):
    """Enable notifications for hostname"""
    hostname = args.hostname
    file = Path(f"hieradata/hosts/{hostname}.yaml")
    lines = file.read_text().splitlines()
    blocker = "profile::monitoring::notifications_enabled: false"

    if blocker not in lines:
        print(f"Notifications already enabled in {file}")
        sys.exit(1)

    lines = [x for x in lines if x != blocker]
    file.write_text("\n".join(lines))

    msg = f"{hostname}.yaml: enable notifications"
    if args.t:
        msg += f"\n\nBug: {args.t}"
    git_commit(file, msg)


def cmd_disable_notif(args):
    """Disable notifications for hostname"""
    hostname = args.hostname
    file = Path(f"hieradata/hosts/{hostname}.yaml")
    lines = file.read_text().splitlines()
    blocker = "profile::monitoring::notifications_enabled: false"

    if blocker in lines:
        print("Already disabled?")
        sys.exit(1)

    lines.append(blocker)
    file.write_text("\n".join(lines))

    msg = f"{hostname}.yaml: disable notifications"
    if args.t:
        msg += f"\n\nBug: {args.t}"
    git_commit(file, msg)


def cmd_add_to_dbctl(args):
    """Add hostname to dbctl instances.yaml"""
    hostname = args.hostname
    dc = extract_dc(hostname)
    file = Path("conftool-data/dbconfig-instance/instances.yaml")

    with file.open() as f:
        y = yaml.safe_load(f)

    assert hostname not in y[dc], "Already in dbctl"

    y[dc].append(hostname)
    y[dc].sort()

    with file.open("w") as f:
        for key, hosts in y.items():
            f.write(f"{key}:\n")
            for host in hosts:
                f.write(f"  - {host}\n")

    msg = f"instances.yaml: add {hostname} to dbctl"
    if args.t:
        msg += f"\n\nBug: {args.t}"

    git_commit(file, msg)


def cmd_fetch_from_zarcillo(args):
    """Fetch all Puppet files from Zarcillo's API and update local files"""
    # zarc_uri = "https://zarcillo.wikimedia.org/api/v1/update_puppet"
    zarc_uri = "http://localhost:8080/api/v1/update_puppet"
    resp = httpx.get(zarc_uri, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    files = data["files"]

    for rel_path, file_data in files.items():
        handle_file(Path(rel_path), file_data, not args.noask)

    # capture
    # for rel_path, file_data in list(files.items()):
    #     b = Path(rel_path).read_text()
    #     files[rel_path]["content"] = b
    # Path("out.json").write_text(json.dumps(files, indent=2, sort_keys=True))

    if args.commit:
        for fn in files:
            subprocess.run(["git", "add", fn], check=True)

        fns = ",".join(files)
        msg = f"{fns}: Update"
        if args.t:
            msg += f"\n\nBug: {args.t}"

        try:
            subprocess.run(["git", "commit", "-m", msg], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Git failed: {e} - remember cleanup the git repo")
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Puppet automation tool", formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    for action in ["remove-preseed", "enable-notif", "disable-notif", "add-to-dbctl"]:
        p = subparsers.add_parser(action, help=f"{action} for hostname")
        p.add_argument("hostname", help="Hostname")
        p.add_argument("-t", help="Task ID")

    p = subparsers.add_parser("fetch-from-zarcillo", help="Fetch Puppet files from Zarcillo API and update locally")
    p.add_argument("--commit", action="store_true", help="Commit all changes to git")
    p.add_argument("--noask", action="store_true", help="")
    p.add_argument("-t", help="Task ID")

    args = parser.parse_args()

    if not Path("Puppetfile.core").exists():
        print("Puppetfile.core not found, please run the script from the puppet repo")
        sys.exit(1)

    if args.command == "remove-preseed":
        cmd_remove_preseed(args)
    elif args.command == "enable-notif":
        cmd_enable_notif(args)
    elif args.command == "disable-notif":
        cmd_disable_notif(args)
    elif args.command == "add-to-dbctl":
        cmd_add_to_dbctl(args)
    elif args.command == "fetch-from-zarcillo":
        cmd_fetch_from_zarcillo(args)


if __name__ == "__main__":
    main()
