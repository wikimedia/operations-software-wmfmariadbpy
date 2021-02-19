import logging
import os
import sys
from typing import List, NoReturn, Tuple

import click

from wmfmariadbpy.test.integration_env import common, dbver, docker_env


@click.group()
@click.option(
    "-l",
    "--log-level",
    default="WARNING",
    show_default=True,
    help="Logging level (DEBUG|INFO|WARNING|ERROR|CRITICAL)",
)
def cli(log_level: str) -> None:
    """Integration testing environment manager."""
    logger = common.logger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(log_level.upper())
    f = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(filename)s:%(funcName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch.setFormatter(f)
    logger.addHandler(ch)


@cli.command()
@click.option(
    "--pull",
    is_flag=True,
    help="Always attempt to pull a newer version of the image",
)
@click.option(
    "--no-cache",
    is_flag=True,
    help="Do not use cache when building the image",
)
@click.option(
    "-v", "--verbose", is_flag=True, default=False, help="Display docker build output"
)
def build(pull: bool, no_cache: bool, verbose: bool) -> None:
    """Build docker image."""
    status, output = docker_env.build(pull, no_cache)
    print(status)
    if not verbose:
        return
    for line in output:
        if "stream" in line:
            print(line["stream"], end="")


@cli.command()
@click.option(
    "--skip-csum",
    is_flag=True,
    help="Do not calculate checksums for existing downloads.",
)
def cache(skip_csum: bool) -> None:
    """Maintain cache of database versions."""
    if not dbver.download_all(skip_csum):
        sys.exit(1)


@cli.command()
def dbvers() -> None:
    """List all database versions available."""
    for d in dbver.DB_VERSIONS:
        print(
            "%s %s%s"
            % (d.ver, d.flavor, " DEFAULT" if d.ver == dbver.DEFAULT_VER else "")
        )


@cli.command()
@click.option(
    "--rm/--no-rm",
    default=True,
    show_default=True,
    help="Auto-remove the container after exit",
)
def start(rm: bool) -> None:
    """Start docker container."""
    status = docker_env.status()
    log = common.logger()
    log.debug("Initial container status: %s", status)
    if status == docker_env.STATUS_RUNNING:
        common.fatal("Already running")
    elif status == docker_env.STATUS_EXITED:
        common.fatal("Container exists but is stopped")
    if status not in docker_env.STATUSES:
        common.fatal("Unsupported container status '%s'" % status)
    docker_env.start(rm)
    docker_env.populate_dbvers(dbver.DB_VERSIONS)
    print("Started")


@cli.command()
def stop() -> None:
    """Stop docker container."""
    print(docker_env.stop())


@cli.command()
def status() -> None:
    """Show status of docker container."""
    print(docker_env.status())


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs=-1)
def sandboxes(args: Tuple[str, ...]) -> None:
    """Show existing sandboxes.

    \b
    $ integration-env sandboxes --header
                name                  type       version           port
    ---------------------------- -------------- --------- ----------------------
     msb_10_4_15              :   single         10.4.15   [10300 ]
     rsandbox_10_1_44         :   master-slave   10.1.44   [10114 10115 10116 ]
     rsandbox_10_4_15         :   master-slave   10.4.15   [10111 10112 10113 ]
    """
    sys.exit(docker_env.exec(["dbdeployer", "sandboxes"] + list(args)))


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("sandbox", nargs=1)
@click.argument("cmd", nargs=1)
@click.argument("args", nargs=-1)
def sandexec(sandbox: str, cmd: str, args: Tuple[str, ...]) -> None:
    """Execute command in specified sandbox.

    The command is run from inside the sandbox directory.

    \b
    $ integration-env sandexec msb_10_4_15 ./use -e 'select @@Version'
    +---------------------+
    | @@Version           |
    +---------------------+
    | 10.4.15-MariaDB-log |
    +---------------------+
    """
    workdir = os.path.join(common.SANDBOXES_DIR, sandbox)
    sys.exit(docker_env.exec([cmd] + list(args), workdir=workdir))


@cli.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "-d",
    "--workdir",
    help="Working dir inside container to run command from.",
)
@click.argument("cmd", nargs=1)
@click.argument("args", nargs=-1)
def exec(workdir: str, cmd: str, args: Tuple[str, ...]) -> None:
    """Exececute command inside container."""
    sys.exit(docker_env.exec([cmd] + list(args), workdir=workdir))


@cli.group()
def deploy() -> None:
    """Deploy a database sandbox inside the docker container."""


@deploy.command(context_settings={"ignore_unknown_options": True})
@click.argument("version", default=None, nargs=1, required=False)
@click.argument("args", nargs=-1)
@click.option(
    "-p",
    "--port",
    default=common.BASE_PORT,
    show_default=True,
    help="Port for deployment",
)
def single(version: str, port: int, args: List[str]) -> None:
    """Deploy 'single' sandbox.

    If VERSION is not specified, the default database version is used."""
    sb_type = common.TOPO_TYPE_SINGLE
    try:
        d = dbver.get_ver(version)
    except KeyError:
        common.fatal("Unknown db version '%s'" % version)
    sb_name = d.sandbox_name(sb_type)
    _deploy(
        sb_name,
        sb_type,
        d.ver,
        "--port=%d" % port,
        "--master",
        *args,
    )


@deploy.command(context_settings={"ignore_unknown_options": True})
@click.argument("version", default=None, nargs=1, required=False)
@click.argument("args", nargs=-1)
@click.option(
    "-p",
    "--port",
    default=common.BASE_PORT,
    show_default=True,
    help="Base port for deployment",
)
def replication(version: str, port: int, args: List[str]) -> None:
    """Deploy 'replication' sandbox.

    If VERSION is not specified, the default database version is used."""
    sb_type = common.TOPO_TYPE_REPLICATION
    try:
        d = dbver.get_ver(version)
    except KeyError:
        common.fatal("Unknown db version '%s'" % version)
    sb_name = d.sandbox_name(sb_type)
    _deploy(
        sb_name,
        sb_type,
        d.ver,
        "--base-port=%d" % port,
        "--change-master-options=master_heartbeat_period=1",
        *args,
    )


def _deploy(sb_name: str, sb_type: str, ver: str, *args: str) -> NoReturn:
    cmd = [
        "dbdeployer",
        "deploy",
        sb_type,
        ver,
        "--log-sb-operations",
        "--concurrent",
        "--port-as-server-id",
        "--skip-report-host",
        "--skip-report-port",
        "--enable-general-log",
        "--my-cnf-options=report_host=localhost",
        "--my-cnf-options=slave_net_timeout=2",
        "--my-cnf-options=log_slave_updates=1",
        "--my-cnf-options=transaction_isolation=READ-COMMITTED",
    ]
    cmd += args
    cmd += ["&&", "apply_sys_schema", sb_name]
    sys.exit(docker_env.exec(["bash", "-c", " ".join(cmd)]))


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("sandbox", nargs=1)
@click.argument("args", nargs=-1)
def delete(sandbox: str, args: Tuple[str, ...]) -> None:
    """Delete a sandbox.

    Use "ALL" to delete all sandboxes.

    \b
    $ integration-env delete ALL
    List of deployed sandboxes:
    /root/sandboxes/rsandbox_10_1_44
    /root/sandboxes/rsandbox_10_4_15
    """
    cmd = [
        "dbdeployer",
        "delete",
        "--concurrent",
        "--skip-confirm",
        sandbox,
    ]
    sys.exit(docker_env.exec(cmd + list(args)))
