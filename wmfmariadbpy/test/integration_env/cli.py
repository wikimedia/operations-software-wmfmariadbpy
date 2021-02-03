import logging
import sys
from typing import Tuple

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
    """Integration testing environment manager"""
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
    """Build docker image"""
    status, output = docker_env.build(pull, no_cache)
    print(status)
    if not verbose:
        return
    for line in output:
        if "stream" in line:
            print(line["stream"], end="")


@cli.command()
def cache() -> None:
    """Maintain cache of database versions"""
    if not dbver.download_all():
        sys.exit(1)


@cli.command()
def dbvers() -> None:
    """List all database versions available"""
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
    """Start docker container"""
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
    """Stop docker container"""
    print(docker_env.stop())


@cli.command()
def status() -> None:
    """Show status of docker container"""
    print(docker_env.status())


@cli.command()
@click.argument("cmd", nargs=1)
@click.argument("args", nargs=-1)
def exec(cmd: str, args: Tuple[str, ...]) -> None:
    """Exececute command inside container"""
    sys.exit(docker_env.exec([cmd] + list(args)))


@cli.command()
@click.option(
    "-t",
    "--type",
    "type_",
    default="single",
    show_default=True,
    help="Type of topology to deploy (single|replication)",
)
@click.option(
    "-p",
    "--port",
    default=common.BASE_PORT,
    show_default=True,
    help="Port/base port for deployment",
)
@click.argument("version", nargs=1)
def deploy(type_: str, version: str, port=int) -> None:
    """Manage deployments within container"""
    d = dbver.get_ver(version)
    if not d:
        common.fatal("Unknown db version '%s'" % version)
    sb_type = type_.lower()
    if sb_type not in common.TOPO_TYPES:
        common.fatal("Unknown topology type '%s'" % sb_type)
    sb_name = d.sandbox_name(sb_type)
    cmd = [
        "dbdeployer",
        "deploy",
        sb_type,
        version,
        "--concurrent",
        "--skip-report-host",
        "--skip-report-port",
        "--my-cnf-options=report_host=localhost",
    ]
    if sb_type == common.TOPO_TYPE_SINGLE:
        cmd.append("--port=%d" % port)
    else:
        cmd.append("--base-port=%d" % port)
    cmd += ["&&", "apply_sys_schema", sb_name]
    sys.exit(docker_env.exec(["bash", "-c", " ".join(cmd)]))
