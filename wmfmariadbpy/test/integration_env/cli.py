import logging
import sys

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
    output = docker_env.build(pull, no_cache)
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
@click.option(
    "--rm/--no-rm",
    default=True,
    show_default=True,
    help="Auto-remove the container after exit",
)
def run(rm: bool) -> None:
    """Run docker container"""
    status = docker_env.status()
    log = common.logger()
    log.debug("Initial container status: %s", status)
    if status == docker_env.STATUS_RUNNING:
        print("Already running")
        return
    elif status == docker_env.STATUS_EXITED:
        common.fatal("Container exists but is stopped")
    if status not in docker_env.STATUSES:
        common.fatal("Unsupported container status '%s'" % status)
    docker_env.run(rm)
    docker_env.populate_dbvers(dbver.DB_VERSIONS)
    print("Started")


@cli.command()
def stop() -> None:
    """Stop docker container"""
    docker_env.stop()


@cli.command()
def status() -> None:
    """Show status of docker container"""
    print(docker_env.status())
