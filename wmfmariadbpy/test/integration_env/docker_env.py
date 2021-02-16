import io
import os
import sys
import tarfile
from typing import Dict, Iterable, List, Optional, Tuple, Union

import docker
import dockerpty

from wmfmariadbpy.test.integration_env import common, dbver

IMAGE = "wmfmariadbpy-integration"
CONTAINER = IMAGE
VOLUME = "wmfmariadbpy-vol"

STATUS_RUNNING = "running"
STATUS_EXITED = "exited"
STATUS_NONEXISTANT = "nonexistant"
STATUSES = (STATUS_RUNNING, STATUS_EXITED, STATUS_NONEXISTANT)

MYSQL_BIN_MNT = "/root/opt/mysql"
CACHE_MNT = "/cache"


def _get_client() -> docker.DockerClient:
    return docker.from_env()


def _get_ctr(c: docker.DockerClient) -> Optional[docker.models.containers.Container]:
    """Get the docker container.

    Returns:
        Container if container exists, otherwise None
    """
    try:
        return c.containers.get(CONTAINER)
    except docker.errors.NotFound:
        return None


def _get_img(c: docker.DockerClient) -> Optional[docker.models.images.Image]:
    """Get the docker image

    Returns:
        Image if image exists, otherwise None
    """
    try:
        return c.images.get(IMAGE)
    except docker.errors.ImageNotFound:
        return None


def build(pull: bool, no_cache: bool) -> Tuple[str, Iterable[Dict[str, str]]]:
    """Build docker image"""
    c = _get_client()
    log = common.logger()
    env_dir = common.env_dir()
    old_img = _get_img(c)
    log.debug("Starting")

    # Create an in-memory build context for docker, to work around it being less
    # flexibile than `docker build`: https://github.com/docker/docker-py/issues/2105
    build_ctx = io.BytesIO()
    build_tar = tarfile.TarFile(fileobj=build_ctx, mode="w")
    build_tar.add(os.path.join(env_dir, "Dockerfile"), arcname="Dockerfile")
    build_tar.add(os.path.join(env_dir, "contents"), arcname="contents")
    build_tar.close()
    build_ctx.seek(0)

    img, output = c.images.build(
        fileobj=build_ctx,
        custom_context=True,
        tag=IMAGE,
        pull=pull,
        nocache=no_cache,
        rm=True,
    )
    if old_img:
        if old_img.id == img.id:
            status = "Image already up-to-date"
        else:
            status = "Image updated"
    else:
        status = "Image built"
    log.debug(status)
    return status, output


def start(rm: bool) -> None:
    """Run docker container"""
    c = _get_client()
    try:
        img = c.images.get(IMAGE)
    except docker.errors.ImageNotFound:
        common.fatal("Docker image '%s' not found" % IMAGE)
    c.containers.run(
        img,
        # Allow container to run in background: https://stackoverflow.com/a/36872226/2104168
        tty=True,
        detach=True,
        auto_remove=rm,
        name=CONTAINER,
        volumes={
            common.cache_dir(): {"bind": CACHE_MNT, "mode": "ro"},
            VOLUME: {"bind": MYSQL_BIN_MNT, "mode": "rw"},
        },
        tmpfs={
            "/root/sandboxes": "exec",
        },
        network_mode="host",
    )


def stop() -> str:
    c = _get_client()
    log = common.logger()
    ctr = _get_ctr(c)
    if not ctr:
        common.fatal("Container not found")
    if ctr.status == "exited":
        status = "Already stopped"
    else:
        log.debug("Stopping")
        ctr.stop()
        status = "Stopped"
    log.info(status)
    return status


def status() -> str:
    c = _get_client()
    ctr = _get_ctr(c)
    if ctr:
        return ctr.status
    return "nonexistant"


def populate_dbvers(dbvers: Tuple[dbver.DBVersion, ...]) -> None:
    """Expand db tarballs into container"""
    c = _get_client()
    ctr = _get_ctr(c)
    assert ctr
    for d in dbvers:
        populate_dbver(ctr, d)


def populate_dbver(
    ctr: docker.models.containers.Container,
    d: dbver.DBVersion,
) -> None:
    log = common.prefix_logger("%s: %s" % (d.flavor, d.ver))
    retcode, output = _run_cmd(
        ctr, "stat -c %%F %s" % os.path.join(MYSQL_BIN_MNT, d.ver)
    )
    log.debug("Stat sandbox dir [%d]: %s", retcode, output)
    if retcode == 0:
        log.debug("Dir already exists in sandbox, skipping")
        return
    log.info("Version not already present in sandbox, unpacking from cache")
    print("Unpacking %s" % d.ver)
    retcode, output = _run_cmd(
        ctr,
        "dbdeployer unpack --verbosity 0 %s" % os.path.join(CACHE_MNT, d.filename()),
    )
    if retcode != 0:
        common.fatal("Unpacking failed [%d]: %s" % (retcode, output))
    else:
        log.debug("Unpacking [%d]: %r", retcode, output)


def _run_cmd(
    ctr: docker.models.containers.Container,
    cmd: Union[str, List[str]],
) -> Tuple[int, str]:
    retcode, output = ctr.exec_run(cmd)
    return retcode, output.decode("utf8").strip()


def exec(cmd: List[str], workdir: str = "") -> int:
    """Run a user-supplied command inside the container"""
    c = _get_client()
    ctr = _get_ctr(c)
    if not ctr:
        common.fatal("Container not running")
    log = common.logger()
    log.debug("Running: %r", cmd)
    # Workaround docker-py ignoring $TERM
    env = {"TERM": os.getenv("TERM", "xterm")}
    tty = sys.stdin.isatty()

    # Based on docker-compose's exec_command implementation.
    exec_id = c.api.exec_create(
        ctr.id,
        cmd,
        stdin=True,
        tty=tty,
        environment=env,
        workdir=workdir,
    )
    op = dockerpty.ExecOperation(c.api, exec_id, interactive=tty)
    pty = dockerpty.PseudoTerminal(c.api, op)
    pty.start()
    ret = c.api.exec_inspect(exec_id).get("ExitCode")
    log.debug("Exited with %d", ret)
    return ret
