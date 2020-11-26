import io
import os
import tarfile
from typing import Dict, Iterable, List, Optional, Tuple, Union

import docker

from wmfmariadbpy.test.integration_env import common, dbver

IMAGE = "wmfmariadbpy-integration"
CONTAINER = IMAGE
VOLUME = "wmfmariadbpy-vol"

STATUS_RUNNING = "running"
STATUS_EXITED = "exited"
STATUS_NONEXISTANT = "nonexistant"
STATUSES = (STATUS_RUNNING, STATUS_EXITED, STATUS_NONEXISTANT)

SANDBOX_MNT = "/root/opt/mysql"
CTR_CACHE_MNT = "/cache"


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


def build(pull: bool, no_cache: bool) -> Iterable[Dict[str, str]]:
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
            log.debug("Image already up-to-date")
        else:
            log.debug("Image updated")
    else:
        log.debug("Image built")
    return output


def run(rm: bool) -> None:
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
        cap_drop="ALL",
        name=CONTAINER,
        volumes={
            common.cache_dir(): {"bind": CTR_CACHE_MNT, "mode": "ro"},
            VOLUME: {"bind": SANDBOX_MNT, "mode": "rw"},
        },
    )


def stop() -> None:
    c = _get_client()
    log = common.logger()
    ctr = _get_ctr(c)
    if not ctr:
        common.fatal("Container not found")
    if ctr.status == "exited":
        log.info("Already stopped")
        return
    log.debug("Stopping")
    ctr.stop()
    log.info("Stopped")


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
    retcode, output = _run_cmd(ctr, "stat -c %%F %s" % os.path.join(SANDBOX_MNT, d.ver))
    log.debug("Stat sandbox dir [%d]: %s", retcode, output)
    if retcode == 0:
        log.debug("Dir already exists in sandbox, skipping")
        return
    log.info("Version not already present in sandbox, unpacking from cache")
    print("Unpacking %s" % d.ver)
    retcode, output = _run_cmd(
        ctr,
        "dbdeployer unpack --verbosity 0 %s"
        % os.path.join(CTR_CACHE_MNT, d.filename()),
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
