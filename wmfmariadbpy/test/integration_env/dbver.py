import hashlib
import os
from typing import Tuple

import requests

from wmfmariadbpy.test.integration_env import common

FLAVOR_MYSQL = "mysql"
FLAVOR_PERCONA = "percona"
FLAVOR_MARIADB = "mariadb"
FLAVORS = (FLAVOR_MYSQL, FLAVOR_PERCONA, FLAVOR_MARIADB)


class DBVersion:
    def __init__(self, flavor: str, ver: str, sha256sum: str) -> None:
        self.flavor = flavor
        self.ver = ver
        self.sha256sum = sha256sum

    def url(self) -> str:
        if self.flavor == FLAVOR_MARIADB:
            return "https://downloads.mariadb.org/f/mariadb-{}/bintar-linux-x86_64/{}/from/https://archive.mariadb.org/?serve".format(
                self.ver, self.filename()
            )
        elif self.flavor == FLAVOR_MYSQL:
            raise NotImplementedError
        elif self.flavor == FLAVOR_PERCONA:
            raise NotImplementedError
        raise NotImplementedError("Unsupported flavor %s" % self.flavor)

    def filename(self) -> str:
        if self.flavor == FLAVOR_MARIADB:
            return "mariadb-{ver}-linux-x86_64.tar.gz".format(ver=self.ver)
        elif self.flavor == FLAVOR_MYSQL:
            raise NotImplementedError
        elif self.flavor == FLAVOR_PERCONA:
            raise NotImplementedError
        raise NotImplementedError("Unsupported flavor %s" % self.flavor)

    def checksum(self, path: str) -> Tuple[bool, str]:
        hash = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                data = f.read(1024 * 1024)
                if not data:
                    break
                hash.update(data)
        digest = hash.hexdigest()
        return digest == self.sha256sum, digest


# For mariadb, checksums can be gotten from this page:
# https://downloads.mariadb.org/mariadb/<VERSION>/#bits=64&os_group=linux_generic
DB_VERSIONS = (
    DBVersion(
        FLAVOR_MARIADB,
        "10.1.44",
        "f0000f84721b88e4d49edb649e5e76c0f79501c91d9a9405d41c751a4214c054",
    ),
    DBVersion(
        FLAVOR_MARIADB,
        "10.4.15",
        "2f22e645ca982bb485423606343cb0757f9e393391b2d4a15ba51e8a2b94aeac",
    ),
)


def download_all() -> bool:
    for ver in DB_VERSIONS:
        if not download(ver):
            return False
    return True


def download(dbver: DBVersion) -> bool:
    log = common.prefix_logger("%s: %s" % (dbver.flavor, dbver.ver))
    target = os.path.join(common.cache_dir(), dbver.filename())
    exists = os.path.exists(target)
    if exists:
        log.debug("File exists: %s. Calculating checksum", dbver.filename())
        ok, digest = dbver.checksum(target)
        if ok:
            log.debug("Checksum matches: %s", dbver.sha256sum)
            log.info("OK")
            return True
        log.error("Checksum failed. Expected %s, got %s", dbver.sha256sum, digest)
        log.debug("Removing file")
        os.remove(target)

    log.debug("Downloading")
    req = requests.get(dbver.url())
    assert req.status_code == 200, req.status_code
    with open(target, "wb") as f:
        for chunk in req.iter_content(chunk_size=1024 * 1024):
            n = f.write(chunk)
            assert n == len(chunk), "Expected %d, got %d" % (len(chunk), n)
    log.debug("Downloaded. Calculating checksum")
    ok, digest = dbver.checksum(target)
    if ok:
        log.info("Downloaded OK")
    else:
        log.critical(
            "Checksum failed on downloaded file. Expected %s, got %s",
            dbver.sha256sum,
            digest,
        )
    return ok
