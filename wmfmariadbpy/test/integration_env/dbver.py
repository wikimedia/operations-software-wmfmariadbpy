from typing import Optional, Tuple

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
        return common.checksum(path, self.sha256sum)

    def sandbox_name(self, sbtype: str) -> str:
        ver = self.ver.replace(".", "_")
        if sbtype == "single":
            return "msb_%s" % ver
        return "rsandbox_%s" % ver


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
DEFAULT_VER = "10.4.15"


def download_all() -> bool:
    for ver in DB_VERSIONS:
        if not download(ver):
            return False
    return True


def download(dbver: DBVersion) -> bool:
    return common.download_cache(
        common.prefix_logger("%s: %s" % (dbver.flavor, dbver.ver)),
        dbver.url(),
        dbver.filename(),
        dbver.sha256sum,
    )


def get_ver(version: Optional[str] = None) -> DBVersion:
    if not version:
        version = DEFAULT_VER
    for db in DB_VERSIONS:
        if db.ver == version:
            return db
    raise KeyError(version)
