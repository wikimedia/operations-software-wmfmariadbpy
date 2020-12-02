import hashlib
import logging
import os
import sys
from typing import NoReturn, Tuple

import requests

TOPO_TYPE_SINGLE = "single"
TOPO_TYPE_REPLICATION = "replication"
TOPO_TYPES = [TOPO_TYPE_SINGLE, TOPO_TYPE_REPLICATION]

BASE_PORT = 10110


class LogPrefixAdaptor(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "[%s] %s" % (self.extra["prefix"], msg), kwargs


def logger() -> logging.Logger:
    return logging.getLogger(__package__)


def prefix_logger(prefix) -> LogPrefixAdaptor:
    return LogPrefixAdaptor(logger(), {"prefix": prefix})


def fatal(msg: str) -> NoReturn:
    print("FATAL: %s" % msg, file=sys.stderr)
    sys.exit(1)


def env_dir() -> str:
    return os.path.dirname(os.path.realpath(__file__))


def cache_dir() -> str:
    return os.path.join(env_dir(), "cache")


def checksum(path: str, expected: str) -> Tuple[bool, str]:
    hash = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(1024 * 1024)
            if not data:
                break
            hash.update(data)
    digest = hash.hexdigest()
    return digest == expected, digest


def download_cache(log: LogPrefixAdaptor, url: str, filename: str, csum: str) -> bool:
    target = os.path.join(cache_dir(), filename)
    exists = os.path.exists(target)
    if exists:
        log.debug("File exists: %s. Calculating checksum", filename)
        ok, digest = checksum(target, csum)
        if ok:
            log.debug("Checksum matches: %s", csum)
            log.info("OK")
            return True
        log.error("Checksum failed. Expected %s, got %s", csum, digest)
        log.debug("Removing file")
        os.remove(target)

    log.debug("Downloading")
    req = requests.get(url)
    assert req.status_code == 200, req.status_code
    with open(target, "wb") as f:
        for chunk in req.iter_content(chunk_size=1024 * 1024):
            n = f.write(chunk)
            assert n == len(chunk), "Expected %d, got %d" % (len(chunk), n)
    log.debug("Downloaded. Calculating checksum")
    ok, digest = checksum(target, csum)
    if ok:
        log.info("Downloaded OK")
    else:
        log.critical(
            "Checksum failed on downloaded file. Expected %s, got %s",
            csum,
            digest,
        )
    return ok
