import logging
import os
import sys
from typing import NoReturn


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
