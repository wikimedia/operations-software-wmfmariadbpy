try:
    from pkg_resources import (
        DistributionNotFound as MissingError,
        get_distribution as version,
    )
except ImportError:
    from importlib.metadata import (
        PackageNotFoundError as MissingError,
        version,
    )

try:
    if version.__name__ == "get_distribution":
        __version__ = version(__name__).version  # Must be the same used as 'name' in setup.py
    else:
        __version__ = version(__name__)
    """:py:class:`str`: the version of the current wmfmariadbpy module."""
except MissingError:  # pragma: no cover - this should never happen during tests
    pass  # package is not installed
