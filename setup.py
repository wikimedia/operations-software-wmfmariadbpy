"""wmfmariadbpy."""
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="wmfmariadbpy",
    description="wmfmariadbpy",
    version="0.7.1",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://phabricator.wikimedia.org/diffusion/OSMD/",
    packages=("wmfmariadbpy", "wmfmariadbpy.RemoteExecution"),
    install_requires=["pymysql>=0.9.3", "tabulate>=0.8.2"],
    extras_require={
        "cumin": [
            "cumin <= 4.0.0; python_version < '3.6'",
            "cumin; python_version >= '3.6'",
        ]
    },
    entry_points={
        "console_scripts": [
            # cli_admin
            "db-compare = wmfmariadbpy.cli_admin.compare:main",
            "db-move-replica = wmfmariadbpy.cli_admin.move_replica:main",
            "db-osc-host = wmfmariadbpy.cli_admin.osc_host:main",
            "db-replication-tree = wmfmariadbpy.cli_admin.replication_tree:main",
            "db-stop-in-sync = wmfmariadbpy.cli_admin.stop_in_sync:main",
            "db-switchover = wmfmariadbpy.cli_admin.switchover:main",
            "mysql.py = wmfmariadbpy.cli_admin.mysql:main",
            # cli_common
            "db-check-health = wmfmariadbpy.cli_common.check_health:main",
            # integration testing
            "integration-env = wmfmariadbpy.test.integration_env.cli:cli",
        ]
    },
    test_suite="wmfmariadbpy.test",
)
