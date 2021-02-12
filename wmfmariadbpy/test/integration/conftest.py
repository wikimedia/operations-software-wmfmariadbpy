import subprocess

import pytest

from wmfmariadbpy.test.integration_env import common, dbver


@pytest.fixture(scope="module", autouse=True)
def manage_env():
    ret = subprocess.run(
        "integration-env build && integration-env start",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if ret.returncode != 0:
        pytest.exit(
            "integration env setup failed: \n\n%s\n." % ret.stdout.decode("utf8"),
            returncode=ret.returncode,
        )
    yield
    ret = subprocess.run(
        ["integration-env", "stop"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    if ret.returncode != 0:
        pytest.exit(
            "integration env teardown failed: \n\n%s\n." % ret.stdout.decode("utf8"),
            returncode=ret.returncode,
        )


@pytest.fixture(scope="class")
def deploy_single():
    d = dbver.get_ver()
    deploy_ver(common.TOPO_TYPE_SINGLE, d.ver)
    yield d.ver
    undeploy_all()


@pytest.fixture(scope="class", params=dbver.DB_VERSIONS, ids=lambda d: d.ver)
def deploy_single_all_versions(request):
    deploy_ver(common.TOPO_TYPE_SINGLE, request.param.ver)
    yield request.param.ver
    undeploy_all()


@pytest.fixture(scope="class")
def deploy_replicate():
    d = dbver.get_ver()
    deploy_ver(common.TOPO_TYPE_REPLICATION, d.ver)
    yield
    undeploy_all()


@pytest.fixture(scope="class", params=dbver.DB_VERSIONS, ids=lambda d: d.ver)
def deploy_replicate_all_versions(request):
    deploy_ver(common.TOPO_TYPE_REPLICATION, request.param.ver)
    yield request.param.ver
    undeploy_all()


def deploy_ver(sb_type: str, ver: str, port=common.BASE_PORT):
    subprocess.run(
        ["integration-env", "deploy", "--type=%s" % sb_type, "--port=%d" % port, ver],
        check=True,
    )


def undeploy_all():
    subprocess.run(
        [
            "integration-env",
            "exec",
            "--",
            "dbdeployer",
            "destroy",
            "ALL",
            "--skip-confirm",
        ],
        check=True,
    )
