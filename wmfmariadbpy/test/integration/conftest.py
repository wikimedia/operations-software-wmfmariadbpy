import subprocess

import pytest

from wmfmariadbpy.test.integration_env import common, dbver


@pytest.fixture(scope="module", autouse=True)
def manage_env():
    ret = subprocess.run(
        "integration-env build && integration-env cache --skip-csum && integration-env start",
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
    yield d
    undeploy_all()


@pytest.fixture(scope="class", params=dbver.DB_VERSIONS, ids=lambda d: d.ver)
def deploy_single_all_versions(request):
    deploy_ver(common.TOPO_TYPE_SINGLE, request.param.ver)
    yield request.param
    undeploy_all()


@pytest.fixture(scope="class")
def deploy_replicate():
    d = dbver.get_ver()
    deploy_ver(common.TOPO_TYPE_REPLICATION, d.ver)
    yield d
    undeploy_all()


@pytest.fixture(params=dbver.DB_VERSIONS, ids=lambda d: d.ver)
def deploy_replicate_all_versions(request):
    yield from _deploy_replicate_all_versions(request)


@pytest.fixture(scope="class", params=dbver.DB_VERSIONS, ids=lambda d: d.ver)
def deploy_replicate_all_versions_class(request):
    yield from _deploy_replicate_all_versions(request)


def _deploy_replicate_all_versions(request):
    deploy_ver(common.TOPO_TYPE_REPLICATION, request.param.ver)
    yield request.param
    undeploy_all()


def deploy_ver(sb_type: str, ver: str, port=common.BASE_PORT):
    subprocess.run(
        ["integration-env", "deploy", sb_type, "--port=%d" % port, ver],
        check=True,
    )


def undeploy_all():
    subprocess.run(["integration-env", "delete", "ALL"], check=True)
