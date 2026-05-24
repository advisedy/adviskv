import pytest

from .cluster import (
    AdvisKVCluster,
    build_e2e_targets,
    clean_data_dirs,
    stop_existing_cluster,
)


@pytest.fixture(scope="session", autouse=True)
def build_targets():
    build_e2e_targets()


@pytest.fixture()
def cluster():
    stop_existing_cluster()
    clean_data_dirs()
    cluster = AdvisKVCluster()
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.stop()
