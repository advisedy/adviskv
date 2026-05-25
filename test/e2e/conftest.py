import pytest

from .cluster import (
    AdvisKVCluster,
    build_e2e_targets,
    clean_data_dirs,
    write_e2e_configs,
)


@pytest.fixture(scope="session", autouse=True)
def build_targets():
    build_e2e_targets()


@pytest.fixture()
def cluster():
    clean_data_dirs()
    write_e2e_configs()
    cluster = AdvisKVCluster()
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()
