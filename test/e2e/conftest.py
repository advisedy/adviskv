import pytest

from .cluster import (
    AdvisKVCluster,
    STORAGE_COUNT,
    build_e2e_targets,
    clean_data_dirs,
    write_e2e_configs,
)


def _cluster_options_from_request(request):
    """Return optional e2e cluster overrides for indirectly parametrized tests."""
    param = getattr(request, "param", None)
    if param is None:
        return STORAGE_COUNT, None
    if "storage_count" in param or "service_envs" in param:
        return param.get("storage_count", STORAGE_COUNT), param.get("service_envs")
    return STORAGE_COUNT, param


@pytest.fixture(scope="session", autouse=True)
def build_targets():
    """Build e2e binaries once before the test session starts."""
    build_e2e_targets()


@pytest.fixture()
def cluster(request):
    """Start a fresh local AdvisKV cluster for one e2e test."""
    clean_data_dirs()
    storage_count, service_envs = _cluster_options_from_request(request)
    write_e2e_configs(storage_count=storage_count)

    test_cluster = AdvisKVCluster(storage_count=storage_count,
                                  service_envs=service_envs)
    try:
        test_cluster.start()
        yield test_cluster
    finally:
        test_cluster.stop()
