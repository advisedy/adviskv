import pytest

from .cluster import (
    AdvisKVCluster,
    build_e2e_targets,
    clean_data_dirs,
    write_e2e_configs,
)


def _service_envs_from_request(request):
    """Return optional per-service env overrides for indirectly parametrized tests."""
    return getattr(request, "param", None)


@pytest.fixture(scope="session", autouse=True)
def build_targets():
    """Build e2e binaries once before the test session starts."""
    build_e2e_targets()


@pytest.fixture()
def cluster(request):
    """Start a fresh local AdvisKV cluster for one e2e test."""
    clean_data_dirs()
    write_e2e_configs()

    service_envs = _service_envs_from_request(request)
    test_cluster = AdvisKVCluster(service_envs=service_envs)
    try:
        test_cluster.start()
        yield test_cluster
    finally:
        test_cluster.stop()
