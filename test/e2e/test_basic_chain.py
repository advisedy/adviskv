from .cluster import run_e2e_client


def test_meta_to_storage_basic_kv_chain(cluster):
    result = run_e2e_client(
        "--db=pytest_e2e_db",
        "--table=pytest_e2e_table",
        "--key_count=16",
    )

    assert result.returncode == 0, (
        result.stdout + "\n" + cluster.logs_summary()
    )
    assert "PASS create table" in result.stdout
    assert "PASS route ready" in result.stdout
    assert "PASS sdk overwrite first key" in result.stdout
    assert "PASS sdk delete last key" in result.stdout
    assert "PASS e2e smoke test passed" in result.stdout
