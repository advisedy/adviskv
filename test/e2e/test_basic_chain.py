import re

from .cluster import run_e2e_client


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def test_meta_to_storage_basic_kv_chain(cluster):
    result = run_e2e_client(
        "--db=pytest_e2e_db",
        "--table=pytest_e2e_table",
        "--key_count=16",
    )

    assert result.returncode == 0, (
        result.stdout + "\n" + cluster.logs_summary()
    )
    output = strip_ansi(result.stdout)
    assert "[ PASS ] create table" in output
    assert "[ PASS ] table normal" in output
    assert "[ PASS ] sdk overwrite first key" in output
    assert "[ PASS ] sdk delete last key" in output
    assert "PASS e2e smoke test passed" in output
