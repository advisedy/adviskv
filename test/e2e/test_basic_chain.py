import re
import time

from .cluster import run_e2e_client


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def assert_e2e_passed(result, cluster):
    assert result.returncode == 0, result.stdout + "\n" + cluster.logs_summary()
    output = strip_ansi(result.stdout)
    assert "[ FAIL ]" not in output, output
    return output


def parse_route_leader(output: str) -> tuple[str, int]:
    match = re.search(r"\[ ROUTE_LEADER \] ([^:]+):(\d+)", output)
    assert match is not None, output
    return match.group(1), int(match.group(2))


def parse_route_replicas(output: str) -> list[tuple[str, int, str]]:
    clean = strip_ansi(output)
    matches = re.findall(r"\[ ROUTE_REPLICA \] ([^:]+):(\d+) role=(\w+)", clean)
    assert matches, output
    return [(host, int(port), role) for host, port, role in matches]


def replica_services(cluster, output: str) -> tuple[str, list[str]]:
    _, leader_port = parse_route_leader(strip_ansi(output))
    leader = cluster.service_name_for_port(leader_port)
    followers = [
        cluster.service_name_for_port(port)
        for _, port, role in parse_route_replicas(output)
        if role == "FOLLOWER"
    ]
    assert len(followers) >= 2, output
    return leader, followers


def test_meta_to_storage_basic_kv_chain(cluster):
    result = run_e2e_client(
        "--db=pytest_e2e_db",
        "--table=pytest_e2e_table",
        "--key_count=16",
    )

    assert_e2e_passed(result, cluster)


def test_restart_persistence_case(cluster):
    seed = run_e2e_client(
        "--case=restart_persistence_seed",
        "--db=pytest_restart_db",
        "--table=pytest_restart_table",
        "--key_count=16",
    )
    assert_e2e_passed(seed, cluster)

    cluster.restart()

    verify = run_e2e_client(
        "--case=restart_persistence_verify",
        "--db=pytest_restart_db",
        "--table=pytest_restart_table",
        "--key_count=16",
    )
    assert_e2e_passed(verify, cluster)


def test_leader_failover_case(cluster):
    seed = run_e2e_client(
        "--case=leader_failover_seed",
        "--db=pytest_leader_failover_db",
        "--table=pytest_leader_failover_table",
        "--key_count=16",
    )
    seed_output = assert_e2e_passed(seed, cluster)
    _, leader_port = parse_route_leader(seed_output)

    cluster.stop_service(cluster.service_name_for_port(leader_port))

    verify = run_e2e_client(
        "--case=leader_failover_verify",
        "--db=pytest_leader_failover_db",
        "--table=pytest_leader_failover_table",
        "--key_count=16",
        "--timeout_ms=90000",
    )
    assert_e2e_passed(verify, cluster)


def test_follower_log_catchup_case(cluster):
    seed = run_e2e_client(
        "--case=follower_log_catchup_prepare",
        "--db=pytest_follower_log_catchup_db",
        "--table=pytest_follower_log_catchup_table",
        "--key_count=16",
    )
    seed_output = assert_e2e_passed(seed, cluster)
    _, followers = replica_services(cluster, seed_output)
    catching_up_follower = followers[0]

    cluster.stop_service(catching_up_follower)
    catching_up_stopped = True
    try:
        gap = run_e2e_client(
            "--case=follower_log_catchup_write_gap",
            "--db=pytest_follower_log_catchup_db",
            "--table=pytest_follower_log_catchup_table",
            "--key_count=16",
        )
        assert_e2e_passed(gap, cluster)

        cluster.start_service(catching_up_follower)
        catching_up_stopped = False
        time.sleep(3)
        verify = run_e2e_client(
            "--case=follower_log_catchup_verify",
            "--db=pytest_follower_log_catchup_db",
            "--table=pytest_follower_log_catchup_table",
            "--key_count=16",
            "--timeout_ms=90000",
        )
        assert_e2e_passed(verify, cluster)
    finally:
        if catching_up_stopped:
            cluster.start_service(catching_up_follower)


def test_follower_snapshot_catchup_case(cluster):
    seed = run_e2e_client(
        "--case=follower_snapshot_catchup_prepare",
        "--db=pytest_follower_snapshot_catchup_db",
        "--table=pytest_follower_snapshot_catchup_table",
        "--key_count=16",
    )
    seed_output = assert_e2e_passed(seed, cluster)
    _, followers = replica_services(cluster, seed_output)
    catching_up_follower = followers[0]

    cluster.stop_service(catching_up_follower)
    catching_up_stopped = True
    try:
        gap = run_e2e_client(
            "--case=follower_snapshot_catchup_write_gap",
            "--db=pytest_follower_snapshot_catchup_db",
            "--table=pytest_follower_snapshot_catchup_table",
            "--key_count=1050",
            "--timeout_ms=120000",
        )
        assert_e2e_passed(gap, cluster)

        cluster.start_service(catching_up_follower)
        catching_up_stopped = False
        time.sleep(3)
        verify = run_e2e_client(
            "--case=follower_snapshot_catchup_verify",
            "--db=pytest_follower_snapshot_catchup_db",
            "--table=pytest_follower_snapshot_catchup_table",
            "--key_count=1050",
            "--timeout_ms=120000",
        )
        assert_e2e_passed(verify, cluster)
    finally:
        if catching_up_stopped:
            cluster.start_service(catching_up_follower)

