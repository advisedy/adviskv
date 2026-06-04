import re
import time

from .cluster import AdvisKVCluster


VERIFY_TIMEOUT_MS = 90_000
SNAPSHOT_TIMEOUT_MS = 120_000
CATCHUP_WAIT_SECONDS = 3

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
ROUTE_LEADER_RE = re.compile(r"\[ ROUTE_LEADER \] ([^:]+):(\d+)")
ROUTE_REPLICA_RE = re.compile(r"\[ ROUTE_REPLICA \] ([^:]+):(\d+) role=(\w+)")


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def clean_case_output(result, cluster: AdvisKVCluster) -> str:
    assert result.returncode == 0, result.stdout + "\n" + cluster.logs_summary()
    output = strip_ansi(result.stdout)
    assert "[ FAIL ]" not in output, output
    return output


def run_case_and_get_output(cluster: AdvisKVCluster, **kwargs) -> str:
    result = cluster.run_case(**kwargs)
    return clean_case_output(result, cluster)


def parse_route_leader(output: str) -> tuple[str, int]:
    """Parse a `[ ROUTE_LEADER ] host:port` line from e2e_client output."""
    match = ROUTE_LEADER_RE.search(strip_ansi(output))
    assert match is not None, output
    return match.group(1), int(match.group(2))


def parse_route_replicas(output: str) -> list[tuple[str, int, str]]:
    """Parse `[ ROUTE_REPLICA ] host:port role=...` lines from e2e_client output."""
    clean = strip_ansi(output)
    matches = ROUTE_REPLICA_RE.findall(clean)
    assert matches, output
    return [(host, int(port), role) for host, port, role in matches]


def follower_services_from_output(cluster: AdvisKVCluster, output: str) -> list[str]:
    followers = [
        cluster.service_name_for_port(port)
        for _, port, role in parse_route_replicas(output)
        if role == "FOLLOWER"
    ]
    assert len(followers) >= 2, output
    return followers


def leader_service_from_output(cluster: AdvisKVCluster, output: str) -> str:
    _, leader_port = parse_route_leader(output)
    return cluster.service_name_for_port(leader_port)


def run_create_table_crash_story(cluster: AdvisKVCluster, *, crash_point: str,
                                 prepare_case: str, verify_case: str, db: str,
                                 table: str) -> None:
    cluster.set_service_env("sdm", {"ADVISKV_ENABLE_CRASH_POINT": crash_point})
    cluster.restart_service("sdm")

    prepare_output = run_case_and_get_output(
        cluster,
        case=prepare_case,
        db=db,
        table=table,
        key_count=8,
    )
    assert prepare_output
    cluster.assert_service_stopped("sdm")

    cluster.clear_service_env("sdm")
    cluster.start_service("sdm")

    run_case_and_get_output(
        cluster,
        case=verify_case,
        db=db,
        table=table,
        key_count=8,
        timeout_ms=VERIFY_TIMEOUT_MS,
    )


def run_follower_catchup_story(cluster: AdvisKVCluster, *, prepare_case: str,
                               gap_case: str, verify_case: str, db: str,
                               table: str, key_count: int,
                               verify_timeout_ms: int) -> None:
    prepare_output = run_case_and_get_output(
        cluster,
        case=prepare_case,
        db=db,
        table=table,
        key_count=16,
    )
    catching_up_follower = follower_services_from_output(cluster,
                                                         prepare_output)[0]

    cluster.stop_service(catching_up_follower)
    follower_stopped = True
    try:
        run_case_and_get_output(
            cluster,
            case=gap_case,
            db=db,
            table=table,
            key_count=key_count,
            timeout_ms=verify_timeout_ms if key_count > 1000 else None,
        )

        cluster.start_service(catching_up_follower)
        follower_stopped = False
        time.sleep(CATCHUP_WAIT_SECONDS)

        run_case_and_get_output(
            cluster,
            case=verify_case,
            db=db,
            table=table,
            key_count=key_count,
            timeout_ms=verify_timeout_ms,
        )
    finally:
        if follower_stopped:
            cluster.start_service(catching_up_follower)


def test_meta_to_storage_basic_kv_chain(cluster: AdvisKVCluster):
    run_case_and_get_output(
        cluster,
        db="pytest_e2e_db",
        table="pytest_e2e_table",
        key_count=16,
    )


def test_restart_persistence_case(cluster: AdvisKVCluster):
    run_case_and_get_output(
        cluster,
        case="restart_persistence_seed",
        db="pytest_restart_db",
        table="pytest_restart_table",
        key_count=16,
    )

    cluster.restart()

    run_case_and_get_output(
        cluster,
        case="restart_persistence_verify",
        db="pytest_restart_db",
        table="pytest_restart_table",
        key_count=16,
    )


def test_create_table_crash_before_persist_case(cluster: AdvisKVCluster):
    run_create_table_crash_story(
        cluster,
        crash_point="sdm.place_table.before_put_table",
        prepare_case="create_table_crash_before_persist_prepare",
        verify_case="create_table_crash_before_persist_verify",
        db="pytest_create_before_persist_db",
        table="pytest_create_before_persist_table",
    )


def test_create_table_crash_after_persist_case(cluster: AdvisKVCluster):
    run_create_table_crash_story(
        cluster,
        crash_point="sdm.place_table.after_put_table",
        prepare_case="create_table_crash_after_persist_prepare",
        verify_case="create_table_crash_after_persist_verify",
        db="pytest_create_after_persist_db",
        table="pytest_create_after_persist_table",
    )


def test_leader_failover_case(cluster: AdvisKVCluster):
    prepare_output = run_case_and_get_output(
        cluster,
        case="leader_failover_seed",
        db="pytest_leader_failover_db",
        table="pytest_leader_failover_table",
        key_count=16,
    )
    cluster.stop_service(leader_service_from_output(cluster, prepare_output))

    run_case_and_get_output(
        cluster,
        case="leader_failover_verify",
        db="pytest_leader_failover_db",
        table="pytest_leader_failover_table",
        key_count=16,
        timeout_ms=VERIFY_TIMEOUT_MS,
    )


def test_follower_log_catchup_case(cluster: AdvisKVCluster):
    run_follower_catchup_story(
        cluster,
        prepare_case="follower_log_catchup_prepare",
        gap_case="follower_log_catchup_write_gap",
        verify_case="follower_log_catchup_verify",
        db="pytest_follower_log_catchup_db",
        table="pytest_follower_log_catchup_table",
        key_count=16,
        verify_timeout_ms=VERIFY_TIMEOUT_MS,
    )


def test_follower_snapshot_catchup_case(cluster: AdvisKVCluster):
    run_follower_catchup_story(
        cluster,
        prepare_case="follower_snapshot_catchup_prepare",
        gap_case="follower_snapshot_catchup_write_gap",
        verify_case="follower_snapshot_catchup_verify",
        db="pytest_follower_snapshot_catchup_db",
        table="pytest_follower_snapshot_catchup_table",
        key_count=1050,
        verify_timeout_ms=SNAPSHOT_TIMEOUT_MS,
    )


def test_sdm_crash_recovery_case(cluster: AdvisKVCluster):
    """SDM 崩溃后重启，验证数据持久化：创建 DB/table 写入数据 → kill SDM → 重启 SDM → 验证数据可读。"""
    run_case_and_get_output(
        cluster,
        case="sdm_crash_seed",
        db="pytest_sdm_crash_db",
        table="pytest_sdm_crash_table",
        key_count=16,
    )

    cluster.kill_service("sdm")
    cluster.assert_service_stopped("sdm")
    cluster.start_service("sdm")

    run_case_and_get_output(
        cluster,
        case="sdm_crash_verify",
        db="pytest_sdm_crash_db",
        table="pytest_sdm_crash_table",
        key_count=16,
        timeout_ms=VERIFY_TIMEOUT_MS,
    )


def test_meta_crash_recovery_case(cluster: AdvisKVCluster):
    """Meta 崩溃后重启，验证数据持久化：创建 DB/table 写入数据 → kill Meta → 重启 Meta → 验证数据可读。"""
    run_case_and_get_output(
        cluster,
        case="meta_crash_seed",
        db="pytest_meta_crash_db",
        table="pytest_meta_crash_table",
        key_count=16,
    )

    cluster.kill_service("meta")
    cluster.assert_service_stopped("meta")
    cluster.start_service("meta")

    run_case_and_get_output(
        cluster,
        case="meta_crash_verify",
        db="pytest_meta_crash_db",
        table="pytest_meta_crash_table",
        key_count=16,
        timeout_ms=VERIFY_TIMEOUT_MS,
    )