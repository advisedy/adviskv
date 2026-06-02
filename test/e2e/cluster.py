import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

from scripts.local_cluster import (
    BIN_DIR,
    BUILD_DIR,
    ROOT_DIR,
    LocalCluster,
    ServiceSpec,
    build_targets,
)


E2E_DIR = BUILD_DIR / "e2e_test"
CONF_DIR = E2E_DIR / "conf"
DATA_DIR = E2E_DIR / "data"
LOG_DIR = E2E_DIR / "logs"


def build_e2e_targets() -> None:
    build_targets("sdm meta storage e2e_client")


def clean_data_dirs() -> None:
    shutil.rmtree(E2E_DIR, ignore_errors=True)
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def write_file(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


def write_e2e_configs() -> None:
    write_file(
        CONF_DIR / "e2e_client.yaml",
        f"""
        logger_name: "e2e_sdk_log"
        log_dir: "{LOG_DIR}"
        log_filename: "sdk.log"
        log_level: "debug"
        log_to_console: false
        log_to_file: true
        """,
    )
    write_file(
        CONF_DIR / "sdm.yaml",
        f"""
        listen_host: "127.0.0.1"
        port: 50049
        data_dir: "{DATA_DIR / "sdm"}"

        logger_name: "e2e_sdm_log"
        log_dir: "{LOG_DIR / "sdm"}"
        log_filename: "sdm.log"
        log_level: "debug"
        log_to_console: false
        log_to_file: true
        """,
    )
    write_file(
        CONF_DIR / "meta.yaml",
        f"""
        listen_host: "127.0.0.1"
        port: 50048
        sdm_host: "127.0.0.1"
        sdm_port: 50049
        data_dir: "{DATA_DIR / "meta"}"

        logger_name: "e2e_meta_log"
        log_dir: "{LOG_DIR / "meta"}"
        log_filename: "meta.log"
        log_level: "debug"
        log_to_console: false
        log_to_file: true
        """,
    )
    for index in range(1, 4):
        write_file(
            CONF_DIR / f"storage-{index}.yaml",
            f"""
            node_id: "storage-{index}"
            ip: "127.0.0.1"
            listen_host: "127.0.0.1"
            port: {50050 + index}
            resource_pool: "default"
            dc: "dc1"
            data_dir: "{DATA_DIR / f"storage-{index}"}"
            manager_host: "127.0.0.1"
            manager_port: 50049
            heartbeat_interval_ms: 3000
            enable_test_api: true

            logger_name: "e2e_storage-{index}_log"
            log_dir: "{LOG_DIR / f"storage-{index}"}"
            log_filename: "storage-{index}.log"
            log_level: "debug"
            log_to_console: false
            log_to_file: true
            """,
        )


class AdvisKVCluster(LocalCluster):
    def __init__(self):
        specs = [
            ServiceSpec(
                "sdm",
                [str(BIN_DIR / "sdm"), str(CONF_DIR / "sdm.yaml")],
                "127.0.0.1",
                50049,
            ),
            ServiceSpec(
                "meta",
                [str(BIN_DIR / "meta"), str(CONF_DIR / "meta.yaml")],
                "127.0.0.1",
                50048,
            ),
            ServiceSpec(
                "storage-1",
                [str(BIN_DIR / "storage"), str(CONF_DIR / "storage-1.yaml")],
                "127.0.0.1",
                50051,
            ),
            ServiceSpec(
                "storage-2",
                [str(BIN_DIR / "storage"), str(CONF_DIR / "storage-2.yaml")],
                "127.0.0.1",
                50052,
            ),
            ServiceSpec(
                "storage-3",
                [str(BIN_DIR / "storage"), str(CONF_DIR / "storage-3.yaml")],
                "127.0.0.1",
                50053,
            ),
        ]
        super().__init__(specs=specs, log_dir=LOG_DIR)


def run_e2e_client(*args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.setdefault("FORCE_COLOR", "1")
    command = [
        str(BIN_DIR / "e2e_client"),
        f"--conf={CONF_DIR / 'e2e_client.yaml'}",
        "--meta_host=127.0.0.1",
        "--meta_port=50048",
        "--sdm_host=127.0.0.1",
        "--sdm_port=50049",
        "--zone=dc1",
        "--resource_pool=default",
        "--shard_count=1",
        "--replica_count=3",
        "--timeout_ms=60000",
        "--poll_interval_ms=500",
        *args,
    ]
    process = subprocess.Popen(
        command,
        cwd=ROOT_DIR,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output_lines: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        output_lines.append(line)
        sys.__stdout__.write(line)
        sys.__stdout__.flush()
    returncode = process.wait()
    return subprocess.CompletedProcess(
        args=command,
        returncode=returncode,
        stdout="".join(output_lines),
        stderr=None,
    )
