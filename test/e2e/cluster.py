import os
import shutil
import subprocess
import sys
import textwrap
from dataclasses import replace
from pathlib import Path
from typing import Optional

from scripts.local_cluster import (
    BIN_DIR,
    BUILD_DIR,
    ROOT_DIR,
    LocalCluster,
    ServiceSpec,
    build_targets,
)


LOCALHOST = "127.0.0.1"
META_PORT = 50048
SDM_PORT = 50049
STORAGE_BASE_PORT = 50050
STORAGE_COUNT = 3
DEFAULT_ZONE = "dc1"
DEFAULT_RESOURCE_POOL = "default"
DEFAULT_TIMEOUT_MS = 60000
DEFAULT_POLL_INTERVAL_MS = 500

E2E_DIR = BUILD_DIR / "e2e_test"
CONF_DIR = E2E_DIR / "conf"
DATA_DIR = E2E_DIR / "data"
LOG_DIR = E2E_DIR / "logs"


def build_e2e_targets() -> None:
    build_targets("sdm sdm_test meta storage e2e_client")


def clean_data_dirs() -> None:
    shutil.rmtree(E2E_DIR, ignore_errors=True)
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def write_config_file(path: Path, content: str) -> None:
    normalized = textwrap.dedent(content).strip()
    lines = [line.lstrip() for line in normalized.splitlines()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def storage_service_name(index: int) -> str:
    return f"storage-{index}"


def storage_port(index: int) -> int:
    return STORAGE_BASE_PORT + index


def logger_block(logger_name: str, log_dir: Path, filename: str) -> str:
    return textwrap.dedent(
        f"""
        logger_name: "{logger_name}"
        log_dir: "{log_dir}"
        log_filename: "{filename}"
        log_level: "debug"
        log_to_console: false
        log_to_file: true
        """
    ).strip()


def write_client_config() -> None:
    write_config_file(
        CONF_DIR / "e2e_client.yaml",
        f"""
        {logger_block('e2e_sdk_log', LOG_DIR, 'sdk.log')}
        """,
    )


def write_sdm_config() -> None:
    write_config_file(
        CONF_DIR / "sdm.yaml",
        f"""
        listen_host: "{LOCALHOST}"
        port: {SDM_PORT}
        data_dir: "{DATA_DIR / 'sdm'}"

        {logger_block('e2e_sdm_log', LOG_DIR / 'sdm', 'sdm.log')}
        """,
    )


def write_meta_config() -> None:
    write_config_file(
        CONF_DIR / "meta.yaml",
        f"""
        listen_host: "{LOCALHOST}"
        port: {META_PORT}
        sdm_host: "{LOCALHOST}"
        sdm_port: {SDM_PORT}
        data_dir: "{DATA_DIR / 'meta'}"

        {logger_block('e2e_meta_log', LOG_DIR / 'meta', 'meta.log')}
        """,
    )


def write_storage_config(index: int) -> None:
    name = storage_service_name(index)
    write_config_file(
        CONF_DIR / f"{name}.yaml",
        f"""
        node_id: "{name}"
        ip: "{LOCALHOST}"
        listen_host: "{LOCALHOST}"
        port: {storage_port(index)}
        resource_pool: "{DEFAULT_RESOURCE_POOL}"
        dc: "{DEFAULT_ZONE}"
        data_dir: "{DATA_DIR / name}"
        manager_host: "{LOCALHOST}"
        manager_port: {SDM_PORT}
        heartbeat_interval_ms: 3000
        enable_test_api: true

        {logger_block(f'e2e_{name}_log', LOG_DIR / name, f'{name}.log')}
        """,
    )


def write_e2e_configs() -> None:
    write_client_config()
    write_sdm_config()
    write_meta_config()
    for index in range(1, STORAGE_COUNT + 1):
        write_storage_config(index)


def e2e_service_specs(
    service_envs: Optional[dict[str, dict[str, str]]] = None,
) -> list[ServiceSpec]:
    envs = service_envs or {}
    specs = [
        ServiceSpec(
            "sdm",
            [str(BIN_DIR / "sdm_test"), str(CONF_DIR / "sdm.yaml")],
            LOCALHOST,
            SDM_PORT,
            env=envs.get("sdm"),
        ),
        ServiceSpec(
            "meta",
            [str(BIN_DIR / "meta"), str(CONF_DIR / "meta.yaml")],
            LOCALHOST,
            META_PORT,
            env=envs.get("meta"),
        ),
    ]
    for index in range(1, STORAGE_COUNT + 1):
        name = storage_service_name(index)
        specs.append(
            ServiceSpec(
                name,
                [str(BIN_DIR / "storage"), str(CONF_DIR / f"{name}.yaml")],
                LOCALHOST,
                storage_port(index),
                env=envs.get(name),
            )
        )
    return specs


def run_and_tee(command: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
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


class AdvisKVCluster(LocalCluster):
    """AdvisKV-specific local cluster facade used by e2e tests."""

    def __init__(self,
                 service_envs: Optional[dict[str, dict[str, str]]] = None):
        self.service_envs = dict(service_envs or {})
        super().__init__(
            specs=e2e_service_specs(self.service_envs),
            log_dir=LOG_DIR,
        )

    def set_service_env(self, name: str,
                        env: Optional[dict[str, str]]) -> None:
        updated_env = None if env is None else dict(env)
        if updated_env is None:
            self.service_envs.pop(name, None)
        else:
            self.service_envs[name] = updated_env

        for index, spec in enumerate(self.specs):
            if spec.name == name:
                self.specs[index] = replace(spec, env=updated_env)
                return
        raise KeyError(f"unknown service: {name}")

    def clear_service_env(self, name: str) -> None:
        self.set_service_env(name, None)

    def assert_service_stopped(self, name: str, timeout_s: float = 10.0) -> None:
        assert self.wait_service_stopped(name, timeout_s=timeout_s), name

    def run_client(self, *args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env.setdefault("FORCE_COLOR", "1")
        command = [
            str(BIN_DIR / "e2e_client"),
            f"--conf={CONF_DIR / 'e2e_client.yaml'}",
            f"--meta_host={LOCALHOST}",
            f"--meta_port={META_PORT}",
            f"--sdm_host={LOCALHOST}",
            f"--sdm_port={SDM_PORT}",
            f"--zone={DEFAULT_ZONE}",
            f"--resource_pool={DEFAULT_RESOURCE_POOL}",
            "--shard_count=1",
            f"--replica_count={STORAGE_COUNT}",
            f"--timeout_ms={DEFAULT_TIMEOUT_MS}",
            f"--poll_interval_ms={DEFAULT_POLL_INTERVAL_MS}",
            *args,
        ]
        return run_and_tee(command, env)

    def run_case(self,
                 *,
                 case: Optional[str] = None,
                 db: str,
                 table: str,
                 key_count: int,
                 timeout_ms: Optional[int] = None) -> subprocess.CompletedProcess:
        args = [
            f"--db={db}",
            f"--table={table}",
            f"--key_count={key_count}",
        ]
        if case is not None:
            args.insert(0, f"--case={case}")
        if timeout_ms is not None:
            args.append(f"--timeout_ms={timeout_ms}")
        return self.run_client(*args)


def run_e2e_client(*args: str) -> subprocess.CompletedProcess:
    return AdvisKVCluster().run_client(*args)
