import os
import shutil
import socket
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
BUILD_DIR = ROOT_DIR / os.environ.get("BUILD_DIR", "build")
BIN_DIR = BUILD_DIR / "bin"
E2E_DIR = BUILD_DIR / "e2e_test"
CONF_DIR = E2E_DIR / "conf"
DATA_DIR = E2E_DIR / "data"
LOG_DIR = E2E_DIR / "logs"


@dataclass(frozen=True)
class ServiceSpec:
    name: str
    command: list[str]
    host: str
    port: int


class ProcessHandle:
    def __init__(self, spec: ServiceSpec, process: subprocess.Popen, log_path: Path):
        self.spec = spec
        self.process = process
        self.log_path = log_path

    @property
    def pid(self) -> int:
        return self.process.pid


def wait_port(host: str, port: int, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {host}:{port}: {last_error}")


def assert_port_free(host: str, port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        if sock.connect_ex((host, port)) == 0:
            raise RuntimeError(
                f"port is already in use: {host}:{port}; "
                "run ./scripts/stop_cluster.sh before pytest e2e"
            )


def build_e2e_targets() -> None:
    env = os.environ.copy()
    env["BUILD_TARGETS"] = "sdm meta storage e2e_client"
    subprocess.run([str(ROOT_DIR / "scripts" / "build.sh")],
                   cwd=ROOT_DIR,
                   env=env,
                   check=True)


def clean_data_dirs() -> None:
    shutil.rmtree(E2E_DIR, ignore_errors=True)
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def write_file(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


def write_e2e_configs() -> None:
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
        log_to_console: true
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
        log_to_console: true
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

            logger_name: "e2e_storage-{index}_log"
            log_dir: "{LOG_DIR / f"storage-{index}"}"
            log_filename: "storage-{index}.log"
            log_level: "debug"
            log_to_console: true
            log_to_file: true
            """,
        )


class AdvisKVCluster:
    def __init__(self):
        self.processes: list[ProcessHandle] = []
        self.specs = [
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

    def start(self) -> None:
        try:
            for spec in self.specs:
                assert_port_free(spec.host, spec.port)

            for spec in self.specs:
                log_path = LOG_DIR / f"{spec.name}.log"
                log_file = log_path.open("w")
                process = subprocess.Popen(
                    spec.command,
                    cwd=ROOT_DIR,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                log_file.close()
                handle = ProcessHandle(spec, process, log_path)
                self.processes.append(handle)
                time.sleep(1.0)
                if process.poll() is not None:
                    raise RuntimeError(
                        f"{spec.name} exited early with code "
                        f"{process.returncode}; log: {log_path}"
                    )
                try:
                    wait_port(spec.host, spec.port)
                except TimeoutError as exc:
                    raise RuntimeError(
                        f"{spec.name} did not listen on {spec.host}:"
                        f"{spec.port}; log: {log_path}"
                    ) from exc
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        for handle in reversed(self.processes):
            if handle.process.poll() is None:
                handle.process.terminate()
        for handle in reversed(self.processes):
            try:
                handle.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                handle.process.kill()
                handle.process.wait(timeout=5)
        self.processes.clear()

    def restart(self) -> None:
        self.stop()
        self.start()

    def logs_summary(self) -> str:
        lines = [f"logs dir: {LOG_DIR}"]
        for handle in self.processes:
            lines.append(f"{handle.spec.name}: {handle.log_path}")
        return "\n".join(lines)


def run_e2e_client(*args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.setdefault("FORCE_COLOR", "1")
    command = [
        str(BIN_DIR / "e2e_client"),
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
