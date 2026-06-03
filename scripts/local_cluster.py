import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


ROOT_DIR = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT_DIR / os.environ.get("BUILD_DIR", "build")
BIN_DIR = BUILD_DIR / "bin"


@dataclass(frozen=True)
class ServiceSpec:
    name: str
    command: list[str]
    host: str
    port: int
    env: Optional[dict[str, str]] = None


class ProcessHandle:
    """Thin wrapper around a running local service process."""

    def __init__(self, spec: ServiceSpec, process: subprocess.Popen,
                 log_path: Optional[Path]):
        self.spec = spec
        self.process = process
        self.log_path = log_path

    @property
    def pid(self) -> int:
        return self.process.pid


def wait_port(host: str, port: int, timeout_s: float = 30.0) -> None:
    """Wait until a TCP port accepts connections."""
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
    """Fail early when a service port is already occupied."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        if sock.connect_ex((host, port)) == 0:
            raise RuntimeError(
                f"port is already in use: {host}:{port}; "
                "run ./scripts/stop_cluster.sh before starting local cluster"
            )


def build_targets(targets: str) -> None:
    env = os.environ.copy()
    env["BUILD_TARGETS"] = targets
    subprocess.run([str(ROOT_DIR / "scripts" / "build.sh")],
                   cwd=ROOT_DIR,
                   env=env,
                   check=True)


def parse_scalar(value: str) -> str:
    value = value.split("#", 1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def load_simple_kv_conf(path: Path) -> dict[str, str]:
    """Read a simple top-level `key: value` config file."""
    result: dict[str, str] = {}
    with path.open(encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip()
            if key:
                result[key] = parse_scalar(value)
    return result


def require_conf_value(values: dict[str, str], path: Path, key: str) -> str:
    value = values.get(key, "")
    if not value:
        raise RuntimeError(f"missing required config key: {path}:{key}")
    return value


def connect_host(host: str) -> str:
    """Return a connectable host for local clients.

    Service configs may listen on 0.0.0.0, but clients must dial localhost.
    """
    if not host or host == "0.0.0.0":
        return "127.0.0.1"
    return host


def service_spec_from_conf(name: str, binary: Path, conf: Path) -> ServiceSpec:
    values = load_simple_kv_conf(conf)
    host = connect_host(require_conf_value(values, conf, "listen_host"))
    port = int(require_conf_value(values, conf, "port"))
    return ServiceSpec(name, [str(binary), str(conf)], host, port)


class LocalCluster:
    """Launch and manage a small set of local service processes."""

    def __init__(
        self,
        specs: list[ServiceSpec],
        log_dir: Path,
        capture_output: bool = False,
    ):
        self.processes: list[ProcessHandle] = []
        self.specs = specs
        self.log_dir = log_dir
        self.capture_output = capture_output

    def _prune_exited_processes(self) -> None:
        self.processes = [
            handle for handle in self.processes if handle.process.poll() is None
        ]

    def _spec_by_name(self, name: str) -> ServiceSpec:
        for spec in self.specs:
            if spec.name == name:
                return spec
        raise KeyError(f"unknown service: {name}")

    def _handle_by_name(self, name: str) -> Optional[ProcessHandle]:
        self._prune_exited_processes()
        for handle in self.processes:
            if handle.spec.name == name:
                return handle
        return None

    def _build_process_env(self, spec: ServiceSpec) -> dict[str, str]:
        env = os.environ.copy()
        if spec.env:
            env.update(spec.env)
        return env

    def _log_path_for(self, spec: ServiceSpec) -> Optional[Path]:
        if not self.capture_output:
            return None
        self.log_dir.mkdir(parents=True, exist_ok=True)
        return (self.log_dir / spec.name).with_suffix(".out")

    def _launch_process(self, spec: ServiceSpec, env: dict[str, str],
                        log_path: Optional[Path]) -> subprocess.Popen:
        if log_path is None:
            return subprocess.Popen(
                spec.command,
                cwd=ROOT_DIR,
                env=env,
                text=True,
            )

        with log_path.open("w", encoding="utf-8") as log_file:
            return subprocess.Popen(
                spec.command,
                cwd=ROOT_DIR,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )

    def _log_hint(self, log_path: Optional[Path]) -> str:
        if log_path is None:
            return "stdout/stderr not captured by LocalCluster"
        return f"log: {log_path}"

    def _wait_until_started(self, handle: ProcessHandle) -> None:
        time.sleep(1.0)
        process = handle.process
        spec = handle.spec
        if process.poll() is not None:
            raise RuntimeError(
                f"{spec.name} exited early with code {process.returncode}; "
                f"{self._log_hint(handle.log_path)}"
            )
        try:
            wait_port(spec.host, spec.port)
        except TimeoutError as exc:
            raise RuntimeError(
                f"{spec.name} did not listen on {spec.host}:{spec.port}; "
                f"{self._log_hint(handle.log_path)}"
            ) from exc

    def _stop_process(self, handle: ProcessHandle, force: bool = False) -> None:
        process = handle.process
        if process.poll() is not None:
            return

        if force:
            process.kill()
            process.wait(timeout=5)
            return

        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def is_service_running(self, name: str) -> bool:
        return self._handle_by_name(name) is not None

    def wait_service_stopped(self, name: str, timeout_s: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not self.is_service_running(name):
                return True
            time.sleep(0.2)
        return not self.is_service_running(name)

    def service_name_for_port(self, port: int) -> str:
        for spec in self.specs:
            if spec.port == port:
                return spec.name
        raise KeyError(f"unknown service port: {port}")

    def start(self) -> None:
        try:
            for spec in self.specs:
                assert_port_free(spec.host, spec.port)

            for spec in self.specs:
                self.start_service(spec.name)
        except Exception:
            self.stop()
            raise

    def start_service(self, name: str) -> None:
        if self.is_service_running(name):
            raise RuntimeError(f"service already started: {name}")

        spec = self._spec_by_name(name)
        assert_port_free(spec.host, spec.port)
        env = self._build_process_env(spec)
        log_path = self._log_path_for(spec)
        process = self._launch_process(spec, env, log_path)
        handle = ProcessHandle(spec, process, log_path)
        self.processes.append(handle)

        try:
            self._wait_until_started(handle)
        except Exception:
            if handle in self.processes:
                self.processes.remove(handle)
            raise

    def stop_service(self, name: str) -> None:
        handle = self._handle_by_name(name)
        if handle is None:
            return
        self._stop_process(handle)
        self.processes.remove(handle)

    def kill_service(self, name: str) -> None:
        handle = self._handle_by_name(name)
        if handle is None:
            return
        self._stop_process(handle, force=True)
        self.processes.remove(handle)

    def stop(self) -> None:
        for handle in reversed(self.processes):
            self._stop_process(handle)
        self.processes.clear()

    def restart(self) -> None:
        self.stop()
        self.start()

    def restart_service(self, name: str) -> None:
        self.stop_service(name)
        self.start_service(name)

    def logs_summary(self) -> str:
        lines = [f"logs dir: {self.log_dir}"]
        for handle in self.processes:
            if handle.log_path is None:
                lines.append(f"{handle.spec.name}: stdout/stderr not captured")
            else:
                lines.append(f"{handle.spec.name}: {handle.log_path}")
        return "\n".join(lines)


def cluster_from_confs(
    *,
    log_dir: Path,
    sdm_conf: Path,
    meta_conf: Path,
    storage_confs: list[Path],
    capture_output: bool = False,
) -> LocalCluster:
    storage_specs = [
        service_spec_from_conf(f"storage-{index}", BIN_DIR / "storage", conf)
        for index, conf in enumerate(storage_confs, start=1)
    ]
    specs = [
        service_spec_from_conf("sdm", BIN_DIR / "sdm", sdm_conf),
        service_spec_from_conf("meta", BIN_DIR / "meta", meta_conf),
        *storage_specs,
    ]
    return LocalCluster(
        specs=specs,
        log_dir=log_dir,
        capture_output=capture_output,
    )
