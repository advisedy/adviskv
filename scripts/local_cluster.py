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


def load_flat_conf(path: Path) -> dict[str, str]:
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


def conf_value(path: Path, key: str) -> str:
    value = load_flat_conf(path).get(key, "")
    if not value:
        raise RuntimeError(f"missing required config key: {path}:{key}")
    return value


def connect_host(host: str) -> str:
    if not host or host == "0.0.0.0":
        return "127.0.0.1"
    return host


def service_spec_from_conf(name: str, binary: Path, conf: Path) -> ServiceSpec:
    return ServiceSpec(
        name,
        [str(binary), str(conf)],
        connect_host(conf_value(conf, "listen_host")),
        int(conf_value(conf, "port")),
    )


class LocalCluster:
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

    def _spec_by_name(self, name: str) -> ServiceSpec:
        for spec in self.specs:
            if spec.name == name:
                return spec
        raise KeyError(f"unknown service: {name}")

    def _handle_by_name(self, name: str) -> Optional[ProcessHandle]:
        for handle in self.processes:
            if handle.spec.name == name:
                return handle
        return None

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
        if self._handle_by_name(name) is not None:
            raise RuntimeError(f"service already started: {name}")
        spec = self._spec_by_name(name)
        assert_port_free(spec.host, spec.port)

        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / spec.name
        if self.capture_output:
            log_path = log_path.with_suffix(".out")
            with log_path.open("w", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    spec.command,
                    cwd=ROOT_DIR,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
        else:
            process = subprocess.Popen(
                spec.command,
                cwd=ROOT_DIR,
                text=True,
            )
        handle = ProcessHandle(spec, process, log_path)
        self.processes.append(handle)
        time.sleep(1.0)
        if process.poll() is not None:
            self.processes.remove(handle)
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

    def stop_service(self, name: str) -> None:
        handle = self._handle_by_name(name)
        if handle is None:
            return
        if handle.process.poll() is None:
            handle.process.terminate()
        try:
            handle.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            handle.process.kill()
            handle.process.wait(timeout=5)
        self.processes.remove(handle)

    def kill_service(self, name: str) -> None:
        handle = self._handle_by_name(name)
        if handle is None:
            return
        if handle.process.poll() is None:
            handle.process.kill()
            handle.process.wait(timeout=5)
        self.processes.remove(handle)

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

    def restart_service(self, name: str) -> None:
        self.stop_service(name)
        self.start_service(name)

    def logs_summary(self) -> str:
        lines = [f"logs dir: {self.log_dir}"]
        for handle in self.processes:
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
    specs = [
        service_spec_from_conf("sdm", BIN_DIR / "sdm", sdm_conf),
        service_spec_from_conf("meta", BIN_DIR / "meta", meta_conf),
        *[
            service_spec_from_conf(f"storage-{index}", BIN_DIR / "storage", conf)
            for index, conf in enumerate(storage_confs, start=1)
        ],
    ]
    return LocalCluster(
        specs=specs,
        log_dir=log_dir,
        capture_output=capture_output,
    )
