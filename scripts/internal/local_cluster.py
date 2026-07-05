import argparse
import os
import shlex
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
BUILD_TYPE = os.environ.get("BUILD_TYPE", "Release")
BUILD_DIR_NAME = os.environ.get(
    "BUILD_DIR",
    {
        "Debug": "build/debug",
        "Release": "build/release",
    }.get(BUILD_TYPE, f"build/{BUILD_TYPE}"),
)
BUILD_DIR = ROOT_DIR / BUILD_DIR_NAME
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
    return ServiceSpec(name, [str(binary), f"--conf={conf}"], host, port)


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
        service_log_dir = self.log_dir / spec.name
        service_log_dir.mkdir(parents=True, exist_ok=True)
        return service_log_dir

    def _launch_process(self, spec: ServiceSpec, env: dict[str, str],
                        log_path: Optional[Path]) -> subprocess.Popen:
        if log_path is None:
            return subprocess.Popen(
                spec.command,
                cwd=ROOT_DIR,
                env=env,
                text=True,
            )

        stdout_path = log_path / "stdout.log"
        stderr_path = log_path / "stderr.log"
        with stdout_path.open("w", encoding="utf-8") as stdout_file, \
                stderr_path.open("w", encoding="utf-8") as stderr_file:
            return subprocess.Popen(
                spec.command,
                cwd=ROOT_DIR,
                env=env,
                stdout=stdout_file,
                stderr=stderr_file,
                text=True,
            )

    def _log_hint(self, log_path: Optional[Path]) -> str:
        if log_path is None:
            return "stdout/stderr not captured by LocalCluster"
        return f"logs: {log_path / 'stdout.log'}, {log_path / 'stderr.log'}"

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


def default_cluster() -> LocalCluster:
    return cluster_from_confs(
        log_dir=BUILD_DIR / "runtime" / "logs",
        sdm_conf=ROOT_DIR / "conf" / "sdm.yaml",
        meta_conf=ROOT_DIR / "conf" / "meta.yaml",
        storage_confs=[
            ROOT_DIR / "conf" / "storage-1.yaml",
            ROOT_DIR / "conf" / "storage-2.yaml",
            ROOT_DIR / "conf" / "storage-3.yaml",
            ROOT_DIR / "conf" / "storage-4.yaml",
            ROOT_DIR / "conf" / "storage-5.yaml",
            ROOT_DIR / "conf" / "storage-6.yaml",
        ],
        capture_output=True,
    )


def list_processes() -> list[tuple[int, str]]:
    output = subprocess.run(
        ["ps", "-Ao", "pid=,command="],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout
    processes: list[tuple[int, str]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pid_text, _, command = line.partition(" ")
        try:
            processes.append((int(pid_text), command.strip()))
        except ValueError:
            continue
    return processes


def spec_matches_command(spec: ServiceSpec, command: str) -> bool:
    try:
        argv = shlex.split(command)
    except ValueError:
        return False
    if not argv:
        return False
    if Path(argv[0]).name != Path(spec.command[0]).name:
        return False
    return all(arg in argv[1:] for arg in spec.command[1:])


def find_pids_for_spec(spec: ServiceSpec) -> list[int]:
    return [
        pid
        for pid, command in list_processes()
        if spec_matches_command(spec, command)
    ]


def print_status(cluster: LocalCluster) -> None:
    for spec in cluster.specs:
        pids = find_pids_for_spec(spec)
        state = "RUNNING" if pids else "STOPPED"
        pid_text = ",".join(str(pid) for pid in pids) if pids else "-"
        print(f"{spec.name:<10} {state:<7} pid={pid_text:<10} {spec.host}:{spec.port}")


def kill_spec(spec: ServiceSpec) -> None:
    pids = find_pids_for_spec(spec)
    if not pids:
        print(f"{spec.name} already stopped")
        return
    print(f"stopping {spec.name}: {pids}")
    for pid in pids:
        try:
            os.kill(pid, 15)
        except ProcessLookupError:
            pass
    time.sleep(1.0)
    remaining = find_pids_for_spec(spec)
    if remaining:
        print(f"force killing {spec.name}: {remaining}")
        for pid in remaining:
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass


def start_spec(cluster: LocalCluster, spec: ServiceSpec) -> None:
    if find_pids_for_spec(spec):
        print(f"{spec.name} already running")
        return
    cluster.start_service(spec.name)
    print(f"{spec.name} started")


def handle_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Manage local AdvisKV demo cluster")
    parser.add_argument("command", choices=["start", "stop", "kill", "restart", "status"])
    parser.add_argument("service", nargs="?", help="sdm, meta, storage-N")
    args = parser.parse_args(argv)

    cluster = default_cluster()
    known = {spec.name: spec for spec in cluster.specs}

    if args.command == "status":
        print_status(cluster)
        return 0

    if args.command == "start" and args.service is None:
        for spec in cluster.specs:
            start_spec(cluster, spec)
        return 0

    if args.command == "stop" and args.service is None:
        for spec in reversed(cluster.specs):
            kill_spec(spec)
        return 0

    if args.service not in known:
        print(f"unknown service: {args.service}", file=os.sys.stderr)
        print(f"known services: {', '.join(known)}", file=os.sys.stderr)
        return 1

    spec = known[args.service]
    if args.command in ("stop", "kill"):
        kill_spec(spec)
    elif args.command == "start":
        start_spec(cluster, spec)
    elif args.command == "restart":
        kill_spec(spec)
        start_spec(cluster, spec)
    return 0


if __name__ == "__main__":
    raise SystemExit(handle_cli(os.sys.argv[1:]))
