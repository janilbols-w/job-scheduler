#!/usr/bin/env python3
import argparse
import json
import logging
import queue
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from job_scheduler.api.api_client import AdminClient
from job_scheduler.tools.trace_events import (
    generate_trace_graph,
    save_component_trace_logs,
    save_trace,
    summarize_trace,
)


logger = logging.getLogger(__name__)


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen


class SimulationRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.quickstart_dir = Path(__file__).resolve().parent
        self.repo_root = self.quickstart_dir.parents[1]
        self.config = _load_seed_config(args.config)
        self.admin = AdminClient(args.scheduler_url, timeout_seconds=2.0)

        self.events: list[dict[str, Any]] = []
        self.resource_events: list[dict[str, Any]] = []
        self.workload_events: list[dict[str, Any]] = []
        self.api_client_events: list[dict[str, Any]] = []
        self.samples: list[dict[str, Any]] = []
        self._log_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self._readers: list[threading.Thread] = []
        self.processes: list[ManagedProcess] = []

        self.started_at_monotonic = 0.0
        self.started_at_wall = ""
        self.output_dir = self._resolve_output_dir(args.output_dir)
        logger.info(
            "simulation initialized scheduler_url=%s config=%s output_dir=%s",
            self.args.scheduler_url,
            self.args.config,
            self.output_dir,
        )

    def _resolve_output_dir(self, raw_output_dir: str | None) -> Path:
        if raw_output_dir:
            return Path(raw_output_dir)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return self.quickstart_dir / "simulation_outputs" / stamp

    def _event_time(self) -> float:
        return time.monotonic() - self.started_at_monotonic

    def _start_reader(self, name: str, stream) -> None:
        def _reader() -> None:
            for line in iter(stream.readline, ""):
                self._log_queue.put(
                    {
                        "source": name,
                        "line": line.rstrip("\n"),
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                )

        t = threading.Thread(target=_reader, daemon=True)
        t.start()
        self._readers.append(t)

    def _spawn(self, name: str, cmd: list[str]) -> ManagedProcess:
        logger.info("spawning process name=%s cmd=%s", name, " ".join(cmd))
        proc = subprocess.Popen(
            cmd,
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        managed = ManagedProcess(name=name, process=proc)
        self.processes.append(managed)
        logger.info("process started name=%s pid=%s", name, proc.pid)
        if proc.stdout is not None:
            self._start_reader(name, proc.stdout)
        return managed

    def _set_core_trace_enabled(self, enabled: bool) -> None:
        logger.info("setting core trace enabled=%s", bool(enabled))
        self.admin.set_trace_config(enabled=enabled)

    def _collect_core_trace_events(self, clear: bool = True) -> None:
        try:
            response = self.admin.get_trace_events(clear=clear)
        except Exception:
            # API may already be down during shutdown; keep simulation teardown robust.
            logger.debug("trace fetch skipped: API unavailable clear=%s", clear)
            return
        incoming = response.get("events", [])
        if isinstance(incoming, list):
            before = len(self.events)
            self.events.extend(event for event in incoming if isinstance(event, dict))
            added = len(self.events) - before
            if added:
                logger.debug("collected trace events added=%s total=%s", added, len(self.events))

        resource_incoming = response.get("resource", [])
        if isinstance(resource_incoming, list):
            self.resource_events.extend(
                event for event in resource_incoming if isinstance(event, dict)
            )

        workload_incoming = response.get("workload", [])
        if isinstance(workload_incoming, list):
            self.workload_events.extend(
                event for event in workload_incoming if isinstance(event, dict)
            )

        self._collect_api_client_trace_events(clear=True)

    def _collect_api_client_trace_events(self, clear: bool = True) -> None:
        getter = getattr(self.admin, "get_client_trace_events", None)
        if not callable(getter):
            return
        incoming = getter(clear=clear)
        if isinstance(incoming, list):
            self.api_client_events.extend(event for event in incoming if isinstance(event, dict))

    def _wait_for_api(self) -> None:
        logger.info("waiting for API readiness timeout_seconds=%s", self.args.api_ready_timeout)
        deadline = time.time() + self.args.api_ready_timeout
        while time.time() < deadline:
            try:
                _ = self.admin.get_resource()
                logger.info("API is ready scheduler_url=%s", self.args.scheduler_url)
                return
            except Exception:
                time.sleep(0.2)
        raise RuntimeError(f"API not ready within {self.args.api_ready_timeout} seconds")

    def _start_api(self) -> None:
        cmd = [
            sys.executable,
            str(self.quickstart_dir / "start_api_server.py"),
            "--host",
            self.args.scheduler_host,
            "--port",
            str(self.args.scheduler_port),
            "--log-level",
            self.args.api_log_level,
        ]
        logger.info(
            "starting API host=%s port=%s uvicorn_level=%s",
            self.args.scheduler_host,
            self.args.scheduler_port,
            self.args.api_log_level,
        )
        self._spawn("api", cmd)

    def _seed_devices(self) -> None:
        logger.info("seeding devices count=%s", len(self.config["devices"]))
        for device in self.config["devices"]:
            self.admin.add_device(device)
        logger.info("device seeding completed")

    def _launch_mock_jobs(self) -> None:
        logger.info("launching mock jobs count=%s", len(self.config["seed_jobs"]))
        for job in self.config["seed_jobs"]:
            base_url = str(job["base_url"])
            host, port_str = base_url.rsplit(":", 1)
            listen_host = host.split("//", 1)[-1]
            listen_port = int(port_str)

            cmd = [
                sys.executable,
                str(self.quickstart_dir / "mock_job.py"),
                "--scheduler-url",
                self.args.scheduler_url,
                "--job-id",
                str(job["job_id"]),
                "--listen-host",
                listen_host,
                "--listen-port",
                str(listen_port),
                "--priority",
                str(job.get("priority", 5)),
                "--devices-json",
                json.dumps(job["devices"], separators=(",", ":")),
                "--mean-wakeup-seconds",
                str(self.args.mock_mean_wakeup_seconds),
                "--action-success-rate",
                str(self.args.mock_action_success_rate),
                "--init-delay-seconds",
                str(job.get("init_delay_seconds", 0.0)),
                "--success-delay-min-seconds",
                str(job.get("success_delay_min_seconds", 0.0)),
                "--success-delay-max-seconds",
                str(job.get("success_delay_max_seconds", 0.0)),
            ]
            self._spawn(f"mock:{job['job_id']}", cmd)
        logger.info("mock job launch completed")

    def _drain_logs(self) -> None:
        while True:
            try:
                item = self._log_queue.get_nowait()
            except queue.Empty:
                return

    @staticmethod
    def _compute_device_usage(workload: dict[str, Any]) -> dict[str, int]:
        usage: dict[str, int] = {}
        jobs = workload.get("jobs", [])
        if not isinstance(jobs, list):
            return usage
        for job in jobs:
            gpu_mem = job.get("gpu_memory_mb", {})
            if not isinstance(gpu_mem, dict):
                continue
            for uuid, memory in gpu_mem.items():
                usage[str(uuid)] = usage.get(str(uuid), 0) + int(memory)
        return usage

    def _sample(self) -> None:
        workload_resp = self.admin.get_workload()
        resource_resp = self.admin.get_resource()

        workload = workload_resp.get("workload", {})
        resource = resource_resp.get("resource", {})

        device_usage = self._compute_device_usage(workload)
        sample = {
            "t": round(self._event_time(), 4),
            "ts": datetime.now(timezone.utc).isoformat(),
            "job_count": int(workload.get("job_count", 0)),
            "device_usage_mb": device_usage,
            "total_requested_memory_mb": int(sum(device_usage.values())),
            "resource": resource,
            "workload_jobs": [
                {
                    "job_id": str(job.get("job_id", "")),
                    "gpu_memory_mb": dict(job.get("gpu_memory_mb", {}))
                    if isinstance(job.get("gpu_memory_mb", {}), dict)
                    else {},
                }
                for job in workload.get("jobs", [])
                if isinstance(job, dict)
            ],
        }
        self.samples.append(sample)
        logger.debug(
            "sampled t=%.4f job_count=%s total_requested_memory_mb=%s",
            sample["t"],
            sample["job_count"],
            sample["total_requested_memory_mb"],
        )

    def _stop_processes(self) -> None:
        logger.info("stopping managed processes count=%s", len(self.processes))
        for managed in reversed(self.processes):
            proc = managed.process
            if proc.poll() is None:
                logger.debug("terminating process name=%s pid=%s", managed.name, proc.pid)
                proc.terminate()

        deadline = time.time() + 4.0
        for managed in reversed(self.processes):
            proc = managed.process
            while proc.poll() is None and time.time() < deadline:
                time.sleep(0.05)
            if proc.poll() is None:
                logger.warning("killing unresponsive process name=%s pid=%s", managed.name, proc.pid)
                proc.kill()

        for managed in reversed(self.processes):
            _ = managed
        logger.info("all managed processes stopped")

    def run(self) -> dict[str, Any]:
        logger.info("simulation run started")
        self.started_at_monotonic = time.monotonic()
        self.started_at_wall = datetime.now(timezone.utc).isoformat()
        end_at = None
        if self.args.simulation_duration > 0:
            end_at = self.started_at_monotonic + self.args.simulation_duration

        def _signal_handler(_signum, _frame):
            raise KeyboardInterrupt

        old_int = signal.signal(signal.SIGINT, _signal_handler)
        old_term = signal.signal(signal.SIGTERM, _signal_handler)

        try:
            self._start_api()
            self._wait_for_api()
            self._set_core_trace_enabled(enabled=not self.args.disable_core_trace)
            self._seed_devices()
            self._launch_mock_jobs()

            while True:
                self._drain_logs()
                self._collect_core_trace_events(clear=True)
                self._sample()
                if end_at is not None and time.monotonic() >= end_at:
                    break
                time.sleep(self.args.sample_interval)
        except KeyboardInterrupt:
            logger.warning("simulation interrupted by signal")
            pass
        finally:
            self._collect_core_trace_events(clear=True)
            self._stop_processes()
            self._drain_logs()
            self._collect_core_trace_events(clear=True)
            self._collect_api_client_trace_events(clear=True)
            self.output_dir.mkdir(parents=True, exist_ok=True)
            graph_paths: list[Path] = []
            try:
                graph_paths = generate_trace_graph(
                    output_dir=self.output_dir,
                    events=self.events,
                    samples=self.samples,
                    config=self.config,
                )
                logger.info("graph generation completed count=%s", len(graph_paths))
            except Exception:
                logger.exception("graph generation failed")

            summary = summarize_trace(
                started_at=self.started_at_wall,
                scheduler_url=self.args.scheduler_url,
                config_path=str(self.args.config),
                sample_interval_seconds=float(self.args.sample_interval),
                simulation_duration_seconds=float(self.args.simulation_duration),
                events=self.events,
                samples=self.samples,
                output_dir=self.output_dir,
                duration_seconds=self._event_time(),
            )
            save_trace(
                output_dir=self.output_dir,
                events=self.events,
                samples=self.samples,
                summary=summary,
            )
            save_component_trace_logs(
                output_dir=self.output_dir,
                component_events={
                    "resource": self.resource_events,
                    "workload": self.workload_events,
                    "api_client": self.api_client_events,
                },
            )
            logger.info(
                "trace saved output_dir=%s events=%s samples=%s resource_events=%s workload_events=%s api_client_events=%s",
                self.output_dir,
                len(self.events),
                len(self.samples),
                len(self.resource_events),
                len(self.workload_events),
                len(self.api_client_events),
            )
            signal.signal(signal.SIGINT, old_int)
            signal.signal(signal.SIGTERM, old_term)

        result = {
            "output_dir": str(self.output_dir.resolve()),
            "events": len(self.events),
            "samples": len(self.samples),
            "resource_events": len(self.resource_events),
            "workload_events": len(self.workload_events),
            "api_client_events": len(self.api_client_events),
            "graphs": [str(path.resolve()) for path in graph_paths],
        }
        logger.info("simulation run completed result=%s", result)
        return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run scheduler quickstart simulation and export telemetry")
    parser.add_argument(
        "--scheduler-host",
        default="127.0.0.1",
        help="Scheduler API host for start_api_server.py",
    )
    parser.add_argument(
        "--scheduler-port",
        type=int,
        default=8000,
        help="Scheduler API port for start_api_server.py",
    )
    parser.add_argument(
        "--scheduler-url",
        default="",
        help="Scheduler base URL. If omitted, computed from host/port",
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("seed_config.json")),
        help="Path to simulation seed config JSON",
    )
    parser.add_argument(
        "--simulation-duration",
        type=float,
        default=60.0,
        help="Auto-stop after N seconds. Use <= 0 to run until Ctrl+C",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=1.0,
        help="Seconds between workload/resource snapshots",
    )
    parser.add_argument(
        "--api-ready-timeout",
        type=float,
        default=20.0,
        help="Maximum seconds to wait for API readiness",
    )
    parser.add_argument(
        "--mock-mean-wakeup-seconds",
        type=float,
        default=8.0,
        help="Poisson mean wake-up interval for launched mock jobs",
    )
    parser.add_argument(
        "--mock-action-success-rate",
        type=float,
        default=0.8,
        help="Success probability for /wake_up and /sleep in launched mock jobs",
    )
    parser.add_argument(
        "--api-log-level",
        default="info",
        choices=["critical", "error", "warning", "info", "debug", "trace"],
        help="Uvicorn log level for API process",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["critical", "error", "warning", "info", "debug"],
        help="Log level for simulation runner",
    )
    parser.add_argument(
        "--disable-core-trace",
        action="store_true",
        help="Disable trace events emitted by scheduler/resource/workload",
    )
    parser.add_argument(
        "--output-dir",
        default="./outputs",
        help="Directory to write simulation outputs. Default: timestamped folder under simulation_outputs",
    )
    args = parser.parse_args()

    if not args.scheduler_url:
        args.scheduler_url = f"http://{args.scheduler_host}:{args.scheduler_port}"

    if args.sample_interval <= 0:
        raise RuntimeError("sample-interval must be > 0")

    return args


def _load_seed_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise RuntimeError(f"Seed config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        raise RuntimeError(f"Invalid seed config format: expected object at {path}")

    devices = raw.get("devices")
    seed_jobs = raw.get("seed_jobs")
    if not isinstance(devices, list):
        raise RuntimeError(f"Invalid seed config: 'devices' must be a list in {path}")
    if not isinstance(seed_jobs, list):
        raise RuntimeError(f"Invalid seed config: 'seed_jobs' must be a list in {path}")

    return raw


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    runner = SimulationRunner(args)
    result = runner.run()

    print("[simulation] completed")
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
