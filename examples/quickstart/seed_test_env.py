import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from job_scheduler.api.api_client import AdminClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed resource and workload for quickstart test environment",
    )
    parser.add_argument(
        "--scheduler-url",
        default="http://127.0.0.1:8000",
        help="Scheduler API base URL",
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("seed_config.json")),
        help="Path to seed config JSON file",
    )
    parser.add_argument(
        "--launch-mock-jobs",
        action="store_true",
        help="Launch one mock_job process per seed job before seeding jobs",
    )
    parser.add_argument(
        "--mock-mean-wakeup-seconds",
        type=float,
        default=8.0,
        help="Poisson mean wake-up interval used for launched mock jobs",
    )
    parser.add_argument(
        "--mock-action-success-rate",
        type=float,
        default=0.8,
        help="Success probability for /wake_up and /sleep in launched mock jobs",
    )
    return parser.parse_args()


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


def _extract_host_port(url: str) -> tuple[str, int]:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.hostname or parsed.port is None:
        raise RuntimeError(f"Invalid base_url in seed job: {url}")
    return parsed.hostname, parsed.port


def launch_mock_jobs(seed_jobs: list[dict], scheduler_url: str, mean_wakeup_seconds: float, action_success_rate: float) -> list[subprocess.Popen]:
    script_path = Path(__file__).with_name("mock_job.py")
    processes: list[subprocess.Popen] = []

    for job in seed_jobs:
        host, port = _extract_host_port(job["base_url"])
        cmd = [
            sys.executable,
            str(script_path),
            "--scheduler-url",
            scheduler_url,
            "--job-id",
            job["job_id"],
            "--listen-host",
            host,
            "--listen-port",
            str(port),
            "--priority",
            str(job.get("priority", 5)),
            "--devices-json",
            json.dumps(job["devices"], separators=(",", ":")),
            "--mean-wakeup-seconds",
            str(mean_wakeup_seconds),
            "--action-success-rate",
            str(action_success_rate),
            "--init-delay-seconds",
            str(job.get("init_delay_seconds", 0.0)),
            "--success-delay-min-seconds",
            str(job.get("success_delay_min_seconds", 0.0)),
            "--success-delay-max-seconds",
            str(job.get("success_delay_max_seconds", 0.0)),
        ]
        # Keep children in the same session so Ctrl+C from parent shell can stop them.
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        print(
            f"[seed] launched mock_job job_id={job['job_id']} pid={proc.pid} host={host} port={port}"
        )

    return processes


def build_devices(config: dict[str, Any] | None = None) -> list[dict]:
    if config is None:
        config = _load_seed_config(Path(__file__).with_name("seed_config.json"))
    devices = config["devices"]
    return [dict(item) for item in devices]


def build_seed_jobs(config: dict[str, Any] | None = None) -> list[dict]:
    if config is None:
        config = _load_seed_config(Path(__file__).with_name("seed_config.json"))
    seed_jobs = config["seed_jobs"]
    return [dict(item) for item in seed_jobs]


def main() -> None:
    args = parse_args()
    client = AdminClient(args.scheduler_url)
    config = _load_seed_config(args.config)
    seed_jobs = build_seed_jobs(config)
    devices = build_devices(config)

    print(f"[seed] scheduler_url={args.scheduler_url}")

    print("[seed] adding devices")
    for device in devices:
        response = client.add_device(device)
        print(json.dumps(response, ensure_ascii=True))

    if args.launch_mock_jobs:
        print("[seed] launching mock jobs")
        launch_mock_jobs(
            seed_jobs=seed_jobs,
            scheduler_url=args.scheduler_url,
            mean_wakeup_seconds=args.mock_mean_wakeup_seconds,
            action_success_rate=args.mock_action_success_rate,
        )

    else:
        print("[seed] adding initial jobs")
        for job in seed_jobs:
            response = client.add_job(
                job_id=job["job_id"],
                base_url=job["base_url"],
                devices=job["devices"],
                priority=job["priority"],
            )
            print(json.dumps(response, ensure_ascii=True))

    print("[seed] resource snapshot")
    print(json.dumps(client.get_resource(), ensure_ascii=True))

    print("[seed] workload snapshot")
    print(json.dumps(client.get_workload(), ensure_ascii=True))


if __name__ == "__main__":
    main()
