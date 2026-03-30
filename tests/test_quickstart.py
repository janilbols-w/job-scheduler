import argparse
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.quickstart import mock_job, seed_test_env, start_api_server
from examples.quickstart import simulation


class DummyPopen:
    def __init__(self, cmd):
        self.cmd = cmd
        self.pid = 4321


class FakeAdminClient:
    def __init__(self, scheduler_url: str):
        self.scheduler_url = scheduler_url
        self.added_devices = []
        self.added_jobs = []
        self.trace_config = []
        self.trace_events_calls = []
        self.trace_events_response = {"events": []}
        self.client_trace_events_calls = []
        self.client_trace_events_response = []

    def add_device(self, device):
        self.added_devices.append(device)
        return {"status": "added"}

    def add_job(self, job_id, base_url, devices, priority=5):
        self.added_jobs.append(
            {
                "job_id": job_id,
                "base_url": base_url,
                "devices": devices,
                "priority": priority,
            }
        )
        return {"status": "added", "added": True}

    def get_resource(self):
        return {"status": "ok", "resource": {"device_count": 4}}

    def get_workload(self):
        return {"status": "ok", "workload": {"job_count": len(self.added_jobs)}}

    def set_trace_config(self, enabled: bool):
        self.trace_config.append(bool(enabled))
        return {"status": "ok", "enabled": bool(enabled)}

    def get_trace_events(self, clear: bool = False):
        self.trace_events_calls.append(bool(clear))
        return self.trace_events_response

    def get_client_trace_events(self, clear: bool = False):
        self.client_trace_events_calls.append(bool(clear))
        return list(self.client_trace_events_response)


def test_start_api_server_main_calls_uvicorn_with_expected_target():
    args = argparse.Namespace(host="0.0.0.0", port=18000, log_level="debug")

    with patch.object(start_api_server, "parse_args", return_value=args), patch(
        "examples.quickstart.start_api_server.uvicorn.run"
    ) as mocked_run:
        start_api_server.main()

    mocked_run.assert_called_once_with(
        "job_scheduler.api.api_service:app",
        host="0.0.0.0",
        port=18000,
        log_level="debug",
    )


def test_extract_host_port_success_and_invalid_case():
    assert seed_test_env._extract_host_port("http://127.0.0.1:9100") == ("127.0.0.1", 9100)

    with pytest.raises(RuntimeError, match="Invalid base_url"):
        seed_test_env._extract_host_port("not-a-url")


def test_launch_mock_jobs_builds_expected_subprocess_commands():
    jobs = [
        {
            "job_id": "seed-job-0",
            "base_url": "http://127.0.0.1:9100",
            "priority": 5,
            "init_delay_seconds": 1.25,
            "devices": [{"uuid": "d-0", "host_name": "host-0", "id": 0, "type": "GPU", "vendor": "NVIDIA", "memory_size_mb": 7000}],
        }
    ]

    captured = {}

    def fake_popen(cmd):
        captured["cmd"] = cmd
        return DummyPopen(cmd)

    with patch("examples.quickstart.seed_test_env.subprocess.Popen", side_effect=fake_popen):
        processes = seed_test_env.launch_mock_jobs(
            seed_jobs=jobs,
            scheduler_url="http://127.0.0.1:8000",
            mean_wakeup_seconds=7.0,
            action_success_rate=0.75,
        )

    assert len(processes) == 1
    assert processes[0].pid == 4321
    assert "mock_job.py" in captured["cmd"][1]
    assert "--scheduler-url" in captured["cmd"]
    assert "http://127.0.0.1:8000" in captured["cmd"]
    assert "--job-id" in captured["cmd"]
    assert "seed-job-0" in captured["cmd"]
    assert "--init-delay-seconds" in captured["cmd"]
    assert "1.25" in captured["cmd"]
    assert "--success-delay-min-seconds" in captured["cmd"]
    assert "--success-delay-max-seconds" in captured["cmd"]


def test_seed_main_without_launch_adds_devices_and_jobs():
    args = argparse.Namespace(
        scheduler_url="http://127.0.0.1:8000",
        config="examples/quickstart/seed_config.json",
        launch_mock_jobs=False,
        mock_mean_wakeup_seconds=8.0,
        mock_action_success_rate=0.8,
    )
    fake_client = FakeAdminClient(args.scheduler_url)

    with patch.object(seed_test_env, "parse_args", return_value=args), patch(
        "examples.quickstart.seed_test_env.AdminClient", return_value=fake_client
    ), patch.object(seed_test_env, "launch_mock_jobs") as mocked_launch:
        seed_test_env.main()

    assert len(fake_client.added_devices) == 4
    assert len(fake_client.added_jobs) == len(seed_test_env.build_seed_jobs())
    mocked_launch.assert_not_called()


def test_seed_main_with_launch_spawns_mock_jobs_and_skips_add_job():
    args = argparse.Namespace(
        scheduler_url="http://127.0.0.1:8000",
        config="examples/quickstart/seed_config.json",
        launch_mock_jobs=True,
        mock_mean_wakeup_seconds=9.0,
        mock_action_success_rate=0.6,
    )
    fake_client = FakeAdminClient(args.scheduler_url)

    with patch.object(seed_test_env, "parse_args", return_value=args), patch(
        "examples.quickstart.seed_test_env.AdminClient", return_value=fake_client
    ), patch.object(seed_test_env, "launch_mock_jobs", return_value=[] ) as mocked_launch:
        seed_test_env.main()

    assert len(fake_client.added_devices) == 4
    assert len(fake_client.added_jobs) == 0
    mocked_launch.assert_called_once()


def test_parse_devices_json_and_build_devices_from_uuids():
    devices = mock_job.parse_devices_json('[{"uuid":"d-0","memory_size_mb":7000}]')
    assert devices[0]["uuid"] == "d-0"
    assert devices[0]["host_name"] == "host-mock"

    generated = mock_job.build_devices_from_uuids(["d-0", "d-1", "d-2"], 9000)
    assert len(generated) == 3
    assert generated[0]["host_name"] == "host-0"
    assert generated[2]["host_name"] == "host-1"
    assert generated[1]["memory_size_mb"] == 9000


def test_mock_job_random_action_response_status_codes():
    worker = mock_job.MockJob(
        scheduler_url="http://127.0.0.1:8000",
        job_id="job-test",
        listen_host="127.0.0.1",
        listen_port=9100,
        devices=[{"uuid": "d-0", "host_name": "host-0", "id": 0, "type": "GPU", "vendor": "NVIDIA", "memory_size_mb": 8000}],
        action_success_rate=0.5,
    )

    with patch("examples.quickstart.mock_job.random.random", return_value=0.1):
        ok_response = asyncio.run(worker._random_action_response("wake_up"))
    assert ok_response.status_code == 200

    with patch("examples.quickstart.mock_job.random.random", return_value=0.9):
        fail_response = asyncio.run(worker._random_action_response("sleep"))
    assert fail_response.status_code == 503


def test_mock_job_status_transitions_on_successful_actions():
    worker = mock_job.MockJob(
        scheduler_url="http://127.0.0.1:8000",
        job_id="job-state",
        listen_host="127.0.0.1",
        listen_port=9102,
        devices=[{"uuid": "d-0", "host_name": "host-0", "id": 0, "type": "GPU", "vendor": "NVIDIA", "memory_size_mb": 8000}],
        action_success_rate=1.0,
    )

    assert worker.status == "asleep"

    with patch("examples.quickstart.mock_job.random.random", return_value=0.0):
        wake_resp = asyncio.run(worker._random_action_response("wake_up"))
    assert wake_resp.status_code == 200
    assert worker.status == "awake"

    with patch("examples.quickstart.mock_job.random.random", return_value=0.0):
        sleep_resp = asyncio.run(worker._random_action_response("sleep"))
    assert sleep_resp.status_code == 200
    assert worker.status == "asleep"


def test_mock_job_success_response_delay_uses_configured_random_range():
    worker = mock_job.MockJob(
        scheduler_url="http://127.0.0.1:8000",
        job_id="job-delay",
        listen_host="127.0.0.1",
        listen_port=9104,
        devices=[{"uuid": "d-0", "host_name": "host-0", "id": 0, "type": "GPU", "vendor": "NVIDIA", "memory_size_mb": 8000}],
        action_success_rate=1.0,
        success_delay_min_seconds=0.2,
        success_delay_max_seconds=0.8,
    )

    captured = {"sleep": None}

    async def fake_sleep(seconds: float):
        captured["sleep"] = seconds

    with patch("examples.quickstart.mock_job.random.random", return_value=0.0), patch(
        "examples.quickstart.mock_job.random.uniform", return_value=0.37
    ), patch("examples.quickstart.mock_job.asyncio.sleep", side_effect=fake_sleep):
        response = asyncio.run(worker._random_action_response("wake_up"))

    assert response.status_code == 200
    assert worker.status == "awake"
    assert captured["sleep"] == 0.37


def test_mock_job_periodic_wake_skips_when_awake():
    worker = mock_job.MockJob(
        scheduler_url="http://127.0.0.1:8000",
        job_id="job-awake",
        listen_host="127.0.0.1",
        listen_port=9103,
        devices=[{"uuid": "d-0", "host_name": "host-0", "id": 0, "type": "GPU", "vendor": "NVIDIA", "memory_size_mb": 8000}],
        action_success_rate=1.0,
    )
    worker.status = "awake"

    called = {"wake_up": 0}

    def fake_wake_up():
        called["wake_up"] += 1
        return {"status": "scheduled"}

    worker.job_client.wake_up = fake_wake_up

    asyncio.run(worker._periodic_wake_step(0.5))

    assert called["wake_up"] == 0


def test_mock_job_run_calls_uvicorn_with_worker_app():
    worker = mock_job.MockJob(
        scheduler_url="http://127.0.0.1:8000",
        job_id="job-run",
        listen_host="127.0.0.1",
        listen_port=9200,
        devices=[{"uuid": "d-1", "host_name": "host-0", "id": 1, "type": "GPU", "vendor": "NVIDIA", "memory_size_mb": 8000}],
    )

    with patch("examples.quickstart.mock_job.uvicorn.run") as mocked_run:
        worker.run()

    mocked_run.assert_called_once_with(
        worker.app,
        host="127.0.0.1",
        port=9200,
        log_level="info",
    )


def test_simulation_runner_trace_helpers_toggle_and_collect_events():
    args = argparse.Namespace(
        scheduler_url="http://127.0.0.1:8000",
        scheduler_host="127.0.0.1",
        scheduler_port=8000,
        config="examples/quickstart/seed_config.json",
        simulation_duration=1.0,
        sample_interval=0.1,
        api_ready_timeout=1.0,
        mock_mean_wakeup_seconds=8.0,
        mock_action_success_rate=0.8,
        api_log_level="info",
        disable_core_trace=False,
        output_dir=None,
    )

    fake_admin = FakeAdminClient(args.scheduler_url)
    fake_admin.trace_events_response = {
        "events": [
            {"component": "scheduler", "event": "scheduler_enqueue"},
            {"component": "resource", "event": "resource_device_added"},
            "invalid-event",
        ],
        "resource": [
            {"component": "resource", "event": "resource_device_added"},
        ],
        "workload": [
            {"component": "workload", "event": "workload_job_added"},
        ],
    }
    fake_admin.client_trace_events_response = [
        {"component": "api_client", "event": "api_request", "ok": True},
    ]

    with patch("examples.quickstart.simulation.AdminClient", return_value=fake_admin):
        runner = simulation.SimulationRunner(args)

    runner._set_core_trace_enabled(True)
    runner._collect_core_trace_events(clear=True)

    assert fake_admin.trace_config == [True]
    assert fake_admin.trace_events_calls == [True]
    assert fake_admin.client_trace_events_calls == [True]
    assert runner.events == [
        {"component": "scheduler", "event": "scheduler_enqueue"},
        {"component": "resource", "event": "resource_device_added"},
    ]
    assert runner.resource_events == [
        {"component": "resource", "event": "resource_device_added"},
    ]
    assert runner.workload_events == [
        {"component": "workload", "event": "workload_job_added"},
    ]
    assert runner.api_client_events == [
        {"component": "api_client", "event": "api_request", "ok": True},
    ]


def test_simulation_parse_args_disable_core_trace_flag(monkeypatch):
    argv = [
        "simulation.py",
        "--scheduler-host",
        "0.0.0.0",
        "--scheduler-port",
        "18000",
        "--disable-core-trace",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    args = simulation.parse_args()

    assert args.scheduler_url == "http://0.0.0.0:18000"
    assert args.disable_core_trace is True


def test_simulation_load_seed_config_rejects_non_object(tmp_path: Path):
    bad_config = tmp_path / "seed.json"
    bad_config.write_text(json.dumps([{"devices": []}]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="expected object"):
        simulation._load_seed_config(bad_config)


def test_simulation_collect_core_trace_events_ignores_connection_errors():
    args = argparse.Namespace(
        scheduler_url="http://127.0.0.1:8000",
        scheduler_host="127.0.0.1",
        scheduler_port=8000,
        config="examples/quickstart/seed_config.json",
        simulation_duration=1.0,
        sample_interval=0.1,
        api_ready_timeout=1.0,
        mock_mean_wakeup_seconds=8.0,
        mock_action_success_rate=0.8,
        api_log_level="info",
        disable_core_trace=False,
        output_dir=None,
    )

    class FailingAdmin:
        def get_trace_events(self, clear: bool = False):
            _ = clear
            raise RuntimeError("connection refused")

    with patch("examples.quickstart.simulation.AdminClient", return_value=FailingAdmin()):
        runner = simulation.SimulationRunner(args)

    runner.events = [{"event": "existing"}]
    runner._collect_core_trace_events(clear=True)

    assert runner.events == [{"event": "existing"}]
