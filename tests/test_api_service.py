import asyncio
import json
from unittest.mock import patch

from job_scheduler.api import api_service
from job_scheduler.core.resource import Device


class FakeQueue:
    def __init__(self, size: int = 0):
        self.size = size

    def qsize(self) -> int:
        return self.size


class FakeResource:
    def __init__(self):
        self.added_devices = []
        self.raise_on_add = None
        self.snapshot = {"device_count": 0, "hosts": {}}
        self.trace_events = []

    def add_device(self, device: Device):
        if self.raise_on_add is not None:
            raise self.raise_on_add
        self.added_devices.append(device)

    def debug_print(self) -> dict:
        return self.snapshot

    def get_trace_events(self, clear: bool = False) -> list[dict]:
        events = list(self.trace_events)
        if clear:
            self.trace_events.clear()
        return events


class FakeWorkload:
    def __init__(self):
        self.snapshot = {"job_count": 0, "jobs": [], "job_map": {}}
        self.trace_events = []

    def debug_print(self) -> dict:
        return self.snapshot

    def get_trace_events(self, clear: bool = False) -> list[dict]:
        events = list(self.trace_events)
        if clear:
            self.trace_events.clear()
        return events


class FakeScheduler:
    def __init__(self):
        self.resource = FakeResource()
        self.workload = FakeWorkload()
        self.jobs_to_wake_up = FakeQueue()
        self.added_jobs = []
        self.removed_jobs = []
        self.add_job_result = True
        self.remove_job_result = True
        self.enqueue_changes_queue = True
        self.trace_enabled = True
        self.trace_events = []

    def get_resource(self):
        return self.resource

    def get_workload(self):
        return self.workload

    def add_job_to_wake(self, job):
        if self.enqueue_changes_queue:
            self.jobs_to_wake_up.size += 1

    def add_job(self, job):
        self.added_jobs.append(job)
        return self.add_job_result

    def remove_job(self, job_id: str):
        self.removed_jobs.append(job_id)
        return self.remove_job_result

    def configure_trace_events(self, enabled: bool):
        self.trace_enabled = bool(enabled)

    def get_trace_events(self, clear: bool = False) -> list[dict]:
        events = list(self.trace_events)
        if clear:
            self.trace_events.clear()
        return events


DEVICE_PAYLOAD = {
    "uuid": "gpu-1",
    "host_name": "node-1",
    "id": 0,
    "type": "GPU",
    "vendor": "NVIDIA",
    "memory_size_mb": 16000,
}

JOB_PAYLOAD = {
    "job_id": "job-1",
    "base_url": "http://job-1",
    "priority": 5,
    "devices": [DEVICE_PAYLOAD],
}


def decode_response(response) -> dict:
    return json.loads(response.body.decode("utf-8"))


def test_get_resource_returns_scheduler_snapshot():
    fake_scheduler = FakeScheduler()
    fake_scheduler.resource.snapshot = {"device_count": 2, "hosts": {"node-1": {}}}

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.get_resource()
        payload = decode_response(response)
        assert payload == {
            "status": "ok",
            "endpoint": "/resource",
            "resource": {"device_count": 2, "hosts": {"node-1": {}}},
        }

    asyncio.run(scenario())


def test_get_workload_returns_scheduler_snapshot():
    fake_scheduler = FakeScheduler()
    fake_scheduler.workload.snapshot = {"job_count": 1, "jobs": [{"job_id": "job-1"}], "job_map": {}}

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.get_workload()
        payload = decode_response(response)
        assert payload == {
            "status": "ok",
            "endpoint": "/workload",
            "workload": {"job_count": 1, "jobs": [{"job_id": "job-1"}], "job_map": {}},
        }

    asyncio.run(scenario())


def test_add_device_returns_added_response_on_success():
    fake_scheduler = FakeScheduler()
    request_model = api_service.DeviceInfo(**DEVICE_PAYLOAD)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.add_device(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "added",
            "endpoint": "/add_device",
            "device": DEVICE_PAYLOAD,
        }
        assert len(fake_scheduler.resource.added_devices) == 1
        assert fake_scheduler.resource.added_devices[0].uuid == "gpu-1"

    asyncio.run(scenario())


def test_add_device_returns_rejected_response_on_error():
    fake_scheduler = FakeScheduler()
    fake_scheduler.resource.raise_on_add = RuntimeError("Duplicate device uuid: gpu-1")
    request_model = api_service.DeviceInfo(**DEVICE_PAYLOAD)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.add_device(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "rejected",
            "endpoint": "/add_device",
            "device": DEVICE_PAYLOAD,
            "message": "Duplicate device uuid: gpu-1",
        }

    asyncio.run(scenario())


def test_wake_up_returns_scheduled_when_job_is_enqueued():
    fake_scheduler = FakeScheduler()
    request_model = api_service.JobRequest(**JOB_PAYLOAD)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.wake_up(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "scheduled",
            "endpoint": "/wake_up",
            "job_id": "job-1",
            "base_url": "http://job-1",
            "priority": 5,
            "devices": [DEVICE_PAYLOAD],
            "enqueued": True,
            "queue_size": 1,
        }

    asyncio.run(scenario())


def test_wake_up_returns_ignored_when_scheduler_skips_enqueue():
    fake_scheduler = FakeScheduler()
    fake_scheduler.enqueue_changes_queue = False
    request_model = api_service.JobRequest(**JOB_PAYLOAD)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.wake_up(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "ignored",
            "endpoint": "/wake_up",
            "job_id": "job-1",
            "base_url": "http://job-1",
            "priority": 5,
            "devices": [DEVICE_PAYLOAD],
            "enqueued": False,
            "queue_size": 0,
        }

    asyncio.run(scenario())


def test_add_job_returns_added_response():
    fake_scheduler = FakeScheduler()
    request_model = api_service.JobRequest(**JOB_PAYLOAD)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.add_job(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "added",
            "endpoint": "/add_job",
            "job_id": "job-1",
            "base_url": "http://job-1",
            "priority": 5,
            "devices": [DEVICE_PAYLOAD],
            "added": True,
        }
        assert len(fake_scheduler.added_jobs) == 1
        assert fake_scheduler.added_jobs[0].job_id == "job-1"

    asyncio.run(scenario())


def test_add_job_returns_rejected_response():
    fake_scheduler = FakeScheduler()
    fake_scheduler.add_job_result = False
    request_model = api_service.JobRequest(**JOB_PAYLOAD)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.add_job(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "rejected",
            "endpoint": "/add_job",
            "job_id": "job-1",
            "base_url": "http://job-1",
            "priority": 5,
            "devices": [DEVICE_PAYLOAD],
            "added": False,
        }

    asyncio.run(scenario())


def test_remove_job_returns_removed_response():
    fake_scheduler = FakeScheduler()
    request_model = api_service.RemoveJobRequest(job_id="job-1")

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.remove_job(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "removed",
            "endpoint": "/remove_job",
            "job_id": "job-1",
            "removed": True,
        }
        assert fake_scheduler.removed_jobs == ["job-1"]

    asyncio.run(scenario())


def test_remove_job_returns_not_found_response():
    fake_scheduler = FakeScheduler()
    fake_scheduler.remove_job_result = False
    request_model = api_service.RemoveJobRequest(job_id="job-404")

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.remove_job(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "not_found",
            "endpoint": "/remove_job",
            "job_id": "job-404",
            "removed": False,
        }

    asyncio.run(scenario())


def test_trace_config_updates_scheduler_trace_flag():
    fake_scheduler = FakeScheduler()
    request_model = api_service.TraceConfigRequest(enabled=False)

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.set_trace_config(request_model)
        payload = decode_response(response)
        assert payload == {
            "status": "ok",
            "endpoint": "/trace/config",
            "enabled": False,
        }
        assert fake_scheduler.trace_enabled is False

    asyncio.run(scenario())


def test_get_trace_events_returns_component_and_merged_events_sorted():
    fake_scheduler = FakeScheduler()
    fake_scheduler.trace_events = [
        {"component": "scheduler", "event": "scheduler_enqueue", "ts": "2026-03-30T10:00:03+00:00"},
    ]
    fake_scheduler.resource.trace_events = [
        {"component": "resource", "event": "resource_device_added", "ts": "2026-03-30T10:00:01+00:00"},
    ]
    fake_scheduler.workload.trace_events = [
        {"component": "workload", "event": "workload_job_added", "ts": "2026-03-30T10:00:02+00:00"},
    ]

    async def scenario():
        with patch.object(api_service, "scheduler", fake_scheduler):
            response = await api_service.get_trace_events(clear=True)
        payload = decode_response(response)
        assert payload["status"] == "ok"
        assert payload["endpoint"] == "/trace/events"
        assert payload["clear"] is True
        assert payload["scheduler"] == [
            {"component": "scheduler", "event": "scheduler_enqueue", "ts": "2026-03-30T10:00:03+00:00"}
        ]
        assert payload["resource"] == [
            {"component": "resource", "event": "resource_device_added", "ts": "2026-03-30T10:00:01+00:00"}
        ]
        assert payload["workload"] == [
            {"component": "workload", "event": "workload_job_added", "ts": "2026-03-30T10:00:02+00:00"}
        ]
        assert [event["component"] for event in payload["events"]] == [
            "resource",
            "workload",
            "scheduler",
        ]
        assert fake_scheduler.trace_events == []
        assert fake_scheduler.resource.trace_events == []
        assert fake_scheduler.workload.trace_events == []

    asyncio.run(scenario())
