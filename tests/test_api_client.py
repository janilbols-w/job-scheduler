import json
from unittest.mock import patch
from urllib import error

from job_scheduler.api.api_client import AdminClient, JobClient
from job_scheduler.core.resource import Device


class DummyResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def make_device(uuid: str = "gpu-1", memory_size_mb: int = 16000) -> Device:
    return Device(
        uuid=uuid,
        host_name="node-1",
        id=0,
        type="GPU",
        vendor="NVIDIA",
        memory_size_mb=memory_size_mb,
    )


def test_admin_client_add_device_posts_device_payload():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["method"] = req.get_method()
        captured["timeout"] = timeout
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return DummyResponse({"status": "added"})

    client = AdminClient("http://scheduler.test")
    device = make_device()

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response = client.add_device(device)

    assert response == {"status": "added"}
    assert captured["url"] == "http://scheduler.test/add_device"
    assert captured["method"] == "POST"
    assert captured["timeout"] == 5.0
    assert captured["payload"]["uuid"] == "gpu-1"
    assert captured["payload"]["memory_size_mb"] == 16000


def test_admin_client_get_resource_uses_get_without_payload():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["method"] = req.get_method()
        captured["data"] = req.data
        captured["timeout"] = timeout
        return DummyResponse({"status": "ok", "resource": {"device_count": 1}})

    client = AdminClient("http://scheduler.test", timeout_seconds=2.5)

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response = client.get_resource()

    assert response == {"status": "ok", "resource": {"device_count": 1}}
    assert captured["url"] == "http://scheduler.test/resource"
    assert captured["method"] == "GET"
    assert captured["data"] is None
    assert captured["timeout"] == 2.5


def test_admin_client_add_job_posts_job_payload():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return DummyResponse({"status": "added", "added": True})

    client = AdminClient("http://scheduler.test")
    device = make_device()

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response = client.add_job(
            job_id="job-1",
            base_url="http://job-1",
            devices=[device],
            priority=7,
        )

    assert response == {"status": "added", "added": True}
    assert captured["url"] == "http://scheduler.test/add_job"
    assert captured["payload"]["job_id"] == "job-1"
    assert captured["payload"]["base_url"] == "http://job-1"
    assert captured["payload"]["priority"] == 7
    assert captured["payload"]["devices"][0]["uuid"] == "gpu-1"


def test_job_client_wake_up_uses_fixed_payload_from_init():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        captured["timeout"] = timeout
        return DummyResponse({"status": "scheduled", "enqueued": True})

    client = JobClient(
        scheduler_base_url="http://scheduler.test",
        job_id="job-fixed",
        base_url="http://job-fixed",
        devices=[make_device("gpu-2", 24000)],
        priority=9,
        timeout_seconds=1.2,
    )

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response = client.wake_up()

    assert response == {"status": "scheduled", "enqueued": True}
    assert captured["url"] == "http://scheduler.test/wake_up"
    assert captured["timeout"] == 1.2
    assert captured["payload"] == {
        "job_id": "job-fixed",
        "base_url": "http://job-fixed",
        "priority": 9,
        "devices": [
            {
                "uuid": "gpu-2",
                "host_name": "node-1",
                "id": 0,
                "type": "GPU",
                "vendor": "NVIDIA",
                "memory_size_mb": 24000,
            }
        ],
    }


def test_admin_client_remove_job_posts_job_id_payload():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return DummyResponse({"status": "removed", "removed": True})

    client = AdminClient("http://scheduler.test")

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response = client.remove_job("job-remove")

    assert response == {"status": "removed", "removed": True}
    assert captured["url"] == "http://scheduler.test/remove_job"
    assert captured["payload"] == {"job_id": "job-remove"}


def test_admin_client_set_trace_config_posts_enabled_payload():
    captured = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["method"] = req.get_method()
        captured["timeout"] = timeout
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return DummyResponse({"status": "ok", "enabled": False})

    client = AdminClient("http://scheduler.test", timeout_seconds=3.0)

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response = client.set_trace_config(False)

    assert response == {"status": "ok", "enabled": False}
    assert captured["url"] == "http://scheduler.test/trace/config"
    assert captured["method"] == "POST"
    assert captured["timeout"] == 3.0
    assert captured["payload"] == {"enabled": False}


def test_admin_client_get_trace_events_passes_clear_query_param():
    calls = []

    def fake_urlopen(req, timeout):
        calls.append({"url": req.full_url, "method": req.get_method(), "timeout": timeout})
        return DummyResponse({"status": "ok", "events": []})

    client = AdminClient("http://scheduler.test", timeout_seconds=1.5)

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        response_default = client.get_trace_events()
        response_clear = client.get_trace_events(clear=True)

    assert response_default == {"status": "ok", "events": []}
    assert response_clear == {"status": "ok", "events": []}
    assert calls[0]["url"] == "http://scheduler.test/trace/events?clear=0"
    assert calls[1]["url"] == "http://scheduler.test/trace/events?clear=1"
    assert calls[0]["method"] == "GET"
    assert calls[1]["method"] == "GET"
    assert calls[0]["timeout"] == 1.5
    assert calls[1]["timeout"] == 1.5


def test_admin_client_collects_client_trace_events_for_success_request():
    def fake_urlopen(req, timeout):
        _ = (req, timeout)
        return DummyResponse({"status": "ok"})

    client = AdminClient("http://scheduler.test")

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        client.get_resource()

    traces = client.get_client_trace_events(clear=False)
    assert len(traces) == 1
    assert traces[0]["component"] == "api_client"
    assert traces[0]["event"] == "api_request"
    assert traces[0]["ok"] is True
    assert traces[0]["method"] == "GET"
    assert traces[0]["path"] == "/resource"

    cleared = client.get_client_trace_events(clear=True)
    assert len(cleared) == 1
    assert client.get_client_trace_events(clear=False) == []


def test_admin_client_collects_client_trace_events_for_error_request():
    def fake_urlopen(req, timeout):
        _ = (req, timeout)
        raise error.URLError("connection refused")

    client = AdminClient("http://scheduler.test")

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        try:
            client.get_resource()
        except error.URLError:
            pass

    traces = client.get_client_trace_events(clear=True)
    assert len(traces) == 1
    assert traces[0]["component"] == "api_client"
    assert traces[0]["event"] == "api_request"
    assert traces[0]["ok"] is False
    assert traces[0]["method"] == "GET"
    assert traces[0]["path"] == "/resource"
    assert "connection refused" in traces[0]["error"]


def test_admin_client_trace_toggle_disables_and_reenables_local_trace_collection():
    def fake_urlopen(req, timeout):
        _ = (req, timeout)
        return DummyResponse({"status": "ok"})

    client = AdminClient("http://scheduler.test")

    with patch("job_scheduler.api.api_client.request.urlopen", side_effect=fake_urlopen):
        client.configure_trace_events(False)
        client.get_resource()
        assert client.get_client_trace_events(clear=False) == []

        client.configure_trace_events(True)
        client.get_resource()

    traces = client.get_client_trace_events(clear=True)
    assert len(traces) == 1
    assert traces[0]["event"] == "api_request"
    assert traces[0]["ok"] is True
