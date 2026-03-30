import json
from typing import Any, Mapping, Sequence
from urllib import request

from job_scheduler.core.resource import Device


class ClientBase:
    """Shared HTTP and payload helpers for scheduler API clients."""

    def __init__(self, base_url: str, timeout_seconds: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(0.1, float(timeout_seconds))

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def _request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        data: bytes | None = None
        headers = {"Accept": "application/json"}

        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(
            self._build_url(path),
            method=method,
            data=data,
            headers=headers,
        )

        with request.urlopen(req, timeout=self.timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
            if not body:
                return {}
            return json.loads(body)

    @staticmethod
    def _device_payload(device: Device | Mapping[str, Any]) -> dict[str, Any]:
        if isinstance(device, Device):
            return device.to_dict()
        return {
            "uuid": str(device["uuid"]),
            "host_name": str(device["host_name"]),
            "id": int(device["id"]),
            "type": str(device["type"]),
            "vendor": str(device["vendor"]),
            "memory_size_mb": int(device["memory_size_mb"]),
        }

    @classmethod
    def _job_payload(
        cls,
        job_id: str,
        base_url: str,
        devices: Sequence[Device | Mapping[str, Any]],
        priority: int = 5,
    ) -> dict[str, Any]:
        return {
            "job_id": job_id,
            "base_url": base_url,
            "priority": int(priority),
            "devices": [cls._device_payload(device) for device in devices],
        }


class AdminClient(ClientBase):
    """General-purpose synchronous REST client for job_scheduler.api_service."""

    def add_device(self, device: Device | Mapping[str, Any]) -> dict[str, Any]:
        """POST /add_device"""
        return self._request("POST", "/add_device", payload=self._device_payload(device))

    def get_resource(self) -> dict[str, Any]:
        """GET /resource"""
        return self._request("GET", "/resource")

    def get_workload(self) -> dict[str, Any]:
        """GET /workload"""
        return self._request("GET", "/workload")

    def add_job(
        self,
        job_id: str,
        base_url: str,
        devices: Sequence[Device | Mapping[str, Any]],
        priority: int = 5,
    ) -> dict[str, Any]:
        """POST /add_job"""
        payload = self._job_payload(job_id, base_url, devices, priority)
        return self._request("POST", "/add_job", payload=payload)

    def wake_up(
        self,
        job_id: str,
        base_url: str,
        devices: Sequence[Device | Mapping[str, Any]],
        priority: int = 5,
    ) -> dict[str, Any]:
        """POST /wake_up"""
        payload = self._job_payload(job_id, base_url, devices, priority)
        return self._request("POST", "/wake_up", payload=payload)

    def remove_job(self, job_id: str) -> dict[str, Any]:
        """POST /remove_job"""
        return self._request("POST", "/remove_job", payload={"job_id": job_id})


class JobClient(ClientBase):
    """Job-scoped client that can only issue wake_up for a fixed job payload."""

    def __init__(
        self,
        scheduler_base_url: str,
        job_id: str,
        base_url: str,
        devices: Sequence[Device | Mapping[str, Any]],
        priority: int = 5,
        timeout_seconds: float = 5.0,
    ):
        super().__init__(scheduler_base_url, timeout_seconds=timeout_seconds)
        self.job_payload = self._job_payload(job_id, base_url, devices, priority)

    def wake_up(self) -> dict[str, Any]:
        """POST /wake_up for the fixed job payload configured at initialization."""
        return self._request("POST", "/wake_up", payload=self.job_payload)
