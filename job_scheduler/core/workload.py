from dataclasses import dataclass, field
import time
from datetime import datetime, timezone
from typing import ClassVar, Dict, List, Optional
import logging
import threading

from .resource import Device, Resource


logger = logging.getLogger(__name__)


class WorkloadError:
    DEVICE_NOT_FOUND = "WL001"
    INVALID_REQUESTED_MEMORY = "WL002"
    INSUFFICIENT_MEMORY = "WL003"

    ERROR_MESSAGES = {
        DEVICE_NOT_FOUND: "device_not_found_in_resource",
        INVALID_REQUESTED_MEMORY: "invalid_requested_memory",
        INSUFFICIENT_MEMORY: "insufficient_memory",
    }

    @classmethod
    def log_error(cls, error_code: str, **context):
        message = cls.ERROR_MESSAGES.get(error_code, "unknown_workload_error")
        context_str = " ".join(f"{k}={v}" for k, v in context.items())
        logger.error("error_code=%s message=%s %s", error_code, message, context_str)


@dataclass
class Job:
    job_id: str
    base_url: str
    devices: list[Device]
    gpu_memory_mb: Dict[str, int] = field(default_factory=dict)
    priority: int = 5
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def verify_values(
        cls,
        job_id: str,
        base_url: str,
        devices: list[Device],
        gpu_memory_mb: Dict[str, int],
        priority: int,
        timestamp: float,
    ):
        if not job_id:
            raise RuntimeError("job_id is required")
        if not base_url:
            raise RuntimeError("base_url is required")
        if not devices:
            raise RuntimeError("devices must not be empty")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise RuntimeError("priority must be an integer")
        if priority < 0:
            raise RuntimeError("priority must be >= 0")
        if not isinstance(timestamp, (int, float)):
            raise RuntimeError("timestamp must be a number")

        device_uuids = set()
        for device in devices:
            if not isinstance(device, Device):
                raise RuntimeError("all devices must be Device instances")
            if device.uuid in device_uuids:
                raise RuntimeError(f"duplicate device uuid in job devices: {device.uuid}")
            device_uuids.add(device.uuid)

        for uuid in device_uuids:
            if uuid not in gpu_memory_mb:
                raise RuntimeError(f"missing gpu_memory_mb for device uuid: {uuid}")

        for uuid, value in gpu_memory_mb.items():
            if uuid not in device_uuids:
                raise RuntimeError(f"unknown gpu_memory_mb device uuid: {uuid}")
            if isinstance(value, bool) or not isinstance(value, int):
                raise RuntimeError(f"gpu_memory_mb must be integer for device uuid: {uuid}")
            if value < 0:
                raise RuntimeError(f"gpu_memory_mb must be >= 0 for device uuid: {uuid}")

    def __post_init__(self):
        if len(self.gpu_memory_mb) != 0:
            raise RuntimeError("gpu_memory_mb should not be provided in Job constructor, it will be initialized based on devices.")
        # Initialize memory map by devices and preserve provided values.
        for device in self.devices:
            self.gpu_memory_mb[device.uuid] = device.memory_size_mb
        Job.verify_values(
            job_id=self.job_id,
            base_url=self.base_url,
            devices=self.devices,
            gpu_memory_mb=self.gpu_memory_mb,
            priority=self.priority,
            timestamp=self.timestamp,
        )


class WorkloadBase:
    _instance: ClassVar[Optional["WorkloadBase"]] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
        resource: Resource,
        jobs: Optional[List[Job]] = None,
        trace_events: Optional[bool] = None,
    ):
        if self._initialized:
            self.resource = resource
            return

        self.resource: Resource = resource
        self.jobs: List[Job] = []
        self.job_map: Dict[str, List[Job]] = {}
        self._lock = threading.RLock()
        if jobs:
            for job in jobs:
                self.jobs.append(job)
                self._add_job_to_map(job)
        self._initialized = True

    @classmethod
    def get_instance(cls, resource: Resource) -> "WorkloadBase":
        return cls(resource)

    def reset(self):
        """Clear all jobs while keeping singleton instance."""
        with self._lock:
            self.jobs.clear()
            self.job_map.clear()
        self._emit_trace("workload_reset")

    def configure_trace_events(self, enabled: bool):
        _ = enabled

    def get_trace_events(self, clear: bool = False) -> list[dict[str, object]]:
        _ = clear
        return []

    def _emit_trace(self, event_type: str, **payload: object) -> None:
        _ = (event_type, payload)

    def _add_job_to_map(self, job: Job):
        for device in job.devices:
            self.job_map.setdefault(device.uuid, []).append(job)

    def _remove_job_from_map(self, job: Job):
        for device in job.devices:
            jobs_on_device = self.job_map.get(device.uuid)
            if not jobs_on_device:
                continue
            self.job_map[device.uuid] = [j for j in jobs_on_device if j.job_id != job.job_id]
            if not self.job_map[device.uuid]:
                del self.job_map[device.uuid]

    def add_job(self, job: Job) -> bool:
        """Add a job to the workload if resource requirements are met."""
        with self._lock:
            for device in job.devices:
                resource_device = self.resource.get_device_by_uuid(device.uuid)
                if resource_device is None:
                    WorkloadError.log_error(
                        WorkloadError.DEVICE_NOT_FOUND,
                        job_id=job.job_id,
                        device_uuid=device.uuid,
                    )
                    self._emit_trace(
                        "workload_job_rejected",
                        reason="device_not_found",
                        job_id=job.job_id,
                        device_uuid=device.uuid,
                    )
                    return False

                requested_memory = job.gpu_memory_mb.get(device.uuid)
                if requested_memory is None or requested_memory < 0:
                    WorkloadError.log_error(
                        WorkloadError.INVALID_REQUESTED_MEMORY,
                        job_id=job.job_id,
                        device_uuid=device.uuid,
                        requested_memory=requested_memory,
                    )
                    self._emit_trace(
                        "workload_job_rejected",
                        reason="invalid_requested_memory",
                        job_id=job.job_id,
                        device_uuid=device.uuid,
                        requested_memory=requested_memory,
                    )
                    return False

                current_usage = self.get_total_memory_usage_on_device(resource_device)
                if current_usage + requested_memory > resource_device.memory_size_mb:
                    WorkloadError.log_error(
                        WorkloadError.INSUFFICIENT_MEMORY,
                        job_id=job.job_id,
                        device_uuid=device.uuid,
                        current_usage_mb=current_usage,
                        requested_mb=requested_memory,
                        capacity_mb=resource_device.memory_size_mb,
                    )
                    self._emit_trace(
                        "workload_job_rejected",
                        reason="insufficient_memory",
                        job_id=job.job_id,
                        device_uuid=device.uuid,
                        current_usage_mb=current_usage,
                        requested_mb=requested_memory,
                        capacity_mb=resource_device.memory_size_mb,
                    )
                    return False

            self.jobs.append(job)
            try:
                self._add_job_to_map(job)
            except Exception:
                self.jobs.pop()
                self._remove_job_from_map(job)
                raise

            logger.debug(
                "Accept add_job: job_id=%s devices=%s",
                job.job_id,
                [device.uuid for device in job.devices],
            )
            self._emit_trace(
                "workload_job_added",
                job_id=job.job_id,
                devices=[device.uuid for device in job.devices],
                priority=job.priority,
            )
            return True

    def get_jobs_on_device(self, device: Device, sort: bool = False) -> List[Job]:
        with self._lock:
            jobs = list(self.job_map.get(device.uuid, []))
        if sort:
            # FCFS: earlier timestamp first.
            jobs.sort(key=lambda job: job.timestamp)
        return jobs

    def get_total_memory_usage_on_device(self, device: Device) -> int:
        """Compute total GPU memory usage for all jobs on the specified device."""
        with self._lock:
            return sum(
                job.gpu_memory_mb.get(device.uuid, 0)
                for job in self.job_map.get(device.uuid, [])
            )

    def debug_print(self) -> dict:
        """Print and return current workload snapshot for debugging."""
        with self._lock:
            snapshot = {
                "job_count": len(self.jobs),
                "jobs": [
                    {
                        "job_id": job.job_id,
                        "base_url": job.base_url,
                        "priority": job.priority,
                        "devices": [device.uuid for device in job.devices],
                        "gpu_memory_mb": dict(job.gpu_memory_mb),
                    }
                    for job in self.jobs
                ],
                "job_map": {
                    device_uuid: [job.job_id for job in jobs]
                    for device_uuid, jobs in self.job_map.items()
                },
            }
        logger.info("debug workload snapshot=%s", snapshot)
        return snapshot

    def remove_job(self, job_id: str) -> bool:
        with self._lock:
            target = next((job for job in self.jobs if job.job_id == job_id), None)
            if target is None:
                return False

            previous_jobs = list(self.jobs)
            previous_job_map = {uuid: list(jobs) for uuid, jobs in self.job_map.items()}

            self.jobs = [j for j in self.jobs if j.job_id != job_id]
            try:
                self._remove_job_from_map(target)
            except Exception:
                self.jobs = previous_jobs
                self.job_map = previous_job_map
                raise

            self._emit_trace(
                "workload_job_removed",
                job_id=job_id,
                removed=True,
            )

            return True


class Workload(WorkloadBase):
    _instance: ClassVar[Optional["Workload"]] = None

    def __init__(
        self,
        resource: Resource,
        jobs: Optional[List[Job]] = None,
        trace_events: Optional[bool] = None,
    ):
        super().__init__(resource=resource, jobs=jobs, trace_events=trace_events)
        if not hasattr(self, "trace_events_enabled"):
            self.trace_events_enabled = bool(trace_events)
        elif trace_events is not None:
            self.trace_events_enabled = bool(trace_events)
        if not hasattr(self, "_trace_started_at_monotonic"):
            self._trace_started_at_monotonic = time.monotonic()
        if not hasattr(self, "_trace_events"):
            self._trace_events: list[dict[str, object]] = []

    @classmethod
    def get_instance(cls, resource: Resource) -> "Workload":
        return cls(resource)

    def configure_trace_events(self, enabled: bool):
        self.trace_events_enabled = bool(enabled)

    def get_trace_events(self, clear: bool = False) -> list[dict[str, object]]:
        events = list(self._trace_events)
        if clear:
            self._trace_events.clear()
        return events

    def _emit_trace(self, event_type: str, **payload: object) -> None:
        if not self.trace_events_enabled:
            return
        event: dict[str, object] = {
            "component": "workload",
            "event": event_type,
            "t": round(time.monotonic() - self._trace_started_at_monotonic, 4),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        event.update(payload)
        self._trace_events.append(event)