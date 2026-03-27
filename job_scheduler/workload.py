from dataclasses import dataclass, field
import time
from typing import ClassVar, Dict, List, Optional
import logging

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
    gpu_memory_mb: Dict[str, int]
    priority: int = 5
    timestamp: float = field(default_factory=time.time)


class Workload:
    _instance: ClassVar[Optional["Workload"]] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, resource: Resource, jobs: Optional[List[Job]] = None):
        if self._initialized:
            self.resource = resource
            return

        self.resource: Resource = resource
        self.jobs: List[Job] = []
        self.job_map: Dict[str, List[Job]] = {}
        if jobs:
            for job in jobs:
                self.jobs.append(job)
                self._add_job_to_map(job)
        self._initialized = True

    @classmethod
    def get_instance(cls, resource: Resource) -> "Workload":
        return cls(resource)

    def reset(self):
        """Clear all jobs while keeping singleton instance."""
        self.jobs.clear()
        self.job_map.clear()

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
        for device in job.devices:
            resource_device = self.resource.get_device_by_uuid(device.uuid)
            if resource_device is None:
                WorkloadError.log_error(
                    WorkloadError.DEVICE_NOT_FOUND,
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
                return False

        self.jobs.append(job)
        self._add_job_to_map(job)
        logger.debug(
            "Accept add_job: job_id=%s devices=%s",
            job.job_id,
            [device.uuid for device in job.devices],
        )
        return True

    def get_jobs_on_device(self, device: Device, sort: bool = False) -> List[Job]:
        jobs = list(self.job_map.get(device.uuid, []))
        if sort:
            # FCFS: earlier timestamp first.
            jobs.sort(key=lambda job: job.timestamp)
        return jobs

    def get_total_memory_usage_on_device(self, device: Device) -> int:
        """Compute total GPU memory usage for all jobs on the specified device."""
        return sum(job.gpu_memory_mb.get(device.uuid, 0) for job in self.job_map.get(device.uuid, []))

    def remove_job(self, job_id: str) -> bool:
        target = next((job for job in self.jobs if job.job_id == job_id), None)
        if target is None:
            return False

        self.jobs = [j for j in self.jobs if j.job_id != job_id]
        self._remove_job_from_map(target)
        return True