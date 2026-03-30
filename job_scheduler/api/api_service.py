from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from job_scheduler.core.resource import Device, Resource
from job_scheduler.core.scheduler import Scheduler
from job_scheduler.core.workload import Job


logger = logging.getLogger(__name__)

resource = Resource.get_instance()
scheduler = Scheduler(resource=resource)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await scheduler.start()
    try:
        yield
    finally:
        await scheduler.stop()


app = FastAPI(title="Wake Scheduler Service", lifespan=lifespan)


class DeviceInfo(BaseModel):
    uuid: str
    host_name: str
    id: int
    type: str
    vendor: str
    memory_size_mb: int


class JobRequest(BaseModel):
    job_id: str
    base_url: str
    devices: list[DeviceInfo]
    priority: int = 5

    def to_workload_job(self) -> Job:
        job = Job(
            job_id=self.job_id,
            base_url=self.base_url,
            devices=[
                Device(
                    uuid=device.uuid,
                    host_name=device.host_name,
                    id=device.id,
                    type=device.type,
                    vendor=device.vendor,
                    memory_size_mb=device.memory_size_mb,
                )
                for device in self.devices
            ],
            priority=self.priority,
        )
        return job


class RemoveJobRequest(BaseModel):
    job_id: str


class TraceConfigRequest(BaseModel):
    enabled: bool = True


@app.get("/resource")
async def get_resource():
    """Return current resource snapshot."""
    return JSONResponse(
        content={
            "status": "ok",
            "endpoint": "/resource",
            "resource": scheduler.get_resource().debug_print(),
        }
    )


@app.get("/workload")
async def get_workload():
    """Return current workload snapshot."""
    return JSONResponse(
        content={
            "status": "ok",
            "endpoint": "/workload",
            "workload": scheduler.get_workload().debug_print(),
        }
    )


@app.post("/add_device")
async def add_device(request: DeviceInfo):
    """Register one physical device into Resource."""
    device = Device(
        uuid=request.uuid,
        host_name=request.host_name,
        id=request.id,
        type=request.type,
        vendor=request.vendor,
        memory_size_mb=request.memory_size_mb,
    )

    try:
        scheduler.get_resource().add_device(device)
    except Exception as exc:
        logger.warning("add_device rejected uuid=%s reason=%s", request.uuid, exc)
        return JSONResponse(
            content={
                "status": "rejected",
                "endpoint": "/add_device",
                "device": request.model_dump(),
                "message": str(exc),
            }
        )

    return JSONResponse(
        content={
            "status": "added",
            "endpoint": "/add_device",
            "device": request.model_dump(),
        }
    )


@app.post("/wake_up")
async def wake_up(request: JobRequest):
    """Receive a wake request and enqueue it into scheduler wake-up queue."""
    job = request.to_workload_job()

    queue_size_before = scheduler.jobs_to_wake_up.qsize()
    scheduler.add_job_to_wake(job)
    queue_size_after = scheduler.jobs_to_wake_up.qsize()
    enqueued = queue_size_after > queue_size_before

    return JSONResponse(
        content={
            "status": "scheduled" if enqueued else "ignored",
            "endpoint": "/wake_up",
            "job_id": request.job_id,
            "base_url": request.base_url,
            "priority": request.priority,
            "devices": [device.model_dump() for device in request.devices],
            "enqueued": enqueued,
            "queue_size": queue_size_after,
        }
    )


@app.post("/add_job")
async def add_job(request: JobRequest):
    """Add a job into scheduler workload state."""
    job = request.to_workload_job()
    added = scheduler.add_job(job)

    return JSONResponse(
        content={
            "status": "added" if added else "rejected",
            "endpoint": "/add_job",
            "job_id": request.job_id,
            "base_url": request.base_url,
            "priority": request.priority,
            "devices": [device.model_dump() for device in request.devices],
            "added": added,
        }
    )


@app.post("/remove_job")
async def remove_job(request: RemoveJobRequest):
    """Remove a job from scheduler workload state by job_id."""
    removed = scheduler.remove_job(request.job_id)

    return JSONResponse(
        content={
            "status": "removed" if removed else "not_found",
            "endpoint": "/remove_job",
            "job_id": request.job_id,
            "removed": removed,
        }
    )


@app.post("/trace/config")
async def set_trace_config(request: TraceConfigRequest):
    scheduler.configure_trace_events(request.enabled)
    return JSONResponse(
        content={
            "status": "ok",
            "endpoint": "/trace/config",
            "enabled": bool(request.enabled),
        }
    )


@app.get("/trace/events")
async def get_trace_events(clear: bool = False):
    scheduler_events = scheduler.get_trace_events(clear=clear)
    resource_events = scheduler.get_resource().get_trace_events(clear=clear)
    workload_events = scheduler.get_workload().get_trace_events(clear=clear)

    merged_events = list(scheduler_events) + list(resource_events) + list(workload_events)
    merged_events.sort(key=lambda item: str(item.get("ts", "")))

    return JSONResponse(
        content={
            "status": "ok",
            "endpoint": "/trace/events",
            "clear": clear,
            "scheduler": scheduler_events,
            "resource": resource_events,
            "workload": workload_events,
            "events": merged_events,
        }
    )

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("job_scheduler.api.api_service:app", host="0.0.0.0", port=8000, log_level="info")
