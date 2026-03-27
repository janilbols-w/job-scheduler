from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from job_scheduler.resource import Device, Resource
from job_scheduler.scheduler_core import Scheduler
from job_scheduler.workload import Job

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


@app.post("/wake_up")
async def wake_up(request: JobRequest):
    """Receive a wake request and enqueue it into scheduler wake-up queue."""
    job = request.to_workload_job()
    scheduler.add_job_to_wake(job)

    return JSONResponse(
        content={
            "status": "scheduled",
            "endpoint": "/wake_up",
            "job_id": request.job_id,
            "base_url": request.base_url,
            "priority": request.priority,
            "devices": [device.dict() for device in request.devices],
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
            "devices": [device.dict() for device in request.devices],
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

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_service:app", host="0.0.0.0", port=8000, log_level="info")
