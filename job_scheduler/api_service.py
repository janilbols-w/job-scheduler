from fastapi import FastAPI
from pydantic import BaseModel

from job_scheduler.scheduler_core import Scheduler

app = FastAPI(title="Wake Scheduler Service")

scheduler = Scheduler()
scheduler.start()

class WakeRequest(BaseModel):
    job_id: str
    detail: str | None = None
    gpus: list["GPUUsage"] | None = None

class GPUUsage(BaseModel):
    gpu_id: int
    used_memory_mb: int

class AwakeRequest(WakeRequest):
    pass

@app.post("/to_wake")
def to_wake(request: WakeRequest):
    """Receive a wake request with GPU usage and enqueue or process it."""
    scheduler.enqueue("to_wake", request.dict())

    return {
        "status": "scheduled",
        "job_id": request.job_id,
        "detail": request.detail,
        "endpoint": "/to_wake",
        "gpus": [
            {"gpu_id": gpu.gpu_id, "used_memory_mb": gpu.used_memory_mb}
            for gpu in request.gpus or []
        ],
    }

@app.post("/awake")
def awake(request: AwakeRequest):
    """Receive a wake confirmation or start signal with GPU usage payload."""
    scheduler.enqueue("awake", request.dict())

    return {
        "status": "awake",
        "job_id": request.job_id,
        "detail": request.detail,
        "endpoint": "/awake",
        "gpus": [
            {"gpu_id": gpu.gpu_id, "used_memory_mb": gpu.used_memory_mb}
            for gpu in request.gpus or []
        ],
    }

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_service:app", host="0.0.0.0", port=8000, log_level="info")
