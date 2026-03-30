import argparse
import asyncio
import json
import logging
import random
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from job_scheduler.api.api_client import JobClient


logger = logging.getLogger(__name__)


class MockJob:
    """Mock worker that serves /wake_up and /sleep and periodically self-enqueues wake requests."""

    def __init__(
        self,
        scheduler_url: str,
        job_id: str,
        listen_host: str,
        listen_port: int,
        devices: list[dict[str, Any]],
        priority: int = 5,
        mean_wakeup_seconds: float = 8.0,
        action_success_rate: float = 0.8,
        init_delay_seconds: float = 0.0,
    ):
        self.scheduler_url = scheduler_url
        self.job_id = job_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.base_url = f"http://{listen_host}:{listen_port}"
        self.devices = devices
        self.priority = int(priority)
        self.mean_wakeup_seconds = max(0.1, float(mean_wakeup_seconds))
        self.action_success_rate = max(0.0, min(1.0, float(action_success_rate)))
        self.init_delay_seconds = max(0.0, float(init_delay_seconds))
        self.status = "asleep"

        self.job_client = JobClient(
            scheduler_base_url=self.scheduler_url,
            job_id=self.job_id,
            base_url=self.base_url,
            devices=self.devices,
            priority=self.priority,
        )

        self._wake_task: asyncio.Task | None = None
        self.app = self._build_app()
        logger.info(
            "mock_job initialized job_id=%s base_url=%s status=%s",
            self.job_id,
            self.base_url,
            self.status,
        )

    def _random_action_response(self, action: str) -> JSONResponse:
        ok = random.random() < self.action_success_rate
        status_code = 200 if ok else 503
        previous_status = self.status
        if ok and action == "wake_up":
            self.status = "awake"
        elif ok and action == "sleep":
            self.status = "asleep"

        if not ok:
            logger.warning(
                "mock_job random reject job_id=%s action=%s base_url=%s status=%s success_rate=%s",
                self.job_id,
                action,
                self.base_url,
                self.status,
                self.action_success_rate,
            )
        else:
            logger.info(
                "mock_job action accepted job_id=%s action=%s status_transition=%s->%s",
                self.job_id,
                action,
                previous_status,
                self.status,
            )
        return JSONResponse(
            status_code=status_code,
            content={
                "ok": ok,
                "job_id": self.job_id,
                "action": action,
                "base_url": self.base_url,
                "status": self.status,
            },
        )

    async def _periodic_wake_step(self, interval: float) -> None:
        if self.status == "awake":
            logger.info(
                "mock_job periodic wake skipped job_id=%s status=%s interval=%.2fs",
                self.job_id,
                self.status,
                interval,
            )
            return

        try:
            response = await asyncio.to_thread(self.job_client.wake_up)
            logger.info(
                "mock_job periodic wake requested job_id=%s interval=%.2fs response=%s",
                self.job_id,
                interval,
                json.dumps(response, ensure_ascii=True),
            )
        except Exception as exc:
            logger.exception(
                "mock_job periodic wake failed job_id=%s interval=%.2fs error=%s",
                self.job_id,
                interval,
                exc,
            )

    async def _wake_up_loop(self) -> None:
        if self.init_delay_seconds > 0:
            await asyncio.sleep(self.init_delay_seconds)
        # Poisson process: inter-arrival times are exponential.
        rate = 1.0 / self.mean_wakeup_seconds
        while True:
            interval = random.expovariate(rate)
            await asyncio.sleep(interval)
            await self._periodic_wake_step(interval)

    def _build_app(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(_: FastAPI):
            self._wake_task = asyncio.create_task(self._wake_up_loop())
            try:
                yield
            finally:
                if self._wake_task is not None:
                    self._wake_task.cancel()
                    try:
                        await self._wake_task
                    except asyncio.CancelledError:
                        pass
                    self._wake_task = None

        app = FastAPI(title=f"MockJob-{self.job_id}", lifespan=lifespan)

        @app.post("/wake_up")
        async def wake_up() -> JSONResponse:
            return self._random_action_response("wake_up")

        @app.post("/sleep")
        async def sleep() -> JSONResponse:
            return self._random_action_response("sleep")

        @app.get("/health")
        async def health() -> JSONResponse:
            return JSONResponse(
                content={
                    "ok": True,
                    "job_id": self.job_id,
                    "base_url": self.base_url,
                }
            )

        return app

    def run(self) -> None:
        uvicorn.run(
            self.app,
            host=self.listen_host,
            port=self.listen_port,
            log_level="info",
        )


def parse_devices_json(value: str) -> list[dict[str, Any]]:
    raw = json.loads(value)
    if not isinstance(raw, list):
        raise ValueError("devices-json must be a JSON list")
    devices: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Each device must be an object")
        devices.append(
            {
                "uuid": str(item["uuid"]),
                "host_name": str(item.get("host_name", "host-mock")),
                "id": int(item.get("id", 0)),
                "type": str(item.get("type", "GPU")),
                "vendor": str(item.get("vendor", "NVIDIA")),
                "memory_size_mb": int(item.get("memory_size_mb", 8000)),
            }
        )
    return devices


def build_devices_from_uuids(uuids: list[str], memory_size_mb: int) -> list[dict[str, Any]]:
    devices = []
    for i, uuid in enumerate(uuids):
        devices.append(
            {
                "uuid": uuid,
                "host_name": f"host-{i // 2}",
                "id": i,
                "type": "GPU",
                "vendor": "NVIDIA",
                "memory_size_mb": memory_size_mb,
            }
        )
    return devices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a mock job worker service")
    parser.add_argument("--scheduler-url", default="http://127.0.0.1:8000")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--priority", type=int, default=5)
    parser.add_argument("--mean-wakeup-seconds", type=float, default=8.0)
    parser.add_argument("--action-success-rate", type=float, default=0.8)
    parser.add_argument(
        "--init-delay-seconds",
        type=float,
        default=0.0,
        help="Delay before the first periodic wake_up attempt",
    )
    parser.add_argument(
        "--device-uuids",
        default="",
        help="Comma-separated device UUIDs (used when --devices-json is not provided)",
    )
    parser.add_argument(
        "--device-memory-mb",
        type=int,
        default=8000,
        help="Default memory for uuids provided via --device-uuids",
    )
    parser.add_argument(
        "--devices-json",
        default="",
        help="Full JSON list of devices for JobClient payload",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()

    if args.devices_json:
        devices = parse_devices_json(args.devices_json)
    else:
        uuids = [part.strip() for part in args.device_uuids.split(",") if part.strip()]
        if not uuids:
            raise RuntimeError("Provide --devices-json or at least one UUID in --device-uuids")
        devices = build_devices_from_uuids(uuids, args.device_memory_mb)

    worker = MockJob(
        scheduler_url=args.scheduler_url,
        job_id=args.job_id,
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        devices=devices,
        priority=args.priority,
        mean_wakeup_seconds=args.mean_wakeup_seconds,
        action_success_rate=args.action_success_rate,
        init_delay_seconds=args.init_delay_seconds,
    )
    worker.run()


if __name__ == "__main__":
    main()
