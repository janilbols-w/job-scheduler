import asyncio
import logging

from job_scheduler.resource import Resource
from job_scheduler.workload import Job, Workload


logger = logging.getLogger(__name__)


class Scheduler:
    """Long-running async scheduler that wakes queued jobs one by one."""

    def __init__(self, resource: Resource | None = None, workload: Workload | None = None):
        self.resource = resource if resource is not None else Resource.get_instance()
        self.workload = (
            workload if workload is not None else Workload.get_instance(self.resource)
        )
        self.jobs_to_wake_up: asyncio.Queue[Job | None] = asyncio.Queue()
        self._shutdown_event = asyncio.Event()
        self._worker_task: asyncio.Task | None = None

    async def start(self):
        if self._worker_task is not None and not self._worker_task.done():
            return

        self._shutdown_event.clear()
        self._worker_task = asyncio.create_task(self._main_loop())
        logger.info("started async worker")

    def add_job_to_wake(self, job: Job):
        """Add a job into wake-up queue without blocking request handlers."""
        self.jobs_to_wake_up.put_nowait(job)

    def add_job(self, job: Job) -> bool:
        """Synchronously add a job to workload state."""
        return self.workload.add_job(job)

    def remove_job(self, job_id: str) -> bool:
        """Synchronously remove a job from workload state."""
        return self.workload.remove_job(job_id)

    def get_resource(self) -> Resource:
        """Return scheduler resource reference."""
        return self.resource

    def get_workload(self) -> Workload:
        """Return scheduler workload reference."""
        return self.workload

    async def wake_up_job(self, job: Job) -> bool:
        """Try waking one job; return whether the attempt succeeded."""
        logger.info(
            "trying to wake job_id=%s device_count=%s",
            job.job_id,
            len(job.devices),
        )

        # TODO: Replace this with actual wake-up implementation.
        await asyncio.sleep(0)
        logger.info(
            "wake success job_id=%s device_count=%s",
            job.job_id,
            len(job.devices),
        )
        return True

    async def stop(self):
        if self._worker_task is None:
            return

        self._shutdown_event.set()
        self.jobs_to_wake_up.put_nowait(None)

        try:
            await asyncio.wait_for(self._worker_task, timeout=5)
        except asyncio.TimeoutError:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        self._worker_task = None

    async def _main_loop(self):
        logger.info("main loop running")

        while True:
            item = await self.jobs_to_wake_up.get()
            if item is None and self._shutdown_event.is_set():
                break
            if item is None:
                continue

            try:
                await self.wake_up_job(item)
            except Exception as exc:
                logger.exception("wake failed job_id=%s error=%s", item.job_id, exc)

        logger.info("main loop exiting")


async def main():
    scheduler = Scheduler()
    await scheduler.start()
    logger.info("running standalone async scheduler")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("stopping via KeyboardInterrupt")
    finally:
        await scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main())
