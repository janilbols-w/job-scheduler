import asyncio
import logging
import time
from datetime import datetime, timezone
from urllib import error, request

from job_scheduler.core.resource import Device, Resource
from job_scheduler.core.workload import Job, Workload


logger = logging.getLogger(__name__)


class SchedulerBase:
    """Long-running async scheduler that wakes queued jobs one by one."""

    def __init__(
        self,
        resource: Resource | None = None,
        workload: Workload | None = None,
        debug_mode: bool = False,
        trace_events: bool = False,
    ):
        self.resource = resource if resource is not None else Resource.get_instance()
        self.workload = (
            workload if workload is not None else Workload.get_instance(self.resource)
        )
        self.debug_mode = debug_mode
        self.jobs_to_wake_up: asyncio.Queue[Job | None] = asyncio.Queue()
        self._jobs_to_wake_ids: set[str] = set()
        self._shutdown_event = asyncio.Event()
        self._worker_task: asyncio.Task | None = None
        _ = trace_events

        if self.debug_mode:
            logger.info("debug_mode enabled: override impl wake/sleep methods")
            self._impl_wake_up_job = self._debug_impl_wake_up_job  # type: ignore[method-assign]
            self._impl_sleep_job = self._debug_impl_sleep_job  # type: ignore[method-assign]

    async def start(self):
        if self._worker_task is not None and not self._worker_task.done():
            return

        self._shutdown_event.clear()
        self._worker_task = asyncio.create_task(self._main_loop())
        logger.info("started async worker")
        self._emit_trace("scheduler_started")

    def configure_trace_events(self, enabled: bool):
        _ = enabled

    def get_trace_events(self, clear: bool = False) -> list[dict[str, object]]:
        _ = clear
        return []

    def _emit_trace(self, event_type: str, **payload: object) -> None:
        _ = (event_type, payload)

    def _job_exists_in_workload(self, job_id: str) -> bool:
        jobs = getattr(self.workload, "jobs", None)
        if isinstance(jobs, list):
            return any(getattr(existing, "job_id", None) == job_id for existing in jobs)
        return False

    def add_job_to_wake(self, job: Job):
        """Add a job into wake-up queue without blocking request handlers."""
        if self._job_exists_in_workload(job.job_id):
            logger.warning(
                "skip enqueue: job_id already exists in workload job_id=%s",
                job.job_id,
            )
            self._emit_trace("scheduler_enqueue", job_id=job.job_id, enqueued=False, reason="already_in_workload", queue_size=self.jobs_to_wake_up.qsize())
            return

        if job.job_id in self._jobs_to_wake_ids:
            logger.warning(
                "skip enqueue: job_id already exists in jobs_to_wake_up job_id=%s",
                job.job_id,
            )
            self._emit_trace("scheduler_enqueue", job_id=job.job_id, enqueued=False, reason="already_in_queue", queue_size=self.jobs_to_wake_up.qsize())
            return

        self._jobs_to_wake_ids.add(job.job_id)
        self.jobs_to_wake_up.put_nowait(job)
        self._emit_trace("scheduler_enqueue", job_id=job.job_id, enqueued=True, queue_size=self.jobs_to_wake_up.qsize())

    def add_job(self, job: Job) -> bool:
        """Synchronously add a job to workload state."""
        if self._job_exists_in_workload(job.job_id):
            logger.warning(
                "reject add_job: job_id already exists in workload job_id=%s",
                job.job_id,
            )
            self._emit_trace("scheduler_add_job", job_id=job.job_id, added=False, reason="duplicate_workload")
            return False
        added = self.workload.add_job(job)
        self._emit_trace("scheduler_add_job", job_id=job.job_id, added=added)
        return added

    def remove_job(self, job_id: str) -> bool:
        """Synchronously remove a job from workload state."""
        removed = self.workload.remove_job(job_id)
        self._emit_trace("scheduler_remove_job", job_id=job_id, removed=removed)
        return removed

    def get_resource(self) -> Resource:
        """Return scheduler resource reference."""
        return self.resource

    def get_workload(self) -> Workload:
        """Return scheduler workload reference."""
        return self.workload

    def _debug_print_state(self, stage: str, job_id: str | None = None):
        queue_size = self.jobs_to_wake_up.qsize()
        logger.info(
            "scheduler_debug stage=%s job_id=%s queue_size=%s",
            stage,
            job_id,
            queue_size,
        )

        resource_debug = getattr(self.resource, "debug_print", None)
        if callable(resource_debug):
            try:
                resource_snapshot = resource_debug()
                # logger.info("scheduler_debug resource_snapshot=%s", resource_snapshot)
            except Exception as exc:
                logger.warning("scheduler_debug resource debug failed error=%s", exc)

        workload_debug = getattr(self.workload, "debug_print", None)
        if callable(workload_debug):
            try:
                workload_snapshot = workload_debug()
                # logger.info("scheduler_debug workload_snapshot=%s", workload_snapshot)
            except Exception as exc:
                logger.warning("scheduler_debug workload debug failed error=%s", exc)

    async def wake_up_job(self, job: Job) -> bool:
        """Try waking one job; return whether the attempt succeeded."""
        logger.info(
            "trying to wake job_id=%s device_count=%s",
            job.job_id,
            len(job.devices),
        )

        accepted = self.add_job(job)
        if not accepted:
            logger.info("add_job failed, trying sleep_jobs for job_id=%s", job.job_id)
            slept = await self.sleep_jobs(job.devices)
            if slept:
                accepted = self.add_job(job)
                logger.info(
                    "retry add_job after sleep_jobs job_id=%s accepted=%s",
                    job.job_id,
                    accepted,
                )

        if not accepted:
            logger.warning(
                "wake rejected before request (workload add failed) job_id=%s",
                job.job_id,
            )
            return False

        succeeded = await self._impl_wake_up_job(job)
        if succeeded:
            logger.info(
                "wake success job_id=%s device_count=%s",
                job.job_id,
                len(job.devices),
            )
        else:
            removed = self.remove_job(job.job_id)
            logger.warning(
                "wake rejected job_id=%s device_count=%s rollback_removed=%s",
                job.job_id,
                len(job.devices),
                removed,
            )
        return succeeded

    async def sleep_jobs(
        self,
        vdevices: list[Device],
        timeout_seconds: float = 5.0,
        retry_times: int = 0,
    ) -> bool:
        """Sleep running jobs as needed to satisfy incoming virtual device requirements."""
        required_uuids = [device.uuid for device in vdevices]
        logger.info("sleep_jobs start required_vdevices=%s", required_uuids)
        self._emit_trace("scheduler_sleep_start", required_vdevices=required_uuids)

        if not hasattr(self.workload, "get_jobs_on_device"):
            logger.warning("sleep_jobs unavailable: workload does not expose get_jobs_on_device")
            return False
        if not hasattr(self.workload, "get_total_memory_usage_on_device"):
            logger.warning("sleep_jobs unavailable: workload does not expose get_total_memory_usage_on_device")
            return False

        reserved_job_ids: set[str] = set()
        slept_job_ids: set[str] = set()
        reservation_lock = asyncio.Lock()

        async def _sleep_for_vdevice(vdevice: Device) -> bool:
            resource_device = self.resource.get_device_by_uuid(vdevice.uuid)
            if resource_device is None:
                logger.warning("sleep_jobs failed: device not found uuid=%s", vdevice.uuid)
                return False

            required_memory = max(0, int(vdevice.memory_size_mb))

            while True:
                current_usage = self.workload.get_total_memory_usage_on_device(resource_device)
                projected = current_usage + required_memory
                if projected <= resource_device.memory_size_mb:
                    return True

                jobs_on_device = self.workload.get_jobs_on_device(resource_device, sort=True)
                victim = None

                async with reservation_lock:
                    for candidate in jobs_on_device:
                        if candidate.job_id in reserved_job_ids:
                            continue
                        reserved_job_ids.add(candidate.job_id)
                        victim = candidate
                        break

                if victim is None:
                    logger.warning(
                        "sleep_jobs failed: no candidates left uuid=%s current_usage=%s required=%s capacity=%s",
                        vdevice.uuid,
                        current_usage,
                        required_memory,
                        resource_device.memory_size_mb,
                    )
                    return False

                logger.info(
                    "sleep_jobs sleeping victim_job_id=%s for uuid=%s",
                    victim.job_id,
                    vdevice.uuid,
                )
                ok = await self._impl_sleep_job(
                    victim,
                    timeout_seconds=timeout_seconds,
                    retry_times=retry_times,
                )
                if not ok:
                    async with reservation_lock:
                        reserved_job_ids.discard(victim.job_id)
                    logger.warning(
                        "sleep_jobs failed: _impl_sleep_job rejected victim_job_id=%s",
                        victim.job_id,
                    )
                    return False

                removed = self.remove_job(victim.job_id)
                async with reservation_lock:
                    reserved_job_ids.discard(victim.job_id)
                    if removed:
                        slept_job_ids.add(victim.job_id)

                if not removed:
                    # Another parallel worker may have removed the same job first.
                    await asyncio.sleep(0)
                    continue

        tasks = [asyncio.create_task(_sleep_for_vdevice(vdevice)) for vdevice in vdevices]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.exception("sleep_jobs failed with exception error=%s", result)
                return False
            if result is False:
                return False

        logger.info("sleep_jobs success slept_job_ids=%s", sorted(slept_job_ids))
        self._emit_trace("scheduler_sleep_done", success=True, slept_job_ids=sorted(slept_job_ids))
        return True

    async def _impl_wake_up_job(
        self,
        job: Job,
        timeout_seconds: float = 5.0,
        retry_times: int = 0,
    ) -> bool:
        """Wake a job by calling <base_url>/wake_up with configurable timeout/retries."""
        url = f"{job.base_url.rstrip('/')}/wake_up"

        timeout_seconds = max(0.1, float(timeout_seconds))
        retry_times = max(0, int(retry_times))
        total_attempts = retry_times + 1

        def _post() -> bool:
            req = request.Request(
                url,
                data=b"",
                method="POST",
            )
            with request.urlopen(req, timeout=timeout_seconds) as response:
                return 200 <= response.status < 300

        for attempt in range(1, total_attempts + 1):
            try:
                return await asyncio.to_thread(_post)
            except error.HTTPError as exc:
                logger.error(
                    "wake call failed with http error job_id=%s url=%s status=%s attempt=%s/%s",
                    job.job_id,
                    url,
                    exc.code,
                    attempt,
                    total_attempts,
                )
            except error.URLError as exc:
                logger.error(
                    "wake call failed with url error job_id=%s url=%s reason=%s attempt=%s/%s",
                    job.job_id,
                    url,
                    exc.reason,
                    attempt,
                    total_attempts,
                )

            if attempt < total_attempts:
                await asyncio.sleep(0)

        return False

    async def _debug_impl_wake_up_job(
        self,
        job: Job,
        timeout_seconds: float = 5.0,
        retry_times: int = 0,
    ) -> bool:
        timeout_seconds = max(0.1, float(timeout_seconds))
        logger.info(
            "debug wake success after timeout job_id=%s timeout=%s retry_times=%s",
            job.job_id,
            timeout_seconds,
            max(0, int(retry_times)),
        )
        await asyncio.sleep(timeout_seconds)
        return True

    async def _impl_sleep_job(
        self,
        job: Job,
        timeout_seconds: float = 5.0,
        retry_times: int = 0,
    ) -> bool:
        """Sleep a job by calling <base_url>/sleep with configurable timeout/retries."""
        url = f"{job.base_url.rstrip('/')}/sleep"

        timeout_seconds = max(0.1, float(timeout_seconds))
        retry_times = max(0, int(retry_times))
        total_attempts = retry_times + 1

        def _post() -> bool:
            req = request.Request(
                url,
                data=b"",
                method="POST",
            )
            with request.urlopen(req, timeout=timeout_seconds) as response:
                return 200 <= response.status < 300

        for attempt in range(1, total_attempts + 1):
            try:
                return await asyncio.to_thread(_post)
            except error.HTTPError as exc:
                logger.error(
                    "sleep call failed with http error job_id=%s url=%s status=%s attempt=%s/%s",
                    job.job_id,
                    url,
                    exc.code,
                    attempt,
                    total_attempts,
                )
            except error.URLError as exc:
                logger.error(
                    "sleep call failed with url error job_id=%s url=%s reason=%s attempt=%s/%s",
                    job.job_id,
                    url,
                    exc.reason,
                    attempt,
                    total_attempts,
                )

            if attempt < total_attempts:
                await asyncio.sleep(0)

        return False

    async def _debug_impl_sleep_job(
        self,
        job: Job,
        timeout_seconds: float = 5.0,
        retry_times: int = 0,
    ) -> bool:
        timeout_seconds = max(0.1, float(timeout_seconds))
        logger.info(
            "debug sleep success after timeout job_id=%s timeout=%s retry_times=%s",
            job.job_id,
            timeout_seconds,
            max(0, int(retry_times)),
        )
        await asyncio.sleep(timeout_seconds)
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
        self._emit_trace("scheduler_stopped")

    async def _main_loop(self):
        logger.info("main loop running")
        self._emit_trace("scheduler_loop_start")
        self._debug_print_state("loop_start")

        while True:
            item = await self.jobs_to_wake_up.get()
            current_job_id = None if item is None else item.job_id
            self._debug_print_state("dequeued", current_job_id)
            if item is None and self._shutdown_event.is_set():
                break
            if item is None:
                continue

            self._jobs_to_wake_ids.discard(item.job_id)
            self._emit_trace("scheduler_dequeued", job_id=item.job_id, queue_size=self.jobs_to_wake_up.qsize())

            try:
                ok = await self.wake_up_job(item)
                self._emit_trace("scheduler_wake_result", job_id=item.job_id, success=ok)
            except Exception as exc:
                logger.exception("wake failed job_id=%s error=%s", item.job_id, exc)
                self._emit_trace("scheduler_wake_result", job_id=item.job_id, success=False, error=str(exc))
            finally:
                self._debug_print_state("post_process", item.job_id)

        logger.info("main loop exiting")
        self._emit_trace("scheduler_loop_exit")
        self._debug_print_state("loop_exit")


class Scheduler(SchedulerBase):
    def __init__(
        self,
        resource: Resource | None = None,
        workload: Workload | None = None,
        debug_mode: bool = False,
        trace_events: bool = False,
    ):
        super().__init__(
            resource=resource,
            workload=workload,
            debug_mode=debug_mode,
            trace_events=trace_events,
        )
        if not hasattr(self, "trace_events_enabled"):
            self.trace_events_enabled = bool(trace_events)
        else:
            self.trace_events_enabled = bool(trace_events)
        if not hasattr(self, "_trace_started_at_monotonic"):
            self._trace_started_at_monotonic = time.monotonic()
        if not hasattr(self, "_trace_events"):
            self._trace_events: list[dict[str, object]] = []

        self._configure_child_trace(self.resource, self.trace_events_enabled)
        self._configure_child_trace(self.workload, self.trace_events_enabled)

    @staticmethod
    def _configure_child_trace(component: object, enabled: bool) -> None:
        configure = getattr(component, "configure_trace_events", None)
        if callable(configure):
            configure(bool(enabled))

    def configure_trace_events(self, enabled: bool):
        self.trace_events_enabled = bool(enabled)
        self._configure_child_trace(self.resource, self.trace_events_enabled)
        self._configure_child_trace(self.workload, self.trace_events_enabled)

    def get_trace_events(self, clear: bool = False) -> list[dict[str, object]]:
        events = list(self._trace_events)
        if clear:
            self._trace_events.clear()
        return events

    def _emit_trace(self, event_type: str, **payload: object) -> None:
        if not self.trace_events_enabled:
            return
        event: dict[str, object] = {
            "component": "scheduler",
            "event": event_type,
            "t": round(time.monotonic() - self._trace_started_at_monotonic, 4),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        event.update(payload)
        self._trace_events.append(event)


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
