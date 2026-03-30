import asyncio
import logging
from urllib import error
from unittest.mock import AsyncMock, patch

from job_scheduler.core.resource import Device, Resource
from job_scheduler.core.scheduler import Scheduler
from job_scheduler.core.workload import Job, Workload


logger = logging.getLogger(__name__)


class DummyResponse:
    def __init__(self, status: int):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeWorkload:
    def __init__(self):
        self.add_calls = []
        self.remove_calls = []

    def add_job(self, job: Job) -> bool:
        self.add_calls.append(job)
        return True

    def remove_job(self, job_id: str) -> bool:
        self.remove_calls.append(job_id)
        return True


class RejectingWorkload(FakeWorkload):
    def add_job(self, job: Job) -> bool:
        self.add_calls.append(job)
        return False


def make_device(uuid: str, id: int, memory_size_mb: int) -> Device:
    return Device(
        uuid=uuid,
        host_name="node-1",
        id=id,
        type="GPU",
        vendor="NVIDIA",
        memory_size_mb=memory_size_mb,
    )


def make_job(job_id: str, device: Device) -> Job:
    return Job(
        job_id=job_id,
        base_url="http://worker",
        devices=[device],
    )


def test_scheduler_add_job_delegates_to_workload():
    logger.info("Running test_scheduler_add_job_delegates_to_workload")
    fake_workload = FakeWorkload()
    scheduler = Scheduler(resource=Resource.get_instance(), workload=fake_workload)

    job = make_job("job-add", make_device("gpu-1", 0, 16000))
    result = scheduler.add_job(job)
    logger.info("add_job result=%s add_calls=%s", result, len(fake_workload.add_calls))

    assert result is True
    assert fake_workload.add_calls == [job]


def test_scheduler_add_job_rejects_duplicate_job_id_in_workload():
    logger.info("Running test_scheduler_add_job_rejects_duplicate_job_id_in_workload")

    resource = Resource.get_instance()
    resource.reset()
    device = make_device("gpu-1", 0, 16000)
    resource.add_device(device)

    workload = Workload.get_instance(resource)
    workload.reset()

    existing = make_job("job-dup", device)
    incoming = make_job("job-dup", device)
    assert workload.add_job(existing) is True

    scheduler = Scheduler(resource=resource, workload=workload)
    result = scheduler.add_job(incoming)
    snapshot = workload.debug_print()

    assert result is False
    assert snapshot["job_count"] == 1
    assert snapshot["jobs"][0]["job_id"] == "job-dup"


def test_scheduler_remove_job_delegates_to_workload():
    logger.info("Running test_scheduler_remove_job_delegates_to_workload")
    fake_workload = FakeWorkload()
    scheduler = Scheduler(resource=Resource.get_instance(), workload=fake_workload)

    result = scheduler.remove_job("job-rm")
    logger.info("remove_job result=%s remove_calls=%s", result, len(fake_workload.remove_calls))

    assert result is True
    assert fake_workload.remove_calls == ["job-rm"]


def test_scheduler_getters_return_bound_references():
    logger.info("Running test_scheduler_getters_return_bound_references")
    resource = Resource.get_instance()
    fake_workload = FakeWorkload()
    scheduler = Scheduler(resource=resource, workload=fake_workload)

    logger.info(
        "resource_id=%s scheduler_resource_id=%s workload_id=%s scheduler_workload_id=%s",
        id(resource),
        id(scheduler.get_resource()),
        id(fake_workload),
        id(scheduler.get_workload()),
    )

    assert scheduler.get_resource() is resource
    assert scheduler.get_workload() is fake_workload


def test_scheduler_stop_without_start_is_safe():
    logger.info("Running test_scheduler_stop_without_start_is_safe")

    async def scenario():
        scheduler = Scheduler(resource=Resource.get_instance(), workload=Workload.get_instance(Resource.get_instance()))
        logger.info("Stopping scheduler without start")
        await scheduler.stop()

    asyncio.run(scenario())


def test_scheduler_start_then_stop_clears_worker_task():
    logger.info("Running test_scheduler_start_then_stop_clears_worker_task")

    async def scenario():
        scheduler = Scheduler(resource=Resource.get_instance(), workload=Workload.get_instance(Resource.get_instance()))

        await scheduler.start()
        logger.info("worker_task_started=%s", scheduler._worker_task is not None)
        assert scheduler._worker_task is not None
        assert scheduler._worker_task.done() is False

        await scheduler.stop()
        logger.info("worker_task_after_stop=%s", scheduler._worker_task)
        assert scheduler._worker_task is None

    asyncio.run(scenario())


def test_scheduler_processes_wake_queue_in_order():
    logger.info("Running test_scheduler_processes_wake_queue_in_order")

    async def scenario():
        resource = Resource.get_instance()
        resource.reset()
        device = make_device("gpu-1", 0, 16000)
        resource.add_device(device)
        workload = Workload.get_instance(resource)
        workload.reset()

        scheduler = Scheduler(resource=resource, workload=workload)
        processed = []

        async def fake_wake(job: Job) -> bool:
            logger.info("fake_wake called job_id=%s", job.job_id)
            processed.append(job.job_id)
            await asyncio.sleep(0)
            return True

        scheduler.wake_up_job = fake_wake  # type: ignore[method-assign]

        await scheduler.start()
        scheduler.add_job_to_wake(make_job("job-1", device))
        scheduler.add_job_to_wake(make_job("job-2", device))

        for _ in range(100):
            if len(processed) == 2:
                break
            await asyncio.sleep(0.01)

        await scheduler.stop()
        logger.info("processed_order=%s", processed)

        assert processed == ["job-1", "job-2"]

    asyncio.run(scenario())


def test_add_job_to_wake_skips_when_job_exists_in_workload():
    logger.info("Running test_add_job_to_wake_skips_when_job_exists_in_workload")

    resource = Resource.get_instance()
    resource.reset()
    device = make_device("gpu-1", 0, 16000)
    resource.add_device(device)

    workload = Workload.get_instance(resource)
    workload.reset()
    existing = make_job("job-exists", device)
    assert workload.add_job(existing) is True

    scheduler = Scheduler(resource=resource, workload=workload)
    scheduler.add_job_to_wake(make_job("job-exists", device))

    assert scheduler.jobs_to_wake_up.qsize() == 0


def test_add_job_to_wake_skips_when_job_exists_in_pending_queue():
    logger.info("Running test_add_job_to_wake_skips_when_job_exists_in_pending_queue")

    scheduler = Scheduler(resource=Resource.get_instance(), workload=FakeWorkload())
    job = make_job("job-pending", make_device("gpu-1", 0, 16000))

    scheduler.add_job_to_wake(job)
    scheduler.add_job_to_wake(job)

    assert scheduler.jobs_to_wake_up.qsize() == 1


def test_scheduler_wake_failure_does_not_stop_loop():
    logger.info("Running test_scheduler_wake_failure_does_not_stop_loop")

    async def scenario():
        resource = Resource.get_instance()
        resource.reset()
        device = make_device("gpu-1", 0, 16000)
        resource.add_device(device)
        workload = Workload.get_instance(resource)
        workload.reset()

        scheduler = Scheduler(resource=resource, workload=workload)
        attempted = []
        succeeded = []

        async def flaky_wake(job: Job) -> bool:
            attempted.append(job.job_id)
            if job.job_id == "job-fail":
                raise RuntimeError("simulated wake failure")
            succeeded.append(job.job_id)
            await asyncio.sleep(0)
            return True

        scheduler.wake_up_job = flaky_wake  # type: ignore[method-assign]

        await scheduler.start()
        scheduler.add_job_to_wake(make_job("job-fail", device))
        scheduler.add_job_to_wake(make_job("job-ok", device))

        for _ in range(100):
            if "job-ok" in succeeded and len(attempted) >= 2:
                break
            await asyncio.sleep(0.01)

        await scheduler.stop()
        logger.info("attempted=%s succeeded=%s", attempted, succeeded)

        assert attempted[:2] == ["job-fail", "job-ok"]
        assert succeeded == ["job-ok"]

    asyncio.run(scenario())


def test_wake_up_job_skips_impl_when_add_job_rejected():
    logger.info("Running test_wake_up_job_skips_impl_when_add_job_rejected")

    async def scenario():
        workload = RejectingWorkload()
        scheduler = Scheduler(resource=Resource.get_instance(), workload=workload)
        job = make_job("job-reject", make_device("gpu-1", 0, 16000))

        called = {"impl": 0}

        async def fake_impl(_job: Job, timeout_seconds: float = 5.0, retry_times: int = 0) -> bool:
            called["impl"] += 1
            return True

        scheduler._impl_wake_up_job = fake_impl  # type: ignore[method-assign]

        result = await scheduler.wake_up_job(job)
        assert result is False
        assert called["impl"] == 0
        assert workload.add_calls == [job]

    asyncio.run(scenario())


def test_wake_up_job_rolls_back_when_impl_fails():
    logger.info("Running test_wake_up_job_rolls_back_when_impl_fails")

    async def scenario():
        workload = FakeWorkload()
        scheduler = Scheduler(resource=Resource.get_instance(), workload=workload)
        job = make_job("job-rollback", make_device("gpu-1", 0, 16000))

        async def fake_impl(_job: Job, timeout_seconds: float = 5.0, retry_times: int = 0) -> bool:
            return False

        scheduler._impl_wake_up_job = fake_impl  # type: ignore[method-assign]

        result = await scheduler.wake_up_job(job)
        assert result is False
        assert workload.add_calls == [job]
        assert workload.remove_calls == [job.job_id]

    asyncio.run(scenario())


def test_wake_up_job_adds_job_into_workload_debug_snapshot():
    logger.info("Running test_wake_up_job_adds_job_into_workload_debug_snapshot")

    async def scenario():
        resource = Resource.get_instance()
        resource.reset()
        device = make_device("gpu-1", 0, 16000)
        resource.add_device(device)
        workload = Workload.get_instance(resource)
        workload.reset()

        scheduler = Scheduler(resource=resource, workload=workload)
        job = make_job("job-added", device)

        async def fake_impl(_job: Job, timeout_seconds: float = 5.0, retry_times: int = 0) -> bool:
            return True

        scheduler._impl_wake_up_job = fake_impl  # type: ignore[method-assign]

        result = await scheduler.wake_up_job(job)
        snapshot = workload.debug_print()
        logger.info("wake result=%s snapshot=%s", result, snapshot)

        assert result is True
        assert snapshot["job_count"] == 1
        assert snapshot["jobs"][0]["job_id"] == "job-added"
        assert snapshot["job_map"]["gpu-1"] == ["job-added"]

    asyncio.run(scenario())


def test_impl_wake_up_job_retries_then_succeeds():
    logger.info("Running test_impl_wake_up_job_retries_then_succeeds")

    async def scenario():
        scheduler = Scheduler(resource=Resource.get_instance(), workload=FakeWorkload())
        job = make_job("job-retry", make_device("gpu-1", 0, 16000))

        calls = {"count": 0}

        def fake_urlopen(req, timeout):
            calls["count"] += 1
            if calls["count"] == 1:
                raise error.URLError("temporary failure")
            assert req.full_url.endswith("/wake_up")
            assert req.data == b""
            return DummyResponse(200)

        with patch("job_scheduler.core.scheduler.request.urlopen", side_effect=fake_urlopen):
            result = await scheduler._impl_wake_up_job(job, timeout_seconds=1.5, retry_times=1)

        assert result is True
        assert calls["count"] == 2

    asyncio.run(scenario())


def test_impl_wake_up_job_uses_min_timeout_and_exhausts_retries():
    logger.info("Running test_impl_wake_up_job_uses_min_timeout_and_exhausts_retries")

    async def scenario():
        scheduler = Scheduler(resource=Resource.get_instance(), workload=FakeWorkload())
        job = make_job("job-timeout", make_device("gpu-1", 0, 16000))

        captured_timeouts = []

        def fake_urlopen(_req, timeout):
            captured_timeouts.append(timeout)
            raise error.URLError("connection refused")

        with patch("job_scheduler.core.scheduler.request.urlopen", side_effect=fake_urlopen):
            result = await scheduler._impl_wake_up_job(job, timeout_seconds=0, retry_times=2)

        assert result is False
        assert len(captured_timeouts) == 3
        assert all(timeout == 0.1 for timeout in captured_timeouts)

    asyncio.run(scenario())


def test_debug_mode_overrides_impl_methods_without_patching_originals():
    logger.info("Running test_debug_mode_overrides_impl_methods_without_patching_originals")

    scheduler = Scheduler(
        resource=Resource.get_instance(),
        workload=FakeWorkload(),
        debug_mode=True,
    )

    assert scheduler._impl_wake_up_job.__func__ is Scheduler._debug_impl_wake_up_job
    assert scheduler._impl_sleep_job.__func__ is Scheduler._debug_impl_sleep_job


def test_debug_impl_wake_up_job_returns_true_after_clamped_sleep():
    logger.info("Running test_debug_impl_wake_up_job_returns_true_after_clamped_sleep")

    async def scenario():
        scheduler = Scheduler(
            resource=Resource.get_instance(),
            workload=FakeWorkload(),
            debug_mode=True,
        )
        job = make_job("job-debug-wake", make_device("gpu-1", 0, 16000))

        mocked_sleep = AsyncMock(return_value=None)
        with patch("job_scheduler.core.scheduler.asyncio.sleep", mocked_sleep):
            result = await scheduler._impl_wake_up_job(job, timeout_seconds=0)

        assert result is True
        mocked_sleep.assert_awaited_once_with(0.1)

    asyncio.run(scenario())


def test_debug_impl_sleep_job_returns_true_after_requested_sleep():
    logger.info("Running test_debug_impl_sleep_job_returns_true_after_requested_sleep")

    async def scenario():
        scheduler = Scheduler(
            resource=Resource.get_instance(),
            workload=FakeWorkload(),
            debug_mode=True,
        )
        job = make_job("job-debug-sleep", make_device("gpu-1", 0, 16000))

        mocked_sleep = AsyncMock(return_value=None)
        with patch("job_scheduler.core.scheduler.asyncio.sleep", mocked_sleep):
            result = await scheduler._impl_sleep_job(job, timeout_seconds=0.25)

        assert result is True
        mocked_sleep.assert_awaited_once_with(0.25)

    asyncio.run(scenario())


def test_sleep_jobs_sleeps_victim_and_removes_from_workload():
    logger.info("Running test_sleep_jobs_sleeps_victim_and_removes_from_workload")

    async def scenario():
        resource = Resource.get_instance()
        resource.reset()
        device = make_device("gpu-1", 0, 16000)
        resource.add_device(device)

        workload = Workload.get_instance(resource)
        workload.reset()
        scheduler = Scheduler(resource=resource, workload=workload)

        victim = make_job("job-victim", device)
        assert workload.add_job(victim) is True

        slept = {"count": 0}

        async def fake_sleep(_job: Job, timeout_seconds: float = 5.0, retry_times: int = 0) -> bool:
            slept["count"] += 1
            return True

        scheduler._impl_sleep_job = fake_sleep  # type: ignore[method-assign]

        result = await scheduler.sleep_jobs([device])
        snapshot = workload.debug_print()
        logger.info("sleep_jobs result=%s snapshot=%s", result, snapshot)

        assert result is True
        assert slept["count"] == 1
        assert snapshot["job_count"] == 0

    asyncio.run(scenario())


def test_sleep_jobs_fails_when_no_candidates():
    logger.info("Running test_sleep_jobs_fails_when_no_candidates")

    async def scenario():
        resource = Resource.get_instance()
        resource.reset()
        device = make_device("gpu-1", 0, 16000)
        resource.add_device(device)

        # Request more memory than capacity so scheduler must try sleeping candidates.
        # With no running jobs, sleep_jobs should fail.
        required_vdevice = make_device("gpu-1", 0, 20000)

        workload = Workload.get_instance(resource)
        workload.reset()
        scheduler = Scheduler(resource=resource, workload=workload)

        result = await scheduler.sleep_jobs([required_vdevice])
        logger.info("sleep_jobs result=%s", result)
        assert result is False

    asyncio.run(scenario())


def test_sleep_jobs_runs_vdevices_in_parallel():
    logger.info("Running test_sleep_jobs_runs_vdevices_in_parallel")

    async def scenario():
        resource = Resource.get_instance()
        resource.reset()

        device_1 = make_device("gpu-1", 0, 16000)
        device_2 = make_device("gpu-2", 1, 16000)
        resource.add_device(device_1)
        resource.add_device(device_2)

        workload = Workload.get_instance(resource)
        workload.reset()
        scheduler = Scheduler(resource=resource, workload=workload)

        victim_1 = make_job("job-victim-1", device_1)
        victim_2 = make_job("job-victim-2", device_2)
        assert workload.add_job(victim_1) is True
        assert workload.add_job(victim_2) is True

        required_1 = make_device("gpu-1", 0, 12000)
        required_2 = make_device("gpu-2", 1, 12000)

        entered = asyncio.Event()
        gate = asyncio.Event()
        call_count = {"n": 0}

        async def fake_sleep(_job: Job, timeout_seconds: float = 5.0, retry_times: int = 0) -> bool:
            call_count["n"] += 1
            if call_count["n"] == 2:
                entered.set()
            await gate.wait()
            return True

        scheduler._impl_sleep_job = fake_sleep  # type: ignore[method-assign]

        task = asyncio.create_task(scheduler.sleep_jobs([required_1, required_2]))

        await asyncio.wait_for(entered.wait(), timeout=1.0)
        gate.set()

        result = await asyncio.wait_for(task, timeout=1.0)
        assert result is True
        assert call_count["n"] == 2

    asyncio.run(scenario())
