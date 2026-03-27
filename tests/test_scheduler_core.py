import asyncio
import logging

from job_scheduler.resource import Device, Resource
from job_scheduler.scheduler_core import Scheduler
from job_scheduler.workload import Job, Workload


logger = logging.getLogger(__name__)


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
