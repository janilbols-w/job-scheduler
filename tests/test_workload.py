import logging
from typing import Optional
import pytest

from job_scheduler.core.resource import Device, Resource
from job_scheduler.core.workload import Job, Workload, WorkloadError


logger = logging.getLogger(__name__)


def make_device(uuid: str, id: int, memory_size_mb: int) -> Device:
    return Device(
        uuid=uuid,
        host_name="node-1",
        id=id,
        type="GPU",
        vendor="NVIDIA",
        memory_size_mb=memory_size_mb,
    )


def make_resource_and_workload(devices: Optional[list[Device]] = None) -> tuple[Resource, Workload]:
    logger.info("Creating resource/workload test fixture with devices=%s", [d.uuid for d in devices] if devices else [])
    resource = Resource()
    resource.reset()
    if devices:
        resource.add_devices(devices)

    workload = Workload(resource)
    workload.reset()
    return resource, workload


def test_add_job_success_and_job_map_indexing():
    logger.info("Running test_add_job_success_and_job_map_indexing")
    d1 = make_device("gpu-1", 0, 16000)
    d2 = make_device("gpu-2", 1, 24000)

    resource, workload = make_resource_and_workload([d1, d2])
    job = Job(
        job_id="job-1",
        base_url="http://worker-a",
        devices=[d1, d2],
   )

    accepted = workload.add_job(job)
    logger.info("Job add result=%s job_map_keys=%s", accepted, list(workload.job_map.keys()))

    assert accepted is True
    assert len(workload.jobs) == 1
    assert workload.job_map["gpu-1"][0].job_id == "job-1"
    assert workload.job_map["gpu-2"][0].job_id == "job-1"


def test_add_job_rejects_when_device_not_found_in_resource():
    logger.info("Running test_add_job_rejects_when_device_not_found_in_resource")
    d1 = make_device("gpu-1", 0, 16000)
    missing = make_device("gpu-missing", 2, 16000)

    resource, workload = make_resource_and_workload([d1])
    job = Job(
        job_id="job-2",
        base_url="http://worker-b",
        devices=[d1, missing],
    )

    accepted = workload.add_job(job)
    logger.info("Job add result=%s", accepted)

    assert accepted is False
    assert len(workload.jobs) == 0
    assert "gpu-1" not in workload.job_map


def test_add_job_rejects_when_memory_exceeds_device_capacity():
    logger.info("Running test_add_job_rejects_when_memory_exceeds_device_capacity")
    d1 = make_device("gpu-1", 0, 8000)

    resource, workload = make_resource_and_workload([d1])
    existing = Job(
        job_id="job-existing",
        base_url="http://worker-c",
        devices=[d1],
    )
    assert workload.add_job(existing) is True

    incoming = Job(
        job_id="job-overflow",
        base_url="http://worker-d",
        devices=[d1],
    )

    accepted = workload.add_job(incoming)
    logger.info("Overflow add result=%s", accepted)

    assert accepted is False
    assert len(workload.jobs) == 1


def test_add_job_rejects_with_invalid_requested_memory_error_code(caplog):
    logger.info("Running test_add_job_rejects_with_invalid_requested_memory_error_code")
    d1 = make_device("gpu-1", 0, 16000)

    resource, workload = make_resource_and_workload([d1])
    job = Job(
        job_id="job-invalid-memory",
        base_url="http://worker-invalid",
        devices=[d1],
    )
    job.gpu_memory_mb = {}

    with caplog.at_level(logging.ERROR):
        accepted = workload.add_job(job)

    assert accepted is False
    assert len(workload.jobs) == 0
    assert WorkloadError.INVALID_REQUESTED_MEMORY in caplog.text


def test_get_jobs_on_device_sort_fcfs_by_timestamp():
    logger.info("Running test_get_jobs_on_device_sort_fcfs_by_timestamp")
    d1 = make_device("gpu-1", 0, 32000)

    resource, workload = make_resource_and_workload([d1])
    late = Job(
        job_id="job-late",
        base_url="http://worker-late",
        devices=[d1],
        timestamp=200.0,
    )
    early = Job(
        job_id="job-early",
        base_url="http://worker-early",
        devices=[d1],
        timestamp=100.0,
    )

    workload.job_map[d1.uuid] = [late, early]

    unsorted_jobs = workload.get_jobs_on_device(d1, sort=False)
    sorted_jobs = workload.get_jobs_on_device(d1, sort=True)
    logger.info("Unsorted order=%s sorted order=%s", [j.job_id for j in unsorted_jobs], [j.job_id for j in sorted_jobs])

    assert [j.job_id for j in unsorted_jobs] == ["job-late", "job-early"]
    assert [j.job_id for j in sorted_jobs] == ["job-early", "job-late"]


def test_get_total_memory_usage_on_device_uses_per_device_memory():
    logger.info("Running test_get_total_memory_usage_on_device_uses_per_device_memory")
    d1 = make_device("gpu-1", 0, 32000)
    d2 = make_device("gpu-2", 1, 32000)

    resource, workload = make_resource_and_workload([d1, d2])
    j1 = Job(
        job_id="job-1",
        base_url="http://worker-1",
        devices=[d1, d2],
    )
    j2 = Job(
        job_id="job-2",
        base_url="http://worker-2",
        devices=[d1],
    )

    workload.job_map[d1.uuid] = [j1, j2]
    workload.job_map[d2.uuid] = [j1]

    d1_total = workload.get_total_memory_usage_on_device(d1)
    d2_total = workload.get_total_memory_usage_on_device(d2)
    logger.info("Device totals: d1=%s d2=%s", d1_total, d2_total)
    assert d1_total == 64000
    assert d2_total == 32000


def test_remove_job_updates_jobs_and_job_map():
    logger.info("Running test_remove_job_updates_jobs_and_job_map")
    d1 = make_device("gpu-1", 0, 16000)
    d2 = make_device("gpu-2", 1, 16000)

    resource, workload = make_resource_and_workload([d1, d2])
    target = Job(
        job_id="job-remove",
        base_url="http://worker-rm",
        devices=[d1, d2],
    )
    keep = Job(
        job_id="job-keep",
        base_url="http://worker-keep",
        devices=[d1],
    )

    workload.jobs = [target, keep]
    workload.job_map = {
        "gpu-1": [target, keep],
        "gpu-2": [target],
    }

    removed = workload.remove_job("job-remove")
    logger.info("Remove result=%s remaining_jobs=%s", removed, [j.job_id for j in workload.jobs])

    assert removed is True
    assert [j.job_id for j in workload.jobs] == ["job-keep"]
    assert [j.job_id for j in workload.job_map["gpu-1"]] == ["job-keep"]
    assert "gpu-2" not in workload.job_map


def test_remove_job_returns_false_when_missing():
    logger.info("Running test_remove_job_returns_false_when_missing")
    resource, workload = make_resource_and_workload()
    assert workload.remove_job("missing-job") is False


def test_job_verify_values_accepts_valid_payload():
    d1 = make_device("gpu-1", 0, 16000)
    d2 = make_device("gpu-2", 1, 24000)

    Job.verify_values(
        job_id="job-ok",
        base_url="http://worker-ok",
        devices=[d1, d2],
        gpu_memory_mb={"gpu-1": 1000, "gpu-2": 2000},
        priority=3,
        timestamp=123.45,
    )


def test_job_verify_values_rejects_unknown_gpu_memory_uuid():
    d1 = make_device("gpu-1", 0, 16000)

    with pytest.raises(RuntimeError, match="unknown gpu_memory_mb device uuid"):
        Job.verify_values(
            job_id="job-bad",
            base_url="http://worker-bad",
            devices=[d1],
            gpu_memory_mb={"gpu-1": 1000, "gpu-x": 2000},
            priority=1,
            timestamp=1.0,
        )


def test_workload_debug_print_snapshot():
    logger.info("Running test_workload_debug_print_snapshot")
    d1 = make_device("gpu-1", 0, 16000)
    d2 = make_device("gpu-2", 1, 24000)

    _, workload = make_resource_and_workload([d1, d2])
    job = Job(
        job_id="job-debug",
        base_url="http://worker-debug",
        devices=[d1, d2],
        priority=7,
    )

    assert workload.add_job(job) is True
    snapshot = workload.debug_print()
    logger.info("debug snapshot=%s", snapshot)

    assert snapshot["job_count"] == 1
    assert "jobs" in snapshot
    assert "job_map" in snapshot
    assert snapshot["jobs"][0]["job_id"] == "job-debug"
    assert snapshot["jobs"][0]["base_url"] == "http://worker-debug"
    assert snapshot["jobs"][0]["priority"] == 7
    assert snapshot["jobs"][0]["devices"] == ["gpu-1", "gpu-2"]
    assert snapshot["job_map"]["gpu-1"] == ["job-debug"]
    assert snapshot["job_map"]["gpu-2"] == ["job-debug"]
