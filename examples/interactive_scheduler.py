import asyncio
import logging
import re
from typing import List

from job_scheduler.core.resource import Device, Resource
from job_scheduler.core.scheduler import Scheduler
from job_scheduler.core.workload import Job, Workload


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("interactive_scheduler")


async def ainput(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


def parse_non_empty(value: str, field_name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} cannot be empty")
    return value


def parse_int(value: str, field_name: str, min_value: int | None = None) -> int:
    parsed = int(value.strip())
    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field_name} must be >= {min_value}")
    return parsed


def get_next_device_id(resource: Resource, host_name: str) -> int:
    host_ids = [device.id for device in resource.devices if device.host_name == host_name]
    if not host_ids:
        return 0
    return max(host_ids) + 1


def get_next_device_uuid(resource: Resource) -> str:
    pattern = re.compile(r"^d-(\d+)$")
    indices: list[int] = []

    for device in resource.devices:
        match = pattern.match(device.uuid)
        if match:
            indices.append(int(match.group(1)))

    next_index = (max(indices) + 1) if indices else 0
    return f"d-{next_index}"


async def choose_devices(resource: Resource, allow_virtual_requirements: bool = False) -> List[Device]:
    print("Select devices one-by-one. Press Enter on UUID when done.")
    if allow_virtual_requirements:
        print(
            "Virtual requirement mode: device id may be non-existent and memory_size_mb may exceed resource capacity."
        )

    selected: list[Device] = []
    seen: set[str] = set()

    while True:
        uuid = (await ainput("device UUID (empty to finish): ")).strip()
        if not uuid:
            if selected:
                return selected
            print("Select at least one device.")
            continue

        if uuid in seen:
            print(f"Device already selected: {uuid}")
            continue

        resource_device = resource.get_device_by_uuid(uuid)
        if resource_device is None and not allow_virtual_requirements:
            print(f"Unknown device UUID: {uuid}")
            continue

        default_host = resource_device.host_name if resource_device else "host_main"
        default_id = resource_device.id if resource_device else 0
        default_type = resource_device.type if resource_device else "GPU"
        default_vendor = resource_device.vendor if resource_device else "NVIDIA"
        default_memory = resource_device.memory_size_mb if resource_device else 8000

        host_input = (await ainput(f"host_name (default: {default_host}): ")).strip()
        device_id_input = (await ainput(f"device id (default: {default_id}): ")).strip()
        type_input = (await ainput(f"type (default: {default_type}): ")).strip()
        vendor_input = (await ainput(f"vendor (default: {default_vendor}): ")).strip()
        memory_raw = (
            await ainput(
                f"requested memory_size_mb (default: {default_memory}): "
            )
        ).strip()

        host_name = host_input if host_input else default_host
        if not device_id_input:
            device_id = default_id
        else:
            try:
                device_id = parse_int(device_id_input, "id", 0)
            except Exception as exc:
                print(f"Invalid id: {exc}")
                continue

        dtype = type_input if type_input else default_type
        vendor = vendor_input if vendor_input else default_vendor

        if not memory_raw:
            requested_memory = default_memory
        else:
            try:
                requested_memory = parse_int(memory_raw, "memory_size_mb", 1)
            except Exception as exc:
                print(f"Invalid memory_size_mb: {exc}")
                continue

        if (not allow_virtual_requirements) and resource_device is not None and requested_memory > resource_device.memory_size_mb:
            print(
                f"Requested memory {requested_memory} exceeds capacity {resource_device.memory_size_mb} for {uuid}"
            )
            continue

        # Keep uuid as the scheduling key while allowing virtual requirement overrides.
        vdevice = Device(
            uuid=uuid,
            host_name=host_name,
            id=device_id,
            type=dtype,
            vendor=vendor,
            memory_size_mb=requested_memory,
        )
        selected.append(vdevice)
        seen.add(uuid)
        print(
            f"Added vdevice uuid={uuid} host={host_name} id={device_id} requested_memory_mb={requested_memory}"
        )


async def create_device(resource: Resource):
    try:
        next_uuid = get_next_device_uuid(resource)
        uuid_input = (await ainput(f"device uuid (default: {next_uuid}): ")).strip()
        uuid = uuid_input if uuid_input else next_uuid
        host_name_input = (await ainput("host name (default: host_main): ")).strip()
        host_name = host_name_input if host_name_input else "host_main"
        next_id = get_next_device_id(resource, host_name)
        device_id_input = (await ainput(f"device id (default: {next_id}): ")).strip()
        dtype_input = (await ainput("device type (default: GPU): ")).strip()
        vendor_input = (await ainput("vendor (default: NVIDIA): ")).strip()
        memory_input = (await ainput("memory_size_mb (default: 8000): ")).strip()

        if not device_id_input:
            local_id = next_id
        else:
            local_id = parse_int(device_id_input, "id", 0)
        dtype = dtype_input if dtype_input else "GPU"
        vendor = vendor_input if vendor_input else "NVIDIA"
        if not memory_input:
            memory_size_mb = 8000
        else:
            memory_size_mb = parse_int(memory_input, "memory_size_mb", 1)

        device = Device(
            uuid=uuid,
            host_name=host_name,
            id=local_id,
            type=dtype,
            vendor=vendor,
            memory_size_mb=memory_size_mb,
        )
        resource.add_device(device)
        print(f"Added device: {uuid}")
    except Exception as exc:
        print(f"Failed to add device: {exc}")


async def start_scheduler_if_needed(
    resource: Resource,
    workload: Workload,
    scheduler: Scheduler | None,
) -> Scheduler:
    if scheduler is None:
        scheduler = Scheduler(resource=resource, workload=workload, debug_mode=True)
        print("Created scheduler in debug mode")

    await scheduler.start()
    print("Scheduler started")
    return scheduler


async def add_job_via_scheduler(resource: Resource, scheduler: Scheduler | None):
    if scheduler is None:
        print("Start scheduler first")
        return

    if not resource.devices:
        print("No devices available. Add devices first.")
        return

    try:
        job_id = parse_non_empty(await ainput("job id: "), "job_id")
        default_base_url = f"http://{job_id}"
        base_url_input = (await ainput(f"base_url (default: {default_base_url}): ")).strip()
        base_url = base_url_input if base_url_input else default_base_url
        print("Available device UUIDs:")
        for device in resource.devices:
            print(f"- {device.uuid} ({device.host_name}:{device.id}, {device.memory_size_mb}MB)")

        devices = await choose_devices(resource, allow_virtual_requirements=True)
        job = Job(job_id=job_id, base_url=base_url, devices=devices)

        ok = scheduler.add_job(job)
        print(f"scheduler.add_job => {ok}")
    except Exception as exc:
        print(f"Failed to add job: {exc}")


async def remove_job_via_scheduler(scheduler: Scheduler | None):
    if scheduler is None:
        print("Start scheduler first")
        return

    try:
        workload = scheduler.get_workload()
        snapshot = workload.debug_print()
        valid_job_ids = [job["job_id"] for job in snapshot.get("jobs", [])]

        if not valid_job_ids:
            print("No active jobs to remove.")
            return

        print("Valid job IDs:")
        for job_id in valid_job_ids:
            print(f"- {job_id}")

        job_id = parse_non_empty(await ainput("job id to remove: "), "job_id")
        ok = scheduler.remove_job(job_id)
        print(f"scheduler.remove_job => {ok}")
    except Exception as exc:
        print(f"Failed to remove job: {exc}")


def show_state(resource: Resource, workload: Workload):
    resource_snapshot = resource.debug_print()
    workload_snapshot = workload.debug_print()

    print("Resource snapshot:")
    print(resource_snapshot)
    print("Workload snapshot:")
    print(workload_snapshot)


async def queue_wake_up_job(resource: Resource, scheduler: Scheduler | None):
    if scheduler is None:
        print("Start scheduler first")
        return

    if not resource.devices:
        print("No devices available. Add devices first.")
        return

    try:
        job_id = parse_non_empty(await ainput("job id to wake: "), "job_id")
        default_base_url = f"http://{job_id}"
        base_url_input = (await ainput(f"base_url (default: {default_base_url}): ")).strip()
        base_url = base_url_input if base_url_input else default_base_url

        print("Available device UUIDs:")
        for device in resource.devices:
            print(f"- {device.uuid} ({device.host_name}:{device.id}, {device.memory_size_mb}MB)")

        devices = await choose_devices(resource, allow_virtual_requirements=True)
        job = Job(job_id=job_id, base_url=base_url, devices=devices)

        scheduler.add_job_to_wake(job)
        print("Job enqueued for wake-up")
    except Exception as exc:
        print(f"Failed to queue wake-up job: {exc}")


def print_menu():
    print("\nInteractive Scheduler Demo")
    print("1) Create device into resource")
    print("2) Start scheduler (debug mode)")
    print("3) Add job via scheduler")
    print("4) Remove job via scheduler")
    print("5) Display current resource/workload")
    print("6) Wake up job by scheduler (enqueue)")
    print("7) Stop scheduler")
    print("0) Exit")


async def main():
    resource = Resource.get_instance()
    resource.reset()

    workload = Workload.get_instance(resource)
    workload.reset()

    scheduler: Scheduler | None = None

    print("Initialized clean Resource and Workload")

    while True:
        print_menu()
        choice = (await ainput("Select option: ")).strip()

        if choice == "1":
            await create_device(resource)
        elif choice == "2":
            scheduler = await start_scheduler_if_needed(resource, workload, scheduler)
        elif choice == "3":
            await add_job_via_scheduler(resource, scheduler)
        elif choice == "4":
            await remove_job_via_scheduler(scheduler)
        elif choice == "5":
            show_state(resource, workload)
        elif choice == "6":
            await queue_wake_up_job(resource, scheduler)
        elif choice == "7":
            if scheduler is None:
                print("Scheduler not created")
            else:
                await scheduler.stop()
                print("Scheduler stopped")
        elif choice == "0":
            break
        else:
            print("Unknown option")

    if scheduler is not None:
        await scheduler.stop()

    print("Bye")


if __name__ == "__main__":
    asyncio.run(main())
