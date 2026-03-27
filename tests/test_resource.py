import pytest
import logging

from job_scheduler.resource import Device, Resource


logger = logging.getLogger(__name__)


def device_payload(host_name="node-1", id=0, uuid="123e4567-e89b-12d3-a456-426655440000", type="GPU", vendor="NVIDIA", memory_size_mb=16384):
    return {
        "uuid": uuid,
        "host_name": host_name,
        "id": id,
        "type": type,
        "vendor": vendor,
        "memory_size_mb": memory_size_mb,
    }


@pytest.fixture(autouse=True)
def reset_resource_singleton():
    # Resource is a singleton; clear state between tests for isolation.
    logger.info("Resetting Resource singleton before test")
    Resource().reset()
    yield
    logger.info("Resetting Resource singleton after test")
    Resource().reset()


def test_device_from_request_json_success():
    logger.info("Running test_device_from_request_json_success")
    d = Device.from_request_json(device_payload())

    logger.info("Device created: %s", d)
    assert d.uuid == "123e4567-e89b-12d3-a456-426655440000"
    assert d.host_name == "node-1"
    assert d.id == 0
    assert d.type == "GPU"
    assert d.vendor == "NVIDIA"
    assert d.memory_size_mb == 16384


def test_device_from_request_json_missing_field():
    logger.info("Running test_device_from_request_json_missing_field")
    payload = device_payload()
    payload.pop("vendor")
    logger.info("Payload after pop: %s", payload)

    with pytest.raises(RuntimeError, match="Missing required fields"):
        Device.from_request_json(payload)


def test_resource_add_and_query():
    logger.info("Running test_resource_add_and_query")
    resource = Resource()
    resource.add_device(Device.from_request_json(device_payload(id=1, uuid="123e4567-e89b-12d3-a456-426655440001")))

    info = resource.get_device_info("node-1", 1)
    assert info["id"] == 1
    assert info["host_name"] == "node-1"

    all_devices = resource.get_all_devices()
    logger.info("All devices: %s", all_devices)
    assert len(all_devices) == 1


def test_resource_singleton_instance_identity():
    logger.info("Running test_resource_singleton_instance_identity")
    r1 = Resource()
    r2 = Resource()
    r3 = Resource.get_instance()
    logger.info("Resource instances: r1=%s r2=%s r3=%s", id(r1), id(r2), id(r3))

    assert r1 is r2
    assert r2 is r3


def test_get_device_by_uuid_and_remove_device():
    logger.info("Running test_get_device_by_uuid_and_remove_device")
    resource = Resource()
    d = Device.from_request_json(device_payload(id=2, uuid="123e4567-e89b-12d3-a456-426655440002"))
    resource.add_device(d)
    logger.info("Added device uuid=%s", d.uuid)

    found = resource.get_device_by_uuid(d.uuid)
    assert found is not None
    assert found.uuid == d.uuid

    removed = resource.remove_device(d.uuid)
    logger.info("Removed device uuid=%s result=%s", d.uuid, removed)
    assert removed is True
    assert resource.get_device_by_uuid(d.uuid) is None


def test_remove_device_returns_false_when_missing():
    logger.info("Running test_remove_device_returns_false_when_missing")
    resource = Resource()
    assert resource.remove_device("does-not-exist") is False


def test_add_device_rejects_duplicate_uuid():
    logger.info("Running test_add_device_rejects_duplicate_uuid")
    resource = Resource()
    d1 = Device.from_request_json(device_payload(id=0, uuid="dup-uuid", host_name="node-1"))
    d2 = Device.from_request_json(device_payload(id=1, uuid="dup-uuid", host_name="node-2"))

    resource.add_device(d1)
    with pytest.raises(RuntimeError, match="Duplicate device uuid"):
        resource.add_device(d2)


def test_add_device_rejects_duplicate_host_id_pair():
    logger.info("Running test_add_device_rejects_duplicate_host_id_pair")
    resource = Resource()
    d1 = Device.from_request_json(device_payload(id=3, uuid="uuid-a", host_name="node-1"))
    d2 = Device.from_request_json(device_payload(id=3, uuid="uuid-b", host_name="node-1"))

    resource.add_device(d1)
    with pytest.raises(RuntimeError, match="Duplicate \(host_name, id\) pair"):
        resource.add_device(d2)


def test_resource_debug_print_hosts_shape():
    resource = Resource()
    d1 = Device.from_request_json(device_payload(host_name="node-a", id=2, uuid="uuid-2"))
    d2 = Device.from_request_json(device_payload(host_name="node-a", id=0, uuid="uuid-0"))
    d3 = Device.from_request_json(device_payload(host_name="node-b", id=1, uuid="uuid-1"))

    resource.add_device(d1)
    resource.add_device(d2)
    resource.add_device(d3)

    snapshot = resource.debug_print()

    assert snapshot["device_count"] == 3
    assert "hosts" in snapshot
    assert snapshot["hosts"]["node-a"][2]["uuid"] == "uuid-2"
    assert snapshot["hosts"]["node-a"][0]["type"] == "GPU"
    assert snapshot["hosts"]["node-b"][1]["vendor"] == "NVIDIA"

