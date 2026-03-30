from dataclasses import dataclass, asdict
from typing import ClassVar, Optional
import logging


logger = logging.getLogger(__name__)

@dataclass
class Device:
    uuid: str           # e.g. GPU-e8f92875-827a-d4fa-1c81-1e1a66a5207f
    host_name: str      # e.g. 192.168.1.3 or node-1
    id: int             # local_id
    type: str           # e.g. A100, V100
    vendor: str         # e.g. NVIDIA, AMD
    memory_size_mb: int # e.g. 40960

    def __post_init__(self):
        if not self.uuid:
            raise RuntimeError("UUID is required")
        if not self.host_name:
            raise RuntimeError("host_name is required")
        if not self.type:
            raise RuntimeError("type is required")
        if not self.vendor:
            raise RuntimeError("vendor is required")

        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise RuntimeError("id must be an integer")
        if self.id < 0:
            raise RuntimeError("id must be >= 0")

        if isinstance(self.memory_size_mb, bool) or not isinstance(self.memory_size_mb, int):
            raise RuntimeError("memory_size_mb must be an integer")
        if self.memory_size_mb <= 0:
            raise RuntimeError("memory_size_mb must be > 0")

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_request_json(cls, data: dict):
        """Create Device instance from request JSON payload."""
        required_keys = [
            'uuid',
            'host_name',
            'id',
            'type',
            'vendor',
            'memory_size_mb',
        ]
        missing = [k for k in required_keys if k not in data]
        if missing:
            raise RuntimeError(f"Missing required fields in request JSON: {', '.join(missing)}")

        return cls(
            uuid=data['uuid'],
            host_name=data['host_name'],
            id=int(data['id']),
            type=data['type'],
            vendor=data['vendor'],
            memory_size_mb=int(data['memory_size_mb']),
        )

class Resource:
    """Manages device resources from the system."""

    _instance: ClassVar[Optional["Resource"]] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, devices: Optional[list[Device]] = None):
        if self._initialized:
            return

        self.devices: list[Device] = []
        self.device_map: dict[str, Device] = {}
        self.device_host_id_map: dict[tuple[str, int], Device] = {}
        if devices:
            self.add_devices(devices)
        self._initialized = True

    @classmethod
    def get_instance(cls) -> "Resource":
        return cls()

    def reset(self):
        """Clear all tracked devices while keeping singleton instance."""
        self.devices.clear()
        self.device_map.clear()
        self.device_host_id_map.clear()

    def add_device(self, device: Device):
        """Add a device to the resource manager."""
        if device.uuid in self.device_map:
            raise RuntimeError(f"Duplicate device uuid: {device.uuid}")

        host_id_key = (device.host_name, device.id)
        if host_id_key in self.device_host_id_map:
            raise RuntimeError(
                f"Duplicate (host_name, id) pair: ({device.host_name}, {device.id})"
            )

        self.devices.append(device)
        self.device_map[device.uuid] = device
        self.device_host_id_map[host_id_key] = device


    def add_devices(self, devices: list[Device]):
        """Add devices to the resource manager."""
        for device in devices:
            self.add_device(device)

    def get_device_info(self, host_name: str, id: int):
        """Get info for a specific device by unique (host_name, id) key."""
        device = self.device_host_id_map.get((host_name, id))
        if device is not None:
            return device.to_dict()
        return None

    def get_all_devices(self):
        """Get all device resources."""
        return [device.to_dict() for device in self.devices]

    def debug_print(self) -> dict:
        """Print and return current resource snapshot for debugging."""
        hosts: dict[str, dict[int, dict[str, object]]] = {}
        for device in self.devices:
            host_devices = hosts.setdefault(device.host_name, {})
            host_devices[device.id] = {
                "uuid": device.uuid,
                "type": device.type,
                "vendor": device.vendor,
                "memory_size_mb": device.memory_size_mb,
            }

        device_count = len(self.devices)
        snapshot = {
            "device_count": device_count,
            "hosts": hosts,
        }
        logger.info("debug resource snapshot=%s", snapshot)
        return snapshot

    def get_device_by_uuid(self, uuid: str) -> Optional[Device]:
        """Get a device by its UUID."""
        return self.device_map.get(uuid)

    def remove_device(self, uuid: str) -> bool:
        """Remove a device by its UUID. Returns True if removed, False if not found."""
        if uuid in self.device_map:
            device = self.device_map.pop(uuid)
            self.devices.remove(device)
            self.device_host_id_map.pop((device.host_name, device.id), None)
            return True
        return False