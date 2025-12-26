from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

from dataclasses_json import dataclass_json


class ManufacturerEnum(str, Enum):
    """
    Enum for Manufacturers.
    """

    AMD = "amd"
    """
    Advanced Micro Devices, Inc.
    """
    ASCEND = "ascend"
    """
    Huawei Technologies Co., Ltd.
    """
    CAMBRICON = "cambricon"
    """
    Cambricon Technologies Corporation Limited
    """
    HYGON = "hygon"
    """
    Chengdu Higon Integrated Circuit Design Co., Ltd.
    """
    ILUVATAR = "iluvatar"
    """
    Shanghai Iluvatar CoreX Semiconductor Co., Ltd.
    """
    METAX = "metax"
    """
    MetaX Integrated Circuits (Shanghai) Co., Ltd.
    """
    MTHREADS = "mthreads"
    """
    Moore Threads Technology Co.,Ltd
    """
    NVIDIA = "nvidia"
    """
    NVIDIA Corporation
    """
    UNKNOWN = "unknown"
    """
    Unknown Manufacturer
    """

    def __str__(self):
        return self.value


_MANUFACTURER_BACKEND_MAPPING: dict[ManufacturerEnum, str] = {
    ManufacturerEnum.AMD: "rocm",
    ManufacturerEnum.ASCEND: "cann",
    ManufacturerEnum.CAMBRICON: "neuware",
    ManufacturerEnum.HYGON: "dtk",
    ManufacturerEnum.ILUVATAR: "corex",
    ManufacturerEnum.METAX: "maca",
    ManufacturerEnum.MTHREADS: "musa",
    ManufacturerEnum.NVIDIA: "cuda",
}
"""
Mapping of manufacturer to runtime backend,
which should map to the gpustack-runner's backend names.
"""


def manufacturer_to_backend(manufacturer: ManufacturerEnum) -> str:
    """
    Convert manufacturer to runtime backend,
    e.g., NVIDIA -> cuda, AMD -> rocm.

    This is used to determine the appropriate runtime backend
    based on the device manufacturer.

    Args:
        manufacturer: The manufacturer of the device.

    Returns:
        The corresponding runtime backend.
        Return "unknown" if the manufacturer is unknown.

    """
    backend = _MANUFACTURER_BACKEND_MAPPING.get(manufacturer)
    if backend:
        return backend
    return ManufacturerEnum.UNKNOWN.value


def backend_to_manufacturer(backend: str) -> ManufacturerEnum:
    """
    Convert runtime backend to manufacturer,
    e.g., cuda -> NVIDIA, rocm -> AMD.

    This is used to determine the device manufacturer
    based on the runtime backend.

    Args:
        backend: The runtime backend.

    Returns:
        The corresponding manufacturer.
        Return ManufacturerEnum.Unknown if the backend is unknown.

    """
    for manufacturer, mapped_backend in _MANUFACTURER_BACKEND_MAPPING.items():
        if mapped_backend == backend:
            return manufacturer
    return ManufacturerEnum.UNKNOWN


def supported_manufacturers() -> list[ManufacturerEnum]:
    """
    Get a list of supported manufacturers.

    Returns:
        A list of supported manufacturers.

    """
    return list(_MANUFACTURER_BACKEND_MAPPING.keys())


def supported_backends() -> list[str]:
    """
    Get a list of supported backends.

    Returns:
        A list of supported backends.

    """
    return list(_MANUFACTURER_BACKEND_MAPPING.values())


@dataclass_json
@dataclass
class Device:
    """
    Device information.
    """

    manufacturer: ManufacturerEnum = ManufacturerEnum.UNKNOWN
    """
    Manufacturer of the device.
    """
    index: int = 0
    """
    Index of the device.
    If GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY is set to 1,
    this will be the physical index of the device.
    Otherwise, it will be the logical index of the device.
    Physical index is adapted to non-virtualized devices.
    """
    name: str = ""
    """
    Name of the device.
    """
    uuid: str = ""
    """
    UUID of the device.
    """
    driver_version: str | None = None
    """
    Driver version of the device.
    """
    runtime_version: str | None = None
    """
    Runtime version in major[.minor] of the device.
    """
    runtime_version_original: str | None = None
    """
    Original runtime version string of the device.
    """
    compute_capability: str | None = None
    """
    Compute capability of the device.
    """
    cores: int | None = None
    """
    Total cores of the device.
    """
    cores_utilization: int | float = 0
    """
    Core utilization of the device in percentage.
    """
    memory: int = 0
    """
    Total memory of the device in MiB.
    """
    memory_used: int = 0
    """
    Used memory of the device in MiB.
    """
    memory_utilization: float = 0
    """
    Memory utilization of the device in percentage.
    """
    temperature: int | float | None = None
    """
    Temperature of the device in Celsius.
    """
    power: int | float | None = None
    """
    Power consumption of the device in Watts.
    """
    power_used: int | float | None = None
    """
    Used power of the device in Watts.
    """
    appendix: dict[str, Any] = None
    """
    Appendix information of the device.
    """


Devices = list[Device]
"""
A list of Device objects.
"""


@dataclass_json
@dataclass
class Topology:
    """
    Topology information between devices.
    """

    manufacturer: ManufacturerEnum
    """
    Manufacturer of the devices that this topology applies to.
    """
    devices_distances: list[list[int]]
    """
    A 2D list representing the distances between devices.
    The value at row i and column j represents the distance
    between device i and device j.
    """
    devices_cpu_affinities: list[str]
    """
    A list representing the CPU affinity associated with each device.
    The value at index i represents the CPU set for device i.
    """
    devices_numa_affinities: list[str]
    """
    A list representing the NUMA affinity associated with each device.
    The value at index i represents the Memory set for device i.
    """

    def __init__(
        self,
        manufacturer: ManufacturerEnum,
        devices_count: int,
    ):
        """
        Initialize the Topology object.

        Args:
            manufacturer:
                Manufacturer of the devices.
            devices_count:
                Count of devices in the topology.

        """
        self.manufacturer = manufacturer
        self.devices_distances = [[0] * devices_count for _ in range(devices_count)]
        self.devices_cpu_affinities = [""] * devices_count
        self.devices_numa_affinities = [""] * devices_count

    def stringify(self) -> list[list[str]]:
        """
        Stringify the devices distances and return the maximum width.

        Returns:
            A 2D list representing the devices distances with string values.

        """
        devices_count = len(self.devices_distances)
        devices_info: list[list[str]] = [[]] * devices_count
        for i in range(devices_count):
            devices_info[i] = [
                stringify_devices_distance(d) for d in self.devices_distances[i]
            ]
            devices_info[i] += [
                self.devices_cpu_affinities[i]
                if self.devices_cpu_affinities[i]
                else "N/A",
            ]
            devices_info[i] += [
                self.devices_numa_affinities[i]
                if self.devices_numa_affinities[i]
                else "N/A",
            ]
        return devices_info

    def get_affinities(
        self,
        device_indexes: list[int] | int,
        deduplicate: bool = True,
    ) -> tuple[list[str], list[str]]:
        """
        Get the CPU and NUMA affinities for the given device indexes.

        Args:
            device_indexes:
                A list of device indexes or a single device index.
                If an empty list is provided, return all affinities.
            deduplicate:
                Whether to deduplicate the affinities.
                If True, the returned lists will contain unique affinities only.

        Returns:
            A tuple containing:
            - A list contains the CPU affinities for the given device indexes.
            - A list contains the NUMA affinities for the given device indexes.

        """
        if isinstance(device_indexes, int):
            device_indexes = [device_indexes]

        cpu_affinities: list[str] = []
        numa_affinities: list[str] = []
        if not device_indexes:
            cpu_affinities.extend(self.devices_cpu_affinities)
            numa_affinities.extend(self.devices_numa_affinities)
        else:
            for index in sorted(set(device_indexes)):
                cpu_affinities.append(self.devices_cpu_affinities[index])
                numa_affinities.append(self.devices_numa_affinities[index])

        if deduplicate:
            cpu_affinities = list(set(cpu_affinities))
            numa_affinities = list(set(numa_affinities))
        return cpu_affinities, numa_affinities


class TopologyDistanceEnum(int, Enum):
    """
    Enum for Topology Distance Levels.
    """

    SELF = 0
    """
    Self connection.
    """
    LINK = 5
    """
    Connection traversing with High-Speed Link (e.g., AMD XGMI, Ascend HCCS, NVIDIA NVLink).
    """
    PIX = 10
    """
    Connection traversing at most a single PCIe bridge.
    """
    PXB = 20
    """
    Connection traversing multiple PCIe bridges (without traversing the PCIe Host Bridge).
    """
    PHB = 30
    """
    Connection traversing PCIe as well as a PCIe Host Bridge (typically the CPU).
    """
    NODE = 40
    """
    Connection traversing PCIe and the interconnect between NUMA nodes.
    """
    SYS = 50
    """
    Connection traversing PCIe as well as the SMP interconnect between NUMA nodes (e.g., QPI/UPI).
    """
    UNK = 100
    """
    Unknown connection.
    """


def stringify_devices_distance(distance: int) -> str:
    """
    Stringify the devices distance to a human-readable format.

    Args:
        distance:
            The distance between two devices.

    Returns:
        A string representing the distance.

    """
    match distance:
        case TopologyDistanceEnum.SELF:
            return "X"
        case TopologyDistanceEnum.LINK:
            return "LINK"
        case TopologyDistanceEnum.PIX:
            return "PIX"
        case TopologyDistanceEnum.PXB:
            return "PXB"
        case TopologyDistanceEnum.PHB:
            return "PHB"
        case TopologyDistanceEnum.NODE:
            return "NODE"
        case TopologyDistanceEnum.SYS:
            return "SYS"
        case _:
            return "N/A"


def reduce_devices_distances(
    devices_distances: list[list[int]],
) -> dict[int, list[int]]:
    """
    Reduce the devices distances and return a brief relationship mapping.

    The key of relationship mapping is the device index,
    and the value is device indexes sorted from near to far.

    For example, given 4 devices with the following devices distances:
    `[[0, 10, 20, 30], [10, 0, 15, 25], [20, 15, 0, 5], [30, 25, 5, 0]]`,
    the resulting relationship will be:
    `{0: [1, 2, 3], 1: [0, 2, 3], 2: [3, 1, 0], 3: [2, 1, 0]}`.

    Args:
        devices_distances:
            A 2D list representing the distances between devices.

    Returns:
        A dictionary representing the relationship.

    """
    result: dict[int, list[int]] = {}

    devices_count = len(devices_distances)
    for index in range(devices_count):
        device_indexes = list(range(devices_count))
        distances = zip(device_indexes, devices_distances[index], strict=False)
        sorted_distances = sorted(distances, key=lambda x: x[1])
        sorted_indexes = [device_index for device_index, _ in sorted_distances]
        result[index] = sorted_indexes[1:]

    return result


class Detector(ABC):
    """
    Base class for all detectors.
    """

    manufacturer: ManufacturerEnum = ManufacturerEnum.UNKNOWN

    @staticmethod
    @abstractmethod
    def is_supported() -> bool:
        """
        Check if the detector is supported on the current environment.

        Returns:
            True if supported, False otherwise.

        """
        raise NotImplementedError

    def __init__(self, manufacturer: ManufacturerEnum):
        self.manufacturer = manufacturer

    @property
    def backend(self) -> str:
        """
        The backend name of the detector, e.g., 'cuda', 'rocm'.
        """
        return manufacturer_to_backend(self.manufacturer)

    @property
    def name(self) -> str:
        """
        The name of the detector, e.g., 'nvidia', 'amd'.
        """
        return str(self.manufacturer)

    @abstractmethod
    def detect(self) -> Devices | None:
        """
        Detect devices and return a list of Device objects.

        Returns:
            A list of detected Device objects, or None if detection fails.

        """
        raise NotImplementedError

    def get_topology(self, devices: Devices | None = None) -> Topology | None:  # noqa: ARG002
        """
        Get the Topology object between the given devices.

        Args:
            devices:
                A list of Device objects.

        Returns:
            A Topology object, or None if not supported.

        """
        return None
