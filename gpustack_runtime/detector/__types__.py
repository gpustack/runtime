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
