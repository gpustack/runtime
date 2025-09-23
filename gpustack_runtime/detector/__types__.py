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

    UNKNOWN = "unknown"
    """
    Unknown Manufacturer
    """
    NVIDIA = "nvidia"
    """
    NVIDIA Corporation
    """
    AMD = "amd"
    """
    Advanced Micro Devices, Inc.
    """
    ASCEND = "ascend"
    """
    Huawei Ascend
    """
    MTHREADS = "mthreads"
    """
    MThreads Technologies Co., Ltd.
    """
    HYGON = "hygon"
    """
    Hygon Information Technology Co., Ltd.
    """
    ILUVATAR = "iluvatar"
    """
    Iluvatar CoreX
    """
    CAMBRICON = "cambricon"
    """
    Cambricon Technologies Corporation Limited
    """


_MANUFACTURER_BACKEND_MAPPING: dict[ManufacturerEnum, str] = {
    ManufacturerEnum.NVIDIA: "cuda",
    ManufacturerEnum.AMD: "rocm",
    ManufacturerEnum.ASCEND: "cann",
    ManufacturerEnum.MTHREADS: "musa",
    ManufacturerEnum.HYGON: "dtk",
    ManufacturerEnum.ILUVATAR: "corex",
    ManufacturerEnum.CAMBRICON: "cnrt",
}
"""
Mapping of manufacturer to runtime backend.
"""


def manufacturer_to_backend(manufacturer: ManufacturerEnum) -> str | None:
    """
    Convert manufacturer to runtime backend,
    e.g., NVIDIA -> cuda, AMD -> rocm.

    This is used to determine the appropriate runtime backend
    based on the device manufacturer.

    Args:
        manufacturer: The manufacturer of the device.

    Returns:
        The corresponding runtime backend. None if the manufacturer is unknown.

    """
    return _MANUFACTURER_BACKEND_MAPPING.get(manufacturer)


def backend_to_manufacturer(backend: str) -> ManufacturerEnum | None:
    """
    Convert runtime backend to manufacturer,
    e.g., cuda -> NVIDIA, rocm -> AMD.

    This is used to determine the device manufacturer
    based on the runtime backend.

    Args:
        backend: The runtime backend.

    Returns:
        The corresponding manufacturer. None if the backend is unknown.

    """
    for manufacturer, mapped_backend in _MANUFACTURER_BACKEND_MAPPING.items():
        if mapped_backend == backend:
            return manufacturer
    return None


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
    driver_version: str = ""
    """
    Driver version of the device.
    """
    driver_version_tuple: list[int | str] | None = None
    """
    Driver version tuple of the device.
    None if `driver_version` is blank.
    """
    runtime_version: str = ""
    """
    Runtime version of the device.
    """
    runtime_version_tuple: list[int | str] | None = None
    """
    Runtime version tuple of the device.
    None if `runtime_version` is blank.
    """
    compute_capability: str = ""
    """
    Compute capability of the device.
    """
    compute_capability_tuple: list[int | str] | None = None
    """
    Compute capability tuple of the device.
    None if `compute_capability` is blank.
    """
    cores: int = 0
    """
    Total cores of the device.
    """
    cores_utilization: int = 0
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
    memory_utilization: int = 0
    """
    Memory utilization of the device in percentage.
    """
    temperature: int = 0
    """
    Temperature of the device in Celsius.
    """
    power: int = 0
    """
    Power consumption of the device in Watts.
    """
    power_used: int = 0
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
    def backend(self) -> str | None:
        """
        The backend name of the detector, e.g., 'cuda', 'rocm'.
        """
        return manufacturer_to_backend(self.manufacturer)

    @abstractmethod
    def detect(self) -> Devices | None:
        """
        Detect devices and return a list of Device objects.

        Returns:
            A list of detected Device objects, or None if detection fails.

        """
        raise NotImplementedError
