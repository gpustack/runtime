from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from dataclasses_json import dataclass_json
from gpustack_runner import ManufacturerEnum, manufacturer_to_backend


@dataclass_json
@dataclass
class Device:
    """
    Device information.
    """

    manufacturer: ManufacturerEnum = ManufacturerEnum.UNKNOWN
    """
    Machine type of the device.
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

    def __init__(self, manufacturer: ManufacturerEnum):
        self.manufacturer = manufacturer

    @property
    def backend(self) -> str | None:
        """
        The backend name of the detector, e.g., 'cuda', 'rocm'.
        """
        return manufacturer_to_backend(self.manufacturer)

    @staticmethod
    @abstractmethod
    def is_supported() -> bool:
        """
        Check if the detector is supported on the current environment.

        Returns:
            True if supported, False otherwise.

        """
        raise NotImplementedError

    @abstractmethod
    def detect(self) -> Devices | None:
        """
        Detect devices and return a list of Device objects.

        Returns:
            A list of detected Device objects, or None if detection fails.

        """
        raise NotImplementedError
