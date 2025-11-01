from __future__ import annotations

from .__types__ import (
    Detector,
    Device,
    Devices,
    ManufacturerEnum,
    backend_to_manufacturer,
    manufacturer_to_backend,
    supported_backends,
    supported_manufacturers,
)
from .amd import AMDDetector
from .ascend import AscendDetector
from .cambricon import CambriconDetector
from .hygon import HygonDetector
from .iluvatar import IluvatarDetector
from .metax import MetaXDetector
from .mthreads import MThreadsDetector
from .nvidia import NVIDIADetector

detectors: list[Detector] = [
    AMDDetector(),
    AscendDetector(),
    CambriconDetector(),
    HygonDetector(),
    IluvatarDetector(),
    MetaXDetector(),
    MThreadsDetector(),
    NVIDIADetector(),
]


def detect_backend(fast: bool = True) -> str | list[str]:
    """
    Detect all supported backend.

    Args:
        fast:
            If True, return the first detected backend.
            Otherwise, return a list of all detected backends.

    Returns:
        A string of the detected backend if `fast` is True and a backend is found.
        A list of detected backends if `fast` is False.

    """
    backends: list[str] = []

    for det in detectors:
        if not det.is_supported():
            continue

        if fast:
            return det.backend

        backends.append(det.backend)

    return backends


def detect_devices(fast: bool = True) -> Devices:
    """
    Detect all available devices.

    Args:
        fast:
            If True, return devices from the first supported detector.
            Otherwise, return devices from all supported detectors.

    Returns:
        A list of detected devices.
        Empty list if no devices are found.

    """
    devices: Devices = []

    for det in detectors:
        if not det.is_supported():
            continue

        if fast:
            return det.detect()

        if devs := det.detect():
            devices.extend(devs)

    return devices


__all__ = [
    "Device",
    "Devices",
    "ManufacturerEnum",
    "backend_to_manufacturer",
    "detect_backend",
    "detect_devices",
    "manufacturer_to_backend",
    "supported_backends",
    "supported_manufacturers",
]
