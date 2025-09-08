from __future__ import annotations

from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .nvidia import NVIDIADetector

detectors: list[Detector] = [
    NVIDIADetector(),
]


def detect_backend() -> str | None:
    """
    Detect the backend of the available devices.

    Returns:
        The name of the backend.
        None if no supported backend is found.

    """
    for det in detectors:
        if not det.is_supported():
            continue

        return det.backend

    return None


def detect_devices() -> Devices:
    """
    Detect all available devices.

    Returns:
        A list of detected devices.
        Empty list if no devices are found.

    """
    for det in detectors:
        if not det.is_supported():
            continue

        return det.detect()

    return []


__all__ = [
    "Device",
    "Devices",
    "ManufacturerEnum",
    "detect_backend",
    "detect_devices",
]
