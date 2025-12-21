from __future__ import annotations

import logging

from .. import envs
from ..logging import debug_log_exception
from .__types__ import (
    Detector,
    Device,
    Devices,
    ManufacturerEnum,
    Topology,
    backend_to_manufacturer,
    manufacturer_to_backend,
    reduce_devices_distances,
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

logger = logging.getLogger(__package__)

_DETECTORS: list[Detector] = [
    AMDDetector(),
    AscendDetector(),
    CambriconDetector(),
    HygonDetector(),
    IluvatarDetector(),
    MetaXDetector(),
    MThreadsDetector(),
    NVIDIADetector(),
]
"""
List of all detectors.
"""

_DETECTORS_MAP: dict[ManufacturerEnum, Detector] = {
    det.manufacturer: det for det in _DETECTORS
}
"""
Mapping from manufacturer to detector.
"""


def supported_list() -> list[Detector]:
    """
    Return supported detectors.

    Returns:
        A list of supported detectors.

    """
    return [det for det in _DETECTORS if det.is_supported()]


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

    for det in _DETECTORS:
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

    Raises:
        If detection fails for the target detector specified by the `GPUSTACK_RUNTIME_DETECT` environment variable.

    """
    devices: Devices = []

    for det in _DETECTORS:
        if not det.is_supported():
            continue

        try:
            if devs := det.detect():
                devices.extend(devs)
            if fast and devices:
                return devices
        except Exception:
            detect_target = envs.GPUSTACK_RUNTIME_DETECT.lower()
            if detect_target == det.name:
                raise
            debug_log_exception(logger, "Failed to detect devices for %s", det.name)

    return devices


def get_devices_topologies(
    devices: Devices | None = None,
    fast: bool = True,
) -> list[Topology]:
    """
    Get the topology information of the given devices.

    Args:
        devices:
            A list of devices to get the topology information from.
            If None, detects devices automatically.
        fast:
            If True, return topologies from the first supported detector.
            Otherwise, return topologies from all supported detectors.

    Returns:
        A list of Topology objects for each manufacturer group.

    """
    if devices is None:
        devices = detect_devices(fast=fast)

    topologies: list[Topology] = []

    # Group devices by manufacturer.
    group_devices = group_devices_by_manufacturer(devices)
    if not group_devices:
        return topologies

    # Get topology for each group.
    for manu, devs in group_devices.items():
        det = _DETECTORS_MAP.get(manu)
        if det is not None:
            topo = det.get_topology(devs)
            if topo:
                topologies.append(topo)
            if fast and topologies:
                return topologies

    return topologies


def group_devices_by_manufacturer(
    devices: Devices | None,
) -> dict[ManufacturerEnum, Devices]:
    """
    Group devices by their manufacturer.

    Args:
        devices:
            A list of devices to be grouped.

    Returns:
        A dictionary mapping each manufacturer to its corresponding list of devices.

    """
    group_devices: dict[ManufacturerEnum, Devices] = {}
    for dev in devices or []:
        if dev.manufacturer not in group_devices:
            group_devices[dev.manufacturer] = []
        group_devices[dev.manufacturer].append(dev)
    return group_devices


__all__ = [
    "Device",
    "Devices",
    "ManufacturerEnum",
    "Topology",
    "backend_to_manufacturer",
    "detect_backend",
    "detect_devices",
    "get_devices_topologies",
    "group_devices_by_manufacturer",
    "manufacturer_to_backend",
    "reduce_devices_distances",
    "supported_backends",
    "supported_list",
    "supported_manufacturers",
]
