from __future__ import annotations as __future_annotations__

import contextlib
import logging
import re
from functools import lru_cache
from pathlib import Path

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pycndev
from .__types__ import (
    Detector,
    Device,
    DeviceMemoryStatusEnum,
    Devices,
    ManufacturerEnum,
    merge_devices_usage,
)
from .__utils__ import (
    PCIDevice,
    get_brief_version,
    get_numa_node_by_bdf,
    get_pci_devices,
    get_utilization,
)

logger = logging.getLogger(__name__)

_NEUWARE_VERSION_PATH = Path("/usr/local/neuware/version.txt")
"""
Where the CNToolkit package records the Neuware version, i.e. the runtime
version. The single path the operator's getRuntimeVersion stats.
"""

_NEUWARE_VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+")
"""
The version pattern the operator matches within that file, so a decorated line
still yields the version alone.
"""


class CambriconDetector(Detector):
    """
    Detect Cambricon MLUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
    def is_supported() -> bool:
        """
        Check if the Cambricon detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "cambricon"):
            logger.debug("Cambricon detection is disabled by environment variable")
            return supported

        pci_devs = CambriconDetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No Cambricon PCI devices found")
            return supported

        try:
            pycndev.cndevInit()
            supported = True
        except Exception:
            debug_log_exception(logger, "Failed to initialize CNDev")

        return supported

    @staticmethod
    @lru_cache(maxsize=1)
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Cambricon.
        pci_devs = get_pci_devices(vendor="0xcabc")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.CAMBRICON)

    def detect_info(self) -> Devices | None:
        """
        Detect Cambricon MLUs' inventory using pycndev, without usage metrics.

        Returns:
            A list of detected Cambricon MLU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pycndev.cndevInit()

            # The Neuware version is a host-wide file read, not a per-device
            # driver call, so it is read once above the loop, as the operator's
            # DetectAccelerator does.
            sys_runtime_ver_original = _get_runtime_version()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            dev_count = pycndev.cndevGetDeviceCount()
            dev_skipped_error = None
            dev_skipped = 0
            for dev_idx in range(dev_count):
                try:
                    dev = pycndev.cndevGetDeviceHandleByIndex(dev_idx)
                    dev_uuid = pycndev.cndevGetUUID(dev)
                    dev_name = pycndev.cndevGetCardNameByDevId(dev)
                    dev_mem_info = pycndev.cndevGetMemoryUsageV2(dev)
                    dev_bdf = pycndev.cndevGetPCIeBusId(dev)
                except pycndev.CNDevError as e:
                    # A card whose identity or inventory cannot be read is
                    # skipped, as the operator's DetectAccelerator does: one
                    # faulty card must cost that card, not every card of the
                    # host. Skipping does not renumber the survivors -- an index
                    # is what the driver enumerated the card at.
                    debug_log_warning(
                        logger,
                        "Failed to fetch device %d, skipping it",
                        dev_idx,
                    )
                    dev_skipped_error = e
                    dev_skipped += 1
                    continue

                # cndev.h calls the unit MB, but the operator assigns it straight
                # into its MiB memory field, cnmon reports MiB, and this repo's
                # Ascend detector treats its own MB-declared sizes the same way.
                # Converting would under-report ~4.8% against the operator on the
                # same host, which is the very discrepancy Story 1 removes.
                dev_mem = dev_mem_info.physicalMemoryTotal
                dev_mem_status = _get_memory_status(dev)

                dev_driver_ver = None
                with contextlib.suppress(pycndev.CNDevError):
                    dev_ver_info = pycndev.cndevGetVersionInfo(dev)
                    # The operator formats major.minor only; the build number
                    # comes free from the same query and is the digit a driver
                    # bug is diagnosed by, so it is kept.
                    dev_driver_ver = (
                        f"{dev_ver_info.driverMajorVersion}"
                        f".{dev_ver_info.driverMinorVersion}"
                        f".{dev_ver_info.driverBuildVersion}"
                    )

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    with contextlib.suppress(pycndev.CNDevError):
                        dev_numa_node_id = pycndev.cndevGetNUMANodeIdByDevId(dev)
                        if dev_numa_node_id.nodeId >= 0:
                            dev_numa = str(dev_numa_node_id.nodeId)

                dev_appendix = {
                    "bdf": dev_bdf,
                }
                if dev_numa:
                    dev_appendix["numa"] = dev_numa

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_idx,
                        name=dev_name,
                        uuid=dev_uuid,
                        driver_version=dev_driver_ver,
                        runtime_version=sys_runtime_ver,
                        runtime_version_original=sys_runtime_ver_original,
                        memory=dev_mem,
                        memory_status=dev_mem_status,
                        appendix=dev_appendix,
                    ),
                )

        except pycndev.CNDevError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        if dev_skipped and dev_skipped == dev_count:
            # Skipping one card of several is the graceful degradation this
            # vendor is allowed; skipping every one of them is systemic -- a
            # driver not exporting a call the loop needs, say -- and reporting it
            # as an empty inventory is indistinguishable from a host that has no
            # MLU at all. Fail as the other eight vendors do.
            raise dev_skipped_error

        return ret

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch the usage of Cambricon MLUs using pycndev, merged into the given
        devices in place.

        Args:
            devices:
                The devices to refresh, matched by UUID.
                If None, detects the devices' information first.

        Returns:
            The devices carrying usage,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        if devices is None:
            devices = self.detect_info()
            if devices is None:
                return None

        usages: Devices = []

        try:
            pycndev.cndevInit()

            dev_count = pycndev.cndevGetDeviceCount()
            dev_skipped_error = None
            dev_skipped = 0
            for dev_idx in range(dev_count):
                # CNDev enumerates by index, so the whole set is read and
                # merge_devices_usage keeps what the caller asked for, as the
                # operator's MonitorAccelerator does.
                try:
                    dev = pycndev.cndevGetDeviceHandleByIndex(dev_idx)
                    dev_uuid = pycndev.cndevGetUUID(dev)
                    dev_mem_info = pycndev.cndevGetMemoryUsageV2(dev)
                except pycndev.CNDevError as e:
                    debug_log_warning(
                        logger,
                        "Failed to fetch device %d usage, skipping it",
                        dev_idx,
                    )
                    dev_skipped_error = e
                    dev_skipped += 1
                    continue

                dev_mem = dev_mem_info.physicalMemoryTotal
                dev_mem_used = dev_mem_info.physicalMemoryUsed
                dev_mem_status = _get_memory_status(dev)

                dev_cores_util = 0
                with contextlib.suppress(pycndev.CNDevError):
                    dev_util_info = pycndev.cndevGetDeviceUtilizationInfo(dev)
                    dev_cores_util = dev_util_info.averageCoreUtilization

                dev_temp = None
                with contextlib.suppress(pycndev.CNDevError):
                    dev_temp_info = pycndev.cndevGetTemperatureInfo(dev)
                    dev_temp = dev_temp_info.chip

                dev_power_used = None
                with contextlib.suppress(pycndev.CNDevError):
                    dev_power_info = pycndev.cndevGetDevicePowerInfo(dev)
                    dev_power_used = dev_power_info.usage

                usages.append(
                    Device(
                        uuid=dev_uuid,
                        cores_utilization=dev_cores_util,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        memory_status=dev_mem_status,
                        temperature=dev_temp,
                        power_used=dev_power_used,
                    ),
                )

        except pycndev.CNDevError:
            debug_log_exception(logger, "Failed to fetch devices usage")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices usage fetching")
            raise

        if dev_skipped and dev_skipped == dev_count:
            # Every card refusing its metrics is systemic, not one faulty card,
            # and merging nothing would leave the caller reading the information
            # query's zeroes as an idle host.
            raise dev_skipped_error

        return merge_devices_usage(devices, usages)


def _get_memory_status(device: int) -> DeviceMemoryStatusEnum:
    """
    Get the memory status of a given device.

    Both the information and the usage query report it, mirroring the operator,
    which flags a card unhealthy from DetectAccelerator and MonitorAccelerator
    alike. The usage query cannot skip it: merging usage overwrites the status,
    so a status it did not read would erase the one the information query found.

    The verdict is the card-wide health bit, exactly as the operator derives it
    (`memoryUnhealthy = healthInfo.Health == 0`): CNDev reports no per-memory
    health, and the ECC counters cndevGetECCInfo exposes take no part in it.

    Args:
        device:
            The device handle.

    Returns:
        The memory status of the device.

    """
    if envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
        return DeviceMemoryStatusEnum.HEALTHY

    with contextlib.suppress(pycndev.CNDevError):
        dev_health_state = pycndev.cndevGetCardHealthStateV2(device)
        if dev_health_state.health == 0:
            return DeviceMemoryStatusEnum.UNHEALTHY

    return DeviceMemoryStatusEnum.HEALTHY


def _get_runtime_version() -> str | None:
    """
    Get the Neuware version installed on the host, i.e. the runtime version.

    Returns:
        The Neuware version, or None if it cannot be read.

    """
    with contextlib.suppress(OSError):
        content = _NEUWARE_VERSION_PATH.read_text().strip()
        if match := _NEUWARE_VERSION_PATTERN.search(content):
            return match.group()

    return None
