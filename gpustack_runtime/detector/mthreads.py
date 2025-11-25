from __future__ import annotations

import logging
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pymtml
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import PCIDevice, byte_to_mebibyte, get_pci_devices, get_utilization

logger = logging.getLogger(__name__)


class MThreadsDetector(Detector):
    """
    Detect MThreads GPUs.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the MThreads detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "mthreads"):
            logger.debug("MThreads detection is disabled by environment variable")
            return supported

        pci_devs = MThreadsDetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No MThreads PCI devices found")
            return supported

        try:
            pymtml.mtmlLibraryInit()
            pymtml.mtmlLibraryShutDown()
            supported = True
        except pymtml.MTMLError:
            debug_log_exception(logger, "Failed to initialize MTML")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Moore+Threads.
        pci_devs = get_pci_devices(vendor="0x1ed5")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.MTHREADS)

    def detect(self) -> Devices | None:
        """
        Detect MThreads GPUs using pymtml.

        Returns:
            A list of detected MThreads GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pymtml.mtmlLibraryInit()

            sys_driver_ver = pymtml.mtmlSystemGetDriverVersion()

            dev_count = pymtml.mtmlLibraryCountDevice()
            for dev_idx in range(dev_count):
                dev_index = dev_idx

                dev_uuid = ""
                dev_name = ""
                dev_cores = 0
                dev_power_used = None
                dev = pymtml.mtmlLibraryInitDeviceByIndex(dev_idx)
                try:
                    dev_props = pymtml.mtmlDeviceGetProperty(dev)
                    dev_is_vgpu = (
                        dev_props.virtRole == pymtml.MTML_VIRT_ROLE_HOST_VIRTDEVICE
                    )
                    if (
                        dev_is_vgpu
                        and dev_props.mpcCap != pymtml.MTML_MPC_TYPE_INSTANCE
                    ):
                        continue

                    dev_uuid = pymtml.mtmlDeviceGetUUID(dev)
                    dev_name = pymtml.mtmlDeviceGetName(dev)
                    dev_cores = pymtml.mtmlDeviceCountGpuCores(dev)
                    dev_power_used = pymtml.mtmlDeviceGetPowerUsage(dev)
                finally:
                    pymtml.mtmlLibraryFreeDevice(dev)

                dev_mem = 0
                dev_mem_used = 0
                devmem = pymtml.mtmlDeviceInitMemory(dev)
                try:
                    dev_mem = byte_to_mebibyte(  # byte to MiB
                        pymtml.mtmlMemoryGetTotal(devmem),
                    )
                    dev_mem_used = byte_to_mebibyte(  # byte to MiB
                        pymtml.mtmlMemoryGetUsed(devmem),
                    )
                finally:
                    pymtml.mtmlDeviceFreeMemory(devmem)

                dev_cores_util = None
                dev_temp = None
                devgpu = pymtml.mtmlDeviceInitGpu(dev)
                try:
                    dev_cores_util = pymtml.mtmlGpuGetUtilization(devgpu)
                    dev_temp = pymtml.mtmlGpuGetTemperature(devgpu)
                finally:
                    pymtml.mtmlDeviceFreeGpu(devgpu)
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_cores_util = 0

                dev_appendix = {
                    "vgpu": dev_is_vgpu,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        uuid=dev_uuid,
                        name=dev_name,
                        driver_version=sys_driver_ver,
                        cores=dev_cores,
                        cores_utilization=dev_cores_util,
                        memory=dev_mem,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        temperature=dev_temp,
                        power_used=dev_power_used,
                        appendix=dev_appendix,
                    ),
                )

        except pymtml.MTMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise
        finally:
            pymtml.mtmlLibraryShutDown()

        return ret
