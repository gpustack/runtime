from __future__ import annotations

import logging
from functools import lru_cache

from .. import envs
from . import pymtml
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import PCIDevice, get_pci_devices

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
        if not pci_devs:
            logger.debug("No MThreads PCI devices found")
            return supported

        try:
            pymtml.mtmlLibraryInit()
            pymtml.mtmlLibraryShutDown()
            supported = True
        except pymtml.MTMLError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to initialize MTML")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice] | None:
        # See https://pcisig.com/membership/member-companies?combine=Moore+Threads.
        pci_devs = get_pci_devices(vendor="0x1ed5")
        if not pci_devs:
            return None
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
            sys_driver_ver_t = [
                int(v) if v.isdigit() else v for v in sys_driver_ver.split(".")
            ]

            dev_count = pymtml.mtmlLibraryCountDevice()
            for dev_idx in range(dev_count):
                dev = pymtml.mtmlLibraryInitDeviceByIndex(dev_idx)
                try:
                    dev_props = pymtml.mtmlDeviceGetProperty(dev)
                    dev_is_gpu = (
                        dev_props.virtRole == pymtml.MTML_VIRT_ROLE_HOST_VIRTDEVICE
                    )
                    if dev_is_gpu and dev_props.mpcCap != pymtml.MTML_MPC_TYPE_INSTANCE:
                        continue

                    dev_index = dev_idx
                    dev_uuid = pymtml.mtmlDeviceGetUUID(dev)
                    dev_name = pymtml.mtmlDeviceGetName(dev)
                    dev_cores = pymtml.mtmlDeviceCountGpuCores(dev)
                    dev_power_used = pymtml.mtmlDeviceGetPowerUsage(dev)
                finally:
                    pymtml.mtmlLibraryFreeDevice(dev)

                devmem = pymtml.mtmlDeviceInitMemory(dev)
                try:
                    dev_mem = pymtml.mtmlMemoryGetTotal(devmem)
                    dev_mem_used = pymtml.mtmlMemoryGetUsed(devmem)
                finally:
                    pymtml.mtmlDeviceFreeMemory(devmem)

                devgpu = pymtml.mtmlDeviceInitGpu(dev)
                try:
                    dev_cores_util = pymtml.mtmlGpuGetUtilization(devgpu)
                    dev_temp = pymtml.mtmlGpuGetTemperature(devgpu)
                finally:
                    pymtml.mtmlDeviceFreeGpu(devgpu)

                dev_appendix = {
                    "vgpu": dev_is_gpu,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        uuid=dev_uuid,
                        name=dev_name,
                        driver_version=sys_driver_ver,
                        driver_version_tuple=sys_driver_ver_t,
                        cores=dev_cores,
                        cores_utilization=dev_cores_util,
                        memory=dev_mem,
                        memory_used=dev_mem_used,
                        memory_utilization=(
                            (dev_mem_used * 100 // dev_mem) if dev_mem > 0 else 0
                        ),
                        temperature=dev_temp,
                        power_used=dev_power_used,
                        appendix=dev_appendix,
                    ),
                )

        except pymtml.MTMLError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to fetch devices")
            raise
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to process devices fetching")
            raise
        finally:
            pymtml.mtmlLibraryShutDown()

        return ret
