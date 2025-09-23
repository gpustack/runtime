from __future__ import annotations

import contextlib
import logging
from functools import lru_cache

from .. import envs
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import PCIDevice, get_device_files, get_pci_devices

logger = logging.getLogger(__name__)


class AMDDetector(Detector):
    """
    Detect AMD GPUs.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the AMD detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "amd"):
            logger.debug("AMD detection is disabled by environment variable")
            return supported

        pci_devs = AMDDetector.detect_pci_devices()
        if not pci_devs:
            logger.debug("No AMD PCI devices found")
            return supported

        try:
            import amdsmi as pyamdsmi  # noqa: PLC0415
        except ImportError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("amdsmi module is not installed")
            return supported

        try:
            pyamdsmi.amdsmi_init()
            pyamdsmi.amdsmi_shut_down()
            supported = True
        except pyamdsmi.AmdSmiException:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to initialize AMD SMI")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice] | None:
        pci_devs = get_pci_devices(vendor="0x1002")
        if not pci_devs:
            return None
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.AMD)

    def detect(self) -> Devices | None:
        """
        Detect AMD GPUs.

        Returns:
            A list of detected AMD GPU devices,
            or None if detection fails.

        """
        if not self.is_supported():
            return None

        try:
            import amdsmi as pyamdsmi  # noqa: PLC0415
        except ImportError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("amdsmi module is not installed")
            return None

        ret: Devices = []

        try:
            pyamdsmi.amdsmi_init()

            sys_runtime_ver = None
            sys_runtime_ver_t = None

            devs = pyamdsmi.amdsmi_get_processor_handles()
            dev_files = None
            for dev_idx, dev in enumerate(devs):
                dev_index = dev_idx
                if envs.GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY:
                    if dev_files is None:
                        dev_files = get_device_files(
                            pattern=r"card(?P<number>\d+)",
                            directory="/dev/dri",
                        )
                    if len(dev_files) > dev_idx:
                        dev_file = dev_files[dev_idx]
                        if dev_file.number is not None:
                            dev_index = dev_file.number - 1

                dev_uuid = pyamdsmi.amdsmi_get_gpu_device_uuid(dev)

                dev_gpu_driver_info = pyamdsmi.amdsmi_get_gpu_driver_info(dev)
                dev_driver_ver = dev_gpu_driver_info.get("driver_version")
                dev_driver_ver_t = [
                    int(v) if v.isdigit() else v for v in dev_driver_ver.split(".")
                ]

                dev_gpu_board_info = pyamdsmi.amdsmi_get_gpu_board_info(dev)
                dev_name = "AMD " + dev_gpu_board_info.get("product_name")

                dev_gpu_metrics_info = pyamdsmi.amdsmi_get_gpu_metrics_info(dev)
                dev_cores_util = dev_gpu_metrics_info.get("average_gfx_activity", 0)
                dev_gpu_vram_usage = pyamdsmi.amdsmi_get_gpu_vram_usage(dev)
                dev_mem = dev_gpu_vram_usage.get("vram_total")
                dev_mem_used = dev_gpu_vram_usage.get("vram_used")
                dev_temp = dev_gpu_metrics_info.get("temperature_hotspot", 0)

                dev_power_info = pyamdsmi.amdsmi_get_power_info(dev)
                dev_power = dev_power_info.get("power_limit", 0) // 1000000  # uW to W
                dev_power_used = (
                    dev_power_info.get("current_socket_power")
                    if dev_power_info.get("current_socket_power", "N/A") != "N/A"
                    else dev_power_info.get("average_socket_power", 0)
                )

                dev_compute_partition = None
                with contextlib.suppress(pyamdsmi.AmdSmiException):
                    dev_compute_partition = pyamdsmi.amdsmi_get_gpu_compute_partition(
                        dev,
                    )

                dev_appendix = {
                    "vgpu": dev_compute_partition is not None,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        name=dev_name,
                        uuid=dev_uuid,
                        driver_version=dev_driver_ver,
                        driver_version_tuple=dev_driver_ver_t,
                        runtime_version=sys_runtime_ver,
                        runtime_version_tuple=sys_runtime_ver_t,
                        cores_utilization=dev_cores_util,
                        memory=dev_mem,
                        memory_used=dev_mem_used,
                        memory_utilization=(
                            (dev_mem_used * 100 // dev_mem) if dev_mem > 0 else 0
                        ),
                        temperature=dev_temp,
                        power=dev_power,
                        power_used=dev_power_used,
                        appendix=dev_appendix,
                    ),
                )
        except pyamdsmi.AmdSmiException:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to fetch devices")
            raise
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to process devices fetching")
            raise
        finally:
            pyamdsmi.amdsmi_shut_down()

        return ret
