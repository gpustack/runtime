from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

from .. import envs
from . import pyhsa, pyrocmcore, pyrocmsmi
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import (
    PCIDevice,
    byte_to_mebibyte,
    get_brief_version,
    get_pci_devices,
    get_utilization,
)

logger = logging.getLogger(__name__)


class HygonDetector(Detector):
    """
    Detect Hygon GPUs.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the Hygon detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "hygon"):
            logger.debug("Hygon detection is disabled by environment variable")
            return supported

        pci_devs = HygonDetector.detect_pci_devices()
        if not pci_devs:
            logger.debug("No Hygon PCI devices found")
            return supported

        try:
            pyrocmsmi.rsmi_init()
            supported = True
        except pyrocmsmi.ROCMSMIError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to initialize ROCM SMI")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice] | None:
        # See https://pcisig.com/membership/member-companies?combine=Higon.
        pci_devs = get_pci_devices(vendor="0x1d94")
        if not pci_devs:
            return None
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.HYGON)

    def detect(self) -> Devices | None:
        """
        Detect Hygon GPUs using pyrocmsmi.

        Returns:
            A list of detected Hygon GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            hsa_agents = {hsa_agent.uuid: hsa_agent for hsa_agent in pyhsa.get_agents()}

            pyrocmsmi.rsmi_init()

            sys_driver_ver = None
            sys_driver_ver_path = Path("/sys/module/hydcu/version")
            if sys_driver_ver_path.exists():
                try:
                    with sys_driver_ver_path.open(encoding="utf-8") as f:
                        sys_driver_ver = f.read().strip()
                except OSError:
                    pass

            sys_runtime_ver_original = pyrocmcore.getROCmVersion()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            devs_count = pyrocmsmi.rsmi_num_monitor_devices()
            for dev_idx in range(devs_count):
                dev_index = dev_idx

                dev_uuid = f"GPU-{pyrocmsmi.rsmi_dev_unique_id_get(dev_idx)[2:]}"
                dev_hsa_agent = hsa_agents.get(dev_uuid)

                if dev_hsa_agent:
                    dev_name = dev_hsa_agent.name
                    if not dev_name:
                        dev_name = pyrocmsmi.rsmi_dev_name_get(dev_idx)
                    dev_cc = dev_hsa_agent.compute_capability
                    dev_cores = dev_hsa_agent.compute_units
                else:
                    dev_name = pyrocmsmi.rsmi_dev_name_get(dev_idx)
                    dev_cc = pyrocmsmi.rsmi_dev_target_graphics_version_get(dev_idx)
                    dev_cores = None

                dev_cores_util = pyrocmsmi.rsmi_dev_busy_percent_get(dev_idx)
                dev_mem = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_total_get(dev_idx),
                )
                dev_mem_used = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_usage_get(dev_idx),
                )
                dev_temp = pyrocmsmi.rsmi_dev_temp_metric_get(dev_idx)

                dev_power = pyrocmsmi.rsmi_dev_power_cap_get(dev_idx)
                dev_power_used = pyrocmsmi.rsmi_dev_power_get(dev_idx)

                dev_appendix = {
                    "vgpu": False,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        name=dev_name,
                        uuid=dev_uuid,
                        driver_version=sys_driver_ver,
                        runtime_version=sys_runtime_ver,
                        runtime_version_original=sys_runtime_ver_original,
                        compute_capability=dev_cc,
                        cores=dev_cores,
                        cores_utilization=dev_cores_util,
                        memory=dev_mem,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        temperature=dev_temp,
                        power=dev_power,
                        power_used=dev_power_used,
                        appendix=dev_appendix,
                    ),
                )
        except pyrocmsmi.ROCMSMIError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to fetch devices")
            raise
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to process devices fetching")
            raise

        return ret
