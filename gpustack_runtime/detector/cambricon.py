from __future__ import annotations

import json
import logging
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import (
    PCIDevice,
    execute_shell_command,
    get_pci_devices,
    get_utilization,
    safe_float,
    safe_int,
    support_command,
)

logger = logging.getLogger(__name__)


class CambriconDetector(Detector):
    """
    Detect Cambricon GPUs.
    """

    @staticmethod
    @lru_cache
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

        supported = support_command("cnmon")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Cambricon.
        pci_devs = get_pci_devices(vendor="0xcabc")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.CAMBRICON)

    def detect(self) -> Devices | None:
        """
        Detect Cambricon GPUs using cnmon tool.

        Returns:
            A list of detected Cambricon GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            output = execute_shell_command(
                "cnmon info -e -m -u -j > /dev/null && cat cnmon_info.json",
            )
            """
            Example output:
            TODO(thxCode): Add example output here.
            """

            output_json = json.loads(output)
            dev_infos = output_json.get("CnmonInfo", [])
            for dev_info in dev_infos:
                dev_index = safe_int(dev_info.get("CardNum"))
                dev_name = dev_info.get("ProductName")
                dev_uuid = dev_info.get("UUID")

                dev_util_info = dev_info.get("Utilization", {})
                dev_cores_util = safe_float(dev_util_info.get("MLUAverage", 0))

                dev_mem_usage_info = dev_info.get("PhysicalMemUsage", {})
                dev_mem = safe_int(dev_mem_usage_info.get("Total", 0))
                dev_mem_used = safe_int(dev_mem_usage_info.get("Used", 0))

                dev_temp_info = dev_info.get("Temperature", {})
                dev_temp = safe_float(dev_temp_info.get("Chip", 0))

                dev_appendix = {
                    "vgpu": False,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        name=dev_name,
                        uuid=dev_uuid,
                        cores_utilization=dev_cores_util,
                        memory=dev_mem,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        temperature=dev_temp,
                        appendix=dev_appendix,
                    ),
                )

        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret
