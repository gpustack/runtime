from __future__ import annotations

import csv
import logging
from functools import lru_cache

from .. import envs
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import (
    PCIDevice,
    execute_command,
    get_pci_devices,
    get_utilization,
    safe_float,
    safe_int,
    support_command,
)

logger = logging.getLogger(__name__)


class IluvatarDetector(Detector):
    """
    Detect Iluvatar GPUs.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the Iluvatar detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "iluvatar"):
            logger.debug("Iluvatar detection is disabled by environment variable")
            return supported

        pci_devs = IluvatarDetector.detect_pci_devices()
        if not pci_devs:
            logger.debug("No Iluvatar PCI devices found")

        supported = support_command("ixsmi")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice] | None:
        # See https://pcisig.com/membership/member-companies?combine=Iluvatar.
        pci_devs = get_pci_devices(vendor="0x1e3e")
        if not pci_devs:
            return None
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.ILUVATAR)

    def detect(self) -> Devices | None:
        """
        Detect Iluvatar GPUs using ixsmi tool.

        Returns:
            A list of detected Iluvatar GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            output = execute_command(
                [
                    "ixsmi",
                    "--format=csv,noheader",
                    "--query-gpu=index,name,utilization.gpu,memory.total,memory.used,temperature.gpu,power.default_limit,power.draw",
                ],
            )
            """
            Example output:
            0, Iluvatar MR-V50, 0 %, 16384 MiB, 116 MiB, 30 C, 320.00 W, 9.26 W
            1, Iluvatar MR-V100, 0 %, 32768 MiB, 27996 MiB, 36 C, 320.00 W, 6.35 W
            """

            output_csv = csv.reader(output.splitlines())
            for row in output_csv:
                if len(row) < 8:
                    continue

                dev_index = safe_int(row[0])
                dev_name = row[1].strip()

                dev_cores_util = safe_float(row[2].split()[0])

                dev_mem = safe_int(row[3].split()[0])
                dev_mem_used = safe_int(row[4].split()[0])

                dev_temp = safe_float(row[5].split()[0])

                dev_power = safe_float(row[6].split()[0])
                dev_power_used = safe_float(row[7].split()[0])

                dev_appendix = {
                    "vgpu": False,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        name=dev_name,
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

        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to process devices fetching")
            raise

        return ret
