from __future__ import annotations

import contextlib
import logging
from functools import lru_cache

from .. import envs
from . import pyamdgpu, pyamdsmi, pyhsa, pyrocmcore, pyrocmsmi
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import (
    PCIDevice,
    get_brief_version,
    get_pci_devices,
    get_utilization,
)

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
        # See https://pcisig.com/membership/member-companies?combine=AMD.
        pci_devs = get_pci_devices(vendor="0x1002")
        if not pci_devs:
            return None
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.AMD)

    def detect(self) -> Devices | None:
        """
        Detect AMD GPUs using pyamdsmi, pyamdgpu and pyrocmsmi.

        Returns:
            A list of detected AMD GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            hsa_agents = {hsa_agent.uuid: hsa_agent for hsa_agent in pyhsa.get_agents()}

            pyamdsmi.amdsmi_init()

            sys_runtime_ver_original = pyrocmcore.getROCmVersion()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            devs = pyamdsmi.amdsmi_get_processor_handles()
            for dev_idx, dev in enumerate(devs):
                dev_index = dev_idx

                dev_gpu_asic_info = pyamdsmi.amdsmi_get_gpu_asic_info(dev)
                dev_uuid = f"GPU-{(dev_gpu_asic_info.get('asic_serial')[2:]).lower()}"
                dev_hsa_agent = hsa_agents.get(dev_uuid)

                dev_card_id = None
                if dev_hsa_agent:
                    dev_card_id = dev_hsa_agent.driver_node_id
                elif hasattr(pyamdsmi, "amdsmi_get_gpu_kfd_info"):
                    dev_kfd_info = pyamdsmi.amdsmi_get_gpu_kfd_info(dev)
                    dev_card_id = dev_kfd_info.get("node_id")
                else:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        pyrocmsmi.rsmi_init()
                        dev_card_id = pyrocmsmi.rsmi_dev_node_id_get(dev_idx)

                dev_gpudev_info = None
                if dev_card_id is not None:
                    with contextlib.suppress(pyamdgpu.AMDGPUError):
                        _, _, dev_gpudev = pyamdgpu.amdgpu_device_initialize(
                            dev_card_id,
                        )
                        dev_gpudev_info = pyamdgpu.amdgpu_query_gpu_info(dev_gpudev)
                        pyamdgpu.amdgpu_device_deinitialize(dev_gpudev)

                dev_gpu_driver_info = pyamdsmi.amdsmi_get_gpu_driver_info(dev)
                dev_driver_ver = dev_gpu_driver_info.get("driver_version")

                if dev_hsa_agent:
                    dev_name = dev_hsa_agent.name
                    if not dev_name:
                        dev_name = dev_gpu_asic_info.get("market_name")
                    dev_cc = dev_hsa_agent.compute_capability
                else:
                    dev_name = dev_gpu_asic_info.get("market_name")
                    dev_cc = None
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        pyrocmsmi.rsmi_init()
                        dev_cc = pyrocmsmi.rsmi_dev_target_graphics_version_get(dev_idx)

                dev_cores = None
                if dev_hsa_agent:
                    dev_cores = dev_hsa_agent.compute_units
                elif dev_gpudev_info and hasattr(dev_gpudev_info, "cu_active_number"):
                    dev_cores = dev_gpudev_info.cu_active_number

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
                    "arch_family": _get_arch_family(dev_gpudev_info),
                    "vgpu": dev_compute_partition is not None,
                    "card_id": dev_card_id,
                }

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        name=dev_name,
                        uuid=dev_uuid,
                        driver_version=dev_driver_ver,
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


def _get_arch_family(
    dev_gpudev_info: pyamdgpu.c_amdgpu_gpu_info | None,
) -> str | None:
    if not dev_gpudev_info:
        return None

    family_id = dev_gpudev_info.family_id
    if family_id is None:
        return None

    arch_family = {
        pyamdgpu.AMDGPU_FAMILY_SI: "Southern Islands",
        pyamdgpu.AMDGPU_FAMILY_CI: "Sea Islands",
        pyamdgpu.AMDGPU_FAMILY_KV: "Kaveri",
        pyamdgpu.AMDGPU_FAMILY_VI: "Volcanic Islands",
        pyamdgpu.AMDGPU_FAMILY_CZ: "Carrizo",
        pyamdgpu.AMDGPU_FAMILY_AI: "Arctic Islands",
        pyamdgpu.AMDGPU_FAMILY_RV: "Raven",
        pyamdgpu.AMDGPU_FAMILY_NV: "Navi",
        pyamdgpu.AMDGPU_FAMILY_VGH: "Van Gogh",
        pyamdgpu.AMDGPU_FAMILY_GC_11_0_0: "GC 11.0.0",
        pyamdgpu.AMDGPU_FAMILY_YC: "Yellow Carp",
        pyamdgpu.AMDGPU_FAMILY_GC_11_0_1: "GC 11.0.1",
        pyamdgpu.AMDGPU_FAMILY_GC_10_3_6: "GC 10.3.6",
        pyamdgpu.AMDGPU_FAMILY_GC_10_3_7: "GC 10.3.7",
        pyamdgpu.AMDGPU_FAMILY_GC_11_5_0: "GC 11.5.0",
        pyamdgpu.AMDGPU_FAMILY_GC_12_0_0: "GC 12.0.0",
    }

    return arch_family.get(family_id, "Unknown")
