from __future__ import annotations

import contextlib
import logging
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import Topology, pyamdgpu, pyamdsmi, pyhsa, pyrocmcore, pyrocmsmi
from .__types__ import Detector, Device, Devices, ManufacturerEnum, TopologyDistanceEnum
from .__utils__ import (
    PCIDevice,
    byte_to_mebibyte,
    compare_pci_devices,
    get_brief_version,
    get_numa_node_by_bdf,
    get_pci_devices,
    get_utilization,
    map_numa_node_to_cpu_affinity,
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
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No AMD PCI devices found")
            return supported

        try:
            pyamdsmi.amdsmi_init()
            pyamdsmi.amdsmi_shut_down()
            supported = True
        except pyamdsmi.AmdSmiException:
            debug_log_exception(logger, "Failed to initialize AMD SMI")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=AMD.
        pci_devs = get_pci_devices(vendor="0x1002")
        if not pci_devs:
            return {}
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
            try:
                pyrocmsmi.rsmi_init()
            except pyrocmsmi.ROCMSMIError:
                debug_log_exception(logger, "Failed to initialize ROCm SMI")

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
                        dev_cc = pyrocmsmi.rsmi_dev_target_graphics_version_get(dev_idx)

                dev_cores = None
                if dev_hsa_agent:
                    dev_cores = dev_hsa_agent.compute_units
                elif dev_gpudev_info and hasattr(dev_gpudev_info, "cu_active_number"):
                    dev_cores = dev_gpudev_info.cu_active_number

                dev_cores_util = None
                dev_temp = None
                try:
                    dev_gpu_metrics_info = pyamdsmi.amdsmi_get_gpu_metrics_info(dev)
                    dev_cores_util = dev_gpu_metrics_info.get("average_gfx_activity", 0)
                    dev_temp = dev_gpu_metrics_info.get("temperature_hotspot", 0)
                except pyamdsmi.AmdSmiException:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_cores_util = pyrocmsmi.rsmi_dev_busy_percent_get(dev_idx)
                        dev_temp = pyrocmsmi.rsmi_dev_temp_metric_get(dev_idx)
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_cores_util = 0

                dev_mem = None
                dev_mem_used = None
                try:
                    dev_gpu_vram_usage = pyamdsmi.amdsmi_get_gpu_vram_usage(dev)
                    dev_mem = dev_gpu_vram_usage.get("vram_total")
                    dev_mem_used = dev_gpu_vram_usage.get("vram_used")
                except pyamdsmi.AmdSmiException:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_mem = byte_to_mebibyte(  # byte to MiB
                            pyrocmsmi.rsmi_dev_memory_total_get(dev_idx),
                        )
                        dev_mem_used = byte_to_mebibyte(  # byte to MiB
                            pyrocmsmi.rsmi_dev_memory_usage_get(dev_idx),
                        )

                dev_power = None
                dev_power_used = None
                try:
                    dev_power_info = pyamdsmi.amdsmi_get_power_info(dev)
                    dev_power = (
                        dev_power_info.get("power_limit", 0) // 1000000
                    )  # uW to W
                    dev_power_used = (
                        dev_power_info.get("current_socket_power")
                        if dev_power_info.get("current_socket_power", "N/A") != "N/A"
                        else dev_power_info.get("average_socket_power", 0)
                    )
                except pyamdsmi.AmdSmiException:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_power = pyrocmsmi.rsmi_dev_power_cap_get(dev_idx)
                        dev_power_used = pyrocmsmi.rsmi_dev_power_get(dev_idx)

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

                with contextlib.suppress(pyamdsmi.AmdSmiException):
                    dev_xgmi = pyamdsmi.amdsmi_get_xgmi_info(dev)
                    for k in [
                        "xgmi_lanes",
                        "xgmi_hive_id",
                        "xgmi_node_id",
                    ]:
                        if value := dev_xgmi.get(k):
                            dev_appendix[k] = value

                with contextlib.suppress(pyamdsmi.AmdSmiException):
                    dev_bdf = pyamdsmi.amdsmi_get_gpu_device_bdf(dev)
                    dev_appendix["bdf"] = dev_bdf

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
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise
        finally:
            pyamdsmi.amdsmi_shut_down()

        return ret

    def get_topology(self, devices: Devices | None = None) -> Topology | None:
        """
        Get the Topology object between AMD GPUs.

        Args:
            devices:
                The list of detected AMD GPU devices.
                If None, detect topology for all available devices.

        Returns:
            A Topology object, or None if not supported.

        """
        if devices is None:
            devices = self.detect()
            if devices is None:
                return None

        devices_count = len(devices)
        topology = Topology(
            manufacturer=self.manufacturer,
            devices_count=devices_count,
        )

        devs_mapping = None

        def get_device_handle(dev: Device):
            if bdf := dev.appendix.get("bdf", None):
                return pyamdsmi.amdsmi_get_processor_handle_from_bdf(bdf)
            nonlocal devs_mapping
            if devs_mapping is None:
                devs = pyamdsmi.amdsmi_get_processor_handles()
                devs_mapping = dict(enumerate(devs))
            return devs_mapping.get(dev.index)

        try:
            pci_devices = self.detect_pci_devices()

            def distance_pci_devices(bdf_a: str, bdf_b: str) -> TopologyDistanceEnum:
                """
                Compute distance between two PCI devices by their BDFs.

                Args:
                    bdf_a:
                        The BDF of the first PCI device.
                    bdf_b:
                        The BDF of the second PCI device.

                Returns:
                    The TopologyDistanceEnum representing the distance.

                """
                pcid_a = pci_devices.get(bdf_a, None)
                pcid_b = pci_devices.get(bdf_b, None)

                score = compare_pci_devices(pcid_a, pcid_b)
                if score > 0:
                    return TopologyDistanceEnum.PIX
                if score == 0:
                    return TopologyDistanceEnum.PXB
                return TopologyDistanceEnum.PHB

            pyamdsmi.amdsmi_init()

            # Get NUMA and CPU affinities.
            for i, dev_i in enumerate(devices):
                # Get affinity with PCIe BDF if possible.
                if dev_i_bdf := dev_i.appendix.get("bdf", ""):
                    topology.devices_numa_affinities[i] = get_numa_node_by_bdf(
                        dev_i_bdf,
                    )
                    topology.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        topology.devices_numa_affinities[i],
                    )
                # Otherwise, get affinity via AMD SMI.
                if not topology.devices_cpu_affinities[i]:
                    dev_i_handle = get_device_handle(dev_i)

                    # Get NUMA affinity.
                    try:
                        dev_i_numa_node = pyamdsmi.amdsmi_topo_get_numa_node_number(
                            dev_i_handle,
                        )
                        topology.devices_numa_affinities[i] = str(dev_i_numa_node)
                    except pyamdsmi.AmdSmiException:
                        debug_log_exception(
                            logger,
                            "Failed to get NUMA affinity for device %d",
                            dev_i.index,
                        )
                    # Get CPU affinity.
                    topology.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        topology.devices_numa_affinities[i],
                    )

            # Get distances to other devices.
            for i, dev_i in enumerate(devices):
                dev_i_handle = get_device_handle(dev_i)

                for j, dev_j in enumerate(devices):
                    if (
                        dev_i.index == dev_j.index
                        or topology.devices_distances[i][j] != 0
                    ):
                        continue

                    dev_j_handle = get_device_handle(dev_j)

                    distance = TopologyDistanceEnum.UNK
                    try:
                        link = pyamdsmi.amdsmi_topo_get_link_type(
                            dev_i_handle,
                            dev_j_handle,
                        )
                        link_type = link.get("type", -1)
                        link_hops = link.get("hops", -1)
                        match link_type:
                            case pyamdsmi.AMDSMI_LINK_TYPE_INTERNAL:
                                distance = TopologyDistanceEnum.SELF
                            case pyamdsmi.AMDSMI_LINK_TYPE_PCIE:
                                dev_i_numa, dev_j_numa = (
                                    topology.devices_numa_affinities[i],
                                    topology.devices_numa_affinities[j],
                                )
                                if dev_i_numa and dev_i_numa == dev_j_numa:
                                    distance = distance_pci_devices(
                                        dev_i.appendix.get("bdf", ""),
                                        dev_j.appendix.get("bdf", ""),
                                    )
                                else:
                                    distance = TopologyDistanceEnum.SYS
                            case pyamdsmi.AMDSMI_LINK_TYPE_XGMI:
                                distance = TopologyDistanceEnum.LINK
                            case _:
                                if link_hops == 0:
                                    distance = TopologyDistanceEnum.SELF
                    except pyamdsmi.AmdSmiException:
                        debug_log_exception(
                            logger,
                            "Failed to get distance between device %d and device %d",
                            dev_i.index,
                            dev_j.index,
                        )

                    topology.devices_distances[i][j] = distance
                    topology.devices_distances[j][i] = distance
        except pyamdsmi.AmdSmiException:
            debug_log_exception(logger, "Failed to fetch topology")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process topology fetching")
            raise
        finally:
            pyamdsmi.amdsmi_shut_down()

        return topology


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
