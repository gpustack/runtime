from __future__ import annotations

import logging
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pymxsml
from .__types__ import (
    Detector,
    Device,
    Devices,
    ManufacturerEnum,
    Topology,
    TopologyDistanceEnum,
)
from .__utils__ import (
    PCIDevice,
    bitmask_to_str,
    get_brief_version,
    get_numa_node_by_bdf,
    get_numa_nodeset_size,
    get_pci_devices,
    get_utilization,
    kibibyte_to_mebibyte,
    map_numa_node_to_cpu_affinity,
)

logger = logging.getLogger(__name__)

_TOPOLOGY_DISTANCE_MAPPING: dict[int, int] = {
    pymxsml.MXSML_TOPOLOGY_INTERNAL: TopologyDistanceEnum.SELF,
    pymxsml.MXSML_TOPOLOGY_METAXLINK: TopologyDistanceEnum.LINK,  # Traversing via high-speed interconnect, RoCE, etc.
    pymxsml.MXSML_TOPOLOGY_SINGLE: TopologyDistanceEnum.PIX,  # Traversing via a single PCIe bridge.
    pymxsml.MXSML_TOPOLOGY_MULTIPLE: TopologyDistanceEnum.PXB,  # Traversing via multiple PCIe bridges without PCIe Host Bridge.
    pymxsml.MXSML_TOPOLOGY_HOSTBRIDGE: TopologyDistanceEnum.PHB,  # Traversing via a PCIe Host Bridge.
    pymxsml.MXSML_TOPOLOGY_NODE: TopologyDistanceEnum.NODE,  # Traversing across NUMA nodes.
    pymxsml.MXSML_TOPOLOGY_SYSTEM: TopologyDistanceEnum.SYS,  # Traversing via SMP interconnect across other NUMA nodes.
}
"""
Mapping of MetaX topology types to distance values.
"""


class MetaXDetector(Detector):
    """
    Detect MetaX GPUs.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the MetaX detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "metax"):
            logger.debug("MetaX detection is disabled by environment variable")
            return supported

        pci_devs = MetaXDetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No MetaX PCI devices found")
            return supported

        try:
            pymxsml.mxSmlInit()
            supported = True
        except pymxsml.MXSMLError:
            debug_log_exception(logger, "Failed to initialize MXSML")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=MetaX.
        pci_devs = get_pci_devices(vendor="0x9999")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.METAX)

    def detect(self) -> Devices | None:
        """
        Detect MetaX GPUs using pymtml.

        Returns:
            A list of detected MetaX GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pymxsml.mxSmlInit()

            sys_runtime_ver_original = pymxsml.mxSmlGetMacaVersion()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            dev_count = pymxsml.mxSmlGetDeviceCount()
            for dev_idx in range(dev_count):
                dev_index = dev_idx

                dev_driver_ver = pymxsml.mxSmlGetDeviceVersion(
                    dev_idx,
                    pymxsml.MXSML_VERSION_DRIVER,
                )

                dev_info = pymxsml.mxSmlGetDeviceInfo(dev_idx)
                dev_uuid = dev_info.uuid
                dev_name = dev_info.deviceName
                if dev_info.mode == pymxsml.MXSML_VIRTUALIZATION_MODE_PF:
                    continue
                dev_is_vgpu = dev_info.mode == pymxsml.MXSML_VIRTUALIZATION_MODE_VF

                dev_core_util = pymxsml.mxSmlGetDeviceIpUsage(
                    dev_idx,
                    pymxsml.MXSML_USAGE_XCORE,
                )
                if dev_core_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_core_util = 0

                dev_mem_info = pymxsml.mxSmlGetMemoryInfo(dev_idx)
                dev_mem = kibibyte_to_mebibyte(  # KiB to MiB
                    dev_mem_info.vramTotal,
                )
                dev_mem_used = kibibyte_to_mebibyte(  # KiB to MiB
                    dev_mem_info.vramUse,
                )

                dev_temp = (
                    pymxsml.mxSmlGetTemperatureInfo(
                        dev_idx,
                        pymxsml.MXSML_TEMPERATURE_HOTSPOT,
                    )
                    // 100  # mC to C
                )

                dev_power = (
                    pymxsml.mxSmlGetBoardPowerLimit(dev_idx) // 1000  # mW to W
                )
                dev_power_used = None
                dev_power_info = pymxsml.mxSmlGetBoardPowerInfo(dev_idx)
                if dev_power_info:
                    dev_power_used = (
                        sum(i.power if i.power else 0 for i in dev_power_info)
                        // 1000  # mW to W
                    )

                dev_appendix = {
                    "vgpu": dev_is_vgpu,
                    "bdf": dev_info.bdfId,
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
                        cores_utilization=dev_core_util,
                        memory=dev_mem,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        temperature=dev_temp,
                        power=dev_power,
                        power_used=dev_power_used,
                        appendix=dev_appendix,
                    ),
                )

        except pymxsml.MXSMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def get_topology(self, devices: Devices | None = None) -> Topology | None:
        """
        Get the Topology object between NVIDIA GPUs.

        Args:
            devices:
                The list of detected NVIDIA devices.
                If None, detect topology for all available devices.

        Returns:
            The Topology object, or None if not supported.

        """
        if devices is None:
            devices = self.detect()
            if devices is None:
                return None

        ret = Topology(
            manufacturer=self.manufacturer,
            devices_count=len(devices),
        )

        try:
            pymxsml.mxSmlInit()

            for i, dev_i in enumerate(devices):
                # Get affinity with PCIe BDF if possible.
                if dev_i_bdf := dev_i.appendix.get("bdf", ""):
                    ret.devices_numa_affinities[i] = get_numa_node_by_bdf(
                        dev_i_bdf,
                    )
                    ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        ret.devices_numa_affinities[i],
                    )
                # Otherwise, get affinity by MXSML.
                if not ret.devices_cpu_affinities[i]:
                    # Get NUMA affinity.
                    try:
                        dev_i_nodeaff = pymxsml.mxSmlGetNodeAffinity(
                            dev_i.index,
                            get_numa_nodeset_size(),
                        )
                        ret.devices_numa_affinities[i] = bitmask_to_str(
                            list(dev_i_nodeaff),
                        )
                    except pymxsml.MXSMLError:
                        debug_log_warning(
                            logger,
                            "Failed to get device %d NUMA node affinity",
                            dev_i.index,
                        )
                    # Get CPU affinity.
                    ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        ret.devices_numa_affinities[i],
                    )

                # Get distances to other devices.
                for j, dev_j in enumerate(devices):
                    if dev_i.index == dev_j.index or ret.devices_distances[i][j] != 0:
                        continue

                    distance = TopologyDistanceEnum.UNK
                    try:
                        topo = pymxsml.mxSmlGetDeviceTopology(
                            dev_i.index,
                            dev_j.index,
                        )
                        distance = _TOPOLOGY_DISTANCE_MAPPING.get(
                            topo,
                            distance,
                        )
                    except pymxsml.MXSMLError:
                        debug_log_warning(
                            logger,
                            "Failed to get distance between device %d and device %d",
                            dev_i.index,
                            dev_j.index,
                        )

                    ret.devices_distances[i][j] = distance
                    ret.devices_distances[j][i] = distance
        except pymxsml.MXSMLError:
            debug_log_exception(logger, "Failed to fetch topology")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process topology fetching")
            raise

        return ret
