from __future__ import annotations as __future_annotations__

import logging
from functools import lru_cache

import pymtml

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
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
    byte_to_mebibyte,
    get_numa_node_by_bdf,
    get_numa_nodeset_size,
    get_pci_devices,
    get_utilization,
    map_numa_node_to_cpu_affinity,
)

logger = logging.getLogger(__name__)

_TOPOLOGY_DISTANCE_MAPPING: dict[int, int] = {
    pymtml.MTML_TOPOLOGY_INTERNAL: TopologyDistanceEnum.SELF,
    pymtml.MTML_TOPOLOGY_SINGLE: TopologyDistanceEnum.PIX,  # Traversing via a single PCIe bridge.
    pymtml.MTML_TOPOLOGY_MULTIPLE: TopologyDistanceEnum.PXB,  # Traversing via multiple PCIe bridges without PCIe Host Bridge.
    pymtml.MTML_TOPOLOGY_HOSTBRIDGE: TopologyDistanceEnum.PHB,  # Traversing via a PCIe Host Bridge.
    pymtml.MTML_TOPOLOGY_NODE: TopologyDistanceEnum.NODE,  # Traversing across NUMA nodes.
    pymtml.MTML_TOPOLOGY_SYSTEM: TopologyDistanceEnum.SYS,  # Traversing via SMP interconnect across other NUMA nodes.
}
"""
Mapping of MThreads topology types to distance values.
"""


class MThreadsDetector(Detector):
    """
    Detect MThreads GPUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
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
    @lru_cache(maxsize=1)
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
            system = pymtml.mtmlLibraryInitSystem()
            sys_driver_ver = pymtml.mtmlSystemGetDriverVersion(system)
            dev_count = pymtml.mtmlLibraryCountDevice()
            for dev_idx in range(dev_count):
                dev_index = dev_idx

                dev_uuid = ""
                dev_name = ""
                dev_cores = 0
                dev_power_used = None
                dev_pci_info = None
                dev_is_vgpu = False
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
                    dev_pci_info = pymtml.mtmlDeviceGetPciInfo(dev)
                finally:
                    pymtml.mtmlLibraryFreeDevice(dev)

                dev_mem = 0
                dev_mem_used = 0
                with pymtml.mtmlMemoryContext(dev) as devmem:
                    dev_mem = byte_to_mebibyte(  # byte to MiB
                        pymtml.mtmlMemoryGetTotal(devmem),
                    )
                    dev_mem_used = byte_to_mebibyte(  # byte to MiB
                        pymtml.mtmlMemoryGetUsed(devmem),
                    )

                dev_cores_util = None
                dev_temp = None
                with pymtml.mtmlGpuContext(dev) as devgpu:
                    dev_cores_util = pymtml.mtmlGpuGetUtilization(devgpu)
                    dev_temp = pymtml.mtmlGpuGetTemperature(devgpu)

                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_cores_util = 0

                dev_bdf = f"{dev_pci_info.segment:04x}:{dev_pci_info.bus:02x}:{dev_pci_info.device:02x}.0"

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    dev_node_affinity = pymtml.mtmlDeviceGetMemoryAffinityWithinNode(
                        dev,
                        get_numa_nodeset_size(),
                    )
                    dev_numa = bitmask_to_str(
                        list(dev_node_affinity),
                    )

                dev_appendix = {
                    "vgpu": dev_is_vgpu,
                    "bdf": dev_bdf,
                    "numa": dev_numa,
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
            pymtml.mtmlLibraryFreeSystem(system)
            pymtml.mtmlLibraryShutDown()

        return ret

    def get_topology(self, devices: Devices | None = None) -> Topology | None:
        """
        Get the Topology object between MThreads GPUs.

        Args:
            devices:
                The list of detected MThreads devices.
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
            pymtml.mtmlLibraryInit()

            for i, dev_i in enumerate(devices):
                dev_i_handle = pymtml.mtmlLibraryInitDeviceByIndex(dev_i.index)

                try:
                    # Get NUMA and CPU affinities.
                    ret.devices_numa_affinities[i] = dev_i.appendix.get("numa", "")
                    ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        ret.devices_numa_affinities[i],
                    )

                    # Get links state if applicable.
                    if dev_i_links_state := _get_links_state(dev_i_handle):
                        ret.appendices[i].update(dev_i_links_state)
                        # In practice, if a card has an active *Link,
                        # then other cards in the same machine should be interconnected with it through the *Link.
                        if dev_i_links_state.get("links_active_count", 0) > 0:
                            for j, dev_j in enumerate(devices):
                                if dev_i.index == dev_j.index:
                                    continue
                                ret.devices_distances[i][j] = TopologyDistanceEnum.LINK
                                ret.devices_distances[j][i] = TopologyDistanceEnum.LINK
                            continue

                    # Get distances to other devices.
                    for j, dev_j in enumerate(devices):
                        if (
                            dev_i.index == dev_j.index
                            or ret.devices_distances[i][j] != 0
                        ):
                            continue

                        dev_j_handle = pymtml.mtmlLibraryInitDeviceByIndex(dev_j.index)

                        distance = TopologyDistanceEnum.UNK
                        try:
                            topo = pymtml.mtmlDeviceGetTopologyLevel(
                                dev_i_handle,
                                dev_j_handle,
                            )
                            distance = _TOPOLOGY_DISTANCE_MAPPING.get(
                                topo,
                                distance,
                            )
                        except pymtml.MTMLError:
                            debug_log_warning(
                                logger,
                                "Failed to get distance between device %d and %d",
                                dev_i.index,
                                dev_j.index,
                            )
                        finally:
                            pymtml.mtmlLibraryFreeDevice(dev_j_handle)

                        ret.devices_distances[i][j] = distance
                        ret.devices_distances[j][i] = distance

                finally:
                    pymtml.mtmlLibraryFreeDevice(dev_i_handle)

        except Exception:
            debug_log_exception(logger, "Failed to process topology fetching")
            raise
        finally:
            pymtml.mtmlLibraryShutDown()

        return ret


def _get_links_state(
    dev: pymtml.c_mtmlDevice_t,
) -> dict | None:
    """
    Get the MTLink links count and state for a device.

    Args:
        dev:
            The MTLink device handle.

    Returns:
        A dict includes links state or None if failed.

    """
    dev_links_count = 0
    try:
        dev_link_spec = pymtml.mtmlDeviceGetMtLinkSpec(dev)
        dev_links_count = dev_link_spec.linkNum
    except pymtml.MTMLError:
        debug_log_warning(logger, "Failed to get MTLink links count")
    if not dev_links_count:
        return None

    dev_links_state = 0
    dev_links_active_count = 0
    try:
        for link_idx in range(int(dev_links_count)):
            dev_link_state = pymtml.mtmlDeviceGetMtLinkState(dev, link_idx)
            if dev_link_state == pymtml.MTML_MTLINK_STATE_UP:
                dev_links_state |= 1 << link_idx
                dev_links_active_count += 1
    except pymtml.MTMLError:
        debug_log_warning(logger, "Failed to get MTLink link state")

    return {
        "links_count": dev_links_count,
        "links_state": dev_links_state,
        "links_active_count": dev_links_active_count,
    }
