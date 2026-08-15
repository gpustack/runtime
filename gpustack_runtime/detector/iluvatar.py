from __future__ import annotations as __future_annotations__

import contextlib
import logging
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pyixml
from .__types__ import (
    Detector,
    Device,
    DeviceMemoryStatusEnum,
    Devices,
    ManufacturerEnum,
    Topology,
    TopologyDistanceEnum,
    merge_devices_usage,
)
from .__utils__ import (
    PCIDevice,
    bitmask_to_str,
    byte_to_mebibyte,
    get_brief_version,
    get_numa_node_by_bdf,
    get_numa_nodeset_size,
    get_pci_devices,
    get_utilization,
    map_numa_node_to_cpu_affinity,
)

logger = logging.getLogger(__name__)


class IluvatarDetector(Detector):
    """
    Detect Iluvatar GPUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
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
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No Iluvatar PCI devices found")
            return supported

        try:
            pyixml.nvmlInit()
            supported = True
        except Exception:
            debug_log_exception(logger, "Failed to initialize IXML")

        return supported

    @staticmethod
    @lru_cache(maxsize=1)
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Iluvatar.
        pci_devs = get_pci_devices(vendor="0x1e3e")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.ILUVATAR)

    def detect_info(self) -> Devices | None:
        """
        Detect Iluvatar GPUs' inventory using pyixml, without usage metrics.

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
            pyixml.nvmlInit()

            sys_driver_ver = pyixml.nvmlSystemGetDriverVersion()

            sys_runtime_ver_original = None
            sys_runtime_ver = None
            with contextlib.suppress(Exception):
                sys_runtime_ver_original = pyixml.nvmlSystemGetCudaDriverVersion()
                sys_runtime_ver_original = ".".join(
                    map(
                        str,
                        [
                            sys_runtime_ver_original // 1000,
                            (sys_runtime_ver_original % 1000) // 10,
                            (sys_runtime_ver_original % 10),
                        ],
                    ),
                )
                sys_runtime_ver = get_brief_version(
                    sys_runtime_ver_original,
                )

            dev_count = pyixml.nvmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pyixml.nvmlDeviceGetHandleByIndex(dev_idx)

                dev_index = dev_idx

                # Device.index is the enumeration index, while the driver's
                # minor number is what /dev/iluvatar{N} is made of, so the
                # latter goes to the appendix. Mirrors the operator, which
                # keeps a sequential Index next to PhysicalIndexes, and omits
                # the physical one when the driver cannot answer.
                dev_minor_number = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_minor_number = pyixml.nvmlDeviceGetMinorNumber(dev)

                dev_name = pyixml.nvmlDeviceGetName(dev)

                dev_uuid = pyixml.nvmlDeviceGetUUID(dev)

                dev_cores = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_cores = pyixml.nvmlDeviceGetNumGpuCores(dev)

                dev_mem = 0
                dev_mem_status = DeviceMemoryStatusEnum.HEALTHY
                with contextlib.suppress(pyixml.NVMLError):
                    # Prefer the v2 memory structure, falling back to v1 --
                    # mirrors the operator's GetMemoryInfoV, which drops the
                    # device only when neither call succeeds.
                    dev_mem_info = _get_memory_info(dev)
                    dev_mem = byte_to_mebibyte(  # byte to MiB
                        dev_mem_info.total,
                    )
                if not envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
                    with contextlib.suppress(pyixml.NVMLError):
                        dev_health = pyixml.ixmlDeviceGetHealth(dev)
                        if dev_health != pyixml.IXML_HEALTH_OK:
                            dev_mem_status = DeviceMemoryStatusEnum.UNHEALTHY

                dev_power = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_power = pyixml.nvmlDeviceGetPowerManagementDefaultLimit(dev)
                    dev_power = dev_power // 1000  # mW to W

                dev_cc = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_cc_t = pyixml.nvmlDeviceGetCudaComputeCapability(dev)
                    if dev_cc_t:
                        dev_cc = ".".join(map(str, dev_cc_t))

                dev_pci_info = pyixml.nvmlDeviceGetPciInfo(dev)
                dev_bdf = str(dev_pci_info.busIdLegacy).lower()

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    with contextlib.suppress(pyixml.NVMLError):
                        dev_node_affinity = pyixml.nvmlDeviceGetMemoryAffinity(
                            dev,
                            get_numa_nodeset_size(),
                            pyixml.NVML_AFFINITY_SCOPE_NODE,
                        )
                        dev_numa = bitmask_to_str(list(dev_node_affinity))

                dev_appendix = {
                    "bdf": dev_bdf,
                }
                if dev_minor_number is not None:
                    dev_appendix["minor_number"] = dev_minor_number
                if dev_numa:
                    dev_appendix["numa"] = dev_numa

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
                        memory=dev_mem,
                        memory_status=dev_mem_status,
                        power=dev_power,
                        appendix=dev_appendix,
                    ),
                )
        except pyixml.NVMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch Iluvatar GPUs' usage using pyixml.

        Args:
            devices:
                The devices to refresh, matched by UUID.
                If None, detects the devices' information first.

        Returns:
            The devices carrying usage, or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        if devices is None:
            devices = self.detect_info()
        if not devices:
            return devices

        usages: Devices = []

        try:
            pyixml.nvmlInit()

            dev_count = pyixml.nvmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pyixml.nvmlDeviceGetHandleByIndex(dev_idx)

                dev_uuid = pyixml.nvmlDeviceGetUUID(dev)

                dev_mem = 0
                dev_mem_used = 0
                dev_mem_status = DeviceMemoryStatusEnum.HEALTHY
                with contextlib.suppress(pyixml.NVMLError):
                    # Same v2-then-v1 fallback as detect_info: the operator's
                    # MonitorAccelerator re-reads the memory info rather than
                    # trusting a previous pass.
                    dev_mem_info = _get_memory_info(dev)
                    dev_mem = byte_to_mebibyte(  # byte to MiB
                        dev_mem_info.total,
                    )
                    dev_mem_used = byte_to_mebibyte(  # byte to MiB
                        dev_mem_info.used,
                    )
                if not envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
                    with contextlib.suppress(pyixml.NVMLError):
                        dev_health = pyixml.ixmlDeviceGetHealth(dev)
                        if dev_health != pyixml.IXML_HEALTH_OK:
                            dev_mem_status = DeviceMemoryStatusEnum.UNHEALTHY

                dev_cores_util = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_util_rates = pyixml.nvmlDeviceGetUtilizationRates(dev)
                    dev_cores_util = dev_util_rates.gpu
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_idx,
                    )
                    dev_cores_util = 0

                dev_temp = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_temp = pyixml.nvmlDeviceGetTemperature(
                        dev,
                        pyixml.NVML_TEMPERATURE_GPU,
                    )

                dev_power_used = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_power_used = (
                        pyixml.nvmlDeviceGetPowerUsage(dev) // 1000
                    )  # mW to W

                usages.append(
                    Device(
                        uuid=dev_uuid,
                        cores_utilization=dev_cores_util,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        memory_status=dev_mem_status,
                        temperature=dev_temp,
                        power_used=dev_power_used,
                    ),
                )
        except pyixml.NVMLError:
            debug_log_exception(logger, "Failed to fetch devices usage")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices usage fetching")
            raise

        return merge_devices_usage(devices, usages)

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
            devices = self.detect_info()
            if devices is None:
                return None

        ret = Topology(
            manufacturer=self.manufacturer,
            devices_count=len(devices),
        )

        try:
            pyixml.nvmlInit()

            for i, dev_i in enumerate(devices):
                dev_i_handle = pyixml.nvmlDeviceGetHandleByUUID(dev_i.uuid)

                # Get NUMA and CPU affinities.
                ret.devices_numa_affinities[i] = dev_i.appendix.get("numa", "")
                ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                    ret.devices_numa_affinities[i],
                )

                # Get distances to other devices.
                for j, dev_j in enumerate(devices):
                    if dev_i.index == dev_j.index or ret.devices_distances[i][j] != 0:
                        continue

                    dev_j_handle = pyixml.nvmlDeviceGetHandleByUUID(dev_j.uuid)

                    distance = TopologyDistanceEnum.UNK
                    try:
                        distance = pyixml.nvmlDeviceGetTopologyCommonAncestor(
                            dev_i_handle,
                            dev_j_handle,
                        )
                        # TODO(thxCode): Support LINK distance.
                    except pyixml.NVMLError:
                        debug_log_exception(
                            logger,
                            "Failed to get distance between device %d and %d",
                            dev_i.index,
                            dev_j.index,
                        )

                    ret.devices_distances[i][j] = distance
                    ret.devices_distances[j][i] = distance
        except Exception:
            debug_log_exception(logger, "Failed to process topology fetching")
            raise

        return ret


def _get_memory_info(dev):
    """
    Read a device's memory info, preferring the v2 structure with a v1
    fallback.

    Mirrors the operator's GetMemoryInfoV, which tries
    ``nvmlDeviceGetMemoryInfo_v2`` before falling back to the v1 call: the
    runtime previously read the v1 accessor only, so a driver exposing just
    v2 failed here where the operator succeeded.

    Args:
        dev:
            The device handle.

    Returns:
        The memory info structure, from whichever accessor answered.

    Raises:
        pyixml.NVMLError: If neither accessor succeeds.

    """
    try:
        return pyixml.nvmlDeviceGetMemoryInfo(dev, version=pyixml.nvmlMemory_v2)
    except pyixml.NVMLError:
        return pyixml.nvmlDeviceGetMemoryInfo(dev)
