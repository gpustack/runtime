from __future__ import annotations

import contextlib
import logging
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pyixml
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
    get_brief_version,
    get_numa_node_by_bdf,
    get_numa_nodeset_size,
    get_pci_devices,
    get_utilization,
    map_numa_node_to_cpu_affinity,
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
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No Iluvatar PCI devices found")

        supported = support_command("ixsmi")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Iluvatar.
        pci_devs = get_pci_devices(vendor="0x1e3e")
        if not pci_devs:
            return {}
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
            pyixml.nvmlInit()

            sys_driver_ver = pyixml.nvmlSystemGetDriverVersion()

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
                dev_uuid = pyixml.nvmlDeviceGetUUID(dev)
                dev_name = pyixml.nvmlDeviceGetName(dev)

                dev_cores = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_cores = pyixml.nvmlDeviceGetNumGpuCores(dev)

                dev_mem = 0
                dev_mem_used = 0
                with contextlib.suppress(pyixml.NVMLError):
                    dev_mem_info = pyixml.nvmlDeviceGetMemoryInfo(dev)
                    dev_mem = byte_to_mebibyte(  # byte to MiB
                        dev_mem_info.total,
                    )
                    dev_mem_used = byte_to_mebibyte(  # byte to MiB
                        dev_mem_info.used,
                    )

                dev_cores_util = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_util_rates = pyixml.nvmlDeviceGetUtilizationRates(dev)
                    dev_cores_util = dev_util_rates.gpu
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_cores_util = 0

                dev_temp = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_temp = pyixml.nvmlDeviceGetTemperature(
                        dev,
                        pyixml.NVML_TEMPERATURE_GPU,
                    )

                dev_power = None
                dev_power_used = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_power = pyixml.nvmlDeviceGetPowerManagementDefaultLimit(dev)
                    dev_power = dev_power // 1000  # mW to W
                    dev_power_used = (
                        pyixml.nvmlDeviceGetPowerUsage(dev) // 1000
                    )  # mW to W

                dev_cc = None
                with contextlib.suppress(pyixml.NVMLError):
                    dev_cc_t = pyixml.nvmlDeviceGetCudaComputeCapability(dev)
                    if dev_cc_t:
                        dev_cc = ".".join(map(str, dev_cc_t))

                dev_is_vgpu = False
                dev_pci_info = pyixml.nvmlDeviceGetPciInfo(dev)

                dev_appendix = {
                    "vgpu": dev_is_vgpu,
                    "bdf": str(dev_pci_info.busIdLegacy).lower(),
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
        except pyixml.NVMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise
        finally:
            pyixml.nvmlShutdown()

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
            for i, dev_i in enumerate(devices):
                dev_i_handle = pyixml.nvmlDeviceGetHandleByUUID(dev_i.uuid)

                # Get affinity with PCIe BDF if possible.
                if dev_i_bdf := dev_i.appendix.get("bdf", ""):
                    ret.devices_numa_affinities[i] = get_numa_node_by_bdf(
                        dev_i_bdf,
                    )
                    ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        ret.devices_numa_affinities[i],
                    )
                # Otherwise, get affinity via IXML.
                if not ret.devices_cpu_affinities[i]:
                    # Get NUMA affinity.
                    try:
                        dev_i_memset = pyixml.nvmlDeviceGetMemoryAffinity(
                            dev_i_handle,
                            get_numa_nodeset_size(),
                            pyixml.NVML_AFFINITY_SCOPE_NODE,
                        )
                        ret.devices_numa_affinities[i] = bitmask_to_str(
                            list(dev_i_memset),
                        )
                    except pyixml.NVMLError:
                        debug_log_exception(
                            logger,
                            "Failed to get NUMA affinity for device %d",
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
        except pyixml.NVMLError:
            debug_log_exception(logger, "Failed to fetch topology")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process topology fetching")
            raise
        finally:
            pyixml.nvmlShutdown()

        return ret
