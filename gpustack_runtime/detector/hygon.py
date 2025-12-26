from __future__ import annotations

import contextlib
import logging
from functools import lru_cache
from pathlib import Path

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import Topology, pyamdgpu, pyhsa, pyrocmcore, pyrocmsmi
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
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No Hygon PCI devices found")
            return supported

        try:
            pyrocmsmi.rsmi_init()
            supported = True
        except pyrocmsmi.ROCMSMIError:
            debug_log_exception(logger, "Failed to initialize ROCM SMI")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Higon.
        pci_devs = get_pci_devices(vendor="0x1d94")
        if not pci_devs:
            return {}
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
            for path in [
                Path("/sys/module/hycu/version"),
                Path("/sys/module/hydcu/version"),
            ]:
                if path.exists():
                    with contextlib.suppress(Exception):
                        sys_driver_ver = path.read_text().strip()
                    break

            sys_runtime_ver_original = pyrocmcore.getROCmVersion()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            devs_count = pyrocmsmi.rsmi_num_monitor_devices()
            for dev_idx in range(devs_count):
                dev_index = dev_idx

                dev_uuid = f"GPU-{pyrocmsmi.rsmi_dev_unique_id_get(dev_idx)[2:]}"
                dev_hsa_agent = hsa_agents.get(dev_uuid)

                dev_name = dev_hsa_agent.name
                if not dev_name:
                    dev_name = pyrocmsmi.rsmi_dev_name_get(dev_idx)

                dev_cc = dev_hsa_agent.compute_capability
                if not dev_cc:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_cc = pyrocmsmi.rsmi_dev_target_graphics_version_get(dev_idx)

                dev_bdf = None
                dev_card_id = None
                dev_renderd_id = None
                with contextlib.suppress(Exception):
                    dev_bdf = pyrocmsmi.rsmi_dev_pci_id_get(dev_idx)
                    dev_card_id, dev_renderd_id = _get_card_and_renderd_id(dev_bdf)

                dev_cores = dev_hsa_agent.compute_units
                if not dev_cores and dev_card_id is not None:
                    with contextlib.suppress(pyamdgpu.AMDGPUError):
                        _, _, dev_gpudev = pyamdgpu.amdgpu_device_initialize(
                            dev_card_id,
                        )
                        dev_gpudev_info = pyamdgpu.amdgpu_query_gpu_info(dev_gpudev)
                        pyamdgpu.amdgpu_device_deinitialize(dev_gpudev)
                        dev_cores = dev_gpudev_info.cu_active_number

                dev_cores_util = pyrocmsmi.rsmi_dev_busy_percent_get(dev_idx)
                dev_temp = pyrocmsmi.rsmi_dev_temp_metric_get(dev_idx)
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_cores_util = 0

                dev_mem = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_total_get(dev_idx),
                )
                dev_mem_used = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_usage_get(dev_idx),
                )

                dev_power = pyrocmsmi.rsmi_dev_power_cap_get(dev_idx)
                dev_power_used = pyrocmsmi.rsmi_dev_power_get(dev_idx)

                dev_appendix = {
                    "vgpu": False,
                }
                if dev_bdf is not None:
                    dev_appendix["bdf"] = dev_bdf
                if dev_card_id is not None:
                    dev_appendix["card_id"] = dev_card_id
                if dev_renderd_id is not None:
                    dev_appendix["renderd_id"] = dev_renderd_id

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
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def get_topology(self, devices: Devices | None = None) -> Topology | None:
        """
        Get the Topology object between Hygon GPUs.

        Args:
            devices:
                The list of detected Hygon GPU devices.
                If None, detect topology for all available devices.

        Returns:
            A Topology object, or None if not supported.

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

            pyrocmsmi.rsmi_init()

            # Get NUMA and CPU affinities.
            for i, dev_i in enumerate(devices):
                # Get affinity with PCIe BDF if possible.
                if dev_i_bdf := dev_i.appendix.get("bdf", ""):
                    ret.devices_numa_affinities[i] = get_numa_node_by_bdf(
                        dev_i_bdf,
                    )
                    ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                        ret.devices_numa_affinities[i],
                    )
                # Otherwise, get affinity via ROCM SMI.
                if not ret.devices_numa_affinities[i]:
                    # Get NUMA affinity.
                    try:
                        dev_i_numa_node = pyrocmsmi.rsmi_topo_get_numa_node_number(
                            dev_i.index,
                        )
                        ret.devices_numa_affinities[i] = str(dev_i_numa_node)
                    except pyrocmsmi.ROCMSMIError:
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
            for i, dev_i in enumerate(devices):
                for j, dev_j in enumerate(devices):
                    if dev_i.index == dev_j.index or ret.devices_distances[i][j] != 0:
                        continue

                    distance = TopologyDistanceEnum.UNK
                    try:
                        link = pyrocmsmi.rsmi_topo_get_link_type(
                            dev_i.index,
                            dev_j.index,
                        )
                        link_type = link.get("type", -1)
                        link_hops = link.get("hops", -1)
                        match link_type:
                            case pyrocmsmi.ROCMSMI_IOLINK_TYPE_XGMI:
                                distance = TopologyDistanceEnum.LINK
                            case pyrocmsmi.ROCMSMI_IOLINK_TYPE_PCIE:
                                dev_i_numa, dev_j_numa = (
                                    ret.devices_numa_affinities[i],
                                    ret.devices_numa_affinities[j],
                                )
                                if dev_i_numa and dev_i_numa == dev_j_numa:
                                    distance = distance_pci_devices(
                                        dev_i.appendix.get("bdf", ""),
                                        dev_j.appendix.get("bdf", ""),
                                    )
                                else:
                                    distance = TopologyDistanceEnum.SYS
                            case pyrocmsmi.ROCMSMI_IOLINK_TYPE_XGMI:
                                distance = TopologyDistanceEnum.LINK
                            case _:
                                if link_hops == 0:
                                    distance = TopologyDistanceEnum.SELF
                    except pyrocmsmi.ROCMSMIError:
                        debug_log_exception(
                            logger,
                            "Failed to get distance between device %d and %d",
                            dev_i.index,
                            dev_j.index,
                        )

                    ret.devices_distances[i][j] = distance
                    ret.devices_distances[j][i] = distance
        except pyrocmsmi.ROCMSMIError:
            debug_log_exception(logger, "Failed to fetch topology")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process topology fetching")
            raise

        return ret


def _get_card_and_renderd_id(dev_bdf: str) -> tuple[int | None, int | None]:
    """
    Get the card ID and renderD ID for a given device bdf.

    Args:
        dev_bdf:
            The device bdf.

    Returns:
        A tuple of (card_id, renderd_id).

    """
    card_id = None
    renderd_id = None

    for path in [
        Path(f"/sys/module/hycu/drivers/pci:hycu/{dev_bdf}/drm"),
        Path(f"/sys/module/hydcu/drivers/pci:hydcu/{dev_bdf}/drm"),
    ]:
        if path.exists():
            for dir_path in path.iterdir():
                if dir_path.name.startswith("card"):
                    card_id = int(dir_path.name[4:])
                elif dir_path.name.startswith("renderD"):
                    renderd_id = int(dir_path.name[7:])
            break

    return card_id, renderd_id
