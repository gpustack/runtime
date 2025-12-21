from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from functools import lru_cache

from dataclasses_json import dataclass_json

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pyacl, pydcmi
from .__types__ import Detector, Device, Devices, ManufacturerEnum, Topology
from .__utils__ import (
    PCIDevice,
    get_brief_version,
    get_pci_devices,
    get_utilization,
    map_cpu_affinity_to_numa_node,
)

logger = logging.getLogger(__name__)
slogger = logger.getChild("internal")

_TOPOLOGY_DISTANCE_UNKNOWN: int = 100
"""
Distance value for unknown Ascend topology.
A larger value indicates a more distant relationship.
"""

_TOPOLOGY_DISTANCE_MAPPING: dict[int, int] = {
    pydcmi.DCMI_TOPO_TYPE_SELF: 0,
    pydcmi.DCMI_TOPO_TYPE_HCCS: 5,  # Traversing via high-speed interconnect, RoCE, etc.
    pydcmi.DCMI_TOPO_TYPE_PIX: 10,  # Traversing via a single PCIe bridge.
    pydcmi.DCMI_TOPO_TYPE_PXB: 20,  # Traversing via multiple PCIe bridges without PCIe Host Bridge.
    pydcmi.DCMI_TOPO_TYPE_PHB: 30,  # Traversing via a PCIe Host Bridge.
    pydcmi.DCMI_TOPO_TYPE_SYS: 50,  # Traversing via SMP interconnect across other NUMA nodes.
}
"""
Mapping of Ascend topologies to distance values.
"""

_DISTANCE_NAME_UNKNOWN: str = "UNK"
"""
Name for unknown Ascend topology.
"""

_DISTANCE_NAME_MAPPING: dict[int, str] = {
    0: "X",
    5: "LINK",
    10: "PIX",
    20: "PXB",
    30: "PHB",
    50: "SYS",
}
"""
Mapping of distance values to Ascend topology names.
"""


@dataclass_json
@dataclass
class AscendTopology(Topology):
    """
    Topology information between Ascend NPUs.
    """

    @staticmethod
    def stringify_devices_distance(distance: int) -> str:
        """
        Map the devices distance to a human-readable format.

        Args:
            distance:
                The distance between two devices.

        Returns:
            A string representing the distance.

        """
        return str(_DISTANCE_NAME_MAPPING.get(distance, _DISTANCE_NAME_UNKNOWN))

    def __init__(self, devices_count: int):
        """
        Initialize the AscendTopology object.

        Args:
            devices_count:
                Count of devices in the topology.

        """
        super().__init__(
            manufacturer=ManufacturerEnum.ASCEND,
            devices_count=devices_count,
        )


class AscendDetector(Detector):
    """
    Detect Ascend NPUs.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the Ascend detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "ascend"):
            logger.debug("Ascend detection is disabled by environment variable")
            return supported

        pci_devs = AscendDetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No Ascend PCI devices found")
            return supported

        try:
            pydcmi.dcmi_init()
            supported = True
        except pydcmi.DCMIError:
            debug_log_exception(logger, "Failed to initialize DCMI")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Huawei.
        pci_devs = get_pci_devices(vendor="0x19e5")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.ASCEND)

    def detect(self) -> Devices | None:
        """
        Detect Ascend NPUs using pydcmi.

        Returns:
            A list of detected Ascend NPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pydcmi.dcmi_init()

            sys_driver_ver = pydcmi.dcmi_get_driver_version()

            sys_runtime_ver_original = pyacl.aclsysGetCANNVersion()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            _, card_list = pydcmi.dcmi_get_card_list()
            for dev_card_id in card_list:
                device_num_in_card = pydcmi.dcmi_get_device_num_in_card(dev_card_id)
                for dev_device_id in range(device_num_in_card):
                    dev_is_vgpu = False
                    dev_virt_info = _get_device_virtual_info(
                        dev_card_id,
                        dev_device_id,
                    )
                    if (
                        dev_virt_info
                        and hasattr(dev_virt_info, "query_info")
                        and hasattr(dev_virt_info.query_info, "computing")
                    ):
                        dev_is_vgpu = True
                        dev_cores_aicore = dev_virt_info.query_info.computing.aic
                        dev_name = dev_virt_info.query_info.name
                        dev_mem, dev_mem_used = 0, 0
                        if hasattr(dev_virt_info.query_info.computing, "memory_size"):
                            dev_mem = dev_virt_info.query_info.computing.memory_size
                        dev_index = dev_virt_info.vdev_id
                    else:
                        dev_chip_info = pydcmi.dcmi_get_device_chip_info_v2(
                            dev_card_id,
                            dev_device_id,
                        )
                        dev_cores_aicore = dev_chip_info.aicore_cnt
                        dev_name = dev_chip_info.chip_name
                        dev_mem, dev_mem_used = _get_device_memory_info(
                            dev_card_id,
                            dev_device_id,
                        )
                        dev_index = pydcmi.dcmi_get_device_logic_id(
                            dev_card_id,
                            dev_device_id,
                        )
                        if envs.GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY:
                            dev_index = pydcmi.dcmi_get_device_phyid_from_logicid(
                                dev_index,
                            )
                    dev_uuid = pydcmi.dcmi_get_device_die_v2(
                        dev_card_id,
                        dev_device_id,
                        pydcmi.DCMI_DIE_TYPE_VDIE,
                    )

                    dev_util_aicore = pydcmi.dcmi_get_device_utilization_rate(
                        dev_card_id,
                        dev_device_id,
                        pydcmi.DCMI_INPUT_TYPE_AICORE,
                    )
                    if dev_util_aicore is None:
                        debug_log_warning(
                            logger,
                            "Failed to get device %d cores utilization, setting to 0",
                            dev_index,
                        )
                        dev_util_aicore = 0

                    dev_temp = pydcmi.dcmi_get_device_temperature(
                        dev_card_id,
                        dev_device_id,
                    )

                    dev_power_used = None
                    with contextlib.suppress(pydcmi.DCMIError):
                        dev_power_used = pydcmi.dcmi_get_device_power_info(
                            dev_card_id,
                            dev_device_id,
                        )
                    if dev_power_used:
                        dev_power_used = dev_power_used / 10  # 0.1W to W

                    dev_appendix = {
                        "arch_family": (
                            pyacl.aclrtGetSocName()
                            or _guess_soc_name_from_dev_name(dev_name)
                        ),
                        "vgpu": dev_is_vgpu,
                        "card_id": dev_card_id,
                        "device_id": dev_device_id,
                    }

                    dev_roce_ip, dev_roce_mask, dev_roce_gateway = (
                        _get_device_roce_network_info(
                            dev_card_id,
                            dev_device_id,
                        )
                    )
                    if dev_roce_ip:
                        dev_appendix["roce_ip"] = str(dev_roce_ip)
                    if dev_roce_mask:
                        dev_appendix["roce_mask"] = str(dev_roce_mask)
                    if dev_roce_gateway:
                        dev_appendix["roce_gateway"] = str(dev_roce_gateway)

                    ret.append(
                        Device(
                            manufacturer=self.manufacturer,
                            index=dev_index,
                            name=dev_name,
                            uuid=dev_uuid.upper(),
                            driver_version=sys_driver_ver,
                            runtime_version=sys_runtime_ver,
                            runtime_version_original=sys_runtime_ver_original,
                            cores=dev_cores_aicore,
                            cores_utilization=dev_util_aicore,
                            memory=dev_mem,
                            memory_used=dev_mem_used,
                            memory_utilization=get_utilization(dev_mem_used, dev_mem),
                            temperature=dev_temp,
                            power_used=dev_power_used,
                            appendix=dev_appendix,
                        ),
                    )
        except pydcmi.DCMIError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def get_topology(self, devices: Devices | None) -> AscendTopology | None:
        """
        Get the Topology object between Ascend NPUs.

        Args:
            devices:
                The list of detected Ascend NPU devices.
                If None, detect topology for all available devices.

        Returns:
            A Topology object, or None if not supported.

        """
        if devices is None:
            devices = self.detect()
            if devices is None:
                return None

        devices_count = len(devices)
        topology = AscendTopology(
            devices_count=devices_count,
        )

        for i, dev_i in enumerate(devices):
            try:
                cpu_affinity = pydcmi.dcmi_get_affinity_cpu_info_by_device_id(
                    dev_i.appendix["card_id"],
                    dev_i.appendix["device_id"],
                )
                topology.devices_cpu_affinities[i] = cpu_affinity
            except pydcmi.DCMIError:
                debug_log_warning(
                    slogger,
                    "Failed to get CPU affinity for device %d",
                    dev_i.index,
                )

            topology.devices_numa_affinities[i] = map_cpu_affinity_to_numa_node(
                cpu_affinity=topology.devices_cpu_affinities[i],
            )

            for j, dev_j in enumerate(devices):
                if i == j:
                    continue
                if topology.devices_distances[i][j] != 0:
                    continue

                distance = _TOPOLOGY_DISTANCE_UNKNOWN
                try:
                    topo_type = pydcmi.dcmi_get_topo_info_by_device_id(
                        dev_i.appendix["card_id"],
                        dev_i.appendix["device_id"],
                        dev_j.appendix["card_id"],
                        dev_j.appendix["device_id"],
                    )
                    distance = _TOPOLOGY_DISTANCE_MAPPING.get(
                        topo_type,
                        _TOPOLOGY_DISTANCE_UNKNOWN,
                    )
                except pydcmi.DCMIError:
                    debug_log_exception(
                        slogger,
                        "Failed to get topology info between device %d and %d",
                        dev_i.index,
                        dev_j.index,
                    )

                topology.devices_distances[i][j] = distance
                topology.devices_distances[j][i] = distance

        return topology


def _get_device_memory_info(dev_card_id, dev_device_id) -> tuple[int, int]:
    """
    Get device memory information.

    Returns:
        A tuple containing total memory and used memory in MiB.

    """
    try:
        dev_hbm_info = pydcmi.dcmi_get_device_hbm_info(dev_card_id, dev_device_id)
        if dev_hbm_info.memory_size > 0:
            dev_mem = dev_hbm_info.memory_size
            dev_mem_used = dev_hbm_info.memory_usage
        else:
            dev_memory_info = pydcmi.dcmi_get_device_memory_info_v3(
                dev_card_id,
                dev_device_id,
            )
            dev_mem = dev_memory_info.memory_size
            dev_mem_used = (
                dev_memory_info.memory_size - dev_memory_info.memory_available
            )
    except pydcmi.DCMIError as e:
        if e.value in [
            pydcmi.DCMI_ERROR_FUNCTION_NOT_FOUND,
            pydcmi.DCMI_ERROR_NOT_SUPPORT,
            pydcmi.DCMI_ERROR_NOT_SUPPORT_IN_CONTAINER,
        ]:
            dev_memory_info = pydcmi.dcmi_get_device_memory_info_v3(
                dev_card_id,
                dev_device_id,
            )
            dev_mem = dev_memory_info.memory_size
            dev_mem_used = (
                dev_memory_info.memory_size - dev_memory_info.memory_available
            )
        else:
            raise

    return dev_mem, dev_mem_used


def _get_device_roce_network_info(
    dev_card_id,
    dev_device_id,
) -> tuple[str | None, str | None, str | None]:
    """
    Get device RoCE network information.

    Returns:
        A tuple containing IP address, subnet mask, and gateway.

    """
    ip, mask, gateway = None, None, None

    try:
        ip, mask = pydcmi.dcmi_get_device_ip(
            dev_card_id,
            dev_device_id,
            pydcmi.DCMI_PORT_TYPE_ROCE_PORT,
        )
        gateway = pydcmi.dcmi_get_device_gateway(
            dev_card_id,
            dev_device_id,
            pydcmi.DCMI_PORT_TYPE_ROCE_PORT,
        )
    except pydcmi.DCMIError:
        debug_log_exception(logger, "Failed to get device roce network info")

    return ip, mask, gateway


def _get_device_virtual_info(
    dev_card_id,
    dev_device_id,
) -> pydcmi.c_dcmi_vdev_query_stru | None:
    """
    Get device virtual information.

    Returns:
        A c_dcmi_vdev_query_stru object if successful, None otherwise.

    """
    try:
        c_vdev_query_stru = pydcmi.c_dcmi_vdev_query_stru()
        pydcmi.dcmi_get_device_info(
            dev_card_id,
            dev_device_id,
            pydcmi.DCMI_MAIN_CMD_VDEV_MNG,
            pydcmi.DCMI_VMNG_SUB_CMD_GET_VDEV_RESOURCE,
            c_vdev_query_stru,
        )
    except pydcmi.DCMIError:
        debug_log_exception(logger, "Failed to get device virtual info")
    else:
        return c_vdev_query_stru

    return None


# Borrowed from https://gitcode.com/Ascend/pytorch/blob/master/torch_npu/csrc/core/npu/NpuVariables.cpp#L13-L40 and
# https://gitcode.com/Ascend/pytorch/blob/master/torch_npu/csrc/core/npu/NpuVariables.h#L5-L34.
# Ascend product category, please refer to:
# https://www.hiascend.com/document/detail/zh/AscendFAQ/ProduTech/productform/hardwaredesc_0001.html and
# https://blog.csdn.net/fuhanghang/article/details/146411242.
_soc_name_version_mapping: dict[str, int] = {
    "Ascend910PremiumA": 100,
    "Ascend910ProA": 101,
    "Ascend910A": 102,
    "Ascend910ProB": 103,
    "Ascend910B": 104,
    "Ascend310P1": 200,
    "Ascend310P2": 201,
    "Ascend310P3": 202,
    "Ascend310P4": 203,
    "Ascend310P5": 204,
    "Ascend310P7": 205,
    "Ascend910B1": 220,
    "Ascend910B2": 221,
    "Ascend910B2C": 222,
    "Ascend910B3": 223,
    "Ascend910B4": 224,
    "Ascend910B4-1": 225,
    "Ascend310B1": 240,
    "Ascend310B2": 241,
    "Ascend310B3": 242,
    "Ascend310B4": 243,
    "Ascend910_9391": 250,
    "Ascend910_9392": 251,
    "Ascend910_9381": 252,
    "Ascend910_9382": 253,
    "Ascend910_9372": 254,
    "Ascend910_9362": 255,
    "Ascend910_9579": 260,
}


def _guess_soc_name_from_dev_name(dev_name: str) -> str | None:
    """
    Guess the SoC name from the device name.

    Args:
        dev_name:
            The name of the device, e.g., "910A", "310P1", etc.

    Returns:
        The guessed SoC name, or None if not found.

    """
    soc_name = f"Ascend{dev_name}"
    if soc_name in _soc_name_version_mapping:
        return soc_name
    return None


def get_ascend_soc_version(name: str | None) -> int:
    """
    Get the Ascend SoC version based on the SoC name.

    Args:
        name:
            The name of the SoC, e.g., "Ascend910A", "Ascend310P1", etc.

    Returns:
        The corresponding version number, or -1 if not found.

    """
    if not name:
        return -1

    version = _soc_name_version_mapping.get(name)
    if version is None:
        return -1

    return version


def get_ascend_cann_variant(name: str | None) -> str | None:
    """
    Get the CANN variant based on the SoC name.

    Args:
        name:
            The name of the SoC, e.g., "Ascend910A", "Ascend310P1", etc.

    Returns:
        The corresponding cluster name, or None if not found.

    """
    if not name:
        return None

    version = get_ascend_soc_version(name)
    if version <= 0:
        return None
    if version < 200:
        return "910"
    if version < 220:
        return "310p"
    if version < 240:
        return "910b"
    if version < 250:
        return "310b"
    if version < 260:
        return "a3"  # 910c
    if version < 270:
        return "a5"  # 910d
    return None
