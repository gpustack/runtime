from __future__ import annotations as __future_annotations__

import contextlib
import logging
import os
import re
from functools import lru_cache
from pathlib import Path

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pydcmi
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
    get_brief_version,
    get_numa_node_by_bdf,
    get_pci_devices,
    get_utilization,
    map_cpu_affinity_to_numa_node,
    map_numa_node_to_cpu_affinity,
)

logger = logging.getLogger(__name__)
slogger = logger.getChild("internal")

_TOPOLOGY_DISTANCE_MAPPING: dict[int, int] = {
    pydcmi.DCMI_TOPO_TYPE_SELF: TopologyDistanceEnum.SELF,
    pydcmi.DCMI_TOPO_TYPE_HCCS: TopologyDistanceEnum.LINK,  # Traversing via high-speed interconnect, RoCE, etc.
    pydcmi.DCMI_TOPO_TYPE_HCCS_SW: TopologyDistanceEnum.LINK,  # Traversing via high-speed interconnect switch.
    pydcmi.DCMI_TOPO_TYPE_PIX: TopologyDistanceEnum.PIX,  # Traversing via a single PCIe bridge.
    pydcmi.DCMI_TOPO_TYPE_PXB: TopologyDistanceEnum.PXB,  # Traversing via multiple PCIe bridges without PCIe Host Bridge.
    pydcmi.DCMI_TOPO_TYPE_PHB: TopologyDistanceEnum.PHB,  # Traversing via a PCIe Host Bridge.
    pydcmi.DCMI_TOPO_TYPE_SYS: TopologyDistanceEnum.SYS,  # Traversing via SMP interconnect across other NUMA nodes.
    pydcmi.DCMI_TOPO_TYPE_SIO: TopologyDistanceEnum.SYS,  # Traversing via Super I/O or other slower interconnects.
}
"""
Mapping of Ascend topology types to distance values.
"""


class AscendDetector(Detector):
    """
    Detect Ascend NPUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
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
        except Exception as v1_error:
            # A V2-only driver refuses the V1 entry point, so this is a probe
            # result, not a verdict on the hardware -- as the operator's
            # DetectDcmiApiVersion treats it, see
            # https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-common/devmanager/devmanager_common.go.
            # Logged only if V2 fails too: on an A5 host a traceback here would
            # sit right above the V2 success line.
            try:
                pydcmi.dcmiv2_init()
                supported = True
                logger.info(
                    "Initialized DCMI through the V2 API, the V1 API being unavailable",
                )
            except Exception:
                # A card on the PCI bus but both APIs refused: the library was
                # found and rejected, not missing -- so name it.
                debug_log_exception(
                    logger,
                    "Failed to initialize DCMI through either API,"
                    " loaded from %s, the V1 API having reported: %s",
                    pydcmi.dcmi_library_path() or "nothing",
                    v1_error,
                )

        return supported

    @staticmethod
    @lru_cache(maxsize=1)
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Huawei.
        pci_devs = get_pci_devices(vendor="0x19e5")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.ASCEND)

    def detect_info(self) -> Devices | None:
        """
        Detect Ascend NPUs' inventory using pydcmi, without usage metrics.

        Returns:
            A list of detected Ascend NPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        if pydcmi.dcmi_api_version() == 2:
            return self._detect_info_v2()

        ret: Devices = []

        try:
            pydcmi.dcmi_init()

            sys_driver_ver = pydcmi.dcmi_get_driver_version()

            sys_runtime_ver_original = _get_toolkit_version()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            _, card_list = pydcmi.dcmi_get_card_list()
            for dev_card_id in card_list:
                device_num_in_card = pydcmi.dcmi_get_device_num_in_card(dev_card_id)
                for dev_device_id in range(device_num_in_card):
                    if not _is_npu_device(dev_card_id, dev_device_id):
                        continue

                    dev_chip_info = _get_device_chip_info(
                        dev_card_id,
                        dev_device_id,
                    )
                    dev_cores_aicore = dev_chip_info.aicore_cnt
                    dev_name = dev_chip_info.chip_name
                    dev_mem, _ = _get_device_memory_info(
                        dev_card_id,
                        dev_device_id,
                    )
                    dev_mem_status = _get_device_memory_status(
                        dev_card_id,
                        dev_device_id,
                    )
                    dev_index = pydcmi.dcmi_get_device_logic_id(
                        dev_card_id,
                        dev_device_id,
                    )
                    # Device.index is the logic id the driver enumerates
                    # the NPU at, while the physical id is what a device
                    # node path is made of, so the latter goes to the
                    # appendix beside the card and device ids. Mirrors the
                    # operator, which keeps a sequential Index next to
                    # PhysicalIndexes.
                    #
                    # A device whose physical id cannot be read cannot be
                    # addressed at all: /dev/davinciN is numbered by it, and
                    # the logic id is a different number, so standing in for
                    # it would hand a container another NPU's node. The
                    # operator skips such a device for the same reason.
                    try:
                        dev_physical_id = pydcmi.dcmi_get_device_phyid_from_logicid(
                            dev_index,
                        )
                    except pydcmi.DCMIError:
                        debug_log_warning(
                            logger,
                            "Failed to fetch physical id of device %d, skipping it",
                            dev_index,
                        )
                        continue

                    dev_uuid = _get_device_die(dev_card_id, dev_device_id)

                    dev_bdf = _get_device_bdf(
                        dev_card_id,
                        dev_device_id,
                    )

                    dev_numa = get_numa_node_by_bdf(dev_bdf)
                    if not dev_numa:
                        with contextlib.suppress(pydcmi.DCMIError):
                            dev_cpu_affinity = (
                                pydcmi.dcmi_get_affinity_cpu_info_by_device_id(
                                    dev_card_id,
                                    dev_device_id,
                                )
                            )
                            dev_numa = map_cpu_affinity_to_numa_node(dev_cpu_affinity)

                    dev_appendix = {
                        "arch_family": _guess_soc_name_from_dev_name(dev_name),
                        "bdf": dev_bdf,
                        "card_id": dev_card_id,
                        "device_id": dev_device_id,
                        "device_id_max": device_num_in_card - 1,
                        "physical_id": dev_physical_id,
                    }
                    if dev_numa:
                        dev_appendix["numa"] = dev_numa

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
                            memory=dev_mem,
                            memory_status=dev_mem_status,
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

    def _detect_info_v2(self) -> Devices:
        """
        Detect Ascend NPUs' inventory through the DCMI V2 API.

        V2 enumerates devices flat, indexed by the logic id that V1 reports
        through a separate call, so there is no card to walk here.

        Returns:
            A list of detected Ascend NPU devices.

        Raises:
            If there is an error during detection.

        """
        ret: Devices = []

        try:
            sys_runtime_ver_original = _get_toolkit_version()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            # V2 declares no driver version call, only the DCMI library's own
            # -- a different number, so it goes to the appendix under its own
            # name instead of standing in for driver_version.
            sys_dcmi_ver = None
            with contextlib.suppress(pydcmi.DCMIError):
                sys_dcmi_ver = pydcmi.dcmiv2_get_dcmi_version()

            for dev_index in pydcmi.dcmiv2_get_device_list():
                if not _is_npu_device_v2(dev_index):
                    continue

                dev_chip_info = pydcmi.dcmiv2_get_device_chip_info(dev_index)
                dev_cores_aicore = dev_chip_info.aicore_cnt
                dev_name = dev_chip_info.chip_name

                # V2 has no non-HBM call to fall back to, so a refusal is final
                # for this device -- dropped like an unreadable physical id
                # below, rather than hiding every other device.
                try:
                    dev_mem, _ = _get_device_memory_info_v2(dev_index)
                except pydcmi.DCMIError:
                    debug_log_warning(
                        logger,
                        "Failed to fetch memory of device %d, skipping it",
                        dev_index,
                    )
                    continue
                dev_mem_status = _get_device_memory_status_v2(dev_index)

                # As on the V1 path, a device whose physical id cannot be read
                # cannot be addressed: /dev/davinciN is numbered by it.
                try:
                    dev_physical_id = pydcmi.dcmiv2_get_chip_phy_id_by_dev_id(
                        dev_index,
                    )
                except pydcmi.DCMIError:
                    debug_log_warning(
                        logger,
                        "Failed to fetch physical id of device %d, skipping it",
                        dev_index,
                    )
                    continue

                dev_bdf = pydcmi.dcmiv2_get_device_bdf(dev_index)

                dev_uuid = _get_device_die_v2(dev_index, dev_bdf)

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    with contextlib.suppress(pydcmi.DCMIError):
                        dev_cpu_affinity = (
                            pydcmi.dcmiv2_get_affinity_cpu_info_by_dev_id(
                                dev_index,
                            )
                        )
                        dev_numa = map_cpu_affinity_to_numa_node(dev_cpu_affinity)

                # No card_id/device_id here: V2 has no card level to report,
                # and the consumers that matter -- CDI's device node, the
                # deployer's CANN variant -- read the physical id and the
                # arch family instead.
                #
                # No roce_* either, by design: V2 declares no IP call, this
                # generation addressing devices by URMA EID over UnifiedBus
                # instead. A consumer grouping by IP needs a UB-aware path.
                dev_appendix = {
                    "arch_family": _guess_soc_name_from_dev_name(dev_name),
                    "bdf": dev_bdf,
                    "physical_id": dev_physical_id,
                }
                if dev_numa:
                    dev_appendix["numa"] = dev_numa
                if sys_dcmi_ver:
                    dev_appendix["dcmi_version"] = sys_dcmi_ver

                ret.append(
                    Device(
                        manufacturer=self.manufacturer,
                        index=dev_index,
                        name=dev_name,
                        uuid=dev_uuid.upper(),
                        driver_version=None,
                        runtime_version=sys_runtime_ver,
                        runtime_version_original=sys_runtime_ver_original,
                        cores=dev_cores_aicore,
                        memory=dev_mem,
                        memory_status=dev_mem_status,
                        appendix=dev_appendix,
                    ),
                )
        except pydcmi.DCMIError:
            debug_log_exception(logger, "Failed to fetch devices through the V2 API")
            raise
        except Exception:
            debug_log_exception(
                logger,
                "Failed to process devices fetching through the V2 API",
            )
            raise

        return ret

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch Ascend NPUs' usage using pydcmi.

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

        if pydcmi.dcmi_api_version() == 2:
            return self._detect_usage_v2(devices)

        usages: Devices = []

        try:
            pydcmi.dcmi_init()

            _, card_list = pydcmi.dcmi_get_card_list()
            for dev_card_id in card_list:
                device_num_in_card = pydcmi.dcmi_get_device_num_in_card(dev_card_id)
                for dev_device_id in range(device_num_in_card):
                    # The operator filters by device type in MonitorAccelerator
                    # as well, not only when detecting.
                    if not _is_npu_device(dev_card_id, dev_device_id):
                        continue

                    dev_uuid = _get_device_die(dev_card_id, dev_device_id)

                    # The operator's MonitorAccelerator re-reads the memory
                    # rather than trusting the detection pass.
                    dev_mem, dev_mem_used = _get_device_memory_info(
                        dev_card_id,
                        dev_device_id,
                    )
                    dev_mem_status = _get_device_memory_status(
                        dev_card_id,
                        dev_device_id,
                    )

                    dev_util_aicore = None
                    with contextlib.suppress(pydcmi.DCMIError):
                        dev_util_aicore = pydcmi.dcmi_get_device_utilization_rate(
                            dev_card_id,
                            dev_device_id,
                            pydcmi.DCMI_INPUT_TYPE_AICORE,
                        )
                    if dev_util_aicore is None:
                        debug_log_warning(
                            logger,
                            "Failed to get device %d/%d cores utilization, "
                            "setting to 0",
                            dev_card_id,
                            dev_device_id,
                        )
                        dev_util_aicore = 0

                    dev_temp = None
                    with contextlib.suppress(pydcmi.DCMIError):
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

                    usages.append(
                        Device(
                            uuid=dev_uuid.upper(),
                            cores_utilization=dev_util_aicore,
                            memory_used=dev_mem_used,
                            memory_utilization=get_utilization(dev_mem_used, dev_mem),
                            memory_status=dev_mem_status,
                            temperature=dev_temp,
                            power_used=dev_power_used,
                        ),
                    )
        except pydcmi.DCMIError:
            debug_log_exception(logger, "Failed to fetch devices usage")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices usage fetching")
            raise

        return merge_devices_usage(devices, usages)

    def _detect_usage_v2(self, devices: Devices) -> Devices:
        """
        Fetch Ascend NPUs' usage through the DCMI V2 API.

        Args:
            devices:
                The devices to refresh, matched by UUID.

        Returns:
            The devices carrying usage.

        Raises:
            If there is an error during detection.

        """
        usages: Devices = []

        # The uuid a device was detected under is what the usage merges on, and
        # it may have fallen back to the address, so it is read off the device
        # rather than derived a second time -- deriving it twice is how the two
        # sides come to disagree.
        uuid_by_index = {dev.index: dev.uuid for dev in devices if dev}

        try:
            for dev_index in pydcmi.dcmiv2_get_device_list():
                dev_uuid = uuid_by_index.get(dev_index)
                if not dev_uuid:
                    continue

                # Polled path: merge_devices_usage joins by uuid, so a device
                # left out keeps its last figures. Propagating would clear
                # every device's metrics over one transient error.
                try:
                    dev_mem, dev_mem_used = _get_device_memory_info_v2(dev_index)
                except pydcmi.DCMIError:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d memory usage, skipping it this round",
                        dev_index,
                    )
                    continue
                dev_mem_status = _get_device_memory_status_v2(dev_index)

                dev_util_aicore = None
                with contextlib.suppress(pydcmi.DCMIError):
                    dev_util_aicore = pydcmi.dcmiv2_get_device_utilization_rate(
                        dev_index,
                        pydcmi.DCMI_INPUT_TYPE_AICORE,
                    )
                if dev_util_aicore is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_index,
                    )
                    dev_util_aicore = 0

                dev_temp = None
                with contextlib.suppress(pydcmi.DCMIError):
                    dev_temp = pydcmi.dcmiv2_get_device_temperature(dev_index)

                dev_power_used = None
                with contextlib.suppress(pydcmi.DCMIError):
                    dev_power_used = pydcmi.dcmiv2_get_device_power_info(dev_index)
                if dev_power_used:
                    dev_power_used = dev_power_used / 10  # 0.1W to W

                usages.append(
                    Device(
                        uuid=dev_uuid,
                        cores_utilization=dev_util_aicore,
                        memory_used=dev_mem_used,
                        memory_utilization=get_utilization(dev_mem_used, dev_mem),
                        memory_status=dev_mem_status,
                        temperature=dev_temp,
                        power_used=dev_power_used,
                    ),
                )
        except pydcmi.DCMIError:
            debug_log_exception(
                logger,
                "Failed to fetch devices usage through the V2 API",
            )
            raise
        except Exception:
            debug_log_exception(
                logger,
                "Failed to process devices usage fetching through the V2 API",
            )
            raise

        return merge_devices_usage(devices, usages)

    def get_topology(self, devices: Devices | None = None) -> Topology | None:
        """
        Get the Topology object between Ascend NPUs.

        Args:
            devices:
                The list of detected Ascend NPU devices.
                If None, detect topology for all available devices.

        Returns:
            A Topology object, or None if not supported.

        """
        # detect_topologies() hands the devices in directly, skipping
        # detect_info() and with it the call that resolves the API version --
        # leaving the V1 dcmi_init() below to raise on a V2-only driver.
        # Cached, so asking again is free.
        if not self.is_supported():
            return None

        if devices is None:
            devices = self.detect_info()
            if devices is None:
                return None

        ret = Topology(
            manufacturer=self.manufacturer,
            devices_count=len(devices),
        )

        # V2 declares no topology call at all, so the distances stay unknown
        # there. The NUMA and CPU affinities come from the appendix and are
        # reported either way.
        distances_available = pydcmi.dcmi_api_version() == 1

        try:
            # V2 needs no call here at all: the affinities are read off the
            # appendix and the distances are unavailable, so nothing has to be
            # initialized to report what can be reported.
            if distances_available:
                pydcmi.dcmi_init()

            for i, dev_i in enumerate(devices):
                dev_i_card_id = dev_i.appendix.get("card_id", i)
                dev_i_device_id = dev_i.appendix.get("device_id", 0)

                # Get NUMA and CPU affinities.
                ret.devices_numa_affinities[i] = dev_i.appendix.get("numa", "")
                ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                    ret.devices_numa_affinities[i],
                )

                # Get distances to other devices.
                if not distances_available:
                    continue
                for j, dev_j in enumerate(devices):
                    if dev_i.index == dev_j.index or ret.devices_distances[i][j] != 0:
                        continue

                    dev_j_card_id = dev_j.appendix.get("card_id", j)
                    dev_j_device_id = dev_j.appendix.get("device_id", 0)

                    # If two devices are the same card,
                    # skip distance calculation.
                    if dev_i_card_id == dev_j_card_id:
                        continue

                    distance = TopologyDistanceEnum.UNK
                    try:
                        topo = pydcmi.dcmi_get_topo_info_by_device_id(
                            dev_i_card_id,
                            dev_i_device_id,
                            dev_j_card_id,
                            dev_j_device_id,
                        )
                        distance = _TOPOLOGY_DISTANCE_MAPPING.get(topo, distance)
                    except pydcmi.DCMIError:
                        debug_log_exception(
                            slogger,
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


def _is_npu_device(dev_card_id, dev_device_id) -> bool:
    """
    Report whether the given device of the card is an NPU.

    A card also carries non-NPU units, like its MCU, which are not
    accelerators. Mirrors the operator, which skips a device only when the
    type call *succeeds* and reports something other than an NPU: a device
    whose type cannot be read is kept.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        True if the device is an NPU, or its type is unreadable.

    """
    dev_type = None
    with contextlib.suppress(pydcmi.DCMIError):
        dev_type = pydcmi.dcmi_get_device_type(dev_card_id, dev_device_id)

    if dev_type is not None and dev_type != pydcmi.DCMI_UNIT_TYPE_NPU:
        slogger.debug(
            "Skipping non-NPU device %d of card %d, type %d",
            dev_device_id,
            dev_card_id,
            dev_type,
        )
        return False

    return True


def _is_npu_device_v2(dev_id) -> bool:
    """
    Report whether the given device is an NPU, through the V2 API.

    As on the V1 path, a device whose type cannot be read is kept: only a
    reading that succeeds and says something other than NPU disqualifies it.

    Args:
        dev_id:
            The device ID of the device.

    Returns:
        True if the device is an NPU, or its type is unreadable.

    """
    dev_type = None
    with contextlib.suppress(pydcmi.DCMIError):
        dev_type = pydcmi.dcmiv2_get_device_type(dev_id)

    if dev_type is not None and dev_type != pydcmi.DCMI_UNIT_TYPE_NPU:
        slogger.debug("Skipping non-NPU device %d, type %d", dev_id, dev_type)
        return False

    return True


def _get_device_die_v2(dev_id, dev_bdf: str) -> str:
    """
    Get the device's SoC die through the V2 API, falling back to its address.

    The A5 driver reports neither die type -- both answer NOT_SUPPORT -- and a
    die is not what the uuid is needed for: it only has to tell one device from
    another, which is what the usage pass merges on and what the inventory is
    keyed by. The PCI address does that on a machine whose cards have not
    moved, where dropping the device would leave the NPU unusable outright.

    Args:
        dev_id:
            The device ID of the device.
        dev_bdf:
            The device's PCI address, used when no die can be read.

    Returns:
        The die as a string, or the PCI address.

    """
    for dev_die_type in (pydcmi.DCMI_DIE_TYPE_VDIE, pydcmi.DCMI_DIE_TYPE_NDIE):
        with contextlib.suppress(pydcmi.DCMIError):
            return pydcmi.dcmiv2_get_device_die_id(dev_id, dev_die_type)

    debug_log_warning(
        logger,
        "Failed to fetch die of device %d, identifying it by its address %s",
        dev_id,
        dev_bdf,
    )
    return dev_bdf


def _get_device_memory_info_v2(dev_id) -> tuple[int, int]:
    """
    Get device memory information through the V2 API.

    V2 declares the HBM call alone, which this generation carries anyway. The
    V1 helper's `memory_size > 0` guard reroutes a non-HBM device; with nowhere
    to reroute, repeating it here would only silence a readable zero.

    Args:
        dev_id:
            The device ID of the device.

    Returns:
        A tuple containing total memory and used memory in MiB.

    Raises:
        pydcmi.DCMIError: If the driver refuses the HBM query.

    """
    dev_hbm_info = pydcmi.dcmiv2_get_device_hbm_info(dev_id)
    return dev_hbm_info.memory_size, dev_hbm_info.memory_usage


def _get_device_memory_status_v2(dev_id) -> DeviceMemoryStatusEnum:
    """
    Get device memory ECC status through the V2 API.

    Args:
        dev_id:
            The device ID of the device.

    Returns:
        DeviceMemoryStatusEnum indicating the ECC status.

    """
    if not envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
        for dev_mem_type in [pydcmi.DCMI_DEVICE_TYPE_HBM, pydcmi.DCMI_DEVICE_TYPE_DDR]:
            with contextlib.suppress(pydcmi.DCMIError):
                dev_ecc_info = pydcmi.dcmiv2_get_device_ecc_info(
                    dev_id,
                    dev_mem_type,
                )
                if dev_ecc_info.enable_flag and (
                    dev_ecc_info.single_bit_error_cnt > 0
                    or dev_ecc_info.double_bit_error_cnt > 0
                ):
                    return DeviceMemoryStatusEnum.UNHEALTHY
                return DeviceMemoryStatusEnum.HEALTHY

    return DeviceMemoryStatusEnum.HEALTHY


def _get_device_die(dev_card_id, dev_device_id) -> str:
    """
    Get the device's SoC die, which identifies it.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        The die as a string.

    """
    try:
        return pydcmi.dcmi_get_device_die_v2(
            dev_card_id,
            dev_device_id,
            pydcmi.DCMI_DIE_TYPE_VDIE,
        )
    except pydcmi.DCMIError:
        # An older driver exposes the V1 call only, which takes no die type
        # and reports the SoC die directly. Mirrors the operator's
        # VDieHandler, which tries V2 then V1.
        return pydcmi.dcmi_get_device_die(dev_card_id, dev_device_id)


def _get_device_bdf(dev_card_id, dev_device_id) -> str:
    """
    Get the device's PCI bus address.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        The BDF as a string.

    """
    try:
        return pydcmi.dcmi_get_device_bdf(dev_card_id, dev_device_id)
    except pydcmi.DCMIError:
        # An older driver exposes the V1 PCIe call only, whose struct carries
        # no PCI domain, so the domain reads as 0 -- as the operator's
        # PcieInfoHandler.V1 leaves it when widening to the V2 struct.
        dev_pcie_info = pydcmi.dcmi_get_device_pcie_info(dev_card_id, dev_device_id)
        return (
            f"0000:{dev_pcie_info.bdf_busid:02x}:"
            f"{dev_pcie_info.bdf_deviceid:02x}.{dev_pcie_info.bdf_funcid:x}"
        )


def _get_device_chip_info(dev_card_id, dev_device_id):
    """
    Get the device's chip information.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        The chip information, carrying at least chip_name and aicore_cnt.

    """
    try:
        return pydcmi.dcmi_get_device_chip_info_v2(dev_card_id, dev_device_id)
    except pydcmi.DCMIError:
        # The binding's V2 wrapper already falls back when the symbol is
        # missing; the operator's ChipInfoHandler falls back on any failure,
        # which an older driver rejecting the V2 struct produces.
        return pydcmi.dcmi_get_device_chip_info(dev_card_id, dev_device_id)


def _get_device_memory_info(dev_card_id, dev_device_id) -> tuple[int, int]:
    """
    Get device memory information.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        A tuple containing total memory and used memory in MiB.

    """
    try:
        dev_hbm_info = pydcmi.dcmi_get_device_hbm_info(dev_card_id, dev_device_id)
        if dev_hbm_info.memory_size > 0:
            return dev_hbm_info.memory_size, dev_hbm_info.memory_usage
    except pydcmi.DCMIError as e:
        if e.value not in [
            pydcmi.DCMI_ERROR_FUNCTION_NOT_FOUND,
            pydcmi.DCMI_ERROR_NOT_SUPPORT,
            pydcmi.DCMI_ERROR_NOT_SUPPORT_IN_CONTAINER,
        ]:
            raise

    return _get_device_memory_info_without_hbm(dev_card_id, dev_device_id)


def _get_device_memory_info_without_hbm(dev_card_id, dev_device_id) -> tuple[int, int]:
    """
    Get device memory information from the non-HBM calls.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        A tuple containing total memory and used memory in MiB.

    """
    try:
        dev_memory_info = pydcmi.dcmi_get_device_memory_info_v3(
            dev_card_id,
            dev_device_id,
        )
    except pydcmi.DCMIError:
        # An older driver exposes the V2 call only, as the operator's
        # MemoryHandler.V2 uses.
        dev_memory_info_v2 = pydcmi.dcmi_get_device_memory_info_v2(
            dev_card_id,
            dev_device_id,
        )
        dev_mem = dev_memory_info_v2.memory_size
        # Divergence from the operator, deliberate: it computes
        # `memory_size - memory_available` here too, but the V2 struct has no
        # available figure at all and its conversion leaves that field zero,
        # so it reports every card as fully used -- indistinguishable from a
        # real out-of-memory condition. The utilization percentage the struct
        # does carry is the only used-memory signal on this path.
        return dev_mem, dev_mem * dev_memory_info_v2.utiliza // 100

    return (
        dev_memory_info.memory_size,
        dev_memory_info.memory_size - dev_memory_info.memory_available,
    )


def _get_device_memory_status(dev_card_id, dev_device_id) -> DeviceMemoryStatusEnum:
    """
    Get device memory ECC status.

    Args:
        dev_card_id:
            The card ID of the device.
        dev_device_id:
            The device ID of the device.

    Returns:
        DeviceMemoryStatusEnum indicating the ECC status.

    """
    if not envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
        for dev_mem_type in [pydcmi.DCMI_DEVICE_TYPE_HBM, pydcmi.DCMI_DEVICE_TYPE_DDR]:
            with contextlib.suppress(pydcmi.DCMIError):
                dev_ecc_info = pydcmi.dcmi_get_device_ecc_info(
                    dev_card_id,
                    dev_device_id,
                    dev_mem_type,
                )
                if dev_ecc_info.enable_flag and (
                    dev_ecc_info.single_bit_error_cnt > 0
                    or dev_ecc_info.double_bit_error_cnt > 0
                ):
                    return DeviceMemoryStatusEnum.UNHEALTHY
                return DeviceMemoryStatusEnum.HEALTHY

    return DeviceMemoryStatusEnum.HEALTHY


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
        debug_log_exception(logger, "Failed to get device RoCE network info")

    return ip, mask, gateway


def _get_toolkit_home() -> Path:
    """
    Resolve the Ascend toolkit home directory.

    Returns:
        The path to the Ascend toolkit home.

    """
    # Example ASCEND_TOOLKIT_HOME
    # - /usr/local/Ascend/cann
    # - /usr/local/Ascend/ascend-toolkit/latest/runtime
    toolkit_home = os.getenv("ASCEND_TOOLKIT_HOME")
    if toolkit_home:
        return Path(toolkit_home)

    default_home = Path("/usr/local/Ascend/cann")
    if default_home.is_dir():
        return default_home

    return Path("/usr/local/Ascend/ascend-toolkit/latest/runtime")


def _get_toolkit_version() -> str | None:
    """
    Read the Ascend toolkit (CANN) version from known version.info files.

    Returns:
        The toolkit version string, or None if not found.

    """
    prefix = "Version="
    toolkit_home = _get_toolkit_home()

    for vf in (
        toolkit_home / "version.info",
        toolkit_home / "share" / "info" / "runtime" / "version.info",
    ):
        if not vf.is_file():
            continue

        try:
            content = vf.read_text()
        except OSError:
            continue

        for line in content.splitlines():
            if line.startswith(prefix):
                return line[len(prefix) :].strip()

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
    "Ascend910": 250,
    "Ascend910_9392": 251,
    "Ascend910_9381": 252,
    "Ascend910_9382": 253,
    "Ascend910_9372": 254,
    "Ascend910_9362": 255,
    "Ascend910_9579": 260,
    "Ascend910_95": 260,
    "Ascend950": 260,
    "Ascend950PR": 260,
}


_910A_REGEX = re.compile(r"^910")
_910B_REGEX = re.compile(r"^(910B\d|A2G\d)")
_310P_REGEX = re.compile(r"^(310P\d?|I2\d?)")

# An A5 chip names itself "Ascend950XX" -- keeping the "Ascend" prefix the
# earlier generations drop -- so the operator matches it by prefix instead of
# by an exact name. See api.Ascend910A5Prefix in
# https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-common/api/default_name_v2.go,
# used by
# https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-docker-runtime/runtime/process/process.go.
_950_PREFIX = "Ascend950"


def _guess_soc_name_from_dev_name(dev_name: str) -> str | None:
    """
    Guess the SoC name from the device name.

    Args:
        dev_name:
            The name of the device, e.g., "910A", "310P1", etc.

    Returns:
        The guessed SoC name, or None if not found.

    """
    dev_name = dev_name.strip()
    if dev_name.startswith("Ascend"):
        dev_name = dev_name[len("Ascend") :].strip()
    soc_name = f"Ascend{dev_name}"
    if soc_name in _soc_name_version_mapping:
        return soc_name

    # https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-common/devmanager/common/utils.go#L159-L176
    #
    # The A5 prefix is matched first: a name the mapping does not carry yet,
    # like a later 950 variant, still belongs to the generation, and none of
    # the regexes below would claim it.
    if soc_name.startswith(_950_PREFIX):
        return "Ascend950"
    if _310P_REGEX.match(dev_name):
        return "Ascend310P1"
    if "310B" in dev_name:
        return "Ascend310B1"
    if _910B_REGEX.match(dev_name):
        return "Ascend910B1"
    if _910A_REGEX.match(dev_name):
        return "Ascend910A"

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
        return "910b"  # 910b/a2
    if version < 250:
        return "310b"
    if version < 260:
        return "a3"  # 910c/a3
    if version < 270:
        return "950"  # 950/a5
    return None
