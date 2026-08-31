from __future__ import annotations as __future_annotations__

import contextlib
import logging
from functools import lru_cache
from pathlib import Path

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import Topology, pyamdgpu, pydmi, pyhsa, pyrocmsmi
from .__types__ import (
    Detector,
    Device,
    DeviceMemoryStatusEnum,
    Devices,
    ManufacturerEnum,
    TopologyDistanceEnum,
    index_mig_devices,
    merge_devices_usage,
)
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
from .amd import _get_arch_family, _get_pci_device_name_by_bdf

logger = logging.getLogger(__name__)

_DMI_MIG_CONFIG_DIR = Path("/etc/dmi_mig_config")
"""
The vendor's registry of live MIG instances: the driver writes one
dev<N>gi<G>ci<C>.conf per compute instance under ci/ the moment it is
created, and the conf's mig_uuid is the identity a workload container binds
to (DMI_MIG_VISIBLE_DEVICE=MIG-<uuid>). A variable so tests can point at a
fixture.
"""

_MIG_UUID_PREFIX = "MIG-"
"""
What the vendor's own tooling prefixes an instance UUID with, in its listing
and in the DMI_MIG_VISIBLE_DEVICE value; reported identities carry it for the
same reason.
"""


class HygonDetector(Detector):
    """
    Detect Hygon GPUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
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
        except Exception:
            debug_log_exception(logger, "Failed to initialize ROCM SMI")

        return supported

    @staticmethod
    @lru_cache(maxsize=1)
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Higon.
        pci_devs = get_pci_devices(vendor="0x1d94")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.HYGON)

    def detect_info(self) -> Devices | None:
        """
        Detect Hygon GPUs' inventory using pyrocmsmi, without usage metrics.

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
            hsa_agents = {
                hsa_agent.bdf or hsa_agent.uuid: hsa_agent
                for hsa_agent in pyhsa.get_agents()
            }

            pyrocmsmi.rsmi_init()

            # The MIG mode is a property of the node, not of a card, so it is
            # read once here; a missing library or an unreadable mode means
            # physical-only detection, exactly as without this branch.
            sys_mig_enabled = False
            try:
                pydmi.dmiInit()
                sys_mig_current, _ = pydmi.dmiGetSystemMigMode()
                sys_mig_enabled = sys_mig_current == pydmi.DMI_DEVICE_MIG_ENABLE
            except pydmi.DMIError:
                debug_log_exception(logger, "Failed to query the node MIG mode")

            # MIG devices of every MIG-enabled card, keyed by the card's
            # enumeration index, and the largest number of MIG devices a card
            # can host: both are needed to number them once every card is
            # detected, see index_mig_devices.
            devs_mig_devices: dict[int, list[dict]] = {}
            devs_mig_slots = 0

            sys_driver_ver = None
            for path in [
                Path("/sys/module/hycu/version"),
                Path("/sys/module/hydcu/version"),
            ]:
                if path.exists():
                    with contextlib.suppress(Exception):
                        sys_driver_ver = path.read_text().strip()
                    break

            sys_runtime_ver_original = pyrocmsmi.rsmi_get_rocm_version()
            sys_runtime_ver = get_brief_version(sys_runtime_ver_original)

            devs_count = pyrocmsmi.rsmi_num_monitor_devices()
            for dev_idx in range(devs_count):
                dev_index = dev_idx

                dev_uuid = f"GPU-{pyrocmsmi.rsmi_dev_unique_id_get(dev_idx)[2:]}"

                dev_bdf = pyrocmsmi.rsmi_dev_pci_id_get(dev_idx)
                dev_card_id, dev_renderd_id = _get_card_and_renderd_id(dev_bdf)

                dev_hsa_agent = (
                    hsa_agents.get(dev_bdf) or hsa_agents.get(dev_uuid) or pyhsa.Agent()
                )

                # The operator resolves the name from the local PCI ID database
                # first: pci.ids knows the board -- the subsystem vendor's name
                # for the card -- where the driver only knows the chip. The
                # driver-reported names stay as fallbacks.
                dev_name = _get_pci_device_name_by_bdf(dev_bdf)
                if not dev_name:
                    dev_name = dev_hsa_agent.name
                if not dev_name and dev_card_id is not None:
                    # The operator asks libdrm for the board's marketing name
                    # between the HSA and the driver name, so this is that step.
                    # Reached only when the two above found nothing, which is
                    # why the device is opened here rather than up front.
                    with (
                        contextlib.suppress(pyamdgpu.AMDGPUError),
                        pyamdgpu.amdgpu_device(dev_card_id) as dev_gpudev,
                    ):
                        dev_name = pyamdgpu.amdgpu_get_marketing_name(dev_gpudev)
                if not dev_name:
                    dev_name = pyrocmsmi.rsmi_dev_name_get(dev_idx)

                dev_cc = dev_hsa_agent.compute_capability
                if not dev_cc:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_cc = pyrocmsmi.rsmi_dev_target_graphics_version_get(dev_idx)

                dev_cores = dev_hsa_agent.compute_units
                dev_asic_family_id = dev_hsa_agent.asic_family_id
                if (
                    not dev_cores or not dev_asic_family_id
                ) and dev_card_id is not None:
                    with (
                        contextlib.suppress(pyamdgpu.AMDGPUError),
                        pyamdgpu.amdgpu_device(dev_card_id) as dev_gpudev,
                    ):
                        dev_gpudev_info = pyamdgpu.amdgpu_query_gpu_info(dev_gpudev)
                        if not dev_cores:
                            dev_cores = dev_gpudev_info.cu_active_number
                        if not dev_asic_family_id:
                            dev_asic_family_id = dev_gpudev_info.family_id

                dev_mem = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_total_get(dev_idx),
                )
                dev_mem_status = DeviceMemoryStatusEnum.HEALTHY
                if not envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_ecc_count = pyrocmsmi.rsmi_dev_ecc_count_get(
                            dev_idx,
                        )
                        if dev_ecc_count.uncorrectable_err > 0:
                            dev_mem_status = DeviceMemoryStatusEnum.UNHEALTHY

                # The power limit is inventory, while the used power belongs to
                # the usage query.
                dev_power = pyrocmsmi.rsmi_dev_power_cap_get(dev_idx)

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_numa = str(
                            pyrocmsmi.rsmi_topo_get_numa_node_number(dev_idx),
                        )

                dev_appendix = {
                    "arch_family": _get_arch_family(dev_asic_family_id),
                    "bdf": dev_bdf,
                }
                if dev_numa:
                    dev_appendix["numa"] = dev_numa
                if dev_card_id is not None:
                    dev_appendix["card_id"] = dev_card_id
                if dev_renderd_id is not None:
                    dev_appendix["renderd_id"] = dev_renderd_id

                if sys_mig_enabled:
                    try:
                        dev_dmi = pydmi.dmiDeviceGetHandleByPciBusId(dev_bdf)
                        dev_dmi_index = pydmi.dmiDeviceGetIndex(dev_dmi)
                    except pydmi.DMIError:
                        debug_log_exception(
                            logger,
                            "Failed to reach device %s through the DMI library",
                            dev_bdf,
                        )
                    else:
                        dev_appendix["mig"] = True
                        dev_mig_slots = 0
                        with contextlib.suppress(pydmi.DMIError):
                            dev_mig_slots = pydmi.dmiDeviceGetMaxMigDeviceCount(
                                dev_dmi,
                            )
                        devs_mig_slots = max(devs_mig_slots, dev_mig_slots)
                        # With MIG enabled HSA exposes a partition's view of
                        # the card, so dev_cores can be one slice's count or
                        # nothing; the profiles always reach the whole card.
                        dev_cores = _get_mig_physical_cores(dev_dmi) or dev_cores
                        dev_mig_devices = _get_mig_devices(
                            dev_dmi,
                            dev_dmi_index,
                            dev_mig_slots,
                            sys_driver_ver,
                            sys_runtime_ver,
                            sys_runtime_ver_original,
                            dev_cc,
                            dev_mem_status,
                            dev_power,
                            dev_bdf,
                            dev_numa,
                        )
                        dev_appendix["mig_devices"] = dev_mig_devices
                        devs_mig_devices[dev_index] = dev_mig_devices

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

            index_mig_devices(ret, devs_mig_devices, devs_mig_slots)
        except pyrocmsmi.ROCMSMIError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch Hygon GPUs' usage using pyrocmsmi.

        Args:
            devices:
                The devices to refresh, matched by UUID, MIG entries in
                ``appendix["mig_devices"]`` included.
                If None, detects the devices' information first.

        Returns:
            The devices carrying usage,
            or None if not supported.

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
            pyrocmsmi.rsmi_init()

            # The MIG mode is a property of the node, not of a card, so it is
            # read once here; a missing library or an unreadable mode means
            # physical-only usage, exactly as without this branch.
            sys_mig_enabled = False
            try:
                pydmi.dmiInit()
                sys_mig_current, _ = pydmi.dmiGetSystemMigMode()
                sys_mig_enabled = sys_mig_current == pydmi.DMI_DEVICE_MIG_ENABLE
            except pydmi.DMIError:
                debug_log_exception(logger, "Failed to query the node MIG mode")

            devs_count = pyrocmsmi.rsmi_num_monitor_devices()
            for dev_idx in range(devs_count):
                dev_uuid = f"GPU-{pyrocmsmi.rsmi_dev_unique_id_get(dev_idx)[2:]}"

                # Each reading is isolated on its own, mirroring the AMD path:
                # ROCm SMI raises on one it cannot serve, and an unwrapped call
                # would cost the whole sweep rather than the reading.
                dev_cores_util = None
                with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                    dev_cores_util = pyrocmsmi.rsmi_dev_busy_percent_get(dev_idx)
                dev_temp = None
                with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                    dev_temp = pyrocmsmi.rsmi_dev_temp_metric_get(dev_idx)
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_idx,
                    )
                    dev_cores_util = 0

                dev_mem = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_total_get(dev_idx),
                )
                dev_mem_used = byte_to_mebibyte(  # byte to MiB
                    pyrocmsmi.rsmi_dev_memory_usage_get(dev_idx),
                )
                # Health is reported by both queries, as the operator reports it
                # from DetectAccelerator and MonitorAccelerator alike.
                dev_mem_status = DeviceMemoryStatusEnum.HEALTHY
                if not envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
                    with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                        dev_ecc_count = pyrocmsmi.rsmi_dev_ecc_count_get(
                            dev_idx,
                        )
                        if dev_ecc_count.uncorrectable_err > 0:
                            dev_mem_status = DeviceMemoryStatusEnum.UNHEALTHY

                dev_power_used = None
                with contextlib.suppress(pyrocmsmi.ROCMSMIError):
                    dev_power_used = pyrocmsmi.rsmi_dev_power_get(dev_idx)

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

                if sys_mig_enabled:
                    dev_bdf = pyrocmsmi.rsmi_dev_pci_id_get(dev_idx)
                    try:
                        dev_dmi = pydmi.dmiDeviceGetHandleByPciBusId(dev_bdf)
                        dev_dmi_index = pydmi.dmiDeviceGetIndex(dev_dmi)
                    except pydmi.DMIError:
                        debug_log_exception(
                            logger,
                            "Failed to reach device %s through the DMI library",
                            dev_bdf,
                        )
                    else:
                        dev_mig_slots = 0
                        with contextlib.suppress(pydmi.DMIError):
                            dev_mig_slots = pydmi.dmiDeviceGetMaxMigDeviceCount(
                                dev_dmi,
                            )
                        usages.extend(
                            _get_mig_usages(
                                dev_dmi,
                                dev_dmi_index,
                                dev_mig_slots,
                                dev_bdf,
                                dev_temp,
                                dev_power_used,
                            ),
                        )
        except pyrocmsmi.ROCMSMIError:
            debug_log_exception(logger, "Failed to fetch devices usage")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices usage fetching")
            raise

        return merge_devices_usage(devices, usages)

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
            devices = self.detect_info()
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

            for i, dev_i in enumerate(devices):
                # Get NUMA and CPU affinities.
                ret.devices_numa_affinities[i] = dev_i.appendix.get("numa", "")
                ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                    ret.devices_numa_affinities[i],
                )

                # Get distances to other devices.
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

    for drm_path in [
        Path(f"/sys/module/hycu/drivers/pci:hycu/{dev_bdf}/drm"),
        Path(f"/sys/module/hydcu/drivers/pci:hydcu/{dev_bdf}/drm"),
    ]:
        if drm_path.exists():
            for dir_path in drm_path.iterdir():
                if dir_path.name.startswith("card"):
                    card_id = int(dir_path.name[4:])
                elif dir_path.name.startswith("renderD"):
                    renderd_id = int(dir_path.name[7:])
            break

    return card_id, renderd_id


def _get_mig_physical_cores(dev_dmi) -> int | None:
    """
    Derive the card's full core count from its GPU-instance profiles.

    A profile's CU count times its instance capacity always spans the whole
    card (e.g. 20x4, 40x2 and 80x1 all reach the C-3000's 80), which HSA's
    partition view does not. None when no profile answers.
    """
    ret = None
    for profile in range(pydmi.DMI_GPU_INSTANCE_PROFILE_COUNT):
        with contextlib.suppress(pydmi.DMIError):
            gi_prf = pydmi.dmiDeviceGetGpuInstanceProfileInfo(dev_dmi, profile)
            cores = gi_prf.cu_count * gi_prf.gi_count_max
            ret = max(ret, cores) if ret is not None else cores
    return ret


def _iter_mig_device_handles(dev_dmi, dev_mig_slots: int) -> list:
    """
    Sweep the node-global MIG device index space for this card's MIG devices.

    The index is not per-card, despite the query taking a card: it numbers
    every MIG device on the node, and an index belonging to another card
    answers NOT_FOUND. The sweep is bounded by the node's own capacity --
    device count times per-card maximum -- and stops early once the card's own
    maximum has been found. A gap or an unreadable index is skipped per index,
    never aborting the sweep.

    Args:
        dev_dmi:
            The DMI handle of the card.
        dev_mig_slots:
            The number of MIG devices the card can host.

    Returns:
        The card's MIG device handles, in index order.

    """
    if not dev_mig_slots:
        return []

    ret = []
    try:
        cards = pydmi.dmiDeviceGetCount()
    except pydmi.DMIError:
        debug_log_exception(logger, "Failed to get the DMI device count")
        return ret

    for mdev_gidx in range(dev_mig_slots * cards):
        if len(ret) >= dev_mig_slots:
            break
        try:
            mdev = pydmi.dmiDeviceGetMigDeviceHandleByIndex(dev_dmi, mdev_gidx)
        except pydmi.DMIError as e:
            if e.value not in (
                pydmi.DMI_ERROR_NOT_SUPPORTED,
                pydmi.DMI_ERROR_NOT_FOUND,
                pydmi.DMI_ERROR_INVALID_ARGUMENT,
            ):
                debug_log_exception(
                    logger,
                    "Failed to get the MIG device at index %d",
                    mdev_gidx,
                )
            continue
        ret.append(mdev)
    return ret


def _get_mig_devices(
    dev_dmi,
    dev_dmi_index: int,
    dev_mig_slots: int,
    sys_driver_ver,
    sys_runtime_ver,
    sys_runtime_ver_original,
    dev_cc,
    dev_mem_status,
    dev_power,
    dev_bdf: str,
    dev_numa,
) -> list[dict]:
    """
    Enumerate the card's current MIG devices with the same inventory detail a
    plain device carries, returned as appendix entries of the physical card
    rather than standalone devices. Empty when MIG is enabled but no instances
    exist yet.

    An entry keeps a Device's shape, so the fields the usage query owns are
    present at a Device's defaults: `_get_mig_usages` fills them.

    Each entry's `index` is the per-card discovery ordinal: index_mig_devices
    turns it into the device index once every card is detected.
    """
    ret: list[dict] = []
    for mdev_ordinal, mdev in enumerate(
        _iter_mig_device_handles(dev_dmi, dev_mig_slots),
    ):
        # Suppressed per instance, not per card: one instance refusing a read
        # must not vanish every later instance from the inventory.
        with contextlib.suppress(pydmi.DMIError):
            mdev_gi_id = pydmi.dmiDeviceGetGpuInstanceId(mdev)
            mdev_ci_id = pydmi.dmiDeviceGetComputeInstanceId(mdev)

            mdev_gi = pydmi.dmiDeviceGetGpuInstanceById(dev_dmi, mdev_gi_id)
            mdev_gi_info = pydmi.dmiGpuInstanceGetInfo(mdev_gi)
            if mdev_gi_info.device != dev_dmi.value:
                # The index space is node-global; attribute strictly by the GI
                # info's parent device, never by index ranges.
                continue

            # The library offers no UUID getter, so the identity comes from
            # the vendor's registry, as the operator's does.
            mdev_uuid = _get_mig_device_uuid(
                dev_dmi_index,
                mdev_gi_id,
                mdev_ci_id,
                dev_bdf,
            )

            # The profile carries the name, the memory and the CU count; find
            # it by sweeping the fixed slice-count space for the profile id
            # the instance reports.
            mdev_name = ""
            mdev_mem = None
            mdev_cores = None
            for width in range(1, pydmi.DMI_GPU_INSTANCE_PROFILE_COUNT + 1):
                with contextlib.suppress(pydmi.DMIError):
                    gi_prf = pydmi.dmiDeviceGetGpuInstanceProfileInfo(
                        dev_dmi,
                        width - 1,
                    )
                    if gi_prf.id != mdev_gi_info.profile_id:
                        continue
                    # memory_size_MB carries MiB despite the name.
                    mdev_mem = gi_prf.memory_size_MB
                    mdev_cores = gi_prf.cu_count
                    mdev_name = gi_prf.name.decode(errors="replace").removeprefix(
                        "MIG ",
                    )
                    break

            mdev_appendix = {
                "sliced": True,
                "mig": True,
                "bdf": dev_bdf,
                "gpu_instance_id": mdev_gi_id,
                "compute_instance_id": mdev_ci_id,
                # The GPU instance's placement, in GPU-slice units; the
                # compute instance's own placement sits inside it.
                "placement": {
                    "start": mdev_gi_info.placement.start,
                    "length": mdev_gi_info.placement.size,
                },
            }
            if dev_numa:
                mdev_appendix["numa"] = dev_numa

            ret.append(
                {
                    # The discovery ordinal, independent of skipped reads:
                    # a failed instance ahead must not renumber this one.
                    "index": mdev_ordinal,
                    "name": mdev_name,
                    "uuid": mdev_uuid,
                    "driver_version": sys_driver_ver,
                    "runtime_version": sys_runtime_ver,
                    "runtime_version_original": sys_runtime_ver_original,
                    "compute_capability": dev_cc,
                    "cores": mdev_cores,
                    "cores_utilization": 0,
                    "memory": mdev_mem,
                    "memory_used": 0,
                    "memory_utilization": 0,
                    # A partition shares its card's memory-health verdict.
                    "memory_status": dev_mem_status,
                    "temperature": None,
                    "power": dev_power,
                    "power_used": None,
                    "appendix": mdev_appendix,
                },
            )
    return ret


def _get_mig_usages(
    dev_dmi,
    dev_dmi_index: int,
    dev_mig_slots: int,
    dev_bdf: str,
    dev_temp,
    dev_power_used,
) -> Devices:
    """
    Fetch the usage of the card's current MIG devices, one UUID-keyed entry per
    instance, to merge into the card's `appendix["mig_devices"]`.

    Memory and utilization are read through each instance's own MIG device
    handle, which reports the partition's figures rather than the card's.

    Args:
        dev_dmi:
            The DMI handle of the card hosting them.
        dev_dmi_index:
            The card's DMI enumeration index, used to resolve identities from
            the vendor's instance registry.
        dev_mig_slots:
            The number of MIG devices the card can host.
        dev_bdf:
            The card's BDF, for the synthetic identity fallback.
        dev_temp:
            The card's temperature.
        dev_power_used:
            The card's used power.

    Returns:
        The MIG devices' usage, keyed by UUID.

    """
    ret: Devices = []
    for mdev in _iter_mig_device_handles(dev_dmi, dev_mig_slots):
        # Suppressed per instance, not per card: one instance refusing its
        # reads keeps the inventory's defaults while its siblings refresh.
        with contextlib.suppress(pydmi.DMIError):
            mdev_gi_id = pydmi.dmiDeviceGetGpuInstanceId(mdev)
            mdev_ci_id = pydmi.dmiDeviceGetComputeInstanceId(mdev)
            mdev_uuid = _get_mig_device_uuid(
                dev_dmi_index,
                mdev_gi_id,
                mdev_ci_id,
                dev_bdf,
            )

            mdev_mem = pydmi.dmiDeviceGetMemoryInfo(mdev)
            mdev_util = pydmi.dmiDeviceGetUtilizationRates(mdev)

            mdev_mem_total = byte_to_mebibyte(mdev_mem.total)  # byte to MiB
            mdev_mem_used = byte_to_mebibyte(mdev_mem.used)  # byte to MiB

            ret.append(
                Device(
                    uuid=mdev_uuid,
                    cores_utilization=mdev_util.gpu,
                    memory_used=mdev_mem_used,
                    memory_utilization=get_utilization(mdev_mem_used, mdev_mem_total),
                    # A MIG device reports neither temperature nor power, so it
                    # carries the card's.
                    temperature=dev_temp,
                    power_used=dev_power_used,
                ),
            )
    return ret


def _get_mig_device_uuid(
    dev_dmi_index: int,
    gi_id: int,
    ci_id: int,
    dev_bdf: str,
) -> str:
    """
    Resolve a MIG device's identity from the vendor's instance registry, the
    mig_uuid line of dev<N>gi<G>ci<C>.conf, reported as MIG-<uuid>.

    The registry is a driver convenience, not part of the library's API, so it
    can be absent or stale (e.g. instances created outside the operator); then
    the identity falls back to a synthetic MIG-<bdf>-gi<G>-ci<C>, which is
    still unique on the node, and the gap is logged at debug.

    Args:
        dev_dmi_index:
            The card's DMI enumeration index, the conf name's N.
        gi_id:
            The GPU instance ID, the conf name's G.
        ci_id:
            The compute instance ID, the conf name's C.
        dev_bdf:
            The parent card's BDF, for the synthetic fallback.

    Returns:
        The instance's identity, prefixed the way the vendor's tooling spells it.

    """
    conf = _DMI_MIG_CONFIG_DIR / "ci" / f"dev{dev_dmi_index}gi{gi_id}ci{ci_id}.conf"
    with contextlib.suppress(OSError):
        for line in conf.read_text(errors="replace").splitlines():
            key, sep, value = line.partition(":")
            if sep and key.strip() == "mig_uuid" and value.strip():
                uuid = value.strip()
                if uuid.startswith(_MIG_UUID_PREFIX):
                    return uuid
                return f"{_MIG_UUID_PREFIX}{uuid}"

    logger.debug(
        "Failed to read %s, falling back to a synthetic MIG identity",
        conf,
    )
    return f"{_MIG_UUID_PREFIX}{dev_bdf}-gi{gi_id}-ci{ci_id}"
