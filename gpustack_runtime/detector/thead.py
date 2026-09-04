from __future__ import annotations as __future_annotations__

import contextlib
import logging
import math
import threading
import time
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pyhgml
from .__types__ import (
    Detector,
    Device,
    DeviceMemoryStatusEnum,
    Devices,
    ManufacturerEnum,
    Topology,
    TopologyDistanceEnum,
    index_mig_devices,
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


class THeadDetector(Detector):
    """
    Detect T-Head PPUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
    def is_supported() -> bool:
        """
        Check if the T-Head detector is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "thead"):
            logger.debug("T-Head detection is disabled by environment variable")
            return supported

        pci_devs = THeadDetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No T-Head PCI devices found")
            return supported

        try:
            pyhgml.hgmlInit()
            supported = True
        except Exception:
            debug_log_exception(logger, "Failed to initialize HGML")

        return supported

    @staticmethod
    @lru_cache(maxsize=1)
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=Alibaba.
        pci_devs = get_pci_devices(vendor="0x1ded")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.THEAD)

    def detect_info(self) -> Devices | None:
        """
        Detect T-Head GPUs' inventory using pyhgml, without usage metrics.

        Returns:
            A list of detected T-Head GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pyhgml.hgmlInit()

            sys_driver_ver = pyhgml.hgmlSystemGetDriverVersion()

            sys_runtime_ver_original = None
            sys_runtime_ver = None
            with contextlib.suppress(pyhgml.HGMLError):
                sys_runtime_ver_original = pyhgml.hgmlSystemGetHggcDriverVersion()
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

            # MIG devices of every MIG-enabled card, keyed by the card's
            # enumeration index, and the largest number of MIG devices a card
            # can host: both are needed to number them once every card is
            # detected, see index_mig_devices.
            devs_mig_devices: dict[int, list[dict]] = {}
            devs_mig_slots = 0

            dev_count = pyhgml.hgmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pyhgml.hgmlDeviceGetHandleByIndex(dev_idx)

                dev_cc_t = pyhgml.hgmlDeviceGetHggcComputeCapability(dev)
                dev_cc = ".".join(map(str, dev_cc_t))

                dev_pci_info = pyhgml.hgmlDeviceGetPciInfo(dev)
                dev_bdf = str(dev_pci_info.busIdLegacy).lower()

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_node_affinity = pyhgml.hgmlDeviceGetMemoryAffinity(
                            dev,
                            get_numa_nodeset_size(),
                            pyhgml.HGML_AFFINITY_SCOPE_NODE,
                        )
                        dev_numa = bitmask_to_str(list(dev_node_affinity))

                # The power limit is inventory; the power actually drawn is
                # usage, and belongs to detect_usage.
                dev_power = None
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_power = pyhgml.hgmlDeviceGetPowerManagementDefaultLimit(dev)
                    dev_power = dev_power // 1000  # mW to W

                dev_mig_mode = pyhgml.HGML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_mig_mode, _ = pyhgml.hgmlDeviceGetMigMode(dev)

                dev_index = dev_idx

                # Device.index is the enumeration index, while the driver's
                # minor number goes to the appendix. Unlike NVIDIA's and
                # Iluvatar's, the T-Head device node is named after the card
                # ordinal and not after this number: the operator records it
                # purely to PROVE a node addresses the card it describes, so it
                # is left absent when the driver cannot answer rather than
                # substituted by the enumeration index -- a substituted value
                # would make a wrong ordinal look proven.
                dev_minor_number = None
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_minor_number = pyhgml.hgmlDeviceGetMinorNumber(dev)

                # Report the physical card, whether or not MIG is enabled.
                # MIG instances are partitioned on demand by the operator's
                # device-manager; they are not separate allocatable devices
                # in this inventory. A MIG-enabled card is marked ``mig``
                # in the appendix instead.

                dev_name = pyhgml.hgmlDeviceGetName(dev)

                dev_uuid = pyhgml.hgmlDeviceGetUUID(dev)

                dev_cores = None
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_cores = pyhgml.hgmlDeviceGetNumGpuCores(dev)

                dev_mem, _ = _get_memory_info(dev)
                dev_mem_status = _get_memory_status(
                    dev,
                    pyhgml.HGML_VOLATILE_ECC,
                    pyhgml.HGML_MEMORY_LOCATION_DRAM,
                )

                dev_appendix = {
                    "mig": dev_mig_mode != pyhgml.HGML_DEVICE_MIG_DISABLE,
                    "bdf": dev_bdf,
                }
                if dev_minor_number is not None:
                    dev_appendix["minor_number"] = dev_minor_number
                if dev_mig_mode != pyhgml.HGML_DEVICE_MIG_DISABLE:
                    dev_mig_slots = 0
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_mig_slots = pyhgml.hgmlDeviceGetMaxMigDeviceCount(dev)
                    devs_mig_slots = max(devs_mig_slots, dev_mig_slots)
                    dev_mig_devices = _get_mig_devices(
                        dev,
                        dev_mig_slots,
                        sys_driver_ver,
                        sys_runtime_ver,
                        sys_runtime_ver_original,
                        dev_cc,
                        dev_power,
                        dev_bdf,
                        dev_numa,
                    )
                    dev_appendix["mig_devices"] = dev_mig_devices
                    devs_mig_devices[dev_idx] = dev_mig_devices
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

            index_mig_devices(ret, devs_mig_devices, devs_mig_slots)
        except pyhgml.HGMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch T-Head GPUs' usage using pyhgml, merged into the given devices.

        Args:
            devices:
                The devices to refresh, matched by UUID, GPU/compute instance
                entries in ``appendix["mig_devices"]`` included.
                If None, detects the devices' information first.

        Returns:
            The devices carrying usage,
            or None if not supported.

        Raises:
            If there is an error during fetching.

        """
        if not self.is_supported():
            return None

        if devices is None:
            devices = self.detect_info()
        if not devices:
            return devices

        # The usage query enumerates the driver's devices on its own and returns
        # them keyed by UUID, mirroring the operator's MonitorAccelerator: a
        # metrics list is joined by device identity, never by index, as an index
        # is not stable across a re-detection.
        usages: Devices = []

        try:
            pyhgml.hgmlInit()

            dev_count = pyhgml.hgmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pyhgml.hgmlDeviceGetHandleByIndex(dev_idx)

                dev_uuid = pyhgml.hgmlDeviceGetUUID(dev)

                dev_cores_util = None
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_util_rates = pyhgml.hgmlDeviceGetUtilizationRates(dev)
                    dev_cores_util = dev_util_rates.gpu
                if dev_cores_util is None:
                    debug_log_warning(
                        logger,
                        "Failed to get device %d cores utilization, setting to 0",
                        dev_idx,
                    )
                    dev_cores_util = 0

                dev_mem, dev_mem_used = _get_memory_info(dev)
                dev_mem_status = _get_memory_status(
                    dev,
                    pyhgml.HGML_VOLATILE_ECC,
                    pyhgml.HGML_MEMORY_LOCATION_DRAM,
                )

                dev_temp = None
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_temp = pyhgml.hgmlDeviceGetTemperature(
                        dev,
                        pyhgml.HGML_TEMPERATURE_GPU,
                    )

                dev_power_used = None
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_power_used = (
                        pyhgml.hgmlDeviceGetPowerUsage(dev) // 1000
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

                dev_mig_mode = pyhgml.HGML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_mig_mode, _ = pyhgml.hgmlDeviceGetMigMode(dev)
                if dev_mig_mode != pyhgml.HGML_DEVICE_MIG_DISABLE:
                    dev_mig_slots = 0
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_mig_slots = pyhgml.hgmlDeviceGetMaxMigDeviceCount(dev)
                    usages.extend(
                        _get_mig_usages(
                            dev,
                            dev_mig_slots,
                            dev_temp,
                            dev_power_used,
                        ),
                    )
        except pyhgml.HGMLError:
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

        get_links_cache = {}

        try:
            pyhgml.hgmlInit()

            for i, dev_i in enumerate(devices):
                dev_i_bdf = dev_i.appendix.get("bdf")
                if dev_i.appendix.get("sliced", False):
                    dev_i_handle = pyhgml.hgmlDeviceGetHandleByPciBusId(dev_i_bdf)
                else:
                    dev_i_handle = pyhgml.hgmlDeviceGetHandleByUUID(dev_i.uuid)

                # Get NUMA and CPU affinities.
                ret.devices_numa_affinities[i] = dev_i.appendix.get("numa", "")
                ret.devices_cpu_affinities[i] = map_numa_node_to_cpu_affinity(
                    ret.devices_numa_affinities[i],
                )

                # Get links state if applicable.
                if dev_i_bdf in get_links_cache:
                    dev_i_links_state = get_links_cache[dev_i_bdf]
                else:
                    dev_i_links_state = _get_links_state(dev_i_handle)
                    get_links_cache[dev_i_bdf] = dev_i_links_state
                if dev_i_links_state:
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
                    if dev_i.index == dev_j.index or ret.devices_distances[i][j] != 0:
                        continue

                    dev_j_bdf = dev_j.appendix.get("bdf")
                    if dev_i_bdf == dev_j_bdf:
                        distance = TopologyDistanceEnum.SELF
                    else:
                        if dev_j.appendix.get("sliced", False):
                            dev_j_handle = pyhgml.hgmlDeviceGetHandleByPciBusId(
                                dev_j_bdf,
                            )
                        else:
                            dev_j_handle = pyhgml.hgmlDeviceGetHandleByUUID(dev_j.uuid)

                        distance = TopologyDistanceEnum.UNK
                        try:
                            distance = pyhgml.hgmlDeviceGetTopologyCommonAncestor(
                                dev_i_handle,
                                dev_j_handle,
                            )
                        except pyhgml.HGMLError:
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


def _get_memory_info(
    dev: pyhgml.c_hgmlDevice_t,
) -> tuple[int, int]:
    """
    Get a device's total and used memory.

    Args:
        dev:
            The HGML device handle.

    Returns:
        The total and used memory in MiB, both 0 if unreadable.

    """
    with contextlib.suppress(pyhgml.HGMLError):
        dev_mem_info = pyhgml.hgmlDeviceGetMemoryInfo(dev)
        return (
            byte_to_mebibyte(dev_mem_info.total),
            byte_to_mebibyte(dev_mem_info.used),
        )

    return 0, 0


def _get_memory_status(
    dev: pyhgml.c_hgmlDevice_t,
    ecc_counter_type: int,
    memory_location: int,
) -> DeviceMemoryStatusEnum:
    """
    Get a device's memory health from its uncorrected ECC error counter.

    Both queries produce it, mirroring the operator, which reports `Unhealthy`
    from `DetectAccelerator` and `MonitorAccelerator` alike. The usage query
    cannot skip it: merging usage overwrites the status, so a status it did not
    read would erase the one the information query found.

    Args:
        dev:
            The HGML device handle.
        ecc_counter_type:
            The ECC counter type to read, volatile or aggregate.
        memory_location:
            The memory location to read the counter of.

    Returns:
        The memory status.

    """
    if envs.GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK:
        return DeviceMemoryStatusEnum.HEALTHY

    try:
        dev_mem_ecc_errors = pyhgml.hgmlDeviceGetMemoryErrorCounter(
            dev,
            pyhgml.HGML_MEMORY_ERROR_TYPE_UNCORRECTED,
            ecc_counter_type,
            memory_location,
        )
        if dev_mem_ecc_errors > 0:
            return DeviceMemoryStatusEnum.UNHEALTHY
    except pyhgml.HGMLError as e:
        # Fail closed: a query the driver errors on marks the device
        # unhealthy, while an unsupported counter means it cannot be judged.
        if e.value != pyhgml.HGML_ERROR_NOT_SUPPORTED:
            return DeviceMemoryStatusEnum.UNHEALTHY

    return DeviceMemoryStatusEnum.HEALTHY


def _get_gpm_metrics(
    metrics: list[int],
    dev: pyhgml.c_hgmlDevice_t,
    gpu_instance_id: int | None = None,
    interval: float = 0.1,
) -> list[pyhgml.c_hgmlGpmMetric_t] | None:
    """
    Get GPM metrics for a device or a MIG GPU instance.

    Args:
        metrics:
            A list of GPM metric IDs to query.
        dev:
            The HGML device handle.
        gpu_instance_id:
            The GPU instance ID for MIG devices.
        interval:
            Interval in seconds between two samples.

    Returns:
        A list of GPM metric structures, or None if failed.

    """
    try:
        dev_gpm_support = pyhgml.hgmlGpmQueryDeviceSupport(dev)
        if not bool(dev_gpm_support.isSupportedDevice):
            return None
    except pyhgml.HGMLError:
        debug_log_warning(logger, "Unsupported GPM query")
        return None

    dev_gpm_metrics = pyhgml.c_hgmlGpmMetricsGet_t()
    try:
        dev_gpm_metrics.sample1 = pyhgml.hgmlGpmSampleAlloc()
        dev_gpm_metrics.sample2 = pyhgml.hgmlGpmSampleAlloc()
        if gpu_instance_id is None:
            pyhgml.hgmlGpmSampleGet(dev, dev_gpm_metrics.sample1)
            time.sleep(interval)
            pyhgml.hgmlGpmSampleGet(dev, dev_gpm_metrics.sample2)
        else:
            pyhgml.hgmlGpmMigSampleGet(dev, gpu_instance_id, dev_gpm_metrics.sample1)
            time.sleep(interval)
            pyhgml.hgmlGpmMigSampleGet(dev, gpu_instance_id, dev_gpm_metrics.sample2)
        dev_gpm_metrics.version = pyhgml.HGML_GPM_METRICS_GET_VERSION
        dev_gpm_metrics.numMetrics = len(metrics)
        for metric_idx, metric in enumerate(metrics):
            dev_gpm_metrics.metrics[metric_idx].metricId = metric
        pyhgml.hgmlGpmMetricsGet(dev_gpm_metrics)
    except pyhgml.HGMLError:
        debug_log_exception(logger, "Failed to get GPM metrics")
        return None
    finally:
        if dev_gpm_metrics.sample1:
            pyhgml.hgmlGpmSampleFree(dev_gpm_metrics.sample1)
        if dev_gpm_metrics.sample2:
            pyhgml.hgmlGpmSampleFree(dev_gpm_metrics.sample2)
    return list(dev_gpm_metrics.metrics)


_gpm_metrics_lock = threading.Lock()


def _get_sm_util_from_gpm_metrics(
    dev: pyhgml.c_hgmlDevice_t,
    gpu_instance_id: int | None = None,
    interval: float = 0.1,
) -> int | None:
    """
    Get SM utilization from GPM metrics.

    Args:
        dev:
            The HGML device handle.
        gpu_instance_id:
            The GPU instance ID for MIG devices.
        interval:
            Interval in seconds between two samples.

    Returns:
        The SM utilization as an integer percentage, or None if failed.

    """
    with _gpm_metrics_lock:
        dev_gpm_metrics = _get_gpm_metrics(
            metrics=[pyhgml.HGML_GPM_METRIC_SM_UTIL],
            dev=dev,
            gpu_instance_id=gpu_instance_id,
            interval=interval,
        )

    if dev_gpm_metrics and not math.isnan(dev_gpm_metrics[0].value):
        return int(dev_gpm_metrics[0].value)

    return None


def _extract_field_value(
    field_value: pyhgml.c_hgmlFieldValue_t,
) -> int | float | None:
    """
    Extract the value from a HGML field value structure.

    Args:
        field_value:
            The HGML field value structure.

    Returns:
        The extracted value as int, float, or None if unknown.

    """
    if field_value.hgmlReturn != pyhgml.HGML_SUCCESS:
        return None
    match field_value.valueType:
        case pyhgml.HGML_VALUE_TYPE_DOUBLE:
            return field_value.value.dVal
        case pyhgml.HGML_VALUE_TYPE_UNSIGNED_INT:
            return field_value.value.uiVal
        case pyhgml.HGML_VALUE_TYPE_UNSIGNED_LONG:
            return field_value.value.ulVal
        case pyhgml.HGML_VALUE_TYPE_UNSIGNED_LONG_LONG:
            return field_value.value.ullVal
        case pyhgml.HGML_VALUE_TYPE_SIGNED_LONG_LONG:
            return field_value.value.sllVal
        case pyhgml.HGML_VALUE_TYPE_SIGNED_INT:
            return field_value.value.siVal
    return None


def _get_links_state(
    dev: pyhgml.c_hgmlDevice_t,
) -> dict | None:
    """
    Get the ICNLink links count and state for a device.

    Args:
        dev:
            The HGML device handle.

    Returns:
        A dict includes links state or None if failed.

    """
    dev_links_count = 0
    try:
        dev_fields = pyhgml.hgmlDeviceGetFieldValues(
            dev,
            fieldIds=[pyhgml.HGML_FI_DEV_ICNLINK_LINK_COUNT],
        )
        dev_links_count = _extract_field_value(dev_fields[0])
    except pyhgml.HGMLError:
        debug_log_warning(logger, "Failed to get ICNLink links count")
    if not dev_links_count:
        return None

    dev_links_state = 0
    dev_links_active_count = 0
    try:
        for link_idx in range(int(dev_links_count)):
            dev_link_state = pyhgml.hgmlDeviceGetIcnLinkState(dev, link_idx)
            if dev_link_state:
                dev_links_state |= 1 << link_idx
                dev_links_active_count += 1
    except pyhgml.HGMLError:
        debug_log_warning(logger, "Failed to get ICNLink link state")

    return {
        "links_count": dev_links_count,
        "links_state": dev_links_state,
        "links_active_count": dev_links_active_count,
    }


def _get_mig_devices(
    dev,
    dev_mig_slots: int,
    sys_driver_ver,
    sys_runtime_ver,
    sys_runtime_ver_original,
    dev_cc,
    dev_power,
    dev_bdf: str,
    dev_numa,
) -> list[dict]:
    """
    Enumerate the card's current MIG devices with the same inventory detail a
    plain device carries (profile name, uuid, cores, total memory and memory
    health), returned as appendix entries of the physical card rather than
    standalone devices. Empty when MIG is enabled but no GPU instances exist
    yet. The operator has no T-Head equivalent of this enumeration; keeping it
    is a deliberate divergence.

    An entry keeps a Device's shape, so the fields the usage query owns are
    present at a Device's defaults: `_get_mig_usages` fills them.

    Each entry's `index` is the driver slot the MIG device was found at:
    index_mig_devices turns it into the device index once every card is
    detected.
    """
    ret: list[dict] = []
    for mdev_idx in range(dev_mig_slots):
        # Suppressed per instance, not per card: one instance refusing a read
        # used to abort the loop, so every later instance vanished from the
        # inventory. An empty slot raises here as well, which is how it is
        # skipped.
        with contextlib.suppress(pyhgml.HGMLError):
            mdev = pyhgml.hgmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)
            if not mdev:
                continue

            mdev_uuid = pyhgml.hgmlDeviceGetUUID(mdev)

            mdev_mem, _ = _get_memory_info(mdev)
            mdev_mem_status = _get_memory_status(
                mdev,
                pyhgml.HGML_AGGREGATE_ECC,
                pyhgml.HGML_MEMORY_LOCATION_SRAM,
            )

            mdev_appendix = {
                "sliced": True,
                "mig": True,
                "bdf": dev_bdf,
            }
            if dev_numa:
                mdev_appendix["numa"] = dev_numa

            mdev_gi_id = pyhgml.hgmlDeviceGetGpuInstanceId(mdev)
            mdev_appendix["gpu_instance_id"] = mdev_gi_id
            mdev_ci_id = pyhgml.hgmlDeviceGetComputeInstanceId(mdev)
            mdev_appendix["compute_instance_id"] = mdev_ci_id

            mdev_name = ""
            mdev_cores = None
            mdev_gi = pyhgml.hgmlDeviceGetGpuInstanceById(dev, mdev_gi_id)
            mdev_ci = pyhgml.hgmlGpuInstanceGetComputeInstanceById(
                mdev_gi,
                mdev_ci_id,
            )
            mdev_gi_info = pyhgml.hgmlGpuInstanceGetInfo(mdev_gi)
            mdev_ci_info = pyhgml.hgmlComputeInstanceGetInfo(mdev_ci)
            for dev_gi_prf_id in range(pyhgml.HGML_GPU_INSTANCE_PROFILE_COUNT):
                try:
                    dev_gi_prf = pyhgml.hgmlDeviceGetGpuInstanceProfileInfo(
                        dev,
                        dev_gi_prf_id,
                    )
                    if dev_gi_prf.id != mdev_gi_info.profileId:
                        continue
                except pyhgml.HGMLError:
                    continue

                for dev_ci_prf_id in range(
                    pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_COUNT,
                ):
                    for dev_cig_prf_id in range(
                        pyhgml.HGML_COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT,
                    ):
                        try:
                            mdev_ci_prf = (
                                pyhgml.hgmlGpuInstanceGetComputeInstanceProfileInfo(
                                    mdev_gi,
                                    dev_ci_prf_id,
                                    dev_cig_prf_id,
                                )
                            )
                            if mdev_ci_prf.id != mdev_ci_info.profileId:
                                continue
                        except pyhgml.HGMLError:
                            continue

                        ci_slice = _get_compute_instance_slice(dev_ci_prf_id)
                        gi_slice = _get_gpu_instance_slice(dev_gi_prf_id)
                        if ci_slice == gi_slice:
                            if hasattr(dev_gi_prf, "name"):
                                mdev_name = dev_gi_prf.name
                            else:
                                gi_mem = round(
                                    math.ceil(dev_gi_prf.memorySizeMB >> 10),
                                )
                                mdev_name = f"{gi_slice}g.{gi_mem}gb"
                        elif hasattr(mdev_ci_prf, "name"):
                            mdev_name = mdev_ci_prf.name
                        else:
                            gi_mem = round(
                                math.ceil(dev_gi_prf.memorySizeMB >> 10),
                            )
                            mdev_name = f"{ci_slice}u.{gi_slice}g.{gi_mem}gb"

                        mdev_cores = mdev_ci_prf.multiprocessorCount

                        break

                break

            ret.append(
                {
                    "index": mdev_idx,
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
                    "memory_status": mdev_mem_status,
                    "temperature": None,
                    "power": dev_power,
                    "power_used": None,
                    "appendix": mdev_appendix,
                },
            )
    return ret


def _get_mig_usages(
    dev,
    dev_mig_slots: int,
    dev_temp,
    dev_power_used,
) -> Devices:
    """
    Fetch the usage of the card's current MIG devices, one UUID-keyed entry per
    instance, to merge into the card's `appendix["mig_devices"]`.

    Args:
        dev:
            The HGML device handle of the card hosting them.
        dev_mig_slots:
            The number of MIG devices the card can host.
        dev_temp:
            The card's temperature.
        dev_power_used:
            The card's used power.

    Returns:
        The MIG devices' usage, keyed by UUID.

    """
    ret: Devices = []
    for mdev_idx in range(dev_mig_slots):
        # Suppressed per instance, not per card: one instance refusing its UUID
        # or its GPU instance id used to abort the loop, so every later instance
        # kept the inventory's defaults -- 0 % and 0 MiB, reported idle while it
        # may be running a workload. An empty slot raises here as well, which is
        # how it is skipped.
        with contextlib.suppress(pyhgml.HGMLError):
            mdev = pyhgml.hgmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)
            if not mdev:
                continue

            mdev_uuid = pyhgml.hgmlDeviceGetUUID(mdev)

            mdev_mem, mdev_mem_used = _get_memory_info(mdev)
            mdev_mem_status = _get_memory_status(
                mdev,
                pyhgml.HGML_AGGREGATE_ECC,
                pyhgml.HGML_MEMORY_LOCATION_SRAM,
            )

            mdev_gi_id = pyhgml.hgmlDeviceGetGpuInstanceId(mdev)
            mdev_cores_util = _get_sm_util_from_gpm_metrics(dev, mdev_gi_id)

            ret.append(
                Device(
                    uuid=mdev_uuid,
                    cores_utilization=mdev_cores_util,
                    memory_used=mdev_mem_used,
                    memory_utilization=get_utilization(mdev_mem_used, mdev_mem),
                    memory_status=mdev_mem_status,
                    # A MIG device reports neither temperature nor power, so it
                    # carries the card's.
                    temperature=dev_temp,
                    power_used=dev_power_used,
                ),
            )
    return ret


def _get_gpu_instance_slice(dev_gi_prf_id: int) -> int:
    """
    Get the number of slices for a given GPU Instance Profile ID.

    Args:
        dev_gi_prf_id:
            The GPU Instance Profile ID.

    Returns:
        The number of slices.

    """
    match dev_gi_prf_id:
        case (
            pyhgml.HGML_GPU_INSTANCE_PROFILE_1_SLICE
            | pyhgml.HGML_GPU_INSTANCE_PROFILE_1_SLICE_REV1
            | pyhgml.HGML_GPU_INSTANCE_PROFILE_1_SLICE_REV2
        ):
            return 1
        case (
            pyhgml.HGML_GPU_INSTANCE_PROFILE_2_SLICE
            | pyhgml.HGML_GPU_INSTANCE_PROFILE_2_SLICE_REV1
        ):
            return 2
        case pyhgml.HGML_GPU_INSTANCE_PROFILE_3_SLICE:
            return 3
        case pyhgml.HGML_GPU_INSTANCE_PROFILE_4_SLICE:
            return 4
        case pyhgml.HGML_GPU_INSTANCE_PROFILE_6_SLICE:
            return 6
        case pyhgml.HGML_GPU_INSTANCE_PROFILE_7_SLICE:
            return 7
        case pyhgml.HGML_GPU_INSTANCE_PROFILE_8_SLICE:
            return 8

    msg = f"Invalid GPU Instance Profile ID: {dev_gi_prf_id}"
    raise AttributeError(msg)


def _get_compute_instance_slice(dev_ci_prf_id: int) -> int:
    """
    Get the number of slice for a given Compute Instance Profile ID.

    Args:
        dev_ci_prf_id:
            The Compute Instance Profile ID.

    Returns:
        The number of slice.

    """
    match dev_ci_prf_id:
        case (
            pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_1_SLICE
            | pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_1_SLICE_REV1
        ):
            return 1
        case pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_2_SLICE:
            return 2
        case pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_3_SLICE:
            return 3
        case pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_4_SLICE:
            return 4
        case pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_6_SLICE:
            return 6
        case pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_7_SLICE:
            return 7
        case pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_8_SLICE:
            return 8

    msg = f"Invalid Compute Instance Profile ID: {dev_ci_prf_id}"
    raise AttributeError(msg)
