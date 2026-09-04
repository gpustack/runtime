from __future__ import annotations as __future_annotations__

import contextlib
import logging
import math
import threading
import time
from _ctypes import byref
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import DeviceMemoryStatusEnum, Topology, pynvml
from .__types__ import (
    Detector,
    Device,
    Devices,
    ManufacturerEnum,
    TopologyDistanceEnum,
    index_mig_devices,
    merge_devices_usage,
)
from .__utils__ import (
    PCIDevice,
    bitmask_to_str,
    byte_to_mebibyte,
    get_brief_version,
    get_memory,
    get_numa_node_by_bdf,
    get_numa_nodeset_size,
    get_pci_devices,
    get_utilization,
    map_numa_node_to_cpu_affinity,
    stringify_uuid,
)

logger = logging.getLogger(__name__)


class NVIDIADetector(Detector):
    """
    Detect NVIDIA GPUs.
    """

    @staticmethod
    @lru_cache(maxsize=1)
    def is_supported() -> bool:
        """
        Check if NVIDIA detection is supported.

        Returns:
            True if supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "nvidia"):
            logger.debug("NVIDIA detection is disabled by environment variable")
            return supported

        pci_devs = NVIDIADetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No NVIDIA PCI devices found")
            return supported

        try:
            pynvml.nvmlInit()
            supported = True
        except Exception:
            debug_log_exception(logger, "Failed to initialize NVML")

        return supported

    @staticmethod
    @lru_cache(maxsize=1)
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=NVIDIA.
        pci_devs = get_pci_devices(vendor="0x10de")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.NVIDIA)

    def detect_info(self) -> Devices | None:
        """
        Detect NVIDIA GPUs' inventory using pynvml, without usage metrics.

        Returns:
            A list of detected NVIDIA GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pynvml.nvmlInit()

            sys_driver_ver = pynvml.nvmlSystemGetDriverVersion()

            sys_runtime_ver_original = pynvml.nvmlSystemGetCudaDriverVersion()
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

            dev_count = pynvml.nvmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pynvml.nvmlDeviceGetHandleByIndex(dev_idx)

                dev_cc_t = pynvml.nvmlDeviceGetCudaComputeCapability(dev)
                dev_cc = ".".join(map(str, dev_cc_t))

                # Unversioned on purpose, unlike the memory query above: pynvml
                # exposes no v2 PCI accessor to prefer or fall back to --
                # `nvmlDeviceGetPciInfo` is its alias of `nvmlDeviceGetPciInfo_v3`,
                # a superset of the v2 structure the operator's `GetPciInfoV`
                # prefers, and `nvmlPciInfo_v2_t` is declared but unused. The
                # operator never reads the structure's string either, its
                # `GetBusId()` formatting the BDF from domain/bus/device.
                dev_pci_info = pynvml.nvmlDeviceGetPciInfo(dev)
                dev_bdf = str(dev_pci_info.busIdLegacy).lower()

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_node_affinity = pynvml.nvmlDeviceGetMemoryAffinity(
                            dev,
                            get_numa_nodeset_size(),
                            pynvml.NVML_AFFINITY_SCOPE_NODE,
                        )
                        dev_numa = bitmask_to_str(list(dev_node_affinity))

                # The power limit is inventory; the power actually drawn is
                # usage, and belongs to detect_usage.
                dev_power = None
                with contextlib.suppress(pynvml.NVMLError):
                    dev_power = pynvml.nvmlDeviceGetPowerManagementDefaultLimit(dev)
                    dev_power = dev_power // 1000  # mW to W

                dev_mig_mode = pynvml.NVML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pynvml.NVMLError):
                    dev_mig_mode, _ = pynvml.nvmlDeviceGetMigMode(dev)

                dev_index = dev_idx

                # Device.index is the enumeration index, while the driver's
                # minor number is what a device node path is made of, so the
                # latter goes to the appendix. Mirrors the operator, which
                # keeps a sequential Index next to PhysicalIndexes, and omits
                # the physical one when the driver cannot answer.
                dev_minor_number = None
                with contextlib.suppress(pynvml.NVMLError):
                    dev_minor_number = pynvml.nvmlDeviceGetMinorNumber(dev)

                # Report the physical card, whether or not MIG is enabled.
                # MIG instances are partitioned on demand by the operator's
                # device-manager; they are not separate allocatable devices
                # in this inventory. A MIG-enabled card is marked ``mig``
                # in the appendix instead.

                dev_name = pynvml.nvmlDeviceGetName(dev)

                dev_uuid = pynvml.nvmlDeviceGetUUID(dev)

                dev_cores = None
                with contextlib.suppress(pynvml.NVMLError):
                    dev_cores = pynvml.nvmlDeviceGetNumGpuCores(dev)

                # Reported as the driver reports it, i.e. what the card can
                # actually allocate. A deliberate divergence: the operator adds
                # back the ~1/16 that ECC parity carves out of a GDDR part, but
                # that capacity is not reachable, and its restored figure is a
                # display value that takes no part in allocation. Here `memory`
                # does take part, and `memory - memory_used` has to mean free
                # space, so restoring it would over-commit every GDDR card with
                # ECC enabled.
                dev_mem, _ = _get_memory_info(dev)
                dev_mem_status = _get_memory_status(
                    dev,
                    pynvml.NVML_VOLATILE_ECC,
                    pynvml.NVML_MEMORY_LOCATION_DRAM,
                )
                if dev_mem == 0:
                    # A deliberate divergence from the operator, which skips a
                    # device whose total reads 0: here it falls back to the host
                    # memory, tolerating WSL and integrated GPUs, which report
                    # no device memory of their own.
                    dev_mem, _ = get_memory()

                dev_appendix = {
                    "arch_family": _get_arch_family(dev_cc_t),
                    "mig": dev_mig_mode != pynvml.NVML_DEVICE_MIG_DISABLE,
                    "bdf": dev_bdf,
                }
                if dev_minor_number is not None:
                    dev_appendix["minor_number"] = dev_minor_number
                if dev_mig_mode != pynvml.NVML_DEVICE_MIG_DISABLE:
                    dev_mig_slots = 0
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_mig_slots = pynvml.nvmlDeviceGetMaxMigDeviceCount(dev)
                    devs_mig_slots = max(devs_mig_slots, dev_mig_slots)
                    dev_mig_devices = _get_mig_devices(
                        dev,
                        dev_mig_slots,
                        dev_cc_t,
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

                if dev_fabric_info := _get_fabric_info(dev):
                    dev_appendix.update(dev_fabric_info)

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
        except pynvml.NVMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise

        return ret

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch NVIDIA GPUs' usage using pynvml, merged into the given devices.

        Args:
            devices:
                The devices to refresh, matched by UUID, MIG entries in
                ``appendix["mig_devices"]`` included.
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
            pynvml.nvmlInit()

            dev_count = pynvml.nvmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pynvml.nvmlDeviceGetHandleByIndex(dev_idx)

                dev_uuid = pynvml.nvmlDeviceGetUUID(dev)

                dev_cores_util = _get_sm_util_from_gpm_metrics(dev)
                if dev_cores_util is None:
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_util_rates = pynvml.nvmlDeviceGetUtilizationRates(dev)
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
                    pynvml.NVML_VOLATILE_ECC,
                    pynvml.NVML_MEMORY_LOCATION_DRAM,
                )
                if dev_mem == 0:
                    # The same deliberate divergence detect_info records: a
                    # device whose total reads 0 falls back to the host memory.
                    dev_mem, dev_mem_used = get_memory()

                dev_temp = None
                with contextlib.suppress(pynvml.NVMLError):
                    dev_temp = pynvml.nvmlDeviceGetTemperature(
                        dev,
                        pynvml.NVML_TEMPERATURE_GPU,
                    )

                dev_power_used = None
                with contextlib.suppress(pynvml.NVMLError):
                    dev_power_used = (
                        pynvml.nvmlDeviceGetPowerUsage(dev) // 1000
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

                dev_mig_mode = pynvml.NVML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pynvml.NVMLError):
                    dev_mig_mode, _ = pynvml.nvmlDeviceGetMigMode(dev)
                if dev_mig_mode != pynvml.NVML_DEVICE_MIG_DISABLE:
                    dev_mig_slots = 0
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_mig_slots = pynvml.nvmlDeviceGetMaxMigDeviceCount(dev)
                    usages.extend(
                        _get_mig_usages(
                            dev,
                            dev_mig_slots,
                            dev_temp,
                            dev_power_used,
                        ),
                    )
        except pynvml.NVMLError:
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
            pynvml.nvmlInit()

            for i, dev_i in enumerate(devices):
                dev_i_bdf = dev_i.appendix.get("bdf")
                if dev_i.appendix.get("sliced", False):
                    dev_i_handle = pynvml.nvmlDeviceGetHandleByPciBusId(dev_i_bdf)
                else:
                    dev_i_handle = pynvml.nvmlDeviceGetHandleByUUID(dev_i.uuid)

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
                            dev_j_handle = pynvml.nvmlDeviceGetHandleByPciBusId(
                                dev_j_bdf,
                            )
                        else:
                            dev_j_handle = pynvml.nvmlDeviceGetHandleByUUID(dev_j.uuid)

                        distance = TopologyDistanceEnum.UNK
                        try:
                            distance = pynvml.nvmlDeviceGetTopologyCommonAncestor(
                                dev_i_handle,
                                dev_j_handle,
                            )
                        except pynvml.NVMLError:
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
    dev: pynvml.c_nvmlDevice_t,
) -> tuple[int, int]:
    """
    Get a device's total and used memory, preferring the v2 memory structure
    with a v1 fallback, as the operator's `GetMemoryInfoV` does.

    Args:
        dev:
            The NVML device handle.

    Returns:
        The total and used memory in MiB, both 0 if unreadable.

    """
    # `version` is the packed struct version the driver validates, not a plain
    # ordinal, so it is the binding's own constant. Probing for it keeps a
    # binding predating the v2 structure on the v1 path, instead of raising the
    # dependency floor for it.
    dev_mem_info_ver = getattr(pynvml, "nvmlMemory_v2", None)
    if dev_mem_info_ver is not None:
        with contextlib.suppress(pynvml.NVMLError):
            dev_mem_info = pynvml.nvmlDeviceGetMemoryInfo(
                dev,
                version=dev_mem_info_ver,
            )
            return (
                byte_to_mebibyte(dev_mem_info.total),
                byte_to_mebibyte(dev_mem_info.used),
            )

    with contextlib.suppress(pynvml.NVMLError):
        dev_mem_info = pynvml.nvmlDeviceGetMemoryInfo(dev)
        return (
            byte_to_mebibyte(dev_mem_info.total),
            byte_to_mebibyte(dev_mem_info.used),
        )

    return 0, 0


def _get_memory_status(
    dev: pynvml.c_nvmlDevice_t,
    ecc_counter_type: int,
    memory_location: int,
) -> DeviceMemoryStatusEnum:
    """
    Get a device's memory health.

    The verdict is the uncorrected ECC error counter plus the driver's
    recovery state: a GSP failure (Xid 119/154) leaves the ECC counters
    readable at zero, so the recovery action and reset status fields are
    probed as well.

    Both queries produce it, mirroring the operator, which reports `Unhealthy`
    from `DetectAccelerator` and `MonitorAccelerator` alike.

    Args:
        dev:
            The NVML device handle.
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
        dev_mem_ecc_errors = pynvml.nvmlDeviceGetMemoryErrorCounter(
            dev,
            pynvml.NVML_MEMORY_ERROR_TYPE_UNCORRECTED,
            ecc_counter_type,
            memory_location,
        )
        if dev_mem_ecc_errors > 0:
            return DeviceMemoryStatusEnum.UNHEALTHY
    except pynvml.NVMLError as e:
        # Fail closed: a query the driver errors on (a wedged GSP answers
        # NVML_ERROR_UNKNOWN after its RPC timeout) marks the card unhealthy,
        # while an unsupported counter means the card cannot be judged.
        if e.value != pynvml.NVML_ERROR_NOT_SUPPORTED:
            return DeviceMemoryStatusEnum.UNHEALTHY

    try:
        dev_fields = pynvml.nvmlDeviceGetFieldValues(
            dev,
            fieldIds=[
                pynvml.NVML_FI_DEV_GET_GPU_RECOVERY_ACTION,
                pynvml.NVML_FI_DEV_RESET_STATUS,
            ],
        )
    except pynvml.NVMLError as e:
        if e.value != pynvml.NVML_ERROR_NOT_SUPPORTED:
            return DeviceMemoryStatusEnum.UNHEALTHY
        return DeviceMemoryStatusEnum.HEALTHY
    for dev_field in dev_fields:
        # A per-field error extracts to None: cannot judge, keep the verdict.
        if _extract_field_value(dev_field):
            return DeviceMemoryStatusEnum.UNHEALTHY

    return DeviceMemoryStatusEnum.HEALTHY


def _get_gpm_metrics(
    metrics: list[int],
    dev: pynvml.c_nvmlDevice_t,
    gpu_instance_id: int | None = None,
    interval: float = 0.1,
) -> list[pynvml.c_nvmlGpmMetric_t] | None:
    """
    Get GPM metrics for a device or a MIG GPU instance.

    Args:
        metrics:
            A list of GPM metric IDs to query.
        dev:
            The NVML device handle.
        gpu_instance_id:
            The GPU instance ID for MIG devices.
        interval:
            Interval in seconds between two samples.

    Returns:
        A list of GPM metric structures, or None if failed.

    """
    try:
        dev_gpm_support = pynvml.nvmlGpmQueryDeviceSupport(dev)
        if not bool(dev_gpm_support.isSupportedDevice):
            return None
    except pynvml.NVMLError:
        debug_log_warning(logger, "Unsupported GPM query")
        return None

    dev_gpm_metrics = pynvml.c_nvmlGpmMetricsGet_t()
    try:
        dev_gpm_metrics.sample1 = pynvml.nvmlGpmSampleAlloc()
        dev_gpm_metrics.sample2 = pynvml.nvmlGpmSampleAlloc()
        if gpu_instance_id is None:
            pynvml.nvmlGpmSampleGet(dev, dev_gpm_metrics.sample1)
            time.sleep(interval)
            pynvml.nvmlGpmSampleGet(dev, dev_gpm_metrics.sample2)
        else:
            pynvml.nvmlGpmMigSampleGet(dev, gpu_instance_id, dev_gpm_metrics.sample1)
            time.sleep(interval)
            pynvml.nvmlGpmMigSampleGet(dev, gpu_instance_id, dev_gpm_metrics.sample2)
        dev_gpm_metrics.version = pynvml.NVML_GPM_METRICS_GET_VERSION
        dev_gpm_metrics.numMetrics = len(metrics)
        for metric_idx, metric in enumerate(metrics):
            dev_gpm_metrics.metrics[metric_idx].metricId = metric
        pynvml.nvmlGpmMetricsGet(dev_gpm_metrics)
    except pynvml.NVMLError:
        debug_log_exception(logger, "Failed to get GPM metrics")
        return None
    finally:
        if dev_gpm_metrics.sample1:
            pynvml.nvmlGpmSampleFree(dev_gpm_metrics.sample1)
        if dev_gpm_metrics.sample2:
            pynvml.nvmlGpmSampleFree(dev_gpm_metrics.sample2)
    return list(dev_gpm_metrics.metrics)


_gpm_metrics_lock = threading.Lock()


def _get_sm_util_from_gpm_metrics(
    dev: pynvml.c_nvmlDevice_t,
    gpu_instance_id: int | None = None,
    interval: float = 0.1,
) -> int | None:
    """
    Get SM utilization from GPM metrics.

    Args:
        dev:
            The NVML device handle.
        gpu_instance_id:
            The GPU instance ID for MIG devices.
        interval:
            Interval in seconds between two samples.

    Returns:
        The SM utilization as an integer percentage, or None if failed.

    """
    with _gpm_metrics_lock:
        dev_gpm_metrics = _get_gpm_metrics(
            metrics=[pynvml.NVML_GPM_METRIC_SM_UTIL],
            dev=dev,
            gpu_instance_id=gpu_instance_id,
            interval=interval,
        )

    if dev_gpm_metrics and not math.isnan(dev_gpm_metrics[0].value):
        return int(dev_gpm_metrics[0].value)

    return None


def _extract_field_value(
    field_value: pynvml.c_nvmlFieldValue_t,
) -> int | float | None:
    """
    Extract the value from a NVML field value structure.

    Args:
        field_value:
            The NVML field value structure.

    Returns:
        The extracted value as int, float, or None if unknown.

    """
    if field_value.nvmlReturn != pynvml.NVML_SUCCESS:
        return None
    match field_value.valueType:
        case pynvml.NVML_VALUE_TYPE_DOUBLE:
            return field_value.value.dVal
        case pynvml.NVML_VALUE_TYPE_UNSIGNED_INT:
            return field_value.value.uiVal
        case pynvml.NVML_VALUE_TYPE_UNSIGNED_LONG:
            return field_value.value.ulVal
        case pynvml.NVML_VALUE_TYPE_UNSIGNED_LONG_LONG:
            return field_value.value.ullVal
        case pynvml.NVML_VALUE_TYPE_SIGNED_LONG_LONG:
            return field_value.value.sllVal
        case pynvml.NVML_VALUE_TYPE_SIGNED_INT:
            return field_value.value.siVal
        case pynvml.NVML_VALUE_TYPE_UNSIGNED_SHORT:
            return field_value.value.usVal
    return None


def _get_fabric_info(
    dev: pynvml.c_nvmlDevice_t,
) -> dict | None:
    """
    Get the NVSwitch fabric information for a device.

    Args:
        dev:
            The NVML device handle.

    Returns:
        A dict includes fabric info or None if failed.

    """
    try:
        dev_fabric = pynvml.c_nvmlGpuFabricInfoV_t()
        ret = pynvml.nvmlDeviceGetGpuFabricInfoV(dev, byref(dev_fabric))
        if ret != pynvml.NVML_SUCCESS:
            return None
        if dev_fabric.state != pynvml.NVML_GPU_FABRIC_STATE_COMPLETED:
            return None
        return {
            "fabric_cluster_uuid": stringify_uuid(bytes(dev_fabric.clusterUuid)),
            "fabric_clique_id": dev_fabric.cliqueId,
        }
    except pynvml.NVMLError:
        debug_log_warning(logger, "Failed to get NVSwitch fabric info")

    return None


def _get_links_state(
    dev: pynvml.c_nvmlDevice_t,
) -> dict | None:
    """
    Get the NVLink links count and state for a device.

    Args:
        dev:
            The NVML device handle.

    Returns:
        A dict includes links state or None if failed.

    """
    dev_links_count = 0
    try:
        dev_fields = pynvml.nvmlDeviceGetFieldValues(
            dev,
            fieldIds=[pynvml.NVML_FI_DEV_NVLINK_LINK_COUNT],
        )
        dev_links_count = _extract_field_value(dev_fields[0])
    except pynvml.NVMLError:
        debug_log_warning(logger, "Failed to get NVLink links count")
    if not dev_links_count:
        return None

    dev_links_state = 0
    dev_links_active_count = 0
    try:
        for link_idx in range(int(dev_links_count)):
            dev_link_state = pynvml.nvmlDeviceGetNvLinkState(dev, link_idx)
            if dev_link_state:
                dev_links_state |= 1 << link_idx
                dev_links_active_count += 1
    except pynvml.NVMLError:
        debug_log_warning(logger, "Failed to get NVLink link state")

    return {
        "links_count": dev_links_count,
        "links_state": dev_links_state,
        "links_active_count": dev_links_active_count,
    }


def _get_mig_devices(
    dev,
    dev_mig_slots: int,
    dev_cc_t,
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
    yet.

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
        with contextlib.suppress(pynvml.NVMLError):
            mdev = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)
            if not mdev:
                continue

            mdev_uuid = pynvml.nvmlDeviceGetUUID(mdev)

            # A MIG device reports the partition's size, which carries no ECC
            # reserve to restore: MIG-capable cards are HBM parts.
            mdev_mem, _ = _get_memory_info(mdev)
            mdev_mem_status = _get_memory_status(
                mdev,
                pynvml.NVML_AGGREGATE_ECC,
                pynvml.NVML_MEMORY_LOCATION_SRAM,
            )

            mdev_appendix = {
                "arch_family": _get_arch_family(dev_cc_t),
                "sliced": True,
                "mig": True,
                "bdf": dev_bdf,
            }
            if dev_numa:
                mdev_appendix["numa"] = dev_numa

            mdev_gi_id = pynvml.nvmlDeviceGetGpuInstanceId(mdev)
            mdev_appendix["gpu_instance_id"] = mdev_gi_id
            mdev_ci_id = pynvml.nvmlDeviceGetComputeInstanceId(mdev)
            mdev_appendix["compute_instance_id"] = mdev_ci_id

            mdev_name = ""
            mdev_cores = None
            mdev_gi = pynvml.nvmlDeviceGetGpuInstanceById(dev, mdev_gi_id)
            mdev_ci = pynvml.nvmlGpuInstanceGetComputeInstanceById(
                mdev_gi,
                mdev_ci_id,
            )
            mdev_gi_info = pynvml.nvmlGpuInstanceGetInfo(mdev_gi)
            mdev_ci_info = pynvml.nvmlComputeInstanceGetInfo(mdev_ci)
            for dev_gi_prf_id in range(pynvml.NVML_GPU_INSTANCE_PROFILE_COUNT):
                try:
                    dev_gi_prf = pynvml.nvmlDeviceGetGpuInstanceProfileInfo(
                        dev,
                        dev_gi_prf_id,
                    )
                    if dev_gi_prf.id != mdev_gi_info.profileId:
                        continue
                except pynvml.NVMLError:
                    continue

                gi_mem = round(math.ceil(dev_gi_prf.memorySizeMB >> 10))
                gi_prf_name = getattr(dev_gi_prf, "name", None)
                mdev_name = (
                    gi_prf_name.removeprefix("MIG ")
                    if gi_prf_name
                    else f"{dev_gi_prf.sliceCount}g.{gi_mem}gb"
                )

                for dev_ci_prf_id in range(
                    pynvml.NVML_COMPUTE_INSTANCE_PROFILE_COUNT,
                ):
                    for dev_cig_prf_id in range(
                        pynvml.NVML_COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT,
                    ):
                        try:
                            mdev_ci_prf = (
                                pynvml.nvmlGpuInstanceGetComputeInstanceProfileInfo(
                                    mdev_gi,
                                    dev_ci_prf_id,
                                    dev_cig_prf_id,
                                )
                            )
                            if mdev_ci_prf.id != mdev_ci_info.profileId:
                                continue
                        except pynvml.NVMLError:
                            continue
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
            The NVML device handle of the card hosting them.
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
        with contextlib.suppress(pynvml.NVMLError):
            mdev = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)
            if not mdev:
                continue

            mdev_uuid = pynvml.nvmlDeviceGetUUID(mdev)

            mdev_mem, mdev_mem_used = _get_memory_info(mdev)
            mdev_mem_status = _get_memory_status(
                mdev,
                pynvml.NVML_AGGREGATE_ECC,
                pynvml.NVML_MEMORY_LOCATION_SRAM,
            )

            mdev_gi_id = pynvml.nvmlDeviceGetGpuInstanceId(mdev)
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


def _get_arch_family(dev_cc_t: list[int]) -> str:
    """
    Get the architecture family based on the CUDA compute capability.

    Args:
        dev_cc_t:
            The CUDA compute capability as a list of two integers.

    Returns:
        The architecture family as a string.

    """
    match dev_cc_t[0]:
        case 1:
            return "Tesla"
        case 2:
            return "Fermi"
        case 3:
            return "Kepler"
        case 5:
            return "Maxwell"
        case 6:
            return "Pascal"
        case 7:
            return "Volta" if dev_cc_t[1] < 5 else "Turing"
        case 8:
            if dev_cc_t[1] < 9:
                return "Ampere"
            return "Ada-Lovelace"
        case 9:
            return "Hopper"
        case 10 | 12:
            return "Blackwell"
    return "Unknown"
