from __future__ import annotations as __future_annotations__

import contextlib
import logging
import math
import time
from functools import lru_cache

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import pyhgml
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
    get_device_files,
    get_numa_node_by_bdf,
    get_numa_nodeset_size,
    get_pci_devices,
    get_physical_function_by_bdf,
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
        if envs.GPUSTACK_RUNTIME_DETECT.lower() not in ("auto", "iluvatar"):
            logger.debug("T-Head detection is disabled by environment variable")
            return supported

        pci_devs = THeadDetector.detect_pci_devices()
        if not pci_devs and not envs.GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK:
            logger.debug("No T-Head PCI devices found")
            return supported

        try:
            pyhgml.hgmlInit()
            pyhgml.hgmlShutdown()
            supported = True
        except pyhgml.HGMLError:
            debug_log_exception(logger, "Failed to initialize HGML library")

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

    def detect(self) -> Devices | None:
        """
        Detect T-Head GPUs using pyhgml.

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

            dev_count = pyhgml.hgmlDeviceGetCount()
            dev_files = None
            for dev_idx in range(dev_count):
                dev = pyhgml.hgmlDeviceGetHandleByIndex(dev_idx)

                dev_cc_t = pyhgml.hgmlDeviceGetHggcComputeCapability(dev)
                dev_cc = ".".join(map(str, dev_cc_t))

                dev_pci_info = pyhgml.hgmlDeviceGetPciInfo(dev)
                dev_bdf = str(dev_pci_info.busIdLegacy).lower()

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    dev_node_affinity = pyhgml.hgmlDeviceGetMemoryAffinity(
                        dev,
                        get_numa_nodeset_size(),
                        pyhgml.HGML_AFFINITY_SCOPE_NODE,
                    )
                    dev_numa = bitmask_to_str(list(dev_node_affinity))

                dev_mig_mode = pyhgml.HGML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pyhgml.HGMLError):
                    dev_mig_mode, _ = pyhgml.hgmlDeviceGetMigMode(dev)

                # With MIG disabled, treat as a single device.

                if dev_mig_mode == pyhgml.HGML_DEVICE_MIG_DISABLE:
                    dev_index = dev_idx
                    if envs.GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY:
                        if dev_files is None:
                            dev_files = get_device_files(
                                pattern=r"alixpu_ppu(?P<number>\d+)",
                            )
                        if len(dev_files) >= dev_count:
                            dev_file = dev_files[dev_idx]
                            if dev_file.number is not None:
                                dev_index = dev_file.number

                    dev_name = pyhgml.hgmlDeviceGetName(dev)

                    dev_uuid = pyhgml.hgmlDeviceGetUUID(dev)

                    dev_cores = None
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_cores = pyhgml.hgmlDeviceGetNumGpuCores(dev)

                    dev_cores_util = None
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_util_rates = pyhgml.hgmlDeviceGetUtilizationRates(dev)
                        dev_cores_util = dev_util_rates.gpu
                    if dev_cores_util is None:
                        debug_log_warning(
                            logger,
                            "Failed to get device %d cores utilization, setting to 0",
                            dev_index,
                        )
                        dev_cores_util = 0

                    dev_mem = 0
                    dev_mem_used = 0
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_mem_info = pyhgml.hgmlDeviceGetMemoryInfo(dev)
                        dev_mem = byte_to_mebibyte(  # byte to MiB
                            dev_mem_info.total,
                        )
                        dev_mem_used = byte_to_mebibyte(  # byte to MiB
                            dev_mem_info.used,
                        )

                    dev_temp = None
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_temp = pyhgml.hgmlDeviceGetTemperature(
                            dev,
                            pyhgml.HGML_TEMPERATURE_GPU,
                        )

                    dev_power = None
                    dev_power_used = None
                    with contextlib.suppress(pyhgml.HGMLError):
                        dev_power = pyhgml.hgmlDeviceGetPowerManagementDefaultLimit(dev)
                        dev_power = dev_power // 1000  # mW to W
                        dev_power_used = (
                            pyhgml.hgmlDeviceGetPowerUsage(dev) // 1000
                        )  # mW to W

                    dev_is_vgpu = False
                    if dev_bdf:
                        dev_is_vgpu = get_physical_function_by_bdf(dev_bdf) != dev_bdf

                    dev_appendix = {
                        "vgpu": dev_is_vgpu,
                        "bdf": dev_bdf,
                        "numa": dev_numa,
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

                    continue

                # Otherwise, get MIG devices.

                mdev_name = ""
                mdev_cores = None
                mdev_count = pyhgml.hgmlDeviceGetMaxMigDeviceCount(dev)
                for mdev_idx in range(mdev_count):
                    mdev = pyhgml.hgmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)

                    mdev_index = mdev_idx
                    mdev_uuid = pyhgml.hgmlDeviceGetUUID(mdev)

                    mdev_mem, mdev_mem_used = 0, 0
                    with contextlib.suppress(pyhgml.HGMLError):
                        mdev_mem_info = pyhgml.hgmlDeviceGetMemoryInfo(mdev)
                        byte_to_mebibyte(  # byte to MiB
                            mdev_mem_info.total,
                        )
                        byte_to_mebibyte(  # byte to MiB
                            mdev_mem_info.used,
                        )

                    mdev_temp = pyhgml.hgmlDeviceGetTemperature(
                        mdev,
                        pyhgml.HGML_TEMPERATURE_GPU,
                    )

                    mdev_power = None
                    with contextlib.suppress(pyhgml.HGMLError):
                        mdev_power = pyhgml.hgmlDeviceGetPowerManagementDefaultLimit(
                            mdev,
                        )
                        mdev_power = mdev_power // 1000  # mW to W
                    mdev_power_used = (
                        pyhgml.hgmlDeviceGetPowerUsage(mdev) // 1000
                    )  # mW to W

                    mdev_appendix = {
                        "vgpu": True,
                        "bdf": dev_bdf,
                        "numa": dev_numa,
                    }

                    mdev_gi_id = pyhgml.hgmlDeviceGetGpuInstanceId(mdev)
                    mdev_appendix["gpu_instance_id"] = mdev_gi_id
                    mdev_ci_id = pyhgml.hgmlDeviceGetComputeInstanceId(mdev)
                    mdev_appendix["compute_instance_id"] = mdev_ci_id

                    mdev_cores_util = _get_sm_util_from_gpm_metrics(dev, mdev_gi_id)

                    if not mdev_name:
                        mdev_gi = pyhgml.hgmlDeviceGetGpuInstanceById(dev, mdev_gi_id)
                        mdev_ci = pyhgml.hgmlGpuInstanceGetComputeInstanceById(
                            mdev_gi,
                            mdev_ci_id,
                        )
                        mdev_gi_info = pyhgml.hgmlGpuInstanceGetInfo(mdev_gi)
                        mdev_ci_info = pyhgml.hgmlComputeInstanceGetInfo(mdev_ci)
                        for dev_gi_prf_id in range(
                            pyhgml.HGML_GPU_INSTANCE_PROFILE_COUNT,
                        ):
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
                                        mdev_ci_prf = pyhgml.hgmlGpuInstanceGetComputeInstanceProfileInfo(
                                            mdev_gi,
                                            dev_ci_prf_id,
                                            dev_cig_prf_id,
                                        )
                                        if mdev_ci_prf.id != mdev_ci_info.profileId:
                                            continue
                                    except pyhgml.HGMLError:
                                        continue

                                    ci_slice = _get_compute_instance_slice(
                                        dev_ci_prf_id,
                                    )
                                    gi_slice = _get_gpu_instance_slice(dev_gi_prf_id)
                                    gi_mem = _get_gpu_instance_memory(
                                        dev_mem_info,
                                        dev_gi_prf,
                                    )

                                    if ci_slice == gi_slice:
                                        mdev_name = f"{gi_slice}g.{gi_mem}gb"
                                    else:
                                        mdev_name = (
                                            f"{ci_slice}u.{gi_slice}g.{gi_mem}gb"
                                        )

                                    mdev_cores = mdev_ci_prf.multiprocessorCount

                                    break

                    ret.append(
                        Device(
                            manufacturer=self.manufacturer,
                            index=mdev_index,
                            name=mdev_name,
                            uuid=mdev_uuid,
                            driver_version=sys_driver_ver,
                            runtime_version=sys_runtime_ver,
                            runtime_version_original=sys_runtime_ver_original,
                            compute_capability=dev_cc,
                            cores=mdev_cores,
                            cores_utilization=mdev_cores_util,
                            memory=mdev_mem,
                            memory_used=mdev_mem_used,
                            memory_utilization=get_utilization(mdev_mem_used, mdev_mem),
                            temperature=mdev_temp,
                            power=mdev_power,
                            power_used=mdev_power_used,
                            appendix=mdev_appendix,
                        ),
                    )
        except pyhgml.HGMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise
        finally:
            pyhgml.hgmlShutdown()

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
            pyhgml.hgmlInit()

            for i, dev_i in enumerate(devices):
                dev_i_handle = pyhgml.hgmlDeviceGetHandleByUUID(dev_i.uuid)

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
                    if dev_i.index == dev_j.index or ret.devices_distances[i][j] != 0:
                        continue

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
        finally:
            pyhgml.hgmlShutdown()

        return ret


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


def _get_gpu_instance_memory(dev_mem, dev_gi_prf) -> int:
    """
    Compute the memory size of a MIG compute instance in GiB.

    Args:
        dev_mem:
            The total memory info of the parent GPU device.
        dev_gi_prf:
            The profile info of the GPU instance.

    Returns:
        The memory size in GiB.

    """
    mem = dev_gi_prf.memorySizeMB * (1 << 20)  # MiB to byte

    gib = round(
        math.ceil(mem / dev_mem.total * 8)
        / 8
        * ((dev_mem.total + (1 << 30) - 1) / (1 << 30)),
    )
    return gib


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
