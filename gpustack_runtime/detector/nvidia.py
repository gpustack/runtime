from __future__ import annotations

import contextlib
import logging
import math
import time
from _ctypes import byref
from functools import lru_cache

import pynvml

from .. import envs
from ..logging import debug_log_exception, debug_log_warning
from . import Topology, pycuda
from .__types__ import Detector, Device, Devices, ManufacturerEnum, TopologyDistanceEnum
from .__utils__ import (
    PCIDevice,
    bitmask_to_str,
    byte_to_mebibyte,
    get_brief_version,
    get_device_files,
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
    @lru_cache
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
            pynvml.nvmlShutdown()
            supported = True
        except pynvml.NVMLError:
            debug_log_exception(logger, "Failed to initialize NVML")

        return supported

    @staticmethod
    @lru_cache
    def detect_pci_devices() -> dict[str, PCIDevice]:
        # See https://pcisig.com/membership/member-companies?combine=NVIDIA.
        pci_devs = get_pci_devices(vendor="0x10de")
        if not pci_devs:
            return {}
        return {dev.address: dev for dev in pci_devs}

    def __init__(self):
        super().__init__(ManufacturerEnum.NVIDIA)

    def detect(self) -> Devices | None:
        """
        Detect NVIDIA GPUs using pynvml.

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
            pci_devs = NVIDIADetector.detect_pci_devices()

            pynvml.nvmlInit()
            if not envs.GPUSTACK_RUNTIME_DETECT_NO_TOOLKIT_CALL:
                try:
                    pycuda.cuInit()
                except pycuda.CUDAError:
                    debug_log_exception(logger, "Failed to initialize CUDA")

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

            dev_count = pynvml.nvmlDeviceGetCount()
            dev_files = None
            for dev_idx in range(dev_count):
                dev = pynvml.nvmlDeviceGetHandleByIndex(dev_idx)

                dev_cc_t = pynvml.nvmlDeviceGetCudaComputeCapability(dev)
                dev_cc = ".".join(map(str, dev_cc_t))

                dev_pci_info = pynvml.nvmlDeviceGetPciInfo(dev)
                dev_bdf = str(dev_pci_info.busIdLegacy).lower()

                dev_numa = get_numa_node_by_bdf(dev_bdf)
                if not dev_numa:
                    dev_node_affinity = pynvml.nvmlDeviceGetMemoryAffinity(
                        dev,
                        get_numa_nodeset_size(),
                        pynvml.NVML_AFFINITY_SCOPE_NODE,
                    )
                    dev_numa = bitmask_to_str(list(dev_node_affinity))

                dev_mig_mode = pynvml.NVML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pynvml.NVMLError):
                    dev_mig_mode, _ = pynvml.nvmlDeviceGetMigMode(dev)

                # With MIG disabled, treat as a single device.

                if dev_mig_mode == pynvml.NVML_DEVICE_MIG_DISABLE:
                    dev_index = dev_idx
                    if envs.GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY:
                        if dev_files is None:
                            dev_files = get_device_files(
                                pattern=r"nvidia(?P<number>\d+)",
                            )
                        if len(dev_files) >= dev_count:
                            dev_file = dev_files[dev_idx]
                            if dev_file.number is not None:
                                dev_index = dev_file.number

                    dev_name = pynvml.nvmlDeviceGetName(dev)

                    dev_uuid = pynvml.nvmlDeviceGetUUID(dev)

                    dev_cores = None
                    if not envs.GPUSTACK_RUNTIME_DETECT_NO_TOOLKIT_CALL:
                        with contextlib.suppress(pycuda.CUDAError):
                            dev_gpudev = pycuda.cuDeviceGet(dev_idx)
                            dev_cores = pycuda.cuDeviceGetAttribute(
                                dev_gpudev,
                                pycuda.CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT,
                            )

                    dev_cores_util = _get_sm_util_from_gpm_metrics(dev)
                    if dev_cores_util is None:
                        with contextlib.suppress(pynvml.NVMLError):
                            dev_util_rates = pynvml.nvmlDeviceGetUtilizationRates(dev)
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
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_mem_info = pynvml.nvmlDeviceGetMemoryInfo(dev)
                        dev_mem = byte_to_mebibyte(  # byte to MiB
                            dev_mem_info.total,
                        )
                        dev_mem_used = byte_to_mebibyte(  # byte to MiB
                            dev_mem_info.used,
                        )
                    if dev_mem == 0:
                        dev_mem, dev_mem_used = get_memory()

                    dev_temp = None
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_temp = pynvml.nvmlDeviceGetTemperature(
                            dev,
                            pynvml.NVML_TEMPERATURE_GPU,
                        )

                    dev_power = None
                    dev_power_used = None
                    with contextlib.suppress(pynvml.NVMLError):
                        dev_power = pynvml.nvmlDeviceGetPowerManagementDefaultLimit(dev)
                        dev_power = dev_power // 1000  # mW to W
                        dev_power_used = (
                            pynvml.nvmlDeviceGetPowerUsage(dev) // 1000
                        )  # mW to W

                    dev_is_vgpu = False
                    if dev_bdf in pci_devs:
                        dev_is_vgpu = _is_vgpu(pci_devs[dev_bdf].config)

                    dev_appendix = {
                        "arch_family": _get_arch_family(dev_cc_t),
                        "vgpu": dev_is_vgpu,
                        "bdf": dev_bdf,
                        "numa": dev_numa,
                    }

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

                # Otherwise, get MIG devices,
                # inspired by https://github.com/NVIDIA/go-nvlib/blob/fdfe25d0ffc9d7a8c166f4639ef236da81116262/pkg/nvlib/device/mig_device.go#L61-L154.

                mdev_name = ""
                mdev_cores = None
                mdev_count = pynvml.nvmlDeviceGetMaxMigDeviceCount(dev)
                for mdev_idx in range(mdev_count):
                    mdev = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)

                    mdev_index = mdev_idx
                    mdev_uuid = pynvml.nvmlDeviceGetUUID(mdev)

                    mdev_mem, mdev_mem_used = 0, 0
                    with contextlib.suppress(pynvml.NVMLError):
                        mdev_mem_info = pynvml.nvmlDeviceGetMemoryInfo(mdev)
                        mdev_mem = byte_to_mebibyte(  # byte to MiB
                            mdev_mem_info.total,
                        )
                        mdev_mem_used = byte_to_mebibyte(  # byte to MiB
                            mdev_mem_info.used,
                        )

                    mdev_temp = pynvml.nvmlDeviceGetTemperature(
                        mdev,
                        pynvml.NVML_TEMPERATURE_GPU,
                    )

                    mdev_power = None
                    with contextlib.suppress(pynvml.NVMLError):
                        mdev_power = pynvml.nvmlDeviceGetPowerManagementDefaultLimit(
                            mdev,
                        )
                        mdev_power = mdev_power // 1000  # mW to W
                    mdev_power_used = (
                        pynvml.nvmlDeviceGetPowerUsage(mdev) // 1000
                    )  # mW to W

                    mdev_appendix = {
                        "arch_family": _get_arch_family(dev_cc_t),
                        "vgpu": True,
                        "bdf": dev_bdf,
                        "numa": dev_numa,
                    }

                    mdev_gi_id = pynvml.nvmlDeviceGetGpuInstanceId(mdev)
                    mdev_appendix["gpu_instance_id"] = mdev_gi_id
                    mdev_ci_id = pynvml.nvmlDeviceGetComputeInstanceId(mdev)
                    mdev_appendix["compute_instance_id"] = mdev_ci_id

                    mdev_cores_util = _get_sm_util_from_gpm_metrics(dev, mdev_gi_id)

                    if not mdev_name:
                        mdev_gi = pynvml.nvmlDeviceGetGpuInstanceById(dev, mdev_gi_id)
                        mdev_ci = pynvml.nvmlGpuInstanceGetComputeInstanceById(
                            mdev_gi,
                            mdev_ci_id,
                        )
                        mdev_gi_info = pynvml.nvmlGpuInstanceGetInfo(mdev_gi)
                        mdev_ci_info = pynvml.nvmlComputeInstanceGetInfo(mdev_ci)
                        for dev_gi_prf_id in range(
                            pynvml.NVML_GPU_INSTANCE_PROFILE_COUNT,
                        ):
                            try:
                                dev_gi_prf = pynvml.nvmlDeviceGetGpuInstanceProfileInfo(
                                    dev,
                                    dev_gi_prf_id,
                                )
                                if dev_gi_prf.id != mdev_gi_info.profileId:
                                    continue
                            except pynvml.NVMLError:
                                continue

                            for dev_ci_prf_id in range(
                                pynvml.NVML_COMPUTE_INSTANCE_PROFILE_COUNT,
                            ):
                                for dev_cig_prf_id in range(
                                    pynvml.NVML_COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT,
                                ):
                                    try:
                                        mdev_ci_prf = pynvml.nvmlGpuInstanceGetComputeInstanceProfileInfo(
                                            mdev_gi,
                                            dev_ci_prf_id,
                                            dev_cig_prf_id,
                                        )
                                        if mdev_ci_prf.id != mdev_ci_info.profileId:
                                            continue
                                    except pynvml.NVMLError:
                                        continue

                                    ci_slice = _get_compute_instance_slice(
                                        dev_ci_prf_id,
                                    )
                                    gi_slice = _get_gpu_instance_slice(dev_gi_prf_id)
                                    gi_mem = _get_gpu_instance_memory(
                                        dev_mem_info,
                                        dev_gi_prf,
                                    )
                                    gi_attrs = _get_gpu_instance_attrs(dev_gi_prf_id)
                                    gi_neg_attrs = _get_gpu_instance_negattrs(
                                        dev_gi_prf_id,
                                    )

                                    if ci_slice == gi_slice:
                                        mdev_name = f"{gi_slice}g.{gi_mem}gb"
                                    else:
                                        mdev_name = (
                                            f"{ci_slice}c.{gi_slice}g.{gi_mem}gb"
                                        )
                                    if gi_attrs:
                                        mdev_name += f"+{gi_attrs}"
                                    if gi_neg_attrs:
                                        mdev_name += f"-{gi_neg_attrs}"

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
        except pynvml.NVMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        except Exception:
            debug_log_exception(logger, "Failed to process devices fetching")
            raise
        finally:
            pynvml.nvmlShutdown()

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
            pynvml.nvmlInit()

            for i, dev_i in enumerate(devices):
                dev_i_handle = pynvml.nvmlDeviceGetHandleByUUID(dev_i.uuid)

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
        finally:
            pynvml.nvmlShutdown()

        return ret


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


def _get_gpu_instance_slice(dev_gi_prf_id: int) -> int:
    """
    Get the number of slice for a given GPU Instance Profile ID.

    Args:
        dev_gi_prf_id:
            The GPU Instance Profile ID.

    Returns:
        The number of slices.

    """
    match dev_gi_prf_id:
        case (
            pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE
            | pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_REV1
            | pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_REV2
            | pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_GFX
            | pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_NO_ME
            | pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_ALL_ME
        ):
            return 1
        case (
            pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_REV1
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_GFX
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_NO_ME
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_ALL_ME
        ):
            return 2
        case pynvml.NVML_GPU_INSTANCE_PROFILE_3_SLICE:
            return 3
        case (
            pynvml.NVML_GPU_INSTANCE_PROFILE_4_SLICE
            | pynvml.NVML_GPU_INSTANCE_PROFILE_4_SLICE_GFX
        ):
            return 4
        case pynvml.NVML_GPU_INSTANCE_PROFILE_6_SLICE:
            return 6
        case pynvml.NVML_GPU_INSTANCE_PROFILE_7_SLICE:
            return 7
        case pynvml.NVML_GPU_INSTANCE_PROFILE_8_SLICE:
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
        The number of slices.

    """
    match dev_ci_prf_id:
        case (
            pynvml.NVML_COMPUTE_INSTANCE_PROFILE_1_SLICE
            | pynvml.NVML_COMPUTE_INSTANCE_PROFILE_1_SLICE_REV1
        ):
            return 1
        case pynvml.NVML_COMPUTE_INSTANCE_PROFILE_2_SLICE:
            return 2
        case pynvml.NVML_COMPUTE_INSTANCE_PROFILE_3_SLICE:
            return 3
        case pynvml.NVML_COMPUTE_INSTANCE_PROFILE_4_SLICE:
            return 4
        case pynvml.NVML_COMPUTE_INSTANCE_PROFILE_6_SLICE:
            return 6
        case pynvml.NVML_COMPUTE_INSTANCE_PROFILE_7_SLICE:
            return 7
        case pynvml.NVML_COMPUTE_INSTANCE_PROFILE_8_SLICE:
            return 8

    msg = f"Invalid Compute Instance Profile ID: {dev_ci_prf_id}"
    raise AttributeError(msg)


def _get_gpu_instance_attrs(dev_gi_prf_id: int) -> str:
    """
    Get attributes for a given GPU Instance Profile ID.

    Args:
        dev_gi_prf_id:
            The GPU Instance Profile ID.

    Returns:
        A string representing the attributes, or an empty string if none.

    """
    match dev_gi_prf_id:
        case (
            pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_REV1
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_REV1
        ):
            return "me"
        case (
            pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_ALL_ME
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_ALL_ME
        ):
            return "me.all"
        case (
            pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_GFX
            | pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_GFX
            | pynvml.NVML_GPU_INSTANCE_PROFILE_4_SLICE_GFX
        ):
            return "gfx"
    return ""


def _get_gpu_instance_negattrs(dev_gi_prf_id) -> str:
    """
    Get negative attributes for a given GPU Instance Profile ID.

    Args:
        dev_gi_prf_id:
            The GPU Instance Profile ID.

    Returns:
        A string representing the negative attributes, or an empty string if none.

    """
    if dev_gi_prf_id in [
        pynvml.NVML_GPU_INSTANCE_PROFILE_1_SLICE_NO_ME,
        pynvml.NVML_GPU_INSTANCE_PROFILE_2_SLICE_NO_ME,
    ]:
        return "me"
    return ""


def _is_vgpu(dev_config: bytes) -> bool:
    """
    Determine if the device is a vGPU based on its PCI configuration space.

    """
    status = 0x06
    cap_supported = 0x10
    cap_start = 0x34
    cap_vendor_specific_id = 0x09

    if dev_config[status] & cap_supported == 0:
        return False

    # Find the capability list
    dev_cap: bytes | None = None
    visited = set()
    pos = dev_config[cap_start]
    while pos != 0 and pos not in visited and pos < len(dev_config) - 2:
        visited.add(pos)
        ptr = dev_config[pos : pos + 3]  # id, next, length
        if ptr[0] == 0xFF:
            break
        if ptr[0] == cap_vendor_specific_id:
            dev_cap = dev_config[pos : pos + ptr[2]]
            break
        pos = ptr[1]

    if not dev_cap or len(dev_cap) < 5:
        return False

    # Check for vGPU signature,
    # which is either 0x56 (NVIDIA vGPU) or 0x46 (NVIDIA GRID).
    return dev_cap[3] == 0x56 or dev_cap[4] == 0x46
