from __future__ import annotations

import contextlib
import logging
from functools import lru_cache
from math import ceil

import pynvml

from .__types__ import Detector, Device, Devices, ManufacturerEnum

logger = logging.getLogger(__name__)


class NVIDIADetector(Detector):
    """
    Detect NVIDIA GPUs.
    """

    def __init__(self):
        super().__init__(ManufacturerEnum.NVIDIA)

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if NVIDIA detection is supported.

        Returns:
            True if supported, False otherwise.

        """
        try:
            pynvml.nvmlInit()
            pynvml.nvmlShutdown()
        except pynvml.NVMLError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to initialize NVML")
        else:
            return True

        return False

    def detect(self) -> Devices | None:
        """
        Detect NVIDIA GPUs using pynvml.

        Returns:
            A list of detected NVIDIA GPU devices.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pynvml.nvmlInit()

            sys_driver_ver = pynvml.nvmlSystemGetDriverVersion()
            sys_driver_ver_t = [
                int(v) if v.isdigit() else v for v in sys_driver_ver.split(".")
            ]

            dev_runtime_ver = pynvml.nvmlSystemGetCudaDriverVersion()
            dev_runtime_ver_t = [
                dev_runtime_ver // 1000,
                (dev_runtime_ver % 1000) // 10,
            ]
            dev_runtime_ver = f"{dev_runtime_ver_t[0]}.{dev_runtime_ver_t[1]}"

            dev_count = pynvml.nvmlDeviceGetCount()
            for dev_idx in range(dev_count):
                dev = pynvml.nvmlDeviceGetHandleByIndex(dev_idx)

                dev_uuid = pynvml.nvmlDeviceGetUUID(dev)
                dev_mem = pynvml.nvmlDeviceGetMemoryInfo(dev)
                dev_util = pynvml.nvmlDeviceGetUtilizationRates(dev)
                dev_temp = pynvml.nvmlDeviceGetTemperature(
                    dev,
                    pynvml.NVML_TEMPERATURE_GPU,
                )
                dev_cc_t = pynvml.nvmlDeviceGetCudaComputeCapability(dev)
                dev_cc = f"{dev_cc_t[0]}.{dev_cc_t[1]}"
                dev_appendix = {
                    "arch_family": _get_arch_family(dev_cc_t),
                }

                dev_fabric = pynvml.c_nvmlGpuFabricInfo_v2_t()
                try:
                    r = pynvml.nvmlDeviceGetGpuFabricInfoV(dev, dev_fabric)
                    if r != pynvml.NVML_SUCCESS:
                        dev_fabric = None
                    if dev_fabric.state != pynvml.NVML_GPU_FABRIC_STATE_COMPLETED:
                        dev_fabric = None
                except pynvml.NVMLError:
                    dev_fabric = None
                if dev_fabric:
                    dev_appendix["fabric_cluster_uuid"] = dev_fabric.clusterUuid
                    dev_appendix["fabric_clique_id"] = dev_fabric.cliqueId

                dev_mig_mode = pynvml.NVML_DEVICE_MIG_DISABLE
                with contextlib.suppress(pynvml.NVMLError):
                    dev_mig_mode, _ = pynvml.nvmlDeviceGetMigMode(dev)

                # If MIG is not enabled, return the GPU itself.

                if dev_mig_mode == pynvml.NVML_DEVICE_MIG_DISABLE:
                    dev_name = pynvml.nvmlDeviceGetName(dev)
                    ret.append(
                        Device(
                            manufacturer=self.manufacturer,
                            name=dev_name,
                            uuid=dev_uuid,
                            driver_version=sys_driver_ver,
                            driver_version_tuple=sys_driver_ver_t,
                            runtime_version=dev_runtime_ver,
                            runtime_version_tuple=dev_runtime_ver_t,
                            compute_capability=dev_cc,
                            compute_capability_tuple=dev_cc_t,
                            cores=1,
                            cores_utilization=dev_util.gpu,
                            memory=dev_mem.total >> 20,
                            memory_used=dev_mem.used >> 20,
                            memory_utilization=(dev_mem.used * 100 // dev_mem.total),
                            temperature=dev_temp,
                            appendix=dev_appendix,
                        ),
                    )

                    continue

                # Otherwise, get MIG devices,
                # inspired by https://github.com/NVIDIA/go-nvlib/blob/fdfe25d0ffc9d7a8c166f4639ef236da81116262/pkg/nvlib/device/mig_device.go#L61-L154.

                mdev_name = ""
                mdev_cores = 0
                mdev_count = pynvml.nvmlDeviceGetMaxMigDeviceCount(dev)
                for mdev_idx in range(mdev_count):
                    mdev = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(dev, mdev_idx)

                    mdev_uuid = pynvml.nvmlDeviceGetUUID(mdev)
                    mdev_mem = pynvml.nvmlDeviceGetMemoryInfo(mdev)
                    mdev_temp = pynvml.nvmlDeviceGetTemperature(
                        mdev,
                        pynvml.NVML_TEMPERATURE_GPU,
                    )
                    mdev_appendix = dev_appendix.copy()

                    mdev_gi_id = pynvml.nvmlDeviceGetGpuInstanceId(mdev)
                    mdev_appendix["gpu_instance_id"] = mdev_gi_id

                    mdev_ci_id = pynvml.nvmlDeviceGetComputeInstanceId(mdev)
                    mdev_appendix["compute_instance_id"] = mdev_ci_id

                    if not mdev_name:
                        mdev_attrs = pynvml.nvmlDeviceGetAttributes(mdev)

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

                                    gi_slices = _get_gpu_instance_slices(dev_gi_prf_id)
                                    gi_attrs = _get_gpu_instance_attrs(dev_gi_prf_id)
                                    gi_neg_attrs = _get_gpu_instance_negative_attrs(
                                        dev_gi_prf_id,
                                    )
                                    ci_slices = _get_compute_instance_slices(
                                        dev_ci_prf_id,
                                    )
                                    ci_mem = _get_compute_instance_memory_in_gib(
                                        dev_mem,
                                        mdev_attrs,
                                    )

                                    if gi_slices == ci_slices:
                                        mdev_name = f"{gi_slices}g.{ci_mem}gb"
                                    else:
                                        mdev_name = (
                                            f"{ci_slices}c.{gi_slices}g.{ci_mem}gb"
                                        )
                                    if gi_attrs:
                                        mdev_name += f"+{gi_attrs}"
                                    if gi_neg_attrs:
                                        mdev_name += f"-{gi_neg_attrs}"

                                    mdev_cores = ci_slices

                                    break

                    ret.append(
                        Device(
                            manufacturer=self.manufacturer,
                            name=mdev_name,
                            uuid=mdev_uuid,
                            driver_version=sys_driver_ver,
                            driver_version_tuple=sys_driver_ver_t,
                            runtime_version=dev_runtime_ver,
                            runtime_version_tuple=dev_runtime_ver_t,
                            compute_capability=dev_cc,
                            compute_capability_tuple=dev_cc_t,
                            cores=mdev_cores,
                            memory=mdev_mem.total >> 20,
                            memory_used=mdev_mem.used >> 20,
                            memory_utilization=(
                                (mdev_mem.used >> 20) * 100 // (mdev_mem.total >> 20)
                            ),
                            temperature=mdev_temp,
                            appendix=mdev_appendix,
                        ),
                    )
        except pynvml.NVMLError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to fetch devices")
            raise
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to process devices fetching")
            raise
        finally:
            pynvml.nvmlShutdown()

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
            return "tesla"
        case 2:
            return "fermi"
        case 3:
            return "kepler"
        case 5:
            return "maxwell"
        case 6:
            return "pascal"
        case 7:
            return "volta" if dev_cc_t[1] < 5 else "turing"
        case 8:
            if dev_cc_t[1] < 9:
                return "ampere"
            return "ada-lovelace"
        case 9:
            return "hopper"
        case 10 | 12:
            return "blackwell"
    return "unknown"


def _get_gpu_instance_slices(dev_gi_prf_id: int) -> int:
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


def _get_gpu_instance_negative_attrs(dev_gi_prf_id) -> str:
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


def _get_compute_instance_slices(dev_ci_prf_id: int) -> int:
    """
    Get the number of slices for a given Compute Instance Profile ID.

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


def _get_compute_instance_memory_in_gib(dev_mem, mdev_attrs) -> int:
    """
    Compute the memory size of a MIG compute instance in GiB.

    Args:
        dev_mem:
            The total memory info of the parent GPU device.
        mdev_attrs:
            The attributes of the MIG device.

    Returns:
        The memory size in GiB.

    """
    gib = round(
        ceil(
            (mdev_attrs.memorySizeMB * (1 << 20)) / dev_mem.total * 8,
        )
        / 8
        * ((dev_mem.total + (1 << 30) - 1) / (1 << 30)),
    )
    return gib
