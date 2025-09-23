from __future__ import annotations

import logging
from functools import lru_cache

from .. import envs
from . import pydcmi
from .__types__ import Detector, Device, Devices, ManufacturerEnum
from .__utils__ import get_pci_devices

logger = logging.getLogger(__name__)


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

        try:
            pydcmi.dcmi_init()
            pydcmi.dcmi_shutdown()
            supported = True
        except pydcmi.DCMIError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to initialize DCMI")

        return supported

    def __init__(self):
        super().__init__(ManufacturerEnum.ASCEND)

    def detect(self) -> Devices | None:
        """
        Detect Ascend NPUs using pydcmi.

        Returns:
            A list of detected Ascend NPU devices,
            or None if detection is not supported.

        """
        if not self.is_supported():
            return None

        ret: Devices = []

        try:
            pci_devs = get_pci_devices(vendor="0x19e5")
            if not pci_devs:
                return ret

            pydcmi.dcmi_init()

            sys_driver_ver = pydcmi.dcmi_get_dcmi_version()
            sys_driver_ver_t = [
                int(v) if v.isdigit() else v for v in sys_driver_ver.split(".")
            ]

            dev_runner_ver = pydcmi.dcmi_get_cann_version()
            dev_runner_ver_t = None
            if dev_runner_ver:
                dev_runner_ver_t = [
                    int(v) if v.isdigit() else v for v in dev_runner_ver.split(".")
                ]

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
                        dev_name = f"Ascend {dev_virt_info.query_info.name}"
                        dev_mem, dev_mem_used = 0, 0
                        if hasattr(dev_virt_info.query_info.computing, "memory_size"):
                            dev_mem = (
                                dev_virt_info.query_info.computing.memory_size << 20
                            )
                        dev_index = dev_virt_info.vdev_id
                    else:
                        dev_chip_info = pydcmi.dcmi_get_device_chip_info_v2(
                            dev_card_id,
                            dev_device_id,
                        )
                        dev_cores_aicore = dev_chip_info.aicore_cnt
                        dev_name = (
                            f"{dev_chip_info.chip_type} {dev_chip_info.chip_name}"
                        )
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
                    dev_temp = pydcmi.dcmi_get_device_temperature(
                        dev_card_id,
                        dev_device_id,
                    )
                    dev_power_used = pydcmi.dcmi_get_device_power_info(
                        dev_card_id,
                        dev_device_id,
                    )
                    dev_appendix = {
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
                            driver_version_tuple=sys_driver_ver_t,
                            runtime_version=dev_runner_ver,
                            runtime_version_tuple=dev_runner_ver_t,
                            compute_capability="",
                            compute_capability_tuple=None,
                            cores=dev_cores_aicore,
                            cores_utilization=dev_util_aicore,
                            memory=dev_mem >> 20,  # Convert from bytes to MiB
                            memory_used=dev_mem_used >> 20,  # Convert from bytes to MiB
                            memory_utilization=(
                                (dev_mem_used / dev_mem) * 100 if dev_mem > 0 else 0
                            ),
                            temperature=dev_temp,
                            power=0,
                            power_used=dev_power_used / 10,  # Convert from 0.1W to W
                            appendix=dev_appendix,
                        ),
                    )

        except pydcmi.DCMIError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to fetch devices")
            raise
        except Exception:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to process devices fetching")
            raise
        finally:
            pydcmi.dcmi_shutdown()

        return ret


def _get_device_memory_info(dev_card_id, dev_device_id) -> tuple[int, int]:
    try:
        dev_hbm_info = pydcmi.dcmi_get_device_hbm_info(dev_card_id, dev_device_id)
        if dev_hbm_info.memory_size > 0:
            dev_mem = dev_hbm_info.memory_size << 20  # Convert from MB to bytes
            dev_mem_used = dev_hbm_info.memory_usage << 20  # Convert from MB to bytes
        else:
            dev_memory_info = pydcmi.dcmi_get_device_memory_info_v3(
                dev_card_id,
                dev_device_id,
            )
            dev_mem = dev_memory_info.memory_size << 20  # Convert from MB to bytes
            dev_mem_used = dev_memory_info.utiliza << 20  # Convert from MB to bytes
    except pydcmi.DCMIError as e:
        if e.value == pydcmi.DCMI_ERROR_FUNCTION_NOT_FOUND:
            dev_memory_info = pydcmi.dcmi_get_device_memory_info_v3(
                dev_card_id,
                dev_device_id,
            )
            dev_mem = dev_memory_info.memory_size << 20  # Convert from MB to bytes
            dev_mem_used = dev_memory_info.utiliza << 20  # Convert from MB to bytes
        else:
            raise

    return dev_mem, dev_mem_used


def _get_device_roce_network_info(
    dev_card_id,
    dev_device_id,
) -> tuple[str | None, str | None, str | None]:
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception("Failed to get device roce network info")

    return ip, mask, gateway


def _get_device_virtual_info(
    dev_card_id,
    dev_device_id,
) -> pydcmi.c_dcmi_vdev_query_stru | None:
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception("Failed to get device virtual info")
    else:
        return c_vdev_query_stru

    return None
