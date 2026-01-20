from __future__ import annotations as __future_annotations__

from ...detector import (
    Devices,
    ManufacturerEnum,
    detect_devices,
    filter_devices_by_manufacturer,
)
from .__types__ import (
    Config,
    ConfigContainerEdits,
    ConfigDevice,
    Generator,
    manufacturer_to_cdi_kind,
    manufacturer_to_runtime_env,
)
from .__utils__ import device_to_cdi_device_node, path_to_cdi_mount


class AscendGenerator(Generator):
    """
    CDI generator for Ascend devices.
    """

    def __init__(self):
        super().__init__(ManufacturerEnum.ASCEND)

    def generate(
        self,
        devices: Devices | None = None,
        include_all_devices: bool = True,
    ) -> Config | None:
        """
        Generate the CDI configuration for Ascend devices.

        Args:
            devices:
                The detected devices.
                If None, all available devices are considered.
            include_all_devices:
                Whether to include a device entry that represents all Ascend devices.

        Returns:
            The Config object, or None if not supported.

        """
        if devices is None:
            devices = detect_devices(manufacturer=self.manufacturer)
        else:
            devices = filter_devices_by_manufacturer(
                devices,
                manufacturer=self.manufacturer,
            )

        if not devices:
            return None

        kind = manufacturer_to_cdi_kind(self.manufacturer)
        if not kind:
            return None

        common_device_nodes = []
        for p in [
            "/dev/davinci_manager_docker",
            "/dev/davinci_manager",
        ]:
            cdn = device_to_cdi_device_node(
                path=p,
                container_path="/dev/davinci_manager",
            )
            if cdn:
                common_device_nodes.append(cdn)
                break
        for p in [
            "/dev/dvpp_cmdlist",
            "/dev/uburma",
            "/dev/ummu",
            "/dev/devmm_svm",
            "/dev/hisi_hdc",
        ]:
            cdn = device_to_cdi_device_node(
                path=p,
            )
            if cdn:
                common_device_nodes.append(cdn)
        if not common_device_nodes:
            return None

        common_mounts = []
        for p in [
            "/etc/hccl_rootinfo.json",
            "/usr/local/Ascend/driver/topo",
            "/usr/local/Ascend/driver/lib64",
            "/usr/local/Ascend/driver/include",
            "/usr/local/dcmi",
            "/usr/local/bin/npu-smi",
            "/var/queue_schedule",
        ]:
            cm = path_to_cdi_mount(
                path=p,
            )
            if cm:
                common_mounts.append(cm)

        cdi_devices: list[ConfigDevice] = []

        all_device_nodes = []

        for dev in devices:
            if not dev:
                continue

            container_device_nodes = []

            cdn_path = f"/dev/davinci{dev.index}"
            if dev.appendix.get("vgpu", False):
                cdn_path = f"/dev/vdavinci{dev.index}"
            cdn = device_to_cdi_device_node(
                path=cdn_path,
            )
            if not cdn:
                continue
            all_device_nodes.append(cdn)
            container_device_nodes.append(cdn)

            # Add specific container edits for each device.
            cdi_devices.append(
                ConfigDevice(
                    name=str(dev.index),
                    container_edits=ConfigContainerEdits(
                        device_nodes=container_device_nodes,
                    ),
                ),
            )

        if not cdi_devices:
            return None

        # Add common container edits for all devices.
        if include_all_devices:
            cdi_devices.append(
                ConfigDevice(
                    name="all",
                    container_edits=ConfigContainerEdits(
                        device_nodes=all_device_nodes,
                    ),
                ),
            )

        runtime_env = manufacturer_to_runtime_env(self.manufacturer)

        return Config(
            kind=kind,
            devices=cdi_devices,
            container_edits=[
                ConfigContainerEdits(
                    env=[
                        f"{runtime_env}=void",
                    ],
                    device_nodes=common_device_nodes,
                    mounts=common_mounts,
                ),
            ],
        )
