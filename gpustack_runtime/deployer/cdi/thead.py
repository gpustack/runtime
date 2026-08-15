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
from .__utils__ import device_to_cdi_device_node


class THeadGenerator(Generator):
    """
    CDI generator for T-Head devices.
    """

    def __init__(self):
        super().__init__(ManufacturerEnum.THEAD)

    def generate(
        self,
        devices: Devices | None = None,
        include_all_devices: bool = True,
    ) -> Config | None:
        """
        Generate the CDI configuration for T-Head devices.

        Args:
            devices:
                The detected devices.
                If None, all available devices are considered.
            include_all_devices:
                Whether to include a device entry that represents all T-Head devices.

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

        cdi_devices: list[ConfigDevice] = []

        common_device_nodes = []
        for p in [
            "/dev/alixpu",
            "/dev/alixpu_ctl",
        ]:
            cdn = device_to_cdi_device_node(
                path=p,
            )
            if cdn:
                common_device_nodes.append(cdn)
        if not common_device_nodes:
            return None

        all_device_nodes = []

        for dev in devices:
            if not dev:
                continue

            container_device_nodes = []

            # Named after the card ordinal, i.e. the enumeration index, not
            # after the driver's minor number that the detector records in
            # appendix["minor_number"]. Unlike /dev/nvidia{N} and
            # /dev/iluvatar{N}, which the minor number does name, the operator
            # records T-Head's purely to PROVE this node addresses the card it
            # describes, by comparing it against the node's character-device
            # minor.
            cdn = device_to_cdi_device_node(
                path=f"/dev/alixpu_ppu{dev.index}",
            )
            if not cdn:
                continue

            # So make that comparison rather than assume it: where the detector
            # read a minor, the node this ordinal names must carry it. The two
            # numbers are independent -- neither is computed from the other, at
            # any offset or none -- so a mismatch means this ordinal addresses a
            # neighbouring accelerator, which the operator's allocator refuses
            # outright rather than hand over.
            dev_minor_number = dev.appendix.get("minor_number")
            if dev_minor_number is not None and cdn.minor != dev_minor_number:
                continue

            all_device_nodes.append(cdn)
            container_device_nodes.append(cdn)

            # Add specific container edits for each device.
            cdi_container_edits = ConfigContainerEdits(
                device_nodes=container_device_nodes,
            )
            cdi_devices.append(
                ConfigDevice(
                    name=str(dev.index),
                    container_edits=cdi_container_edits,
                ),
            )
            cdi_devices.append(
                ConfigDevice(
                    name=dev.uuid,
                    container_edits=cdi_container_edits,
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
            container_edits=ConfigContainerEdits(
                env=[
                    f"{runtime_env}=void",
                ],
                device_nodes=common_device_nodes,
            ),
        )
