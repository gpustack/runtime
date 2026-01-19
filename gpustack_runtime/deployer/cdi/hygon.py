from __future__ import annotations as __future_annotations__

from pathlib import Path

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
    manufacturer_to_config_kind,
)


class HygonGenerator(Generator):
    """
    CDI generator for Hygon devices.
    """

    def __init__(self):
        super().__init__(ManufacturerEnum.HYGON)

    def generate(self, devices: Devices | None = None) -> Config | None:
        """
        Generate the CDI configuration for Hygon devices.

        Args:
            devices: The detected devices.
            If None, all available devices are considered.

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

        kind = manufacturer_to_config_kind(self.manufacturer)
        if not kind:
            return None

        common_device_nodes = []
        for p in [
            "/dev/kfd",
            "/dev/mkfd",
        ]:
            if Path(p).exists():
                common_device_nodes.append(p)
        if not common_device_nodes:
            return None

        cdi_devices: list[ConfigDevice] = []

        all_device_nodes = list(common_device_nodes)

        for dev in devices:
            if not dev:
                continue

            container_device_nodes = list(common_device_nodes)

            card_id = dev.appendix.get("card_id")
            if card_id is not None:
                dn = f"/dev/dri/card{card_id}"
                all_device_nodes.append(dn)
                container_device_nodes.append(dn)
            renderd_id = dev.appendix.get("renderd_id")
            if renderd_id is not None:
                dn = f"/dev/dri/renderD{renderd_id}"
                all_device_nodes.append(dn)
                container_device_nodes.append(dn)

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
        cdi_devices.append(
            ConfigDevice(
                name="all",
                container_edits=ConfigContainerEdits(
                    device_nodes=all_device_nodes,
                ),
            ),
        )

        return Config(
            kind=kind,
            devices=cdi_devices,
        )
