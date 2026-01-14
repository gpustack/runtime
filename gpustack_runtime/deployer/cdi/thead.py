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
    manufacturer_to_config_kind,
)


class THeadGenerator(Generator):
    """
    CDI generator for T-Head devices.
    """

    def __init__(self):
        super().__init__(ManufacturerEnum.THEAD)

    def generate(self, devices: Devices | None = None) -> Config | None:
        """
        Generate the CDI configuration for T-Head devices.

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

        cdi_devices: list[ConfigDevice] = []

        all_container_edits_device_nodes = [
            "/dev/alixpu",
            "/dev/alixpu_ctl",
        ]
        for dev in devices:
            if not dev:
                continue
            all_container_edits_device_nodes.append(
                f"/dev/alixpu_ppu{dev.index}",
            )

            # Add specific container edits for each device
            cdi_container_edits = ConfigContainerEdits(
                device_nodes=[
                    "/dev/alixpu",
                    "/dev/alixpu_ctl",
                    f"/dev/alixpu_ppu{dev.index}",
                ],
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

        # Add common container edits for all devices
        cdi_devices.append(
            ConfigDevice(
                name="all",
                container_edits=ConfigContainerEdits(
                    device_nodes=all_container_edits_device_nodes,
                ),
            ),
        )

        return Config(
            kind=kind,
            devices=cdi_devices,
        )
