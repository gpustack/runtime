from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PCIDevice:
    vendor: str
    """
    Vendor ID of the PCI device.
    """
    path: Path
    """
    Path to the PCI device in sysfs.
    """
    address: str
    """
    Address of the PCI device.
    """
    class_: bytes
    """
    Class of the PCI device.
    """
    config: bytes
    """
    Device ID of the PCI device.
    """


def get_pci_devices(
    address: list[str] | str | None = None,
    vendor: list[str] | str | None = None,
) -> list[PCIDevice]:
    """
    Get PCI devices.

    Args:
        address: List of PCI addresses or a single address to filter by.
        vendor: List of vendor IDs or a single vendor ID to filter by.

    Returns:
        List of PCIDevice objects.

    """
    pci_devices = []
    sysfs_pci_path = Path("/sys/bus/pci/devices")
    if not sysfs_pci_path.exists():
        return pci_devices

    if address and isinstance(address, str):
        address = [address]
    if vendor and isinstance(vendor, str):
        vendor = [vendor]

    for dev_path in sysfs_pci_path.iterdir():
        dev_address = dev_path.name
        if address and dev_address not in address:
            continue

        dev_vendor_file = dev_path / "vendor"
        if not dev_vendor_file.exists():
            continue
        with dev_vendor_file.open("r") as vf:
            dev_vendor = vf.read().strip()
            if vendor and dev_vendor not in vendor:
                continue

        dev_class_file = dev_path / "class"
        dev_config_file = dev_path / "config"
        if not dev_class_file.exists() or not dev_config_file.exists():
            continue

        with dev_class_file.open("rb") as f:
            dev_class = f.read().strip()
        with dev_config_file.open("rb") as f:
            dev_config = f.read().strip()

        pci_devices.append(
            PCIDevice(
                vendor=dev_vendor,
                path=dev_path,
                address=dev_address,
                class_=dev_class,
                config=dev_config,
            ),
        )

    return pci_devices
