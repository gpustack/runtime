from __future__ import annotations

import re
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


@dataclass
class DeviceFile:
    path: Path
    """
    Path to the device file.
    """
    number: int | None = None
    """
    Number of the device file.
    """


def get_device_files(pattern: str, directory: Path = Path("/dev")) -> list[DeviceFile]:
    r"""
    Get device files with the given pattern.

    Args:
        pattern:
            Pattern of the device files to search for.
            Pattern must include a regex group for the number,
            e.g nvidia(?P<number>\d+).
        directory:
            Directory to search for device files,
            e.g /dev.

    Returns:
        List of DeviceFile objects.

    """
    if "(?P<number>" not in pattern:
        msg = "Pattern must include a regex group for the number, e.g nvidia(?P<number>\\d+)."
        raise ValueError(msg)

    device_files = []
    if not directory.exists():
        return device_files

    regex = re.compile(f"^{directory!s}/{pattern}$")
    for path in directory.iterdir():
        matched = regex.match(str(path))
        if not matched:
            continue
        number = matched.group("number")
        try:
            number = int(number)
        except ValueError:
            number = None
        device_files.append(
            DeviceFile(
                path=path,
                number=number,
            ),
        )

    # Sort by number in ascending order, None values at the end
    return sorted(
        device_files,
        key=lambda df: (df.number is None, df.number),
    )
