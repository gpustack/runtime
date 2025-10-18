from __future__ import annotations

import contextlib
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class PCIDevice:
    vendor: str
    """
    Vendor ID of the PCI device.
    """
    path: str
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
        with contextlib.suppress(OSError), dev_vendor_file.open("r") as vf:
            dev_vendor = vf.read().strip()
            if vendor and dev_vendor not in vendor:
                continue
        if not dev_vendor:
            continue

        dev_class_file = dev_path / "class"
        dev_config_file = dev_path / "config"
        if not dev_class_file.exists() or not dev_config_file.exists():
            continue

        dev_class, dev_config = None, None
        with contextlib.suppress(OSError):
            with dev_class_file.open("rb") as f:
                dev_class = f.read().strip()
            with dev_config_file.open("rb") as f:
                dev_config = f.read().strip()
        if dev_class is None or dev_config is None:
            continue

        pci_devices.append(
            PCIDevice(
                vendor=dev_vendor,
                path=str(dev_path),
                address=dev_address,
                class_=dev_class,
                config=dev_config,
            ),
        )

    return pci_devices


@dataclass
class DeviceFile:
    path: str
    """
    Path to the device file.
    """
    number: int | None = None
    """
    Number of the device file.
    """


def get_device_files(pattern: str, directory: Path | str = "/dev") -> list[DeviceFile]:
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

    if isinstance(directory, str):
        directory = Path(directory)

    device_files = []
    if not directory.exists():
        return device_files

    regex = re.compile(f"^{directory!s}/{pattern}$")
    for file_path in directory.iterdir():
        matched = regex.match(str(file_path))
        if not matched:
            continue
        file_number = matched.group("number")
        try:
            file_number = int(file_number)
        except ValueError:
            file_number = None
        device_files.append(
            DeviceFile(
                path=str(file_path),
                number=file_number,
            ),
        )

    # Sort by number in ascending order, None values at the end
    return sorted(
        device_files,
        key=lambda df: (df.number is None, df.number),
    )


def support_command(command: str) -> bool:
    """
    Determine whether a command is available.

    Args:
        command:
            The name of the command to check.

    Returns:
        True if the command is available, False otherwise.

    """
    return shutil.which(command) is not None


def execute_shell_command(command: str, cwd: str | None = None) -> str | None:
    """
    Execute a shell command and return its output.

    Args:
        command:
            The command to run.
        cwd:
            The working directory to run the command in, or None to use a temporary directory.

    Returns:
        The output of the command.

    Raises:
        If the command fails or returns a non-zero exit code.

    """
    if cwd is None:
        cwd = tempfile.gettempdir()

    command = command.strip()
    if not command:
        msg = "Command is empty"
        raise ValueError(msg)

    try:
        result = subprocess.run(  # noqa: S602
            command,
            capture_output=True,
            check=False,
            shell=True,
            text=True,
            cwd=cwd,
            encoding="utf-8",
        )
    except Exception as e:
        msg = f"Failed to run command '{command}'"
        raise RuntimeError(msg) from e
    else:
        if result.returncode != 0:
            msg = f"Unexpected result: {result}"
            raise RuntimeError(msg)

        return result.stdout


def execute_command(command: list[str], cwd: str | None = None) -> str | None:
    """
    Execute a command and return its output.

    Args:
        command:
            The command to run.
        cwd:
            The working directory to run the command in, or None to use a temporary directory.

    Returns:
        The output of the command.

    Raises:
        If the command fails or returns a non-zero exit code.

    """
    if cwd is None:
        cwd = tempfile.gettempdir()

    if not command:
        msg = "Command list is empty"
        raise ValueError(msg)

    try:
        result = subprocess.run(  # noqa: S603
            command,
            capture_output=True,
            check=False,
            text=True,
            cwd=cwd,
            encoding="utf-8",
        )
    except Exception as e:
        msg = f"Failed to run command '{command}'"
        raise RuntimeError(msg) from e
    else:
        if result.returncode != 0:
            msg = f"Unexpected result: {result}"
            raise RuntimeError(msg)

        return result.stdout


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to int.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted int value, or 0 if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to float.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted float value, or 0.0 if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, float):
        return value
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    """
    Safely convert a value to bool.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted bool value, or False if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value_lower = value.strip().lower()
        if value_lower in ("true", "1", "yes", "on"):
            return True
        if value_lower in ("false", "0", "no", "off"):
            return False
    try:
        return bool(value)
    except (ValueError, TypeError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    """
    Safely convert a value to str.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted str value, or an empty string if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore")
        except (ValueError, TypeError):
            return default
    try:
        return str(value)
    except (ValueError, TypeError):
        return default


def kibibyte_to_mebibyte(value: int) -> int:
    """
    Convert KiB to MiB.

    Args:
        value:
            The value in kilobytes.

    Returns:
        The value in MiB, or 0 if the input is None or negative.

    """
    if value is None or value < 0:
        return 0

    try:
        return value >> 10
    except (ValueError, TypeError, OverflowError):
        return 0


def byte_to_mebibyte(value: int) -> int:
    """
    Convert bytes to MiB.

    Args:
        value:
            The value in bytes.

    Returns:
        The value in MiB, or 0 if the input is None or negative.

    """
    if value is None or value < 0:
        return 0

    try:
        return value >> 20
    except (ValueError, TypeError, OverflowError):
        return 0


def get_brief_version(version: str | None) -> str | None:
    """
    Get a brief version string,
    e.g., "11.2.152" -> "11.2".

    Args:
        version:
            The full version string.

    Returns:
        The brief version string, or None if the input is None or empty.

    """
    if not version:
        return None

    splits = version.split(".", 3)
    if len(splits) >= 2:
        return ".".join(splits[:2])
    if len(splits) == 1:
        return splits[0]
    return None


def get_utilization(used: int | None, total: int | None) -> float:
    """
    Calculate utilization percentage.

    Args:
        used:
            The used value.
        total:
            The total value.

    Returns:
        The utilization percentage, rounded to two decimal places.

    """
    if used is None or total is None or used < 0 or total <= 0:
        return 0.0
    try:
        result = (used / total) * 100
    except (OverflowError, ZeroDivisionError):
        return 0.0
    return round(result, 2)


def get_memory() -> tuple[int, int]:
    """
    Get total and used memory in MiB on Linux systems.
    Refer to https://docs.nvidia.com/dgx/dgx-spark/known-issues.html.

    Returns:
        A tuple containing total and used memory in MiB.
        If unable to read /proc/meminfo, returns (0, 0).

    """
    try:
        with Path("/proc/meminfo").open() as f:
            mem_total_kb = -1
            mem_available_kb = -1
            swap_total_kb = -1
            swap_free_kb = -1
            huge_tlb_total_pages = -1
            huge_tlb_free_pages = -1
            huge_tlb_page_size = -1

            for line in f:
                line = line.strip()  # noqa: PLW2901

                if line.startswith("MemTotal:"):
                    with contextlib.suppress(ValueError, IndexError):
                        mem_total_kb = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    with contextlib.suppress(ValueError, IndexError):
                        mem_available_kb = int(line.split()[1])
                elif line.startswith("SwapTotal:"):
                    with contextlib.suppress(ValueError, IndexError):
                        swap_total_kb = int(line.split()[1])
                elif line.startswith("SwapFree:"):
                    with contextlib.suppress(ValueError, IndexError):
                        swap_free_kb = int(line.split()[1])
                elif line.startswith("HugePages_Total:"):
                    with contextlib.suppress(ValueError, IndexError):
                        huge_tlb_total_pages = int(line.split()[1])
                elif line.startswith("HugePages_Free:"):
                    with contextlib.suppress(ValueError, IndexError):
                        huge_tlb_free_pages = int(line.split()[1])
                elif line.startswith("Hugepagesize:"):
                    with contextlib.suppress(ValueError, IndexError):
                        huge_tlb_page_size = int(line.split()[1])

                if (
                    mem_total_kb != -1
                    and mem_available_kb != -1
                    and swap_total_kb != -1
                    and swap_free_kb != -1
                    and huge_tlb_total_pages != -1
                    and huge_tlb_free_pages != -1
                    and huge_tlb_page_size != -1
                ):
                    break

            if huge_tlb_total_pages not in (0, -1):
                mem_available_kb = huge_tlb_free_pages * huge_tlb_page_size
                swap_free_kb = 0

            mem_total_kb = mem_total_kb + swap_total_kb
            mem_available_kb = mem_available_kb + swap_free_kb
            mem_used_kb = mem_total_kb - mem_available_kb

            return (
                kibibyte_to_mebibyte(mem_total_kb),
                kibibyte_to_mebibyte(mem_used_kb),
            )

    except OSError:
        return 0, 0
